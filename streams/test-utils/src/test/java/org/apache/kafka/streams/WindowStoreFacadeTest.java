/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.TopologyTestDriver.WindowStoreFacade;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WindowStoreFacadeTest {
    @SuppressWarnings("unchecked")
    private final TimestampedWindowStore<String, String> mockedWindowTimestampStore = mock(TimestampedWindowStore.class);

    private WindowStoreFacade<String, String> windowStoreFacade;

    @BeforeEach
    public void setup() {
        windowStoreFacade = new WindowStoreFacade<>(mockedWindowTimestampStore);
    }

    @Test
    public void shouldForwardInit() {
        final StateStoreContext context = mock(StateStoreContext.class);
        final StateStore store = mock(StateStore.class);

        windowStoreFacade.init(context, store);
        verify(mockedWindowTimestampStore)
            .init(context, store);
    }

    @Deprecated
    @Test
    public void shouldForwardFlush() {
        windowStoreFacade.flush();
        verify(mockedWindowTimestampStore).flush();
    }

    @Test
    public void shouldPutWindowStartTimestampWithUnknownTimestamp() {
        windowStoreFacade.put("key", "value", 21L);
        verify(mockedWindowTimestampStore)
            .put("key", ValueAndTimestamp.make("value", ConsumerRecord.NO_TIMESTAMP), 21L);
    }

    @Test
    public void shouldForwardCommit() {
        windowStoreFacade.commit(Map.of());
        verify(mockedWindowTimestampStore).commit(Map.of());
    }

    @Test
    public void shouldReturnCommitOffsets() {
        final TopicPartition topicPartition = new TopicPartition("topic", 0);
        when(mockedWindowTimestampStore.committedOffset(any())).thenReturn(42L);

        assertEquals(42L, windowStoreFacade.committedOffset(topicPartition));
        verify(mockedWindowTimestampStore).committedOffset(topicPartition);
    }

    @Deprecated
    @Test
    public void shouldReturnManagedOffsets() {
        when(mockedWindowTimestampStore.managesOffsets()).thenReturn(true);

        assertTrue(windowStoreFacade.managesOffsets());
        verify(mockedWindowTimestampStore).managesOffsets();
    }

    @Test
    public void shouldForwardClose() {
        windowStoreFacade.close();
        verify(mockedWindowTimestampStore).close();
    }

    @Test
    public void shouldReturnName() {
        when(mockedWindowTimestampStore.name()).thenReturn("name");

        assertThat(windowStoreFacade.name(), is("name"));
        verify(mockedWindowTimestampStore).name();
    }

    @Test
    public void shouldReturnIsPersistent() {
        when(mockedWindowTimestampStore.persistent())
            .thenReturn(true, false);

        assertThat(windowStoreFacade.persistent(), is(true));
        assertThat(windowStoreFacade.persistent(), is(false));
        verify(mockedWindowTimestampStore, times(2)).persistent();
    }

    @Test
    public void shouldReturnIsOpen() {
        when(mockedWindowTimestampStore.isOpen())
            .thenReturn(true, false);

        assertThat(windowStoreFacade.isOpen(), is(true));
        assertThat(windowStoreFacade.isOpen(), is(false));
        verify(mockedWindowTimestampStore, times(2)).isOpen();
    }

    @Test
    public void shouldFetchTimeRangeAndConvertValues() {
        @SuppressWarnings("unchecked")
        final WindowStoreIterator<ValueAndTimestamp<String>> mockIterator = mock(WindowStoreIterator.class);
        final long from = 100L;
        final long to = 200L;

        when(mockedWindowTimestampStore.fetch("key", Instant.ofEpochMilli(from), Instant.ofEpochMilli(to))).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(100L, ValueAndTimestamp.make("value1", 10L)))
            .thenReturn(KeyValue.pair(150L, ValueAndTimestamp.make("value2", 20L)));

        try (final WindowStoreIterator<String> iterator = windowStoreFacade.fetch("key", from, to)) {
            assertThat(iterator.next(), is(KeyValue.pair(100L, "value1")));
            assertThat(iterator.next(), is(KeyValue.pair(150L, "value2")));
        }
    }

    @Test
    public void shouldFetchAllTimeRangeAndConvertValues() {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockIterator = mock(KeyValueIterator.class);
        final long from = 100L;
        final long to = 200L;
        final Windowed<String> windowedKey = new Windowed<>("key", new TimeWindow(100L, 200L));

        when(mockedWindowTimestampStore.fetchAll(Instant.ofEpochMilli(from), Instant.ofEpochMilli(to))).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(windowedKey, ValueAndTimestamp.make("value", 10L)));

        try (final KeyValueIterator<Windowed<String>, String> iterator = windowStoreFacade.fetchAll(from, to)) {
            assertThat(iterator.next(), is(KeyValue.pair(windowedKey, "value")));
        }
    }

    @Test
    public void shouldFetchKeyRangeTimeRangeAndConvertValues() {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockIterator = mock(KeyValueIterator.class);
        final long from = 100L;
        final long to = 200L;
        final Windowed<String> windowedKey = new Windowed<>("key", new TimeWindow(100L, 200L));

        when(mockedWindowTimestampStore.fetch("key", "key", Instant.ofEpochMilli(from), Instant.ofEpochMilli(to))).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(windowedKey, ValueAndTimestamp.make("value", 10L)));

        try (final KeyValueIterator<Windowed<String>, String> iterator = windowStoreFacade.fetch("key", "key", from, to)) {
            assertThat(iterator.next(), is(KeyValue.pair(windowedKey, "value")));
        }
    }

    @Test
    public void shouldReturnPosition() {
        when(mockedWindowTimestampStore.getPosition())
            .thenReturn(Position.emptyPosition());

        assertThat(windowStoreFacade.getPosition(), is(Position.emptyPosition()));
        verify(mockedWindowTimestampStore).getPosition();
    }

    @Test
    public void shouldReturnQueryResult() {
        final Query<Object> query = new Query<>() { };
        final QueryConfig queryConfig = new QueryConfig(true);
        final QueryResult<Integer> queryResult = QueryResult.forResult(42);
        when(mockedWindowTimestampStore.<Integer>query(any(), any(), any())).thenReturn(queryResult);

        assertThat(
            windowStoreFacade.query(
                query,
                PositionBound.unbounded(),
                queryConfig
            ),
            is(queryResult));
        verify(mockedWindowTimestampStore).query(query, PositionBound.unbounded(), queryConfig);
    }
}
