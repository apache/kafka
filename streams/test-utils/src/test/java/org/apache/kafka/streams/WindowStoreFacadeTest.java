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
import org.apache.kafka.streams.TopologyTestDriver.WindowStoreFacade;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
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
import static org.junit.jupiter.api.Assertions.assertNull;
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
    public void shouldFetchSingleValueAndConvert() {
        when(mockedWindowTimestampStore.fetch("key", 100L))
            .thenReturn(ValueAndTimestamp.make("value", 42L));
        when(mockedWindowTimestampStore.fetch("key2", 200L))
            .thenReturn(null);

        assertThat(windowStoreFacade.fetch("key", 100L), is("value"));
        assertNull(windowStoreFacade.fetch("key2", 200L));
    }

    @Test
    public void shouldFetchTimeRangeAndConvertValues() {
        @SuppressWarnings("unchecked")
        final WindowStoreIterator<ValueAndTimestamp<String>> mockIterator = mock(WindowStoreIterator.class);
        final Instant from = Instant.ofEpochMilli(100L);
        final Instant to = Instant.ofEpochMilli(200L);

        when(mockedWindowTimestampStore.fetch("key", from, to)).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(100L, ValueAndTimestamp.make("value1", 10L)))
            .thenReturn(KeyValue.pair(150L, ValueAndTimestamp.make("value2", 20L)));

        final WindowStoreIterator<String> iterator = windowStoreFacade.fetch("key", from, to);
        assertThat(iterator.next(), is(KeyValue.pair(100L, "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(150L, "value2")));
    }

    @Test
    public void shouldBackwardFetchTimeRangeAndConvertValues() {
        @SuppressWarnings("unchecked")
        final WindowStoreIterator<ValueAndTimestamp<String>> mockIterator = mock(WindowStoreIterator.class);
        final Instant from = Instant.ofEpochMilli(100L);
        final Instant to = Instant.ofEpochMilli(200L);

        when(mockedWindowTimestampStore.backwardFetch("key", from, to)).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(150L, ValueAndTimestamp.make("value", 20L)));

        final WindowStoreIterator<String> iterator = windowStoreFacade.backwardFetch("key", from, to);
        assertThat(iterator.next(), is(KeyValue.pair(150L, "value")));
    }

    @Test
    public void shouldFetchKeyRangeAndConvertValues() {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockIterator = mock(KeyValueIterator.class);
        final Instant from = Instant.ofEpochMilli(100L);
        final Instant to = Instant.ofEpochMilli(200L);
        final Windowed<String> windowedKey = new Windowed<>("key1", new TimeWindow(100L, 200L));

        when(mockedWindowTimestampStore.fetch("key1", "key2", from, to)).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(windowedKey, ValueAndTimestamp.make("value", 10L)));

        final KeyValueIterator<Windowed<String>, String> iterator =
            windowStoreFacade.fetch("key1", "key2", from, to);
        assertThat(iterator.next(), is(KeyValue.pair(windowedKey, "value")));
    }

    @Test
    public void shouldBackwardFetchKeyRangeAndConvertValues() {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockIterator = mock(KeyValueIterator.class);
        final Instant from = Instant.ofEpochMilli(100L);
        final Instant to = Instant.ofEpochMilli(200L);
        final Windowed<String> windowedKey = new Windowed<>("key1", new TimeWindow(100L, 200L));

        when(mockedWindowTimestampStore.backwardFetch("key1", "key2", from, to)).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(windowedKey, ValueAndTimestamp.make("value", 10L)));

        final KeyValueIterator<Windowed<String>, String> iterator =
            windowStoreFacade.backwardFetch("key1", "key2", from, to);
        assertThat(iterator.next(), is(KeyValue.pair(windowedKey, "value")));
    }

    @Test
    public void shouldFetchAllTimeRangeAndConvertValues() {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockIterator = mock(KeyValueIterator.class);
        final Instant from = Instant.ofEpochMilli(100L);
        final Instant to = Instant.ofEpochMilli(200L);
        final Windowed<String> windowedKey = new Windowed<>("key", new TimeWindow(100L, 200L));

        when(mockedWindowTimestampStore.fetchAll(from, to)).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(windowedKey, ValueAndTimestamp.make("value", 10L)));

        final KeyValueIterator<Windowed<String>, String> iterator = windowStoreFacade.fetchAll(from, to);
        assertThat(iterator.next(), is(KeyValue.pair(windowedKey, "value")));
    }

    @Test
    public void shouldBackwardFetchAllTimeRangeAndConvertValues() {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockIterator = mock(KeyValueIterator.class);
        final Instant from = Instant.ofEpochMilli(100L);
        final Instant to = Instant.ofEpochMilli(200L);
        final Windowed<String> windowedKey = new Windowed<>("key", new TimeWindow(100L, 200L));

        when(mockedWindowTimestampStore.backwardFetchAll(from, to)).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(windowedKey, ValueAndTimestamp.make("value", 10L)));

        final KeyValueIterator<Windowed<String>, String> iterator = windowStoreFacade.backwardFetchAll(from, to);
        assertThat(iterator.next(), is(KeyValue.pair(windowedKey, "value")));
    }

    @Test
    public void shouldGetAllAndConvertValues() {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockIterator = mock(KeyValueIterator.class);
        final Windowed<String> windowedKey = new Windowed<>("key", new TimeWindow(100L, 200L));

        when(mockedWindowTimestampStore.all()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(windowedKey, ValueAndTimestamp.make("value", 10L)));

        final KeyValueIterator<Windowed<String>, String> iterator = windowStoreFacade.all();
        assertThat(iterator.next(), is(KeyValue.pair(windowedKey, "value")));
    }

    @Test
    public void shouldBackwardAllAndConvertValues() {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockIterator = mock(KeyValueIterator.class);
        final Windowed<String> windowedKey = new Windowed<>("key", new TimeWindow(100L, 200L));

        when(mockedWindowTimestampStore.backwardAll()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next())
            .thenReturn(KeyValue.pair(windowedKey, ValueAndTimestamp.make("value", 10L)));

        final KeyValueIterator<Windowed<String>, String> iterator = windowStoreFacade.backwardAll();
        assertThat(iterator.next(), is(KeyValue.pair(windowedKey, "value")));
    }

}
