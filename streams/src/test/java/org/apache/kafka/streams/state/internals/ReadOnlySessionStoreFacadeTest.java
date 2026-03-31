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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ReadOnlySessionStoreFacadeTest {
    @Mock
    private SessionStoreWithHeaders<String, String> mockedSessionStoreWithHeaders;
    @Mock
    private KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> mockedIterator;

    private ReadOnlySessionStoreFacade<String, String> readOnlySessionStoreFacade;

    @BeforeEach
    public void setup() {
        readOnlySessionStoreFacade = new ReadOnlySessionStoreFacade<>(mockedSessionStoreWithHeaders);
    }

    @Test
    public void shouldReturnPlainValueOnFetchSession() {
        when(mockedSessionStoreWithHeaders.fetchSession("key", 10L, 20L))
            .thenReturn(AggregationWithHeaders.make("value", new RecordHeaders()));

        assertThat(readOnlySessionStoreFacade.fetchSession("key", 10L, 20L), is("value"));
    }

    @Test
    public void shouldReturnNullOnFetchSessionWhenNull() {
        when(mockedSessionStoreWithHeaders.fetchSession("unknownKey", 10L, 20L))
            .thenReturn(null);

        assertNull(readOnlySessionStoreFacade.fetchSession("unknownKey", 10L, 20L));
    }

    @Test
    public void shouldReturnStrippedKeyValuePairsOnFindSessions() {
        when(mockedIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(10L, 20L)),
                AggregationWithHeaders.make("value1", new RecordHeaders())))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new SessionWindow(30L, 40L)),
                AggregationWithHeaders.make("value2", new RecordHeaders())));
        when(mockedSessionStoreWithHeaders.findSessions("key1", 10L, 40L))
            .thenReturn(mockedIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.findSessions("key1", 10L, 40L);

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(10L, 20L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new SessionWindow(30L, 40L)), "value2")));
    }

    @Test
    public void shouldReturnStrippedKeyValuePairsOnBackwardFindSessions() {
        when(mockedIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(30L, 40L)),
                AggregationWithHeaders.make("value1", new RecordHeaders())));
        when(mockedSessionStoreWithHeaders.backwardFindSessions("key1", 10L, 40L))
            .thenReturn(mockedIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.backwardFindSessions("key1", 10L, 40L);

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(30L, 40L)), "value1")));
    }

    @Test
    public void shouldReturnStrippedKeyValuePairsOnFindSessionsWithKeyRange() {
        when(mockedIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(10L, 20L)),
                AggregationWithHeaders.make("value1", new RecordHeaders())));
        when(mockedSessionStoreWithHeaders.findSessions("key1", "key2", 10L, 40L))
            .thenReturn(mockedIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.findSessions("key1", "key2", 10L, 40L);

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(10L, 20L)), "value1")));
    }

    @Test
    public void shouldReturnStrippedKeyValuePairsOnBackwardFindSessionsWithKeyRange() {
        when(mockedIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new SessionWindow(30L, 40L)),
                AggregationWithHeaders.make("value2", new RecordHeaders())));
        when(mockedSessionStoreWithHeaders.backwardFindSessions("key1", "key2", 10L, 40L))
            .thenReturn(mockedIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.backwardFindSessions("key1", "key2", 10L, 40L);

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new SessionWindow(30L, 40L)), "value2")));
    }

    @Test
    public void shouldReturnStrippedKeyValuePairsOnFetch() {
        when(mockedIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(10L, 20L)),
                AggregationWithHeaders.make("value1", new RecordHeaders())))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(30L, 40L)),
                AggregationWithHeaders.make("value2", new RecordHeaders())));
        when(mockedSessionStoreWithHeaders.fetch("key1")).thenReturn(mockedIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.fetch("key1");

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(10L, 20L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(30L, 40L)), "value2")));
    }

    @Test
    public void shouldReturnStrippedKeyValuePairsOnBackwardFetch() {
        when(mockedIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(30L, 40L)),
                AggregationWithHeaders.make("value1", new RecordHeaders())));
        when(mockedSessionStoreWithHeaders.backwardFetch("key1")).thenReturn(mockedIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.backwardFetch("key1");

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(30L, 40L)), "value1")));
    }

    @Test
    public void shouldReturnStrippedKeyValuePairsOnFetchWithKeyRange() {
        when(mockedIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new SessionWindow(10L, 20L)),
                AggregationWithHeaders.make("value1", new RecordHeaders())));
        when(mockedSessionStoreWithHeaders.fetch("key1", "key2")).thenReturn(mockedIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.fetch("key1", "key2");

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new SessionWindow(10L, 20L)), "value1")));
    }

    @Test
    public void shouldReturnStrippedKeyValuePairsOnBackwardFetchWithKeyRange() {
        when(mockedIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new SessionWindow(30L, 40L)),
                AggregationWithHeaders.make("value2", new RecordHeaders())));
        when(mockedSessionStoreWithHeaders.backwardFetch("key1", "key2")).thenReturn(mockedIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlySessionStoreFacade.backwardFetch("key1", "key2");

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new SessionWindow(30L, 40L)), "value2")));
    }
}
