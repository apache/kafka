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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ReadOnlyWindowStoreFacadeTest {
    @Mock
    private TimestampedWindowStore<String, String> mockedWindowTimestampStore;
    @Mock
    private WindowStoreIterator<ValueAndTimestamp<String>> mockedWindowTimestampIterator;
    @Mock
    private KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockedKeyValueWindowTimestampIterator;

    private ReadOnlyWindowStoreFacade<String, String> readOnlyWindowStoreFacade;

    @Before
    public void setup() {
        readOnlyWindowStoreFacade = new ReadOnlyWindowStoreFacade<>(mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnSingleKeyFetch() {
        when(mockedWindowTimestampStore.fetch("key1", 21L))
            .thenReturn(ValueAndTimestamp.make("value1", 42L));
        when(mockedWindowTimestampStore.fetch("unknownKey", 21L))
            .thenReturn(null);

        assertThat(readOnlyWindowStoreFacade.fetch("key1", 21L), is("value1"));
        assertNull(readOnlyWindowStoreFacade.fetch("unknownKey", 21L));
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnSingleKeyFetchLongParameters() {
        when(mockedWindowTimestampIterator.next())
            .thenReturn(KeyValue.pair(21L, ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(42L, ValueAndTimestamp.make("value2", 23L)));
        when(mockedWindowTimestampStore.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedWindowTimestampIterator);

        final WindowStoreIterator<String> iterator =
            readOnlyWindowStoreFacade.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertThat(iterator.next(), is(KeyValue.pair(21L, "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(42L, "value2")));
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnSingleKeyFetchInstantParameters() {
        when(mockedWindowTimestampIterator.next())
            .thenReturn(KeyValue.pair(21L, ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(42L, ValueAndTimestamp.make("value2", 23L)));
        when(mockedWindowTimestampStore.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedWindowTimestampIterator);

        final WindowStoreIterator<String> iterator =
            readOnlyWindowStoreFacade.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertThat(iterator.next(), is(KeyValue.pair(21L, "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(42L, "value2")));
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnRangeFetchLongParameters() {
        when(mockedKeyValueWindowTimestampIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        when(mockedWindowTimestampStore.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedKeyValueWindowTimestampIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlyWindowStoreFacade.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnRangeFetchInstantParameters() {
        when(mockedKeyValueWindowTimestampIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        when(mockedWindowTimestampStore.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedKeyValueWindowTimestampIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlyWindowStoreFacade.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnFetchAllLongParameters() {
        when(mockedKeyValueWindowTimestampIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        when(mockedWindowTimestampStore.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedKeyValueWindowTimestampIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlyWindowStoreFacade.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnFetchAllInstantParameters() {
        when(mockedKeyValueWindowTimestampIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        when(mockedWindowTimestampStore.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedKeyValueWindowTimestampIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlyWindowStoreFacade.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnAll() {
        when(mockedKeyValueWindowTimestampIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        when(mockedWindowTimestampStore.all()).thenReturn(mockedKeyValueWindowTimestampIterator);

        final KeyValueIterator<Windowed<String>, String> iterator = readOnlyWindowStoreFacade.all();

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
    }
}
