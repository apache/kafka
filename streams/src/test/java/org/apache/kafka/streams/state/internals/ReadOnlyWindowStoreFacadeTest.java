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
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Instant;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;

@RunWith(EasyMockRunner.class)
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
        expect(mockedWindowTimestampStore.fetch("key1", 21L))
            .andReturn(ValueAndTimestamp.make("value1", 42L));
        expect(mockedWindowTimestampStore.fetch("unknownKey", 21L))
            .andReturn(null);
        replay(mockedWindowTimestampStore);

        assertThat(readOnlyWindowStoreFacade.fetch("key1", 21L), is("value1"));
        assertNull(readOnlyWindowStoreFacade.fetch("unknownKey", 21L));

        verify(mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnSingleKeyFetchLongParameters() {
        expect(mockedWindowTimestampIterator.next())
            .andReturn(KeyValue.pair(21L, ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(42L, ValueAndTimestamp.make("value2", 23L)));
        expect(mockedWindowTimestampStore.fetch("key1", 21L, 42L))
            .andReturn(mockedWindowTimestampIterator);
        replay(mockedWindowTimestampIterator, mockedWindowTimestampStore);

        final WindowStoreIterator<String> iterator =
            readOnlyWindowStoreFacade.fetch("key1", 21L, 42L);

        assertThat(iterator.next(), is(KeyValue.pair(21L, "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(42L, "value2")));
        verify(mockedWindowTimestampIterator, mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnSingleKeyFetchInstantParameters() {
        expect(mockedWindowTimestampIterator.next())
            .andReturn(KeyValue.pair(21L, ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(42L, ValueAndTimestamp.make("value2", 23L)));
        expect(mockedWindowTimestampStore.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .andReturn(mockedWindowTimestampIterator);
        replay(mockedWindowTimestampIterator, mockedWindowTimestampStore);

        final WindowStoreIterator<String> iterator =
            readOnlyWindowStoreFacade.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertThat(iterator.next(), is(KeyValue.pair(21L, "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(42L, "value2")));
        verify(mockedWindowTimestampIterator, mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnRangeFetchLongParameters() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.fetch("key1", "key2", 21L, 42L))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlyWindowStoreFacade.fetch("key1", "key2", 21L, 42L);

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnRangeFetchInstantParameters() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlyWindowStoreFacade.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnFetchAllLongParameters() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.fetchAll(21L, 42L))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlyWindowStoreFacade.fetchAll(21L, 42L);

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnFetchAllInstantParameters() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator =
            readOnlyWindowStoreFacade.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }

    @Test
    public void shouldReturnPlainKeyValuePairsOnAll() {
        expect(mockedKeyValueWindowTimestampIterator.next())
            .andReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .andReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        expect(mockedWindowTimestampStore.all()).andReturn(mockedKeyValueWindowTimestampIterator);
        replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

        final KeyValueIterator<Windowed<String>, String> iterator = readOnlyWindowStoreFacade.all();

        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1")));
        assertThat(iterator.next(), is(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2")));
        verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
    }
}
