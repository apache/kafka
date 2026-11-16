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
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class GenericReadOnlyWindowStoreFacadeTest {

    @Mock
    private ReadOnlyWindowStore<String, ValueAndTimestamp<String>> mockedTimestampedStore;
    @Mock
    private ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> mockedHeadersStore;
    @Mock
    private WindowStoreIterator<ValueAndTimestamp<String>> mockedTimestampedWindowIterator;
    @Mock
    private WindowStoreIterator<ValueTimestampHeaders<String>> mockedHeadersWindowIterator;
    @Mock
    private KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> mockedTimestampedKeyValueIterator;
    @Mock
    private KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> mockedHeadersKeyValueIterator;

    @Test
    public void shouldConvertSingleValueFetch() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedStore.fetch("key1", 21L))
            .thenReturn(ValueAndTimestamp.make("value1", 42L));
        when(mockedTimestampedStore.fetch("unknownKey", 21L))
            .thenReturn(null);

        assertEquals("value1", facade.fetch("key1", 21L));
        assertNull(facade.fetch("unknownKey", 21L));
    }

    @Test
    public void shouldConvertSingleValueFetchWithHeadersConverter() {
        final Function<ValueTimestampHeaders<String>, String> converter = ValueConverters.extractValueFromHeaders();
        final GenericReadOnlyWindowStoreFacade<String, ValueTimestampHeaders<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedHeadersStore, converter);

        when(mockedHeadersStore.fetch("key1", 21L))
            .thenReturn(ValueTimestampHeaders.make("value1", 42L, new RecordHeaders()));
        when(mockedHeadersStore.fetch("unknownKey", 21L))
            .thenReturn(null);

        assertEquals("value1", facade.fetch("key1", 21L));
        assertNull(facade.fetch("unknownKey", 21L));
    }

    @Test
    public void shouldConvertToValueAndTimestamp() {
        final Function<ValueTimestampHeaders<String>, ValueAndTimestamp<String>> converter =
            ValueConverters.extractValueAndTimestampFromHeaders();
        final GenericReadOnlyWindowStoreFacade<String, ValueTimestampHeaders<String>, ValueAndTimestamp<String>> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedHeadersStore, converter);

        when(mockedHeadersStore.fetch("key1", 21L))
            .thenReturn(ValueTimestampHeaders.make("value1", 42L, new RecordHeaders()));

        final ValueAndTimestamp<String> result = facade.fetch("key1", 21L);
        assertEquals("value1", result.value());
        assertEquals(42L, result.timestamp());
    }

    @Test
    public void shouldConvertWindowStoreIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedWindowIterator.next())
            .thenReturn(KeyValue.pair(21L, ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(42L, ValueAndTimestamp.make("value2", 23L)));
        when(mockedTimestampedStore.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedTimestampedWindowIterator);

        final WindowStoreIterator<String> iterator =
            facade.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertEquals(KeyValue.pair(21L, "value1"), iterator.next());
        assertEquals(KeyValue.pair(42L, "value2"), iterator.next());
    }

    @Test
    public void shouldConvertBackwardFetchWindowStoreIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedWindowIterator.next())
            .thenReturn(KeyValue.pair(42L, ValueAndTimestamp.make("value2", 23L)))
            .thenReturn(KeyValue.pair(21L, ValueAndTimestamp.make("value1", 22L)));
        when(mockedTimestampedStore.backwardFetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedTimestampedWindowIterator);

        final WindowStoreIterator<String> iterator =
            facade.backwardFetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertEquals(KeyValue.pair(42L, "value2"), iterator.next());
        assertEquals(KeyValue.pair(21L, "value1"), iterator.next());
    }

    @Test
    public void shouldConvertKeyRangeFetchIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedKeyValueIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        when(mockedTimestampedStore.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedTimestampedKeyValueIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            facade.fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertEquals(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1"), iterator.next());
        assertEquals(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2"), iterator.next());
    }

    @Test
    public void shouldConvertBackwardFetchKeyRangeIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedKeyValueIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)));
        when(mockedTimestampedStore.backwardFetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedTimestampedKeyValueIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            facade.backwardFetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertEquals(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2"), iterator.next());
        assertEquals(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1"), iterator.next());
    }

    @Test
    public void shouldConvertFetchAllIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedKeyValueIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        when(mockedTimestampedStore.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedTimestampedKeyValueIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            facade.fetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertEquals(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1"), iterator.next());
        assertEquals(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2"), iterator.next());
    }

    @Test
    public void shouldConvertBackwardFetchAllIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedKeyValueIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)));
        when(mockedTimestampedStore.backwardFetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedTimestampedKeyValueIterator);

        final KeyValueIterator<Windowed<String>, String> iterator =
            facade.backwardFetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertEquals(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2"), iterator.next());
        assertEquals(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1"), iterator.next());
    }

    @Test
    public void shouldConvertAllIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedKeyValueIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)));
        when(mockedTimestampedStore.all()).thenReturn(mockedTimestampedKeyValueIterator);

        final KeyValueIterator<Windowed<String>, String> iterator = facade.all();

        assertEquals(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1"), iterator.next());
        assertEquals(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2"), iterator.next());
    }

    @Test
    public void shouldConvertBackwardAllIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedKeyValueIterator.next())
            .thenReturn(KeyValue.pair(
                new Windowed<>("key2", new TimeWindow(42L, 43L)),
                ValueAndTimestamp.make("value2", 100L)))
            .thenReturn(KeyValue.pair(
                new Windowed<>("key1", new TimeWindow(21L, 22L)),
                ValueAndTimestamp.make("value1", 22L)));
        when(mockedTimestampedStore.backwardAll()).thenReturn(mockedTimestampedKeyValueIterator);

        final KeyValueIterator<Windowed<String>, String> iterator = facade.backwardAll();

        assertEquals(KeyValue.pair(new Windowed<>("key2", new TimeWindow(42L, 43L)), "value2"), iterator.next());
        assertEquals(KeyValue.pair(new Windowed<>("key1", new TimeWindow(21L, 22L)), "value1"), iterator.next());
    }

    @Test
    public void shouldHandleNullValuesInWindowIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyWindowStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyWindowStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedWindowIterator.next())
            .thenReturn(KeyValue.pair(21L, null))
            .thenReturn(KeyValue.pair(42L, ValueAndTimestamp.make("value2", 23L)));
        when(mockedTimestampedStore.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
            .thenReturn(mockedTimestampedWindowIterator);

        final WindowStoreIterator<String> iterator =
            facade.fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

        assertEquals(KeyValue.pair(21L, null), iterator.next());
        assertEquals(KeyValue.pair(42L, "value2"), iterator.next());
    }
}