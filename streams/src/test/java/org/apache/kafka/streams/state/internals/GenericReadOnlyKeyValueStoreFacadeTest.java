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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class GenericReadOnlyKeyValueStoreFacadeTest {

    @Mock
    private ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> mockedTimestampedStore;
    @Mock
    private ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> mockedHeadersStore;
    @Mock
    private KeyValueIterator<String, ValueAndTimestamp<String>> mockedTimestampedIterator;
    @Mock
    private KeyValueIterator<String, ValueTimestampHeaders<String>> mockedHeadersIterator;

    @Test
    public void shouldConvertValueWithExtractValueConverter() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedStore.get("key"))
            .thenReturn(ValueAndTimestamp.make("value", 42L));
        when(mockedTimestampedStore.get("unknownKey"))
            .thenReturn(null);

        assertEquals("value", facade.get("key"));
        assertNull(facade.get("unknownKey"));
    }

    @Test
    public void shouldConvertValueWithHeadersConverter() {
        final Function<ValueTimestampHeaders<String>, String> converter = ValueConverters.extractValueFromHeaders();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueTimestampHeaders<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedHeadersStore, converter);

        when(mockedHeadersStore.get("key"))
            .thenReturn(ValueTimestampHeaders.make("value", 42L, new RecordHeaders()));
        when(mockedHeadersStore.get("unknownKey"))
            .thenReturn(null);

        assertEquals("value", facade.get("key"));
        assertNull(facade.get("unknownKey"));
    }

    @Test
    public void shouldConvertValueToValueAndTimestamp() {
        final Function<ValueTimestampHeaders<String>, ValueAndTimestamp<String>> converter =
            ValueConverters.extractValueAndTimestampFromHeaders();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueTimestampHeaders<String>, ValueAndTimestamp<String>> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedHeadersStore, converter);

        when(mockedHeadersStore.get("key"))
            .thenReturn(ValueTimestampHeaders.make("value", 42L, new RecordHeaders()));
        when(mockedHeadersStore.get("unknownKey"))
            .thenReturn(null);

        final ValueAndTimestamp<String> result = facade.get("key");
        assertEquals("value", result.value());
        assertEquals(42L, result.timestamp());
        assertNull(facade.get("unknownKey"));
    }

    @Test
    public void shouldConvertIteratorValues() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedIterator.next())
            .thenReturn(KeyValue.pair("key1", ValueAndTimestamp.make("value1", 21L)))
            .thenReturn(KeyValue.pair("key2", ValueAndTimestamp.make("value2", 42L)));
        when(mockedTimestampedStore.range("key1", "key2")).thenReturn(mockedTimestampedIterator);

        final KeyValueIterator<String, String> iterator = facade.range("key1", "key2");
        assertEquals(KeyValue.pair("key1", "value1"), iterator.next());
        assertEquals(KeyValue.pair("key2", "value2"), iterator.next());
    }

    @Test
    public void shouldConvertReverseRangeIteratorValues() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedIterator.next())
            .thenReturn(KeyValue.pair("key2", ValueAndTimestamp.make("value2", 42L)))
            .thenReturn(KeyValue.pair("key1", ValueAndTimestamp.make("value1", 21L)));
        when(mockedTimestampedStore.reverseRange("key1", "key2")).thenReturn(mockedTimestampedIterator);

        final KeyValueIterator<String, String> iterator = facade.reverseRange("key1", "key2");
        assertEquals(KeyValue.pair("key2", "value2"), iterator.next());
        assertEquals(KeyValue.pair("key1", "value1"), iterator.next());
    }

    @Test
    public void shouldConvertPrefixScanIteratorValues() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedTimestampedStore, converter);

        final StringSerializer stringSerializer = new StringSerializer();
        when(mockedTimestampedIterator.next())
            .thenReturn(KeyValue.pair("key1", ValueAndTimestamp.make("value1", 21L)))
            .thenReturn(KeyValue.pair("key2", ValueAndTimestamp.make("value2", 42L)));
        when(mockedTimestampedStore.prefixScan("key", stringSerializer)).thenReturn(mockedTimestampedIterator);

        final KeyValueIterator<String, String> iterator = facade.prefixScan("key", stringSerializer);
        assertEquals(KeyValue.pair("key1", "value1"), iterator.next());
        assertEquals(KeyValue.pair("key2", "value2"), iterator.next());
    }

    @Test
    public void shouldConvertAllIteratorValues() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedIterator.next())
            .thenReturn(KeyValue.pair("key1", ValueAndTimestamp.make("value1", 21L)))
            .thenReturn(KeyValue.pair("key2", ValueAndTimestamp.make("value2", 42L)));
        when(mockedTimestampedStore.all()).thenReturn(mockedTimestampedIterator);

        final KeyValueIterator<String, String> iterator = facade.all();
        assertEquals(KeyValue.pair("key1", "value1"), iterator.next());
        assertEquals(KeyValue.pair("key2", "value2"), iterator.next());
    }

    @Test
    public void shouldConvertReverseAllIteratorValues() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedIterator.next())
            .thenReturn(KeyValue.pair("key2", ValueAndTimestamp.make("value2", 42L)))
            .thenReturn(KeyValue.pair("key1", ValueAndTimestamp.make("value1", 21L)));
        when(mockedTimestampedStore.reverseAll()).thenReturn(mockedTimestampedIterator);

        final KeyValueIterator<String, String> iterator = facade.reverseAll();
        assertEquals(KeyValue.pair("key2", "value2"), iterator.next());
        assertEquals(KeyValue.pair("key1", "value1"), iterator.next());
    }

    @Test
    public void shouldForwardApproximateNumEntries() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedStore.approximateNumEntries()).thenReturn(42L);

        assertEquals(42L, facade.approximateNumEntries());
    }

    @Test
    public void shouldHandleNullValuesInIterator() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedIterator.next())
            .thenReturn(KeyValue.pair("key1", null))
            .thenReturn(KeyValue.pair("key2", ValueAndTimestamp.make("value2", 42L)));
        when(mockedTimestampedStore.all()).thenReturn(mockedTimestampedIterator);

        final KeyValueIterator<String, String> iterator = facade.all();
        assertEquals(KeyValue.pair("key1", null), iterator.next());
        assertEquals(KeyValue.pair("key2", "value2"), iterator.next());
    }
}