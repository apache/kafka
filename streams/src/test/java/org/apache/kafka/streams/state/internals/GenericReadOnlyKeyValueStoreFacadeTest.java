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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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

        assertThat(facade.get("key"), is("value"));
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

        assertThat(facade.get("key"), is("value"));
        assertNull(facade.get("unknownKey"));
    }

    @Test
    public void shouldConvertValueToValueAndTimestamp() {
        final Function<ValueTimestampHeaders<String>, ValueAndTimestamp<String>> converter =
            ValueConverters.headersToValueAndTimestamp();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueTimestampHeaders<String>, ValueAndTimestamp<String>> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedHeadersStore, converter);

        when(mockedHeadersStore.get("key"))
            .thenReturn(ValueTimestampHeaders.make("value", 42L, new RecordHeaders()));
        when(mockedHeadersStore.get("unknownKey"))
            .thenReturn(null);

        final ValueAndTimestamp<String> result = facade.get("key");
        assertThat(result.value(), is("value"));
        assertThat(result.timestamp(), is(42L));
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
        assertThat(iterator.next(), is(KeyValue.pair("key1", "value1")));
        assertThat(iterator.next(), is(KeyValue.pair("key2", "value2")));
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
        assertThat(iterator.next(), is(KeyValue.pair("key2", "value2")));
        assertThat(iterator.next(), is(KeyValue.pair("key1", "value1")));
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
        assertThat(iterator.next(), is(KeyValue.pair("key1", "value1")));
        assertThat(iterator.next(), is(KeyValue.pair("key2", "value2")));
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
        assertThat(iterator.next(), is(KeyValue.pair("key1", "value1")));
        assertThat(iterator.next(), is(KeyValue.pair("key2", "value2")));
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
        assertThat(iterator.next(), is(KeyValue.pair("key2", "value2")));
        assertThat(iterator.next(), is(KeyValue.pair("key1", "value1")));
    }

    @Test
    public void shouldForwardApproximateNumEntries() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();
        final GenericReadOnlyKeyValueStoreFacade<String, ValueAndTimestamp<String>, String> facade =
            new GenericReadOnlyKeyValueStoreFacade<>(mockedTimestampedStore, converter);

        when(mockedTimestampedStore.approximateNumEntries()).thenReturn(42L);

        assertThat(facade.approximateNumEntries(), is(42L));
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
        assertThat(iterator.next(), is(KeyValue.pair("key1", null)));
        assertThat(iterator.next(), is(KeyValue.pair("key2", "value2")));
    }
}