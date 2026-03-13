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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.internals.PlainToHeadersStoreAdapter;
import org.apache.kafka.streams.state.internals.PlainToHeadersWindowStoreAdapter;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.apache.kafka.streams.state.internals.TimestampedToHeadersStoreAdapter;
import org.apache.kafka.streams.state.internals.TimestampedToHeadersWindowStoreAdapter;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.test.MockKeyValueStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;

import static org.apache.kafka.streams.state.internals.RecordConverters.identity;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToHeadersValue;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToTimestampedValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StateManagerUtilConverterTest {

    @Test
    public void shouldReturnHeadersConverterForHeadersAwareKeyValueStore() {
        final TimestampedKeyValueStoreWithHeaders<String, String> store =
            Stores.timestampedKeyValueStoreBuilderWithHeaders(
                Stores.inMemoryKeyValueStore("test-store"),
                Serdes.String(),
                Serdes.String())
                .build();

        final RecordConverter converter = StateManagerUtil.converterForStore(store);

        assertEquals(rawValueToHeadersValue(), converter);
    }

    @Test
    public void shouldReturnHeadersConverterForHeadersAwareWindowStore() {
        final TimestampedWindowStoreWithHeaders<String, String> store =
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.inMemoryWindowStore(
                    "test-window-store",
                    Duration.ofMillis(100),
                    Duration.ofMillis(10),
                    false),
                Serdes.String(),
                Serdes.String())
                .build();

        final RecordConverter converter = StateManagerUtil.converterForStore(store);

        assertEquals(rawValueToHeadersValue(), converter);
    }

    @Test
    public void shouldReturnTimestampedConverterForTimestampedKeyValueStore() {
        final StateStore store = Stores.timestampedKeyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("test-ts-store"),
            Serdes.String(),
            Serdes.String())
            .build();

        final RecordConverter converter = StateManagerUtil.converterForStore(store);

        assertEquals(rawValueToTimestampedValue(), converter);
    }

    @Test
    public void shouldReturnTimestampedConverterForTimestampedWindowStore() {
        final StateStore store = Stores.timestampedWindowStoreBuilder(
            Stores.inMemoryWindowStore(
                "test-ts-window-store",
                Duration.ofMillis(100),
                Duration.ofMillis(10),
                false),
            Serdes.String(),
            Serdes.String())
            .build();

        final RecordConverter converter = StateManagerUtil.converterForStore(store);

        assertEquals(rawValueToTimestampedValue(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnTimestampedConverterForTimestampedToHeadersStoreAdapter() {
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(TimestampedToHeadersStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(rawValueToTimestampedValue(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnTimestampedConverterForTimestampedToHeadersWindowStoreAdapter() {
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(TimestampedToHeadersWindowStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(rawValueToTimestampedValue(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnIdentityConverterForPlainToHeadersStoreAdapter() {
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(PlainToHeadersStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(identity(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnIdentityConverterForPlainToHeadersWindowStoreAdapter() {
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(PlainToHeadersWindowStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(identity(), converter);
    }

    @Test
    public void shouldReturnIdentityConverterForPlainKeyValueStore() {
        final StateStore store = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("test-plain-store"),
            Serdes.String(),
            Serdes.String())
            .build();

        final RecordConverter converter = StateManagerUtil.converterForStore(store);

        assertEquals(identity(), converter);
    }

    @Test
    public void shouldReturnIdentityConverterForMockKeyValueStore() {
        final StateStore store = new MockKeyValueStore("mock-store", false);

        final RecordConverter converter = StateManagerUtil.converterForStore(store);

        assertEquals(identity(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldHandleNestedWrappedStores() {
        final WrappedStateStore<?, ?, ?> outerWrapper = mock(WrappedStateStore.class);
        final WrappedStateStore<?, ?, ?> innerWrapper = mock(WrappedStateStore.class);
        final StateStore adapter = mock(TimestampedToHeadersStoreAdapter.class);

        doReturn(innerWrapper).when(outerWrapper).wrapped();
        doReturn(adapter).when(innerWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(outerWrapper);

        assertEquals(rawValueToTimestampedValue(), converter);
    }
}