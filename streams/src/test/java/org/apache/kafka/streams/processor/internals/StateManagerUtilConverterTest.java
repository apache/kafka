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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.InMemorySessionStore;
import org.apache.kafka.streams.state.internals.InMemoryWindowStore;
import org.apache.kafka.streams.state.internals.KeyValueToTimestampedKeyValueByteStoreAdapter;
import org.apache.kafka.streams.state.internals.MeteredSessionStoreWithHeaders;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.internals.MeteredTimestampedWindowStore;
import org.apache.kafka.streams.state.internals.MeteredTimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.internals.PlainToHeadersStoreAdapter;
import org.apache.kafka.streams.state.internals.PlainToHeadersWindowStoreAdapter;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.apache.kafka.streams.state.internals.SessionToHeadersStoreAdapter;
import org.apache.kafka.streams.state.internals.TimestampedToHeadersStoreAdapter;
import org.apache.kafka.streams.state.internals.TimestampedToHeadersWindowStoreAdapter;
import org.apache.kafka.streams.state.internals.WindowToTimestampedWindowByteStoreAdapter;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.apache.kafka.streams.state.internals.RecordConverters.identity;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToHeadersValue;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToSessionHeadersValue;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToTimestampedValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StateManagerUtilConverterTest {

    @Test
    public void shouldReturnIdentityConverterForPlainToTimestampedPersistentKeyValueStore() {
        // persistent plain kv -> ts kv
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(KeyValueToTimestampedKeyValueByteStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(identity(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnIdentityConverterForPlainToTimestampedInMemoryKeyValueStore() {
        // in memory kv -> ts kv (using InMemoryTimestampedKeyValueStoreMarker)
        final StateStore mockInnerStore = mock(InMemoryKeyValueStore.class);
        final WrappedStateStore<?, ?, ?> mockMarker = mock(MeteredTimestampedKeyValueStore.class);

        doReturn(mockInnerStore).when(mockMarker).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockMarker);

        assertEquals(identity(), converter);
    }

    @Test
    public void shouldReturnIdentityConverterForPlainToHeadersPersistentKeyValueStore() {
        // persistent plain kv -> headers kv
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(PlainToHeadersStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(identity(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnIdentityConverterForPlainToHeadersInMemoryKeyValueStore() {
        // in memory kv -> headers kv (using InMemoryTimestampedKeyValueStoreWithHeadersMarker)
        final StateStore mockInnerStore = mock(InMemoryKeyValueStore.class, withSettings().extraInterfaces(HeadersBytesStore.class));
        final WrappedStateStore<?, ?, ?> mockMarker = mock(MeteredTimestampedKeyValueStoreWithHeaders.class);

        doReturn(mockInnerStore).when(mockMarker).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockMarker);

        assertEquals(rawValueToHeadersValue(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnTimestampedConverterForTimestampedToHeadersPersistentKeyValueStore() {
        // ts kv -> headers kv
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(TimestampedToHeadersStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(rawValueToTimestampedValue(), converter);
    }

    @Test
    public void shouldReturnIdentityConverterForPlainToTimestampedPersistentWindowStore() {
        // persistent plain window -> ts window
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(WindowToTimestampedWindowByteStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(identity(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnIdentityConverterForPlainToTimestampedInMemoryWindowStore() {
        // in memory window -> ts window (using InMemoryTimestampedWindowStoreMarker)
        final StateStore mockInnerStore = mock(InMemoryKeyValueStore.class);
        final WrappedStateStore<?, ?, ?> mockMarker = mock(MeteredTimestampedWindowStore.class);

        doReturn(mockInnerStore).when(mockMarker).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockMarker);

        assertEquals(identity(), converter);
    }

    @Test
    public void shouldReturnIdentityConverterForPlainToHeadersPersistentWindowStore() {
        // persistent plain window -> headers window
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(PlainToHeadersWindowStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(identity(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnIdentityConverterForPlainToHeadersInMemoryWindowStore() {
        // in memory window -> headers window (using InMemoryTimestampedWindowStoreWithHeadersMarker)
        final StateStore mockInnerStore = mock(InMemoryWindowStore.class, withSettings().extraInterfaces(HeadersBytesStore.class));
        final WrappedStateStore<?, ?, ?> mockMarker = mock(MeteredTimestampedWindowStoreWithHeaders.class);

        doReturn(mockInnerStore).when(mockMarker).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockMarker);

        assertEquals(rawValueToHeadersValue(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnTimestampedConverterForTimestampedToHeadersPersistentWindowStore() {
        // ts window -> headers window
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(TimestampedToHeadersWindowStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(rawValueToTimestampedValue(), converter);
    }

    @Test
    public void shouldReturnIdentityConverterForPlainToHeadersPersistentSessionStore() {
        // persistent plain session -> headers session
        final WrappedStateStore<?, ?, ?> mockWrapper = mock(WrappedStateStore.class);
        final StateStore mockAdapter = mock(SessionToHeadersStoreAdapter.class);

        doReturn(mockAdapter).when(mockWrapper).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockWrapper);

        assertEquals(identity(), converter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnIdentityConverterForPlainToHeadersInMemorySessionStore() {
        // in memory session -> headers session (using InMemorySessionStoreWithHeadersMarker)
        final StateStore mockInnerStore = mock(InMemorySessionStore.class, withSettings().extraInterfaces(HeadersBytesStore.class));
        final WrappedStateStore<?, ?, ?> mockMarker = mock(MeteredSessionStoreWithHeaders.class);

        doReturn(mockInnerStore).when(mockMarker).wrapped();

        final RecordConverter converter = StateManagerUtil.converterForStore(mockMarker);

        assertEquals(rawValueToSessionHeadersValue(), converter);
    }

}