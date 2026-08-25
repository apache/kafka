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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.InMemorySessionStore;
import org.apache.kafka.streams.state.internals.InMemoryWindowStore;
import org.apache.kafka.streams.state.internals.MeteredSessionStoreWithHeaders;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.internals.MeteredTimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.internals.PlainToHeadersStoreAdapter;
import org.apache.kafka.streams.state.internals.PlainToHeadersWindowStoreAdapter;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.apache.kafka.streams.state.internals.SessionToHeadersStoreAdapter;
import org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedToHeadersStoreAdapter;
import org.apache.kafka.streams.state.internals.TimestampedToHeadersWindowStoreAdapter;
import org.apache.kafka.streams.state.internals.TimestampedWindowStoreBuilder;
import org.apache.kafka.streams.state.internals.WindowKeySchema;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

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

    private static final long TIMESTAMP = 42L;
    private static final long WINDOW_START = 0L;

    @Test
    public void shouldReturnIdentityConverterForPlainToTimestampedPersistentKeyValueStore() {
        // persistent plain kv -> ts kv (via KeyValueToTimestampedKeyValueByteStoreAdapter):
        // restore bypasses the adapter and writes into the plain inner store directly
        final TimestampedKeyValueStore<String, String> store =
            timestampedKeyValueStore(Stores.persistentKeyValueStore("store"));

        assertEquals(identity(), StateManagerUtil.converterForStore(store));
    }

    @Test
    public void shouldReturnTimestampedConverterForPlainToTimestampedInMemoryKeyValueStore() {
        // in memory kv -> ts kv (via InMemoryTimestampedKeyValueStoreMarker):
        // the inner store holds the timestamped format natively
        final TimestampedKeyValueStore<String, String> store =
            timestampedKeyValueStore(Stores.inMemoryKeyValueStore("store"));

        assertEquals(rawValueToTimestampedValue(), StateManagerUtil.converterForStore(store));
    }

    @Test
    public void shouldReturnTimestampedConverterForPersistentTimestampedKeyValueStore() {
        final TimestampedKeyValueStore<String, String> store =
            timestampedKeyValueStore(Stores.persistentTimestampedKeyValueStore("store"));

        assertEquals(rawValueToTimestampedValue(), StateManagerUtil.converterForStore(store));
    }

    @Test
    public void shouldReturnIdentityConverterForPlainToTimestampedPersistentWindowStore() {
        // persistent plain window -> ts window (via WindowToTimestampedWindowByteStoreAdapter)
        final StateStore store = timestampedWindowStore(
            Stores.persistentWindowStore("store", Duration.ofMillis(1000), Duration.ofMillis(100), false));

        assertEquals(identity(), StateManagerUtil.converterForStore(store));
    }

    @Test
    public void shouldReturnTimestampedConverterForPlainToTimestampedInMemoryWindowStore() {
        // in memory window -> ts window (via InMemoryTimestampedWindowStoreMarker):
        // the inner store holds the timestamped format natively
        final StateStore store = timestampedWindowStore(
            Stores.inMemoryWindowStore("store", Duration.ofMillis(1000), Duration.ofMillis(100), false));

        assertEquals(rawValueToTimestampedValue(), StateManagerUtil.converterForStore(store));
    }

    @Test
    public void shouldReturnTimestampedConverterForPersistentTimestampedWindowStore() {
        final StateStore store = timestampedWindowStore(
            Stores.persistentTimestampedWindowStore("store", Duration.ofMillis(1000), Duration.ofMillis(100), false));

        assertEquals(rawValueToTimestampedValue(), StateManagerUtil.converterForStore(store));
    }

    @Test
    public void shouldRestorePlainPersistentTimestampedKeyValueStoreInPlainFormat() {
        // regression test for KAFKA-16141: restore bypasses the adapter, so restored records
        // must not get a timestamp prepended even though the adapter advertises the timestamped format
        final TimestampedKeyValueStore<String, String> store =
            timestampedKeyValueStore(Stores.persistentKeyValueStore("store"));

        final ValueAndTimestamp<String> restored = restoreAndGet(store);

        // the plain inner store cannot retain the record timestamp; reads surface the dummy `-1`
        assertEquals("value", restored.value());
        assertEquals(-1L, restored.timestamp());
    }

    @Test
    public void shouldRestoreInMemoryTimestampedKeyValueStoreInTimestampedFormat() {
        final TimestampedKeyValueStore<String, String> store =
            timestampedKeyValueStore(Stores.inMemoryKeyValueStore("store"));

        final ValueAndTimestamp<String> restored = restoreAndGet(store);

        assertEquals("value", restored.value());
        assertEquals(TIMESTAMP, restored.timestamp());
    }

    @Test
    public void shouldRestorePersistentTimestampedKeyValueStoreInTimestampedFormat() {
        final TimestampedKeyValueStore<String, String> store =
            timestampedKeyValueStore(Stores.persistentTimestampedKeyValueStore("store"));

        final ValueAndTimestamp<String> restored = restoreAndGet(store);

        assertEquals("value", restored.value());
        assertEquals(TIMESTAMP, restored.timestamp());
    }

    @Test
    public void shouldRestorePlainPersistentTimestampedWindowStoreInPlainFormat() {
        // restore bypasses WindowToTimestampedWindowByteStoreAdapter and writes plain values into the
        // inner store; reads then surface the adapter's dummy `-1` timestamp
        final TimestampedWindowStore<String, String> store = timestampedWindowStore(
            Stores.persistentWindowStore("store", Duration.ofMillis(1000), Duration.ofMillis(100), false));

        final ValueAndTimestamp<String> restored = restoreAndGet(store);

        assertEquals("value", restored.value());
        assertEquals(-1L, restored.timestamp());
    }

    @Test
    public void shouldRestoreInMemoryTimestampedWindowStoreInTimestampedFormat() {
        // the InMemoryTimestampedWindowStoreMarker's inner store holds the timestamped format
        // natively, so the record timestamp is retained through restore
        final TimestampedWindowStore<String, String> store = timestampedWindowStore(
            Stores.inMemoryWindowStore("store", Duration.ofMillis(1000), Duration.ofMillis(100), false));

        final ValueAndTimestamp<String> restored = restoreAndGet(store);

        assertEquals("value", restored.value());
        assertEquals(TIMESTAMP, restored.timestamp());
    }

    @Test
    public void shouldRestorePersistentTimestampedWindowStoreInTimestampedFormat() {
        final TimestampedWindowStore<String, String> store = timestampedWindowStore(
            Stores.persistentTimestampedWindowStore("store", Duration.ofMillis(1000), Duration.ofMillis(100), false));

        final ValueAndTimestamp<String> restored = restoreAndGet(store);

        assertEquals("value", restored.value());
        assertEquals(TIMESTAMP, restored.timestamp());
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

    private static TimestampedKeyValueStore<String, String> timestampedKeyValueStore(final KeyValueBytesStoreSupplier supplier) {
        return new TimestampedKeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), Time.SYSTEM).build();
    }

    private static TimestampedWindowStore<String, String> timestampedWindowStore(final WindowBytesStoreSupplier supplier) {
        return new TimestampedWindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), Time.SYSTEM).build();
    }

    private static ValueAndTimestamp<String> restoreAndGet(final TimestampedWindowStore<String, String> store) {
        // window changelog keys carry the window start (here WINDOW_START) in their binary encoding
        final byte[] key = WindowKeySchema.toStoreKeyBinary(
            Bytes.wrap("key".getBytes(StandardCharsets.UTF_8)), WINDOW_START, 0).get();
        return restoreAndRead(store, key, () -> store.fetch("key", WINDOW_START));
    }

    private static ValueAndTimestamp<String> restoreAndGet(final TimestampedKeyValueStore<String, String> store) {
        return restoreAndRead(store, "key".getBytes(StandardCharsets.UTF_8), () -> store.get("key"));
    }

    /**
     * Feeds a changelog-format record (plain value, timestamp in the record timestamp field) through
     * the converter and the store's registered restore callback, mirroring the restore code path,
     * then reads the restored value back via {@code read}.
     */
    private static ValueAndTimestamp<String> restoreAndRead(final StateStore store,
                                                            final byte[] key,
                                                            final Supplier<ValueAndTimestamp<String>> read) {
        final InternalMockProcessorContext<?, ?> context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(StreamsTestUtils.getStreamsConfig())
        );
        store.init(context, store);
        try {
            final byte[] plainValue = "value".getBytes(StandardCharsets.UTF_8);
            final ConsumerRecord<byte[], byte[]> changelogRecord = new ConsumerRecord<>(
                "changelog",
                0,
                0L,
                TIMESTAMP,
                TimestampType.CREATE_TIME,
                key.length,
                plainValue.length,
                key,
                plainValue,
                new RecordHeaders(),
                Optional.empty()
            );

            final RecordConverter converter = StateManagerUtil.converterForStore(store);
            context.restoreWithHeaders(store.name(), List.of(converter.convert(changelogRecord)));

            return read.get();
        } finally {
            store.close();
        }
    }

}
