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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.TimestampedWindowKeyWithHeadersQuery;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedWindowStoreWithHeadersBuilderTest {

    private enum StoreType { NATIVE, ADAPTER, PLAIN_ADAPTER, IN_MEMORY }

    @Nested
    class BuilderTests {
        private static final String STORE_NAME = "name";
        private static final String METRICS_SCOPE = "metricsScope";

        @Mock
        private WindowBytesStoreSupplier supplier;
        @Mock
        private RocksDBTimestampedWindowStoreWithHeaders timestampedStoreWithHeaders;

        private TimestampedWindowStoreWithHeadersBuilder<String, String> builder;

        public void setUp() {
            when(supplier.name()).thenReturn(STORE_NAME);
            when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
            when(supplier.get()).thenReturn(timestampedStoreWithHeaders);

            builder = new TimestampedWindowStoreWithHeadersBuilder<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime());
        }

        @Test
        public void shouldHaveMeteredStoreAsOuterStore() {
            setUp();
            final TimestampedWindowStoreWithHeaders<String, String> store = builder.build();
            assertInstanceOf(MeteredTimestampedWindowStoreWithHeaders.class, store);
        }

        @Test
        public void shouldHaveChangeLoggingStoreByDefault() {
            setUp();
            final TimestampedWindowStoreWithHeaders<String, String> store = builder.build();
            final StateStore next = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(ChangeLoggingTimestampedWindowBytesStoreWithHeaders.class, next);
        }

        @Test
        public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
            setUp();
            final TimestampedWindowStoreWithHeaders<String, String> store = builder.withLoggingDisabled().build();
            final StateStore next = ((WrappedStateStore) store).wrapped();
            assertSame(timestampedStoreWithHeaders, next);
        }

        @Test
        public void shouldHaveCachingStoreWhenEnabled() {
            setUp();
            final TimestampedWindowStoreWithHeaders<String, String> store = builder.withCachingEnabled().build();
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(MeteredTimestampedWindowStoreWithHeaders.class, store);
            assertInstanceOf(CachingWindowStore.class, wrapped);
        }

        @Test
        public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
            setUp();
            final TimestampedWindowStoreWithHeaders<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(MeteredTimestampedWindowStoreWithHeaders.class, store);
            assertInstanceOf(ChangeLoggingTimestampedWindowBytesStoreWithHeaders.class, wrapped);
            assertSame(timestampedStoreWithHeaders, ((WrappedStateStore) wrapped).wrapped());
        }

        @Test
        public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
            setUp();
            final TimestampedWindowStoreWithHeaders<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
            final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
            final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
            assertInstanceOf(MeteredTimestampedWindowStoreWithHeaders.class, store);
            assertInstanceOf(CachingWindowStore.class, caching);
            assertInstanceOf(ChangeLoggingTimestampedWindowBytesStoreWithHeaders.class, changeLogging);
            assertSame(timestampedStoreWithHeaders, changeLogging.wrapped());
        }

        @Test
        public void shouldNotWrapHeadersByteStore() {
            when(supplier.name()).thenReturn(STORE_NAME);
            when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
            when(supplier.get()).thenReturn(new RocksDBTimestampedWindowStoreWithHeaders(
                new RocksDBTimestampedSegmentedBytesStoreWithHeaders(
                    "name",
                    "metric-scope",
                    10L,
                    5L,
                    new WindowKeySchema()),
                false,
                1L));

            builder = new TimestampedWindowStoreWithHeadersBuilder<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime());

            final TimestampedWindowStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();
            assertInstanceOf(RocksDBTimestampedWindowStoreWithHeaders.class, ((WrappedStateStore) store).wrapped());
        }

        @Test
        public void shouldWrapTimestampedStoreAsHeadersStore() {
            when(supplier.name()).thenReturn(STORE_NAME);
            when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
            when(supplier.get()).thenReturn(new RocksDBTimestampedWindowStore(
                new RocksDBTimestampedSegmentedBytesStore(
                    "name",
                    "metric-scope",
                    10L,
                    5L,
                    new WindowKeySchema()),
                false,
                1L));

            builder = new TimestampedWindowStoreWithHeadersBuilder<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime());

            final TimestampedWindowStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();
            assertInstanceOf(TimestampedToHeadersWindowStoreAdapter.class, ((WrappedStateStore) store).wrapped());
        }

        @Test
        public void shouldDisableCachingWithRetainDuplicates() {
            when(supplier.name()).thenReturn(STORE_NAME);
            when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
            when(supplier.retainDuplicates()).thenReturn(true);
            when(supplier.get()).thenReturn(timestampedStoreWithHeaders);

            builder = new TimestampedWindowStoreWithHeadersBuilder<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime());

            final TimestampedWindowStoreWithHeaders<String, String> store = builder
                .withCachingEnabled()
                .withLoggingDisabled()
                .build();

            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            // Caching should be automatically disabled when retainDuplicates is true
            assertSame(timestampedStoreWithHeaders, wrapped);
        }

        @Test
        public void shouldThrowNullPointerIfInnerIsNull() {
            assertThrows(NullPointerException.class, () -> new TimestampedWindowStoreWithHeadersBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
        }
    }

    /**
     * End-to-end query behavior of {@link TimestampedWindowKeyWithHeadersQuery} against a real store
     * built by the builder over each supported build path. Caching is disabled: a window key query reads
     * the underlying store directly (it never consults the cache), so writes must be store-served.
     */
    @Nested
    class QueryTests {
        private static final String STORE_NAME = "test-window-store";
        private static final String METRICS_SCOPE = "metrics-scope";
        private static final long RETENTION = 60_000L;
        private static final long SEGMENT_INTERVAL = 30_000L;
        private static final long WINDOW_SIZE = 10_000L;
        private static final long WINDOW_START = 1_000L;

        @Mock
        private WindowBytesStoreSupplier supplier;

        private WindowStore<Bytes, byte[]> innerStore(final StoreType storeType) {
            switch (storeType) {
                case NATIVE:
                    return new RocksDBTimestampedWindowStoreWithHeaders(
                        new RocksDBTimestampedSegmentedBytesStoreWithHeaders(
                            STORE_NAME, METRICS_SCOPE, RETENTION, SEGMENT_INTERVAL, new WindowKeySchema()),
                        false, WINDOW_SIZE);
                case ADAPTER:
                    return new RocksDBTimestampedWindowStore(
                        new RocksDBTimestampedSegmentedBytesStore(
                            STORE_NAME, METRICS_SCOPE, RETENTION, SEGMENT_INTERVAL, new WindowKeySchema()),
                        false, WINDOW_SIZE);
                case PLAIN_ADAPTER:
                    return new RocksDBWindowStore(
                        new RocksDBSegmentedBytesStore(
                            STORE_NAME, METRICS_SCOPE, RETENTION, SEGMENT_INTERVAL, new WindowKeySchema()),
                        false, WINDOW_SIZE);
                case IN_MEMORY:
                    // Non-persistent supplier: the builder wraps it with the in-memory headers marker,
                    // which (like the native store) persists the value-with-headers byte format.
                    return new InMemoryWindowStore(STORE_NAME, RETENTION, WINDOW_SIZE, false, METRICS_SCOPE);
                default:
                    throw new IllegalArgumentException("unknown store type: " + storeType);
            }
        }

        private TimestampedWindowStoreWithHeaders<String, String> buildAndInitStore(final StoreType storeType) {
            lenient().when(supplier.name()).thenReturn(STORE_NAME);
            lenient().when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
            lenient().when(supplier.windowSize()).thenReturn(WINDOW_SIZE);
            lenient().when(supplier.get()).thenReturn(innerStore(storeType));

            final TimestampedWindowStoreWithHeaders<String, String> store =
                new TimestampedWindowStoreWithHeadersBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime())
                    .withLoggingDisabled()
                    .withCachingDisabled()
                    .build();

            final ThreadCache cache = new ThreadCache(new LogContext("test "), 0, new MockStreamsMetrics(new Metrics()));
            final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                TestUtils.tempDirectory(), Serdes.String(), Serdes.String(), null, cache);
            context.setRecordContext(new ProcessorRecordContext(0L, 0L, 0, "topic", new RecordHeaders()));
            store.init(context, store);
            return store;
        }

        private Headers headersWith(final String key, final String value) {
            return new RecordHeaders().add(key, value.getBytes(StandardCharsets.UTF_8));
        }

        private QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> windowKeyQuery(
                final TimestampedWindowStoreWithHeaders<String, String> store) {
            return store.query(
                TimestampedWindowKeyWithHeadersQuery.<String, String>withKeyAndWindowStartRange(
                    "k", Instant.ofEpochMilli(0), Instant.ofEpochMilli(RETENTION)),
                PositionBound.unbounded(),
                new QueryConfig(false));
        }

        @ParameterizedTest
        @CsvSource({"NATIVE", "IN_MEMORY"})
        public void shouldReturnHeadersForTimestampedWindowKeyWithHeadersQueryOnHeaderPersistingStore(final StoreType storeType) {
            // The native and in-memory builds both persist headers: the native store keeps them in its
            // headers column family, and the in-memory marker stores the value bytes verbatim (the metered
            // layer serializes the headers into those bytes). The adapter build is the one that drops them.
            final TimestampedWindowStoreWithHeaders<String, String> store = buildAndInitStore(storeType);
            try {
                final Headers headers = headersWith("h", "x");
                store.put("k", ValueTimestampHeaders.make("v", 1_005L, headers), WINDOW_START);

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = windowKeyQuery(store);

                assertTrue(result.isSuccess(), "Expected TimestampedWindowKeyWithHeadersQuery to succeed");
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    assertTrue(iterator.hasNext());
                    final ReadOnlyRecord<Windowed<String>, String> record = iterator.next();
                    assertEquals("k", record.key().key());
                    assertEquals(WINDOW_START, record.key().window().start());
                    assertEquals(WINDOW_START + WINDOW_SIZE, record.key().window().end());
                    assertEquals("v", record.value());
                    assertEquals(1_005L, record.timestamp());
                    assertEquals(headers, record.headers());
                    // The IQ result is a read-only snapshot: its headers are immutable (neither add nor remove).
                    assertThrows(IllegalStateException.class, () -> record.headers().add("new", new byte[0]));
                    assertThrows(IllegalStateException.class, () -> record.headers().remove("h"));
                    assertFalse(iterator.hasNext());
                }
                assertNotNull(result.getPosition(), "Expected position to be set");
            } finally {
                store.close();
            }
        }

        @Test
        public void shouldReturnEmptyHeadersForTimestampedWindowKeyWithHeadersQueryOnAdapterStore() {
            // The timestamped adapter keeps the timestamp but drops headers on write, so the record comes
            // back with empty (never null) headers while value and timestamp still round-trip.
            final TimestampedWindowStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.ADAPTER);
            try {
                store.put("k", ValueTimestampHeaders.make("v", 1_005L, headersWith("h", "x")), WINDOW_START);

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = windowKeyQuery(store);

                assertTrue(result.isSuccess());
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    assertTrue(iterator.hasNext());
                    final ReadOnlyRecord<Windowed<String>, String> record = iterator.next();
                    assertEquals("k", record.key().key());
                    assertEquals(WINDOW_START, record.key().window().start());
                    assertEquals("v", record.value());
                    assertEquals(1_005L, record.timestamp());
                    assertEquals(new RecordHeaders(), record.headers());
                    // Even the empty headers are a read-only snapshot: neither add nor remove is allowed.
                    assertThrows(IllegalStateException.class, () -> record.headers().add("new", new byte[0]));
                    assertThrows(IllegalStateException.class, () -> record.headers().remove("h"));
                    assertFalse(iterator.hasNext());
                }
                assertNotNull(result.getPosition(), "Expected position to be set");
            } finally {
                store.close();
            }
        }

        @Test
        public void shouldThrowForTimestampedWindowKeyWithHeadersQueryOnPlainSupplier() {
            // A plain (non-timestamped) window supplier surfaces every entry with timestamp = -1, which
            // cannot be represented as a ReadOnlyRecord. The query succeeds, but iterating throws.
            final TimestampedWindowStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.PLAIN_ADAPTER);
            try {
                store.put("k", ValueTimestampHeaders.make("v", 1_005L, headersWith("h", "x")), WINDOW_START);

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = windowKeyQuery(store);

                assertTrue(result.isSuccess(), "The query itself succeeds; the failure surfaces while iterating");
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    final StreamsException e = assertThrows(StreamsException.class, iterator::next,
                        "An entry with ts=-1 cannot be represented as a ReadOnlyRecord");
                    assertTrue(e.getMessage().contains("as a ReadOnlyRecord") && e.getMessage().contains("is negative"),
                        "unexpected message: " + e.getMessage());
                }
            } finally {
                store.close();
            }
        }

        @Test
        public void shouldThrowForNegativeStoredTimestampForTimestampedWindowKeyWithHeadersQuery() {
            // A caller can store a negative timestamp directly on a native (header-persisting) store.
            // The query succeeds, but the lazily-evaluated iterator throws while advancing (a ReadOnlyRecord
            // timestamp must be non-negative).
            final TimestampedWindowStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.NATIVE);
            try {
                store.put("k", ValueTimestampHeaders.make("v", -1L, headersWith("h", "x")), WINDOW_START);

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = windowKeyQuery(store);

                assertTrue(result.isSuccess());
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    final StreamsException e = assertThrows(StreamsException.class, iterator::next);
                    assertTrue(e.getMessage().contains("as a ReadOnlyRecord") && e.getMessage().contains("is negative"),
                        "unexpected message: " + e.getMessage());
                }
            } finally {
                store.close();
            }
        }

        @Test
        public void shouldCollectExecutionInfoForTimestampedWindowKeyWithHeadersQueryWhenRequested() {
            // With execution info enabled, the result must carry both the wrapped (native) store's entry
            // and the metered handler's entry.
            final TimestampedWindowStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.NATIVE);
            try {
                store.put("k", ValueTimestampHeaders.make("v", 1_005L, headersWith("h", "x")), WINDOW_START);

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
                    TimestampedWindowKeyWithHeadersQuery.<String, String>withKeyAndWindowStartRange(
                        "k", Instant.ofEpochMilli(0), Instant.ofEpochMilli(RETENTION)),
                    PositionBound.unbounded(),
                    new QueryConfig(true));

                assertTrue(result.isSuccess());
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    final String info = String.join("\n", result.getExecutionInfo());
                    assertTrue(
                        info.contains(RocksDBTimestampedWindowStoreWithHeaders.class.getName())
                            && info.contains(MeteredTimestampedWindowStoreWithHeaders.class.getName()),
                        "execution info missing an entry: " + info);
                }
            } finally {
                store.close();
            }
        }

        @Test
        public void shouldNotCollectExecutionInfoForTimestampedWindowKeyWithHeadersQueryWhenNotRequested() {
            final TimestampedWindowStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.NATIVE);
            try {
                store.put("k", ValueTimestampHeaders.make("v", 1_005L, headersWith("h", "x")), WINDOW_START);

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = windowKeyQuery(store);

                assertTrue(result.isSuccess());
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    assertTrue(result.getExecutionInfo().isEmpty(), "Expected no execution info: " + result.getExecutionInfo());
                }
            } finally {
                store.close();
            }
        }

        @Test
        public void shouldReturnIdenticalResultsForNativeAndAdapterBuiltStores() {
            // Build-path parity for the existing (header-stripped) WindowKeyQuery: KIP-1356 makes the native
            // store serve it exactly as the adapter build already did.
            final ValueTimestampHeaders<String> v1 = ValueTimestampHeaders.make("v1", 1_005L, headersWith("h", "1"));
            final ValueTimestampHeaders<String> v2 = ValueTimestampHeaders.make("v2", 3_005L, headersWith("h", "2"));
            final ValueTimestampHeaders<String> v3 = ValueTimestampHeaders.make("v3", 5_005L, headersWith("h", "3"));
            final ValueTimestampHeaders<String> o1 = ValueTimestampHeaders.make("o1", 2_005L, headersWith("h", "o"));
            final ValueTimestampHeaders<String> o2 = ValueTimestampHeaders.make("o2", 4_005L, headersWith("h", "o"));

            final TimestampedWindowStoreWithHeaders<String, String> nativeStore = buildAndInitStore(StoreType.NATIVE);
            final TimestampedWindowStoreWithHeaders<String, String> adapterStore = buildAndInitStore(StoreType.ADAPTER);
            try {
                // ascending window-start insertion order, interleaved with a different key at shared windows
                nativeStore.put("other", o1, WINDOW_START);
                nativeStore.put("k", v1, WINDOW_START);
                nativeStore.put("k", v2, WINDOW_START + 2_000L);
                nativeStore.put("other", o2, WINDOW_START + 2_000L);
                nativeStore.put("k", v3, WINDOW_START + 4_000L);
                // descending window-start insertion order (same windows, reversed), plus the other key
                adapterStore.put("k", v3, WINDOW_START + 4_000L);
                adapterStore.put("other", o2, WINDOW_START + 2_000L);
                adapterStore.put("k", v2, WINDOW_START + 2_000L);
                adapterStore.put("k", v1, WINDOW_START);
                adapterStore.put("other", o1, WINDOW_START);

                final List<KeyValue<Long, ValueAndTimestamp<String>>> expected = List.of(
                    KeyValue.pair(WINDOW_START, ValueAndTimestamp.make("v1", 1_005L)),
                    KeyValue.pair(WINDOW_START + 2_000L, ValueAndTimestamp.make("v2", 3_005L)),
                    KeyValue.pair(WINDOW_START + 4_000L, ValueAndTimestamp.make("v3", 5_005L)));

                assertEquals(expected, plainWindowKeyResults(nativeStore),
                    "native WindowKeyQuery should return all windows in window-start order");
                assertEquals(expected, plainWindowKeyResults(adapterStore),
                    "adapter WindowKeyQuery should return all windows in window-start order");
            } finally {
                nativeStore.close();
                adapterStore.close();
            }
        }

        // Drains the (header-stripped) plain WindowKeyQuery, which yields the window-start timestamp keyed
        // to a ValueAndTimestamp -- used to compare native and adapter build paths.
        private List<KeyValue<Long, ValueAndTimestamp<String>>> plainWindowKeyResults(
                final TimestampedWindowStoreWithHeaders<String, String> store) {
            final WindowKeyQuery<String, ValueAndTimestamp<String>> query =
                WindowKeyQuery.withKeyAndWindowStartRange("k", Instant.ofEpochMilli(0), Instant.ofEpochMilli(RETENTION));
            final List<KeyValue<Long, ValueAndTimestamp<String>>> out = new ArrayList<>();
            try (WindowStoreIterator<ValueAndTimestamp<String>> iterator =
                     store.query(query, PositionBound.unbounded(), new QueryConfig(false)).getResult()) {
                while (iterator.hasNext()) {
                    out.add(iterator.next());
                }
            }
            return out;
        }
    }
}
