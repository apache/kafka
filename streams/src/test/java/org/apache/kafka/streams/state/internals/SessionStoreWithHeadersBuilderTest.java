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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.TimestampedWindowRangeWithHeadersQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
public class SessionStoreWithHeadersBuilderTest {

    private enum StoreType { NATIVE, ADAPTER, IN_MEMORY }

    @Nested
    class BuilderTests {
        private static final String STORE_NAME = "name";
        private static final String METRICS_SCOPE = "metricsScope";

        @Mock
        private SessionBytesStoreSupplier supplier;
        @Mock
        private RocksDBSessionStoreWithHeaders sessionStoreWithHeaders;

        private SessionStoreWithHeadersBuilder<String, String> builder;

        public void setUp() {
            when(supplier.name()).thenReturn(STORE_NAME);
            when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
            when(supplier.get()).thenReturn(sessionStoreWithHeaders);

            builder = new SessionStoreWithHeadersBuilder<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime());
        }

        @Test
        public void shouldHaveMeteredStoreAsOuterStore() {
            setUp();
            final SessionStoreWithHeaders<String, String> store = builder.build();
            assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
        }

        @Test
        public void shouldHaveChangeLoggingStoreByDefault() {
            setUp();
            final SessionStoreWithHeaders<String, String> store = builder.build();
            final StateStore next = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(ChangeLoggingSessionBytesStoreWithHeaders.class, next);
        }

        @Test
        public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
            setUp();
            final SessionStoreWithHeaders<String, String> store = builder.withLoggingDisabled().build();
            final StateStore next = ((WrappedStateStore) store).wrapped();
            assertSame(sessionStoreWithHeaders, next);
        }

        @Test
        public void shouldHaveCachingStoreWhenEnabled() {
            setUp();
            final SessionStoreWithHeaders<String, String> store = builder.withCachingEnabled().build();
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
            assertInstanceOf(CachingSessionStore.class, wrapped);
        }

        @Test
        public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
            setUp();
            final SessionStoreWithHeaders<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
            assertInstanceOf(ChangeLoggingSessionBytesStoreWithHeaders.class, wrapped);
            assertSame(sessionStoreWithHeaders, ((WrappedStateStore) wrapped).wrapped());
        }

        @Test
        public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
            setUp();
            final SessionStoreWithHeaders<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
            final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
            final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
            assertInstanceOf(MeteredSessionStoreWithHeaders.class, store);
            assertInstanceOf(CachingSessionStore.class, caching);
            assertInstanceOf(ChangeLoggingSessionBytesStoreWithHeaders.class, changeLogging);
            assertSame(sessionStoreWithHeaders, changeLogging.wrapped());
        }

        @Test
        public void shouldNotWrapHeadersByteStore() {
            when(supplier.name()).thenReturn(STORE_NAME);
            when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
            when(supplier.get()).thenReturn(new RocksDBSessionStoreWithHeaders(
                new SessionRocksDBSegmentedBytesStoreWithHeaders(
                    "name",
                    "metric-scope",
                    10L,
                    5L,
                    new SessionKeySchema())));

            builder = new SessionStoreWithHeadersBuilder<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime());

            final SessionStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();
            assertInstanceOf(RocksDBSessionStoreWithHeaders.class, ((WrappedStateStore) store).wrapped());
        }

        @Test
        public void shouldWrapPlainStoreAsHeadersStore() {
            when(supplier.name()).thenReturn(STORE_NAME);
            when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
            when(supplier.get()).thenReturn(new RocksDBSessionStore(
                new RocksDBSegmentedBytesStore(
                    "name",
                    "metric-scope",
                    10L,
                    5L,
                    new SessionKeySchema())));

            builder = new SessionStoreWithHeadersBuilder<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime());

            final SessionStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();
            assertInstanceOf(SessionToHeadersStoreAdapter.class, ((WrappedStateStore) store).wrapped());
        }

        @Test
        public void shouldThrowNullPointerIfInnerIsNull() {
            assertThrows(NullPointerException.class, () -> new SessionStoreWithHeadersBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
        }
    }

    /**
     * End-to-end query behavior of the {@code withKey} form of
     * {@link TimestampedWindowRangeWithHeadersQuery} against a real store built by the builder over
     * each supported build path. Caching is disabled: the query reads the underlying store directly
     * (it never consults the cache), so writes must be store-served.
     */
    @Nested
    class QueryTests {
        private static final String STORE_NAME = "test-session-store";
        private static final String METRICS_SCOPE = "metrics-scope";
        private static final long RETENTION = 60_000L;
        private static final long SEGMENT_INTERVAL = 30_000L;
        private static final long SESSION_START = 1_000L;
        private static final long SESSION_END = 5_000L;

        @Mock
        private SessionBytesStoreSupplier supplier;

        private SessionStore<Bytes, byte[]> innerStore(final StoreType storeType) {
            switch (storeType) {
                case NATIVE:
                    return new RocksDBSessionStoreWithHeaders(
                        new SessionRocksDBSegmentedBytesStoreWithHeaders(
                            STORE_NAME, METRICS_SCOPE, RETENTION, SEGMENT_INTERVAL, new SessionKeySchema()));
                case ADAPTER:
                    return new RocksDBSessionStore(
                        new RocksDBSegmentedBytesStore(
                            STORE_NAME, METRICS_SCOPE, RETENTION, SEGMENT_INTERVAL, new SessionKeySchema()));
                case IN_MEMORY:
                    // Non-persistent supplier: the builder wraps it with the in-memory headers marker,
                    // which (like the native store) persists the value-with-headers byte format.
                    return new InMemorySessionStore(STORE_NAME, RETENTION, METRICS_SCOPE);
                default:
                    throw new IllegalArgumentException("unknown store type: " + storeType);
            }
        }

        private SessionStoreWithHeaders<String, String> buildAndInitStore(final StoreType storeType) {
            lenient().when(supplier.name()).thenReturn(STORE_NAME);
            lenient().when(supplier.metricsScope()).thenReturn(METRICS_SCOPE);
            lenient().when(supplier.segmentIntervalMs()).thenReturn(SEGMENT_INTERVAL);
            lenient().when(supplier.retentionPeriod()).thenReturn(RETENTION);
            lenient().when(supplier.get()).thenReturn(innerStore(storeType));

            final SessionStoreWithHeaders<String, String> store =
                new SessionStoreWithHeadersBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime())
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

        private QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> withKeyQuery(
                final SessionStoreWithHeaders<String, String> store) {
            return store.query(
                TimestampedWindowRangeWithHeadersQuery.<String, String>withKey("k"),
                PositionBound.unbounded(),
                new QueryConfig(false));
        }

        @ParameterizedTest
        @CsvSource({"NATIVE", "IN_MEMORY"})
        public void shouldReturnHeadersForTimestampedWindowRangeWithHeadersQueryOnHeaderPersistingStore(final StoreType storeType) {
            // The native and in-memory builds both persist headers: the native store keeps them in its
            // headers column family, and the in-memory marker stores the value bytes verbatim (the metered
            // layer serializes the headers into those bytes). The adapter build is the one that drops them.
            final SessionStoreWithHeaders<String, String> store = buildAndInitStore(storeType);
            try {
                final Headers headers = headersWith("h", "x");
                store.put(new Windowed<>("k", new SessionWindow(SESSION_START, SESSION_END)), AggregationWithHeaders.make("v", headers));

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = withKeyQuery(store);

                assertTrue(result.isSuccess(), "Expected TimestampedWindowRangeWithHeadersQuery to succeed");
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    assertTrue(iterator.hasNext());
                    final ReadOnlyRecord<Windowed<String>, String> record = iterator.next();
                    assertEquals("k", record.key().key());
                    assertEquals(SESSION_START, record.key().window().start());
                    assertEquals(SESSION_END, record.key().window().end());
                    assertEquals("v", record.value());
                    // Timestamp is sourced from the session window's end (AggregationWithHeaders carries
                    // no timestamp of its own).
                    assertEquals(SESSION_END, record.timestamp());
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
        public void shouldReturnEmptyHeadersForTimestampedWindowRangeWithHeadersQueryOnAdapterStore() {
            // The adapter drops headers on write, so the record comes back with empty (never null)
            // headers while value and timestamp (session window end) still round-trip.
            final SessionStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.ADAPTER);
            try {
                store.put(new Windowed<>("k", new SessionWindow(SESSION_START, SESSION_END)), AggregationWithHeaders.make("v", headersWith("h", "x")));

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = withKeyQuery(store);

                assertTrue(result.isSuccess());
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    assertTrue(iterator.hasNext());
                    final ReadOnlyRecord<Windowed<String>, String> record = iterator.next();
                    assertEquals("k", record.key().key());
                    assertEquals(SESSION_START, record.key().window().start());
                    assertEquals(SESSION_END, record.key().window().end());
                    assertEquals("v", record.value());
                    assertEquals(SESSION_END, record.timestamp());
                    assertEquals(new RecordHeaders(), record.headers());
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
        public void shouldNotThrowForPlainSessionStoreSinceTimestampIsSessionWindowEnd() {
            // Sessions have a single adapter path (SessionToHeadersStoreAdapter over a plain session store);
            // there is no timestamped-vs-plain split like window stores have. Where a plain WINDOW store
            // surfaces a stored ts=-1 and makes next() throw, a plain SESSION store never persists a
            // per-record timestamp: the ReadOnlyRecord timestamp is the (always non-negative) session-window
            // end, so iteration always succeeds.
            final SessionStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.ADAPTER);
            try {
                store.put(new Windowed<>("k", new SessionWindow(SESSION_START, SESSION_END)),
                    AggregationWithHeaders.make("v", headersWith("h", "x")));

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = withKeyQuery(store);

                assertTrue(result.isSuccess());
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    final ReadOnlyRecord<Windowed<String>, String> record = assertDoesNotThrow(iterator::next);
                    assertEquals(SESSION_END, record.timestamp());
                    assertFalse(iterator.hasNext());
                }
            } finally {
                store.close();
            }
        }

        @Test
        public void shouldRejectWithWindowStartRangeFormAgainstSessionStore() {
            final SessionStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.NATIVE);
            try {
                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
                    TimestampedWindowRangeWithHeadersQuery.<String, String>withWindowStartRange(
                        Instant.ofEpochMilli(0), Instant.ofEpochMilli(RETENTION)),
                    PositionBound.unbounded(),
                    new QueryConfig(false));

                assertFalse(result.isSuccess());
                assertTrue(
                    result.getFailureMessage().contains("SessionStores only support TimestampedWindowRangeWithHeadersQuery.withKey"),
                    "unexpected message: " + result.getFailureMessage());
            } finally {
                store.close();
            }
        }

        @Test
        public void shouldCollectExecutionInfoForTimestampedWindowRangeWithHeadersQueryWhenRequested() {
            // With execution info enabled, the result must carry both the wrapped (native) store's entry
            // and the metered handler's entry.
            final SessionStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.NATIVE);
            try {
                store.put(new Windowed<>("k", new SessionWindow(SESSION_START, SESSION_END)), AggregationWithHeaders.make("v", headersWith("h", "x")));

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = store.query(
                    TimestampedWindowRangeWithHeadersQuery.<String, String>withKey("k"),
                    PositionBound.unbounded(),
                    new QueryConfig(true));

                assertTrue(result.isSuccess());
                try (ReadOnlyRecordIterator<Windowed<String>, String> iterator = result.getResult()) {
                    final String info = String.join("\n", result.getExecutionInfo());
                    assertTrue(
                        info.contains(RocksDBSessionStoreWithHeaders.class.getName())
                            && info.contains(MeteredSessionStoreWithHeaders.class.getName()),
                        "execution info missing an entry: " + info);
                }
            } finally {
                store.close();
            }
        }

        @Test
        public void shouldNotCollectExecutionInfoForTimestampedWindowRangeWithHeadersQueryWhenNotRequested() {
            final SessionStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.NATIVE);
            try {
                store.put(new Windowed<>("k", new SessionWindow(SESSION_START, SESSION_END)), AggregationWithHeaders.make("v", headersWith("h", "x")));

                final QueryResult<ReadOnlyRecordIterator<Windowed<String>, String>> result = withKeyQuery(store);

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
            // Build-path parity for the existing (header-stripped) WindowRangeQuery.withKey: KIP-1356 makes
            // the native store serve it exactly as the adapter build already did. withKey filters by key, so
            // an interleaved "other" (noise) key must be excluded, and both builds must return only key k's
            // sessions in session-start order.
            // Session windows are variable-length (end is not start + a fixed size), so each session below
            // has a different duration -- this verifies the session end round-trips from the raw result
            // rather than being derived from any fixed window size.
            final SessionStoreWithHeaders<String, String> nativeStore = buildAndInitStore(StoreType.NATIVE);
            final SessionStoreWithHeaders<String, String> adapterStore = buildAndInitStore(StoreType.ADAPTER);
            try {
                // ascending session-start insertion into native, interleaved with a noise key
                nativeStore.put(new Windowed<>("other", new SessionWindow(1_000L, 2_000L)), AggregationWithHeaders.make("o1", headersWith("h", "o")));
                nativeStore.put(new Windowed<>("k", new SessionWindow(1_000L, 1_500L)), AggregationWithHeaders.make("v1", headersWith("h", "1")));
                nativeStore.put(new Windowed<>("k", new SessionWindow(3_000L, 8_000L)), AggregationWithHeaders.make("v2", headersWith("h", "2")));
                nativeStore.put(new Windowed<>("other", new SessionWindow(6_000L, 9_000L)), AggregationWithHeaders.make("o2", headersWith("h", "o")));
                nativeStore.put(new Windowed<>("k", new SessionWindow(10_000L, 12_500L)), AggregationWithHeaders.make("v3", headersWith("h", "3")));
                // descending session-start insertion into adapter (same sessions reversed), plus the noise key
                adapterStore.put(new Windowed<>("k", new SessionWindow(10_000L, 12_500L)), AggregationWithHeaders.make("v3", headersWith("h", "3")));
                adapterStore.put(new Windowed<>("other", new SessionWindow(6_000L, 9_000L)), AggregationWithHeaders.make("o2", headersWith("h", "o")));
                adapterStore.put(new Windowed<>("k", new SessionWindow(3_000L, 8_000L)), AggregationWithHeaders.make("v2", headersWith("h", "2")));
                adapterStore.put(new Windowed<>("k", new SessionWindow(1_000L, 1_500L)), AggregationWithHeaders.make("v1", headersWith("h", "1")));
                adapterStore.put(new Windowed<>("other", new SessionWindow(1_000L, 2_000L)), AggregationWithHeaders.make("o1", headersWith("h", "o")));

                // Each "k" session has a distinct length (500, 5000, 2500 ms); "other" is filtered out.
                final List<KeyValue<Windowed<String>, String>> expected = List.of(
                    KeyValue.pair(new Windowed<>("k", new SessionWindow(1_000L, 1_500L)), "v1"),
                    KeyValue.pair(new Windowed<>("k", new SessionWindow(3_000L, 8_000L)), "v2"),
                    KeyValue.pair(new Windowed<>("k", new SessionWindow(10_000L, 12_500L)), "v3"));

                assertEquals(expected, plainWindowRangeResults(nativeStore),
                    "native WindowRangeQuery.withKey should return only key k's sessions in session-start order");
                assertEquals(expected, plainWindowRangeResults(adapterStore),
                    "adapter WindowRangeQuery.withKey should return only key k's sessions in session-start order");
            } finally {
                nativeStore.close();
                adapterStore.close();
            }
        }

        // Drains the (header-stripped) plain WindowRangeQuery.withKey, keeping the full windowed key and
        // value -- used to compare native and adapter build paths.
        private List<KeyValue<Windowed<String>, String>> plainWindowRangeResults(final SessionStoreWithHeaders<String, String> store) {
            final WindowRangeQuery<String, String> query = WindowRangeQuery.withKey("k");
            final List<KeyValue<Windowed<String>, String>> out = new ArrayList<>();
            try (KeyValueIterator<Windowed<String>, String> iterator =
                     store.query(query, PositionBound.unbounded(), new QueryConfig(false)).getResult()) {
                while (iterator.hasNext()) {
                    out.add(iterator.next());
                }
            }
            return out;
        }
    }
}
