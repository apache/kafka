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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.TimestampedKeyQuery;
import org.apache.kafka.streams.query.TimestampedKeyWithHeadersQuery;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedKeyValueStoreBuilderWithHeadersTest {

    @Mock
    private KeyValueBytesStoreSupplier supplier;
    @Mock
    private RocksDBTimestampedStoreWithHeaders inner;
    private TimestampedKeyValueStoreBuilderWithHeaders<String, String> builder;

    private void setUpWithoutInner() {
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn("metricScope");

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
    }

    private void setUp() {
        when(supplier.get()).thenReturn(inner);
        setUpWithoutInner();
    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder.build();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder.build();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders.class, next);
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertEquals(next, inner);
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
        assertInstanceOf(CachingKeyValueStore.class, wrapped);
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
            .withLoggingEnabled(Collections.emptyMap())
            .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
        assertInstanceOf(ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders.class, wrapped);
        assertEquals(((WrappedStateStore) wrapped).wrapped(), inner);
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
            .withLoggingEnabled(Collections.emptyMap())
            .withCachingEnabled()
            .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
        assertInstanceOf(CachingKeyValueStore.class, caching);
        assertInstanceOf(ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders.class, changeLogging);
        assertEquals(changeLogging.wrapped(), inner);
    }

    @Test
    public void shouldNotWrapTimestampedByteStore() {
        setUp();
        when(supplier.get()).thenReturn(new RocksDBTimestampedStoreWithHeaders("name", "metrics-scope"));

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertInstanceOf(RocksDBTimestampedStoreWithHeaders.class, ((WrappedStateStore) store).wrapped());
    }

    @Test
    public void shouldWrapTimestampKeyValueStoreAsHeadersStore() {
        setUp();
        when(supplier.get()).thenReturn(new RocksDBTimestampedStore("name", "metrics-scope"));

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertInstanceOf(TimestampedToHeadersStoreAdapter.class, ((WrappedStateStore) store).wrapped());
    }

    @Test
    public void shouldThrowNullPointerIfInnerIsNull() {
        setUpWithoutInner();
        assertThrows(NullPointerException.class, () ->
            new TimestampedKeyValueStoreBuilderWithHeaders<>(null, Serdes.String(), Serdes.String(), new MockTime()));
    }

    @Test
    public void shouldNotThrowNullPointerIfKeySerdeIsNull() {
        setUpWithoutInner();
        // does not throw
        new TimestampedKeyValueStoreBuilderWithHeaders<>(supplier, null, Serdes.String(), new MockTime());
    }

    @Test
    public void shouldNotThrowNullPointerIfValueSerdeIsNull() {
        setUpWithoutInner();
        // does not throw
        new TimestampedKeyValueStoreBuilderWithHeaders<>(supplier, Serdes.String(), null, new MockTime());
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        setUpWithoutInner();
        assertThrows(NullPointerException.class, () ->
            new TimestampedKeyValueStoreBuilderWithHeaders<>(supplier, Serdes.String(), Serdes.String(), null));
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        setUpWithoutInner();
        when(supplier.metricsScope()).thenReturn(null);

        final Exception e = assertThrows(NullPointerException.class,
            () -> new TimestampedKeyValueStoreBuilderWithHeaders<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertTrue(e.getMessage().contains("storeSupplier's metricsScope can't be null"));
    }

    @Test
    public void shouldWrapPlainKeyValueStoreAsHeadersStore() {
        setUp();
        when(supplier.get()).thenReturn(new RocksDBStore("name", "metrics-scope"));

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();
        assertInstanceOf(PlainToHeadersStoreAdapter.class, ((WrappedStateStore) store).wrapped());
    }

    // ----------------------------------------------------------------------------------------------
    // IQv2 query handling for the built header store.
    //
    // The header store can be built three ways; each is exercised with caching on and off through a
    // single helper that builds the real store chain (Metered -> [Caching] -> inner) with a real
    // record cache, then writes and reads real data at the typed (metered) level:
    //   NATIVE    -> RocksDBTimestampedStoreWithHeaders                       (persists headers)
    //   ADAPTER   -> RocksDBTimestampedStore via TimestampedToHeadersStoreAdapter (drops headers)
    //   IN_MEMORY -> InMemoryKeyValueStore via the in-memory headers marker
    // ----------------------------------------------------------------------------------------------

    private enum StoreType { NATIVE, ADAPTER, IN_MEMORY }

    private KeyValueStore<Bytes, byte[]> innerStore(final StoreType storeType) {
        switch (storeType) {
            case NATIVE:    return new RocksDBTimestampedStoreWithHeaders("test-store", "metrics-scope");
            case ADAPTER:   return new RocksDBTimestampedStore("test-store", "metrics-scope");
            case IN_MEMORY: return new InMemoryKeyValueStore("test-store");
            default:        throw new IllegalArgumentException("unknown store type: " + storeType);
        }
    }

    private TimestampedKeyValueStoreWithHeaders<String, String> buildAndInitStore(
            final StoreType storeType,
            final boolean cachingEnabled) {
        lenient().when(supplier.name()).thenReturn("test-store");
        lenient().when(supplier.metricsScope()).thenReturn("metricScope");
        lenient().when(supplier.get()).thenReturn(innerStore(storeType));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
            supplier, Serdes.String(), Serdes.String(), new MockTime());
        final TimestampedKeyValueStoreWithHeaders<String, String> store =
            (cachingEnabled
                ? builder.withLoggingDisabled().withCachingEnabled()
                : builder.withLoggingDisabled().withCachingDisabled())
                .build();

        final ThreadCache cache = new ThreadCache(
            new LogContext("test "),
            cachingEnabled ? 10 * 1024 * 1024 : 0,
            new MockStreamsMetrics(new Metrics()));
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(), Serdes.String(), Serdes.String(), null, cache);
        context.setRecordContext(new ProcessorRecordContext(0L, 0L, 0, "topic", new RecordHeaders()));
        store.init(context, store);
        return store;
    }

    private static Headers headersWith(final String key, final String value) {
        return new RecordHeaders().add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @CsvSource({"NATIVE, true", "NATIVE, false", "ADAPTER, true", "ADAPTER, false", "IN_MEMORY, true", "IN_MEMORY, false"})
    public void shouldHandleKeyQuery(final StoreType storeType, final boolean cachingEnabled) {
        final TimestampedKeyValueStoreWithHeaders<String, String> store = buildAndInitStore(storeType, cachingEnabled);
        try {
            store.put("k", ValueTimestampHeaders.make("v", 123L, headersWith("h", "x")));

            final QueryResult<String> result =
                store.query(KeyQuery.withKey("k"), PositionBound.unbounded(), new QueryConfig(false));

            assertTrue(result.isSuccess(), "Expected KeyQuery to succeed");
            assertEquals("v", result.getResult(), "KeyQuery returns the value only");
            assertNotNull(result.getPosition(), "Expected position to be set");
        } finally {
            store.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"NATIVE, true", "NATIVE, false", "ADAPTER, true", "ADAPTER, false", "IN_MEMORY, true", "IN_MEMORY, false"})
    public void shouldHandleTimestampedKeyQuery(final StoreType storeType, final boolean cachingEnabled) {
        final TimestampedKeyValueStoreWithHeaders<String, String> store = buildAndInitStore(storeType, cachingEnabled);
        try {
            store.put("k", ValueTimestampHeaders.make("v", 123L, headersWith("h", "x")));

            final QueryResult<ValueAndTimestamp<String>> result =
                store.query(TimestampedKeyQuery.withKey("k"), PositionBound.unbounded(), new QueryConfig(false));

            assertTrue(result.isSuccess(), "Expected TimestampedKeyQuery to succeed");
            assertEquals("v", result.getResult().value());
            assertEquals(123L, result.getResult().timestamp());
            assertNotNull(result.getPosition(), "Expected position to be set");
        } finally {
            store.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"NATIVE, true", "NATIVE, false", "IN_MEMORY, true", "IN_MEMORY, false"})
    public void shouldReturnHeadersForTimestampedKeyWithHeadersQueryOnHeaderPersistingStore(
            final StoreType storeType, final boolean cachingEnabled) {
        // The native and in-memory builds both persist headers: the native store keeps them in its
        // headers column family, and the in-memory marker stores the value bytes verbatim (the metered
        // layer serializes the headers into those bytes). The adapter build is the one that drops them.
        final TimestampedKeyValueStoreWithHeaders<String, String> store = buildAndInitStore(storeType, cachingEnabled);
        try {
            final Headers headers = headersWith("h", "x");
            store.put("k", ValueTimestampHeaders.make("v", 123L, headers));

            final QueryResult<ReadOnlyRecord<String, String>> result =
                store.query(TimestampedKeyWithHeadersQuery.withKey("k"), PositionBound.unbounded(), new QueryConfig(false));

            assertTrue(result.isSuccess(), "Expected TimestampedKeyWithHeadersQuery to succeed");
            final ReadOnlyRecord<String, String> record = result.getResult();
            assertEquals("k", record.key());
            assertEquals("v", record.value());
            assertEquals(123L, record.timestamp());
            // Headers must round-trip on both the persistent path and the cache path.
            assertEquals(headers, record.headers());
            assertNotNull(result.getPosition(), "Expected position to be set");
        } finally {
            store.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldReflectFlushTimingForTimestampedKeyWithHeadersQueryOnAdapterStore(final boolean cachingEnabled) {
        // Feeding a non-header supplier into the WithHeaders builder yields an adapter-built store
        // (TimestampedToHeadersStoreAdapter over a plain timestamped store), which cannot persist
        // headers. The metered layer still serializes headers into the value bytes, and the record
        // cache sits above the adapter, so the header result depends on who serves the read:
        //   - store-served (caching disabled, or -- when caching is enabled -- once the entry has been
        //     evicted/flushed out of the cache): the read goes through the adapter, which stripped the
        //     headers on write, so they come back empty (never null) even though they were written;
        //   - cache-served (caching enabled, entry still warm): returned with headers intact
        //     (read-your-writes), since the cache holds the full serialized bytes.
        // The cache is the only thing preserving headers on this build. The caching-disabled run reads
        // through the same adapter a post-eviction cache miss would, so it covers the store-served
        // (empty) outcome; the caching-enabled run covers the cache-served (headers) outcome.
        // value/timestamp round-trip either way.
        final TimestampedKeyValueStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.ADAPTER, cachingEnabled);
        try {
            final Headers headers = headersWith("h", "x");
            store.put("k", ValueTimestampHeaders.make("v", 123L, headers));

            final QueryResult<ReadOnlyRecord<String, String>> result =
                store.query(TimestampedKeyWithHeadersQuery.withKey("k"), PositionBound.unbounded(), new QueryConfig(false));

            assertTrue(result.isSuccess(), "Expected TimestampedKeyWithHeadersQuery to succeed on an adapter-built store");
            final ReadOnlyRecord<String, String> record = result.getResult();
            assertEquals("k", record.key());
            assertEquals("v", record.value());
            assertEquals(123L, record.timestamp());
            // Cache-served (caching enabled, warm) -> written headers; store-served (caching disabled,
            // or a post-eviction cache miss) -> empty, since the adapter stripped them on write.
            final Headers expectedHeaders = cachingEnabled ? headers : new RecordHeaders();
            assertEquals(expectedHeaders, record.headers());
            assertNotNull(result.getPosition(), "Expected position to be set");
        } finally {
            store.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFailTimestampedKeyWithHeadersQueryForNegativeStoredTimestamp(final boolean cachingEnabled) {
        // A negative stored timestamp can't arise from the normal record-driven flow, but a caller can
        // store one directly. Because the result is modeled as a Record (whose constructor rejects
        // negative timestamps), the query must surface a failed result rather than throw out of query().
        final TimestampedKeyValueStoreWithHeaders<String, String> store = buildAndInitStore(StoreType.NATIVE, cachingEnabled);
        try {
            store.put("k", ValueTimestampHeaders.make("v", -1L, headersWith("h", "x")));

            final QueryResult<ReadOnlyRecord<String, String>> result =
                store.query(TimestampedKeyWithHeadersQuery.withKey("k"), PositionBound.unbounded(), new QueryConfig(false));

            assertFalse(result.isSuccess(), "A negative stored timestamp should yield a failed result, not throw");
            assertEquals(FailureReason.STORE_EXCEPTION, result.getFailureReason());
            assertNotNull(result.getPosition(), "Expected the failure to preserve the queried partition's position");
        } finally {
            store.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"NATIVE", "ADAPTER", "IN_MEMORY"})
    public void shouldHandleRangeQuery(final StoreType storeType) {
        // Caching disabled: an IQv2 RangeQuery forwards straight to the store (it does not merge the
        // record cache), so the write is served from the persistent store.
        final TimestampedKeyValueStoreWithHeaders<String, String> store = buildAndInitStore(storeType, false);
        try {
            store.put("k", ValueTimestampHeaders.make("v", 123L, headersWith("h", "x")));

            final QueryResult<KeyValueIterator<String, String>> result =
                store.query(RangeQuery.withNoBounds(), PositionBound.unbounded(), new QueryConfig(false));

            assertTrue(result.isSuccess(), "Expected RangeQuery to succeed");
            try (KeyValueIterator<String, String> iterator = result.getResult()) {
                assertTrue(iterator.hasNext(), "Expected the stored key in the range result");
                final KeyValue<String, String> keyValue = iterator.next();
                assertEquals("k", keyValue.key);
                assertEquals("v", keyValue.value);
                assertFalse(iterator.hasNext());
            }
            assertNotNull(result.getPosition(), "Expected position to be set");
        } finally {
            store.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"NATIVE", "ADAPTER", "IN_MEMORY"})
    public void shouldReturnEmptyPositionInitially(final StoreType storeType) {
        final TimestampedKeyValueStoreWithHeaders<String, String> store = buildAndInitStore(storeType, false);
        try {
            final Position position = ((WrappedStateStore) store).wrapped().getPosition();
            assertNotNull(position, "Expected non-null position");
            assertTrue(position.isEmpty(), "Expected position to be empty initially");
        } finally {
            store.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"NATIVE", "ADAPTER"})
    public void shouldCollectExecutionInfoWhenRequested(final StoreType storeType) {
        final TimestampedKeyValueStoreWithHeaders<String, String> store = buildAndInitStore(storeType, false);
        try {
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            final QueryResult<byte[]> result = wrapped.query(
                KeyQuery.withKey(new Bytes("k".getBytes())), PositionBound.unbounded(), new QueryConfig(true));

            final String executionInfo = String.join("\n", result.getExecutionInfo());
            assertFalse(executionInfo.isEmpty(), "Expected execution info to be collected");
            assertTrue(executionInfo.contains("Handled in"), "Expected execution info to contain handling information");
            final String expectedClass = storeType == StoreType.NATIVE
                ? RocksDBTimestampedStoreWithHeaders.class.getName()
                : TimestampedToHeadersStoreAdapter.class.getName();
            assertTrue(executionInfo.contains(expectedClass), "Expected execution info to mention " + expectedClass);
        } finally {
            store.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldReturnIdenticalResultsForNativeAndAdapterBuiltStores(final boolean cachingEnabled) {
        // The existing (header-stripped) query types must return identical results on both build paths.
        final ValueTimestampHeaders<String> value = ValueTimestampHeaders.make("v", 123L, headersWith("h", "x"));

        final TimestampedKeyValueStoreWithHeaders<String, String> nativeStore = buildAndInitStore(StoreType.NATIVE, cachingEnabled);
        final TimestampedKeyValueStoreWithHeaders<String, String> adapterStore = buildAndInitStore(StoreType.ADAPTER, cachingEnabled);
        try {
            nativeStore.put("k", value);
            adapterStore.put("k", value);

            assertEquals(
                nativeStore.query(KeyQuery.withKey("k"), PositionBound.unbounded(), new QueryConfig(false)).getResult(),
                adapterStore.query(KeyQuery.withKey("k"), PositionBound.unbounded(), new QueryConfig(false)).getResult(),
                "KeyQuery results should be identical across native and adapter build paths");
            assertEquals(
                nativeStore.query(TimestampedKeyQuery.withKey("k"), PositionBound.unbounded(), new QueryConfig(false)).getResult(),
                adapterStore.query(TimestampedKeyQuery.withKey("k"), PositionBound.unbounded(), new QueryConfig(false)).getResult(),
                "TimestampedKeyQuery results should be identical across native and adapter build paths");
        } finally {
            nativeStore.close();
            adapterStore.close();
        }
    }
}
