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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedWindowStoreWithHeadersBuilderTest {
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

    // IQv2 Tests for native headers store (RocksDBTimestampedWindowStoreWithHeaders)

    private static final long WINDOW_SIZE = 10_000L;
    private static final long RETENTION_PERIOD = 60_000L;
    private static final long SEGMENT_INTERVAL = 30_000L;

    private RocksDBTimestampedWindowStoreWithHeaders nativeHeadersStore;
    private InternalMockProcessorContext<String, String> context;
    private File baseDir;

    @BeforeEach
    public void setUpIQv2Tests() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        baseDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                baseDir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );

        final SegmentedBytesStore segmentedBytesStore = new RocksDBTimestampedSegmentedBytesStoreWithHeaders(
                "iqv2-native-headers-test-store",
                "test-metrics-scope",
                RETENTION_PERIOD,
                SEGMENT_INTERVAL,
                new WindowKeySchema()
        );

        nativeHeadersStore = new RocksDBTimestampedWindowStoreWithHeaders(
                segmentedBytesStore,
                false,  // retainDuplicates
                WINDOW_SIZE
        );

        nativeHeadersStore.init(context, nativeHeadersStore);
    }

    @AfterEach
    public void tearDownIQv2Tests() {
        if (nativeHeadersStore != null) {
            nativeHeadersStore.close();
        }
    }

    @Test
    public void shouldReturnUnknownQueryTypeForWindowKeyQueryOnNativeHeadersStore() {
        final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);

        final QueryResult<WindowStoreIterator<byte[]>> result = nativeHeadersStore.query(query, positionBound, config);

        // Verify: Native headers store returns UNKNOWN_QUERY_TYPE for IQv2
        assertFalse(result.isSuccess(), "Expected query to fail with unknown query type");
        assertEquals(
                FailureReason.UNKNOWN_QUERY_TYPE,
                result.getFailureReason(),
                "Expected UNKNOWN_QUERY_TYPE failure reason"
        );
        assertNotNull(result.getPosition(), "Expected position to be set");
    }

    @Test
    public void shouldReturnUnknownQueryTypeForWindowRangeQueryOnNativeHeadersStore() {
        final WindowRangeQuery<Bytes, byte[]> query = WindowRangeQuery.withWindowStartRange(
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);

        final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> result =
                nativeHeadersStore.query(query, positionBound, config);

        // Verify: Native headers store returns UNKNOWN_QUERY_TYPE for IQv2
        assertFalse(result.isSuccess(), "Expected query to fail with unknown query type");
        assertEquals(
                FailureReason.UNKNOWN_QUERY_TYPE,
                result.getFailureReason(),
                "Expected UNKNOWN_QUERY_TYPE failure reason"
        );
        assertNotNull(result.getPosition(), "Expected position to be set");
    }

    @Test
    public void shouldCollectExecutionInfoForNativeHeadersStoreWhenRequested() {
        final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(true); // Enable execution info

        final QueryResult<WindowStoreIterator<byte[]>> result = nativeHeadersStore.query(query, positionBound, config);

        // Verify: Execution info was collected
        assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
        boolean foundHeadersStoreInfo = false;
        for (final String info : result.getExecutionInfo()) {
            if (info.contains("Handled in") && info.contains(RocksDBTimestampedWindowStoreWithHeaders.class.getName())) {
                foundHeadersStoreInfo = true;
                break;
            }
        }
        assertTrue(foundHeadersStoreInfo, "Expected execution info to mention the native headers store class");
    }

    @Test
    public void shouldNotCollectExecutionInfoForNativeHeadersStoreWhenNotRequested() {
        final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false); // Disable execution info

        final QueryResult<WindowStoreIterator<byte[]>> result = nativeHeadersStore.query(query, positionBound, config);

        // Verify: No execution info was collected
        assertTrue(result.getExecutionInfo().isEmpty(), "Expected no execution info to be collected");
    }
}
