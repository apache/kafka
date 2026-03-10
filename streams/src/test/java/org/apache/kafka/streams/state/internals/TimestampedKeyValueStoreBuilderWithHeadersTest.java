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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.TimestampedKeyQuery;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    @Test
    public void shouldHandleKeyQueryOnInMemoryStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("in-memory");
        when(supplier.get()).thenReturn(new InMemoryKeyValueStore("test-store"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            // Put data into the store
            final Headers headers = new RecordHeaders();
            headers.add("key1", "value1".getBytes());
            store.put("test-key", ValueTimestampHeaders.make("test-value", 12345L, headers));

            // Verify wrapper type for InMemoryKeyValueStore
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(HeadersBytesStore.class, wrapped,
                    "Expected wrapper to implement HeadersBytesStore for InMemoryKeyValueStore");

            // Query at typed level - KeyQuery should return just the value
            final KeyQuery<String, String> query = KeyQuery.withKey("test-key");
            final QueryResult<String> result = store.query(query, PositionBound.unbounded(), new QueryConfig(false));

            // Verify IQv2 query result
            assertTrue(result.isSuccess(), "Expected query to succeed on InMemoryKeyValueStore");
            assertNotNull(result.getPosition(), "Expected position to be set");
            assertInstanceOf(String.class, result.getResult());
            assertEquals("test-value", result.getResult(), "KeyQuery should return just the value");
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldHandleTimestampedKeyQueryOnInMemoryStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("in-memory");
        when(supplier.get()).thenReturn(new InMemoryKeyValueStore("test-store"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            // Put data into the store
            final Headers headers = new RecordHeaders();
            headers.add("key1", "value1".getBytes());
            store.put("test-key", ValueTimestampHeaders.make("test-value", 12345L, headers));

            // Verify wrapper type for InMemoryKeyValueStore
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(HeadersBytesStore.class, wrapped,
                    "Expected wrapper to implement HeadersBytesStore for InMemoryKeyValueStore");

            // Query at typed level - TimestampedKeyQuery should return value + timestamp
            final TimestampedKeyQuery<String, String> query = TimestampedKeyQuery.withKey("test-key");
            final QueryResult<ValueAndTimestamp<String>> result = store.query(query, PositionBound.unbounded(), new QueryConfig(false));

            // Verify IQv2 query result
            assertTrue(result.isSuccess(), "Expected query to succeed on InMemoryKeyValueStore");
            assertNotNull(result.getPosition(), "Expected position to be set");
            assertNotNull(result.getResult(), "Expected non-null result");
            assertInstanceOf(ValueAndTimestamp.class, result.getResult());
            assertEquals("test-value", result.getResult().value(), "TimestampedKeyQuery should return the value");
            assertEquals(12345L, result.getResult().timestamp(), "TimestampedKeyQuery should return the timestamp");
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldReturnPositionFromHeadersStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStoreWithHeaders("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            final Position position = wrapped.getPosition();

            // Verify: Position is returned (should be non-null)
            assertNotNull(position, "Expected non-null position");
            assertTrue(position.isEmpty(), "Expected position to be empty initially");
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldReturnPositionFromAdaptedTimestampedStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStore("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            // Verify adapter is used
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(TimestampedToHeadersStoreAdapter.class, wrapped);

            // Get position from adapter (should delegate to underlying store)
            final Position position = wrapped.getPosition();

            assertNotNull(position, "Expected non-null position from adapter");
            assertTrue(position.isEmpty(), "Expected position to be empty initially");
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldReturnPositionFromInMemoryStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("in-memory");
        when(supplier.get()).thenReturn(new InMemoryKeyValueStore("test-store"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            // Verify marker wrapper is used
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(HeadersBytesStore.class, wrapped);

            // Get position from marker (should delegate to InMemoryKeyValueStore)
            final Position position = wrapped.getPosition();

            assertNotNull(position, "Expected non-null position from in-memory store");
            assertTrue(position.isEmpty(), "Expected position to be empty initially");
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldMaintainPositionAcrossOperationsOnHeadersStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStoreWithHeaders("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            
            // Get initial position
            final Position initialPosition = wrapped.getPosition();
            assertNotNull(initialPosition, "Expected non-null initial position");

            // Put some data
            store.put("key1", ValueTimestampHeaders.make("value1", 100L, new RecordHeaders()));
            store.put("key2", ValueTimestampHeaders.make("value2", 200L, new RecordHeaders()));

            // Get position after puts
            final Position afterPutPosition = wrapped.getPosition();
            assertNotNull(afterPutPosition, "Expected non-null position after puts");
            
            // Position object should be the same instance (stores maintain a single position)
            // The position content might be updated internally by the context
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldReturnUnknownQueryTypeForKeyQueryOnHeadersStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStoreWithHeaders("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(new Bytes("test-key".getBytes()));
            final PositionBound positionBound = PositionBound.unbounded();
            final QueryConfig config = new QueryConfig(false);

            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            final QueryResult<byte[]> result = wrapped.query(query, positionBound, config);

            // Verify: Headers store currently returns UNKNOWN_QUERY_TYPE
            assertFalse(result.isSuccess(), "Expected query to fail with unknown query type");
            assertEquals(
                    FailureReason.UNKNOWN_QUERY_TYPE,
                    result.getFailureReason(),
                    "Expected UNKNOWN_QUERY_TYPE failure reason"
            );
            assertNotNull(result.getPosition(), "Expected position to be set");
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldReturnUnknownQueryTypeForRangeQueryOnHeadersStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStoreWithHeaders("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            final RangeQuery<Bytes, byte[]> query = RangeQuery.withRange(
                    new Bytes("a".getBytes()),
                    new Bytes("z".getBytes())
            );
            final PositionBound positionBound = PositionBound.unbounded();
            final QueryConfig config = new QueryConfig(false);

            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            final QueryResult<KeyValueIterator<Bytes, byte[]>> result = wrapped.query(query, positionBound, config);

            // Verify: Headers store currently returns UNKNOWN_QUERY_TYPE
            assertFalse(result.isSuccess(), "Expected query to fail with unknown query type");
            assertEquals(
                    FailureReason.UNKNOWN_QUERY_TYPE,
                    result.getFailureReason(),
                    "Expected UNKNOWN_QUERY_TYPE failure reason"
            );
            assertNotNull(result.getPosition(), "Expected position to be set");
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldCollectExecutionInfoForQueryOnHeadersStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStoreWithHeaders("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(new Bytes("test-key".getBytes()));
            final PositionBound positionBound = PositionBound.unbounded();
            final QueryConfig config = new QueryConfig(true); // Enable execution info

            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            final QueryResult<byte[]> result = wrapped.query(query, positionBound, config);

            // Verify: Execution info was collected
            assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
            assertTrue(
                    result.getExecutionInfo().get(0).contains("Handled in"),
                    "Expected execution info to contain handling information"
            );
            assertTrue(
                    result.getExecutionInfo().get(0).contains(RocksDBTimestampedStoreWithHeaders.class.getName()),
                    "Expected execution info to mention the class name"
            );
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldHandleKeyQueryOnAdaptedTimestampedStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStore("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            // Put data into the store (headers will be discarded when adapted to timestamped store)
            final Headers headers = new RecordHeaders();
            headers.add("adapter", "test".getBytes());
            store.put("test-key", ValueTimestampHeaders.make("adapter-value", 55555L, headers));

            // Verify adapter is used for legacy timestamped store
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(TimestampedToHeadersStoreAdapter.class, wrapped,
                    "Expected TimestampedToHeadersStoreAdapter for legacy timestamped store");

            // Query at typed level - KeyQuery should return just the value
            final KeyQuery<String, String> query = KeyQuery.withKey("test-key");
            final QueryResult<String> result = store.query(query, PositionBound.unbounded(), new QueryConfig(false));

            // Verify IQv2 query result
            // Adapter delegates to RocksDBTimestampedStore which supports IQv2 through RocksDBStore
            assertTrue(result.isSuccess(),
                    "Expected query to succeed since RocksDBTimestampedStore supports IQv2");
            assertNotNull(result.getPosition(), "Expected position to be set");
            assertInstanceOf(String.class, result.getResult());
            assertEquals("adapter-value", result.getResult(), "KeyQuery should return just the value");
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldHandleTimestampedKeyQueryOnAdaptedTimestampedStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStore("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            // Put data into the store (headers will be discarded when adapted to timestamped store)
            final Headers headers = new RecordHeaders();
            headers.add("adapter", "test".getBytes());
            store.put("test-key", ValueTimestampHeaders.make("adapter-value", 55555L, headers));

            // Verify adapter is used for legacy timestamped store
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(TimestampedToHeadersStoreAdapter.class, wrapped,
                    "Expected TimestampedToHeadersStoreAdapter for legacy timestamped store");

            // Query at typed level - TimestampedKeyQuery should return value + timestamp
            final TimestampedKeyQuery<String, String> query = TimestampedKeyQuery.withKey("test-key");
            final QueryResult<ValueAndTimestamp<String>> result = store.query(query, PositionBound.unbounded(), new QueryConfig(false));

            // Verify IQv2 query result
            // Adapter delegates to RocksDBTimestampedStore which supports IQv2 through RocksDBStore
            assertTrue(result.isSuccess(),
                    "Expected query to succeed since RocksDBTimestampedStore supports IQv2");
            assertNotNull(result.getPosition(), "Expected position to be set");
            assertNotNull(result.getResult(), "Expected non-null result");
            assertInstanceOf(ValueAndTimestamp.class, result.getResult());
            assertEquals("adapter-value", result.getResult().value(), "TimestampedKeyQuery should return the value");
            assertEquals(55555L, result.getResult().timestamp(), "TimestampedKeyQuery should return the timestamp");
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldCollectExecutionInfoForQueryOnAdaptedTimestampedStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStore("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        store.init(context, store);

        try {
            final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(new Bytes("test-key".getBytes()));
            final PositionBound positionBound = PositionBound.unbounded();
            final QueryConfig config = new QueryConfig(true); // Enable execution info

            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            final QueryResult<byte[]> result = wrapped.query(query, positionBound, config);

            // Verify: Execution info includes both adapter and underlying store
            assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");

            final String executionInfo = String.join("\n", result.getExecutionInfo());
            assertTrue(
                    executionInfo.contains("Handled in"),
                    "Expected execution info to contain handling information"
            );
            // Should mention the adapter class
            assertTrue(
                    executionInfo.contains(TimestampedToHeadersStoreAdapter.class.getName()),
                    "Expected execution info to mention the adapter class"
            );
        } finally {
            store.close();
        }
    }
}
