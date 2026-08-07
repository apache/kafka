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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class TimestampedToHeadersWindowStoreAdapterTest {

    private static final long WINDOW_SIZE = 10_000L;
    private static final long RETENTION_PERIOD = 60_000L;
    private static final long SEGMENT_INTERVAL = 30_000L;

    private TimestampedToHeadersWindowStoreAdapter adapter;
    private RocksDBTimestampedWindowStore underlyingStore;
    private InternalMockProcessorContext<String, String> context;
    private File baseDir;

    @BeforeEach
    public void setUp() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        baseDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                baseDir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );

        final RocksDBSegmentedBytesStore segmentedBytesStore = new RocksDBSegmentedBytesStore(
                "iqv2-test-store",
                "test-metrics-scope",
                RETENTION_PERIOD,
                SEGMENT_INTERVAL,
                new WindowKeySchema()
        );

        underlyingStore = new RocksDBTimestampedWindowStore(
                segmentedBytesStore,
                false,
                WINDOW_SIZE
        );

        adapter = new TimestampedToHeadersWindowStoreAdapter(underlyingStore);
        adapter.init(context, adapter);
    }

    @AfterEach
    public void tearDown() {
        if (adapter != null) {
            adapter.close();
        }
    }

    @Test
    public void shouldHandleWindowKeyQuerySuccessfully() {
        // Build a typed window store using timestamped window store (adapter wraps it)
        final TimestampedWindowStoreWithHeaders<String, String> store = Stores.timestampedWindowStoreWithHeadersBuilder(
            Stores.persistentTimestampedWindowStore(
                "typed-adapter-test",
                ofMillis(RETENTION_PERIOD),
                ofMillis(WINDOW_SIZE),
                false),
            Serdes.String(),
            Serdes.String())
            .withLoggingDisabled()
            .build();

        store.init(context, store);

        try {
            // Put data into the store (headers will be empty when adapted from timestamped store)
            final Headers headers1 = new RecordHeaders();
            headers1.add("key", "header1".getBytes());
            store.put("test-key", ValueTimestampHeaders.make("value1", 1000L, headers1), 1000L);

            final Headers headers2 = new RecordHeaders();
            headers2.add("key", "header2".getBytes());
            store.put("test-key", ValueTimestampHeaders.make("value2", 5000L, headers2), 5000L);

            // Verify adapter is used for legacy timestamped window store
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(TimestampedToHeadersWindowStoreAdapter.class, wrapped,
                    "Expected TimestampedToHeadersWindowStoreAdapter for legacy timestamped window store");

            // Query at typed level - WindowKeyQuery should return windowed values with timestamps
            final WindowKeyQuery<String, ValueAndTimestamp<String>> query = WindowKeyQuery.withKeyAndWindowStartRange(
                "test-key",
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(10000L)
            );
            final QueryResult<WindowStoreIterator<ValueAndTimestamp<String>>> result =
                store.query(query, PositionBound.unbounded(), new QueryConfig(false));

            // Verify IQv2 query result
            // Adapter delegates to RocksDBTimestampedWindowStore which supports IQv2
            assertTrue(result.isSuccess(),
                    "Expected query to succeed since RocksDBTimestampedWindowStore supports IQv2");
            assertNotNull(result.getPosition(), "Expected position to be set");
            assertNotNull(result.getResult(), "Expected non-null result");

            // Verify the actual query results
            final WindowStoreIterator<ValueAndTimestamp<String>> iterator = result.getResult();
            assertTrue(iterator.hasNext(), "Expected at least one result");

            KeyValue<Long, ValueAndTimestamp<String>> kv = iterator.next();
            assertEquals(1000L, kv.key, "Expected first window timestamp");
            assertInstanceOf(ValueAndTimestamp.class, kv.value);
            assertEquals("value1", kv.value.value(), "WindowKeyQuery should return the value");
            assertEquals(1000L, kv.value.timestamp(), "WindowKeyQuery should return the timestamp");

            assertTrue(iterator.hasNext(), "Expected second result");
            kv = iterator.next();
            assertEquals(5000L, kv.key, "Expected second window timestamp");
            assertEquals("value2", kv.value.value(), "WindowKeyQuery should return the value");
            assertEquals(5000L, kv.value.timestamp(), "WindowKeyQuery should return the timestamp");

            assertFalse(iterator.hasNext(), "Expected no more results");
            iterator.close();
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldHandleWindowRangeQuerySuccessfully() {
        // Build a typed window store using timestamped window store (adapter wraps it)
        final TimestampedWindowStoreWithHeaders<String, String> store = Stores.timestampedWindowStoreWithHeadersBuilder(
            Stores.persistentTimestampedWindowStore(
                "typed-range-adapter-test",
                ofMillis(RETENTION_PERIOD),
                ofMillis(WINDOW_SIZE),
                false),
            Serdes.String(),
            Serdes.String())
            .withLoggingDisabled()
            .build();

        store.init(context, store);

        try {
            // Put data into the store (headers will be empty when adapted from timestamped store)
            final Headers headers1 = new RecordHeaders();
            headers1.add("source", "key1".getBytes());
            store.put("key1", ValueTimestampHeaders.make("value1", 1000L, headers1), 1000L);

            final Headers headers2 = new RecordHeaders();
            headers2.add("source", "key2".getBytes());
            store.put("key2", ValueTimestampHeaders.make("value2", 5000L, headers2), 5000L);

            final Headers headers3 = new RecordHeaders();
            headers3.add("source", "key3".getBytes());
            store.put("key3", ValueTimestampHeaders.make("value3", 3000L, headers3), 3000L);

            // Verify adapter is used for legacy timestamped window store
            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(TimestampedToHeadersWindowStoreAdapter.class, wrapped,
                    "Expected TimestampedToHeadersWindowStoreAdapter for legacy timestamped window store");

            // Query at typed level - WindowRangeQuery should return all windowed key-values with timestamps
            final WindowRangeQuery<String, ValueAndTimestamp<String>> query = WindowRangeQuery.withWindowStartRange(
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(10000L)
            );
            final QueryResult<KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>>> result =
                store.query(query, PositionBound.unbounded(), new QueryConfig(false));

            // Verify IQv2 query result
            // Adapter delegates to RocksDBTimestampedWindowStore which supports IQv2
            assertTrue(result.isSuccess(),
                    "Expected query to succeed since RocksDBTimestampedWindowStore supports IQv2");
            assertNotNull(result.getResult(), "Expected result iterator to be present");
            assertNotNull(result.getPosition(), "Expected position to be set");

            // Verify the actual query results (should be sorted by key then timestamp)
            final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> iterator = result.getResult();

            assertTrue(iterator.hasNext(), "Expected at least one result");
            KeyValue<Windowed<String>, ValueAndTimestamp<String>> kv = iterator.next();
            assertEquals("key1", kv.key.key(), "Expected first key");
            assertEquals(1000L, kv.key.window().start(), "Expected first window start");
            assertInstanceOf(ValueAndTimestamp.class, kv.value);
            assertEquals("value1", kv.value.value(), "WindowRangeQuery should return the value");
            assertEquals(1000L, kv.value.timestamp(), "WindowRangeQuery should return the timestamp");

            assertTrue(iterator.hasNext(), "Expected second result");
            kv = iterator.next();
            assertEquals("key2", kv.key.key(), "Expected second key");
            assertEquals(5000L, kv.key.window().start(), "Expected second window start");
            assertEquals("value2", kv.value.value(), "WindowRangeQuery should return the value");

            assertTrue(iterator.hasNext(), "Expected third result");
            kv = iterator.next();
            assertEquals("key3", kv.key.key(), "Expected third key");
            assertEquals(3000L, kv.key.window().start(), "Expected third window start");
            assertEquals("value3", kv.value.value(), "WindowRangeQuery should return the value");

            assertFalse(iterator.hasNext(), "Expected no more results");
            iterator.close();
        } finally {
            store.close();
        }
    }

    @Test
    public void shouldCollectExecutionInfoForWindowKeyQueryWhenRequested() {
        final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(true); // Enable execution info

        final QueryResult<WindowStoreIterator<byte[]>> result = adapter.query(query, positionBound, config);

        assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
        boolean foundAdapterInfo = false;
        for (final String info : result.getExecutionInfo()) {
            if (info.contains("Handled in") && info.contains(TimestampedToHeadersWindowStoreAdapter.class.getName())) {
                foundAdapterInfo = true;
                break;
            }
        }
        assertTrue(foundAdapterInfo, "Expected execution info to mention the adapter class");
    }

    @Test
    public void shouldCollectExecutionInfoForWindowRangeQueryWhenRequested() {
        final WindowRangeQuery<Bytes, byte[]> query = WindowRangeQuery.withWindowStartRange(
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(true); // Enable execution info

        final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> result =
                adapter.query(query, positionBound, config);

        assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
        boolean foundAdapterInfo = false;
        for (final String info : result.getExecutionInfo()) {
            if (info.contains("Handled in") && info.contains(TimestampedToHeadersWindowStoreAdapter.class.getName())) {
                foundAdapterInfo = true;
                break;
            }
        }
        assertTrue(foundAdapterInfo, "Expected execution info to mention the adapter class");
    }

    @Test
    public void shouldNotCollectExecutionInfoWhenNotRequested() {
        final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false); // Disable execution info

        final QueryResult<WindowStoreIterator<byte[]>> result = adapter.query(query, positionBound, config);

        // Verify: Adapter's execution info was not collected
        boolean foundAdapterInfo = false;
        for (final String info : result.getExecutionInfo()) {
            if (info.contains(TimestampedToHeadersWindowStoreAdapter.class.getName())) {
                foundAdapterInfo = true;
                break;
            }
        }
        assertFalse(foundAdapterInfo, "Expected no execution info from adapter when not requested");
    }

    // Method-level behavioral coverage over a mocked inner store: reads convert/wrap, put strips
    // headers. See TimestampedToHeadersWindowStoreAdapterCompletenessTest for the completeness guard.
    private static final Bytes KEY = new Bytes("key".getBytes());
    private static final Bytes KEY_TO = new Bytes("key-to".getBytes());
    private static final byte[] RAW_VALUE = new byte[] {0, 0, 0, 0, 0, 0, 0, 42, 'v', 'a', 'l'};
    private static final Instant T_FROM = Instant.ofEpochMilli(0L);
    private static final Instant T_TO = Instant.ofEpochMilli(10L);

    private WindowStore<Bytes, byte[]> mockInner;
    private TimestampedToHeadersWindowStoreAdapter mockAdapter;

    private void setUpMockAdapter() {
        mockInner = mock(WindowStore.class, withSettings().extraInterfaces(TimestampedBytesStore.class));
        when(mockInner.persistent()).thenReturn(true);
        mockAdapter = new TimestampedToHeadersWindowStoreAdapter(mockInner);
    }

    @Test
    public void shouldStripHeadersOnPut() {
        setUpMockAdapter();
        mockAdapter.put(KEY, convertToHeaderFormat(RAW_VALUE), 5L);
        verify(mockInner).put(KEY, RAW_VALUE, 5L);
    }

    @Test
    public void shouldConvertFetchByKeyAndTimestampToHeaderFormat() {
        setUpMockAdapter();
        when(mockInner.fetch(KEY, 5L)).thenReturn(RAW_VALUE);
        assertArrayEquals(convertToHeaderFormat(RAW_VALUE), mockAdapter.fetch(KEY, 5L));
    }

    @Test
    public void shouldReturnNullWhenFetchByKeyAndTimestampReturnsNull() {
        setUpMockAdapter();
        when(mockInner.fetch(KEY, 5L)).thenReturn(null);
        assertNull(mockAdapter.fetch(KEY, 5L));
    }

    @Test
    public void shouldWrapFetchByKeyAndTimeRangeIterator() {
        setUpMockAdapter();
        final WindowStoreIterator<byte[]> inner = mock(WindowStoreIterator.class);
        when(mockInner.fetch(KEY, 0L, 10L)).thenReturn(inner);
        assertInstanceOf(WindowStoreIterator.class, mockAdapter.fetch(KEY, 0L, 10L));
    }

    @Test
    public void shouldWrapFetchByKeyAndInstantRangeIterator() {
        setUpMockAdapter();
        final WindowStoreIterator<byte[]> inner = mock(WindowStoreIterator.class);
        when(mockInner.fetch(KEY, T_FROM, T_TO)).thenReturn(inner);
        assertInstanceOf(WindowStoreIterator.class, mockAdapter.fetch(KEY, T_FROM, T_TO));
    }

    @Test
    public void shouldWrapBackwardFetchByKeyAndTimeRangeIterator() {
        setUpMockAdapter();
        final WindowStoreIterator<byte[]> inner = mock(WindowStoreIterator.class);
        when(mockInner.backwardFetch(KEY, 0L, 10L)).thenReturn(inner);
        assertInstanceOf(WindowStoreIterator.class, mockAdapter.backwardFetch(KEY, 0L, 10L));
    }

    @Test
    public void shouldWrapBackwardFetchByKeyAndInstantRangeIterator() {
        setUpMockAdapter();
        final WindowStoreIterator<byte[]> inner = mock(WindowStoreIterator.class);
        when(mockInner.backwardFetch(KEY, T_FROM, T_TO)).thenReturn(inner);
        assertInstanceOf(WindowStoreIterator.class, mockAdapter.backwardFetch(KEY, T_FROM, T_TO));
    }

    @Test
    public void shouldWrapFetchByKeyRangeAndTimeRangeIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.fetch(KEY, KEY_TO, 0L, 10L)).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.fetch(KEY, KEY_TO, 0L, 10L));
    }

    @Test
    public void shouldWrapFetchByKeyRangeAndInstantRangeIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.fetch(KEY, KEY_TO, T_FROM, T_TO)).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.fetch(KEY, KEY_TO, T_FROM, T_TO));
    }

    @Test
    public void shouldWrapBackwardFetchByKeyRangeAndTimeRangeIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.backwardFetch(KEY, KEY_TO, 0L, 10L)).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.backwardFetch(KEY, KEY_TO, 0L, 10L));
    }

    @Test
    public void shouldWrapBackwardFetchByKeyRangeAndInstantRangeIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.backwardFetch(KEY, KEY_TO, T_FROM, T_TO)).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.backwardFetch(KEY, KEY_TO, T_FROM, T_TO));
    }

    @Test
    public void shouldWrapFetchAllByTimeRangeIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.fetchAll(0L, 10L)).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.fetchAll(0L, 10L));
    }

    @Test
    public void shouldWrapFetchAllByInstantRangeIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.fetchAll(T_FROM, T_TO)).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.fetchAll(T_FROM, T_TO));
    }

    @Test
    public void shouldWrapBackwardFetchAllByTimeRangeIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.backwardFetchAll(0L, 10L)).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.backwardFetchAll(0L, 10L));
    }

    @Test
    public void shouldWrapBackwardFetchAllByInstantRangeIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.backwardFetchAll(T_FROM, T_TO)).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.backwardFetchAll(T_FROM, T_TO));
    }

    @Test
    public void shouldWrapAllIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.all()).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.all());
    }

    @Test
    public void shouldWrapBackwardAllIterator() {
        setUpMockAdapter();
        final KeyValueIterator<Windowed<Bytes>, byte[]> inner = mock(KeyValueIterator.class);
        when(mockInner.backwardAll()).thenReturn(inner);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, mockAdapter.backwardAll());
    }

    @Test
    public void shouldDelegateName() {
        setUpMockAdapter();
        when(mockInner.name()).thenReturn("inner-store");
        assertEquals("inner-store", mockAdapter.name());
    }

    @Test
    public void shouldDelegateIsOpen() {
        setUpMockAdapter();
        when(mockInner.isOpen()).thenReturn(true);
        assertTrue(mockAdapter.isOpen());
    }

    @Test
    public void shouldReturnPersistentTrue() {
        setUpMockAdapter();
        assertTrue(mockAdapter.persistent());
    }
}
