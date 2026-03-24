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
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PlainToHeadersWindowStoreAdapterTest {

    private static final long WINDOW_SIZE = 10_000L;
    private static final long RETENTION_PERIOD = 60_000L;
    private static final long SEGMENT_INTERVAL = 30_000L;

    private PlainToHeadersWindowStoreAdapter adapter;
    private RocksDBWindowStore underlyingStore;
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

        final SegmentedBytesStore segmentedBytesStore = new RocksDBSegmentedBytesStore(
            "iqv2-test-store",
            "test-metrics-scope",
            RETENTION_PERIOD,
            SEGMENT_INTERVAL,
            new WindowKeySchema()
        );

        underlyingStore = new RocksDBWindowStore(
            segmentedBytesStore,
            false,
            WINDOW_SIZE
        );

        adapter = new PlainToHeadersWindowStoreAdapter(underlyingStore);
        adapter.init(context, adapter);
    }

    @AfterEach
    public void tearDown() {
        if (adapter != null) {
            adapter.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldHandleWindowKeyQuerySuccessfully() {
        final TimestampedWindowStoreWithHeaders<String, String> store = Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentWindowStore(
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
            final Headers headers1 = new RecordHeaders();
            headers1.add("key", "header1".getBytes());
            store.put("test-key", ValueTimestampHeaders.make("value1", 1000L, headers1), 1000L);

            final Headers headers2 = new RecordHeaders();
            headers2.add("key", "header2".getBytes());
            store.put("test-key", ValueTimestampHeaders.make("value2", 5000L, headers2), 5000L);

            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(PlainToHeadersWindowStoreAdapter.class, wrapped,
                "Expected PlainToHeadersWindowStoreAdapter for plain window store");

            // Query at typed level - For plain stores, WindowKeyQuery returns plain V (String), not ValueTimestampHeaders
            final WindowKeyQuery<String, ValueTimestampHeaders<String>> query = WindowKeyQuery.withKeyAndWindowStartRange(
                "test-key",
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(10000L)
            );
            final QueryResult<WindowStoreIterator<String>> result =
                (QueryResult<WindowStoreIterator<String>>) (QueryResult<?>) store.query(query, PositionBound.unbounded(), new QueryConfig(false));

            assertTrue(result.isSuccess(), "Expected query to succeed");
            assertNotNull(result.getPosition(), "Expected position to be set");
            assertNotNull(result.getResult(), "Expected non-null result");

            final WindowStoreIterator<String> iterator = result.getResult();
            assertTrue(iterator.hasNext(), "Expected at least one result");

            KeyValue<Long, String> kv = iterator.next();
            assertEquals(1000L, kv.key, "Expected first window timestamp");
            assertEquals("value1", kv.value, "WindowKeyQuery should return the plain value");

            assertTrue(iterator.hasNext(), "Expected second result");
            kv = iterator.next();
            assertEquals(5000L, kv.key, "Expected second window timestamp");
            assertEquals("value2", kv.value, "WindowKeyQuery should return the plain value");

            assertFalse(iterator.hasNext(), "Expected no more results");
            iterator.close();
        } finally {
            store.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldHandleWindowRangeQuerySuccessfully() {
        final TimestampedWindowStoreWithHeaders<String, String> store = Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentWindowStore(
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
            final Headers headers1 = new RecordHeaders();
            headers1.add("source", "key1".getBytes());
            store.put("key1", ValueTimestampHeaders.make("value1", 1000L, headers1), 1000L);

            final Headers headers2 = new RecordHeaders();
            headers2.add("source", "key2".getBytes());
            store.put("key2", ValueTimestampHeaders.make("value2", 5000L, headers2), 5000L);

            final Headers headers3 = new RecordHeaders();
            headers3.add("source", "key3".getBytes());
            store.put("key3", ValueTimestampHeaders.make("value3", 3000L, headers3), 3000L);

            final StateStore wrapped = ((WrappedStateStore) store).wrapped();
            assertInstanceOf(PlainToHeadersWindowStoreAdapter.class, wrapped,
                "Expected PlainToHeadersWindowStoreAdapter for plain window store");

            // Query at typed level - For plain stores, WindowRangeQuery returns plain V (String), not ValueTimestampHeaders
            final WindowRangeQuery<String, ValueTimestampHeaders<String>> query = WindowRangeQuery.withWindowStartRange(
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(10000L)
            );
            final QueryResult<KeyValueIterator<Windowed<String>, String>> result =
                (QueryResult<KeyValueIterator<Windowed<String>, String>>) (QueryResult<?>) store.query(query, PositionBound.unbounded(), new QueryConfig(false));

            assertTrue(result.isSuccess(), "Expected query to succeed");
            assertNotNull(result.getResult(), "Expected result iterator to be present");
            assertNotNull(result.getPosition(), "Expected position to be set");

            final KeyValueIterator<Windowed<String>, String> iterator = result.getResult();

            assertTrue(iterator.hasNext(), "Expected at least one result");
            KeyValue<Windowed<String>, String> kv = iterator.next();
            assertEquals("key1", kv.key.key(), "Expected first key");
            assertEquals(1000L, kv.key.window().start(), "Expected first window start");
            assertEquals("value1", kv.value, "WindowRangeQuery should return the plain value");

            assertTrue(iterator.hasNext(), "Expected second result");
            kv = iterator.next();
            assertEquals("key2", kv.key.key(), "Expected second key");
            assertEquals(5000L, kv.key.window().start(), "Expected second window start");
            assertEquals("value2", kv.value, "WindowRangeQuery should return the plain value");

            assertTrue(iterator.hasNext(), "Expected third result");
            kv = iterator.next();
            assertEquals("key3", kv.key.key(), "Expected third key");
            assertEquals(3000L, kv.key.window().start(), "Expected third window start");
            assertEquals("value3", kv.value, "WindowRangeQuery should return the plain value");

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
            if (info.contains("Handled in") && info.contains(PlainToHeadersWindowStoreAdapter.class.getName())) {
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
            if (info.contains("Handled in") && info.contains(PlainToHeadersWindowStoreAdapter.class.getName())) {
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

        boolean foundAdapterInfo = false;
        for (final String info : result.getExecutionInfo()) {
            if (info.contains(PlainToHeadersWindowStoreAdapter.class.getName())) {
                foundAdapterInfo = true;
                break;
            }
        }
        assertFalse(foundAdapterInfo, "Expected no execution info from adapter when not requested");
    }
}
