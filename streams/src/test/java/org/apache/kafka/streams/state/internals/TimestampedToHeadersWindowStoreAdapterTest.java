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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        final SegmentedBytesStore segmentedBytesStore = new RocksDBSegmentedBytesStore(
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
        final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);

        final QueryResult<WindowStoreIterator<byte[]>> result = adapter.query(query, positionBound, config);

        assertTrue(result.isSuccess(), "Expected query to succeed");
        assertNotNull(result.getResult(), "Expected result iterator to be present");
        assertNotNull(result.getPosition(), "Expected position to be set");
    }

    @Test
    public void shouldHandleWindowRangeQuerySuccessfully() {
        final WindowRangeQuery<Bytes, byte[]> query = WindowRangeQuery.withWindowStartRange(
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);

        final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> result =
                adapter.query(query, positionBound, config);

        assertTrue(result.isSuccess(), "Expected query to succeed");
        assertNotNull(result.getResult(), "Expected result iterator to be present");
        assertNotNull(result.getPosition(), "Expected position to be set");
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
}