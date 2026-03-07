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
import org.apache.kafka.streams.query.FailureReason;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RocksDBTimestampedWindowStoreWithHeadersTest {

    private static final String STORE_NAME = "test-window-store";
    private static final long WINDOW_SIZE = 10_000L;
    private static final long RETENTION_PERIOD = 60_000L;
    private static final long SEGMENT_INTERVAL = 30_000L;

    private RocksDBTimestampedWindowStoreWithHeaders windowStore;
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
                STORE_NAME,
                "test-metrics-scope",
                RETENTION_PERIOD,
                SEGMENT_INTERVAL,
                new WindowKeySchema()
        );

        windowStore = new RocksDBTimestampedWindowStoreWithHeaders(
                segmentedBytesStore,
                false,
                WINDOW_SIZE
        );

        windowStore.init(context, windowStore);
    }

    @AfterEach
    public void tearDown() {
        if (windowStore != null) {
            windowStore.close();
        }
    }

    @Test
    public void shouldReturnUnknownQueryTypeForWindowKeyQuery() {
        final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);

        final QueryResult<WindowStoreIterator<byte[]>> result = windowStore.query(query, positionBound, config);

        // Verify: Window store with headers currently returns UNKNOWN_QUERY_TYPE
        assertFalse(result.isSuccess(), "Expected query to fail with unknown query type");
        assertEquals(
                FailureReason.UNKNOWN_QUERY_TYPE,
                result.getFailureReason(),
                "Expected UNKNOWN_QUERY_TYPE failure reason"
        );
        assertNotNull(result.getPosition(), "Expected position to be set");
    }

    @Test
    public void shouldReturnUnknownQueryTypeForWindowRangeQuery() {
        final WindowRangeQuery<Bytes, byte[]> query = WindowRangeQuery.withWindowStartRange(
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);

        final QueryResult<KeyValueIterator<org.apache.kafka.streams.kstream.Windowed<Bytes>, byte[]>> result =
                windowStore.query(query, positionBound, config);

        // Verify: Window store with headers currently returns UNKNOWN_QUERY_TYPE
        assertFalse(result.isSuccess(), "Expected query to fail with unknown query type");
        assertEquals(
                FailureReason.UNKNOWN_QUERY_TYPE,
                result.getFailureReason(),
                "Expected UNKNOWN_QUERY_TYPE failure reason"
        );
        assertNotNull(result.getPosition(), "Expected position to be set");
    }

    @Test
    public void shouldCollectExecutionInfoWhenRequested() {
        final WindowKeyQuery<Bytes, byte[]> query = WindowKeyQuery.withKeyAndWindowStartRange(
                new Bytes("test-key".getBytes()),
                Instant.ofEpochMilli(0),
                Instant.ofEpochMilli(Long.MAX_VALUE)
        );
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(true); // Enable execution info

        final QueryResult<WindowStoreIterator<byte[]>> result = windowStore.query(query, positionBound, config);

        // Verify: Execution info was collected
        assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
        assertTrue(
                result.getExecutionInfo().get(0).contains("Handled in"),
                "Expected execution info to contain handling information"
        );
        assertTrue(
                result.getExecutionInfo().get(0).contains(RocksDBTimestampedWindowStoreWithHeaders.class.getName()),
                "Expected execution info to mention the class name"
        );
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

        final QueryResult<WindowStoreIterator<byte[]>> result = windowStore.query(query, positionBound, config);

        // Verify: No execution info was collected
        assertTrue(result.getExecutionInfo().isEmpty(), "Expected no execution info to be collected");
    }
}
