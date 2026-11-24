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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RocksDBTimeOrderedSessionStoreWithHeadersTest {

    private static final String STORE_NAME = "test-session-store";
    private static final long RETENTION_PERIOD = 60_000L;
    private static final long SEGMENT_INTERVAL = 30_000L;

    private RocksDBTimeOrderedSessionStoreWithHeaders sessionStore;
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

        sessionStore = new RocksDBTimeOrderedSessionStoreWithHeaders(
            new RocksDBTimeOrderedSessionSegmentedBytesStoreWithHeaders(
                STORE_NAME,
                "test-metrics-scope",
                RETENTION_PERIOD,
                SEGMENT_INTERVAL,
                true
            )
        );
        sessionStore.init(context, sessionStore);
    }

    @AfterEach
    public void tearDown() {
        if (sessionStore != null) {
            sessionStore.close();
        }
    }

    @Test
    public void shouldHandleWindowRangeQuery() {
        // KIP-1356: the withKey form of the headers-aware TimestampedWindowRangeWithHeadersQuery
        // forwards a raw WindowRangeQuery to this native store, so enable WindowRangeQuery via the
        // inherited RocksDBTimeOrderedSessionStore handling (StoreQueryUtils), returning the raw
        // stored header-format bytes (previously UNKNOWN_QUERY_TYPE for every query). This also fixes
        // the same pre-existing gap for the plain (non-headers) WindowRangeQuery.withKey.
        final Bytes key = new Bytes("test-key".getBytes());
        final byte[] storedBytes = "headers+aggregation".getBytes();
        final Windowed<Bytes> windowedKey = new Windowed<>(key, new SessionWindow(0L, 1_000L));
        sessionStore.put(windowedKey, storedBytes);

        final WindowRangeQuery<Bytes, byte[]> query = WindowRangeQuery.withKey(key);
        final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> result =
            sessionStore.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertTrue(result.isSuccess(), "Expected WindowRangeQuery to succeed");
        try (KeyValueIterator<Windowed<Bytes>, byte[]> iterator = result.getResult()) {
            assertTrue(iterator.hasNext(), "Expected the stored session in the window range result");
            final KeyValue<Windowed<Bytes>, byte[]> keyValue = iterator.next();
            assertEquals(key, keyValue.key.key());
            assertArrayEquals(storedBytes, keyValue.value, "Expected the raw stored bytes to be returned");
            assertFalse(iterator.hasNext(), "Expected exactly one entry in the window range result");
        }
        assertNotNull(result.getPosition());
    }

    @Test
    public void shouldCollectExecutionInfoWhenRequested() {
        final WindowRangeQuery<Bytes, byte[]> query = WindowRangeQuery.withKey(
            new Bytes("test-key".getBytes())
        );
        final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> result =
            sessionStore.query(query, PositionBound.unbounded(), new QueryConfig(true));

        // The query now succeeds and returns an open store iterator, so close it to avoid a leak.
        try (KeyValueIterator<Windowed<Bytes>, byte[]> iterator = result.getResult()) {
            assertFalse(result.getExecutionInfo().isEmpty());
            assertTrue(result.getExecutionInfo().get(0).contains("Handled in"));
            assertTrue(result.getExecutionInfo().get(0).contains(
                RocksDBTimeOrderedSessionStoreWithHeaders.class.getName()));
        }
    }

    @Test
    public void shouldNotCollectExecutionInfoWhenNotRequested() {
        final WindowRangeQuery<Bytes, byte[]> query = WindowRangeQuery.withKey(
            new Bytes("test-key".getBytes())
        );
        final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> result =
            sessionStore.query(query, PositionBound.unbounded(), new QueryConfig(false));

        try (KeyValueIterator<Windowed<Bytes>, byte[]> iterator = result.getResult()) {
            assertTrue(result.getExecutionInfo().isEmpty());
        }
    }
}
