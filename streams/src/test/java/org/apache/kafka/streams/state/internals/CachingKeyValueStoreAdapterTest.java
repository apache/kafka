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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * IQv2 read path for a TimestampedKeyValueStore backed by a plain persistent store (via
 * {@link KeyValueToTimestampedKeyValueByteStoreAdapter}): a cached entry is returned in
 * timestamped format as-is, while a cache-bypassing read surfaces the adapter's dummy `-1`
 * timestamp because the inner store never held one.
 */
public class CachingKeyValueStoreAdapterTest {

    private static final Bytes KEY = Bytes.wrap("key".getBytes(StandardCharsets.UTF_8));
    private static final byte[] PLAIN_VALUE = "value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_WITH_TIMESTAMP = ByteBuffer
        .allocate(8 + PLAIN_VALUE.length)
        .putLong(42L)
        .put(PLAIN_VALUE)
        .array();

    private CachingKeyValueStore store;

    @BeforeEach
    public void setUp() {
        store = new CachingKeyValueStore(
            new KeyValueToTimestampedKeyValueByteStoreAdapter(new RocksDBStore("store", "rocksdb-state")));
        final ThreadCache cache = new ThreadCache(new LogContext("test "), 1_000_000, new MockStreamsMetrics(new Metrics()));
        final InternalMockProcessorContext<?, ?> context =
            new InternalMockProcessorContext<>(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, "topic", new RecordHeaders()));
        store.init(context, store);
    }

    @AfterEach
    public void tearDown() {
        store.close();
    }

    @Test
    public void shouldReturnTimestampedBytesFromCacheHit() {
        store.put(KEY, VALUE_WITH_TIMESTAMP);

        final QueryResult<byte[]> result =
            store.query(KeyQuery.withKey(KEY), PositionBound.unbounded(), new QueryConfig(false));

        // the cache holds the timestamped bytes and must not strip the timestamp, since the
        // adapter advertises the timestamped format
        assertArrayEquals(VALUE_WITH_TIMESTAMP, result.getResult());
    }

    @Test
    public void shouldReturnDummyTimestampFromAdapterOnCacheBypass() {
        store.put(KEY, VALUE_WITH_TIMESTAMP);
        store.commit(Map.of());

        final QueryResult<byte[]> result =
            store.query(KeyQuery.<Bytes, byte[]>withKey(KEY).skipCache(), PositionBound.unbounded(), new QueryConfig(false));

        // the flush through the adapter stripped the timestamp, so the read fabricates `-1`
        assertArrayEquals(convertToTimestampedFormat(PLAIN_VALUE), result.getResult());
    }
}
