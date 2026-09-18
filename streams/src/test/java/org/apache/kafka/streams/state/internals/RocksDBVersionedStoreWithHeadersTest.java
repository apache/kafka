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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RocksDBVersionedStoreWithHeadersTest {

    private static final long HISTORY_RETENTION = 60_000L;
    private static final long SEGMENT_INTERVAL = 20_000L;

    private RocksDBVersionedStoreWithHeaders store;

    @BeforeEach
    void setUp() {
        store = new RocksDBVersionedStoreWithHeaders("test-store", "rocksdb", HISTORY_RETENTION, SEGMENT_INTERVAL);
        final InternalMockProcessorContext context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            new StreamsConfig(StreamsTestUtils.getStreamsConfig())
        );
        store.init((StateStoreContext) context, store);
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
    }

    private static Bytes key(final String k) {
        return Bytes.wrap(k.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] value(final String v) {
        return v.getBytes(StandardCharsets.UTF_8);
    }

    // End-to-end tests using the store's public API

    @Test
    public void shouldPutAndGetWithHeaders() {
        final Bytes key = key("testKey");
        final byte[] val = value("testValue");
        final long timestamp = 1000L;
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("traceId", "trace-123".getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("userId", "user-456".getBytes(StandardCharsets.UTF_8)));

        store.put(key, val, timestamp, headers);

        final VersionedRecord<byte[]> record = store.get(key);
        assertNotNull(record);
        assertArrayEquals(val, record.value());
        assertEquals(timestamp, record.timestamp());

        final Headers returnedHeaders = record.headers();
        assertNotNull(returnedHeaders);
        assertEquals("trace-123",
            new String(returnedHeaders.lastHeader("traceId").value(), StandardCharsets.UTF_8));
        assertEquals("user-456",
            new String(returnedHeaders.lastHeader("userId").value(), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldPutAndGetWithEmptyHeaders() {
        final Bytes key = key("emptyHeadersKey");
        final byte[] val = value("emptyHeadersValue");
        final long timestamp = 2000L;
        final RecordHeaders headers = new RecordHeaders();

        store.put(key, val, timestamp, headers);

        final VersionedRecord<byte[]> record = store.get(key);
        assertNotNull(record);
        assertArrayEquals(val, record.value());
        assertEquals(timestamp, record.timestamp());

        final Headers returnedHeaders = record.headers();
        assertNotNull(returnedHeaders);
        assertFalse(returnedHeaders.iterator().hasNext());
    }

    @Test
    public void shouldGetByTimestampWithHeaders() {
        final Bytes key = key("versionedKey");
        final long ts1 = 1000L;
        final long ts2 = 2000L;

        final RecordHeaders headers1 = new RecordHeaders();
        headers1.add(new RecordHeader("version", "1".getBytes(StandardCharsets.UTF_8)));

        final RecordHeaders headers2 = new RecordHeaders();
        headers2.add(new RecordHeader("version", "2".getBytes(StandardCharsets.UTF_8)));

        store.put(key, value("value1"), ts1, headers1);
        store.put(key, value("value2"), ts2, headers2);

        // Get as of ts2
        final VersionedRecord<byte[]> record = store.get(key, ts2);
        assertNotNull(record);
        assertArrayEquals(value("value2"), record.value());
        assertEquals(ts2, record.timestamp());
        assertEquals("2",
            new String(record.headers().lastHeader("version").value(), StandardCharsets.UTF_8));

        // Get as of ts1 (should return version 1)
        final VersionedRecord<byte[]> record1 = store.get(key, ts1);
        assertNotNull(record1);
        assertArrayEquals(value("value1"), record1.value());
        assertEquals(ts1, record1.timestamp());
        assertEquals("1",
            new String(record1.headers().lastHeader("version").value(), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldDeleteRecordWithHeaders() {
        final Bytes key = key("deleteKey");
        final byte[] val = value("deleteValue");
        final long timestamp = 1000L;
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("operation", "put".getBytes(StandardCharsets.UTF_8)));

        store.put(key, val, timestamp, headers);

        // Verify the record exists before deletion
        final VersionedRecord<byte[]> record = store.get(key);
        assertNotNull(record);
        assertArrayEquals(val, record.value());
        assertEquals(timestamp, record.timestamp());
        assertEquals("put",
            new String(record.headers().lastHeader("operation").value(), StandardCharsets.UTF_8));

        // Delete the record by putting a tombstone at a later timestamp
        store.put(key, null, 2000L, new RecordHeaders());

        // Verify the record is now deleted (returns null for get)
        final VersionedRecord<byte[]> afterDelete = store.get(key);
        assertNull(afterDelete);
    }

    @Test
    public void shouldHandleTombstoneViaHeadersPut() {
        final Bytes key = key("tombstoneKey");
        final long timestamp = 1000L;
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("tombstoneMarker", "true".getBytes(StandardCharsets.UTF_8)));

        // Put null value with headers (tombstone)
        store.put(key, null, timestamp, headers);

        // Get should return null for the value
        final VersionedRecord<byte[]> record = store.get(key);
        assertNull(record);
    }

    @Test
    public void shouldPutWithoutHeadersDefaultsToEmptyHeaders() {
        final Bytes key = key("noHeadersKey");
        final byte[] val = value("noHeadersValue");
        final long timestamp = 3000L;

        // Use put without headers argument
        store.put(key, val, timestamp);

        final VersionedRecord<byte[]> record = store.get(key);
        assertNotNull(record);
        assertArrayEquals(val, record.value());
        assertEquals(timestamp, record.timestamp());

        final Headers returnedHeaders = record.headers();
        assertNotNull(returnedHeaders);
        assertFalse(returnedHeaders.iterator().hasNext());
    }

    @Test
    public void shouldPreserveHeadersWithNullHeaderValues() {
        final Bytes key = key("nullHeaderValueKey");
        final byte[] val = value("testValue");
        final long timestamp = 4000L;
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("nullValue", null));
        headers.add(new RecordHeader("normalValue", "normal".getBytes(StandardCharsets.UTF_8)));

        store.put(key, val, timestamp, headers);

        final VersionedRecord<byte[]> record = store.get(key);
        assertNotNull(record);
        assertArrayEquals(val, record.value());

        final Headers returnedHeaders = record.headers();
        assertNotNull(returnedHeaders);
        assertNull(returnedHeaders.lastHeader("nullValue").value());
        assertEquals("normal",
            new String(returnedHeaders.lastHeader("normalValue").value(), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldPreserveMultipleHeadersSameKey() {
        final Bytes key = key("multiHeaderKey");
        final byte[] val = value("multiHeaderValue");
        final long timestamp = 5000L;
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("tag", "tag1".getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("tag", "tag2".getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("tag", "tag3".getBytes(StandardCharsets.UTF_8)));

        store.put(key, val, timestamp, headers);

        final VersionedRecord<byte[]> record = store.get(key);
        assertNotNull(record);
        assertArrayEquals(val, record.value());

        final Headers returnedHeaders = record.headers();
        assertNotNull(returnedHeaders);

        // Verify all headers with same key are preserved
        final java.util.List<Header> tagHeaders = new java.util.ArrayList<>();
        for (final Header h : returnedHeaders) {
            if (h.key().equals("tag")) {
                tagHeaders.add(h);
            }
        }
        assertEquals(3, tagHeaders.size());
        assertEquals("tag1", new String(tagHeaders.get(0).value(), StandardCharsets.UTF_8));
        assertEquals("tag2", new String(tagHeaders.get(1).value(), StandardCharsets.UTF_8));
        assertEquals("tag3", new String(tagHeaders.get(2).value(), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldRestoreNativeChangelogHeadersIntoLocalFormat() {
        final Bytes key = key("restoreKey");
        final byte[] val = value("restoreValue");
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("traceId", value("trace-restore")));

        store.restoreBatch(List.of(changelogRecord(key, val, 1000L, headers)));

        final VersionedRecord<byte[]> record = store.get(key);
        assertNotNull(record);
        assertArrayEquals(val, record.value());
        assertEquals("trace-restore", new String(record.headers().lastHeader("traceId").value(), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldRestoreEmptyNativeChangelogHeadersIntoLocalFormat() {
        final Bytes key = key("restoreEmptyHeadersKey");
        final byte[] val = value("restoreEmptyHeadersValue");

        store.restoreBatch(List.of(changelogRecord(key, val, 1000L, new RecordHeaders())));

        final VersionedRecord<byte[]> record = store.get(key);
        assertNotNull(record);
        assertArrayEquals(val, record.value());
        assertFalse(record.headers().iterator().hasNext());
    }

    @Test
    public void shouldDecodeMultiVersionedQueryResults() {
        final Bytes key = key("multiVersionKey");
        final RecordHeaders headers1 = new RecordHeaders();
        headers1.add(new RecordHeader("version", value("1")));
        final RecordHeaders headers2 = new RecordHeaders();
        headers2.add(new RecordHeader("version", value("2")));

        store.put(key, value("value1"), 1000L, headers1);
        store.put(key, value("value2"), 2000L, headers2);

        try (final VersionedRecordIterator<byte[]> iterator = store.get(key, 0L, 3000L, ResultOrder.DESCENDING)) {
            final VersionedRecord<byte[]> latest = iterator.next();
            assertArrayEquals(value("value2"), latest.value());
            assertEquals("2", new String(latest.headers().lastHeader("version").value(), StandardCharsets.UTF_8));

            final VersionedRecord<byte[]> older = iterator.next();
            assertArrayEquals(value("value1"), older.value());
            assertEquals("1", new String(older.headers().lastHeader("version").value(), StandardCharsets.UTF_8));

            assertFalse(iterator.hasNext());
        }
    }

    private static ConsumerRecord<byte[], byte[]> changelogRecord(final Bytes key,
                                                                  final byte[] value,
                                                                  final long timestamp,
                                                                  final Headers headers) {
        return new ConsumerRecord<>(
            "test-store-changelog",
            0,
            0L,
            timestamp,
            TimestampType.CREATE_TIME,
            key.get().length,
            value == null ? ConsumerRecord.NULL_SIZE : value.length,
            key.get(),
            value,
            headers,
            Optional.empty()
        );
    }
}
