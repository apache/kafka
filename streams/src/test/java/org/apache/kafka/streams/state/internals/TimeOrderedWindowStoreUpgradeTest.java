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
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.delete;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests upgrade path from time-ordered window store (without headers) to
 * time-ordered window store with headers using direct supplier creation.
 * <p>
 * This test validates lazy migration from ts-CF to headers-CF and ensures
 * the store can read old data after upgrade.
 */
public class TimeOrderedWindowStoreUpgradeTest {

    private static final String STORE_NAME = "time-ordered-window-store";
    private static final long WINDOW_SIZE_MS = 1000L;
    private static final long RETENTION_MS = Duration.ofMinutes(5).toMillis();
    private static final long SEGMENT_INTERVAL_MS = Duration.ofMinutes(1).toMillis();

    private InternalMockProcessorContext<Bytes, byte[]> context;
    private File baseDir;

    @BeforeEach
    public void setUp() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        baseDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
            baseDir,
            Serdes.Bytes(),
            Serdes.ByteArray(),
            new StreamsConfig(props)
        );
    }

    @AfterEach
    public void tearDown() {
        if (baseDir != null) {
            try {
                delete(baseDir);
            } catch (final Exception e) {
                // Ignore
            }
        }
    }

    /**
     * Helper method to serialize value with headers in [headers][value] format.
     * This is used for window stores with headers where the format is:
     * [headersSize(varint)][headersBytes][value]
     */
    private static byte[] serializeValueWithHeaders(final byte[] value, final Headers headers) {
        if (value == null) {
            return null;
        }
        final HeadersSerializer.PreSerializedHeaders preSerializedHeaders = HeadersSerializer.prepareSerialization(headers);
        final byte[] rawHeaders = HeadersSerializer.serialize(
            preSerializedHeaders,
            ByteBuffer.allocate(preSerializedHeaders.requiredBufferSizeForHeaders)
        ).array();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            ByteUtils.writeVarint(rawHeaders.length, out);
            out.write(rawHeaders);
            out.write(value);
            return baos.toByteArray();
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize value with headers", e);
        }
    }

    /**
     * Helper method to extract raw value from [headers][value] format.
     * This is used for window stores with headers where the format is:
     * [headersSize(varint)][headersBytes][value]
     */
    private static byte[] extractRawValue(final byte[] valueWithHeaders) {
        if (valueWithHeaders == null) {
            return null;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(valueWithHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize);
        final byte[] rawValue = new byte[buffer.remaining()];
        buffer.get(rawValue);
        return rawValue;
    }

    /**
     * Helper method to extract headers from [headers][value] format.
     * This is used for window stores with headers where the format is:
     * [headersSize(varint)][headersBytes][value]
     */
    private static Headers extractHeaders(final byte[] valueWithHeaders) {
        if (valueWithHeaders == null) {
            return null;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(valueWithHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        final byte[] headerBytes = new byte[headersSize];
        buffer.get(headerBytes);
        return HeadersDeserializer.deserialize(headerBytes);
    }

    @Test
    public void shouldMigrateFromWithoutHeadersToWithHeaders() {
        final RocksDbIndexedTimeOrderedWindowBytesStoreSupplier oldSupplier =
            new RocksDbIndexedTimeOrderedWindowBytesStoreSupplier(
                STORE_NAME,
                RETENTION_MS,
                SEGMENT_INTERVAL_MS,
                WINDOW_SIZE_MS,
                false,
                true,
                false   // withHeaders = FALSE (old format)
            );

        final WindowStore<Bytes, byte[]> oldStore = oldSupplier.get();
        oldStore.init(context, oldStore);

        // Write data in old format
        final long baseTime = System.currentTimeMillis();
        final Bytes key1 = Bytes.wrap("key1".getBytes());
        final Bytes key2 = Bytes.wrap("key2".getBytes());
        final Bytes key3 = Bytes.wrap("key3".getBytes());

        oldStore.put(key1, "value1".getBytes(), baseTime + 100);
        oldStore.put(key2, "value2".getBytes(), baseTime + 200);
        oldStore.put(key3, "value3".getBytes(), baseTime + 300);

        // Verify old data
        assertEquals("value1", new String(oldStore.fetch(key1, baseTime + 100)));
        assertEquals("value2", new String(oldStore.fetch(key2, baseTime + 200)));
        assertEquals("value3", new String(oldStore.fetch(key3, baseTime + 300)));

        oldStore.close();

        final RocksDbIndexedTimeOrderedWindowBytesStoreSupplier newSupplier =
            new RocksDbIndexedTimeOrderedWindowBytesStoreSupplier(
                STORE_NAME,
                RETENTION_MS,
                SEGMENT_INTERVAL_MS,
                WINDOW_SIZE_MS,
                false,  // retainDuplicates
                true,   // withIndex
                true    // withHeaders = TRUE (new format with headers support)
            );

        final WindowStore<Bytes, byte[]> newStore = newSupplier.get();
        newStore.init(context, newStore);

        // Verify we can read old data (lazy migration)
        byte[] fetch = newStore.fetch(key1, baseTime + 100);
        assertEquals("value1", new String(extractRawValue(fetch)));
        assertEquals(0, extractHeaders(fetch).toArray().length, "Old data should have empty headers after migration");
        fetch = newStore.fetch(key2, baseTime + 200);
        assertEquals("value2", new String(extractRawValue(fetch)));
        assertEquals(0, extractHeaders(fetch).toArray().length, "Old data should have empty headers after migration");
        fetch = newStore.fetch(key3, baseTime + 300);
        assertEquals("value3", new String(extractRawValue(fetch)));
        assertEquals(0, extractHeaders(fetch).toArray().length, "Old data should have empty headers after migration");


        // Write new data (should use headers-CF)
        final Bytes key4 = Bytes.wrap("key4".getBytes());

        // Write key3 with empty headers
        newStore.put(key3, serializeValueWithHeaders("value3-updated".getBytes(), new RecordHeaders()), baseTime + 350);

        // Write key4 with actual headers
        final RecordHeaders headersWithData = new RecordHeaders();
        headersWithData.add("header-key-1", "header-value-1".getBytes());
        headersWithData.add("header-key-2", "header-value-2".getBytes());
        newStore.put(key4, serializeValueWithHeaders("value4".getBytes(), headersWithData), baseTime + 400);

        // Verify new data - raw values
        assertEquals("value3-updated", new String(extractRawValue(newStore.fetch(key3, baseTime + 350))));
        assertEquals("value4", new String(extractRawValue(newStore.fetch(key4, baseTime + 400))));

        // Verify headers for key3 (empty headers)
        final byte[] fetchedKey3Value = newStore.fetch(key3, baseTime + 350);
        final Headers retrievedKey3Headers = extractHeaders(fetchedKey3Value);
        assertEquals(0, retrievedKey3Headers.toArray().length);

        // Verify headers for key4 (headers with data)
        final byte[] fetchedKey4Value = newStore.fetch(key4, baseTime + 400);
        final Headers retrievedKey4Headers = extractHeaders(fetchedKey4Value);
        assertEquals(2, retrievedKey4Headers.toArray().length);
        assertEquals("header-value-1", new String(retrievedKey4Headers.lastHeader("header-key-1").value()));
        assertEquals("header-value-2", new String(retrievedKey4Headers.lastHeader("header-key-2").value()));

        newStore.close();
    }

    @Test
    public void shouldMigrateFromWithIndexToWithIndexAndHeaders() {
        // Test: withIndex=true, withHeaders=false → withIndex=true, withHeaders=true
        final RocksDbIndexedTimeOrderedWindowBytesStoreSupplier oldSupplier =
            new RocksDbIndexedTimeOrderedWindowBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, SEGMENT_INTERVAL_MS, WINDOW_SIZE_MS,
                false, true, false  // withIndex=true, withHeaders=false
            );

        final WindowStore<Bytes, byte[]> oldStore = oldSupplier.get();
        oldStore.init(context, oldStore);

        final long baseTime = System.currentTimeMillis();
        final Bytes key1 = Bytes.wrap("key1".getBytes());

        oldStore.put(key1, "value1".getBytes(), baseTime + 100);
        oldStore.close();

        // Upgrade to headers
        final RocksDbIndexedTimeOrderedWindowBytesStoreSupplier newSupplier =
            new RocksDbIndexedTimeOrderedWindowBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, SEGMENT_INTERVAL_MS, WINDOW_SIZE_MS,
                false, true, true  // withIndex=true, withHeaders=true
            );

        final WindowStore<Bytes, byte[]> newStore = newSupplier.get();
        newStore.init(context, newStore);

        // Verify old data still accessible
        assertEquals("value1", new String(extractRawValue(newStore.fetch(key1, baseTime + 100))));

        newStore.close();
    }

    @Test
    public void shouldMigrateFromWithoutIndexToWithIndexAndHeaders() {
        // Test: withIndex=false, withHeaders=false → withIndex=true, withHeaders=true
        final RocksDbIndexedTimeOrderedWindowBytesStoreSupplier oldSupplier =
            new RocksDbIndexedTimeOrderedWindowBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, SEGMENT_INTERVAL_MS, WINDOW_SIZE_MS,
                false, false, false  // withIndex=false, withHeaders=false
            );

        final WindowStore<Bytes, byte[]> oldStore = oldSupplier.get();
        oldStore.init(context, oldStore);

        final long baseTime = System.currentTimeMillis();
        final Bytes key1 = Bytes.wrap("key1".getBytes());

        oldStore.put(key1, "value1".getBytes(), baseTime + 100);
        oldStore.close();

        // Upgrade to both index and headers
        final RocksDbIndexedTimeOrderedWindowBytesStoreSupplier newSupplier =
            new RocksDbIndexedTimeOrderedWindowBytesStoreSupplier(
                STORE_NAME, RETENTION_MS, SEGMENT_INTERVAL_MS, WINDOW_SIZE_MS,
                false, true, true  // withIndex=true, withHeaders=true
            );

        final WindowStore<Bytes, byte[]> newStore = newSupplier.get();
        newStore.init(context, newStore);

        // Verify old data still accessible
        assertEquals("value1", new String(extractRawValue(newStore.fetch(key1, baseTime + 100))));

        newStore.close();
    }
}
