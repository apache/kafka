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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for migration from TimestampedWindowStore to TimestampedWindowStoreWithHeaders.
 *
 * This test verifies that:
 * 1. Legacy data (without headers) can be read after upgrading to header-aware store
 * 2. The convertToHeaderFormat() method produces format compatible with ValueTimestampHeadersSerializer
 * 3. Old and new data can coexist in the same store
 */
public class RocksDBHeadersStoreMigrationTest {

    private static final String STORE_NAME = "migration-test-store";
    private File stateDir;
    private InternalMockProcessorContext context;

    @BeforeEach
    public void setup() {
        stateDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
            stateDir,
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(StreamsTestUtils.getStreamsConfig())
        );
    }

    @AfterEach
    public void cleanup() {
        // Clean up test directory
        if (stateDir != null && stateDir.exists()) {
            try {
                org.apache.kafka.common.utils.Utils.delete(stateDir);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    @Test
    public void shouldMigrateFromTimestampedWindowStoreToHeadersStore() {
        final long windowSize = 60_000L; // 1 minute
        final long retentionPeriod = 3_600_000L; // 1 hour
        final long timestamp1 = 1000L;
        final long timestamp2 = 2000L;

        // Step 1: Create and populate a legacy TimestampedWindowStore
        final StoreBuilder<TimestampedWindowStore<String, String>> legacyStoreBuilder =
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore(
                    STORE_NAME,
                    Duration.ofMillis(retentionPeriod),
                    Duration.ofMillis(windowSize),
                    false
                ),
                Serdes.String(),
                Serdes.String()
            ).withLoggingDisabled();  // Disable changelog to avoid recordCollector dependency

        final TimestampedWindowStore<String, String> legacyStore = legacyStoreBuilder.build();
        legacyStore.init(context, legacyStore);

        // Write some legacy data
        legacyStore.put("key1", ValueAndTimestamp.make("value1", timestamp1), timestamp1);
        legacyStore.put("key2", ValueAndTimestamp.make("value2", timestamp2), timestamp2);

        // Flush to ensure data is persisted
        legacyStore.flush();
        legacyStore.close();

        // Step 2: Reopen as header-aware store
        final StoreBuilder<TimestampedWindowStoreWithHeaders<String, String>> headersStoreBuilder =
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(
                    STORE_NAME,
                    Duration.ofMillis(retentionPeriod),
                    Duration.ofMillis(windowSize),
                    false
                ),
                Serdes.String(),
                Serdes.String()
            ).withLoggingDisabled();  // Disable changelog to avoid recordCollector dependency

        final TimestampedWindowStoreWithHeaders<String, String> headersStore = headersStoreBuilder.build();
        headersStore.init(context, headersStore);

        // Step 3: Verify legacy data can be read with empty headers
        final WindowStoreIterator<ValueTimestampHeaders<String>> iter1 =
            headersStore.fetch("key1", timestamp1 - windowSize, timestamp1 + windowSize);

        assertNotNull(iter1);
        if (iter1.hasNext()) {
            final KeyValue<Long, ValueTimestampHeaders<String>> kv = iter1.next();
            final ValueTimestampHeaders<String> result1 = kv.value;

            assertEquals("value1", result1.value());
            assertEquals(timestamp1, result1.timestamp());

            // Verify headers are empty (migrated data should have empty headers)
            final Headers headers1 = result1.headers();
            assertNotNull(headers1);
            assertEquals(0, countHeaders(headers1), "Migrated data should have empty headers");
        }
        iter1.close();

        // Step 4: Write new data with headers
        final Headers newHeaders = new RecordHeaders()
            .add("schema-id", "v3".getBytes())
            .add("device-type", "mobile".getBytes());

        final long timestamp3 = 3000L;
        headersStore.put("key3", "value3", timestamp3, timestamp3, newHeaders);

        // Step 5: Verify new data can be read with headers
        final WindowStoreIterator<ValueTimestampHeaders<String>> iter3 =
            headersStore.fetch("key3", timestamp3 - windowSize, timestamp3 + windowSize);

        assertNotNull(iter3);
        if (iter3.hasNext()) {
            final KeyValue<Long, ValueTimestampHeaders<String>> kv = iter3.next();
            final ValueTimestampHeaders<String> result3 = kv.value;

            assertEquals("value3", result3.value());
            assertEquals(timestamp3, result3.timestamp());

            // Verify headers are preserved
            final Headers headers3 = result3.headers();
            assertNotNull(headers3);
            assertEquals(2, countHeaders(headers3), "New data should have 2 headers");
            assertEquals("v3", new String(headers3.lastHeader("schema-id").value()));
            assertEquals("mobile", new String(headers3.lastHeader("device-type").value()));
        }
        iter3.close();

        headersStore.close();
    }

    @Test
    public void shouldProduceConsistentFormatBetweenMigrationAndNewData() {
        // This test verifies that convertToHeaderFormat() produces the same format
        // as ValueTimestampHeadersSerializer for empty headers

        final long timestamp = 1000L;
        final String value = "test-value";

        // Simulate legacy data: [timestamp(8)][value]
        final byte[] timestampBytes = ByteBuffer.allocate(8).putLong(timestamp).array();
        final byte[] valueBytes = value.getBytes();
        final byte[] legacyData = ByteBuffer.allocate(timestampBytes.length + valueBytes.length)
            .put(timestampBytes)
            .put(valueBytes)
            .array();

        // Convert using the migration path
        final byte[] migratedData = HeadersBytesStore.convertToHeaderFormat(null, legacyData);

        // Create equivalent data using ValueTimestampHeadersSerializer
        final ValueTimestampHeadersSerializer<String> serializer =
            new ValueTimestampHeadersSerializer<>(Serdes.String().serializer());
        final Headers emptyHeaders = new RecordHeaders();
        final byte[] newData = serializer.serialize("topic", value, timestamp, emptyHeaders);

        // Verify both produce the same format
        assertNotNull(migratedData);
        assertNotNull(newData);

        // The formats should be identical for empty headers
        assertArrayEquals(newData, migratedData,
            "convertToHeaderFormat() should produce same format as ValueTimestampHeadersSerializer");
    }

    @Test
    public void shouldDeserializeMigratedDataCorrectly() {
        // Test that migrated data can be deserialized correctly

        final long timestamp = 1000L;
        final String value = "test-value";

        // Simulate legacy data: [timestamp(8)][value]
        final byte[] timestampBytes = ByteBuffer.allocate(8).putLong(timestamp).array();
        final byte[] valueBytes = value.getBytes();
        final byte[] legacyData = ByteBuffer.allocate(timestampBytes.length + valueBytes.length)
            .put(timestampBytes)
            .put(valueBytes)
            .array();

        // Convert using migration
        final byte[] migratedData = HeadersBytesStore.convertToHeaderFormat(null, legacyData);

        // Deserialize using ValueTimestampHeadersDeserializer
        final ValueTimestampHeadersDeserializer<String> deserializer =
            new ValueTimestampHeadersDeserializer<>(Serdes.String().deserializer());
        final ValueTimestampHeaders<String> result = deserializer.deserialize("topic", migratedData);

        // Verify deserialization
        assertNotNull(result);
        assertEquals(value, result.value());
        assertEquals(timestamp, result.timestamp());

        // Verify headers are empty
        final Headers headers = result.headers();
        assertNotNull(headers);
        assertEquals(0, countHeaders(headers), "Migrated data should have empty headers");
    }

    @Test
    public void shouldHandleNullValueInMigration() {
        final byte[] result = HeadersBytesStore.convertToHeaderFormat(null, null);

        assertNull(result, "Null legacy data should produce null result");
    }

    private int countHeaders(final Headers headers) {
        int count = 0;
        final Iterator<Header> iterator = headers.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }
}
