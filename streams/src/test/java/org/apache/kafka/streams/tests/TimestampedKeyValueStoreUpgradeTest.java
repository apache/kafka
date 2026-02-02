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
package org.apache.kafka.streams.tests;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for upgrading from TimestampedKeyValueStore to TimestampedKeyValueStoreWithHeaders.
 * This test validates the upgrade path described in KIP-1271.
 *
 * The upgrade should allow:
 * 1. Reading legacy data (written without headers) with empty headers
 * 2. Writing new data with headers
 * 3. Both legacy and new data coexisting in the same store
 */
public class TimestampedKeyValueStoreUpgradeTest {

    @TempDir
    File stateDir;

    @Test
    public void shouldUpgradeFromTimestampedStoreToTimestampedStoreWithHeaders() throws Exception {
        final String storeName = "upgrade-test-store";
        final long timestamp1 = 1000L;
        final long timestamp2 = 2000L;
        final long timestamp3 = 3000L;
        final File tempStateDir = new File(stateDir, "backup");

        // Step 1: Create a legacy TimestampedKeyValueStore and write data
        TopologyTestDriver legacyDriver = null;
        try {
            final StoreBuilder<TimestampedKeyValueStore<String, String>> legacyStoreBuilder =
                    Stores.timestampedKeyValueStoreBuilder(
                            Stores.persistentTimestampedKeyValueStore(storeName),
                            Serdes.String(),
                            Serdes.String()
                    );

            final Topology topology = new Topology();
            topology.addSource("source", "input-topic")
                    .addProcessor("processor", () -> new Processor<Object, Object, Object, Object>() {
                        @Override
                        public void process(Record<Object, Object> record) {
                        }
                    }, "source")
                    .addStateStore(legacyStoreBuilder, "processor");

            final Properties props = createStreamsConfig();

            legacyDriver = new TopologyTestDriver(topology, props);
            final KeyValueStore<String, ValueAndTimestamp<String>> legacyStore =
                    legacyDriver.getTimestampedKeyValueStore(storeName);

            assertNotNull(legacyStore, "Legacy store should not be null");

            // Write legacy data without headers
            legacyStore.put("key1", ValueAndTimestamp.make("value1", timestamp1));
            legacyStore.put("key2", ValueAndTimestamp.make("value2", timestamp2));

            // Verify legacy data can be read
            final ValueAndTimestamp<String> result1 = legacyStore.get("key1");
            assertNotNull(result1);
            assertEquals("value1", result1.value());
            assertEquals(timestamp1, result1.timestamp());

            // Flush the store to ensure data is written to disk
            legacyStore.flush();

            // Copy the state directory BEFORE closing the driver
            copyDirectory(new File(stateDir, "upgrade-test-app"), tempStateDir);
        } finally {
            if (legacyDriver != null) {
                legacyDriver.close();
            }
        }

        // Step 2: Reopen the same store as TimestampedKeyValueStoreWithHeaders
        {
            // Restore the state directory from backup
            copyDirectory(tempStateDir, new File(stateDir, "upgrade-test-app"));

            final StoreBuilder<TimestampedKeyValueStoreWithHeaders<String, String>> headerStoreBuilder =
                    Stores.timestampedKeyValueStoreBuilderWithHeaders(
                            Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                            Serdes.String(),
                            Serdes.String()
                    );

            final Topology topology = new Topology();
            topology.addSource("source", "input-topic")
                    .addProcessor("processor", () -> new Processor<Object, Object, Object, Object>() {
                        @Override
                        public void process(Record<Object, Object> record) {
                        }
                    }, "source")
                    .addStateStore(headerStoreBuilder, "processor");

            final Properties props = createStreamsConfig();

            try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
                final KeyValueStore<String, ValueTimestampHeaders<String>> headerStore =
                        driver.getTimestampedKeyValueStoreWithHeaders(storeName);

                assertNotNull(headerStore, "Header store should not be null");

                // Step 3: Verify legacy data can be read with empty headers
                final ValueTimestampHeaders<String> legacyResult1 = headerStore.get("key1");
                assertNotNull(legacyResult1, "Legacy key1 should be readable");
                assertEquals("value1", legacyResult1.value());
                assertEquals(timestamp1, legacyResult1.timestamp());
                assertNotNull(legacyResult1.headers(), "Headers should not be null");
                assertEquals(0, legacyResult1.headers().toArray().length, "Legacy data should have empty headers");

                final ValueTimestampHeaders<String> legacyResult2 = headerStore.get("key2");
                assertNotNull(legacyResult2, "Legacy key2 should be readable");
                assertEquals("value2", legacyResult2.value());
                assertEquals(timestamp2, legacyResult2.timestamp());
                assertEquals(0, legacyResult2.headers().toArray().length, "Legacy data should have empty headers");

                // Step 4: Write new data with headers
                final RecordHeaders headers3 = new RecordHeaders();
                headers3.add("source", "upgrade-test".getBytes());
                headers3.add("version", "2.0".getBytes());

                headerStore.put(
                        "key3",
                        ValueTimestampHeaders.make("value3", timestamp3, headers3)
                );

                // Step 5: Verify new data can be read with headers
                final ValueTimestampHeaders<String> newResult = headerStore.get("key3");
                assertNotNull(newResult, "New key3 should be readable");
                assertEquals("value3", newResult.value());
                assertEquals(timestamp3, newResult.timestamp());
                assertNotNull(newResult.headers());
                assertEquals(2, newResult.headers().toArray().length, "New data should have 2 headers");
                assertEquals("upgrade-test", new String(newResult.headers().lastHeader("source").value()));
                assertEquals("2.0", new String(newResult.headers().lastHeader("version").value()));

                // Step 6: Verify we can update legacy data with headers
                final RecordHeaders headers1Updated = new RecordHeaders();
                headers1Updated.add("updated", "true".getBytes());

                headerStore.put(
                        "key1",
                        ValueTimestampHeaders.make("value1-updated", timestamp1 + 100, headers1Updated)
                );

                final ValueTimestampHeaders<String> updatedResult = headerStore.get("key1");
                assertNotNull(updatedResult);
                assertEquals("value1-updated", updatedResult.value());
                assertEquals(timestamp1 + 100, updatedResult.timestamp());
                assertEquals(1, updatedResult.headers().toArray().length);
                assertEquals("true", new String(updatedResult.headers().lastHeader("updated").value()));

                // Step 7: Verify all keys are present (legacy and new)
                assertNotNull(headerStore.get("key1"), "key1 should still exist");
                assertNotNull(headerStore.get("key2"), "key2 should still exist");
                assertNotNull(headerStore.get("key3"), "key3 should exist");
            }
        }
    }

    private Properties createStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upgrade-test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    private void copyDirectory(final File source, final File target) throws IOException {
        if (!source.exists()) {
            return;
        }
        if (!target.exists()) {
            target.mkdirs();
        }
        try (Stream<Path> paths = Files.walk(source.toPath())) {
            paths.forEach(sourcePath -> {
                try {
                    final Path targetPath = target.toPath().resolve(source.toPath().relativize(sourcePath));
                    if (Files.isDirectory(sourcePath)) {
                        if (!Files.exists(targetPath)) {
                            Files.createDirectories(targetPath);
                        }
                    } else {
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}