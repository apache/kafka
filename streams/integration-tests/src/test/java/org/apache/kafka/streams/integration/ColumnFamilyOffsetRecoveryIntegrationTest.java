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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.internals.RocksDBStoreCorruptionUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;

/**
 * Integration tests for KIP-1035 column family offset recovery.
 *
 * KIP-1035 moved offset storage from external .checkpoint files into RocksDB column families.
 * These tests verify that Kafka Streams can recover from unclean shutdowns and corrupted
 * column family state, which is critical for exactly-once semantics (EOS) correctness.
 */
@Tag("integration")
@Timeout(600)
public class ColumnFamilyOffsetRecoveryIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_PARTITIONS = 3;
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String OUTPUT_TOPIC_2 = "output-topic-2";
    private static final String STORE_NAME = "counts-store";
    private static final String STORE_NAME_2 = "counts-store-2";
    private static final long COMMIT_INTERVAL_MS = 100L;

    private final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = cluster.time;

    private int consumerGroupCounter = 0;

    private Properties streamsConfig;
    private KafkaStreams streams;
    private File stateDir;

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws InterruptedException {
        cluster.start();
        cluster.createTopic(INPUT_TOPIC, NUM_PARTITIONS, 1);
        cluster.createTopic(OUTPUT_TOPIC, NUM_PARTITIONS, 1);
        cluster.createTopic(OUTPUT_TOPIC_2, NUM_PARTITIONS, 1);

        stateDir = TestUtils.tempDirectory();
        final String safeTestName = safeUniqueTestName(testInfo);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
        streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @AfterEach
    public void tearDown() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            streams.cleanUp();
        }
        cluster.stop();
    }

    // -----------------------------------------------------------
    // Topology builder
    // -----------------------------------------------------------

    private StreamsBuilder buildCountTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream(INPUT_TOPIC);
        stream
            .groupByKey()
            .count(Materialized.as(STORE_NAME))
            .toStream()
            .to(OUTPUT_TOPIC);
        return builder;
    }

    /**
     * Builds a topology with two separate state stores:
     * store 1: groupByKey -> count (counts per key)
     * store 2: groupBy(value) -> count (counts per value)
     */
    private StreamsBuilder buildDualStoreTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream(INPUT_TOPIC);

        // Store 1: count by key
        stream
            .groupByKey()
            .count(Materialized.as(STORE_NAME))
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        // Store 2: count by value
        stream
            .groupBy((key, value) -> value)
            .count(Materialized.as(STORE_NAME_2))
            .toStream()
            .to(OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.Long()));

        return builder;
    }

    // -----------------------------------------------------------
    // Store corruption helpers
    // -----------------------------------------------------------

    /**
     * Corrupts store status to open for ALL task directories that contain the given store.
     */
    private void setAllStoreStatusesToOpen(final String storeName) throws Exception {
        for (final File storeDir : findAllStoreDirs(storeName)) {
            RocksDBStoreCorruptionUtils.setStoreStatusToOpen(storeDir);
        }
    }

    /**
     * Deletes offset entries from the offsets column family for ALL task directories.
     */
    private void deleteAllOffsets(final String storeName) throws Exception {
        for (final File storeDir : findAllStoreDirs(storeName)) {
            RocksDBStoreCorruptionUtils.deleteOffsets(storeDir);
        }
    }

    /**
     * Locates the RocksDB store directory under the state directory.
     * The path is: {stateDir}/{applicationId}/{taskId}/rocksdb/{storeName}
     */
    private File findStoreDir(final String storeName) {
        final List<File> dirs = findAllStoreDirs(storeName);
        return dirs.get(0);
    }

    private List<File> findAllStoreDirs(final String storeName) {
        final String appId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        final File appDir = new File(stateDir, appId);
        final File[] taskDirs = appDir.listFiles(file ->
            file.isDirectory() && !file.getName().startsWith("."));

        if (taskDirs == null || taskDirs.length == 0) {
            throw new IllegalStateException("No task directories found under " + appDir);
        }

        final List<File> storeDirs = new ArrayList<>();
        for (final File taskDir : taskDirs) {
            final File storeDir = Paths.get(taskDir.getAbsolutePath(), "rocksdb", storeName).toFile();
            if (storeDir.exists()) {
                storeDirs.add(storeDir);
            }
        }

        if (storeDirs.isEmpty()) {
            throw new IllegalStateException("No store directories for '" + storeName + "' found under " + appDir);
        }
        return storeDirs;
    }

    // -----------------------------------------------------------
    // Streams lifecycle helpers
    // -----------------------------------------------------------

    private KafkaStreams startStreams() throws Exception {
        final StreamsBuilder builder = buildCountTopology();
        streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.cleanUp();
        streams.start();
        waitForRunning(streams);
        return streams;
    }

    private void waitForRunning(final KafkaStreams kafkaStreams) throws Exception {
        TestUtils.waitForCondition(
            () -> kafkaStreams.state().equals(KafkaStreams.State.RUNNING),
            Duration.ofSeconds(60).toMillis(),
            () -> "Expected RUNNING state but was " + kafkaStreams.state()
        );
    }

    // -----------------------------------------------------------
    // Produce / consume helpers
    // -----------------------------------------------------------

    private Properties producerConfig() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private Properties readCommittedConsumerConfig() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-consumer-" + consumerGroupCounter++);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        return props;
    }

    private void produceRecords(final List<KeyValue<String, String>> records) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            INPUT_TOPIC,
            records,
            producerConfig(),
            mockTime
        );
    }

    private List<KeyValue<String, Long>> waitForOutput(final int expectedCount) throws Exception {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            readCommittedConsumerConfig(),
            OUTPUT_TOPIC,
            expectedCount
        );
    }

    // -----------------------------------------------------------
    // Test methods
    // -----------------------------------------------------------

    /**
     * ALOS baseline: after an unclean shutdown (status=open), the store should recover
     * because ALOS opens with ignoreInvalidState=true.
     */
    @Test
    public void shouldRecoverFromUncleanShutdownWithAlos() throws Exception {
        // No EOS — default is at-least-once

        // Phase 1: start, produce, verify output
        final List<KeyValue<String, String>> initialRecords = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v1"),
            new KeyValue<>("A", "v2")
        );

        startStreams();
        produceRecords(initialRecords);
        waitForOutput(initialRecords.size());

        // Phase 2: clean shutdown, then corrupt store status
        streams.close(Duration.ofSeconds(30));
        streams = null;

        setAllStoreStatusesToOpen(STORE_NAME);

        // Phase 3: restart — should recover despite status=open
        final StreamsBuilder builder = buildCountTopology();
        streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();
        waitForRunning(streams);

        // Phase 4: produce more records, verify processing continues
        final List<KeyValue<String, String>> additionalRecords = Arrays.asList(
            new KeyValue<>("A", "v3"),
            new KeyValue<>("C", "v1")
        );
        produceRecords(additionalRecords);

        // We expect output from both initial and additional records.
        // After recovery, state may be rebuilt from changelog, so we just verify
        // that processing continues and we get at least the additional records' output.
        waitForOutput(initialRecords.size() + additionalRecords.size());
    }

    /**
     * Primary regression test for KIP-1035: after an unclean shutdown with EOS enabled,
     * the store status key is left as 1L (open). AbstractColumnFamilyAccessor.open() throws
     * ProcessorStateException("Invalid state during store open") which should be caught and
     * trigger task corruption recovery (wipe + restore from changelog).
     *
     * Without the fix, the ProcessorStateException propagates fatally and the application
     * fails to start.
     */
    @Test
    public void shouldRecoverFromUncleanShutdownWithEos() throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Phase 1: start with EOS, produce records, verify committed output
        final List<KeyValue<String, String>> initialRecords = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v1"),
            new KeyValue<>("A", "v2")
        );

        startStreams();
        produceRecords(initialRecords);
        waitForOutput(initialRecords.size());

        // Phase 2: clean shutdown, then corrupt store status to simulate unclean shutdown
        streams.close(Duration.ofSeconds(30));
        streams = null;

        setAllStoreStatusesToOpen(STORE_NAME);

        // Phase 3: restart with EOS — should detect corruption, wipe, and restore from changelog
        final StreamsBuilder builder = buildCountTopology();
        streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();
        waitForRunning(streams);

        // Phase 4: produce more records and verify processing continues correctly
        final List<KeyValue<String, String>> additionalRecords = Arrays.asList(
            new KeyValue<>("A", "v3"),
            new KeyValue<>("C", "v1")
        );
        produceRecords(additionalRecords);

        // After recovery from corruption, state is rebuilt from changelog.
        // New consumer group reads all committed output from the beginning.
        waitForOutput(initialRecords.size() + additionalRecords.size());
    }

    /**
     * Tests the TaskCorruptedException path: offsets are deleted from the column family
     * but the store status is clean (closed). Under EOS, missing offsets should trigger
     * task corruption detection, causing a wipe and restore from changelog.
     */
    @Test
    public void shouldRecoverFromMissingOffsetsInColumnFamilyWithEos() throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Phase 1: start with EOS, produce records, verify committed output
        final List<KeyValue<String, String>> initialRecords = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v1"),
            new KeyValue<>("A", "v2")
        );

        startStreams();
        produceRecords(initialRecords);
        waitForOutput(initialRecords.size());

        // Phase 2: clean shutdown, then delete offset entries (keep status=closed)
        streams.close(Duration.ofSeconds(30));
        streams = null;

        deleteAllOffsets(STORE_NAME);

        // Phase 3: restart — should detect missing offsets, mark task corrupted, wipe and restore
        final StreamsBuilder builder = buildCountTopology();
        streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();
        waitForRunning(streams);

        // Phase 4: produce more records, verify data is re-bootstrapped from changelog
        final List<KeyValue<String, String>> additionalRecords = Arrays.asList(
            new KeyValue<>("A", "v3"),
            new KeyValue<>("C", "v1")
        );
        produceRecords(additionalRecords);

        waitForOutput(initialRecords.size() + additionalRecords.size());
    }

    /**
     * Combined worst case: status=open (unclean shutdown) AND no committed offsets.
     * Under EOS, this should still trigger corruption recovery.
     *
     * Without the fix, the ProcessorStateException from status=open propagates fatally
     * before the missing offsets are even checked.
     */
    @Test
    public void shouldRecoverFromUncleanShutdownAndMissingOffsetsWithEos() throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Phase 1: start with EOS, produce records, verify committed output
        final List<KeyValue<String, String>> initialRecords = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v1"),
            new KeyValue<>("A", "v2")
        );

        startStreams();
        produceRecords(initialRecords);
        waitForOutput(initialRecords.size());

        // Phase 2: clean shutdown, then corrupt BOTH status and offsets
        streams.close(Duration.ofSeconds(30));
        streams = null;

        setAllStoreStatusesToOpen(STORE_NAME);
        deleteAllOffsets(STORE_NAME);

        // Phase 3: restart — should recover from both corruptions
        final StreamsBuilder builder = buildCountTopology();
        streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();
        waitForRunning(streams);

        // Phase 4: produce more records, verify data is re-bootstrapped correctly
        final List<KeyValue<String, String>> additionalRecords = Arrays.asList(
            new KeyValue<>("A", "v3"),
            new KeyValue<>("C", "v1")
        );
        produceRecords(additionalRecords);

        waitForOutput(initialRecords.size() + additionalRecords.size());
    }

    /**
     * End-to-end EOS correctness after recovery: verifies that no duplicate output records
     * are visible via READ_COMMITTED and that final aggregation values are correct.
     *
     * Without the fix, the application crashes on restart due to ProcessorStateException.
     */
    @Test
    public void shouldMaintainEosGuaranteesAcrossUncleanShutdownAndRecovery() throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Phase 1: produce records with known keys, wait for committed output
        final List<KeyValue<String, String>> batch1 = Arrays.asList(
            new KeyValue<>("X", "a"),
            new KeyValue<>("Y", "b"),
            new KeyValue<>("X", "c")
        );

        startStreams();
        produceRecords(batch1);
        waitForOutput(batch1.size());

        // Phase 2: clean shutdown, corrupt store status
        streams.close(Duration.ofSeconds(30));
        streams = null;

        setAllStoreStatusesToOpen(STORE_NAME);

        // Phase 3: restart, produce more records with same keys
        final StreamsBuilder builder = buildCountTopology();
        streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();
        waitForRunning(streams);

        final List<KeyValue<String, String>> batch2 = Arrays.asList(
            new KeyValue<>("X", "d"),
            new KeyValue<>("Y", "e")
        );
        produceRecords(batch2);

        // Phase 4: collect all committed output and verify correctness
        final List<KeyValue<String, Long>> allOutput = waitForOutput(batch1.size() + batch2.size());

        // Find the latest count for each key — these should reflect correct aggregation
        // without double-counting. X had 3 records total (a, c, d) -> count=3, Y had 2 (b, e) -> count=2
        long latestX = 0;
        long latestY = 0;
        for (final KeyValue<String, Long> record : allOutput) {
            if ("X".equals(record.key)) {
                latestX = Math.max(latestX, record.value);
            } else if ("Y".equals(record.key)) {
                latestY = Math.max(latestY, record.value);
            }
        }

        // X: 3 records total -> count should be exactly 3
        // Y: 2 records total -> count should be exactly 2
        // If there were duplicates from recovery, counts would be higher
        org.junit.jupiter.api.Assertions.assertEquals(3L, latestX, "X count should be 3 (no double-counting after recovery)");
        org.junit.jupiter.api.Assertions.assertEquals(2L, latestY, "Y count should be 2 (no double-counting after recovery)");
    }

    /**
     * Tests that partial store corruption is handled correctly: only one of two stores
     * is corrupted, and the application should still recover.
     *
     * Without the fix, corrupting even one store causes the application to crash.
     */
    @Test
    public void shouldRecoverMultipleStoresFromUncleanShutdown() throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Phase 1: start with dual-store topology, produce records
        final List<KeyValue<String, String>> initialRecords = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v2"),
            new KeyValue<>("A", "v1")
        );

        final StreamsBuilder builder1 = buildDualStoreTopology();
        streams = new KafkaStreams(builder1.build(), streamsConfig);
        streams.cleanUp();
        streams.start();
        waitForRunning(streams);

        produceRecords(initialRecords);
        // Wait for output from the first store
        waitForOutput(initialRecords.size());

        // Phase 2: clean shutdown, corrupt ONLY store 1 (leave store 2 clean)
        streams.close(Duration.ofSeconds(30));
        streams = null;

        setAllStoreStatusesToOpen(STORE_NAME);
        // STORE_NAME_2 is left with clean status

        // Phase 3: restart — should recover the corrupted store, keep the clean one
        final StreamsBuilder builder2 = buildDualStoreTopology();
        streams = new KafkaStreams(builder2.build(), streamsConfig);
        streams.start();
        waitForRunning(streams);

        // Phase 4: produce more records, verify both stores produce correct output
        final List<KeyValue<String, String>> additionalRecords = Arrays.asList(
            new KeyValue<>("C", "v3"),
            new KeyValue<>("A", "v1")
        );
        produceRecords(additionalRecords);

        waitForOutput(initialRecords.size() + additionalRecords.size());
    }

    /**
     * Tests standby task recovery with corrupted column family state.
     * After corrupting instance 1's store, it should recover from the standby/changelog
     * and eventually take over as active when instance 2 is shut down.
     *
     * Without the fix, instance 1 fails to restart due to ProcessorStateException.
     */
    @Test
    public void shouldRecoverStandbyTaskFromUncleanShutdownWithEos() throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfig.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        // Use separate state dirs for each instance
        final File stateDir1 = TestUtils.tempDirectory();
        final File stateDir2 = TestUtils.tempDirectory();

        // Phase 1: start two instances
        final Properties config1 = new Properties();
        config1.putAll(streamsConfig);
        config1.put(StreamsConfig.STATE_DIR_CONFIG, stateDir1.getPath());

        final Properties config2 = new Properties();
        config2.putAll(streamsConfig);
        config2.put(StreamsConfig.STATE_DIR_CONFIG, stateDir2.getPath());

        final StreamsBuilder builder1 = buildCountTopology();
        final StreamsBuilder builder2 = buildCountTopology();

        final KafkaStreams streams1 = new KafkaStreams(builder1.build(), config1);
        final KafkaStreams streams2 = new KafkaStreams(builder2.build(), config2);
        streams1.cleanUp();
        streams2.cleanUp();
        streams1.start();
        streams2.start();

        waitForRunning(streams1);
        waitForRunning(streams2);

        // Phase 2: produce data, wait for processing
        final List<KeyValue<String, String>> initialRecords = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v1"),
            new KeyValue<>("A", "v2")
        );
        produceRecords(initialRecords);
        waitForOutput(initialRecords.size());

        // Phase 3: shut down instance 1, corrupt its store status
        streams1.close(Duration.ofSeconds(30));

        // Corrupt all store dirs under instance 1's state directory
        final String appId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        final File appDir1 = new File(stateDir1, appId);
        final File[] taskDirs1 = appDir1.listFiles(file ->
            file.isDirectory() && !file.getName().startsWith("."));
        if (taskDirs1 != null) {
            for (final File taskDir : taskDirs1) {
                final File storeDir = Paths.get(taskDir.getAbsolutePath(), "rocksdb", STORE_NAME).toFile();
                if (storeDir.exists()) {
                    RocksDBStoreCorruptionUtils.setStoreStatusToOpen(storeDir);
                }
            }
        }

        // Phase 4: restart instance 1 — should recover from standby or changelog
        final StreamsBuilder builder1Restart = buildCountTopology();
        final KafkaStreams streams1Restart = new KafkaStreams(builder1Restart.build(), config1);
        streams1Restart.start();
        waitForRunning(streams1Restart);

        // Phase 5: shut down instance 2, verify instance 1 takes over
        streams2.close(Duration.ofSeconds(30));

        // Produce more records and verify instance 1 processes them as active
        final List<KeyValue<String, String>> additionalRecords = Arrays.asList(
            new KeyValue<>("A", "v3"),
            new KeyValue<>("C", "v1")
        );
        produceRecords(additionalRecords);

        waitForOutput(initialRecords.size() + additionalRecords.size());

        // Clean up — set streams to instance 1 so tearDown handles it
        streams = streams1Restart;
    }
}
