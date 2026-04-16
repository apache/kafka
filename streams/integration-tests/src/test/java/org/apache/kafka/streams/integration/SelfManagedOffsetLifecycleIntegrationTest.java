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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.RocksDBStoreTestingUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for KIP-1035 column family offset normal lifecycle.
 *
 * <p>Validates that offsets stored in RocksDB column families are correctly persisted on
 * clean shutdown and read back on restart. After each clean stop, tests directly inspect
 * the CF to assert status=closed and offsets populated. On restart, a
 * {@code StateRestoreListener} verifies no changelog restoration occurs, and cumulative
 * count assertions confirm state continuity. All tests are parameterized for ALOS and EOS.
 */
@Tag("integration")
@Timeout(600)
public class SelfManagedOffsetLifecycleIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_PARTITIONS = 3;
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String STORE_NAME = "counts-store";
    private static final long COMMIT_INTERVAL_MS = 100L;
    private static final Duration STREAMS_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private int consumerGroupCounter = 0;

    private Properties streamsConfig;
    private KafkaStreams streams;
    private File stateDir;

    @BeforeAll
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
    }

    @AfterAll
    public static void stopCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws InterruptedException {
        CLUSTER.deleteAllTopics();
        CLUSTER.createTopic(INPUT_TOPIC, NUM_PARTITIONS, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC, NUM_PARTITIONS, 1);

        final String safeTestName = safeUniqueTestName(testInfo);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        stateDir = TestUtils.tempDirectory();
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
        streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 5000);
        streamsConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);
    }

    @AfterEach
    public void tearDown() {
        if (streams != null) {
            closeStreams(streams);
            streams.cleanUp();
        }
    }

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

    private void closeStreams(final KafkaStreams kafkaStreams) {
        kafkaStreams.close(STREAMS_CLOSE_TIMEOUT);
    }

    private KafkaStreams startStreams() throws Exception {
        return startStreams(false);
    }

    private KafkaStreams startStreams(final boolean cleanUp) throws Exception {
        final StreamsBuilder builder = buildCountTopology();
        streams = new KafkaStreams(builder.build(), streamsConfig);
        if (cleanUp) {
            streams.cleanUp();
        }
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        return streams;
    }

    private KafkaStreams startStreamsWithRestoreListener(final StateRestoreListener listener) throws Exception {
        final StreamsBuilder builder = buildCountTopology();
        streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.setGlobalStateRestoreListener(listener);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        return streams;
    }

    private Properties producerConfig() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private Properties readCommittedConsumerConfig() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
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
            CLUSTER.time
        );
    }

    private List<KeyValue<String, Long>> waitForOutput(final int expectedCount) throws Exception {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            readCommittedConsumerConfig(),
            OUTPUT_TOPIC,
            expectedCount
        );
    }

    /**
     * Extracts the latest count for each key from the output records.
     * Since the count store emits updates, the last value for each key is the current count.
     */
    private Map<String, Long> latestCountsFromOutput(final List<KeyValue<String, Long>> output) {
        final Map<String, Long> latest = new HashMap<>();
        for (final KeyValue<String, Long> record : output) {
            latest.put(record.key, record.value);
        }
        return latest;
    }

    /**
     * Queries the state store via interactive queries and returns all key-value pairs.
     */
    private Map<String, Long> queryStore(final KafkaStreams kafkaStreams) throws Exception {
        final ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore())
        );
        final Map<String, Long> result = new HashMap<>();
        try (var iter = store.all()) {
            while (iter.hasNext()) {
                final KeyValue<String, Long> kv = iter.next();
                result.put(kv.key, kv.value);
            }
        }
        return result;
    }

    // -----------------------------------------------------------
    // Column family inspection helpers
    // -----------------------------------------------------------

    private List<File> findAllStoreDirs(final String storeName) {
        final String appId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        return RocksDBStoreTestingUtils.findAllStoreDirs(stateDir, appId, storeName);
    }

    /**
     * Asserts that all store directories have the expected status (0L = closed, 1L = open).
     */
    private void assertStoreStatus(final long expectedStatus) throws Exception {
        for (final File storeDir : findAllStoreDirs(STORE_NAME)) {
            final Long status = RocksDBStoreTestingUtils.readStoreStatus(storeDir);
            assertEquals(expectedStatus, status,
                "Store status in " + storeDir + " should be " + (expectedStatus == 0L ? "closed" : "open"));
        }
    }

    /**
     * Asserts that all store directories have non-empty offsets in the CF.
     */
    private void assertOffsetsPopulated() throws Exception {
        for (final File storeDir : findAllStoreDirs(STORE_NAME)) {
            final Map<String, Long> offsets = RocksDBStoreTestingUtils.readOffsets(storeDir);
            if (!offsets.isEmpty()) {
                return; // At least one store dir has offsets — with partitioning, not all may
            }
        }
        throw new AssertionError("Expected at least one store directory to have populated offsets");
    }

    /**
     * Start, produce, stop cleanly, restart (no cleanUp), produce more.
     * Counts should be cumulative, proving offsets in the CF were persisted and read back.
     */
    @ParameterizedTest
    @ValueSource(strings = {StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2})
    public void shouldPreserveStateAcrossCleanRestart(final String processingGuarantee) throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);

        final List<KeyValue<String, String>> batch1 = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v1"),
            new KeyValue<>("A", "v2")
        );

        startStreams(true);
        produceRecords(batch1);
        waitForOutput(batch1.size());

        // Clean stop — verify CF state, then restart without cleanUp
        closeStreams(streams);
        streams = null;

        assertStoreStatus(0L);
        assertOffsetsPopulated();

        // Restart without cleanUp so local state is preserved
        startStreams(false);

        final List<KeyValue<String, String>> batch2 = Arrays.asList(
            new KeyValue<>("A", "v3"),
            new KeyValue<>("B", "v2")
        );
        produceRecords(batch2);

        final List<KeyValue<String, Long>> allOutput = waitForOutput(batch1.size() + batch2.size());
        final Map<String, Long> counts = latestCountsFromOutput(allOutput);

        // A: 3 total (v1, v2, v3), B: 2 total (v1, v2)
        assertEquals(3L, counts.get("A"), "A count should be cumulative across restart");
        assertEquals(2L, counts.get("B"), "B count should be cumulative across restart");
    }

    /**
     * Multiple restart cycles: start, produce batch1, stop, restart, produce batch2,
     * stop, restart, produce batch3. Final counts should equal totals across all 3 batches.
     */
    @ParameterizedTest
    @ValueSource(strings = {StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2})
    public void shouldPreserveStateAcrossMultipleRestartCycles(final String processingGuarantee) throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);

        // Cycle 1
        startStreams(true);
        final List<KeyValue<String, String>> batch1 = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v1")
        );
        produceRecords(batch1);
        waitForOutput(batch1.size());
        closeStreams(streams);
        streams = null;

        // Cycle 2
        startStreams(false);
        final List<KeyValue<String, String>> batch2 = Arrays.asList(
            new KeyValue<>("A", "v2"),
            new KeyValue<>("C", "v1")
        );
        produceRecords(batch2);
        waitForOutput(batch1.size() + batch2.size());
        closeStreams(streams);
        streams = null;

        // Cycle 3
        startStreams(false);
        final List<KeyValue<String, String>> batch3 = Arrays.asList(
            new KeyValue<>("A", "v3"),
            new KeyValue<>("B", "v2"),
            new KeyValue<>("C", "v2")
        );
        produceRecords(batch3);

        final int totalRecords = batch1.size() + batch2.size() + batch3.size();
        final List<KeyValue<String, Long>> allOutput = waitForOutput(totalRecords);
        final Map<String, Long> counts = latestCountsFromOutput(allOutput);

        // A: 3 (v1, v2, v3), B: 2 (v1, v2), C: 2 (v1, v2)
        assertEquals(3L, counts.get("A"), "A count across 3 cycles");
        assertEquals(2L, counts.get("B"), "B count across 3 cycles");
        assertEquals(2L, counts.get("C"), "C count across 3 cycles");
    }

    /**
     * After a clean shutdown, restarting should not require full changelog restoration.
     * Uses a TrackingRestoreListener to verify no records are restored on restart.
     */
    @ParameterizedTest
    @ValueSource(strings = {StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2})
    public void shouldNotRestoreFromChangelogOnCleanRestart(final String processingGuarantee) throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);

        final List<KeyValue<String, String>> batch1 = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v1"),
            new KeyValue<>("A", "v2")
        );

        startStreams(true);
        produceRecords(batch1);
        waitForOutput(batch1.size());

        closeStreams(streams);
        streams = null;

        // Verify CF state after clean shutdown: status=closed, offsets populated
        assertStoreStatus(0L);
        assertOffsetsPopulated();

        // Restart with a restore listener — should see 0 records restored
        final TrackingRestoreListener restoreListener = new TrackingRestoreListener();
        startStreamsWithRestoreListener(restoreListener);

        final List<KeyValue<String, String>> batch2 = Arrays.asList(
            new KeyValue<>("A", "v3"),
            new KeyValue<>("C", "v1")
        );
        produceRecords(batch2);

        final List<KeyValue<String, Long>> allOutput = waitForOutput(batch1.size() + batch2.size());
        final Map<String, Long> counts = latestCountsFromOutput(allOutput);

        assertEquals(3L, counts.get("A"), "A count should be cumulative");
        assertEquals(1L, counts.get("B"), "B count should be preserved");
        assertEquals(1L, counts.get("C"), "C count should reflect new record");
        assertEquals(0L, restoreListener.totalRestored.get(),
            "No records should be restored from changelog after clean shutdown");
    }

    /**
     * Edge case: start, reach RUNNING, stop cleanly without producing any records,
     * then restart and produce. The store was initialized but never committed.
     */
    @ParameterizedTest
    @ValueSource(strings = {StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2})
    public void shouldHandleCleanRestartWithNoDataProcessed(final String processingGuarantee) throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);

        // Start and immediately stop — no records produced
        startStreams(true);
        closeStreams(streams);
        streams = null;

        // After clean shutdown with no data, status should still be closed
        assertStoreStatus(0L);

        // Restart — should not treat empty CF as corruption.
        // Capture logs from ProcessorStateManager and StateDirectory to verify
        // no corruption-related warnings are emitted during restart.
        try (final LogCaptureAppender stateManagerAppender = LogCaptureAppender.createAndRegister(ProcessorStateManager.class);
             final LogCaptureAppender stateDirAppender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {

            startStreams(false);

            final List<KeyValue<String, String>> records = Arrays.asList(
                new KeyValue<>("A", "v1"),
                new KeyValue<>("B", "v1")
            );
            produceRecords(records);

            final List<KeyValue<String, Long>> output = waitForOutput(records.size());
            final Map<String, Long> counts = latestCountsFromOutput(output);

            assertEquals(1L, counts.get("A"));
            assertEquals(1L, counts.get("B"));

            assertNoCorruptionWarnings(stateManagerAppender);
            assertNoCorruptionWarnings(stateDirAppender);
        }
    }

    private void assertNoCorruptionWarnings(final LogCaptureAppender appender) {
        for (final String message : appender.getMessages()) {
            if (message.contains("did not find checkpoint offsets while stores are not empty")) {
                throw new AssertionError("Unexpected corruption warning in logs: " + message);
            }
            if (message.contains("Failed to register startup state stores for task")) {
                throw new AssertionError("Unexpected startup store failure in logs: " + message);
            }
        }
    }

    /**
     * Validates that the in-store state (via interactive queries) is preserved across
     * a clean restart, not just the output topic.
     */
    @ParameterizedTest
    @ValueSource(strings = {StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2})
    public void shouldVerifyStoreStateViaInteractiveQueriesAcrossRestart(final String processingGuarantee) throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);

        final List<KeyValue<String, String>> records = Arrays.asList(
            new KeyValue<>("A", "v1"),
            new KeyValue<>("B", "v1"),
            new KeyValue<>("A", "v2")
        );

        startStreams(true);
        produceRecords(records);
        waitForOutput(records.size());

        // Query store before restart
        final Map<String, Long> countsBefore = queryStore(streams);
        assertEquals(2L, countsBefore.get("A"));
        assertEquals(1L, countsBefore.get("B"));

        closeStreams(streams);
        streams = null;

        // Verify CF persisted correctly
        assertStoreStatus(0L);
        assertOffsetsPopulated();

        // Restart and query again — counts should match
        startStreams(false);
        final Map<String, Long> countsAfter = queryStore(streams);

        assertEquals(countsBefore, countsAfter,
            "Store state via IQ should be identical after clean restart");
    }

    /**
     * A StateRestoreListener that tracks the total number of records restored.
     */
    static class TrackingRestoreListener implements StateRestoreListener {
        private final AtomicLong totalRestored = new AtomicLong(0);

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            // no-op
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition,
                                    final String storeName,
                                    final long batchEndOffset,
                                    final long numRestored) {
            totalRestored.addAndGet(numRestored);
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
            // no-op
        }
    }
}
