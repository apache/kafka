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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class VersionedKeyValueStoreWithHeadersIntegrationTest {

    private static final String STORE_NAME = "versioned-headers-store";
    private static final Duration HISTORY_RETENTION = Duration.ofMinutes(10);

    private String inputStream;
    private String outputStream;
    private long baseTimestamp;

    private KafkaStreams kafkaStreams;

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    public TestInfo testInfo;

    @BeforeAll
    public static void before() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void after() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws Exception {
        this.testInfo = testInfo;
        final String testName = safeUniqueTestName(testInfo);
        inputStream = "input-" + testName;
        outputStream = "output-" + testName;
        baseTimestamp = System.currentTimeMillis() - Duration.ofMinutes(5).toMillis();
        CLUSTER.createTopic(inputStream, 1, 1);
        CLUSTER.createTopic(outputStream, 1, 1);
    }

    @AfterEach
    public void tearDown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30));
        }
    }

    @Test
    public void shouldPutAndGetWithHeaders() throws Exception {
        final Topology topology = buildTopology(() -> new VersionedStoreWithHeadersCheckerProcessor(true));
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration());
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, headers1(),
            new KeyValue<>(1, "value1"));

        final List<KeyValue<Integer, Integer>> results =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(), outputStream, 1);

        assertEquals(0, results.get(0).value, "Store content check failed");
    }

    @Test
    public void shouldPutAndGetWithEmptyHeaders() throws Exception {
        final Topology topology = buildTopology(() -> new VersionedStoreWithHeadersCheckerProcessor(true));
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration());
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, emptyHeaders(),
            new KeyValue<>(1, "value1"));

        final List<KeyValue<Integer, Integer>> results =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(), outputStream, 1);

        assertEquals(0, results.get(0).value, "Store content check failed for empty headers");
    }

    @Test
    public void shouldPreserveHeadersAcrossMultipleVersions() throws Exception {
        final Topology topology = buildTopology(() -> new VersionedStoreWithHeadersCheckerProcessor(true));
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration());
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        int numRecordsProduced = 0;
        numRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp, headers1(),
            new KeyValue<>(1, "version1"));
        numRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp + 1000, headers2(),
            new KeyValue<>(1, "version2"));

        final List<KeyValue<Integer, Integer>> results =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(), outputStream, numRecordsProduced);

        for (final KeyValue<Integer, Integer> result : results) {
            assertEquals(0, result.value, "Store content check failed");
        }
    }

    @Test
    public void shouldRestoreHeadersFromChangelog() throws Exception {
        final Properties config = streamsConfiguration();
        // Use a fixed state dir so we can restart and restore
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

        Topology topology = buildTopology(() -> new VersionedStoreWithHeadersCheckerProcessor(true));
        kafkaStreams = new KafkaStreams(topology, config);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, headers1(),
            new KeyValue<>(1, "restored-value"));

        final List<KeyValue<Integer, Integer>> firstRunResults =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(), outputStream, 1);
        assertEquals(0, firstRunResults.get(0).value, "First run store check failed");

        kafkaStreams.close(Duration.ofSeconds(30));
        kafkaStreams.cleanUp();

        topology = buildTopology(() -> {
            final VersionedStoreWithHeadersCheckerProcessor processor =
                new VersionedStoreWithHeadersCheckerProcessor(false);
            processor.seedExpectedData(1, "restored-value", baseTimestamp, headers1());
            return processor;
        });
        kafkaStreams = new KafkaStreams(topology, config);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        produceDataToTopicWithHeaders(inputStream, baseTimestamp + 5000, emptyHeaders(),
            new KeyValue<>(99, "trigger"));

        final List<KeyValue<Integer, Integer>> secondRunResults =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig(), outputStream, 2);
        assertEquals(0, secondRunResults.get(1).value, "Restoration check failed: headers not preserved");
    }

    private Topology buildTopology(
            final ProcessorSupplier<Integer, String, Integer, Integer> processorSupplier) {
        final var supplier = Stores.persistentVersionedKeyValueStoreWithHeaders(
            STORE_NAME, HISTORY_RETENTION);

        final Topology topology = new Topology();
        topology.addSource("source", new IntegerDeserializer(),
            Serdes.String().deserializer(), inputStream);
        topology.addProcessor("processor", processorSupplier, "source");
        topology.addStateStore(
            Stores.versionedKeyValueStoreBuilderWithHeaders(
                supplier, Serdes.Integer(), Serdes.String()),
            "processor");
        topology.addSink("sink", outputStream, new IntegerSerializer(),
            new IntegerSerializer(), "processor");
        return topology;
    }

    private Properties streamsConfiguration() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
            "versioned-headers-it-" + safeUniqueTestName(testInfo));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        return config;
    }

    private Properties consumerConfig() {
        return TestUtils.consumerConfig(CLUSTER.bootstrapServers(), IntegerDeserializer.class, IntegerDeserializer.class);
    }

    private static Headers headers1() {
        return new RecordHeaders()
            .add("source", "test".getBytes())
            .add("version", "1.0".getBytes());
    }

    private static Headers headers2() {
        return new RecordHeaders()
            .add("source", "test".getBytes())
            .add("version", "2.0".getBytes());
    }

    private static Headers emptyHeaders() {
        return new RecordHeaders();
    }

    @SuppressWarnings("varargs")
    @SafeVarargs
    private final int produceDataToTopicWithHeaders(final String topic,
                                                    final long timestamp,
                                                    final Headers headers,
                                                    final KeyValue<Integer, String>... keyValues) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            topic,
            Arrays.asList(keyValues),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class),
            headers,
            timestamp,
            false);
        return keyValues.length;
    }

    /**
     * Processor that stores records in a VersionedKeyValueStoreWithHeaders and validates
     * that the store contents match expectations. Forwards the number of failed checks
     * as the output value.
     */
    private static class VersionedStoreWithHeadersCheckerProcessor
            implements Processor<Integer, String, Integer, Integer> {

        private ProcessorContext<Integer, Integer> context;
        private VersionedKeyValueStoreWithHeaders<Integer, String> store;

        private final boolean writeToStore;
        private final Map<Integer, Optional<VersionedRecordExpectation>> data;

        VersionedStoreWithHeadersCheckerProcessor(final boolean writeToStore) {
            this.writeToStore = writeToStore;
            this.data = new HashMap<>();
        }

        void seedExpectedData(final int key, final String value,
                              final long timestamp, final Headers headers) {
            data.put(key, Optional.of(new VersionedRecordExpectation(value, timestamp, headers)));
        }

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            if (writeToStore) {
                store.put(record.key(), record.value(), record.timestamp(), record.headers());
                data.put(record.key(), Optional.of(
                    new VersionedRecordExpectation(record.value(), record.timestamp(), record.headers())));
            }

            final int failedChecks = checkStoreContents();
            context.forward(record.withValue(failedChecks));
        }

        private int checkStoreContents() {
            int failedChecks = 0;
            for (final Map.Entry<Integer, Optional<VersionedRecordExpectation>> entry : data.entrySet()) {
                final Integer key = entry.getKey();
                final VersionedRecordExpectation expected = entry.getValue().orElse(null);
                if (expected == null) {
                    continue;
                }

                final VersionedRecord<String> actual = store.get(key);
                if (actual == null) {
                    failedChecks++;
                    continue;
                }

                if (!Objects.equals(actual.value(), expected.value)
                        || actual.timestamp() != expected.timestamp) {
                    failedChecks++;
                    continue;
                }

                final Headers actualHeaders = actual.headers();
                final Headers expectedHeaders = expected.headers;
                if (!headersEqual(actualHeaders, expectedHeaders)) {
                    failedChecks++;
                }
            }
            return failedChecks;
        }

        private static boolean headersEqual(final Headers a, final Headers b) {
            if (a == b) return true;
            if (a == null || b == null) return false;

            final var iterA = a.iterator();
            final var iterB = b.iterator();
            while (iterA.hasNext() && iterB.hasNext()) {
                final var hA = iterA.next();
                final var hB = iterB.next();
                if (!Objects.equals(hA.key(), hB.key())) return false;
                if (!java.util.Arrays.equals(hA.value(), hB.value())) return false;
            }
            return !iterA.hasNext() && !iterB.hasNext();
        }
    }

    private static class VersionedRecordExpectation {
        final String value;
        final long timestamp;
        final Headers headers;

        VersionedRecordExpectation(final String value, final long timestamp, final Headers headers) {
            this.value = value;
            this.timestamp = timestamp;
            this.headers = headers;
        }
    }
}
