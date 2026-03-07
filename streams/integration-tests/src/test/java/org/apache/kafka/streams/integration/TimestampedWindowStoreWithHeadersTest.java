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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;
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
public class TimestampedWindowStoreWithHeadersTest {

    private static final String STORE_NAME = "headers-window-store";
    private static final long WINDOW_SIZE_MS = 100L;
    private static final long RETENTION_MS = 1000L;

    private String inputStream;
    private String outputStream;
    private long baseTimestamp;

    private KafkaStreams kafkaStreams;

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final Headers HEADERS1 = new RecordHeaders()
        .add("source", "test".getBytes())
        .add("version", "1.0".getBytes());

    private static final Headers HEADERS2 = new RecordHeaders()
        .add("source", "test".getBytes())
        .add("version", "2.0".getBytes());

    private static final Headers EMPTY_HEADERS = new RecordHeaders();

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
    public void beforeTest(final TestInfo testInfo) throws InterruptedException {
        this.testInfo = testInfo;
        final String uniqueTestName = safeUniqueTestName(testInfo);
        inputStream = "input-stream-" + uniqueTestName;
        outputStream = "output-stream-" + uniqueTestName;
        CLUSTER.createTopic(inputStream);
        CLUSTER.createTopic(outputStream);

        baseTimestamp = CLUSTER.time.milliseconds();
    }

    @AfterEach
    public void afterTest() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30L));
            kafkaStreams.cleanUp();
        }
    }

    @Test
    public void shouldPutFetchAndDelete() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedWindowStoreWithHeadersContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce source data with headers
        int numRecordsProduced = 0;

        // Window 1: [baseTimestamp, baseTimestamp + WINDOW_SIZE_MS)
        numRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS1,
            KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));

        // Window 1: updates in same window
        numRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp + 50, HEADERS2,
            KeyValue.pair(1, "a50"), KeyValue.pair(2, null), KeyValue.pair(3, "c50"));

        // Window 2: [baseTimestamp + WINDOW_SIZE_MS, baseTimestamp + 2 * WINDOW_SIZE_MS)
        numRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp + WINDOW_SIZE_MS,
            EMPTY_HEADERS,
            KeyValue.pair(1, "a100"), KeyValue.pair(2, "b100"), KeyValue.pair(3, null));

        final List<KeyValue<Integer, Integer>> receivedRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            numRecordsProduced);

        receivedRecords.forEach(receivedRecord -> assertEquals(0, receivedRecord.value));
    }

    @Test
    public void shouldSetChangelogTopicProperties() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedWindowStoreWithHeadersContentCheckerProcessor(false), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, new RecordHeaders(), KeyValue.pair(0, "foo"));

        IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            1);

        // verify changelog topic properties
        final String changelogTopic = props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-" + STORE_NAME + "-changelog";
        final Properties changelogTopicConfig = CLUSTER.getLogConfig(changelogTopic);
        assertEquals("compact", changelogTopicConfig.getProperty("cleanup.policy"));
    }

    @Test
    public void shouldRestore() throws Exception {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedWindowStoreWithHeadersContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        int initialRecordsProduced = 0;

        initialRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS1,
            KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));

        initialRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp + 50, HEADERS2,
            KeyValue.pair(1, "a50"), KeyValue.pair(2, null), KeyValue.pair(3, "c50"));

        initialRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp + WINDOW_SIZE_MS, EMPTY_HEADERS,
            KeyValue.pair(1, "a100"), KeyValue.pair(2, "b100"), KeyValue.pair(3, "c100"));

        IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            initialRecordsProduced);

        // wipe out state store to trigger restore process on restart
        kafkaStreams.close();
        kafkaStreams.cleanUp();

        // restart app - use processor WITHOUT validation of initial data, just write to store
        streamsBuilder = new StreamsBuilder();

        streamsBuilder.addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedWindowStoreWithHeadersContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce additional records to verify restored store works correctly
        final Headers finalHeaders = new RecordHeaders().add("final", "true".getBytes());
        final int additionalRecordsProduced = produceDataToTopicWithHeaders(inputStream, baseTimestamp + 2 * WINDOW_SIZE_MS, finalHeaders,
            KeyValue.pair(1, "a200"), KeyValue.pair(2, "b200"), KeyValue.pair(3, "c200"));

        final List<KeyValue<Integer, Integer>> receivedRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            initialRecordsProduced + additionalRecordsProduced);

        receivedRecords.forEach(receivedRecord -> assertEquals(0, receivedRecord.value));
    }

    @Test
    public void shouldManualUpgradeFromTimestampedToHeaders() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.addStateStore(
                Stores.timestampedWindowStoreBuilder(
                    Stores.persistentTimestampedWindowStore(STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(TimestampedWindowStoreContentCheckerProcessor::new, STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        shouldManualUpgradeFromTimestampedToHeaders(streamsBuilder.build());
    }

    private void shouldManualUpgradeFromTimestampedToHeaders(final Topology originalTopology) throws Exception {
        // build original timestamped (legacy) topology and start app
        final Properties props = props();
        kafkaStreams = new KafkaStreams(originalTopology, props);
        kafkaStreams.start();

        // produce source data to legacy timestamped store (without headers)
        int initialRecordsProduced = 0;
        initialRecordsProduced += produceDataToTopic(inputStream, baseTimestamp,
            KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));
        initialRecordsProduced += produceDataToTopic(inputStream, baseTimestamp + 50,
            KeyValue.pair(1, "a50"), KeyValue.pair(2, null), KeyValue.pair(3, "c50"));
        initialRecordsProduced += produceDataToTopic(inputStream, baseTimestamp + WINDOW_SIZE_MS,
            KeyValue.pair(1, "a100"), KeyValue.pair(2, "b100"), KeyValue.pair(3, null));

        List<KeyValue<Integer, Integer>> receivedRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            initialRecordsProduced);

        receivedRecords.forEach(receivedRecord -> assertEquals(0, receivedRecord.value));

        kafkaStreams.close();
        kafkaStreams.cleanUp();

        // restart app with headers-aware store to test upgrade path
        // The store should migrate legacy timestamped data (without headers)
        // and add empty headers to existing data
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_SIZE_MS), false),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedWindowStoreWithHeadersContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce additional records with headers to verify upgraded store works
        final Headers upgradedHeaders = new RecordHeaders().add("upgraded", "true".getBytes());
        final int additionalRecordsProduced = produceDataToTopicWithHeaders(inputStream, baseTimestamp + 2 * WINDOW_SIZE_MS, upgradedHeaders,
            KeyValue.pair(1, "a200"), KeyValue.pair(2, "b200"), KeyValue.pair(3, "c200"));

        receivedRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            initialRecordsProduced + additionalRecordsProduced);

        receivedRecords.forEach(receivedRecord -> assertEquals(0, receivedRecord.value));
    }

    private Properties props() {
        final String safeTestName = safeUniqueTestName(testInfo);
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    /**
     * @return number of records produced
     */
    @SuppressWarnings("varargs")
    @SafeVarargs
    private final int produceDataToTopic(final String topic,
                                         final long timestamp,
                                         final KeyValue<Integer, String>... keyValues) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            topic,
            Arrays.asList(keyValues),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class),
            timestamp);
        return keyValues.length;
    }

    /**
     * Produce records with headers.
     *
     * @return number of records produced
     */
    @SuppressWarnings("varargs")
    @SafeVarargs
    private int produceDataToTopicWithHeaders(final String topic,
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
     * Processor for validating expected contents of a timestamped window store with headers, and forwards
     * the number of failed checks downstream for consumption.
     */
    private static class TimestampedWindowStoreWithHeadersContentCheckerProcessor implements Processor<Integer, String, Integer, Integer> {

        private ProcessorContext<Integer, Integer> context;
        private TimestampedWindowStoreWithHeaders<Integer, String> store;

        // whether the processor should write records to the store as they arrive.
        private final boolean writeToStore;
        // in-memory copy of seen data, to validate for testing purposes.
        // Maps key -> windowStartTime -> ValueTimestampHeaders
        private final Map<Integer, Map<Long, Optional<ValueTimestampHeaders<String>>>> data;

        TimestampedWindowStoreWithHeadersContentCheckerProcessor(final boolean writeToStore) {
            this.writeToStore = writeToStore;
            this.data = new HashMap<>();
        }

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            final long windowStartTime = record.timestamp() - (record.timestamp() % WINDOW_SIZE_MS);

            if (writeToStore) {
                final ValueTimestampHeaders<String> valueTimestampHeaders =
                    ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
                store.put(record.key(), valueTimestampHeaders, windowStartTime);

                data.computeIfAbsent(record.key(), k -> new HashMap<>());
                data.get(record.key()).put(windowStartTime, Optional.ofNullable(valueTimestampHeaders));
            }

            //
            final int failedChecks = checkStoreContents();
            context.forward(record.withValue(failedChecks));
        }

        /**
         * Check expected contents of store, and signal completion by writing number of failures to downstream
         * @return number of failed checks
         */
        private int checkStoreContents() {
            int failedChecks = 0;
            for (final Map.Entry<Integer, Map<Long, Optional<ValueTimestampHeaders<String>>>> keyEntry : data.entrySet()) {
                final Integer key = keyEntry.getKey();

                for (final Map.Entry<Long, Optional<ValueTimestampHeaders<String>>> windowEntry : keyEntry.getValue().entrySet()) {
                    final Long windowStartTime = windowEntry.getKey();
                    final ValueTimestampHeaders<String> expectedValueTimestampHeaders =
                        windowEntry.getValue().orElse(null);

                    // validate fetch from store
                    try (final WindowStoreIterator<ValueTimestampHeaders<String>> iterator =
                             store.fetch(key, windowStartTime, windowStartTime)) {
                        final ValueTimestampHeaders<String> actualValueTimestampHeaders =
                            iterator.hasNext() ? iterator.next().value : null;
                        if (!Objects.equals(actualValueTimestampHeaders, expectedValueTimestampHeaders)) {
                            failedChecks++;
                        }
                    }
                }
            }
            return failedChecks;
        }
    }

    /**
     * Processor for validating expected contents of a timestamped window store (without headers).
     * Used for testing the upgrade path from TimestampedWindowStore to TimestampedWindowStoreWithHeaders.
     */
    private static class TimestampedWindowStoreContentCheckerProcessor implements Processor<Integer, String, Integer, Integer> {

        private ProcessorContext<Integer, Integer> context;
        private TimestampedWindowStore<Integer, String> store;

        // in-memory copy of seen data, to validate for testing purposes.
        // Maps key -> windowStartTime -> ValueAndTimestamp
        private final Map<Integer, Map<Long, Optional<ValueAndTimestamp<String>>>> data;

        TimestampedWindowStoreContentCheckerProcessor() {
            this.data = new HashMap<>();
        }

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            final long windowStartTime = record.timestamp() - (record.timestamp() % WINDOW_SIZE_MS);

            final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(record.value(), record.timestamp());
            store.put(record.key(), valueAndTimestamp, windowStartTime);

            data.computeIfAbsent(record.key(), k -> new HashMap<>());
            data.get(record.key()).put(windowStartTime, Optional.ofNullable(valueAndTimestamp));

            final int failedChecks = checkStoreContents();
            context.forward(record.withValue(failedChecks));
        }

        /**
         * Check expected contents of store, and signal completion by writing
         * @return number of failed checks
         */
        private int checkStoreContents() {
            int failedChecks = 0;
            for (final Map.Entry<Integer, Map<Long, Optional<ValueAndTimestamp<String>>>> keyEntry : data.entrySet()) {
                final Integer key = keyEntry.getKey();

                for (final Map.Entry<Long, Optional<ValueAndTimestamp<String>>> windowEntry : keyEntry.getValue().entrySet()) {
                    final Long windowStartTime = windowEntry.getKey();
                    final ValueAndTimestamp<String> expectedValueAndTimestamp = windowEntry.getValue().orElse(null);

                    // validate fetch from store
                    try (final WindowStoreIterator<ValueAndTimestamp<String>> iterator =
                             store.fetch(key, windowStartTime, windowStartTime)) {
                        final ValueAndTimestamp<String> actualValueAndTimestamp =
                            iterator.hasNext() ? iterator.next().value : null;
                        if (!Objects.equals(actualValueAndTimestamp, expectedValueAndTimestamp)) {
                            failedChecks++;
                        }
                    }
                }
            }
            return failedChecks;
        }
    }
}
