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
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
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
public class TimestampedKeyValueStoreWithHeadersTest {

    private static final String STORE_NAME = "headers-store";

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
    public void shouldPutGetAndDelete() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedStoreWithHeadersContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce source data with headers
        int numRecordsProduced = 0;

        numRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS1,
            KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));

        numRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp + 5, HEADERS2,
            KeyValue.pair(1, "a5"), KeyValue.pair(2, null), KeyValue.pair(3, "c5"));

        numRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp + 2,
            EMPTY_HEADERS,
            KeyValue.pair(1, "a2"), KeyValue.pair(2, "b2"), KeyValue.pair(3, null));

        // wait for output and verify
        final List<KeyValue<Integer, Integer>> receivedRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            numRecordsProduced);

        for (final KeyValue<Integer, Integer> receivedRecord : receivedRecords) {
            // verify zero failed checks for each record
            assertEquals(receivedRecord.value, 0);
        }
    }

    @Test
    public void shouldSetChangelogTopicProperties() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedStoreWithHeadersContentCheckerProcessor(false), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce record (and wait for result) to create changelog
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
        assertEquals(changelogTopicConfig.getProperty("cleanup.policy"), "compact");
    }

    @Test
    public void shouldRestore() throws Exception {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedStoreWithHeadersContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce source data with headers
        final Map<Integer, Optional<ValueTimestampHeaders<String>>> expectedData = new HashMap<>();
        int initialRecordsProduced = 0;

        initialRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS1,
            KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));
        expectedData.put(1, Optional.of(ValueTimestampHeaders.make("a0", baseTimestamp, HEADERS1)));
        expectedData.put(2, Optional.of(ValueTimestampHeaders.make("b0", baseTimestamp, HEADERS1)));
        expectedData.put(3, Optional.empty());  // null value

        initialRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp + 5, HEADERS2,
            KeyValue.pair(1, "a5"), KeyValue.pair(2, null), KeyValue.pair(3, "c5"));
        expectedData.put(1, Optional.of(ValueTimestampHeaders.make("a5", baseTimestamp + 5, HEADERS2)));
        expectedData.put(2, Optional.empty());  // null value
        expectedData.put(3, Optional.of(ValueTimestampHeaders.make("c5", baseTimestamp + 5, HEADERS2)));

        initialRecordsProduced += produceDataToTopicWithHeaders(inputStream, baseTimestamp + 10, EMPTY_HEADERS,
            KeyValue.pair(1, "a10"), KeyValue.pair(2, "b10"), KeyValue.pair(3, "c10"));
        expectedData.put(1, Optional.of(ValueTimestampHeaders.make("a10", baseTimestamp + 10, EMPTY_HEADERS)));
        expectedData.put(2, Optional.of(ValueTimestampHeaders.make("b10", baseTimestamp + 10, EMPTY_HEADERS)));
        expectedData.put(3, Optional.of(ValueTimestampHeaders.make("c10", baseTimestamp + 10, EMPTY_HEADERS)));

        // wait for output
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

        streamsBuilder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedStoreWithHeadersContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce additional records to verify restored store works correctly
        final Headers finalHeaders = new RecordHeaders().add("final", "true".getBytes());
        final int additionalRecordsProduced = produceDataToTopicWithHeaders(inputStream, baseTimestamp + 12, finalHeaders,
            KeyValue.pair(1, "a12"), KeyValue.pair(2, "b12"), KeyValue.pair(3, "c12"));

        // wait for output and verify
        final List<KeyValue<Integer, Integer>> receivedRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            initialRecordsProduced + additionalRecordsProduced);

        for (final KeyValue<Integer, Integer> receivedRecord : receivedRecords) {
            // verify zero failed checks for each record
            assertEquals(receivedRecord.value, 0);
        }
    }

    @Test
    public void shouldManualUpgradeFromTimestampedToHeaders() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilder(
                    Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(TimestampedStoreContentCheckerProcessor::new, STORE_NAME)
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
        initialRecordsProduced += produceDataToTopic(inputStream, baseTimestamp + 5,
            KeyValue.pair(1, "a5"), KeyValue.pair(2, null), KeyValue.pair(3, "c5"));
        initialRecordsProduced += produceDataToTopic(inputStream, baseTimestamp + 2,
            KeyValue.pair(1, "a2"), KeyValue.pair(2, "b2"), KeyValue.pair(3, null));

        // wait for output and verify
        List<KeyValue<Integer, Integer>> receivedRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            initialRecordsProduced);

        for (final KeyValue<Integer, Integer> receivedRecord : receivedRecords) {
            // verify zero failed checks for each record
            assertEquals(receivedRecord.value, 0);
        }

        // wipe out state store to trigger restore process on restart
        kafkaStreams.close();
        kafkaStreams.cleanUp();

        // restart app with headers-aware store to test upgrade path
        // The store should migrate legacy timestamped data (without headers)
        // and add empty headers to existing data
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilderWithHeaders(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new TimestampedStoreWithHeadersContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce additional records with headers to verify upgraded store works
        final Headers upgradedHeaders = new RecordHeaders().add("upgraded", "true".getBytes());
        final int additionalRecordsProduced = produceDataToTopicWithHeaders(inputStream, baseTimestamp + 12, upgradedHeaders,
            KeyValue.pair(1, "a12"), KeyValue.pair(2, "b12"), KeyValue.pair(3, "c12"));

        // wait for output and verify
        receivedRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            initialRecordsProduced + additionalRecordsProduced);

        for (final KeyValue<Integer, Integer> receivedRecord : receivedRecords) {
            // verify zero failed checks for each record
            assertEquals(receivedRecord.value, 0);
        }
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
     * Processor for validating expected contents of a timestamped store with headers, and forwards
     * the number of failed checks downstream for consumption.
     */
    private static class TimestampedStoreWithHeadersContentCheckerProcessor implements Processor<Integer, String, Integer, Integer> {

        private ProcessorContext<Integer, Integer> context;
        private TimestampedKeyValueStoreWithHeaders<Integer, String> store;

        // whether the processor should write records to the store as they arrive.
        private final boolean writeToStore;
        // in-memory copy of seen data, to validate for testing purposes.
        private final Map<Integer, Optional<ValueTimestampHeaders<String>>> data;

        TimestampedStoreWithHeadersContentCheckerProcessor(final boolean writeToStore) {
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
            if (writeToStore) {
                final ValueTimestampHeaders<String> valueTimestampHeaders =
                    ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
                store.put(record.key(), valueTimestampHeaders);
                data.put(record.key(), Optional.ofNullable(valueTimestampHeaders));
            }

            // check expected contents of store, and signal completion by writing number of failures to downstream
            final int failedChecks = checkStoreContents();
            context.forward(record.withValue(failedChecks));
        }

        /**
         * @return number of failed checks
         */
        private int checkStoreContents() {
            int failedChecks = 0;
            for (final Map.Entry<Integer, Optional<ValueTimestampHeaders<String>>> keyWithValueTimestampHeaders : data.entrySet()) {
                final Integer key = keyWithValueTimestampHeaders.getKey();
                final ValueTimestampHeaders<String> expectedValueTimestampHeaders =
                    keyWithValueTimestampHeaders.getValue().orElse(null);

                // validate get from store
                final ValueTimestampHeaders<String> actualValueTimestampHeaders = store.get(key);
                if (!Objects.equals(actualValueTimestampHeaders, expectedValueTimestampHeaders)) {
                    failedChecks++;
                }
            }
            return failedChecks;
        }
    }

    /**
     * Processor for validating expected contents of a timestamped store (without headers).
     * Used for testing the upgrade path from TimestampedKeyValueStore to TimestampedKeyValueStoreWithHeaders.
     */
    private static class TimestampedStoreContentCheckerProcessor implements Processor<Integer, String, Integer, Integer> {

        private ProcessorContext<Integer, Integer> context;
        private TimestampedKeyValueStore<Integer, String> store;

        // in-memory copy of seen data, to validate for testing purposes.
        private final Map<Integer, Optional<ValueAndTimestamp<String>>> data;

        TimestampedStoreContentCheckerProcessor() {
            this.data = new HashMap<>();
        }

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(record.value(), record.timestamp());
            store.put(record.key(), valueAndTimestamp);
            data.put(record.key(), Optional.ofNullable(valueAndTimestamp));

            // check expected contents of store, and signal completion by writing
            // number of failures to downstream
            final int failedChecks = checkStoreContents();
            context.forward(record.withValue(failedChecks));
        }

        /**
         * @return number of failed checks
         */
        private int checkStoreContents() {
            int failedChecks = 0;
            for (final Map.Entry<Integer, Optional<ValueAndTimestamp<String>>> keyWithValueAndTimestamp : data.entrySet()) {
                final Integer key = keyWithValueAndTimestamp.getKey();
                final ValueAndTimestamp<String> valueAndTimestamp = keyWithValueAndTimestamp.getValue().orElse(null);

                // validate get from store
                final ValueAndTimestamp<String> record = store.get(key);
                if (!Objects.equals(record, valueAndTimestamp)) {
                    failedChecks++;
                }
            }
            return failedChecks;
        }
    }
}
