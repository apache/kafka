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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.internals.VersionedKeyValueToBytesStoreAdapter;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag("integration")
public class VersionedKeyValueStoreIntegrationTest {

    private static final String STORE_NAME = "versioned-store";
    private static final long HISTORY_RETENTION = 3600_000L;

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
        // build topology and start app
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.versionedKeyValueStoreBuilder(
                    Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMillis(HISTORY_RETENTION)),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new VersionedStoreContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce source data
        int numRecordsProduced = 0;
        numRecordsProduced += produceDataToTopic(inputStream, baseTimestamp, KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));
        numRecordsProduced += produceDataToTopic(inputStream, baseTimestamp + 5, KeyValue.pair(1, "a5"), KeyValue.pair(2, null), KeyValue.pair(3, "c5"));
        numRecordsProduced += produceDataToTopic(inputStream, baseTimestamp + 2, KeyValue.pair(1, "a2"), KeyValue.pair(2, "b2"), KeyValue.pair(3, null)); // out-of-order data
        numRecordsProduced += produceDataToTopic(inputStream, baseTimestamp + 5, KeyValue.pair(1, "a5_new"), KeyValue.pair(2, "b5"), KeyValue.pair(3, null)); // replace existing records
        numRecordsProduced += produceDataToTopic(inputStream, baseTimestamp + 7, KeyValue.pair(1, DataTracker.DELETE_VALUE_KEYWORD), KeyValue.pair(2, DataTracker.DELETE_VALUE_KEYWORD), KeyValue.pair(3, DataTracker.DELETE_VALUE_KEYWORD)); // delete

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
            assertThat(receivedRecord.value, equalTo(0));
        }
    }

    @Test
    public void shouldSetChangelogTopicProperties() throws Exception {
        // build topology and start app
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.versionedKeyValueStoreBuilder(
                    Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMillis(HISTORY_RETENTION)),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new VersionedStoreContentCheckerProcessor(false), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce record (and wait for result) to create changelog
        produceDataToTopic(inputStream, baseTimestamp, KeyValue.pair(0, "foo"));

        IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            1);

        // verify changelog topic properties
        final String changelogTopic = props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-versioned-store-changelog";
        final Properties changelogTopicConfig = CLUSTER.getLogConfig(changelogTopic);
        assertThat(changelogTopicConfig.getProperty("cleanup.policy"), equalTo("compact"));
        assertThat(changelogTopicConfig.getProperty("min.compaction.lag.ms"), equalTo(Long.toString(HISTORY_RETENTION + 24 * 60 * 60 * 1000L)));
    }

    @Test
    public void shouldRestore() throws Exception {
        // build topology and start app
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.versionedKeyValueStoreBuilder(
                    Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMillis(HISTORY_RETENTION)),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new VersionedStoreContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce source data and track in-memory to verify after restore
        final DataTracker data = new DataTracker();
        int initialRecordsProduced = 0;
        initialRecordsProduced += produceDataToTopic(inputStream, data, baseTimestamp, KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));
        initialRecordsProduced += produceDataToTopic(inputStream, data, baseTimestamp + 5, KeyValue.pair(1, "a5"), KeyValue.pair(2, null), KeyValue.pair(3, "c5"));
        initialRecordsProduced += produceDataToTopic(inputStream, data, baseTimestamp + 2, KeyValue.pair(1, "a2"), KeyValue.pair(2, "b2"), KeyValue.pair(3, null)); // out-of-order data
        initialRecordsProduced += produceDataToTopic(inputStream, data, baseTimestamp + 5, KeyValue.pair(1, "a5_new"), KeyValue.pair(2, "b5"), KeyValue.pair(3, null)); // replace existing records
        initialRecordsProduced += produceDataToTopic(inputStream, data, baseTimestamp + 7, KeyValue.pair(1, DataTracker.DELETE_VALUE_KEYWORD), KeyValue.pair(2, DataTracker.DELETE_VALUE_KEYWORD), KeyValue.pair(3, DataTracker.DELETE_VALUE_KEYWORD)); // delete
        initialRecordsProduced += produceDataToTopic(inputStream, data, baseTimestamp + 10, KeyValue.pair(1, "a10"), KeyValue.pair(2, "b10"), KeyValue.pair(3, "c10")); // new data so latest is not tombstone

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

        // restart app and pass expected store contents to processor
        streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.versionedKeyValueStoreBuilder(
                    Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMillis(HISTORY_RETENTION)),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new VersionedStoreContentCheckerProcessor(true, data), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce additional records
        final int additionalRecordsProduced = produceDataToTopic(inputStream, baseTimestamp + 12, KeyValue.pair(1, "a12"), KeyValue.pair(2, "b12"), KeyValue.pair(3, "c12"));

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
            assertThat(receivedRecord.value, equalTo(0));
        }
    }

    @Test
    public void shouldAllowCustomIQv2ForCustomStoreImplementations() {
        // build topology and start app
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.versionedKeyValueStoreBuilder(
                    new CustomIQv2VersionedStoreSupplier(),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new VersionedStoreContentCheckerProcessor(false), STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // issue IQv2 query and verify result
        final StateQueryRequest<String> request =
            StateQueryRequest.inStore(STORE_NAME)
                .withQuery(new TestQuery())
                .withPartitions(Collections.singleton(0));
        final StateQueryResult<String> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        assertThat(result.getOnlyPartitionResult().getResult(), equalTo("success"));
    }

    @Test
    public void shouldManualUpgradeFromNonVersionedTimestampedToVersioned() throws Exception {
        // build non-versioned (timestamped) topology
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

        shouldManualUpgradeFromNonVersionedToVersioned(streamsBuilder.build());
    }

    @Test
    public void shouldManualUpgradeFromNonVersionedNonTimestampedToVersioned() throws Exception {
        // build non-versioned (non-timestamped) topology
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(KeyValueStoreContentCheckerProcessor::new, STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        shouldManualUpgradeFromNonVersionedToVersioned(streamsBuilder.build());
    }

    private void shouldManualUpgradeFromNonVersionedToVersioned(final Topology originalTopology) throws Exception {
        // build original (non-versioned) topology and start app
        final Properties props = props();
        // additional property to prevent premature compaction of older record versions while using timestamped store
        props.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, 60_000L);
        kafkaStreams = new KafkaStreams(originalTopology, props);
        kafkaStreams.start();

        // produce source data and track in-memory to verify after restore
        final DataTracker data = new DataTracker();
        int initialRecordsProduced = 0;
        initialRecordsProduced += produceDataToTopic(inputStream, data, baseTimestamp, KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));
        initialRecordsProduced += produceDataToTopic(inputStream, data, baseTimestamp + 5, KeyValue.pair(1, "a5"), KeyValue.pair(2, null), KeyValue.pair(3, "c5"));
        initialRecordsProduced += produceDataToTopic(inputStream, data, baseTimestamp + 2, KeyValue.pair(1, "a2"), KeyValue.pair(2, "b2"), KeyValue.pair(3, null)); // out-of-order data

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
            assertThat(receivedRecord.value, equalTo(0));
        }

        // wipe out state store to trigger restore process on restart
        kafkaStreams.close();
        kafkaStreams.cleanUp();

        // restart app with versioned store, and pass expected store contents to processor
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.versionedKeyValueStoreBuilder(
                    Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMillis(HISTORY_RETENTION)),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new VersionedStoreContentCheckerProcessor(true, data), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce additional records
        final int additionalRecordsProduced = produceDataToTopic(inputStream, baseTimestamp + 12, KeyValue.pair(1, "a12"), KeyValue.pair(2, "b12"), KeyValue.pair(3, "c12"));

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
            assertThat(receivedRecord.value, equalTo(0));
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
     * @param topic       topic to produce to
     * @param dataTracker map of key -> timestamp -> value for tracking data which is produced to
     *                    the topic. This method will add the produced data into this in-memory
     *                    tracker in addition to producing to the topic, in order to keep the two
     *                    in sync.
     * @param timestamp   timestamp to produce with
     * @param keyValues   key-value pairs to produce
     *
     * @return number of records produced
     */
    @SuppressWarnings("varargs")
    @SafeVarargs
    private final int produceDataToTopic(final String topic,
                                         final DataTracker dataTracker,
                                         final long timestamp,
                                         final KeyValue<Integer, String>... keyValues) {
        produceDataToTopic(topic, timestamp, keyValues);

        for (final KeyValue<Integer, String> keyValue : keyValues) {
            dataTracker.add(keyValue.key, timestamp, keyValue.value);
        }

        return keyValues.length;
    }

    /**
     * Test-only processor for validating expected contents of a versioned store, and forwards
     * the number of failed checks downstream for consumption. Callers specify whether the
     * processor should also be responsible for inserting records into the store (while also
     * tracking them separately in-memory for use in validation).
     */
    private static class VersionedStoreContentCheckerProcessor implements Processor<Integer, String, Integer, Integer> {

        private ProcessorContext<Integer, Integer> context;
        private VersionedKeyValueStore<Integer, String> store;

        // whether or not the processor should write records to the store as they arrive.
        // must be false for global stores.
        private final boolean writeToStore;
        // in-memory copy of seen data, to validate for testing purposes.
        private final DataTracker data;

        /**
         * @param writeToStore whether or not this processor should write to the store
         */
        VersionedStoreContentCheckerProcessor(final boolean writeToStore) {
            this(writeToStore, new DataTracker());
        }

        /**
         * @param writeToStore whether or not this processor should write to the store
         * @param initialData  expected store contents which have already been inserted from
         *                     outside of this processor
         */
        VersionedStoreContentCheckerProcessor(final boolean writeToStore,
                                              final DataTracker initialData) {
            this.writeToStore = writeToStore;
            this.data = initialData;
        }

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            if (writeToStore) {
                // add record to store. special value "delete" is interpreted as a delete() call,
                // in contrast to null value, which is a tombstone inserted via put()
                if (DataTracker.DELETE_VALUE_KEYWORD.equals(record.value())) {
                    store.delete(record.key(), record.timestamp());
                } else {
                    store.put(record.key(), record.value(), record.timestamp());
                }
                data.add(record.key(), record.timestamp(), record.value());
            }

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
            for (final Map.Entry<Integer, Map<Long, Optional<String>>> keyWithTimestampsAndValues : data.data.entrySet()) {
                final Integer key = keyWithTimestampsAndValues.getKey();
                final Map<Long, Optional<String>> timestampsAndValues = keyWithTimestampsAndValues.getValue();

                // track largest timestamp seen for key
                long maxExpectedTimestamp = -1L;
                String expectedValueForMaxTimestamp = null;

                for (final Map.Entry<Long, Optional<String>> timestampAndValue : timestampsAndValues.entrySet()) {
                    final Long expectedTimestamp = timestampAndValue.getKey();
                    final String expectedValue = timestampAndValue.getValue().orElse(null);

                    if (expectedTimestamp > maxExpectedTimestamp) {
                        maxExpectedTimestamp = expectedTimestamp;
                        expectedValueForMaxTimestamp = expectedValue;
                    }

                    // validate timestamped get on store
                    final VersionedRecord<String> versionedRecord = store.get(key, expectedTimestamp);
                    if (!contentsMatch(versionedRecord, expectedValue, expectedTimestamp)) {
                        failedChecks++;
                    }
                }

                // validate get latest on store
                final VersionedRecord<String> versionedRecord = store.get(key);
                if (!contentsMatch(versionedRecord, expectedValueForMaxTimestamp, maxExpectedTimestamp)) {
                    failedChecks++;
                }
            }
            return failedChecks;
        }

        private static boolean contentsMatch(final VersionedRecord<String> versionedRecord,
                                             final String expectedValue,
                                             final long expectedTimestamp) {
            if (expectedValue == null) {
                return versionedRecord == null;
            } else {
                if (versionedRecord == null) {
                    return false;
                }
                return expectedValue.equals(versionedRecord.value())
                    && expectedTimestamp == versionedRecord.timestamp();
            }
        }
    }

    /**
     * Same as {@link VersionedStoreContentCheckerProcessor} but for timestamped stores instead,
     * for use in validating the manual upgrade path from non-versioned to versioned stores.
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
            // add record to store
            if (DataTracker.DELETE_VALUE_KEYWORD.equals(record.value())) {
                // special value "delete" is interpreted as a delete() call from
                // VersionedStoreContentCheckerProcessor but we do not support it here
                throw new IllegalArgumentException("Using 'delete' keyword for "
                    + "TimestampedStoreContentCheckerProcessor will result in the record "
                    + "timestamp being ignored. Use regular put with null value instead.");
            }
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

    /**
     * Same as {@link VersionedStoreContentCheckerProcessor} but for regular key-value stores instead,
     * for use in validating the manual upgrade path from non-versioned to versioned stores.
     */
    private static class KeyValueStoreContentCheckerProcessor implements Processor<Integer, String, Integer, Integer> {

        private ProcessorContext<Integer, Integer> context;
        private KeyValueStore<Integer, String> store;

        // in-memory copy of seen data, to validate for testing purposes.
        private final Map<Integer, Optional<String>> data;

        KeyValueStoreContentCheckerProcessor() {
            this.data = new HashMap<>();
        }

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            // add record to store
            if (DataTracker.DELETE_VALUE_KEYWORD.equals(record.value())) {
                // special value "delete" is interpreted as a delete() call from
                // VersionedStoreContentCheckerProcessor but we do not support it here
                throw new IllegalArgumentException("Using 'delete' keyword for "
                    + "KeyValueStoreContentCheckerProcessor will result in the record "
                    + "timestamp being ignored. Use regular put with null value instead.");
            }
            store.put(record.key(), record.value());
            data.put(record.key(), Optional.ofNullable(record.value()));

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
            for (final Map.Entry<Integer, Optional<String>> keyValue : data.entrySet()) {
                final Integer expectedKey = keyValue.getKey();
                final String expectedValue = keyValue.getValue().orElse(null);

                // validate get from store
                final String foundValue = store.get(expectedKey);
                if (!Objects.equals(foundValue, expectedValue)) {
                    failedChecks++;
                }
            }
            return failedChecks;
        }
    }

    /**
     * In-memory copy of data put to versioned store, for verification purposes.
     */
    private static class DataTracker {

        // special value which is interpreted as call to store.delete()
        static final String DELETE_VALUE_KEYWORD = "delete";

        // maps from key -> timestamp -> value.
        // value is represented as Optional to ensure proper recording of nulls.
        final Map<Integer, Map<Long, Optional<String>>> data = new HashMap<>();

        void add(final Integer key, final long timestamp, final String value) {
            data.computeIfAbsent(key, k -> new HashMap<>());
            if (DELETE_VALUE_KEYWORD.equals(value)) {
                data.get(key).put(timestamp, Optional.empty());
            } else {
                data.get(key).put(timestamp, Optional.ofNullable(value));
            }
        }
    }

    /**
     * Custom {@link Query} type for test purposes. Versioned stores do not currently support the
     * built-in {@link KeyQuery} or {@link RangeQuery} query types, but custom types are allowed.
     */
    private static class TestQuery implements Query<String> {
    }

    /**
     * Supplies a custom {@link VersionedKeyValueStore} implementation solely for the purpose
     * of testing IQv2 queries. A hard-coded "success" result is returned in response to
     * {@code TestQuery} queries.
     */
    private static class CustomIQv2VersionedStoreSupplier implements VersionedBytesStoreSupplier {

        @Override
        public String name() {
            return STORE_NAME;
        }

        @Override
        public KeyValueStore<Bytes, byte[]> get() {
            return new VersionedKeyValueToBytesStoreAdapter(new CustomIQv2VersionedStore());
        }

        @Override
        public String metricsScope() {
            return "metrics-scope";
        }

        @Override
        public long historyRetentionMs() {
            return HISTORY_RETENTION;
        }

        /**
         * Custom {@link VersionedKeyValueStore} implementation solely for the purpose of testing
         * IQv2 queries. All other methods are unsupported / no-ops.
         */
        private static class CustomIQv2VersionedStore implements VersionedKeyValueStore<Bytes, byte[]> {

            @SuppressWarnings("unchecked")
            @Override
            public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
                if (query instanceof TestQuery) {
                    return (QueryResult<R>) QueryResult.forResult("success");
                } else {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public long put(final Bytes key, final byte[] value, final long timestamp) {
                throw new UnsupportedOperationException();
            }

            @Override
            public VersionedRecord<byte[]> delete(final Bytes key, final long timestamp) {
                throw new UnsupportedOperationException();
            }

            @Override
            public VersionedRecord<byte[]> get(final Bytes key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public VersionedRecord<byte[]> get(final Bytes key, final long asOfTimestamp) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String name() {
                return STORE_NAME;
            }

            @Override
            public void init(final StateStoreContext stateStoreContext, final StateStore root) {
                stateStoreContext.register(
                    root,
                    (key, value) -> { }
                );
            }

            @Override
            public void flush() {
                // do nothing
            }

            @Override
            public void close() {
                // do nothing
            }

            @Override
            public boolean persistent() {
                return false;
            }

            @Override
            public boolean isOpen() {
                return true;
            }
        }
    }
}
