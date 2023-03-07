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

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
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
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.internals.RocksDbVersionedKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.VersionedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.VersionedKeyValueToBytesStoreAdapter;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({IntegrationTest.class})
public class VersionedKeyValueStoreIntegrationTest {

    private static final String STORE_NAME = "versioned-store";
    private static final long HISTORY_RETENTION = 3600_000L;

    private String inputStream;
    private String outputStream;
    private long baseTimestamp;

    private KafkaStreams kafkaStreams;

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void before() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void after() {
        CLUSTER.stop();
    }

    @Before
    public void beforeTest() throws InterruptedException {
        final String uniqueTestName = safeUniqueTestName(getClass(), testName);
        inputStream = "input-stream-" + uniqueTestName;
        outputStream = "output-stream-" + uniqueTestName;
        CLUSTER.createTopic(inputStream);
        CLUSTER.createTopic(outputStream);

        baseTimestamp = CLUSTER.time.milliseconds();
    }

    @After
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
                new VersionedKeyValueStoreBuilder<>(
                    new RocksDbVersionedKeyValueBytesStoreSupplier(STORE_NAME, HISTORY_RETENTION),
                    Serdes.Integer(),
                    Serdes.String(),
                    Time.SYSTEM
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(VersionedStoreContentCheckerProcessor::new, STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce source data
        int numRecordsProduced = 0;
        numRecordsProduced += produceSourceData(baseTimestamp, KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));
        numRecordsProduced += produceSourceData(baseTimestamp + 5, KeyValue.pair(1, "a5"), KeyValue.pair(2, null), KeyValue.pair(3, "c5"));
        numRecordsProduced += produceSourceData(baseTimestamp + 2, KeyValue.pair(1, "a2"), KeyValue.pair(2, "b2"), KeyValue.pair(3, null)); // out-of-order data
        numRecordsProduced += produceSourceData(baseTimestamp + 5, KeyValue.pair(1, "a5_new"), KeyValue.pair(2, "b5"), KeyValue.pair(3, null)); // replace existing records
        numRecordsProduced += produceSourceData(baseTimestamp + 7, KeyValue.pair(1, "delete"), KeyValue.pair(2, "delete"), KeyValue.pair(3, "delete")); // delete

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
            assertThat(0, equalTo(receivedRecord.value));
        }
    }

    @Test
    public void shouldSetChangelogTopicProperties() throws Exception {
        // build topology and start app
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                new VersionedKeyValueStoreBuilder<>(
                    new RocksDbVersionedKeyValueBytesStoreSupplier(STORE_NAME, HISTORY_RETENTION),
                    Serdes.Integer(),
                    Serdes.String(),
                    Time.SYSTEM
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(VersionedStoreCountProcessor::new, STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce record (and wait for result) to create changelog
        produceSourceData(baseTimestamp, KeyValue.pair(0, "foo"));

        IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            1);

        // verify changelog topic properties
        final String changelogTopic = "app-VersionedKeyValueStoreIntegrationTestshouldSetChangelogTopicProperties-versioned-store-changelog";
        final Properties changelogTopicConfig = CLUSTER.getLogConfig(changelogTopic);
        assertThat(changelogTopicConfig.getProperty("cleanup.policy"), equalTo("compact"));
        assertThat(changelogTopicConfig.getProperty("min.compaction.lag.ms"), equalTo(Long.toString(HISTORY_RETENTION + 24 * 60 * 60 * 1000L)));
    }

    @Test
    public void shouldRestore() throws Exception {
        // build topology and start app
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                new VersionedKeyValueStoreBuilder<>(
                    new RocksDbVersionedKeyValueBytesStoreSupplier(STORE_NAME, HISTORY_RETENTION),
                    Serdes.Integer(),
                    Serdes.String(),
                    Time.SYSTEM
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(VersionedStoreCountProcessor::new, STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce source data
        int initialRecordsProduced = 0;
        initialRecordsProduced += produceSourceData(baseTimestamp, KeyValue.pair(1, "a0"), KeyValue.pair(2, "b0"), KeyValue.pair(3, null));
        initialRecordsProduced += produceSourceData(baseTimestamp + 5, KeyValue.pair(1, "a5"), KeyValue.pair(2, null), KeyValue.pair(3, "c5"));
        initialRecordsProduced += produceSourceData(baseTimestamp + 2, KeyValue.pair(1, "a2"), KeyValue.pair(2, "b2"), KeyValue.pair(3, null)); // out-of-order data
        initialRecordsProduced += produceSourceData(baseTimestamp + 5, KeyValue.pair(1, "a5_new"), KeyValue.pair(2, "b5"), KeyValue.pair(3, null)); // replace existing records
        initialRecordsProduced += produceSourceData(baseTimestamp + 7, KeyValue.pair(1, "delete"), KeyValue.pair(2, "delete"), KeyValue.pair(3, "delete")); // delete
        initialRecordsProduced += produceSourceData(baseTimestamp + 10, KeyValue.pair(1, "a10"), KeyValue.pair(2, "b10"), KeyValue.pair(3, "c10")); // new data so latest is not tombstone

        // wait for output
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            initialRecordsProduced);

        // wipe out state store to trigger restore process on restart
        kafkaStreams.close();
        kafkaStreams.cleanUp();

        // restart app
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // produce additional records
        final int additionalRecordsProduced = produceSourceData(baseTimestamp + 12, KeyValue.pair(1, "a12"), KeyValue.pair(2, "b12"), KeyValue.pair(3, "c12"));

        // wait for output and verify
        final List<KeyValue<Integer, Integer>> receivedRecords = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class),
            outputStream,
            initialRecordsProduced + additionalRecordsProduced);

        for (int i = 1; i <= additionalRecordsProduced; i++) {
            final KeyValue<Integer, Integer> receivedRecord = receivedRecords.get(receivedRecords.size() - i);
            // verify more than one record version found, which confirms that restore took place
            assertThat(1, lessThan(receivedRecord.value));
        }
    }

    @Test
    public void shouldAllowCustomIQv2ForCustomStoreImplementations() {
        // build topology and start app
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                new VersionedKeyValueStoreBuilder<>(
                    new CustomIQv2VersionedStoreSupplier(),
                    Serdes.Integer(),
                    Serdes.String(),
                    Time.SYSTEM
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(VersionedStoreCountProcessor::new, STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

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
        assertThat("success", equalTo(result.getOnlyPartitionResult().getResult()));
    }

    private Properties props() {
        final Properties streamsConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(getClass(), testName);
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
    private final int produceSourceData(final long timestamp,
                                        final KeyValue<Integer, String>... keyValues) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            Arrays.asList(keyValues),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class),
            timestamp);
        return keyValues.length;
    }

    /**
     * Test-only processor for inserting records into a versioned store while also tracking
     * them separately in-memory, and performing checks to validate expected store contents.
     * Forwards the number of failed checks downstream for consumption.
     */
    private static class VersionedStoreContentCheckerProcessor implements Processor<Integer, String, Integer, Integer> {

        private ProcessorContext<Integer, Integer> context;
        private VersionedKeyValueStore<Integer, String> store;

        // in-memory copy of seen data, to validate for testing purposes.
        // maps from key -> timestamp -> value
        private final Map<Integer, Map<Long, String>> data = new HashMap<>();

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            // add record to store. special value "delete" is interpreted as a delete() call,
            // in contrast to null value, which is a tombstone inserted via put()
            if ("delete".equals(record.value())) {
                store.delete(record.key(), record.timestamp());
                addToSeenData(record.key(), record.timestamp(), null);
            } else {
                store.put(record.key(), record.value(), record.timestamp());
                addToSeenData(record.key(), record.timestamp(), record.value());
            }

            // check expected contents of store, and signal completion by writing
            // number of failures to downstream
            final int failedChecks = checkStoreContents();
            context.forward(record.withValue(failedChecks));
        }

        private void addToSeenData(final Integer key, final long timestamp, final String value) {
            data.computeIfAbsent(key, k -> new HashMap<>());
            data.get(key).put(timestamp, value);
        }

        /**
         * @return number of failed checks
         */
        private int checkStoreContents() {
            int failedChecks = 0;
            for (final Map.Entry<Integer, Map<Long, String>> keyWithTimestampsAndValues : data.entrySet()) {
                final Integer key = keyWithTimestampsAndValues.getKey();
                final Map<Long, String> timestampsAndValues = keyWithTimestampsAndValues.getValue();

                // track largest timestamp seen for key
                long maxExpectedTimestamp = -1L;
                String expectedValueForMaxTimestamp = null;

                for (final Map.Entry<Long, String> timestampAndValue : timestampsAndValues.entrySet()) {
                    final Long expectedTimestamp = timestampAndValue.getKey();
                    final String expectedValue = timestampAndValue.getValue();

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
     * Test-only processor for counting the number of record versions for a specific key,
     * and forwards this count downstream for consumption. The count only includes record
     * versions earlier than the current one, and stops as soon as a null is encountered.
     */
    private static class VersionedStoreCountProcessor implements Processor<Integer, String, Integer, Integer> {

        private ProcessorContext<Integer, Integer> context;
        private VersionedKeyValueStore<Integer, String> store;

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            // add record to store. special value "delete" is interpreted as a delete() call,
            // in contrast to null value, which is a tombstone inserted via put()
            if ("delete".equals(record.value())) {
                store.delete(record.key(), record.timestamp());
            } else {
                store.put(record.key(), record.value(), record.timestamp());
            }

            // count number of versions for this key, up through the current version.
            // count stops as soon as a null is reached
            int numVersions = 0;
            long timestamp = record.timestamp();
            while (true) {
                final VersionedRecord<String> versionedRecord = store.get(record.key(), timestamp);
                if (versionedRecord != null) {
                    numVersions++;
                    // search using earlier timestamp in order to find the next earlier record version
                    timestamp = versionedRecord.timestamp() - 1;
                } else {
                    break;
                }
            }

            context.forward(record.withValue(numVersions));
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
            public void put(final Bytes key, final byte[] value, final long timestamp) {
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

            @Deprecated
            @Override
            public void init(final org.apache.kafka.streams.processor.ProcessorContext context, final StateStore root) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void init(final StateStoreContext context, final StateStore root) {
                context.register(
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
