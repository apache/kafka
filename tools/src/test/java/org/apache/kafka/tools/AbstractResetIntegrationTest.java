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
package org.apache.kafka.tools;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public abstract class AbstractResetIntegrationTest {

    static ClusterInstance cluster;

    private static final MockTime MOCK_TIME = new MockTime();
    protected static KafkaStreams streams;
    protected static Admin adminClient;

    protected static Map<String, String> defaultBrokerProps() {
        return Map.of(
            SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, "-1",
            GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "0",
            GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0",
            GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "5",
            GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1",
            TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, "5",
            TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, "1",
            ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "true",
            ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, "true"
        );
    }

    Map<String, Object> getClientSecurityConfig() {
        return Map.of();
    }

    protected Properties commonClientConfig;
    protected Properties streamsConfig;
    private Properties producerConfig;
    protected Properties resultConsumerConfig;

    private void prepareEnvironment() {
        if (adminClient == null) {
            adminClient = Admin.create(commonClientConfig);
        }

        boolean timeSet = false;
        while (!timeSet) {
            timeSet = setCurrentTime();
        }
    }

    private boolean setCurrentTime() {
        boolean currentTimeSet = false;
        try {
            // we align time to seconds to get clean window boundaries and thus ensure the same result for each run
            // otherwise, input records could fall into different windows for different runs depending on the initial mock time
            final long alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000;
            MOCK_TIME.setCurrentTimeMs(alignedTime);
            currentTimeSet = true;
        } catch (final IllegalArgumentException e) {
            // don't care will retry until set
        }
        return currentTimeSet;
    }

    private void prepareConfigs(final String appID) {
        commonClientConfig = new Properties();
        commonClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        commonClientConfig.putAll(getClientSecurityConfig());

        producerConfig = new Properties();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.putAll(commonClientConfig);

        resultConsumerConfig = new Properties();
        resultConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, appID + "-result-consumer");
        resultConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        resultConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        resultConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        resultConsumerConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        resultConsumerConfig.putAll(commonClientConfig);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(STREAMS_CONSUMER_TIMEOUT));
        streamsConfig.putAll(commonClientConfig);
    }

    protected static final String INPUT_TOPIC = "inputTopic";
    protected static final String OUTPUT_TOPIC = "outputTopic";
    private static final String OUTPUT_TOPIC_2 = "outputTopic2";
    private static final String OUTPUT_TOPIC_2_RERUN = "outputTopic2_rerun";
    private static final String INTERMEDIATE_USER_TOPIC = "userTopic";

    protected static final int STREAMS_CONSUMER_TIMEOUT = 2000;
    protected static final int CLEANUP_CONSUMER_TIMEOUT = 2000;
    protected static final int TIMEOUT_MULTIPLIER = 30;

    void prepareTest(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        cluster = clusterInstance;
        final String appID = safeUniqueTestName(testInfo);
        prepareConfigs(appID);
        prepareEnvironment();

        createTopics(INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN);

        add10InputElements();
    }

    void cleanupTest() throws Exception {
        Utils.closeQuietly(streams, "kafka streams");
        Utils.delete(new File((String) streamsConfig.get(StreamsConfig.STATE_DIR_CONFIG)));
        if (adminClient != null) {
            Utils.closeQuietly(adminClient, "admin client");
            adminClient = null;
        }
    }

    @AfterEach
    public void after() throws Exception {
        cleanupTest();
    }

    private void add10InputElements() {
        final List<KeyValue<Long, String>> records = List.of(KeyValue.pair(0L, "aaa"),
                                                                   KeyValue.pair(1L, "bbb"),
                                                                   KeyValue.pair(0L, "ccc"),
                                                                   KeyValue.pair(1L, "ddd"),
                                                                   KeyValue.pair(0L, "eee"),
                                                                   KeyValue.pair(1L, "fff"),
                                                                   KeyValue.pair(0L, "ggg"),
                                                                   KeyValue.pair(1L, "hhh"),
                                                                   KeyValue.pair(0L, "iii"),
                                                                   KeyValue.pair(1L, "jjj"));

        for (final KeyValue<Long, String> record : records) {
            MOCK_TIME.sleep(10);
            produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Set.of(record), producerConfig, MOCK_TIME.milliseconds());
        }
    }

    @Timeout(600)
    @ClusterTemplate("clusterConfigs")
    public void testResetWhenInternalTopicsAreSpecified(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithIntermediateTopic(true, OUTPUT_TOPIC_2), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);
        waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // RESET
        streams.cleanUp();

        final List<String> internalTopics = getAllTopicsInCluster().stream()
                .filter(StreamsResetter::matchesInternalTopicFormat)
                .toList();
        cleanGlobal(false,
                "--internal-topics",
                String.join(",", internalTopics.subList(1, internalTopics.size())),
                appID);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(internalTopics.get(0));
    }

    @Timeout(600)
    @ClusterTemplate("clusterConfigs")
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);


        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> result = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // RESET
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();
        cleanGlobal(false, null, null, appID);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(null);

        // RE-RUN
        startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> resultRerun = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertEquals(result, resultRerun);

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null, appID);
    }

    @Timeout(600)
    @ClusterTemplate("clusterConfigs")
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopic(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        testReprocessingFromScratchAfterResetWithIntermediateUserTopic(false, testInfo);
    }

    @Timeout(600)
    @ClusterTemplate("clusterConfigs")
    public void testReprocessingFromScratchAfterResetWithIntermediateInternalTopic(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        testReprocessingFromScratchAfterResetWithIntermediateUserTopic(true, testInfo);
    }

    private void testReprocessingFromScratchAfterResetWithIntermediateUserTopic(final boolean useRepartitioned, final TestInfo testInfo) throws Exception {
        if (!useRepartitioned) {
            cluster.createTopic(INTERMEDIATE_USER_TOPIC, 1, (short) 1);
        }

        final String appID = safeUniqueTestName(testInfo);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithIntermediateTopic(useRepartitioned, OUTPUT_TOPIC_2), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> result = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        // receive only first values to make sure intermediate user topic is not consumed completely
        // => required to test "seekToEnd" for intermediate topics
        final List<KeyValue<Long, Long>> result2 = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2, 40);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // insert bad record to make sure intermediate user topic gets seekToEnd()
        MOCK_TIME.sleep(1);
        final KeyValue<Long, String> badMessage = new KeyValue<>(-1L, "badRecord-ShouldBeSkipped");
        if (!useRepartitioned) {
            produceKeyValuesSynchronouslyWithTimestamp(
                INTERMEDIATE_USER_TOPIC,
                Set.of(badMessage),
                producerConfig,
                MOCK_TIME.milliseconds());
        }

        // RESET
        streams = new KafkaStreams(setupTopologyWithIntermediateTopic(useRepartitioned, OUTPUT_TOPIC_2_RERUN), streamsConfig);
        streams.cleanUp();
        cleanGlobal(!useRepartitioned, null, null, appID);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(useRepartitioned ? null : INTERMEDIATE_USER_TOPIC);

        // RE-RUN
        startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> resultRerun = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        final List<KeyValue<Long, Long>> resultRerun2 = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2_RERUN, 40);
        streams.close();

        assertEquals(result, resultRerun);
        assertEquals(result2, resultRerun2);

        if (!useRepartitioned) {
            final Properties props = TestUtils.consumerConfig(cluster.bootstrapServers(), appID + "-result-consumer", LongDeserializer.class, StringDeserializer.class, commonClientConfig);
            final List<KeyValue<Long, String>> resultIntermediate = waitUntilMinKeyValueRecordsReceived(props, INTERMEDIATE_USER_TOPIC, 21);

            for (int i = 0; i < 10; i++) {
                assertEquals(resultIntermediate.get(i + 11), resultIntermediate.get(i));
            }
            assertEquals(badMessage, resultIntermediate.get(10));
        }

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(!useRepartitioned, null, null, appID);


    }

    private Topology setupTopologyWithIntermediateTopic(final boolean useRepartitioned,
                                                        final String outputTopic2) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map(KeyValue::new)
            .groupByKey()
            .count()
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

        final KStream<Long, String> stream;
        if (useRepartitioned) {
            stream = input.repartition();
        } else {
            input.to(INTERMEDIATE_USER_TOPIC);
            stream = builder.stream(INTERMEDIATE_USER_TOPIC);
        }
        stream.groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(35)).advanceBy(ofMillis(10)))
            .count()
            .toStream()
            .map((key, value) -> new KeyValue<>(key.window().start() + key.window().end(), value))
            .to(outputTopic2, Produced.with(Serdes.Long(), Serdes.Long()));

        return builder.build();
    }

    protected Topology setupTopologyWithoutIntermediateUserTopic() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map((key, value) -> new KeyValue<>(key, key))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

        return builder.build();
    }

    protected boolean tryCleanGlobal(final boolean withIntermediateTopics,
                                   final String resetScenario,
                                   final String resetScenarioArg,
                                   final String appID) throws Exception {
        final List<String> parameterList = new ArrayList<>(
            List.of("--application-id", appID,
                    "--bootstrap-server", cluster.bootstrapServers(),
                    "--input-topics", INPUT_TOPIC
            ));
        if (withIntermediateTopics) {
            parameterList.add("--intermediate-topics");
            parameterList.add(INTERMEDIATE_USER_TOPIC);
        }

        final Map<String, Object> securityConfig = getClientSecurityConfig();
        if (!securityConfig.isEmpty()) {
            final File configFile = TestUtils.tempFile();
            final Properties commandConfig = new Properties();
            for (final Map.Entry<String, Object> entry : securityConfig.entrySet()) {
                final Object value = entry.getValue();
                final String configValue;
                if (value instanceof Password) {
                    configValue = ((Password) value).value();
                } else if (value instanceof Collection<?>) {
                    configValue = String.join(",", ((Collection<?>) value).stream().map(Object::toString).toList());
                } else {
                    configValue = String.valueOf(value);
                }
                commandConfig.setProperty(entry.getKey(), configValue);
            }
            try (final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile))) {
                commandConfig.store(writer, null);
            }

            parameterList.add("--command-config");
            parameterList.add(configFile.getAbsolutePath());
        }
        if (resetScenario != null) {
            parameterList.add(resetScenario);
        }
        if (resetScenarioArg != null) {
            parameterList.add(resetScenarioArg);
        }

        final String[] parameters = parameterList.toArray(new String[0]);

        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        return new StreamsResetter().execute(parameters, cleanUpConfig) == 0;
    }

    protected void cleanGlobal(final boolean withIntermediateTopics,
                             final String resetScenario,
                             final String resetScenarioArg,
                             final String appID) throws Exception {
        final boolean cleanResult = tryCleanGlobal(withIntermediateTopics, resetScenario, resetScenarioArg, appID);
        assertTrue(cleanResult);
    }

    protected static void startApplicationAndWaitUntilRunning(final KafkaStreams streams) throws Exception {
        streams.start();
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.RUNNING,
            Duration.ofSeconds(60).toMillis(),
            "Kafka Streams application did not reach RUNNING state"
        );
    }

    protected static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                            final Collection<KeyValue<K, V>> records,
                                                                            final Properties producerConfig,
                                                                            final long timestamp) {
        try (final KafkaProducer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            for (final KeyValue<K, V> record : records) {
                producer.send(new ProducerRecord<>(topic, null, timestamp, record.key, record.value)).get();
            }
            producer.flush();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                     final String topic,
                                                                                     final int expectedNumRecords) throws Exception {
        final List<KeyValue<K, V>> accumData = new ArrayList<>();
        final String reason = String.format(
            "Did not receive all %d records from topic %s within %d ms",
            expectedNumRecords,
            topic,
            60000L
        );
        try (final Consumer<K, V> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerConfig)) {
            consumer.subscribe(List.of(topic));
            TestUtils.waitForCondition(() -> {
                final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<K, V> record : records) {
                    accumData.add(KeyValue.pair(record.key(), record.value()));
                }
                return accumData.size() >= expectedNumRecords;
            }, 60000L, reason + ", currently accumulated data is " + accumData);
        }
        return accumData;
    }

    protected static boolean isEmptyConsumerGroup(final Admin adminClient, final String appID) throws Exception {
        try {
            return adminClient.describeConsumerGroups(List.of(appID))
                    .describedGroups()
                    .get(appID)
                    .get()
                    .members()
                    .isEmpty();
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
                return true;
            }
            throw e;
        }
    }

    protected void waitForEmptyConsumerGroup(final Admin adminClient,
                                             final String appID,
                                             final long timeout) throws Exception {
        try (final Admin freshAdmin = Admin.create(commonClientConfig)) {
            TestUtils.waitForCondition(() -> isEmptyConsumerGroup(freshAdmin, appID), timeout, "Group is not empty: " + appID);
        }
    }

    protected static void createTopics(final String... topics) throws Exception {
        for (final String topic : topics) {
            cluster.createTopic(topic, 1, (short) 1);
        }
    }

    protected static Set<String> getAllTopicsInCluster() throws Exception {
        try (final Admin admin = cluster.admin()) {
            return admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
        }
    }
    protected void assertInternalTopicsGotDeleted(final String additionalExistingTopic) throws Exception {
        final Set<String> remainingTopics = additionalExistingTopic == null ?
                Set.of(INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN, Topic.GROUP_METADATA_TOPIC_NAME) :
                Set.of(INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN, Topic.GROUP_METADATA_TOPIC_NAME, additionalExistingTopic);
        TestUtils.waitForCondition(() -> getAllTopicsInCluster().equals(remainingTopics), 30000,
                "Unexpected topics remaining in cluster: " + getAllTopicsInCluster());
    }
}
