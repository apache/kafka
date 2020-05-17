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

import kafka.tools.StreamsResetter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForEmptyConsumerGroup;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public abstract class AbstractResetIntegrationTest {
    static String testId;
    static EmbeddedKafkaCluster cluster;

    private static MockTime mockTime;
    private static KafkaStreams streams;
    private static Admin adminClient = null;

    abstract Map<String, Object> getClientSslConfig();

    @AfterClass
    public static void afterClassCleanup() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(10));
            adminClient = null;
        }
    }

    private String appID = "abstract-reset-integration-test";
    private Properties commonClientConfig;
    private Properties streamsConfig;
    private Properties producerConfig;
    private Properties resultConsumerConfig;

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
            mockTime = cluster.time;
            // we align time to seconds to get clean window boundaries and thus ensure the same result for each run
            // otherwise, input records could fall into different windows for different runs depending on the initial mock time
            final long alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000;
            mockTime.setCurrentTimeMs(alignedTime);
            currentTimeSet = true;
        } catch (final IllegalArgumentException e) {
            // don't care will retry until set
        }
        return currentTimeSet;
    }

    private void prepareConfigs() {
        commonClientConfig = new Properties();
        commonClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        final Map<String, Object> sslConfig = getClientSslConfig();
        if (sslConfig != null) {
            commonClientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            commonClientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password) sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
            commonClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }

        producerConfig = new Properties();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.putAll(commonClientConfig);

        resultConsumerConfig = new Properties();
        resultConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, testId + "-result-consumer");
        resultConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        resultConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        resultConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        resultConsumerConfig.putAll(commonClientConfig);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + STREAMS_CONSUMER_TIMEOUT);
        streamsConfig.putAll(commonClientConfig);
    }

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(TestUtils.tempDirectory());

    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String OUTPUT_TOPIC_2 = "outputTopic2";
    private static final String OUTPUT_TOPIC_2_RERUN = "outputTopic2_rerun";
    private static final String INTERMEDIATE_USER_TOPIC = "userTopic";
    private static final String NON_EXISTING_TOPIC = "nonExistingTopic";

    private static final long STREAMS_CONSUMER_TIMEOUT = 2000L;
    private static final long CLEANUP_CONSUMER_TIMEOUT = 2000L;
    private static final int TIMEOUT_MULTIPLIER = 15;

    void prepareTest() throws Exception {
        prepareConfigs();
        prepareEnvironment();

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT);

        cluster.deleteAllTopicsAndWait(120000);
        cluster.createTopics(INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN);

        add10InputElements();
    }

    void cleanupTest() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    private void add10InputElements() throws java.util.concurrent.ExecutionException, InterruptedException {
        final List<KeyValue<Long, String>> records = Arrays.asList(KeyValue.pair(0L, "aaa"),
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
            mockTime.sleep(10);
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(record), producerConfig, mockTime.milliseconds());
        }
    }

    void shouldNotAllowToResetWhileStreamsIsRunning() {
        appID = testId + "-not-reset-during-runtime";
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-servers", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC,
            "--execute"
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);

        streams.close();
    }

    public void shouldNotAllowToResetWhenInputTopicAbsent() throws Exception {
        appID = testId + "-not-reset-without-input-topic";
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-servers", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC,
            "--execute"
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);
    }

    public void shouldNotAllowToResetWhenIntermediateTopicAbsent() throws Exception {
        appID = testId + "-not-reset-without-intermediate-topic";
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-servers", cluster.bootstrapServers(),
            "--intermediate-topics", NON_EXISTING_TOPIC,
            "--execute"
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);
    }

    void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() throws Exception {
        appID = testId + "-from-scratch";
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // RESET
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();
        cleanGlobal(false, null, null);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(null);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null);
    }

    void testReprocessingFromScratchAfterResetWithIntermediateUserTopic() throws Exception {
        cluster.createTopic(INTERMEDIATE_USER_TOPIC);

        appID = testId + "-from-scratch-with-intermediate-topic";
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        // receive only first values to make sure intermediate user topic is not consumed completely
        // => required to test "seekToEnd" for intermediate topics
        final List<KeyValue<Long, Long>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2, 40);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // insert bad record to make sure intermediate user topic gets seekToEnd()
        mockTime.sleep(1);
        final KeyValue<Long, String> badMessage = new KeyValue<>(-1L, "badRecord-ShouldBeSkipped");
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            INTERMEDIATE_USER_TOPIC,
            Collections.singleton(badMessage),
                producerConfig,
            mockTime.milliseconds());

        // RESET
        streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2_RERUN), streamsConfig);
        streams.cleanUp();
        cleanGlobal(true, null, null);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(INTERMEDIATE_USER_TOPIC);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        final List<KeyValue<Long, Long>> resultRerun2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2_RERUN, 40);
        streams.close();

        assertThat(resultRerun, equalTo(result));
        assertThat(resultRerun2, equalTo(result2));

        final Properties props = TestUtils.consumerConfig(cluster.bootstrapServers(), testId + "-result-consumer", LongDeserializer.class, StringDeserializer.class, commonClientConfig);
        final List<KeyValue<Long, String>> resultIntermediate = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(props, INTERMEDIATE_USER_TOPIC, 21);

        for (int i = 0; i < 10; i++) {
            assertThat(resultIntermediate.get(i), equalTo(resultIntermediate.get(i + 11)));
        }
        assertThat(resultIntermediate.get(10), equalTo(badMessage));

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(true, null, null);

        cluster.deleteTopicAndWait(INTERMEDIATE_USER_TOPIC);
    }

    void testReprocessingFromFileAfterResetWithoutIntermediateUserTopic() throws Exception {
        appID = testId + "-from-file";
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // RESET
        final File resetFile = File.createTempFile("reset", ".csv");
        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
            writer.write(INPUT_TOPIC + ",0,1");
        }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();

        cleanGlobal(false, "--from-file", resetFile.getAbsolutePath());
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(null);

        resetFile.deleteOnExit();

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 5);
        streams.close();

        result.remove(0);
        assertThat(resultRerun, equalTo(result));

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null);
    }

    void testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic() throws Exception {
        appID = testId + "-from-datetime";
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // RESET
        final File resetFile = File.createTempFile("reset", ".csv");
        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
            writer.write(INPUT_TOPIC + ",0,1");
        }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();


        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        final Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);

        cleanGlobal(false, "--to-datetime", format.format(calendar.getTime()));
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(null);

        resetFile.deleteOnExit();

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null);
    }

    void testReprocessingByDurationAfterResetWithoutIntermediateUserTopic() throws Exception {
        appID = testId + "-from-duration";
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // RESET
        final File resetFile = File.createTempFile("reset", ".csv");
        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
            writer.write(INPUT_TOPIC + ",0,1");
        }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();
        cleanGlobal(false, "--by-duration", "PT1M");

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(null);

        resetFile.deleteOnExit();

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null);
    }

    void testReprocessingFromScratchAfterResetAllExternalTopics() throws Exception {
        appID = testId + "from-reset-all-topics";
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithKTableStoreUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        mockTime.sleep(1);

        resetAllExternalTopics(appID);

        // RE-RUN
        streams = new KafkaStreams(setupTopologyWithKTableStoreUserTopic(), streamsConfig);
        streams.cleanUp();

        TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");


        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        resetAllExternalTopics(appID);

        cluster.deleteTopicAndWait(INTERMEDIATE_USER_TOPIC);
    }

    private Topology setupTopologyWithIntermediateUserTopic(final String outputTopic2) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map(KeyValue::new)
            .groupByKey()
            .count()
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

        input.through(INTERMEDIATE_USER_TOPIC)
            .groupByKey()
            .windowedBy(TimeWindows.of(ofMillis(35)).advanceBy(ofMillis(10)))
            .count()
            .toStream()
            .map((key, value) -> new KeyValue<>(key.window().start() + key.window().end(), value))
            .to(outputTopic2, Produced.with(Serdes.Long(), Serdes.Long()));

        return builder.build();
    }

    private Topology setupTopologyWithoutIntermediateUserTopic() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map((key, value) -> new KeyValue<>(key, key))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

        return builder.build();
    }

    private Topology setupTopologyWithKTableStoreUserTopic() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        input.groupByKey()
            .windowedBy(TimeWindows.of(ofMillis(35)).advanceBy(ofMillis(10)))
            .count()
            .toStream()
            .map((key, value) -> new KeyValue<>(key.window().start() + key.window().end(), value))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

        return builder.build();
    }

    private void cleanGlobal(final boolean withIntermediateTopics,
                             final String resetScenario,
                             final String resetScenarioArg) throws Exception {
        // leaving --zookeeper arg here to ensure tool works if users add it
        final List<String> parameterList = new ArrayList<>(
            Arrays.asList("--application-id", appID,
                    "--bootstrap-servers", cluster.bootstrapServers(),
                    "--input-topics", INPUT_TOPIC,
                    "--execute"));
        if (withIntermediateTopics) {
            parameterList.add("--intermediate-topics");
            parameterList.add(INTERMEDIATE_USER_TOPIC);
        }

        final Map<String, Object> sslConfig = getClientSslConfig();
        if (sslConfig != null) {
            final File configFile = TestUtils.tempFile();
            final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
            writer.write(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG + "=SSL\n");
            writer.write(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG + "=" + sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) + "\n");
            writer.write(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG + "=" + ((Password) sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value() + "\n");
            writer.close();

            parameterList.add("--config-file");
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
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(0, exitCode);
    }

    private void resetAllExternalTopics(final String appID) {
        final List<String> parameterList = new ArrayList<>(
                Arrays.asList("--application-id", appID,
                    "--bootstrap-servers", cluster.bootstrapServers(),
                    "--reset-all-external-topics",
                    "--execute"));

        final String[] parameters = parameterList.toArray(new String[0]);

        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(0, exitCode);
    }

    private void assertInternalTopicsGotDeleted(final String intermediateUserTopic) throws Exception {
        // do not use list topics request, but read from the embedded cluster's zookeeper path directly to confirm
        if (intermediateUserTopic != null) {
            cluster.waitForRemainingTopics(30000, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN,
                    Topic.GROUP_METADATA_TOPIC_NAME, intermediateUserTopic);
        } else {
            cluster.waitForRemainingTopics(30000, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN,
                    Topic.GROUP_METADATA_TOPIC_NAME);
        }
    }
}
