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

import kafka.admin.AdminClient;
import kafka.tools.StreamsResetter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
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
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public abstract class AbstractResetIntegrationTest {
    static String testId;
    static EmbeddedKafkaCluster cluster;
    static Map<String, Object> sslConfig = null;
    private static MockTime mockTime;
    private static AdminClient adminClient = null;
    private static KafkaAdminClient kafkaAdminClient = null;


    static void afterClassCleanup() {
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }
        if (kafkaAdminClient != null) {
            kafkaAdminClient.close(10, TimeUnit.SECONDS);
            kafkaAdminClient = null;
        }
    }

    private String appID;
    private Properties commonClientConfig;

    private void prepareEnvironment() {
        if (adminClient == null) {
            adminClient = AdminClient.create(commonClientConfig);
        }
        if (kafkaAdminClient == null) {
            kafkaAdminClient =  (KafkaAdminClient) org.apache.kafka.clients.admin.AdminClient.create(commonClientConfig);
        }

        // we align time to seconds to get clean window boundaries and thus ensure the same result for each run
        // otherwise, input records could fall into different windows for different runs depending on the initial mock time
        final long alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000;
        mockTime = cluster.time;
        mockTime.setCurrentTimeMs(alignedTime);
    }

    private void prepareConfigs() {
        commonClientConfig = new Properties();
        commonClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        if (sslConfig != null) {
            commonClientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            commonClientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password) sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
            commonClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }

        PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, 0);
        PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        PRODUCER_CONFIG.putAll(commonClientConfig);

        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, testId + "-result-consumer");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        RESULT_CONSUMER_CONFIG.putAll(commonClientConfig);

        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        STREAMS_CONFIG.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + STREAMS_CONSUMER_TIMEOUT);
        STREAMS_CONFIG.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        STREAMS_CONFIG.putAll(commonClientConfig);
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
    private static final int TIMEOUT_MULTIPLIER = 5;

    private final TestCondition consumerGroupInactiveCondition = new TestCondition() {
        @Override
        public boolean conditionMet() {
            return adminClient.describeConsumerGroup(testId + "-result-consumer", 0).consumers().get().isEmpty();
        }
    };

    private static final Properties STREAMS_CONFIG = new Properties();
    private final static Properties PRODUCER_CONFIG = new Properties();
    private final static Properties RESULT_CONSUMER_CONFIG = new Properties();

    void prepareTest() throws Exception {
        cluster.deleteAndRecreateTopics(INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN);

        prepareConfigs();
        prepareEnvironment();

        add10InputElements();
    }

    void cleanupTest() throws Exception {
        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
    }

    private void add10InputElements() throws java.util.concurrent.ExecutionException, InterruptedException {
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "aaa")), PRODUCER_CONFIG, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "bbb")), PRODUCER_CONFIG, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "ccc")), PRODUCER_CONFIG, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "ddd")), PRODUCER_CONFIG, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "eee")), PRODUCER_CONFIG, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "fff")), PRODUCER_CONFIG, mockTime.milliseconds());
        mockTime.sleep(1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "ggg")), PRODUCER_CONFIG, mockTime.milliseconds());
        mockTime.sleep(1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "hhh")), PRODUCER_CONFIG, mockTime.milliseconds());
        mockTime.sleep(1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "iii")), PRODUCER_CONFIG, mockTime.milliseconds());
        mockTime.sleep(1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "jjj")), PRODUCER_CONFIG, mockTime.milliseconds());
    }

    void shouldNotAllowToResetWhileStreamsIsRunning() throws Exception {
        appID = testId + "-not-reset-during-runtime";
        final String[] parameters = new String[] {
                "--application-id", appID,
                "--bootstrap-servers", cluster.bootstrapServers(),
                "--input-topics", NON_EXISTING_TOPIC };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), STREAMS_CONFIG);
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
                "--input-topics", NON_EXISTING_TOPIC };
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
                "--input-topics", NON_EXISTING_TOPIC };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);
    }

    void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() throws Exception {
        appID = testId + "-from-scratch";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), STREAMS_CONFIG);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC,10);

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // RESET
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), STREAMS_CONFIG);
        streams.cleanUp();
        cleanGlobal(false, null, null);
        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted(null);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC,10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        cleanGlobal(false, null, null);
    }

    void testReprocessingFromScratchAfterResetWithIntermediateUserTopic() throws Exception {
        cluster.createTopic(INTERMEDIATE_USER_TOPIC);

        appID = testId + "-from-scratch-with-intermediate-topic";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2), STREAMS_CONFIG);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 10);
        // receive only first values to make sure intermediate user topic is not consumed completely
        // => required to test "seekToEnd" for intermediate topics
        final List<KeyValue<Long, Long>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC_2, 40);

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // insert bad record to make sure intermediate user topic gets seekToEnd()
        mockTime.sleep(1);
        KeyValue<Long, String> badMessage = new KeyValue<>(-1L, "badRecord-ShouldBeSkipped");
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            INTERMEDIATE_USER_TOPIC,
            Collections.singleton(badMessage),
            PRODUCER_CONFIG,
            mockTime.milliseconds());

        // RESET
        streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2_RERUN), STREAMS_CONFIG);
        streams.cleanUp();
        cleanGlobal(true, null, null);
        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted(INTERMEDIATE_USER_TOPIC);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 10);
        final List<KeyValue<Long, Long>> resultRerun2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC_2_RERUN, 40);
        streams.close();

        assertThat(resultRerun, equalTo(result));
        assertThat(resultRerun2, equalTo(result2));

        final Properties props = TestUtils.consumerConfig(cluster.bootstrapServers(), testId + "-result-consumer", LongDeserializer.class, StringDeserializer.class, commonClientConfig);
        final List<KeyValue<Long, String>> resultIntermediate = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(props, INTERMEDIATE_USER_TOPIC, 21);

        for (int i = 0; i < 10; i++) {
            assertThat(resultIntermediate.get(i), equalTo(resultIntermediate.get(i + 11)));
        }
        assertThat(resultIntermediate.get(10), equalTo(badMessage));

        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        cleanGlobal(true, null, null);

        cluster.deleteTopicAndWait(INTERMEDIATE_USER_TOPIC);
    }

    void testReprocessingFromFileAfterResetWithoutIntermediateUserTopic() throws Exception {
        appID = testId + "-from-file";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), STREAMS_CONFIG);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 10);

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // RESET
        final File resetFile = File.createTempFile("reset", ".csv");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
            writer.write(INPUT_TOPIC + ",0,1");
            writer.close();
        }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), STREAMS_CONFIG);
        streams.cleanUp();

        cleanGlobal(false, "--from-file", resetFile.getAbsolutePath());
        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted(null);

        resetFile.deleteOnExit();

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 5);
        streams.close();

        result.remove(0);
        assertThat(resultRerun, equalTo(result));

        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        cleanGlobal(false, null, null);
    }

    void testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic() throws Exception {
        appID = testId + "-from-datetime";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), STREAMS_CONFIG);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC,10);

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // RESET
        final File resetFile = File.createTempFile("reset", ".csv");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
            writer.write(INPUT_TOPIC + ",0,1");
            writer.close();
        }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), STREAMS_CONFIG);
        streams.cleanUp();


        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        final Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);

        cleanGlobal(false, "--to-datetime", format.format(calendar.getTime()));
        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted(null);

        resetFile.deleteOnExit();

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        cleanGlobal(false, null, null);
    }

    void testReprocessingByDurationAfterResetWithoutIntermediateUserTopic() throws Exception {
        appID = testId + "-from-duration";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), STREAMS_CONFIG);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived( RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 10);

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // RESET
        final File resetFile = File.createTempFile("reset", ".csv");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
            writer.write(INPUT_TOPIC + ",0,1");
            writer.close();
        }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), STREAMS_CONFIG);
        streams.cleanUp();
        cleanGlobal(false, "--by-duration", "PT1M");

        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted(null);

        resetFile.deleteOnExit();

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC,10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        TestUtils.waitForCondition(consumerGroupInactiveCondition, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        cleanGlobal(false, null, null);
    }

    private Topology setupTopologyWithIntermediateUserTopic(final String outputTopic2) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map(new KeyValueMapper<Long, String, KeyValue<Long, String>>() {
            @Override
            public KeyValue<Long, String> apply(final Long key, final String value) {
                return new KeyValue<>(key, value);
            }
        })
            .groupByKey()
            .count()
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

        input.through(INTERMEDIATE_USER_TOPIC)
            .groupByKey()
            .windowedBy(TimeWindows.of(35).advanceBy(10))
            .count()
            .toStream()
            .map(new KeyValueMapper<Windowed<Long>, Long, KeyValue<Long, Long>>() {
                @Override
                public KeyValue<Long, Long> apply(final Windowed<Long> key, final Long value) {
                    return new KeyValue<>(key.window().start() + key.window().end(), value);
                }
            })
            .to(outputTopic2, Produced.with(Serdes.Long(), Serdes.Long()));

        return builder.build();
    }

    private Topology setupTopologyWithoutIntermediateUserTopic() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map(new KeyValueMapper<Long, String, KeyValue<Long, Long>>() {
            @Override
            public KeyValue<Long, Long> apply(final Long key, final String value) {
                return new KeyValue<>(key, key);
            }
        }).to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

        return builder.build();
    }

    private void cleanGlobal(final boolean withIntermediateTopics,
                             final String resetScenario,
                             final String resetScenarioArg) throws Exception {
        // leaving --zookeeper arg here to ensure tool works if users add it
        final List<String> parameterList = new ArrayList<>(
            Arrays.asList("--application-id", appID,
                "--bootstrap-servers", cluster.bootstrapServers(),
                "--input-topics", INPUT_TOPIC));
        if (withIntermediateTopics) {
            parameterList.add("--intermediate-topics");
            parameterList.add(INTERMEDIATE_USER_TOPIC);
        }
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

        final String[] parameters = parameterList.toArray(new String[parameterList.size()]);

        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(0, exitCode);
    }

    private void assertInternalTopicsGotDeleted(final String intermediateUserTopic) throws Exception {
        // do not use list topics request, but read from the embedded cluster's zookeeper path directly to confirm
        if (intermediateUserTopic != null) {
            cluster.waitForRemainingTopics(30000, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN, TestUtils.GROUP_METADATA_TOPIC_NAME, intermediateUserTopic);
        } else {
            cluster.waitForRemainingTopics(30000, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN, TestUtils.GROUP_METADATA_TOPIC_NAME);
        }
    }
}
