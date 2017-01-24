/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import kafka.admin.AdminClient;
import kafka.server.KafkaConfig$;
import kafka.tools.StreamsResetter;
import kafka.utils.MockTime;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Tests local state store and global application cleanup.
 */
public class ResetIntegrationTest {
    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER;
    static {
        final Properties props = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        props.put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);
        CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, props);
    }

    private static final String APP_ID = "cleanup-integration-test";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String OUTPUT_TOPIC_2 = "outputTopic2";
    private static final String OUTPUT_TOPIC_2_RERUN = "outputTopic2_rerun";
    private static final String INTERMEDIATE_USER_TOPIC = "userTopic";

    private static final long STREAMS_CONSUMER_TIMEOUT = 2000L;
    private static final long CLEANUP_CONSUMER_TIMEOUT = 2000L;
    private static final int TIMEOUT_MULTIPLIER = 5;

    private static int testNo = 0;
    private static AdminClient adminClient = null;

    private final MockTime mockTime = CLUSTER.time;
    private final WaitUntilConsumerGroupGotClosed consumerGroupInactive = new WaitUntilConsumerGroupGotClosed();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_TOPIC);
        CLUSTER.createTopic(OUTPUT_TOPIC);
        CLUSTER.createTopic(OUTPUT_TOPIC_2);
        CLUSTER.createTopic(OUTPUT_TOPIC_2_RERUN);
    }

    @AfterClass
    public static void globalCleanup() {
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }
    }

    @Before
    public void cleanup() throws Exception {
        ++testNo;

        if (adminClient == null) {
            adminClient = AdminClient.createSimplePlaintext(CLUSTER.bootstrapServers());
        }

        // busy wait until cluster (ie, ConsumerGroupCoordinator) is available
        while (true) {
            Thread.sleep(50);

            try {
                TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Test consumer group active even after waiting " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
            } catch (GroupCoordinatorNotAvailableException e) {
                continue;
            } catch (IllegalArgumentException e) {
                continue;
            }
            break;
        }

        if (testNo == 1) {
            prepareInputData();
        }
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopic() throws Exception {
        CLUSTER.createTopic(INTERMEDIATE_USER_TOPIC);

        final Properties streamsConfiguration = prepareTest();
        final Properties resultTopicConsumerConfig = TestUtils.consumerConfig(
            CLUSTER.bootstrapServers(),
            APP_ID + "-standard-consumer-" + OUTPUT_TOPIC,
            LongDeserializer.class,
            LongDeserializer.class);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2), streamsConfiguration);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC,
            10,
            60000);
        // receive only first values to make sure intermediate user topic is not consumed completely
        // => required to test "seekToEnd" for intermediate topics
        final List<KeyValue<Long, Long>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC_2,
            10
        );

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // insert bad record to maks sure intermediate user topic gets seekToEnd()
        mockTime.sleep(1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                INTERMEDIATE_USER_TOPIC,
                Collections.singleton(new KeyValue<>(-1L, "badRecord-ShouldBeSkipped")), TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, StringSerializer.class), mockTime.milliseconds());

        // RESET
        streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2_RERUN), streamsConfiguration);
        streams.cleanUp();
        cleanGlobal(INTERMEDIATE_USER_TOPIC);
        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted(INTERMEDIATE_USER_TOPIC);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC,
            10,
            60000);
        final List<KeyValue<Long, Long>> resultRerun2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC_2_RERUN,
            10
        );
        streams.close();

        assertThat(resultRerun, equalTo(result));
        assertThat(resultRerun2, equalTo(result2));

        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        cleanGlobal(INTERMEDIATE_USER_TOPIC);

        CLUSTER.deleteTopic(INTERMEDIATE_USER_TOPIC);
        Set<String> allTopics;
        ZkUtils zkUtils = null;
        try {
            zkUtils = ZkUtils.apply(CLUSTER.zKConnectString(),
                    30000,
                    30000,
                    JaasUtils.isZkSecurityEnabled());

            do {
                Utils.sleep(100);
                allTopics = new HashSet<>();
                allTopics.addAll(scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics()));
            } while (allTopics.contains(INTERMEDIATE_USER_TOPIC));
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() throws Exception {
        final Properties streamsConfiguration = prepareTest();
        final Properties resultTopicConsumerConfig = TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                APP_ID + "-standard-consumer-" + OUTPUT_TOPIC,
                LongDeserializer.class,
                LongDeserializer.class);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfiguration);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                resultTopicConsumerConfig,
                OUTPUT_TOPIC,
                10,
                60000);

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                "Streams Application consumer group did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // RESET
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfiguration);
        streams.cleanUp();
        cleanGlobal(null);
        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted(null);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                resultTopicConsumerConfig,
                OUTPUT_TOPIC,
                10,
                60000);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        cleanGlobal(null);
    }

    private Properties prepareTest() throws Exception {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + testNo);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + STREAMS_CONSUMER_TIMEOUT);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        return streamsConfiguration;
    }

    private void prepareInputData() throws Exception {
        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, StringSerializer.class);

        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "aaa")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "bbb")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "ccc")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "ddd")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "eee")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "fff")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "ggg")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "hhh")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "iii")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "jjj")), producerConfig, mockTime.milliseconds());
    }

    private KStreamBuilder setupTopologyWithIntermediateUserTopic(final String outputTopic2) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map(new KeyValueMapper<Long, String, KeyValue<Long, String>>() {
                @Override
                public KeyValue<Long, String> apply(final Long key, final String value) {
                    return new KeyValue<>(key, value);
                }
            })
            .groupByKey()
            .count("global-count")
            .to(Serdes.Long(), Serdes.Long(), OUTPUT_TOPIC);

        input.through(INTERMEDIATE_USER_TOPIC)
            .groupByKey()
            .count(TimeWindows.of(35).advanceBy(10), "count")
            .toStream()
            .map(new KeyValueMapper<Windowed<Long>, Long, KeyValue<Long, Long>>() {
                @Override
                public KeyValue<Long, Long> apply(final Windowed<Long> key, final Long value) {
                    return new KeyValue<>(key.window().start() + key.window().end(), value);
                }
            })
            .to(Serdes.Long(), Serdes.Long(), outputTopic2);

        return builder;
    }

    private KStreamBuilder setupTopologyWithoutIntermediateUserTopic() {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map(new KeyValueMapper<Long, String, KeyValue<Long, Long>>() {
            @Override
            public KeyValue<Long, Long> apply(final Long key, final String value) {
                return new KeyValue<>(key, key);
            }
        }).to(Serdes.Long(), Serdes.Long(), OUTPUT_TOPIC);

        return builder;
    }

    private void cleanGlobal(final String intermediateUserTopic) {
        final String[] parameters;
        if (intermediateUserTopic != null) {
            parameters = new String[]{
                "--application-id", APP_ID + testNo,
                "--bootstrap-server", CLUSTER.bootstrapServers(),
                "--zookeeper", CLUSTER.zKConnectString(),
                "--input-topics", INPUT_TOPIC,
                "--intermediate-topics", INTERMEDIATE_USER_TOPIC
            };
        } else {
            parameters = new String[]{
                "--application-id", APP_ID + testNo,
                "--bootstrap-server", CLUSTER.bootstrapServers(),
                "--zookeeper", CLUSTER.zKConnectString(),
                "--input-topics", INPUT_TOPIC
            };
        }
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(0, exitCode);
    }

    private void assertInternalTopicsGotDeleted(final String intermediateUserTopic) {
        final Set<String> expectedRemainingTopicsAfterCleanup = new HashSet<>();
        expectedRemainingTopicsAfterCleanup.add(INPUT_TOPIC);
        if (intermediateUserTopic != null) {
            expectedRemainingTopicsAfterCleanup.add(intermediateUserTopic);
        }
        expectedRemainingTopicsAfterCleanup.add(OUTPUT_TOPIC);
        expectedRemainingTopicsAfterCleanup.add(OUTPUT_TOPIC_2);
        expectedRemainingTopicsAfterCleanup.add(OUTPUT_TOPIC_2_RERUN);
        expectedRemainingTopicsAfterCleanup.add("__consumer_offsets");

        Set<String> allTopics;
        ZkUtils zkUtils = null;
        try {
            zkUtils = ZkUtils.apply(CLUSTER.zKConnectString(),
                30000,
                30000,
                JaasUtils.isZkSecurityEnabled());

            do {
                Utils.sleep(100);
                allTopics = new HashSet<>();
                allTopics.addAll(scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics()));
            } while (allTopics.size() != expectedRemainingTopicsAfterCleanup.size());
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
        assertThat(allTopics, equalTo(expectedRemainingTopicsAfterCleanup));
    }

    private class WaitUntilConsumerGroupGotClosed implements TestCondition {
        @Override
        public boolean conditionMet() {
            return adminClient.describeConsumerGroup(APP_ID + testNo).consumers().get().isEmpty();
        }
    }

}
