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
import kafka.tools.StreamsResetter;
import kafka.utils.MockTime;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
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
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;

    private static final String APP_ID = "cleanup-integration-test";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String OUTPUT_TOPIC_2 = "outputTopic2";
    private static final String OUTPUT_TOPIC_2_RERUN = "outputTopic2_rerun";
    private static final String INTERMEDIATE_USER_TOPIC = "userTopic";

    private static final long STREAMS_CONSUMER_TIMEOUT = 2000L;
    private static final long CLEANUP_CONSUMER_TIMEOUT = 2000L;

    private final WaitUntilConsumerGroupGotClosed consumerGroupInactive = new WaitUntilConsumerGroupGotClosed();

    private AdminClient adminClient = null;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_TOPIC);
        CLUSTER.createTopic(OUTPUT_TOPIC);
        CLUSTER.createTopic(OUTPUT_TOPIC_2);
        CLUSTER.createTopic(OUTPUT_TOPIC_2_RERUN);
        CLUSTER.createTopic(INTERMEDIATE_USER_TOPIC);
    }

    @Before
    public void prepare() {
        adminClient = AdminClient.createSimplePlaintext(CLUSTER.bootstrapServers());
    }

    @After
    public void cleanup() {
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }
    }

    @Test
    public void testReprocessingFromScratchAfterReset() throws Exception {
        final Properties streamsConfiguration = prepareTest();
        final Properties resultTopicConsumerConfig = TestUtils.consumerConfig(
            CLUSTER.bootstrapServers(),
            APP_ID + "-standard-consumer-" + OUTPUT_TOPIC,
            LongDeserializer.class,
            LongDeserializer.class);

        prepareInputData();
        final KStreamBuilder builder = setupTopology(OUTPUT_TOPIC_2);

        // RUN
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC,
            10,
            60000);
        // receive only first values to make sure intermediate user topic is not consumed completely
        // => required to test "seekToEnd" for intermediate topics
        final KeyValue<Object, Object> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC_2,
            1
        ).get(0);

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactive, 5 * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group did not time out after " + (5 * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // RESET
        streams = new KafkaStreams(setupTopology(OUTPUT_TOPIC_2_RERUN), streamsConfiguration);
        streams.cleanUp();
        cleanGlobal();
        TestUtils.waitForCondition(consumerGroupInactive, 5 * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (5 * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted();

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC,
            10);
        final KeyValue<Object, Object> resultRerun2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC_2_RERUN,
            1
        ).get(0);
        streams.close();

        assertThat(resultRerun, equalTo(result));
        assertThat(resultRerun2, equalTo(result2));
    }

    private Properties prepareTest() throws Exception {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 8);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1);
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

    private KStreamBuilder setupTopology(final String outputTopic2) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        final KTable<Long, Long> globalCounts = input
            .map(new KeyValueMapper<Long, String, KeyValue<Long, String>>() {
                @Override
                public KeyValue<Long, String> apply(final Long key, final String value) {
                    return new KeyValue<>(key, value);
                }
            })
            .groupByKey()
            .count("global-count");
        globalCounts.to(Serdes.Long(), Serdes.Long(), OUTPUT_TOPIC);

        final KStream<Long, Long> windowedCounts = input
            .through(INTERMEDIATE_USER_TOPIC)
            .map(new KeyValueMapper<Long, String, KeyValue<Long, String>>() {
                private long sleep = 1000;

                @Override
                public KeyValue<Long, String> apply(final Long key, final String value) {
                    // must sleep long enough to avoid processing the whole intermediate topic before application gets stopped
                    // => want to test "skip over" unprocessed records
                    // increasing the sleep time only has disadvantage that test run time is increased
                    mockTime.sleep(sleep);
                    sleep *= 2;
                    return new KeyValue<>(key, value);
                }
            })
            .groupByKey()
            .count(TimeWindows.of(35).advanceBy(10), "count")
            .toStream()
            .map(new KeyValueMapper<Windowed<Long>, Long, KeyValue<Long, Long>>() {
                @Override
                public KeyValue<Long, Long> apply(final Windowed<Long> key, final Long value) {
                    return new KeyValue<>(key.window().start() + key.window().end(), value);
                }
            });
        windowedCounts.to(Serdes.Long(), Serdes.Long(), outputTopic2);

        return builder;
    }

    private void cleanGlobal() {
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(
            new String[]{
                "--application-id", APP_ID,
                "--bootstrap-server", CLUSTER.bootstrapServers(),
                "--zookeeper", CLUSTER.zKConnectString(),
                "--input-topics", INPUT_TOPIC,
                "--intermediate-topics", INTERMEDIATE_USER_TOPIC
            },
            cleanUpConfig);
        Assert.assertEquals(0, exitCode);
    }

    private void assertInternalTopicsGotDeleted() {
        final Set<String> expectedRemainingTopicsAfterCleanup = new HashSet<>();
        expectedRemainingTopicsAfterCleanup.add(INPUT_TOPIC);
        expectedRemainingTopicsAfterCleanup.add(INTERMEDIATE_USER_TOPIC);
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
            return adminClient.describeGroup(APP_ID).members().isEmpty();
        }
    }

}
