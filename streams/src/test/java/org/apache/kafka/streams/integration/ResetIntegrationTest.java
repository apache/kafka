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

import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.StreamsResetter;
import org.junit.Assert;
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
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final String APP_ID = "cleanup-integration-test";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String OUTPUT_TOPIC_2 = "outputTopic2";
    private static final String OUTPUT_TOPIC_2_RERUN = "outputTopic2_rerun";
    private static final String INTERMEDIATE_USER_TOPIC = "userTopic";

    private static final long STREAMS_CONSUMER_TIMEOUT = 2000L;
    private static final long CLEANUP_CONSUMER_TIMEOUT = 2000L;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_TOPIC);
        CLUSTER.createTopic(OUTPUT_TOPIC);
        CLUSTER.createTopic(OUTPUT_TOPIC_2);
        CLUSTER.createTopic(OUTPUT_TOPIC_2_RERUN);
        CLUSTER.createTopic(INTERMEDIATE_USER_TOPIC);
    }

    @Test
    public void testReprocessingFromScratchAfterReset() throws Exception {
        final Properties streamsConfiguration = prepareTest();
        final Properties resultTopicConsumerConfig = prepareResultConsumer();

        prepareInputData();
        final KStreamBuilder builder = setupTopology(OUTPUT_TOPIC_2);

        // RUN
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultTopicConsumerConfig, OUTPUT_TOPIC, 10);
        // receive only first values to make sure intermediate user topic is not consumed completely
        // => required to test "seekToEnd" for intermediate topics
        final KeyValue<Object, Object> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultTopicConsumerConfig, OUTPUT_TOPIC_2, 1).get(0);

        streams.close();

        // RESET
        Utils.sleep(STREAMS_CONSUMER_TIMEOUT);
        streams.cleanUp();
        cleanGlobal();
        assertInternalTopicsGotDeleted();
        Utils.sleep(CLEANUP_CONSUMER_TIMEOUT);

        // RE-RUN
        streams = new KafkaStreams(setupTopology(OUTPUT_TOPIC_2_RERUN), streamsConfiguration);
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultTopicConsumerConfig, OUTPUT_TOPIC, 10);
        final KeyValue<Object, Object> resultRerun2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultTopicConsumerConfig, OUTPUT_TOPIC_2_RERUN, 1).get(0);
        streams.close();

        assertThat(resultRerun, equalTo(result));
        assertThat(resultRerun2, equalTo(result2));
    }

    private Properties prepareTest() throws Exception {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("kafka-test").getPath());
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

    private Properties prepareResultConsumer() {
        final Properties resultTopicConsumerConfig = new Properties();
        resultTopicConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        resultTopicConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, APP_ID + "-standard-consumer-" + OUTPUT_TOPIC);
        resultTopicConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        resultTopicConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        resultTopicConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

        return resultTopicConsumerConfig;
    }

    private void prepareInputData() throws Exception {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "aaa")), producerConfig, 10L);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "bbb")), producerConfig, 20L);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "ccc")), producerConfig, 30L);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "ddd")), producerConfig, 40L);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "eee")), producerConfig, 50L);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "fff")), producerConfig, 60L);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "ggg")), producerConfig, 61L);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "hhh")), producerConfig, 62L);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(0L, "iii")), producerConfig, 63L);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(new KeyValue<>(1L, "jjj")), producerConfig, 64L);
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
            .countByKey("global-count");
        globalCounts.to(Serdes.Long(), Serdes.Long(), OUTPUT_TOPIC);

        final KStream<Long, Long> windowedCounts = input
            .through(INTERMEDIATE_USER_TOPIC)
            .map(new KeyValueMapper<Long, String, KeyValue<Long, String>>() {
                @Override
                public KeyValue<Long, String> apply(final Long key, final String value) {
                    // must sleep long enough to avoid processing the whole intermediate topic before application gets stopped
                    // => want to test "skip over" unprocessed records
                    // increasing the sleep time only has disadvantage that test run time is increased
                    Utils.sleep(1000);
                    return new KeyValue<>(key, value);
                }
            })
            .countByKey(TimeWindows.of("count", 35).advanceBy(10))
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

}
