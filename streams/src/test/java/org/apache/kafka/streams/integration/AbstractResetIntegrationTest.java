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
import kafka.utils.MockTime;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

abstract class AbstractResetIntegrationTest {
    private final static Logger log = LoggerFactory.getLogger(AbstractResetIntegrationTest.class);

    static final int NUM_BROKERS = 1;

    private static final String APP_ID = "cleanup-integration-test";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String OUTPUT_TOPIC_2 = "outputTopic2";
    private static final String OUTPUT_TOPIC_2_RERUN = "outputTopic2_rerun";
    private static final String INTERMEDIATE_USER_TOPIC = "userTopic";

    private static final long STREAMS_CONSUMER_TIMEOUT = 2000L;
    private static final long CLEANUP_CONSUMER_TIMEOUT = 2000L;
    private static final int TIMEOUT_MULTIPLIER = 5;

    private static AdminClient adminClient = null;
    private static KafkaAdminClient kafkaAdminClient = null;
    private static int testNo = 0;

    static EmbeddedKafkaCluster cluster;
    static String bootstrapServers;
    static MockTime mockTime;

    private final AbstractResetIntegrationTest.WaitUntilConsumerGroupGotClosed consumerGroupInactive = new AbstractResetIntegrationTest.WaitUntilConsumerGroupGotClosed();

    private class WaitUntilConsumerGroupGotClosed implements TestCondition {
        @Override
        public boolean conditionMet() {
            return adminClient.describeConsumerGroup(APP_ID, 0).consumers().get().isEmpty();
        }
    }

    static void afterClassGlobalCleanup() {
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }

        if (kafkaAdminClient != null) {
            kafkaAdminClient.close(10, TimeUnit.SECONDS);
            kafkaAdminClient = null;
        }
    }

    void beforePrepareTest() throws Exception {
        ++testNo;
        bootstrapServers = cluster.bootstrapServers();
        mockTime = cluster.time;

        Properties sslConfig = getClientSslConfig();
        if (sslConfig == null) {
            sslConfig = new Properties();
            sslConfig.put("bootstrap.servers", bootstrapServers);
        }

        if (adminClient == null) {
            adminClient = AdminClient.create(sslConfig);
        }

        if (kafkaAdminClient == null) {
            kafkaAdminClient =  (KafkaAdminClient) org.apache.kafka.clients.admin.AdminClient.create(sslConfig);
        }

        // busy wait until cluster (ie, ConsumerGroupCoordinator) is available
        while (true) {
            Thread.sleep(50);

            try {
                TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Test consumer group active even after waiting " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
            } catch (final TimeoutException e) {
                continue;
            }
            break;
        }

        prepareInputData();
    }

    Properties getClientSslConfig() {
        return null;
    }

    void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic() throws Exception {
        final Properties sslConfig = getClientSslConfig();
        final Properties streamsConfiguration = prepareTest();

        final Properties resultTopicConsumerConfig = new Properties();
        if (sslConfig != null) {
            resultTopicConsumerConfig.putAll(sslConfig);
        }
        resultTopicConsumerConfig.putAll(TestUtils.consumerConfig(
            bootstrapServers,
            APP_ID + "-standard-consumer-" + OUTPUT_TOPIC,
            LongDeserializer.class,
            LongDeserializer.class));

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfiguration);
        final KafkaStreams handlerReference = streams;
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                handlerReference.close(10, TimeUnit.SECONDS);
                log.error("Streams application failed: ", e);
            }
        });
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC,
            10);

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // RESET
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfiguration);
        final KafkaStreams handlerReference2 = streams;
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                handlerReference2.close(10, TimeUnit.SECONDS);
                log.error("Streams application failed: ", e);
            }
        });
        streams.cleanUp();
        cleanGlobal(null, sslConfig);
        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted(null);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC,
            10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        cleanGlobal(null, sslConfig);
    }

    void testReprocessingFromScratchAfterResetWithIntermediateUserTopic() throws Exception {
        cluster.createTopic(INTERMEDIATE_USER_TOPIC);

        final Properties sslConfig = getClientSslConfig();
        final Properties streamsConfiguration = prepareTest();

        final Properties resultTopicConsumerConfig = new Properties();
        if (sslConfig != null) {
            resultTopicConsumerConfig.putAll(sslConfig);
        }
        resultTopicConsumerConfig.putAll(TestUtils.consumerConfig(
            bootstrapServers,
            APP_ID + "-standard-consumer-" + OUTPUT_TOPIC,
            LongDeserializer.class,
            LongDeserializer.class));

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2), streamsConfiguration);
        final KafkaStreams handlerReference = streams;
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                handlerReference.close(10, TimeUnit.SECONDS);
                log.error("Streams application failed: ", e);
            }
        });
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC,
            10);
        // receive only first values to make sure intermediate user topic is not consumed completely
        // => required to test "seekToEnd" for intermediate topics
        final List<KeyValue<Long, Long>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC_2,
            40
        );

        streams.close();
        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
            "Streams Application consumer group did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

        // insert bad record to make sure intermediate user topic gets seekToEnd()
        mockTime.sleep(1);
        Properties producerConfig = sslConfig;
        if (producerConfig == null) {
            producerConfig = new Properties();
        }
        producerConfig.putAll(TestUtils.producerConfig(bootstrapServers, LongSerializer.class, StringSerializer.class));
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            INTERMEDIATE_USER_TOPIC,
            Collections.singleton(new KeyValue<>(-1L, "badRecord-ShouldBeSkipped")),
            producerConfig,
            mockTime.milliseconds());

        // RESET
        streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2_RERUN), streamsConfiguration);
        final KafkaStreams handlerReference2 = streams;
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                handlerReference2.close(10, TimeUnit.SECONDS);
                log.error("Streams application failed: ", e);
            }
        });
        streams.cleanUp();
        cleanGlobal(INTERMEDIATE_USER_TOPIC, sslConfig);
        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

        assertInternalTopicsGotDeleted(INTERMEDIATE_USER_TOPIC);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC,
            10);
        final List<KeyValue<Long, Long>> resultRerun2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC_2_RERUN,
            40
        );
        streams.close();

        assertThat(resultRerun, equalTo(result));
        assertThat(resultRerun2, equalTo(result2));

        TestUtils.waitForCondition(consumerGroupInactive, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
            "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
        cleanGlobal(INTERMEDIATE_USER_TOPIC, sslConfig);

        cluster.deleteTopicAndWait(INTERMEDIATE_USER_TOPIC);
    }

    private Properties prepareTest() throws IOException {
        Properties streamsConfiguration = getClientSslConfig();
        if (streamsConfiguration == null) {
            streamsConfiguration = new Properties();
        }
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + testNo);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + STREAMS_CONSUMER_TIMEOUT);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        return streamsConfiguration;
    }

    private void prepareInputData() throws Exception {
        cluster.deleteAndRecreateTopics(INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN);

        Properties producerConfig = getClientSslConfig();
        if (producerConfig == null) {
            producerConfig = new Properties();
        }
        producerConfig.putAll(TestUtils.producerConfig(bootstrapServers, LongSerializer.class, StringSerializer.class));

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

    private void cleanGlobal(final String intermediateUserTopic, final Properties sslConfig) throws Exception {
        // leaving --zookeeper arg here to ensure tool works if users add it
        final String[] parameters;
        if (intermediateUserTopic != null) {
            parameters = new String[]{
                "--application-id", APP_ID + testNo,
                "--bootstrap-servers", bootstrapServers,
                "--input-topics", INPUT_TOPIC,
                "--intermediate-topics", INTERMEDIATE_USER_TOPIC,
                "--zookeeper", "localhost:2181"
            };
        } else {
            if (sslConfig != null) {
                final File configFile = TestUtils.tempFile();
                final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
                writer.write(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG + "=SSL\n");
                writer.write(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG + "=" + sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) + "\n");
                writer.write(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG + "=" + sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG) + "\n");
                writer.close();

                parameters = new String[]{
                    "--application-id", APP_ID + testNo,
                    "--bootstrap-servers", bootstrapServers,
                    "--input-topics", INPUT_TOPIC,
                    "--config-file", configFile.getAbsolutePath()
                };
            } else {
                parameters = new String[]{
                    "--application-id", APP_ID + testNo,
                    "--bootstrap-servers", bootstrapServers,
                    "--input-topics", INPUT_TOPIC
                };
            }
        }
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

        final int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
        Assert.assertEquals(0, exitCode);
    }

    private void assertInternalTopicsGotDeleted(final String intermediateUserTopic) throws Exception {
        final Set<String> expectedRemainingTopicsAfterCleanup = new HashSet<>();
        expectedRemainingTopicsAfterCleanup.add(INPUT_TOPIC);
        if (intermediateUserTopic != null) {
            expectedRemainingTopicsAfterCleanup.add(intermediateUserTopic);
        }
        expectedRemainingTopicsAfterCleanup.add(OUTPUT_TOPIC);
        expectedRemainingTopicsAfterCleanup.add(OUTPUT_TOPIC_2);
        expectedRemainingTopicsAfterCleanup.add(OUTPUT_TOPIC_2_RERUN);
        expectedRemainingTopicsAfterCleanup.add("__consumer_offsets");

        final Set<String> allTopics = new HashSet<>();

        final ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        allTopics.addAll(kafkaAdminClient.listTopics(listTopicsOptions).names().get(30000, TimeUnit.MILLISECONDS));
        assertThat(allTopics, equalTo(expectedRemainingTopicsAfterCleanup));
    }

}
