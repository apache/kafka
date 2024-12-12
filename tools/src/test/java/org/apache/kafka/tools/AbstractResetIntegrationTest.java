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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
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
import org.apache.kafka.common.utils.Utils;
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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForEmptyConsumerGroup;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
public abstract class AbstractResetIntegrationTest {

    static EmbeddedKafkaCluster cluster;

    private static MockTime mockTime;
    protected static KafkaStreams streams;
    protected static Admin adminClient;

    abstract Map<String, Object> getClientSslConfig();

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

    private void prepareConfigs(final String appID) {
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

    void prepareTest(final TestInfo testInfo) throws Exception {
        final String appID = safeUniqueTestName(testInfo);
        prepareConfigs(appID);
        prepareEnvironment();

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT);

        cluster.deleteAllTopics();
        cluster.createTopics(INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN);

        add10InputElements();
    }

    void cleanupTest() throws Exception {
        Utils.closeQuietly(streams, "kafka streams");
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
        if (adminClient != null) {
            Utils.closeQuietly(adminClient, "admin client");
            adminClient = null;
        }
    }

    private void add10InputElements() {
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

    @Test
    public void testResetWhenInternalTopicsAreSpecified(final TestInfo testInfo) throws Exception {
        final String appID = safeUniqueTestName(testInfo);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithIntermediateTopic(true, OUTPUT_TOPIC_2), streamsConfig);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // RESET
        streams.cleanUp();

        final List<String> internalTopics = cluster.getAllTopicsInCluster().stream()
                .filter(StreamsResetter::matchesInternalTopicFormat)
                .collect(Collectors.toList());
        cleanGlobal(false,
                "--internal-topics",
                String.join(",", internalTopics.subList(1, internalTopics.size())),
                appID);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(internalTopics.get(0));
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic(final TestInfo testInfo) throws Exception {
        final String appID = safeUniqueTestName(testInfo);
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
        cleanGlobal(false, null, null, appID);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(null);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertEquals(result, resultRerun);

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null, appID);
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopic(final TestInfo testInfo) throws Exception {
        testReprocessingFromScratchAfterResetWithIntermediateUserTopic(false, testInfo);
    }

    @Test
    public void testReprocessingFromScratchAfterResetWithIntermediateInternalTopic(final TestInfo testInfo) throws Exception {
        testReprocessingFromScratchAfterResetWithIntermediateUserTopic(true, testInfo);
    }

    private void testReprocessingFromScratchAfterResetWithIntermediateUserTopic(final boolean useRepartitioned, final TestInfo testInfo) throws Exception {
        if (!useRepartitioned) {
            cluster.createTopic(INTERMEDIATE_USER_TOPIC);
        }

        final String appID = safeUniqueTestName(testInfo);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithIntermediateTopic(useRepartitioned, OUTPUT_TOPIC_2), streamsConfig);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        // receive only first values to make sure intermediate user topic is not consumed completely
        // => required to test "seekToEnd" for intermediate topics
        final List<KeyValue<Long, Long>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2, 40);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // insert bad record to make sure intermediate user topic gets seekToEnd()
        mockTime.sleep(1);
        final KeyValue<Long, String> badMessage = new KeyValue<>(-1L, "badRecord-ShouldBeSkipped");
        if (!useRepartitioned) {
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                INTERMEDIATE_USER_TOPIC,
                Collections.singleton(badMessage),
                producerConfig,
                mockTime.milliseconds());
        }

        // RESET
        streams = new KafkaStreams(setupTopologyWithIntermediateTopic(useRepartitioned, OUTPUT_TOPIC_2_RERUN), streamsConfig);
        streams.cleanUp();
        cleanGlobal(!useRepartitioned, null, null, appID);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(useRepartitioned ? null : INTERMEDIATE_USER_TOPIC);

        // RE-RUN
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        final List<KeyValue<Long, Long>> resultRerun2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2_RERUN, 40);
        streams.close();

        assertEquals(result, resultRerun);
        assertEquals(result2, resultRerun2);

        if (!useRepartitioned) {
            final Properties props = TestUtils.consumerConfig(cluster.bootstrapServers(), appID + "-result-consumer", LongDeserializer.class, StringDeserializer.class, commonClientConfig);
            final List<KeyValue<Long, String>> resultIntermediate = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(props, INTERMEDIATE_USER_TOPIC, 21);

            for (int i = 0; i < 10; i++) {
                assertEquals(resultIntermediate.get(i + 11), resultIntermediate.get(i));
            }
            assertEquals(badMessage, resultIntermediate.get(10));
        }

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(!useRepartitioned, null, null, appID);

        if (!useRepartitioned) {
            cluster.deleteTopic(INTERMEDIATE_USER_TOPIC);
        }
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
            Arrays.asList("--application-id", appID,
                    "--bootstrap-server", cluster.bootstrapServers(),
                    "--input-topics", INPUT_TOPIC
            ));
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
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));
        cleanUpConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name);

        return new StreamsResetter().execute(parameters, cleanUpConfig) == 0;
    }

    protected void cleanGlobal(final boolean withIntermediateTopics,
                             final String resetScenario,
                             final String resetScenarioArg,
                             final String appID) throws Exception {
        final boolean cleanResult = tryCleanGlobal(withIntermediateTopics, resetScenario, resetScenarioArg, appID);
        assertTrue(cleanResult);
    }

    protected void assertInternalTopicsGotDeleted(final String additionalExistingTopic) throws Exception {
        if (additionalExistingTopic != null) {
            cluster.waitForRemainingTopics(30000, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN,
                    Topic.GROUP_METADATA_TOPIC_NAME, additionalExistingTopic);
        } else {
            cluster.waitForRemainingTopics(30000, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN,
                    Topic.GROUP_METADATA_TOPIC_NAME);
        }
    }
}
