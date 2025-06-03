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
package org.apache.kafka.tools.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.GroupState.EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Timeout(600)
@Tag("integration")
public class DeleteStreamsGroupOffsetTest {
    private static final String TOPIC_PREFIX = "foo-";
    private static final String APP_ID_PREFIX = "streams-group-command-test";

    private static final int RECORD_TOTAL = 2;
    public static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;
    private static final String OUTPUT_TOPIC_PREFIX = "output-topic-";
    @BeforeAll
    public static void startCluster() {
        final Properties props = new Properties();
        props.setProperty(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams");
        props.setProperty(GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, "50");
        props.setProperty(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "50");
        props.setProperty(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, "50");
        props.setProperty(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "50");
        props.setProperty(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "100");
        props.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        props.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1");
        props.setProperty(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, "60");
        props.setProperty(GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "60");
        props.setProperty(GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, "60");

        cluster = new EmbeddedKafkaCluster(1, props);
        cluster.start();

        bootstrapServers = cluster.bootstrapServers();
    }

    private Properties createStreamsConfig(String bootstrapServers, String appId) {
        final Properties configs = new Properties();
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        configs.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
        configs.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        return configs;
    }

    @AfterAll
    public static void closeCluster() {
        cluster.stop();
    }

    @Test
    public void testDeleteOffsetsNonExistingGroup() {
        String group = "not-existing";
        String topic = "foo:1";
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", topic};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(topic));
            assertEquals(Errors.GROUP_ID_NOT_FOUND, res.getKey());
        }
    }

    @Test
    public void testDeleteOffsetsOfStableConsumerGroupWithTopicPartition() {
        final String group = generateRandomAppId();
        final String topic = generateRandomTopic();
        String topicPartition = topic + ":0";
        String[] args;
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", topicPartition};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startKSApp(group, topic, service)) {
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(topicPartition));
            assertError(res, topic, 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
            cluster.deleteTopics(topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeleteOffsetsOfStableConsumerGroupWithTopicOnly() {
        final String group = generateRandomAppId();
        final String topic = generateRandomTopic();
        String[] args;
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", topic};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startKSApp(group, topic, service)) {
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(topic));
            assertError(res, topic, -1, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
            cluster.deleteTopics(topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeleteOffsetsOfStableConsumerGroupWithUnknownTopicPartition() {
        final String group = generateRandomAppId();
        final String topic = generateRandomTopic();
        final String unknownTopicPartition = "unknown-topic:0";
        String[] args;
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", unknownTopicPartition};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startKSApp(group, topic, service)) {
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(unknownTopicPartition));
            assertError(res, topic, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            cluster.deleteTopics(topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeleteOffsetsOfEmptyConsumerGroupWithTopicPartition() {
        final String group = generateRandomAppId();
        final String topic = generateRandomTopic();
        final String topicPartition = topic + ":0";
        String[] args;
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", topicPartition};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startKSApp(group, topic, service)) {
            stopKSApp(group, topic, streams, service);
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(topicPartition));
            assertError(res, topic, 0, Errors.NONE);
            cluster.deleteTopics(topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeleteOffsetsOfEmptyConsumerGroupWithTopicOnly() {
        final String group = generateRandomAppId();
        final String topic = generateRandomTopic();
        String[] args;
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", topic};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startKSApp(group, topic, service)) {
            stopKSApp(group, topic, streams, service);
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(topic));
            assertError(res, topic, -1, Errors.NONE);
            cluster.deleteTopics(topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicPartition() {
        final String group = generateRandomAppId();
        final String topic = generateRandomTopic();
        final String unknownTopicPartition = "unknown-topic:0";
        String[] args;
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", unknownTopicPartition};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startKSApp(group, topic, service)) {
            stopKSApp(group, topic, streams, service);
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(unknownTopicPartition));
            assertError(res, topic, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            cluster.deleteTopics(topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeleteOffsetsOfEmptyConsumerGroupWithUnknownTopicOnly() {
        final String group = generateRandomAppId();
        final String topic = generateRandomTopic();
        final String unknownTopic = "unknown-topic";
        String[] args;
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", unknownTopic};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startKSApp(group, topic, service)) {
            stopKSApp(group, topic, streams, service);
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(unknownTopic));
            assertError(res, topic, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            cluster.deleteTopics(topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertError(Map.Entry<Errors, Map<TopicPartition, Throwable>> res,
                          String inputTopic,
                          int inputPartition,
                          Errors expectedError) {
        Errors topLevelError = res.getKey();
        Map<TopicPartition, Throwable> partitions = res.getValue();
        TopicPartition tp = new TopicPartition(inputTopic, 0);
        // Partition level error should propagate to top level, unless this is due to a missed partition attempt.
        if (inputPartition >= 0) {
            assertEquals(expectedError, topLevelError);
        }
        if (expectedError == Errors.NONE)
            assertNull(partitions.get(tp));
        else
            assertEquals(expectedError.exception(), partitions.get(tp).getCause());
    }

    private String generateRandomTopic() {
        return TOPIC_PREFIX + TestUtils.randomString(10);
    }

    private String generateRandomAppId() {
        return APP_ID_PREFIX + TestUtils.randomString(10);
    }

    private void stopKSApp(String appId, String topic, KafkaStreams streams, StreamsGroupCommand.StreamsGroupService service) throws InterruptedException {
        if (streams != null) {
            KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
            closeOptions.timeout(Duration.ofSeconds(30));
            closeOptions.leaveGroup(true);
            streams.close(closeOptions);
            streams.cleanUp();

            TestUtils.waitForCondition(
                () -> checkGroupState(service, appId, EMPTY),
                "The group did not become empty as expected."
            );
            TestUtils.waitForCondition(
                () -> service.collectGroupMembers(appId).isEmpty(),
                "The group size is not zero as expected."
            );

//            TestUtils.waitForCondition(
//                () -> !service.isGroupSubscribedToTopic(appId, topic),
//                "Group did not unsubscribe from topic " + topic + " in time."
//            );
        }
    }

    private KafkaStreams startKSApp(String appId, String inputTopic, StreamsGroupCommand.StreamsGroupService service) throws Exception {
        String outputTopic = generateRandomTopicId(OUTPUT_TOPIC_PREFIX);
        StreamsBuilder builder = builder(inputTopic, outputTopic);
        produceMessages(inputTopic);

        final KStream<String, String> inputStream = builder.stream(inputTopic);

        final AtomicInteger recordCount = new AtomicInteger(0);
        final KTable<String, String> valueCounts = inputStream
            .groupByKey()
            .aggregate(
                () -> "()",
                (key, value, aggregate) -> aggregate + ",(" + key + ": " + value + ")",
                Materialized.as("aggregated_value"));

        valueCounts.toStream().peek((key, value) -> {
            if (recordCount.incrementAndGet() > RECORD_TOTAL) {
                throw new IllegalStateException("Crash on the " + RECORD_TOTAL + " record");
            }
        });

        KafkaStreams streams = IntegrationTestUtils.getStartedStreams(createStreamsConfig(bootstrapServers, appId), builder, true);

        TestUtils.waitForCondition(
            () -> !service.collectGroupMembers(appId).isEmpty(),
            "The group did not initialize as expected."
        );
        TestUtils.waitForCondition(
            () -> checkGroupState(service, appId, GroupState.STABLE),
            "The group did not become stable as expected."
        );
//        TestUtils.waitForCondition(() -> recordCount.get() == RECORD_TOTAL,
//            "Expected " + RECORD_TOTAL + " records processed but only got " + recordCount.get());

        return streams;
    }

    private String generateRandomTopicId(String prefix) {
        return prefix + TestUtils.randomString(10);
    }

    private String generateGroupAppId() {
        return APP_ID_PREFIX + TestUtils.randomString(10);
    }

    private boolean checkGroupState(StreamsGroupCommand.StreamsGroupService service, String groupId, GroupState state) throws Exception {
        return Objects.equals(service.collectGroupState(groupId), state);
    }

    private static StreamsBuilder builder(String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder;
    }

    private static void produceMessages(final String topic) {
        List<KeyValueTimestamp<String, String>> data = new ArrayList<>(RECORD_TOTAL);
        for (long v = 0; v < RECORD_TOTAL; ++v) {
            data.add(new KeyValueTimestamp<>(v + "0" + topic, v + "0", cluster.time.milliseconds()));
        }

        IntegrationTestUtils.produceSynchronously(
            TestUtils.producerConfig(bootstrapServers, StringSerializer.class, StringSerializer.class),
            false,
            topic,
            Optional.empty(),
            data
        );
    }

    private StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = StreamsGroupCommandOptions.fromArgs(args);
        return new StreamsGroupCommand.StreamsGroupService(
            opts, cluster.createAdminClient());

    }
}
