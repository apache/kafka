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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static java.time.LocalDateTime.now;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(600)
@Tag("integration")
public class DeleteStreamsGroupOffsetTest {
    private static final String TOPIC_PREFIX = "foo-";
    private static final String APP_ID_PREFIX = "streams-group-command-test";
    private static final Properties STREAMS_CONFIG = new Properties();
    private static final int RECORD_TOTAL = 10;
    public static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;
    private static Admin adminClient;

    @BeforeAll
    public static void startCluster() {
        final Properties props = new Properties();
        props.setProperty(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams");
        cluster = new EmbeddedKafkaCluster(2, props);
        cluster.start();

        bootstrapServers = cluster.bootstrapServers();
        adminClient = cluster.createAdminClient();

        createStreamsConfig(bootstrapServers);
    }

    private static void createStreamsConfig(String bootstrapServers) {
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        STREAMS_CONFIG.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
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
    public void testDeleteOffsetsOfStableStreamsGroupWithTopicPartition() throws Exception {
        final String group = generateRandomAppId();
        final String topic = generateRandomTopic();
        String[] args;
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", topic};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startApp(group, topic, service, GroupState.STABLE)) {
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(topic));
            assertEquals(Errors.GROUP_SUBSCRIBED_TO_TOPIC, res.getKey());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    @Test///////////?
//    public void testDeleteOffsetsOfStableStreamsGroupWithUnknownTopicPartition() {
//        final String group = generateRandomAppId();
//        final String topic = generateRandomTopic();
//        final String unknownTopic = "unknown-topic";
//        cluster.createTopic(unknownTopic, 1);
//        String[] args;
//        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", unknownTopic};
//        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startApp(group, topic, service, GroupState.EMPTY)) {
//            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(unknownTopic));
//            assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, res.getKey());
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

    @Test
    public void testDeleteOffsetsOfEmptyConsumerGroupWithTopicOnly() {
        final String group = generateRandomAppId();
        final String topic = generateRandomTopic();
        String[] args;
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", topic};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args); KafkaStreams streams = startApp(group, topic, service, GroupState.EMPTY)) {
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(topic));
            assertEquals(Errors.NONE, res.getKey());
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
        args = new String[]{"--bootstrap-server", bootstrapServers, "--delete-offsets", "--group", group, "--topic", topic};
        KafkaStreams streams = null;
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            streams = startApp(group, topic, service, GroupState.EMPTY);
            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(group, Collections.singletonList(topic));
            assertEquals(Errors.NONE, res.getKey());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (streams != null) {
                streams.close();
            }
        }
    }

    private String generateRandomTopic() {
        return TOPIC_PREFIX + TestUtils.randomString(10);
    }

    private String generateRandomAppId() {
        return APP_ID_PREFIX + TestUtils.randomString(10);
    }


    private void produceConsume(String appId, String topic1, String topic2, long numOfCommittedMessages) throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        cluster.createTopic(topic1, 1);
        cluster.createTopic(topic2, 1);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> inputStream1 = builder.stream(topic1);
        final KStream<String, String> inputStream2 = builder.stream(topic2);

        final AtomicInteger recordCount = new AtomicInteger(0);

        final KTable<String, String> valueCounts = inputStream1.merge(inputStream2)
            // Explicit repartition step with a custom internal topic name
            .groupBy((key, value) -> key, Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                () -> "()",
                (key, value, aggregate) -> aggregate + ",(" + key + ": " + value + ")",
                Materialized.as("aggregated_value"));

        valueCounts.toStream().peek((key, value) -> {
            if (recordCount.incrementAndGet() > numOfCommittedMessages) {
                throw new IllegalStateException("Crash on the " + numOfCommittedMessages + " record");
            }
        });


        final KafkaStreams streams =  new KafkaStreams(builder.build(), STREAMS_CONFIG);
        streams.cleanUp();
        streams.start();

        produceMessages(RECORD_TOTAL, topic1);
        produceMessages(RECORD_TOTAL, topic2);


        TestUtils.waitForCondition(() -> streams.state().equals(KafkaStreams.State.RUNNING),
            "Expected RUNNING state but streams is on " + streams.state());


        try {
            TestUtils.waitForCondition(() -> recordCount.get() == numOfCommittedMessages,
                "Expected " + numOfCommittedMessages + " records processed but only got " + recordCount.get());
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            assertEquals(numOfCommittedMessages, recordCount.get(), "Expected " + numOfCommittedMessages + " records processed but only got " + recordCount.get());
        }
    }

    private KafkaStreams startApp(String appId, String topic, StreamsGroupCommand.StreamsGroupService service, GroupState state) throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        cluster.createTopic(topic, 1);

        final KafkaStreams streams =  new KafkaStreams(topology(topic, generateRandomTopic()), STREAMS_CONFIG);
        streams.cleanUp();
        streams.start();

        TestUtils.waitForCondition(() -> streams.state().equals(KafkaStreams.State.RUNNING),
            "Expected RUNNING state but streams is on " + streams.state());
        switch (state) {
            case STABLE:
                TestUtils.waitForCondition(() -> service.collectGroupsState(appId).equals(GroupState.STABLE),
                    "Expected STABLE state.");
                break;
//            case PREPARING_REBALANCE:
//                produceConsume(appId, topic, generateRandomTopic(), RECORD_TOTAL / 2);
//                break;
//            case REBALANCING:
//                produceConsume(appId, topic, generateRandomTopic(), RECORD_TOTAL / 4);
//                break;
            case EMPTY:
                streams.close();
                streams.cleanUp();
                TestUtils.waitForCondition(() -> streams.state().equals(KafkaStreams.State.NOT_RUNNING),
                    "Expected NOT RUNNING state but streams is on " + streams.state());
                TestUtils.waitForCondition(() -> service.collectGroupsState(appId).equals(GroupState.EMPTY),
                    "Expected EMPTY state.");
                System.out.println("S********************************************" + service.collectGroupsState(appId));
                break;
//            default:
//                throw new IllegalStateException("Unexpected state: " + state);
        }

        return streams;
    }

    private static Topology topology(String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    /**
     * Produces messages to two partitions of the specified topic.
     *
     * @param numOfMessages The number of messages to produce for each partition.
     * @param topic The topic to which the messages will be produced.
     */
    private static void produceMessages(final int numOfMessages, final String topic) {

        List<KeyValueTimestamp<String, String>> data = new ArrayList<>(numOfMessages);
        for (long v = 0; v < numOfMessages; ++v) {
            data.add(new KeyValueTimestamp<>(v + "0" + topic, v + "0", cluster.time.milliseconds()));
        }

        IntegrationTestUtils.produceSynchronously(
            TestUtils.producerConfig(bootstrapServers, StringSerializer.class, StringSerializer.class),
            false,
            topic,
            Optional.of(0),
            data
        );
    }

    private StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = StreamsGroupCommandOptions.fromArgs(args);
        return new StreamsGroupCommand.StreamsGroupService(
            opts,
            Map.of(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }
}
