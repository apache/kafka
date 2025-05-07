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

import joptsimple.OptionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.api.ClusterTest;
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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.apache.kafka.tools.consumer.group.ConsumerGroupCommand;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.time.LocalDateTime.now;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.common.GroupState.EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class DeleteStreamsGroupTest {
    private static final String INPUT_TOPIC_PREFIX = "input-topic-";
    private static final String OUTPUT_TOPIC_PREFIX = "output-topic-";
    private static final String APP_ID_PREFIX = "delete-group-test-";
    public static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() throws InterruptedException {
        final Properties props = new Properties();
        props.setProperty(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams");
        cluster = new EmbeddedKafkaCluster(2, props);
        cluster.start();

        bootstrapServers = cluster.bootstrapServers();



//        streams =  new KafkaStreams(topology(), STREAMS_CONFIG);
//        streams.cleanUp();
//        streams.start();
//        produceMessagesOnTwoPartitions(RECORD_TOTAL, INPUT_TOPIC_1);
//        produceMessagesOnTwoPartitions(RECORD_TOTAL, INPUT_TOPIC_2);
//        TestUtils.waitForCondition(() -> streams.state().equals(KafkaStreams.State.RUNNING),
//            "Expected RUNNING state but streams is on " + streams.state());
    }

    private static Properties createStreamsConfig(String bootstrapServers, String appId) {
        Properties streamsConfig = new Properties();

        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        return streamsConfig;
    }

    @AfterEach
    public void closeCluster() throws ExecutionException, InterruptedException {
//        adminClient.deleteTopics(List.of(INPUT_TOPIC_1, INPUT_TOPIC_2, OUTPUT_TOPIC)).all().get();
        cluster.stop();
    }

    private static final int RECORD_TOTAL = 10;

//    @Test //todo
//    public void testResetWithUnrecognizedOption() {
//        final String[] args = new String[]{"--unrecognized-option", "--bootstrap-server", bootstrapServers, "--reset-offsets", "--all-topics", "--to-offset", "5"};
//        assertThrows(OptionException.class, () -> getStreamsGroupService(args));
//    }


    @Test
    public void testDeleteSingleGroup() throws Exception {
        final String appId = generateGroupAppId();
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--group", appId};

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        try (KafkaStreams streams = startKSApp(appId, service)) {

            /* test 1: delete NON_EMPTY Streams group */
            String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups);
            Map<String, Throwable> result = service.deleteGroups();

            assertTrue(output.contains("Group '" + appId + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
                "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Output was: (" + output + ")");

            assertNotNull(result.get(appId),
                "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");

            assertEquals(1, result.size());
            assertInstanceOf(GroupNotEmptyException.class,
                result.get(appId),
                "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Result was:(" + result + ")");

            /* test 2: delete EMPTY Streams group */
            stopKSApp(appId, streams, service);
            final Map<String, Throwable> emptyGrpRes = new HashMap<>();
            output = ToolsTestUtils.grabConsoleOutput(() -> emptyGrpRes.putAll(service.deleteGroups()));

            assertTrue(output.contains("Deletion of requested Streams groups ('" + appId + "') was successful."),
                "The Streams group could not be deleted as expected");
            assertTrue(output.contains("Deletion of all associated internal topics was successful."),
                "The internal topics could not be deleted as expected");
            assertEquals(1, emptyGrpRes.size());
            assertTrue(emptyGrpRes.containsKey(appId));
            assertNull(emptyGrpRes.get(appId), "The Streams group could not be deleted as expected");

            /* test 3: delete an already deleted Streams group (non-existing group) */
            result = service.deleteGroups();
            assertEquals(1, result.size());
            assertNotNull(result.get(appId));
            assertInstanceOf(GroupIdNotFoundException.class,
                result.get(appId),
                "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");
        }
    }

    @Test
    public void testDeleteMultipleGroup() throws Exception {
        final String appId1 = "appId1";//generateGroupAppId();
        final String appId2 = "appId2";generateGroupAppId();
        final String appId3 = generateGroupAppId();

        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--all-groups", appId1};

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams1 = startKSApp(appId1, service);
        KafkaStreams streams2 = startKSApp(appId2, service);


        /* test 1: delete NON_EMPTY Streams groups */
//        final Map<String, Throwable> result = new HashMap<>();
//        String output = ToolsTestUtils.grabConsoleOutput(() -> result.putAll(service.deleteGroups()));
//
//        assertTrue(output.contains("Group '" + appId1 + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
//            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Output was: (" + output + ")");
//        assertTrue(output.contains("Group '" + appId2 + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
//            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Output was: (" + output + ")");
//        assertTrue(output.contains("Group '" + appId3 + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
//            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Output was: (" + output + ")");
//
//        assertNotNull(result.get(appId1),
//            "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");
//        assertNotNull(result.get(appId2),
//            "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");
//        assertNotNull(result.get(appId3),
//            "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");
//
//        assertEquals(3, result.size());
//        assertInstanceOf(GroupNotEmptyException.class,
//            result.get(appId1),
//            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Result was:(" + result + ")");
//        assertInstanceOf(GroupNotEmptyException.class,
//            result.get(appId2),
//            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Result was:(" + result + ")");
//        assertInstanceOf(GroupNotEmptyException.class,
//            result.get(appId3),
//            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Result was:(" + result + ")");

        /* test 2: delete mix of EMPTY and NON_EMPTY Streams group */
        stopKSApp(appId1, streams1, service);
        final Map<String, Throwable> mixGrpsRes = new HashMap<>();
        String output = ToolsTestUtils.grabConsoleOutput(() -> mixGrpsRes.putAll(service.deleteGroups()));

        assertTrue(output.contains("Group '" + appId2 + "' could not be deleted due to:")
                && output.contains(Errors.NON_EMPTY_GROUP.message())
                && output.contains("These Streams groups were deleted successfully: '" + appId1 + "'"),
            "The Streams groups deletion did not work as expected");

//        assertTrue(output.contains("Deletion of requested Streams groups ('" + appId1 + "') was successful."),
//            "The Streams group could not be deleted as expected");
//        assertTrue(output.contains("Group '" + appId2 + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
//            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Output was: (" + output + ")");
////        assertTrue(output.contains("Group '" + appId3 + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
////            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting Streams group. Output was: (" + output + ")");
////        assertTrue(output.contains("Deletion of all associated internal topics was successful."),
////            "The internal topics could not be deleted as expected");

//        assertEquals(1, emptyGrpRes.size());
//        assertTrue(emptyGrpRes.containsKey(appId));
//        assertNull(emptyGrpRes.get(appId), "The Streams group could not be deleted as expected");
//
//        /* test 3: delete an already deleted Streams group (non-existing group) */
//        result = service.deleteGroups();
//        assertEquals(1, result.size());
//        assertNotNull(result.get(appId));
//        assertInstanceOf(GroupIdNotFoundException.class,
//            result.get(appId),
//            "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");

    }




//    private void deleteGroupAndAssert(String[] args,
//                                            List<TopicPartition> expectedTopicPartitions,
//                                            Errors expectedError) {
//        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
//            Map.Entry<Errors, Map<TopicPartition, Throwable>> res = service.deleteOffsets(APP_ID, List.of(INPUT_TOPIC_1));
//            Map<TopicPartition, Throwable> partitions = res.getValue();
//
//
//            assertEquals(expectedError, res.getKey());
//            if (expectedError == Errors.NONE) {
//                for (TopicPartition tp : expectedTopicPartitions) {
//                    assertNull(partitions.get(tp));
//                }
//            } else {
////                assertEquals(expectedTopicPartitions.size(), partitions.size());
//                for (TopicPartition tp : expectedTopicPartitions) {
//                    assertNotNull(partitions.get(tp));
//                    assertEquals(expectedError.exception(), partitions.get(tp).getCause());
//                }
//            }
//        }
//    }
//



//        try {
//            TestUtils.waitForCondition(() -> recordCount.get() == numOfCommittedMessages,
//                "Expected " + numOfCommittedMessages + " records processed but only got " + recordCount.get());
//        } catch (final Exception e) {
//            e.printStackTrace();
//        } finally {
//            assertEquals(numOfCommittedMessages, recordCount.get(), "Expected " + numOfCommittedMessages + " records processed but only got " + recordCount.get());
//            streams.close(new KafkaStreams.CloseOptions().leaveGroup(true));
//            adminClient.describeStreamsGroups(List.of(appId)).all().get().forEach((groupId, groupDescription) -> {
//                assertEquals(GroupState.EMPTY, groupDescription.groupState());
//                assertEquals(0, groupDescription.members().size());
//            });
//        }
//    }

    private StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = StreamsGroupCommandOptions.fromArgs(args);
        return new StreamsGroupCommand.StreamsGroupService(
            opts,
            Map.of(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }



    private String[] addTo(String[] args, String... extra) {
        List<String> res = new ArrayList<>(asList(args));
        res.addAll(asList(extra));
        return res.toArray(new String[0]);
    }


//    private Set<KafkaStreams> startKSApp(String... appIds) {
//        Set<KafkaStreams> streamsSet = new HashSet<>();
//        for(String appId : appIds) {
//            String inputTopic = generateRandomTopicId(INPUT_TOPIC_PREFIX);
//            String outputTopic = generateRandomTopicId(OUTPUT_TOPIC_PREFIX);
//
//            KafkaStreams streams =  new KafkaStreams(topology(inputTopic, outputTopic), createStreamsConfig(bootstrapServers, appId));
//            try (streams) {
//                streams.cleanUp();
//                streams.start();
//                TestUtils.waitForCondition(() -> streams.state().equals(KafkaStreams.State.RUNNING),
//                    "Expected RUNNING state but streams is on " + streams.state());
//                streamsSet.add(streams);
//            } catch (final Exception e) {
//                e.printStackTrace();
//            }
//        }
//        return streamsSet;
//    }

    private KafkaStreams startKSApp(String appId, StreamsGroupCommand.StreamsGroupService service) throws Exception {
        String inputTopic = generateRandomTopicId(INPUT_TOPIC_PREFIX);
        String outputTopic = generateRandomTopicId(OUTPUT_TOPIC_PREFIX);
//        produceMessages(RECORD_TOTAL, inputTopic);
        KafkaStreams streams = IntegrationTestUtils.getStartedStreams(createStreamsConfig(bootstrapServers, appId), builder(inputTopic, outputTopic), true);

        TestUtils.waitForCondition(
            () -> !service.collectGroupMembers(appId).isEmpty(),
            "The group did not initialize as expected."
        );

        return streams;
    }

    private Set<KafkaStreams> startKSApp(StreamsGroupCommand.StreamsGroupService service, String... appIds) {
        Set<KafkaStreams> streamsSet = new HashSet<>();
        for (String appId : appIds) {
            try {
                streamsSet.add(startKSApp(appId, service));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return streamsSet;
    }


    private void stopKSApp(String appId, KafkaStreams streams, StreamsGroupCommand.StreamsGroupService service) throws InterruptedException {
        if (streams != null) {
//            streams.close(new KafkaStreams.CloseOptions().leaveGroup(true));
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
        }
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

//    private void startKSApp(String... appIds, String topic1, String topic2) {
//        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
//        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
//
//
//        try (KafkaStreams streams = new KafkaStreams(topology(topic1, topic2), STREAMS_CONFIG); streams) {
//            streams.cleanUp();
//            streams.start();
//            produceMessagesOnTwoPartitions(RECORD_TOTAL, topic1);
//            produceMessagesOnTwoPartitions(RECORD_TOTAL, topic2);
//            TestUtils.waitForCondition(() -> streams.state().equals(KafkaStreams.State.RUNNING),
//                "Expected RUNNING state but streams is on " + streams.state());
//        } catch (final Exception e) {
//            e.printStackTrace();
//        }
//        //            TestUtils.waitForCondition(() -> streams.state().equals(KafkaStreams.State.NOT_RUNNING),
////                "Expected NOT RUNNING state but streams is on " + streams.state());
//    }

    private static void produceMessages(final int numOfMessages, final String topic) {

        // partition 0
        List<KeyValueTimestamp<String, String>> data = new ArrayList<>(numOfMessages);
        for (long v = 0; v < numOfMessages; ++v) {
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

    /**
     * Produces messages to two partitions of the specified topic.
     *
     * @param numOfMessages The number of messages to produce for each partition.
     * @param topic The topic to which the messages will be produced.
     */
    private static void produceMessagesOnTwoPartitions(final int numOfMessages, final String topic) {

        // partition 0
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

        // partition 1
        data = new ArrayList<>(numOfMessages);
        for (long v = 0; v < 10; ++v) {
            data.add(new KeyValueTimestamp<>(v + "1" + topic, v + "1", cluster.time.milliseconds()));
        }

        IntegrationTestUtils.produceSynchronously(
            TestUtils.producerConfig(bootstrapServers, StringSerializer.class, StringSerializer.class),
            false,
            topic,
            Optional.of(1),
            data
        );
    }

    private static StreamsBuilder builder (String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder;
    }
}
