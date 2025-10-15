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
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.test.TestUtils;

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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static java.time.LocalDateTime.now;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class ResetStreamsGroupOffsetTest {
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

    @AfterEach
    public void deleteTopicsAndGroups() {
        try (final Admin adminClient = cluster.createAdminClient()) {
            // delete all topics
            final Set<String> topics = adminClient.listTopics().names().get();
            adminClient.deleteTopics(topics).all().get();
            // delete all groups
            List<String> groupIds =
                adminClient.listGroups(ListGroupsOptions.forStreamsGroups().timeoutMs(1000)).all().get()
                    .stream().map(GroupListing::groupId).toList();
            adminClient.deleteStreamsGroups(groupIds).all().get();
        } catch (final UnknownTopicOrPartitionException ignored) {
        } catch (final ExecutionException | InterruptedException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw new RuntimeException(e);
            }
        }
    }

    @AfterAll
    public static void closeCluster() {
        cluster.stop();
    }

    @Test
    public void testResetWithUnrecognizedOption() {
        String[] args = new String[]{"--unrecognized-option", "--bootstrap-server", bootstrapServers, "--reset-offsets", "--all-groups", "--all-input-topics", "--to-offset", "5"};
        assertThrows(OptionException.class, () -> getStreamsGroupService(args));
    }

    @Test
    public void testResetOffsetsWithoutGroupOption() {
        final String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--to-offset", "5"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [reset-offsets] takes one of these options: [all-groups], [group]"));
            exited.set(true);
        }));
        try {
            getStreamsGroupService(args);
        } finally {
            assertTrue(exited.get());
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testResetOffsetsWithDeleteInternalTopicsOption() {
        final String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--all-groups", "--all-input-topics", "--to-offset", "5", "--delete-all-internal-topics"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [delete-all-internal-topics] takes [execute] when [reset-offsets] is used"));
            exited.set(true);
        }));
        try {
            getStreamsGroupService(args);
        } finally {
            assertTrue(exited.get());
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testResetOffset() throws Exception {
        final String appId = generateRandomAppId();
        final String topic1 = generateRandomTopic();
        final String topic2 = generateRandomTopic();
        final int  numOfPartitions = 2;
        String[] args;
        produceConsumeShutdown(appId, topic1, topic2, RECORD_TOTAL * numOfPartitions * 2);
        produceMessagesOnTwoPartitions(RECORD_TOTAL, topic1);
        produceMessagesOnTwoPartitions(RECORD_TOTAL, topic2);
        /////////////////////////////////////////////// Specific topic (--topic topic1) ////////////////////////////////////////////////
        // reset to specific offset, offset already on 10
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--to-offset", "5"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 5L, 10L, 0, 1);

        resetForNextTest(appId, 10L, topic1);

        // reset to specific offset when after end offset, offset already on 10
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--to-offset", "30"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 20L, 10L, 0, 1);

        // reset to specific offset when before begin offset, offset already on 20
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--to-offset", "-30"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 0L, 20L, 0, 1);

        resetForNextTest(appId, 10L, topic1);

        // reset to specific date time
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
        LocalDateTime dateTime = now().minusDays(1);
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--to-datetime", format.format(dateTime)};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 0L, 10L, 0, 1);

        resetForNextTest(appId, 10L, topic1);

        // reset by duration to earliest
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--by-duration", "PT5M"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 0L, 10L, 0, 1);

        resetForNextTest(appId, 10L, topic1);

        // reset to earliest
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--to-earliest"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 0L, 10L, 0, 1);

        resetForNextTest(appId, 10L, topic1);

        // reset to latest
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--to-latest"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 20L, 10L, 0, 1);

        resetForNextTest(appId, 5L, topic1);

        // reset to current
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--to-current"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 5L, 5L, 0, 1);

        // reset offset shift+. The current offset is 5, as of the prev test is executed (by --execute)
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--shift-by", "3"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 8L, 5L, 0, 1);

        // reset offset shift-. The current offset is 8, as of the prev test is executed (by --execute)
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--shift-by", "-3"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 5L, 8L, 0, 1);

        // reset offset shift by lower than earliest. The current offset is 5, as of the prev test is executed (by --execute)
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--shift-by", "-150"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 0L, 5L, 0, 1);

        // reset offset shift by higher than latest. The current offset is 0, as of the prev test is executed (by --execute)
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--shift-by", "150"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 20L, 0L, 0, 1);

        // export to file
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--to-offset", "5", "--export"};
        File file = TestUtils.tempFile("reset", ".csv");
        Map<TopicPartition, Long> exp = Map.of(new TopicPartition(topic1, 0), 5L, new TopicPartition(topic1, 1), 5L);
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            Map<String, Map<TopicPartition, OffsetAndMetadata>> exportedOffsets = service.resetOffsets();
            writeContentToFile(file, service.exportOffsetsToCsv(exportedOffsets));

            assertEquals(exp, toOffsetMap(exportedOffsets.get(appId)));
        }
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--from-file", file.getCanonicalPath()};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            Map<String, Map<TopicPartition, OffsetAndMetadata>> importedOffsets = service.resetOffsets();
            assertEquals(exp, toOffsetMap(importedOffsets.get(appId)));
        }

        ///////////////////////////////////////// Specific topic and partition (--topic topic1, --topic topic2) /////////////////////////////////////////
        resetForNextTest(appId, 10L, topic1);

        // reset to specific offset
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1 + ":1", "--to-offset", "5"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, 5L, 10L, 1);

        resetForNextTest(appId, 10L, topic1);

        // reset both partitions of topic1 and topic2:1 to specific offset
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId,
            "--input-topic", topic1,  "--input-topic", topic2 + ":1", "--to-offset", "5"};
        final Map<TopicPartition, Long> expectedOffsets = Map.of(
            new TopicPartition(topic1, 0), 5L,
            new TopicPartition(topic1, 1), 5L,
            new TopicPartition(topic2, 1), 5L);

        resetOffsetsAndAssert(args, appId, List.of(topic1, topic2), expectedOffsets,
            Map.of(
                new TopicPartition(topic1, 0), 10L,
                new TopicPartition(topic1, 1), 10L,
                new TopicPartition(topic2, 0), 10L,
                new TopicPartition(topic2, 1), 10L));
        resetOffsetsAndAssert(addTo(args, "--execute"), appId, List.of(topic1, topic2), expectedOffsets,
            Map.of(new TopicPartition(topic1, 0), 5L,
                new TopicPartition(topic1, 1), 5L,
                new TopicPartition(topic2, 0), 10L,
                new TopicPartition(topic2, 1), 5L));

        ///////////////////////////////////////// All topics (--all-input-topics) /////////////////////////////////////////
        resetForNextTest(appId, 10L, topic1, topic2);

        // reset to specific offset
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--all-input-topics", "--to-offset", "5"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, topic2, 5L, 10L);

        resetForNextTest(appId, 10L, topic1, topic2);

        // reset to specific offset with two --topic options
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--input-topic", topic2, "--to-offset", "5"};
        resetOffsetsAndAssertForDryRunAndExecute(args, appId, topic1, topic2, 5L, 10L);

        resetForNextTest(appId, 10L, topic1, topic2);

        // export to file
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--all-input-topics", "--to-offset", "5", "--export"};
        file = TestUtils.tempFile("reset-all", ".csv");
        exp = Map.of(new TopicPartition(topic1, 0), 5L,
            new TopicPartition(topic1, 1), 5L,
            new TopicPartition(topic2, 0), 5L,
            new TopicPartition(topic2, 1), 5L);
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            Map<String, Map<TopicPartition, OffsetAndMetadata>> exportedOffsets = service.resetOffsets();
            writeContentToFile(file, service.exportOffsetsToCsv(exportedOffsets));

            assertEquals(exp, toOffsetMap(exportedOffsets.get(appId)));
        }
        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--input-topic", topic1, "--from-file", file.getCanonicalPath()};
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            Map<String, Map<TopicPartition, OffsetAndMetadata>> importedOffsets = service.resetOffsets();

            assertEquals(exp, toOffsetMap(importedOffsets.get(appId)));
        }

        // assert that the internal topics are not deleted
        assertEquals(2, getInternalTopics(appId).size());
    }

    @Test
    public void testResetOffsetsWithDeleteSpecifiedInternalTopics() throws Exception {
        final String appId = generateRandomAppId();
        final String internalTopic = appId + "-aggregated_value-changelog";
        final String topic1 = generateRandomTopic();
        final String topic2 = generateRandomTopic();
        final int numOfPartitions = 2;
        String[] args;
        produceConsumeShutdown(appId, topic1, topic2, RECORD_TOTAL * numOfPartitions * 2);
        produceMessagesOnTwoPartitions(RECORD_TOTAL, topic1);
        produceMessagesOnTwoPartitions(RECORD_TOTAL, topic2);

        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--all-input-topics", "--execute", "--to-offset", "5",
            "--delete-internal-topic", internalTopic
        };

        resetOffsetsAndAssertInternalTopicDeletion(args, appId, internalTopic);
    }

    @Test
    public void testResetOffsetsWithDeleteAllInternalTopics() throws Exception {
        final String appId = generateRandomAppId();
        final String topic1 = generateRandomTopic();
        final String topic2 = generateRandomTopic();
        final int numOfPartitions = 2;
        String[] args;
        produceConsumeShutdown(appId, topic1, topic2, RECORD_TOTAL * numOfPartitions * 2);
        produceMessagesOnTwoPartitions(RECORD_TOTAL, topic1);
        produceMessagesOnTwoPartitions(RECORD_TOTAL, topic2);

        args = new String[]{"--bootstrap-server", bootstrapServers, "--reset-offsets", "--group", appId, "--all-input-topics", "--delete-all-internal-topics", "--execute", "--to-offset", "5"};
        resetOffsetsAndAssertInternalTopicDeletion(args, appId);
    }

    private void resetForNextTest(String appId, long desiredOffset, String... topics) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (String topic : topics) {
            offsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(desiredOffset));
            offsets.put(new TopicPartition(topic, 1), new OffsetAndMetadata(desiredOffset));
        }
        adminClient.alterStreamsGroupOffsets(appId, offsets).all().get();
        Map<TopicPartition, Long> committedOffsets = committedOffsets(List.of(topics), appId);
        for (TopicPartition tp: offsets.keySet()) {
            assertEquals(desiredOffset, committedOffsets.get(tp));
        }
    }

    private void AssertCommittedOffsets(String appId,
                                        String topic,
                                        long expectedCommittedOffset,
                                        int... partitions) throws ExecutionException, InterruptedException {
        List<TopicPartition> affectedTPs = Arrays.stream(partitions)
            .mapToObj(partition -> new TopicPartition(topic, partition))
            .toList();
        Map<TopicPartition, Long> committedOffsets = committedOffsets(List.of(topic), appId);
        for (TopicPartition tp: affectedTPs) {
            assertEquals(expectedCommittedOffset, committedOffsets.get(tp));
        }
    }

    private void AssertCommittedOffsets(String appId,
                                        String topic1,
                                        String topic2,
                                        long expectedCommittedOffset) throws ExecutionException, InterruptedException {
        TopicPartition tp10 = new TopicPartition(topic1, 0);
        TopicPartition tp11 = new TopicPartition(topic2, 0);
        TopicPartition tp20 = new TopicPartition(topic1, 1);
        TopicPartition tp21 = new TopicPartition(topic2, 1);
        Map<TopicPartition, Long> committedOffsets = committedOffsets(List.of(topic1, topic2), appId);
        assertEquals(Map.of(
            tp10, expectedCommittedOffset,
            tp20, expectedCommittedOffset,
            tp11, expectedCommittedOffset,
            tp21, expectedCommittedOffset), committedOffsets);
    }

    /**
     * Resets offsets for a specific topic and partition(s) and verifies the results.
     *
     * <p>This method performs the following steps:</p>
     * <ul>
     *   <li>Resets offsets for the specified topic and partitions using the provided arguments.</li>
     *   <li>Asserts that the reset offsets match the expected offsets.</li>
     *   <li>Asserts that the committed offsets match the expected committed offsets.</li>
     * </ul>
     *
     * @param args The command-line arguments for resetting offsets.
     * @param appId The application ID for the Kafka Streams application.
     * @param topic The topic for which offsets will be reset.
     * @param expectedOffset The expected offset value after the reset.
     * @param expectedCommittedOffset The expected committed offset value after the reset.
     * @param partitions The partitions of the topic to reset offsets for.
     * @throws ExecutionException If an error occurs during the execution of the reset operation.
     * @throws InterruptedException If the thread is interrupted during the reset operation.
     */
    private void resetOffsetsAndAssert(String[] args,
                                       String appId,
                                       String topic,
                                       long expectedOffset,
                                       long expectedCommittedOffset,
                                       int... partitions) throws ExecutionException, InterruptedException {
        Map<String, Map<TopicPartition, Long>> resetOffsetsResultByGroup;
        Map<TopicPartition, Long> expectedOffetMap = Arrays.stream(partitions)
            .boxed()
            .collect(Collectors.toMap(
                partition -> new TopicPartition(topic, partition),
                partition -> expectedOffset
            ));
        Map<String, Map<TopicPartition, Long>> expectedResetResults = Map.of(appId, expectedOffetMap);
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            resetOffsetsResultByGroup = convertOffsetsToLong(service.resetOffsets());
        }
        // assert that the reset offsets are as expected
        assertEquals(expectedResetResults, resetOffsetsResultByGroup);
        assertEquals(expectedResetResults.size(), resetOffsetsResultByGroup.size());
        // assert that the committed offsets are as expected
        AssertCommittedOffsets(appId, topic, expectedCommittedOffset, partitions);
    }

    private void resetOffsetsAndAssertInternalTopicDeletion(String[] args, String appId, String... specifiedInternalTopics) throws InterruptedException {
        List<String> specifiedInternalTopicsList = List.of(specifiedInternalTopics);
        Set<String> allInternalTopics = getInternalTopics(appId);
        specifiedInternalTopicsList.forEach(allInternalTopics::remove);

        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            service.resetOffsets();
        }

        // assert that the internal topics are deleted
        if (specifiedInternalTopics.length > 0) {
            TestUtils.waitForCondition(
                () -> getInternalTopics(appId).size() == allInternalTopics.size(),
                30_000, "Internal topics were not deleted as expected after reset"
            );
            // verify that the specified internal topics were deleted
            Set<String> internalTopicsAfterReset = getInternalTopics(appId);
            specifiedInternalTopicsList.forEach(topic ->
                assertFalse(internalTopicsAfterReset.contains(topic),
                    "Internal topic '" + topic + "' was not deleted as expected after reset")
            );

        } else {
            TestUtils.waitForCondition(() -> {
                Set<String> internalTopicsAfterReset = getInternalTopics(appId);
                return internalTopicsAfterReset.isEmpty();
            }, 30_000, "Internal topics were not deleted after reset");
        }
    }

    private Set<String> getInternalTopics(String appId) {
        try {
            Set<String> topics = adminClient.listTopics().names().get();
            return topics.stream()
                .filter(topic -> topic.startsWith(appId + "-"))
                .filter(topic -> topic.endsWith("-changelog") || topic.endsWith("-repartition"))
                .collect(Collectors.toSet());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Resets offsets for two topics and verifies the results.
     *
     * <p>This method performs the following steps:</p>
     * <ul>
     *   <li>Resets offsets for the specified topics using the provided arguments.</li>
     *   <li>Asserts that the reset offsets match the expected offsets.</li>
     *   <li>Asserts that the committed offsets match the expected committed offsets.</li>
     * </ul>
     *
     * @param args The command-line arguments for resetting offsets.
     * @param appId The application ID for the Kafka Streams application.
     * @param topic1 The first topic for which offsets will be reset.
     * @param topic2 The second topic for which offsets will be reset.
     * @param expectedOffset The expected offset value after the reset.
     * @param expectedCommittedOffset The expected committed offset value after the reset.
     * @throws ExecutionException If an error occurs during the execution of the reset operation.
     * @throws InterruptedException If the thread is interrupted during the reset operation.
     */
    private void resetOffsetsAndAssert(String[] args,
                                       String appId,
                                       String topic1,
                                       String topic2,
                                       long expectedOffset,
                                       long expectedCommittedOffset) throws ExecutionException, InterruptedException {
        Map<String, Map<TopicPartition, Long>> resetOffsetsResultByGroup;
        Map<String, Map<TopicPartition, Long>> expectedResetResults = Map.of(
            appId, Map.of(
                new TopicPartition(topic1, 0), expectedOffset,
                new TopicPartition(topic2, 0), expectedOffset,
                new TopicPartition(topic1, 1), expectedOffset,
                new TopicPartition(topic2, 1), expectedOffset
            )
        );

        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            resetOffsetsResultByGroup = convertOffsetsToLong(service.resetOffsets());
        }
        // assert that the reset offsets are as expected
        assertEquals(expectedResetResults, resetOffsetsResultByGroup);
        assertEquals(expectedResetResults.size(), resetOffsetsResultByGroup.size());
        // assert that the committed offsets are as expected
        AssertCommittedOffsets(appId, topic1, topic2, expectedCommittedOffset);
    }

    /**
     * Resets offsets for the specified topics and verifies the results.
     *
     * <p>This method performs the following steps:</p>
     * <ul>
     *   <li>Resets offsets for the given topics using the provided arguments.</li>
     *   <li>Asserts that the reset offsets match the expected offsets.</li>
     *   <li>Asserts that the committed offsets match the expected committed offsets.</li>
     * </ul>
     *
     * @param args The command-line arguments for resetting offsets.
     * @param appId The application ID for the Kafka Streams application.
     * @param topics The list of topics for which offsets will be reset.
     * @param expectedOffsets A map of expected offsets for each topic partition after the reset.
     * @param expectedCommittedOffsets A map of expected committed offsets for each topic partition after the reset.
     * @throws ExecutionException If an error occurs during the execution of the reset operation.
     * @throws InterruptedException If the thread is interrupted during the reset operation.
     */
    private void resetOffsetsAndAssert(String[] args,
                                       String appId,
                                       List<String> topics,
                                       Map<TopicPartition, Long> expectedOffsets,
                                       Map<TopicPartition, Long> expectedCommittedOffsets) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> resetOffsetsResult;

        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            resetOffsetsResult = convertOffsetsToLong(service.resetOffsets()).get(appId);
        }
        // assert that the reset offsets are as expected
        assertEquals(expectedOffsets, resetOffsetsResult);
        assertEquals(expectedOffsets.size(), resetOffsetsResult.size());
        // assert that the committed offsets are as expected
        assertEquals(expectedCommittedOffsets, committedOffsets(topics, appId));
    }

    private void resetOffsetsAndAssertForDryRunAndExecute(String[] args,
                                                          String appId,
                                                          String topic,
                                                          long expectedOffset,
                                                          long expectedCommittedOffset,
                                                          int... partitions) throws ExecutionException, InterruptedException {
        resetOffsetsAndAssert(args, appId, topic, expectedOffset, expectedCommittedOffset, partitions);
        resetOffsetsAndAssert(addTo(args, "--dry-run"), appId, topic,  expectedOffset, expectedCommittedOffset, partitions);
        resetOffsetsAndAssert(addTo(args, "--execute"), appId, topic, expectedOffset, expectedOffset, partitions);
    }

    private void resetOffsetsAndAssertForDryRunAndExecute(String[] args,
                                       String appId,
                                       String topic1,
                                       String topic2,
                                       long expectedOffset,
                                       long expectedCommittedOffset) throws ExecutionException, InterruptedException {
        resetOffsetsAndAssert(args, appId, topic1, topic2, expectedOffset, expectedCommittedOffset);
        resetOffsetsAndAssert(addTo(args, "--dry-run"), appId, topic1, topic2, expectedOffset, expectedCommittedOffset);
        resetOffsetsAndAssert(addTo(args, "--execute"), appId, topic1, topic2, expectedOffset, expectedOffset);
    }

    private Map<TopicPartition, Long> committedOffsets(List<String> topics,
                                                       String group) throws ExecutionException, InterruptedException {
        return adminClient.listConsumerGroupOffsets(group)
            .all().get()
            .get(group).entrySet()
            .stream()
            .filter(e -> topics.contains(e.getKey().topic()))
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private static Map<String, Map<TopicPartition, Long>> convertOffsetsToLong(Map<String, Map<TopicPartition, OffsetAndMetadata>> map) {
        return map.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, e1 -> e1.getValue().offset()))));
    }

    private Map<TopicPartition, Long> toOffsetMap(Map<TopicPartition, OffsetAndMetadata> map) {
        return map.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = StreamsGroupCommandOptions.fromArgs(args);
        return new StreamsGroupCommand.StreamsGroupService(
            opts,
            Map.of(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private void writeContentToFile(File file, String content) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            bw.write(content);
        }
    }

    private String[] addTo(String[] args, String... extra) {
        List<String> res = new ArrayList<>(List.of(args));
        res.addAll(List.of(extra));
        return res.toArray(new String[0]);
    }

    private String generateRandomTopic() {
        return TOPIC_PREFIX + TestUtils.randomString(10);
    }

    private String generateRandomAppId() {
        return APP_ID_PREFIX + TestUtils.randomString(10);
    }

    /**
     * Produces messages to two partitions of the specified topic and consumes them.
     *
     * @param appId The application ID for the Kafka Streams application.
     * @param topic1 The first topic to produce and consume messages from.
     * @param topic2 The second topic to produce and consume messages from.
     * @param numOfCommittedMessages The number of committed messages to process before shutting down.
     */
    private void produceConsumeShutdown(String appId, String topic1, String topic2, long numOfCommittedMessages) throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        cluster.createTopic(topic1, 2);
        cluster.createTopic(topic2, 2);

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

        produceMessagesOnTwoPartitions(RECORD_TOTAL, topic1);
        produceMessagesOnTwoPartitions(RECORD_TOTAL, topic2);


        TestUtils.waitForCondition(() -> streams.state().equals(KafkaStreams.State.RUNNING),
                () -> "Expected RUNNING state but streams is on " + streams.state());


        try {
            TestUtils.waitForCondition(() -> recordCount.get() == numOfCommittedMessages,
                    () -> "Expected " + numOfCommittedMessages + " records processed but only got " + recordCount.get());
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            assertEquals(numOfCommittedMessages, recordCount.get(), "Expected " + numOfCommittedMessages + " records processed but only got " + recordCount.get());
            streams.close();
        }
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
}
