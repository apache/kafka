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
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.CloseOptions;
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
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static org.apache.kafka.common.GroupState.EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
    public void testDeleteWithUnrecognizedOption() {
        final String[] args = new String[]{"--unrecognized-option", "--bootstrap-server", bootstrapServers, "--delete", "--all-groups"};
        assertThrows(OptionException.class, () -> getStreamsGroupService(args));
    }

    @Test
    public void testDeleteWithoutGroupOption() {
        final String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [delete] takes one of these options: [all-groups], [group]"));
            exited.set(true);
        }));
        try {
            getStreamsGroupService(args);
        } finally {
            assertTrue(exited.get());
        }
    }

    @Test
    public void testDeleteWithDeleteInternalTopicOption() {
        final String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--all-groups", "--delete-internal-topic", "foo"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [delete-internal-topic] takes [reset-offsets] when [execute] is used."));
            exited.set(true);
        }));
        try {
            getStreamsGroupService(args);
        } finally {
            assertTrue(exited.get());
        }
    }

    @Test
    public void testDeleteSingleGroupWithoutDeletingInternalTopics() throws Exception {
        final String appId = generateGroupAppId();
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--group", appId};

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        try (KafkaStreams streams = startKSApp(appId, service)) {
            /* test 1: delete NON_EMPTY streams group */
            String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups);
            Map<String, Throwable> result = service.deleteGroups();

            assertTrue(output.contains("Group '" + appId + "' could not be deleted due to:")
                    && output.contains("Streams group '" + appId + "' is not EMPTY."),
                "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Output was: (" + output + ")");

            assertNotNull(result.get(appId),
                "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");

            assertEquals(1, result.size());
            assertInstanceOf(GroupNotEmptyException.class,
                result.get(appId),
                "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Result was:(" + result + ")");

            /* test 2: delete EMPTY streams group without deleting internal topics */
            stopKSApp(appId, streams, service);
            final Map<String, Throwable> emptyGrpRes = new HashMap<>();
            output = ToolsTestUtils.grabConsoleOutput(() -> emptyGrpRes.putAll(service.deleteGroups()));

            assertTrue(output.contains("Deletion of requested streams groups ('" + appId + "') was successful."),
                "The streams group could not be deleted as expected");
            assertFalse(output.contains("Deletion of associated internal topics of the streams groups ('" + appId + "') was successful."),
                "The internal topics could not be deleted as expected.");
            assertEquals(1, emptyGrpRes.size());
            assertTrue(emptyGrpRes.containsKey(appId));
            assertNull(emptyGrpRes.get(appId), "The streams group could not be deleted as expected");
            assertEquals(3, getInternalTopics(appId).size(),
                "The internal topics were deleted, but they shouldn't have been.");

            /* test 3: delete an already deleted streams group (non-existing group) */
            result = service.deleteGroups();
            assertEquals(1, result.size());
            assertNotNull(result.get(appId));
            assertInstanceOf(IllegalArgumentException.class,
                result.get(appId),
                "The expected error was not detected while deleting streams group");
        }
    }

    @Test
    public void testDeleteSingleGroupWithDeletingInternalTopics() throws Exception {
        final String appId = generateGroupAppId();
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--group", appId, "--delete-all-internal-topics"};

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        try (KafkaStreams streams = startKSApp(appId, service)) {
            stopKSApp(appId, streams, service);
            final Map<String, Throwable> emptyGrpRes = new HashMap<>();
            String output = ToolsTestUtils.grabConsoleOutput(() -> emptyGrpRes.putAll(service.deleteGroups()));

            assertTrue(output.contains("Deletion of requested streams groups ('" + appId + "') was successful."),
                "The streams group could not be deleted as expected");
            assertTrue(output.contains("Deletion of associated internal topics of the streams groups ('" + appId + "') was successful."),
                "The internal topics could not be deleted as expected.");
            assertEquals(1, emptyGrpRes.size());
            assertTrue(emptyGrpRes.containsKey(appId));
            assertNull(emptyGrpRes.get(appId), "The streams group could not be deleted as expected");
            TestUtils.waitForCondition(() -> getInternalTopics(appId).isEmpty(),
                "The internal topics of the streams group " + appId + " were not deleted as expected.");
        }
    }

    @Test
    public void testDeleteMultipleGroupsWithoutDeletingInternalTopics() throws Exception {
        final String appId1 = generateGroupAppId();
        final String appId2 = generateGroupAppId();
        final String appId3 = generateGroupAppId();

        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--all-groups"};

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams1 = startKSApp(appId1, service);
        KafkaStreams streams2 = startKSApp(appId2, service);
        KafkaStreams streams3 = startKSApp(appId3, service);


        /* test 1: delete NON_EMPTY streams groups */
        final Map<String, Throwable> result = new HashMap<>();
        String output = ToolsTestUtils.grabConsoleOutput(() -> result.putAll(service.deleteGroups()));

        assertTrue(output.contains("Group '" + appId1 + "' could not be deleted due to:")
                && output.contains("Streams group '" + appId1 + "' is not EMPTY."),
            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Output was: (" + output + ")");
        assertTrue(output.contains("Group '" + appId3 + "' could not be deleted due to:")
                && output.contains("Streams group '" + appId3 + "' is not EMPTY."),
            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Output was: (" + output + ")");
        assertTrue(output.contains("Group '" + appId2 + "' could not be deleted due to:")
                && output.contains("Streams group '" + appId2 + "' is not EMPTY."),
            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Output was: (" + output + ")");


        assertNotNull(result.get(appId1),
            "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");
        assertNotNull(result.get(appId2),
            "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");
        assertNotNull(result.get(appId3),
            "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");

        assertEquals(3, result.size());
        assertInstanceOf(GroupNotEmptyException.class,
            result.get(appId1),
            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Result was:(" + result + ")");
        assertInstanceOf(GroupNotEmptyException.class,
            result.get(appId2),
            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Result was:(" + result + ")");
        assertInstanceOf(GroupNotEmptyException.class,
            result.get(appId3),
            "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Result was:(" + result + ")");

        /* test 2: delete mix of EMPTY and NON_EMPTY streams group */
        stopKSApp(appId1, streams1, service);
        final Map<String, Throwable> mixGrpsRes = new HashMap<>();
        output = ToolsTestUtils.grabConsoleOutput(() -> mixGrpsRes.putAll(service.deleteGroups()));

        assertTrue(output.contains("Deletion of some streams groups failed:"), "The streams groups deletion did not work as expected");
        assertTrue(output.contains("Group '" + appId2 + "' could not be deleted due to:")
            && output.contains("Streams group '" + appId2 + "' is not EMPTY."), "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Result was:(" + mixGrpsRes + ")");
        assertTrue(output.contains("Group '" + appId3 + "' could not be deleted due to:")
            && output.contains("Streams group '" + appId3 + "' is not EMPTY."), "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Result was:(" + mixGrpsRes + ")");
        assertTrue(output.contains("These streams groups were deleted successfully: '" + appId1 + "'"),
            "The streams groups deletion did not work as expected");
        assertFalse(output.contains("Deletion of associated internal topics of the streams groups ('" + appId1 + "') was successful."),
            "The internal topics could not be deleted as expected");

        assertEquals(3, mixGrpsRes.size());
        assertNull(mixGrpsRes.get(appId1));
        assertNotNull(mixGrpsRes.get(appId2));
        assertNotNull(mixGrpsRes.get(appId3));
        assertEquals(3, getInternalTopics(appId1).size(),
            "The internal topics were deleted, but they shouldn't have been.");
        assertEquals(3, getInternalTopics(appId2).size(),
            "The internal topics were deleted, but they shouldn't have been.");
        assertEquals(3, getInternalTopics(appId3).size(),
            "The internal topics were deleted, but they shouldn't have been.");

        /* test 3: delete all groups */
        stopKSApp(appId2, streams2, service);
        stopKSApp(appId3, streams3, service);

        final Map<String, Throwable> allGrpsRes = new HashMap<>();
        output = ToolsTestUtils.grabConsoleOutput(() -> allGrpsRes.putAll(service.deleteGroups()));

        assertTrue(output.contains("Deletion of requested streams groups ('" + appId2 + "', '" + appId3 + "') was successful.") |
                output.contains("Deletion of requested streams groups ('" + appId3 + "', '" + appId2 + "') was successful."),
            "The streams groups deletion did not work as expected");
        assertFalse(output.contains("Deletion of associated internal topics of the streams groups ('" + appId2 + "', '" + appId3 + "') was successful.") |
                output.contains("Deletion of associated internal topics of the streams groups ('" + appId3 + "', '" + appId2 + "') was successful."),
            "The internal topics could not be deleted as expected");

        assertEquals(2, allGrpsRes.size());
        assertNull(allGrpsRes.get(appId2));
        assertNull(allGrpsRes.get(appId3));
        assertEquals(3, getInternalTopics(appId1).size(),
            "The internal topics were deleted, but they shouldn't have been.");
        assertEquals(3, getInternalTopics(appId2).size(),
            "The internal topics were deleted, but they shouldn't have been.");
        assertEquals(3, getInternalTopics(appId3).size(),
            "The internal topics were deleted, but they shouldn't have been.");
    }

    @Test
    public void testDeleteAllGroupsWithDeletingInternalTopics() throws Exception {
        final String appId1 = generateGroupAppId();
        final String appId2 = generateGroupAppId();
        final String appId3 = generateGroupAppId();

        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--all-groups", "--delete-all-internal-topics"};

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams1 = startKSApp(appId1, service);
        KafkaStreams streams2 = startKSApp(appId2, service);
        KafkaStreams streams3 = startKSApp(appId3, service);

        /* test 1: delete mix of EMPTY and NON_EMPTY streams group */
        stopKSApp(appId1, streams1, service);
        final Map<String, Throwable> mixGrpsRes = new HashMap<>();
        String output = ToolsTestUtils.grabConsoleOutput(() -> mixGrpsRes.putAll(service.deleteGroups()));

        assertTrue(output.contains("Deletion of some streams groups failed:"), "The streams groups deletion did not work as expected");
        assertTrue(output.contains("Group '" + appId2 + "' could not be deleted due to:")
            && output.contains("Streams group '" + appId2 + "' is not EMPTY."), "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Result was:(" + mixGrpsRes + ")");
        assertTrue(output.contains("Group '" + appId3 + "' could not be deleted due to:")
            && output.contains("Streams group '" + appId3 + "' is not EMPTY."), "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting streams group. Result was:(" + mixGrpsRes + ")");
        assertTrue(output.contains("These streams groups were deleted successfully: '" + appId1 + "'"),
            "The streams groups deletion did not work as expected");
        assertTrue(output.contains("Deletion of associated internal topics of the streams groups ('" + appId1 + "') was successful."),
            "The internal topics could not be deleted as expected");

        assertEquals(3, mixGrpsRes.size());
        assertNull(mixGrpsRes.get(appId1));
        assertNotNull(mixGrpsRes.get(appId2));
        assertNotNull(mixGrpsRes.get(appId3));
        TestUtils.waitForCondition(() -> getInternalTopics(appId1).isEmpty(),
            "The internal topics of the streams group " + appId1 + " were not deleted as expected.");
        assertFalse(getInternalTopics(appId2).isEmpty());
        assertFalse(getInternalTopics(appId3).isEmpty());

        /* test 2: delete all groups */
        stopKSApp(appId2, streams2, service);
        stopKSApp(appId3, streams3, service);

        final Map<String, Throwable> allGrpsRes = new HashMap<>();
        output = ToolsTestUtils.grabConsoleOutput(() -> allGrpsRes.putAll(service.deleteGroups()));

        assertTrue(output.contains("Deletion of requested streams groups ('" + appId2 + "', '" + appId3 + "') was successful.") |
                output.contains("Deletion of requested streams groups ('" + appId3 + "', '" + appId2 + "') was successful."),
            "The streams groups deletion did not work as expected");
        assertTrue(output.contains("Deletion of associated internal topics of the streams groups ('" + appId2 + "', '" + appId3 + "') was successful.") |
                output.contains("Deletion of associated internal topics of the streams groups ('" + appId3 + "', '" + appId2 + "') was successful."),
            "The internal topics could not be deleted as expected");

        assertEquals(2, allGrpsRes.size());
        assertNull(allGrpsRes.get(appId2));
        assertNull(allGrpsRes.get(appId3));
        TestUtils.waitForCondition(() -> getInternalTopics(appId2).isEmpty(),
            "The internal topics of the streams group " + appId2 + " were not deleted as expected.");
        TestUtils.waitForCondition(() -> getInternalTopics(appId3).isEmpty(),
            "The internal topics of the streams group " + appId3 + " were not deleted as expected.");
    }

    @Test
    public void testDeleteAllGroupsAfterVersionDowngrade() throws Exception {
        final String appId = generateGroupAppId();
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--all-groups", "--delete-all-internal-topics"};

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        try (KafkaStreams streams = startKSApp(appId, service)) {
            stopKSApp(appId, streams, service);
            // downgrade the streams.version to 0
            updateStreamsGroupProtocol((short) 0);
            final Map<String, Throwable> result = new HashMap<>();
            String output = ToolsTestUtils.grabConsoleOutput(() -> result.putAll(service.deleteGroups()));
            System.out.println(output);

            assertTrue(output.contains("Deletion of requested streams groups ('" + appId + "') was successful."),
                "The streams group could not be deleted as expected");
            assertTrue(output.contains("Retrieving internal topics is not supported by the broker version."));
            assertTrue(output.contains("Use 'kafka-topics.sh' to delete the group's internal topics."));
            // Validate the list of internal topics in error message
            assertTrue(output.contains("Internal topics:"));
            System.out.println(output);
            assertTrue(
                output.matches("(?s).*" + APP_ID_PREFIX + "[a-zA-Z0-9\\-]+-(aggregated_value-changelog|repartition|changelog).*"),
                "The internal topic name does not match the expected format. Output: " + output
            );

            assertEquals(1, result.size());
            assertTrue(result.containsKey(appId));
            assertNull(result.get(appId), "The streams group could not be deleted as expected");
            assertEquals(3, getInternalTopics(appId).size(),
                "The internal topics were deleted, but they shouldn't have been.");
        } finally {
            // upgrade back the streams.version to 1
            updateStreamsGroupProtocol((short) 1);
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

    private void updateStreamsGroupProtocol(short version) {
        try (Admin admin = cluster.createAdminClient()) {
            Map<String, FeatureUpdate> updates = Utils.mkMap(
                Utils.mkEntry("streams.version", new FeatureUpdate(version, version == 0 ? FeatureUpdate.UpgradeType.SAFE_DOWNGRADE : FeatureUpdate.UpgradeType.UPGRADE)));
            admin.updateFeatures(updates, new UpdateFeaturesOptions()).all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    private StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = StreamsGroupCommandOptions.fromArgs(args);
        return new StreamsGroupCommand.StreamsGroupService(
            opts,
            Map.of(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private KafkaStreams startKSApp(String appId, StreamsGroupCommand.StreamsGroupService service) throws Exception {
        String inputTopic = generateRandomTopicId(INPUT_TOPIC_PREFIX);
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
        TestUtils.waitForCondition(() -> recordCount.get() == RECORD_TOTAL,
            "Expected " + RECORD_TOTAL + " records processed but only got " + recordCount.get());

        return streams;
    }

    private void stopKSApp(String appId, KafkaStreams streams, StreamsGroupCommand.StreamsGroupService service) throws InterruptedException {
        if (streams != null) {
            CloseOptions closeOptions = CloseOptions.timeout(Duration.ofSeconds(30))
                    .withGroupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP);
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

    private static StreamsBuilder builder(String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> List.of(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder;
    }
}
