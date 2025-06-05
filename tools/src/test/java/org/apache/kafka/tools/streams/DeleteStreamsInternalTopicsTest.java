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
import org.apache.kafka.common.GroupState;
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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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
import java.util.Arrays;
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

import joptsimple.OptionException;

import static org.apache.kafka.common.GroupState.EMPTY;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class DeleteStreamsInternalTopicsTest {
    private static final String APP_ID_PREFIX = "delete-internal-topics-test-";
    private static final String INPUT_TOPIC_PREFIX = "input-topic-";
    private static final int RECORD_TOTAL = 10;
    public static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() {
        final Properties props = new Properties();
        cluster = new EmbeddedKafkaCluster(2, props);
        cluster.start();
        bootstrapServers = cluster.bootstrapServers();
    }

    @AfterEach
    public void deleteTopics() {
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
        final String[] args = new String[]{"--unrecognized-option", "--bootstrap-server", bootstrapServers, "--delete",
            "--internal-topic", "foo", "--group", "bar"};
        assertThrows(OptionException.class, () -> getStreamsGroupService(args));
    }

    @Test
    public void testDeleteWithoutDeleteOption() {
        final String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--internal-topic", "bar", "--group", "foo"};
        assertThrows(OptionException.class, () -> getStreamsGroupService(args));
    }

    @Test
    public void testDeleteWithoutGroupOption() {
        final String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--internal-topic", "foo"};
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
    public void testDeleteInternalTopicNotExistingGroup() {
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--internal-topic", "foo", "--group", "bar"};
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        assertTrue(output.contains("Group 'bar' does not exist or is not a streams group."));
    }

    @Test
    public void testDeleteInternalTopicsMixedAssociatedWithTheGroup() throws Exception {
        final String appId = generateGroupAppId();
        final List<String> internalTopics = Arrays.asList(
            appId + "-aggregated_value-changelog",
            appId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition",
            appId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog"
        );
        final String wrongInternalTopic = "foo";

        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete",
            "--internal-topic", wrongInternalTopic, "--internal-topic", internalTopics.get(0), "--internal-topic", internalTopics.get(1), "--internal-topic", internalTopics.get(2),
            "--group", appId};
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams = startKSApp(appId, service);
        stopKSApp(appId, streams, service);
        List<String> allTopics = service.collectAllTopics(appId);

        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);

        assertTrue(output.contains("The specified internal topic 'foo' is not associated to the any of the groups ('" + appId + "') " +
            "as an internal topic and thus will not be deleted."));
        assertTrue(
            output.contains("Deletion of requested internal topics (") &&
                output.contains("was successful.") &&
                internalTopics.stream().allMatch(output::contains));
        // Verify that the internal topics are deleted
        allTopics.addAll(List.of("__consumer_offsets", "__transaction_state"));
        allTopics.removeAll(internalTopics);
        cluster.waitForRemainingTopics(30000, allTopics.toArray(new String[0]));

    }

    @Test
    public void testDeleteAllInternalTopicsFromNonEmptyGroup() throws Exception {
        final String appId = generateGroupAppId();

        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete",
            "--all-internal-topics", "--group", appId};
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams = startKSApp(appId, service);

        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);

        assertTrue(output.contains("The specified group '" + appId + "' still has active members. Please terminate the members before retrying the operation."));

        streams.close();
    }

    @Test
    public void testDeleteSpecifiedInternalTopics() throws Exception {
        final String appId = generateGroupAppId();
        final List<String> internalTopics = Arrays.asList(
            appId + "-aggregated_value-changelog",
            appId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition",
            appId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog"
        );
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete",
            "--internal-topic", internalTopics.get(0), "--internal-topic", internalTopics.get(1), "--internal-topic", internalTopics.get(2), "--group", appId};
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams = startKSApp(appId, service);
        stopKSApp(appId, streams, service);
        List<String> allTopics = service.collectAllTopics(appId);

        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);

        assertTrue(
            output.contains("Deletion of requested internal topics (") &&
                output.contains("was successful.") &&
                internalTopics.stream().allMatch(output::contains));
        // Verify that the internal topics are deleted
        allTopics.addAll(List.of("__consumer_offsets", "__transaction_state"));
        allTopics.removeAll(internalTopics);
        cluster.waitForRemainingTopics(30000, allTopics.toArray(new String[0]));
    }

    @Test
    public void testDeleteSpecifiedInternalTopicsWithAllGroupsOption() throws Exception {
        final String appId = generateGroupAppId();
        final List<String> internalTopics = Arrays.asList(
            appId + "-aggregated_value-changelog",
            appId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition",
            appId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog"
        );
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete",
            "--internal-topic", internalTopics.get(0), "--internal-topic", internalTopics.get(1), "--internal-topic", internalTopics.get(2), "--all-groups"};
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams = startKSApp(appId, service);
        stopKSApp(appId, streams, service);
        List<String> allTopics = service.collectAllTopics(appId);

        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);

        assertTrue(
            output.contains("Deletion of requested internal topics (") &&
                output.contains("was successful.") &&
                internalTopics.stream().allMatch(output::contains));
        // Verify that the internal topics are deleted
        allTopics.addAll(List.of("__consumer_offsets", "__transaction_state"));
        allTopics.removeAll(internalTopics);
        cluster.waitForRemainingTopics(30000, allTopics.toArray(new String[0]));
    }

    @Test
    public void testDeleteAllInternalTopics() throws Exception {
        final String appId = generateGroupAppId();

        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete",
            "--all-internal-topics", "--group", appId};
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams = startKSApp(appId, service);
        List<String> allTopics = service.collectAllTopics(appId);
        final List<String> internalTopics = service.retrieveInternalTopics(List.of(appId)).get(appId);
        stopKSApp(appId, streams, service);

        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);

        assertTrue(
            output.contains("Deletion of requested internal topics (") &&
                output.contains("was successful.") &&
                internalTopics.stream().allMatch(output::contains));
        // Verify that the internal topics are deleted
        allTopics.addAll(List.of("__consumer_offsets", "__transaction_state"));
        allTopics.removeAll(internalTopics);
        cluster.waitForRemainingTopics(30000, allTopics.toArray(new String[0]));
    }

    @Test
    public void testDeleteAllInternalTopicsFromMultipleGroups() throws Exception {
        final String appId1 = generateGroupAppId();
        final String appId2 = generateGroupAppId();

        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete",
            "--all-internal-topics", "--group", appId1, "--group", appId2};
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams1 = startKSApp(appId1, service);
        KafkaStreams streams2 = startKSApp(appId2, service);
        List<String> allTopics = service.collectAllTopics(appId1);
        allTopics.addAll(service.collectAllTopics(appId2));
        final List<String> internalTopics = service.retrieveInternalTopics(List.of(appId1, appId2)).values().stream().flatMap(List::stream).toList();
        stopKSApp(appId1, streams1, service);
        stopKSApp(appId2, streams2, service);


        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);

        assertTrue(
            output.contains("Deletion of requested internal topics (") &&
                output.contains("was successful.") &&
                internalTopics.stream().allMatch(output::contains));
        // Verify that the internal topics are deleted
        allTopics.addAll(List.of("__consumer_offsets", "__transaction_state"));
        allTopics.removeAll(internalTopics);
        cluster.waitForRemainingTopics(30000, allTopics.toArray(new String[0]));
    }

    @Test
    public void testDeleteAllInternalTopicsFromAllGroups() throws Exception {
        final String appId1 = generateGroupAppId();
        final String appId2 = generateGroupAppId();

        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete",
            "--all-internal-topics", "--all-groups"};
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        KafkaStreams streams1 = startKSApp(appId1, service);
        KafkaStreams streams2 = startKSApp(appId2, service);
        List<String> allTopics = service.collectAllTopics(appId1);
        allTopics.addAll(service.collectAllTopics(appId2));
        final List<String> internalTopics = service.retrieveInternalTopics(List.of(appId1, appId2)).values().stream().flatMap(List::stream).toList();
        stopKSApp(appId1, streams1, service);
        stopKSApp(appId2, streams2, service);


        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);

        assertTrue(
            output.contains("Deletion of requested internal topics (") &&
                output.contains("was successful.") &&
                internalTopics.stream().allMatch(output::contains));
        // Verify that the internal topics are deleted
        allTopics.addAll(List.of("__consumer_offsets", "__transaction_state"));
        allTopics.removeAll(internalTopics);
        cluster.waitForRemainingTopics(30000, allTopics.toArray(new String[0]));
    }

    private StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = StreamsGroupCommandOptions.fromArgs(args);
        return new StreamsGroupCommand.StreamsGroupService(
            opts,
            Map.of(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private String generateGroupAppId() {
        return APP_ID_PREFIX + TestUtils.randomString(10);
    }

    private String generateRandomTopicId(String prefix) {
        return prefix + TestUtils.randomString(10);
    }

    private KafkaStreams startKSApp(String appId, StreamsGroupCommand.StreamsGroupService service) throws Exception {
        String inputTopic = generateRandomTopicId(INPUT_TOPIC_PREFIX);
        StreamsBuilder builder = builder(inputTopic);
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

    private void stopKSApp(String appId, KafkaStreams streams, StreamsGroupCommand.StreamsGroupService service) throws InterruptedException {
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

    private static StreamsBuilder builder(String inputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count();
        return builder;
    }

    private boolean checkGroupState(StreamsGroupCommand.StreamsGroupService service, String groupId, GroupState state) throws Exception {
        return Objects.equals(service.collectGroupState(groupId), state);
    }
}
