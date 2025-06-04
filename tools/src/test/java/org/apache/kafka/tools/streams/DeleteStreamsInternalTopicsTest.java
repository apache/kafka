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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;
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
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.AfterAll;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionException;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.GroupState.EMPTY;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class DeleteStreamsInternalTopicsTest {
    private static final String INPUT_TOPIC_PREFIX = "input-topic-";
    private static final String OUTPUT_TOPIC_PREFIX = "output-topic-";
    private static final String APP_ID_PREFIX = "delete-group-test-";
    private static final int RECORD_TOTAL = 10;
    public static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() {
        final Properties props = new Properties();
        props.setProperty(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams");
        cluster = new EmbeddedKafkaCluster(2, props);
        cluster.start();

        bootstrapServers = cluster.bootstrapServers();
    }

    @AfterAll
    public static void closeCluster() {
        cluster.stop();
    }

    @Test
    public void testDeleteWithUnrecognizedOption() {
        final String[] args = new String[]{"--unrecognized-option", "--bootstrap-server", bootstrapServers, "--delete", "--internal-topics", "foo,bar"};
        assertThrows(OptionException.class, () -> getStreamsGroupService(args));
    }

    @Test
    public void testDeleteWithoutInternalTopicsOption() {
        final String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [delete] takes [internal-topics] as an argument to delete internal topics."));
            exited.set(true);
        }));
        try {
            getStreamsGroupService(args);
        } finally {
            assertTrue(exited.get());
        }
    }

    @Test
    public void testDeleteWithoutSpecifiedInternalTopics() {
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--internal-topics"};
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        assertTrue(output.contains("No internal topics specified for deletion."));
    }

    @Test
    public void testDeleteInternalTopicsWithNamesWithWrongPattern() {
        final List<String> wrongInternalTopics = Arrays.asList(
            "foo",
            "bar"
        );
        wrongInternalTopics.forEach(topic -> {
            try {
                cluster.createTopic(topic, 1, (short) 1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--internal-topics", String.join(",", wrongInternalTopics)};
        assertWithDryRunAndExecute(args);

        cluster.deleteTopics(String.join(",", wrongInternalTopics));
    }

    @Test
    public void testDeleteMixedInternalTopicsWithNamesWithAndWithoutWrongPattern() {
        final String appId = generateGroupAppId();
        final List<String> wrongInternalTopics = Arrays.asList(
            "foo",
            "bar"
        );
        final List<String> internalTopics = Arrays.asList(
            appId + "-changelog",
            appId + "-repartition",
            appId + "-global-changelog"
        );
        List<String> allTopics = new ArrayList<>(wrongInternalTopics);
        allTopics.addAll(internalTopics);

        allTopics.forEach(topic -> {
            try {
                cluster.createTopic(topic, 1, (short) 1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--internal-topics", String.join(",", allTopics)};
        assertWithDryRunAndExecute(args, wrongInternalTopics, internalTopics);
    }

    @Test
    public void testDeleteInternalTopics() {
        final String appId = generateGroupAppId();
        final List<String> internalTopics = Arrays.asList(
            appId + "-changelog",
            appId + "-repartition",
            appId + "-global-changelog"
        );
        internalTopics.forEach(topic -> {
            try {
                cluster.createTopic(topic, 1, (short) 1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--delete", "--internal-topics", String.join(",", internalTopics)};
        assertWithDryRunAndExecute(args, internalTopics);
    }

    private void assertWithDryRunAndExecute(String[] args, List<String> internalTopics) {
        // default: no dry-run, no execute
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        assertTrue(
            output.contains("Dry run: The following internal topics would be deleted:") &&
                internalTopics.stream().allMatch(output::contains),
            "The internal topics could not be deleted as expected"
        );

        // Test dry-run
        String[] dryRunArgs = addTo(args, "--dry-run");
        service = getStreamsGroupService(dryRunArgs);
        output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        assertTrue(
            output.contains("Dry run: The following internal topics would be deleted:") &&
                internalTopics.stream().allMatch(output::contains),
            "The internal topics could not be deleted as expected"
        );

        // Test execute
        String[] executeArgs = addTo(args, "--execute");
        service = getStreamsGroupService(executeArgs);
        output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        assertTrue(
            output.contains("Deletion of requested internal topics (") &&
                output.contains("was successful.") &&
                internalTopics.stream().allMatch(output::contains),
            "The --execute option for deleting internal topics did not work as expected"
        );
    }

    private void assertWithDryRunAndExecute(String[] args) {
        // default: no dry-run, no execute
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        assertTrue(
            output.contains("No internal topics specified for deletion."),
            "The internal topics could not be deleted as expected"
        );

        // Test dry-run
        String[] dryRunArgs = addTo(args, "--dry-run");
        service = getStreamsGroupService(dryRunArgs);
        output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        assertTrue(
            output.contains("No internal topics specified for deletion."),
            "The internal topics could not be deleted as expected"
        );

        // Test execute
        String[] executeArgs = addTo(args, "--execute");
        service = getStreamsGroupService(executeArgs);
        output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        assertTrue(
            output.contains("No internal topics specified for deletion."),
            "The --execute option for deleting internal topics did not work as expected"
        );
    }

    private void assertWithDryRunAndExecute(String[] args, List<String> wrongInternalTopics, List<String> internalTopics) {
        // default: no dry-run, no execute
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args);
        final String output = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        wrongInternalTopics.forEach(topic ->
            assertTrue(
                output.contains("Invalid internal topic format: " + topic),
                "The wrong internal topics could not be deleted as expected"
            )
        );
        assertTrue(
            output.contains("Dry run: The following internal topics would be deleted:") &&
                internalTopics.stream().allMatch(output::contains),
            "The internal topics could not be deleted as expected"
        );

        // Test dry-run
        String[] dryRunArgs = addTo(args, "--dry-run");
        service = getStreamsGroupService(dryRunArgs);
        final String dryRunOutput = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        wrongInternalTopics.forEach(topic ->
            assertTrue(
                dryRunOutput.contains("Invalid internal topic format: " + topic),
                "The wrong internal topics could not be deleted as expected"
            )
        );
        assertTrue(
            dryRunOutput.contains("Dry run: The following internal topics would be deleted:") &&
                internalTopics.stream().allMatch(dryRunOutput::contains),
            "The internal topics could not be deleted as expected"
        );

        // Test execute
        String[] executeArgs = addTo(args, "--execute");
        service = getStreamsGroupService(executeArgs);
        final String executeOutput = ToolsTestUtils.grabConsoleOutput(service::deleteInternalTopics);
        wrongInternalTopics.forEach(topic ->
            assertTrue(
                executeOutput.contains("Invalid internal topic format: " + topic),
                "The wrong internal topics could not be deleted as expected"
            )
        );
        assertTrue(
            executeOutput.contains("Deletion of requested internal topics (") &&
                executeOutput.contains("was successful.") &&
                internalTopics.stream().allMatch(executeOutput::contains),
            "The --execute option for deleting internal topics did not work as expected"
        );
    }

    private String[] addTo(String[] args, String... extra) {
        List<String> res = new ArrayList<>(asList(args));
        res.addAll(asList(extra));
        return res.toArray(new String[0]);
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

//        TestUtils.waitForCondition(
//            () -> !service.collectGroupMembers(appId).isEmpty(),
//            "The group did not initialize as expected."
//        );
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
            KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
            closeOptions.timeout(Duration.ofSeconds(30));
            closeOptions.leaveGroup(true);
            streams.close(closeOptions);
            streams.cleanUp();

            TestUtils.waitForCondition(
                () -> checkGroupState(service, appId, EMPTY),
                "The group did not become empty as expected."
            );
//            TestUtils.waitForCondition(
//                () -> service.collectGroupMembers(appId).isEmpty(),
//                "The group size is not zero as expected."
//            );
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
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder;
    }
}
