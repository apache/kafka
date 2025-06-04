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
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import joptsimple.OptionException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class DeleteStreamsInternalTopicsTest {
    private static final String APP_ID_PREFIX = "delete-internal-topics-test-";
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
}
