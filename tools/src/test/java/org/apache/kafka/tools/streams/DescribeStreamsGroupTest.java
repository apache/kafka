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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class DescribeStreamsGroupTest {
    public static EmbeddedKafkaCluster cluster = null;
    static KafkaStreams streams;
    private static final String APP_ID = "streams-group-command-test";
    private static final String APP_ID_2 = "streams-group-command-test-2";

    private static final String INPUT_TOPIC = "customInputTopic";
    private static final String OUTPUT_TOPIC = "customOutputTopic";
    private static final String INPUT_TOPIC_2 = "customInputTopic2";
    private static final String OUTPUT_TOPIC_2 = "customOutputTopic2";
    private static String bootstrapServers;
    @BeforeAll
    public static void setup() throws Exception {
        // start the cluster and create the input topic
        final Properties props = new Properties();
        props.setProperty(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams");
        cluster = new EmbeddedKafkaCluster(1, props);
        cluster.start();
        cluster.createTopic(INPUT_TOPIC, 2, 1);
        bootstrapServers = cluster.bootstrapServers();


        // start kafka streams
        Properties streamsProp = streamsProp(APP_ID);
        streams = new KafkaStreams(topology(INPUT_TOPIC, OUTPUT_TOPIC), streamsProp);
        startApplicationAndWaitUntilRunning(streams);
    }

    @AfterAll
    public static void closeCluster() {
        streams.close();
        cluster.stop();
        cluster = null;
    }

    @Test
    public void testDescribeWithUnrecognizedOption() {
        String[] args = new String[]{"--unrecognized-option", "--bootstrap-server", bootstrapServers, "--describe", "--group", APP_ID};
        assertThrows(OptionException.class, () -> getStreamsGroupService(args));
    }

    @Test
    public void testDescribeWithoutGroupOption() {
        final String[] args = new String[]{"--bootstrap-server", bootstrapServers, "--describe"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [describe] takes one of these options: [all-groups], [group]"));
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
    public void testDescribeStreamsGroup() throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "TOPIC", "PARTITION", "OFFSET-LAG");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, INPUT_TOPIC, "0", "0"),
            List.of(APP_ID, INPUT_TOPIC, "1", "0"),
            List.of(APP_ID, "streams-group-command-test-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition", "0", "0"),
            List.of(APP_ID, "streams-group-command-test-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition", "1", "0"));

        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--group", APP_ID), expectedHeader, expectedRows, List.of());
        // --describe --offsets has the same output as --describe
        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--offsets", "--group", APP_ID), expectedHeader, expectedRows, List.of());
    }

    @Test
    public void testDescribeStreamsGroupWithVerboseOption() throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LEADER-EPOCH", "LOG-END-OFFSET", "OFFSET-LAG");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, INPUT_TOPIC, "0", "-", "-", "0", "0"),
            List.of(APP_ID, INPUT_TOPIC, "1", "-", "-", "0", "0"),
            List.of(APP_ID, "streams-group-command-test-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition", "0", "-", "-", "0", "0"),
            List.of(APP_ID, "streams-group-command-test-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition", "1", "-", "-", "0", "0"));

        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--verbose", "--group", APP_ID), expectedHeader, expectedRows, List.of());
        // --describe --offsets has the same output as --describe
        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--offsets", "--verbose", "--group", APP_ID), expectedHeader, expectedRows, List.of());
        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--verbose", "--offsets", "--group", APP_ID), expectedHeader, expectedRows, List.of());
    }

    @Test
    public void testDescribeStreamsGroupWithStateOption() throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "COORDINATOR", "(ID)", "STATE", "#MEMBERS");
        final Set<List<String>> expectedRows = Set.of(List.of(APP_ID, "", "", "Stable", "2"));
        // The coordinator is not deterministic, so we don't care about it.
        final List<Integer> dontCares = List.of(1, 2);
        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--state", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithStateAndVerboseOptions() throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "COORDINATOR", "(ID)", "STATE", "GROUP-EPOCH", "TARGET-ASSIGNMENT-EPOCH", "#MEMBERS");
        final Set<List<String>> expectedRows = Set.of(List.of(APP_ID, "", "", "Stable", "", "", "2"));
        // The coordinator is not deterministic, so we don't care about it.
        // The GROUP-EPOCH and TARGET-ASSIGNMENT-EPOCH can vary due to rebalance timing, so we don't care about them either.
        final List<Integer> dontCares = List.of(1, 2, 4, 5);

        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--state", "--verbose", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--verbose", "--state", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithMembersOption() throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "MEMBER", "PROCESS", "CLIENT-ID", "ASSIGNMENTS");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, "", "", "", "ACTIVE:", "0:[1];", "1:[1];"),
            List.of(APP_ID, "", "", "", "ACTIVE:", "0:[0];", "1:[0];"));
        // The member and process names as well as client-id are not deterministic, so we don't care about them.
        final List<Integer> dontCares = List.of(1, 2, 3);

        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--members", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithMembersAndVerboseOptions() throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "TARGET-ASSIGNMENT-EPOCH", "TOPOLOGY-EPOCH", "MEMBER", "MEMBER-PROTOCOL", "MEMBER-EPOCH", "PROCESS", "CLIENT-ID", "ASSIGNMENTS");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, "", "0", "", "streams", "", "", "", "ACTIVE:", "0:[1];", "1:[1];", "TARGET-ACTIVE:", "0:[1];", "1:[1];"),
            List.of(APP_ID, "", "0", "", "streams", "", "", "", "ACTIVE:", "0:[0];", "1:[0];", "TARGET-ACTIVE:", "0:[0];", "1:[0];"));
        // The member and process names as well as client-id are not deterministic, so we don't care about them.
        // The TARGET-ASSIGNMENT-EPOCH and MEMBER-EPOCH can vary due to rebalance timing, so we don't care about them either.
        final List<Integer> dontCares = List.of(1, 3, 5, 6, 7);

        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--members", "--verbose", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--verbose", "--members", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
    }

    @Test
    public void testDescribeMultipleStreamsGroupWithMembersAndVerboseOptions() throws Exception {
        cluster.createTopic(INPUT_TOPIC_2, 1, 1);
        TestUtils.waitForCondition(
            () -> cluster.getAllTopicsInCluster().contains(INPUT_TOPIC_2),
            30000,
            "Topic " + INPUT_TOPIC_2 + " not created"
        );
        KafkaStreams streams2 = new KafkaStreams(topology(INPUT_TOPIC_2, OUTPUT_TOPIC_2), streamsProp(APP_ID_2));
        startApplicationAndWaitUntilRunning(streams2);

        final List<String> expectedHeader = List.of("GROUP", "TARGET-ASSIGNMENT-EPOCH", "TOPOLOGY-EPOCH", "MEMBER", "MEMBER-PROTOCOL", "MEMBER-EPOCH", "PROCESS", "CLIENT-ID", "ASSIGNMENTS");
        final Set<List<String>> expectedRows1 = Set.of(
            List.of(APP_ID, "", "0", "", "streams", "", "", "", "ACTIVE:", "0:[1];", "1:[1];", "TARGET-ACTIVE:", "0:[1];", "1:[1];"),
            List.of(APP_ID, "", "0", "", "streams", "", "", "", "ACTIVE:", "0:[0];", "1:[0];", "TARGET-ACTIVE:", "0:[0];", "1:[0];"));
        final Set<List<String>> expectedRows2 = Set.of(
            List.of(APP_ID_2, "", "0", "", "streams", "", "", "", "ACTIVE:", "1:[0];", "TARGET-ACTIVE:", "1:[0];"),
            List.of(APP_ID_2, "", "0", "", "streams", "", "", "", "ACTIVE:", "0:[0];", "TARGET-ACTIVE:", "0:[0];"));
        final Map<String, Set<List<String>>> expectedRowsMap = new HashMap<>();
        expectedRowsMap.put(APP_ID, expectedRows1);
        expectedRowsMap.put(APP_ID_2, expectedRows2);

        // The member and process names as well as client-id are not deterministic, so we don't care about them.
        // The TARGET-ASSIGNMENT-EPOCH and MEMBER-EPOCH can vary due to rebalance timing, so we don't care about them either.
        final List<Integer> dontCares = List.of(1, 3, 5, 6, 7);

        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--members", "--verbose", "--group", APP_ID, "--group", APP_ID_2),
            expectedHeader, expectedRowsMap, dontCares);
        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--verbose", "--members", "--group", APP_ID, "--group", APP_ID_2),
            expectedHeader, expectedRowsMap, dontCares);
        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--verbose", "--members", "--all-groups"),
            expectedHeader, expectedRowsMap, dontCares);

        streams2.close();
        streams2.cleanUp();
    }

    @Test
    public void testDescribeNonExistingStreamsGroup() {
        final String nonExistingGroup = "non-existing-group";
        final String errorMessage = String.format(
            "Error: Executing streams group command failed due to org.apache.kafka.common.errors.GroupIdNotFoundException: Group %s not found.",
            nonExistingGroup);

        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--members", "--verbose", "--group", nonExistingGroup), errorMessage);
        validateDescribeOutput(
            List.of("--bootstrap-server", bootstrapServers, "--describe", "--verbose", "--members", "--group", nonExistingGroup), errorMessage);
    }

    @Test
    public void testDescribeStreamsGroupWithShortTimeout() {
        // Note: 1ms timeout may not always trigger timeout on fast machines with warm groups
        // Using 0ms to ensure timeout
        List<String> args = List.of("--bootstrap-server", bootstrapServers, "--describe", "--members", "--verbose", "--group", APP_ID, "--timeout", "0");
        Throwable e = assertThrows(ExecutionException.class, () -> getStreamsGroupService(args.toArray(new String[0])).describeGroups());
        assertEquals(TimeoutException.class, e.getCause().getClass());
    }

    private static Topology topology(String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> List.of(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    private static Properties streamsProp(String appId) {
        Properties streamsProp = new Properties();
        streamsProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsProp.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsProp.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsProp.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
        return streamsProp;
    }

    private StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = StreamsGroupCommandOptions.fromArgs(args);
        return new StreamsGroupCommand.StreamsGroupService(
            opts,
            Map.of(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private static void validateDescribeOutput(List<String> args, String errorMessage) {
        String output = ToolsTestUtils.grabConsoleOutput(() -> assertEquals(1, StreamsGroupCommand.execute(args.toArray(new String[0]))));
        assertEquals(errorMessage, output.trim());
    }

    private static void validateDescribeOutput(
        List<String> args,
        List<String> expectedHeader,
        Set<List<String>> expectedRows,
        List<Integer> dontCareIndices
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> assertEquals(0, StreamsGroupCommand.execute(args.toArray(new String[0]))));
            out.set(output);

            String[] lines = output.split("\n");
            if (lines.length == 1 && lines[0].isEmpty()) lines = new String[]{};

            if (lines.length == 0) return false;
            List<String> header = List.of(lines[0].split("\\s+"));
            if (!expectedHeader.equals(header)) return false;

            Set<List<String>> groupDesc = Arrays.stream(Arrays.copyOfRange(lines, 1, lines.length))
                .map(line -> List.of(line.split("\\s+")))
                .collect(Collectors.toSet());
            if (groupDesc.size() != expectedRows.size()) return false;
            // clear the dontCare fields and then compare two sets
            return expectedRows
                .equals(
                    groupDesc.stream()
                        .map(list -> {
                            List<String> listCloned = new ArrayList<>(list);
                            dontCareIndices.forEach(index -> listCloned.set(index, ""));
                            return listCloned;
                        }).collect(Collectors.toSet())
                );
        }, () -> String.format("Expected header=%s and groups=%s, but found:%n%s", expectedHeader, expectedRows, out.get()));
    }

    private static void validateDescribeOutput(
        List<String> args,
        List<String> expectedHeader,
        Map<String, Set<List<String>>> expectedRows,
        List<Integer> dontCareIndices
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> assertEquals(0, StreamsGroupCommand.execute(args.toArray(new String[0]))));
            out.set(output);

            String[] lines = output.split("\n");
            if (lines.length == 1 && lines[0].isEmpty()) lines = new String[]{};

            if (lines.length == 0) return false;
            List<String> header = List.of(lines[0].split("\\s+"));
            if (!expectedHeader.equals(header)) return false;

            Map<String, Set<List<String>>> groupdescMap = splitOutputByGroup(lines);

            if (groupdescMap.size() != expectedRows.size()) return false;

            // clear the dontCare fields and then compare two sets
            boolean compareResult = true;
            for (Map.Entry<String, Set<List<String>>> entry : groupdescMap.entrySet()) {
                String group = entry.getKey();
                Set<List<String>> groupDesc = entry.getValue();
                if (!expectedRows.containsKey(group)) return false;
                Set<List<String>> expectedGroupDesc = expectedRows.get(group);
                if (expectedGroupDesc.size() != groupDesc.size())
                    compareResult = false;
                for (List<String> list : groupDesc) {
                    List<String> listCloned = new ArrayList<>(list);
                    dontCareIndices.forEach(index -> listCloned.set(index, ""));
                    if (!expectedGroupDesc.contains(listCloned)) {
                        compareResult = false;
                    }
                }
            }

            return compareResult;
        }, () -> String.format("Expected header=%s and groups=%s, but found:%n%s", expectedHeader, expectedRows, out.get()));
    }

    private static Map<String, Set<List<String>>> splitOutputByGroup(String[] lines) {
        Map<String, Set<List<String>>> groupdescMap = new HashMap<>();
        String headerLine = lines[0].replaceAll(" ", "");
        String groupName = lines[1].split("\\s+")[0];
        int j = 1;
        for (int i = j; i < lines.length; i++) {
            if (lines[i].replaceAll(" ", "").equals(headerLine) || i == lines.length - 1) {
                if (i == lines.length - 1) i++;
                Set<List<String>> groupDesc = Arrays.stream(Arrays.copyOfRange(lines, j, i))
                    .map(line -> List.of(line.split("\\s+")))
                    .collect(Collectors.toSet());
                groupdescMap.put(groupName, groupDesc);
                if (i + 1 < lines.length) {
                    j = i + 1;
                    groupName = lines[j].split("\\s+")[0];
                }
            }
        }
        return groupdescMap;
    }
}
