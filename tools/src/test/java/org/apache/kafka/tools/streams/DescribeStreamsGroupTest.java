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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
        @ClusterConfigProperty(key = STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "100"),
    }
)
public class DescribeStreamsGroupTest {
    private static final String APP_ID = "streams-group-command-test";
    private static final String APP_ID_2 = "streams-group-command-test-2";

    private static final String INPUT_TOPIC = "customInputTopic";
    private static final String OUTPUT_TOPIC = "customOutputTopic";
    private static final String INPUT_TOPIC_2 = "customInputTopic2";
    private static final String OUTPUT_TOPIC_2 = "customOutputTopic2";

    @Test
    public void testDescribeWithUnrecognizedOption() {
        String[] args = new String[]{"--unrecognized-option", "--bootstrap-server", "localhost:9092", "--describe", "--group", APP_ID};
        assertThrows(OptionException.class, () -> getStreamsGroupService(args));
    }

    @ClusterTest
    public void testDescribeWithoutGroupOption(ClusterInstance cluster) {
        final String[] args = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--describe"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [describe] takes one of these options: [all-groups], [group]"));
            exited.set(true);
        }));
        try (StreamsGroupCommand.StreamsGroupService ignored = getStreamsGroupService(args)) {
            assertTrue(exited.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @ClusterTest
    public void testDescribeStreamsGroup(ClusterInstance cluster) throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "TOPIC", "PARTITION", "OFFSET-LAG");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, INPUT_TOPIC, "0", "0"),
            List.of(APP_ID, INPUT_TOPIC, "1", "0"),
            List.of(APP_ID, "streams-group-command-test-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition", "0", "0"),
            List.of(APP_ID, "streams-group-command-test-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition", "1", "0"));

        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster, APP_ID, INPUT_TOPIC, OUTPUT_TOPIC)) {
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--group", APP_ID), expectedHeader, expectedRows, List.of());
            // --describe --offsets has the same output as --describe
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--offsets", "--group", APP_ID), expectedHeader, expectedRows, List.of());
        }
    }

    @ClusterTest
    public void testDescribeStreamsGroupWithVerboseOption(ClusterInstance cluster) throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LEADER-EPOCH", "LOG-END-OFFSET", "OFFSET-LAG");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, INPUT_TOPIC, "0", "-", "-", "0", "0"),
            List.of(APP_ID, INPUT_TOPIC, "1", "-", "-", "0", "0"),
            List.of(APP_ID, "streams-group-command-test-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition", "0", "-", "-", "0", "0"),
            List.of(APP_ID, "streams-group-command-test-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition", "1", "-", "-", "0", "0"));

        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster, APP_ID, INPUT_TOPIC, OUTPUT_TOPIC)) {
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--group", APP_ID), expectedHeader, expectedRows, List.of());
            // --describe --offsets has the same output as --describe
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--offsets", "--verbose", "--group", APP_ID), expectedHeader, expectedRows, List.of());
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--offsets", "--group", APP_ID), expectedHeader, expectedRows, List.of());
        }
    }

    @ClusterTest
    public void testDescribeStreamsGroupWithStateOption(ClusterInstance cluster) throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "COORDINATOR", "(ID)", "STATE", "#MEMBERS");
        final Set<List<String>> expectedRows = Set.of(List.of(APP_ID, "", "", "Stable", "2"));
        // The coordinator is not deterministic, so we don't care about it.
        final List<Integer> dontCares = List.of(1, 2);

        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster, APP_ID, INPUT_TOPIC, OUTPUT_TOPIC)) {
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--state", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
        }
    }

    @ClusterTest
    public void testDescribeStreamsGroupWithStateAndVerboseOptions(ClusterInstance cluster) throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "COORDINATOR", "(ID)", "STATE", "GROUP-EPOCH", "TARGET-ASSIGNMENT-EPOCH", "#MEMBERS");
        final Set<List<String>> expectedRows = Set.of(List.of(APP_ID, "", "", "Stable", "", "", "2"));
        // The coordinator is not deterministic, so we don't care about it.
        // The GROUP-EPOCH and TARGET-ASSIGNMENT-EPOCH can vary due to rebalance timing, so we don't care about them either.
        final List<Integer> dontCares = List.of(1, 2, 4, 5);

        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster, APP_ID, INPUT_TOPIC, OUTPUT_TOPIC)) {
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--state", "--verbose", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--state", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
        }
    }

    @ClusterTest
    public void testDescribeStreamsGroupWithMembersOption(ClusterInstance cluster) throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "MEMBER", "PROCESS", "CLIENT-ID", "ASSIGNMENTS");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, "", "", "", "ACTIVE:", "0:[1];", "1:[1];"),
            List.of(APP_ID, "", "", "", "ACTIVE:", "0:[0];", "1:[0];"));
        // The member and process names as well as client-id are not deterministic, so we don't care about them.
        final List<Integer> dontCares = List.of(1, 2, 3);

        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster, APP_ID, INPUT_TOPIC, OUTPUT_TOPIC)) {
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--members", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
        }
    }

    @ClusterTest
    public void testDescribeStreamsGroupWithMembersAndVerboseOptions(ClusterInstance cluster) throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "TARGET-ASSIGNMENT-EPOCH", "TOPOLOGY-EPOCH", "MEMBER", "MEMBER-PROTOCOL", "MEMBER-EPOCH", "PROCESS", "CLIENT-ID", "ASSIGNMENTS");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, "", "0", "", "streams", "", "", "", "ACTIVE:", "0:[1];", "1:[1];", "TARGET-ACTIVE:", "0:[1];", "1:[1];"),
            List.of(APP_ID, "", "0", "", "streams", "", "", "", "ACTIVE:", "0:[0];", "1:[0];", "TARGET-ACTIVE:", "0:[0];", "1:[0];"));
        // The member and process names as well as client-id are not deterministic, so we don't care about them.
        // The TARGET-ASSIGNMENT-EPOCH and MEMBER-EPOCH can vary due to rebalance timing, so we don't care about them either.
        final List<Integer> dontCares = List.of(1, 3, 5, 6, 7);

        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster, APP_ID, INPUT_TOPIC, OUTPUT_TOPIC)) {
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--members", "--verbose", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--members", "--group", APP_ID), expectedHeader, expectedRows, dontCares);
        }
    }

    @ClusterTest
    public void testDescribeMultipleStreamsGroupWithMembersAndVerboseOptions(ClusterInstance cluster) throws Exception {
        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        cluster.createTopic(INPUT_TOPIC_2, 1, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster, APP_ID, INPUT_TOPIC, OUTPUT_TOPIC);
             KafkaStreams ignored2 = startStreamsApp(cluster, APP_ID_2, INPUT_TOPIC_2, OUTPUT_TOPIC_2)) {
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
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--members", "--verbose", "--group", APP_ID, "--group", APP_ID_2),
                expectedHeader, expectedRowsMap, dontCares);
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--members", "--group", APP_ID, "--group", APP_ID_2),
                expectedHeader, expectedRowsMap, dontCares);
            validateDescribeOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--members", "--all-groups"),
                expectedHeader, expectedRowsMap, dontCares);
        }
    }

    @ClusterTest
    public void testDescribeNonExistingStreamsGroup(ClusterInstance cluster) {
        final String nonExistingGroup = "non-existing-group";
        final String errorMessage = String.format(
            "Error: Executing streams group command failed due to org.apache.kafka.common.errors.GroupIdNotFoundException: Group %s not found.",
            nonExistingGroup);

        validateDescribeOutput(
            List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--members", "--verbose", "--group", nonExistingGroup), errorMessage);
        validateDescribeOutput(
            List.of("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--members", "--group", nonExistingGroup), errorMessage);
    }

    private KafkaStreams startStreamsApp(ClusterInstance cluster, String appId, String inputTopic, String outputTopic) throws InterruptedException {
        KafkaStreams streams = new KafkaStreams(topology(inputTopic, outputTopic), streamsProp(cluster, appId));
        StreamsGroupCommandTestUtils.startApplicationAndWaitUntilRunning(streams);
        return streams;
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

    private Properties streamsProp(ClusterInstance cluster, String appId) {
        Properties streamsProp = new Properties();
        streamsProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
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
