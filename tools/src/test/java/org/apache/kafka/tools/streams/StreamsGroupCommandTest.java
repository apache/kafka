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
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.serialization.Serdes;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;

@Timeout(600)
@Tag("integration")
public class StreamsGroupCommandTest {

    public static EmbeddedKafkaCluster cluster = null;
    static KafkaStreams streams;
    private static final String APP_ID = "streams-group-command-test";
    private static final String INPUT_TOPIC = "customInputTopic";
    private static final String OUTPUT_TOPIC = "customOutputTopic";

    @BeforeAll
    public static void setup() throws Exception {
        // start the cluster and create the input topic
        final Properties props = new Properties();
        props.setProperty(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams");
        cluster = new EmbeddedKafkaCluster(1, props);
        cluster.start();
        cluster.createTopic(INPUT_TOPIC, 2, 1);


        // start kafka streams
        Properties streamsProp = new Properties();
        streamsProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsProp.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        streamsProp.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsProp.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));

        streams = new KafkaStreams(topology(), streamsProp);
        startApplicationAndWaitUntilRunning(streams);
    }

    @AfterAll
    public static void closeCluster() {
        streams.close();
        cluster.stop();
        cluster = null;
    }

    @Test
    public void testListStreamsGroupWithoutFilters() throws Exception {
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--list"})) {
            Set<String> expectedGroups = new HashSet<>(Collections.singleton(APP_ID));

            final AtomicReference<Set> foundGroups = new AtomicReference<>();
            TestUtils.waitForCondition(() -> {
                foundGroups.set(new HashSet<>(service.listStreamsGroups()));
                return Objects.equals(expectedGroups, foundGroups.get());
            }, "Expected --list to show streams groups " + expectedGroups + ", but found " + foundGroups.get() + ".");

        }
    }

    @Test
    public void testListWithUnrecognizedNewOption() throws Exception {
        String[] cgcArgs = new String[]{"--new-option", "--bootstrap-server", cluster.bootstrapServers(), "--list"};
        Assertions.assertThrows(OptionException.class, () -> getStreamsGroupService(cgcArgs));
    }

    @Test
    public void testListStreamsGroupWithStates() throws Exception {
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--list", "--state"})) {
            Set<GroupListing> expectedListing = Set.of(
                new GroupListing(
                    APP_ID,
                    Optional.of(GroupType.STREAMS),
                    "streams",
                    Optional.of(GroupState.STABLE))
            );

            final AtomicReference<Set<GroupListing>> foundListing = new AtomicReference<>();

            TestUtils.waitForCondition(() -> {
                foundListing.set(new HashSet<>(service.listStreamsGroupsInStates(Collections.emptySet())));
                return Objects.equals(expectedListing, foundListing.get());
            }, "Expected --list to show streams groups " + expectedListing + ", but found " + foundListing.get() + ".");
        }
    }

    @Test
    public void testListStreamsGroupWithSpecifiedStates() throws Exception {
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--list", "--state", "stable"})) {
            Set<GroupListing> expectedListing = Set.of(
                new GroupListing(
                    APP_ID,
                    Optional.of(GroupType.STREAMS),
                    "streams",
                    Optional.of(GroupState.STABLE))
            );

            final AtomicReference<Set<GroupListing>> foundListing = new AtomicReference<>();

            TestUtils.waitForCondition(() -> {
                foundListing.set(new HashSet<>(service.listStreamsGroupsInStates(Collections.emptySet())));
                return Objects.equals(expectedListing, foundListing.get());
            }, "Expected --list to show streams groups " + expectedListing + ", but found " + foundListing.get() + ".");
        }

        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--list", "--state", "PreparingRebalance"})) {
            Set<GroupListing> expectedListing = Collections.emptySet();

            final AtomicReference<Set<GroupListing>> foundListing = new AtomicReference<>();

            TestUtils.waitForCondition(() -> {
                foundListing.set(new HashSet<>(service.listStreamsGroupsInStates(Collections.singleton(GroupState.PREPARING_REBALANCE))));
                return Objects.equals(expectedListing, foundListing.get());
            }, "Expected --list to show streams groups " + expectedListing + ", but found " + foundListing.get() + ".");
        }
    }

    @Test
    public void testListStreamsGroupOutput() throws Exception {
        validateListOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--list"),
            Collections.emptyList(),
            Set.of(Collections.singletonList(APP_ID))
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--list", "--state"),
            Arrays.asList("GROUP", "STATE"),
            Set.of(Arrays.asList(APP_ID, "Stable"))
        );

        validateListOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--list", "--state", "Stable"),
            Arrays.asList("GROUP", "STATE"),
            Set.of(Arrays.asList(APP_ID, "Stable"))
        );

        // Check case-insensitivity in state filter.
        validateListOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--list", "--state", "stable"),
            Arrays.asList("GROUP", "STATE"),
            Set.of(Arrays.asList(APP_ID, "Stable"))
        );
    }

    @Test
    public void testDescribeStreamsGroup() throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "TOPIC", "PARTITION", "OFFSET-LAG");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, INPUT_TOPIC, "0", "0"),
            List.of(APP_ID, INPUT_TOPIC, "1", "0"),
            List.of(APP_ID, "some-state-store-topic", "0", "0"),
            List.of(APP_ID, "some-state-store-topic", "1", "0"));
        // The state-store-topic name is not deterministic, so we don't care about topic names.
        final List<Integer> dontCares = List.of(1);


        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe"), expectedHeader, expectedRows, dontCares);
        // --describe --offsets has the same output as --describe
        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--offsets"), expectedHeader, expectedRows, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithVerboseOption() throws Exception {
        final List<String> expectedHeader = List.of("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "OFFSET-LAG");
        final Set<List<String>> expectedRows = Set.of(
            List.of(APP_ID, INPUT_TOPIC, "0", "-", "0", "0"),
            List.of(APP_ID, INPUT_TOPIC, "1", "-", "0", "0"),
            List.of(APP_ID, "some-state-store-topic", "0", "-", "0", "0"),
            List.of(APP_ID, "some-state-store-topic", "1", "-", "0", "0"));
        // The state-store-topic name is not deterministic, so we don't care about topic names.
        final List<Integer> dontCares = List.of(1);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose"), expectedHeader, expectedRows, dontCares);
        // --describe --offsets has the same output as --describe
        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--offsets", "--verbose"), expectedHeader, expectedRows, dontCares);
        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--offsets"), expectedHeader, expectedRows, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithStateOption() throws Exception {
        final List<String> expectedHeader = Arrays.asList("GROUP", "COORDINATOR", "(ID)", "STATE", "#MEMBERS");
        final Set<List<String>> expectedRows = Set.of(Arrays.asList(APP_ID, "", "", "Stable", "2"));
        // The coordinator is not deterministic, so we don't care about it.
        final List<Integer> dontCares = List.of(1, 2);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--state"), expectedHeader, expectedRows, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithStateAndVerboseOptions() throws Exception {
        final List<String> expectedHeader = Arrays.asList("GROUP", "COORDINATOR", "(ID)", "STATE", "GROUP-EPOCH", "TARGET-ASSIGNMENT-EPOCH", "#MEMBERS");
        final Set<List<String>> expectedRows = Set.of(Arrays.asList(APP_ID, "localhost:57555", "(0)", "Stable", "3", "3", "2"));
        // The coordinator is not deterministic, so we don't care about it.
        final List<Integer> dontCares = List.of(1, 2);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--state", "--verbose"), expectedHeader, expectedRows, dontCares);
        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--state"), expectedHeader, expectedRows, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithMembersOption() throws Exception {
        final Set<List<List<String>>> expectedRows = Set.of(
            List.of(
                List.of("GROUP", "MEMBER", "PROCESS", "CLIENT-ID"),
                List.of(APP_ID, "", "", ""),
                List.of("ACTIVE-TASKS:", "0:[0,1]"),
                List.of("STANDBY-TASKS:", ""),
                List.of("WARMUP-TASKS:", "")),

            List.of(
                List.of("GROUP", "MEMBER", "PROCESS", "CLIENT-ID"),
                List.of(APP_ID, "", "", ""),
                List.of("ACTIVE-TASKS:", "1:[0,1]"),
                List.of("STANDBY-TASKS:", ""),
                List.of("WARMUP-TASKS:", "")));
        // The member and process names as well as client-id are not deterministic, so we don't care about them.
        final List<Integer> dontCares = List.of(1, 2, 3);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--members"), expectedRows, dontCares, false);
    }

    @Test
    public void testDescribeStreamsGroupWithMembersAndVerboseOptions() throws Exception {
        final Set<List<List<String>>> expectedRows = Set.of(
            List.of(
                List.of("GROUP", "TARGET-ASSIGNMENT-EPOCH", "TOPOLOGY-EPOCH", "MEMBER", "MEMBER-PROTOCOL", "MEMBER-EPOCH", "PROCESS", "CLIENT-ID"),
                List.of(APP_ID, "3", "0", "", "streams", "3", "", ""),
                List.of("ACTIVE-TASKS:", "0:[0,1]"),
                List.of("STANDBY-TASKS:", ""),
                List.of("WARMUP-TASKS:", ""),
                List.of("TARGET-ACTIVE-TASKS:", "0:[0,1]"),
                List.of("TARGET-STANDBY-TASKS:", ""),
                List.of("TARGET-WARMUP-TASKS:", "")),

            List.of(
                List.of("GROUP", "TARGET-ASSIGNMENT-EPOCH", "TOPOLOGY-EPOCH", "MEMBER", "MEMBER-PROTOCOL", "MEMBER-EPOCH", "PROCESS", "CLIENT-ID"),
                List.of(APP_ID, "3", "0", "", "streams", "3", "", ""),
                List.of("ACTIVE-TASKS:", "1:[0,1]"),
                List.of("STANDBY-TASKS:", ""),
                List.of("WARMUP-TASKS:", ""),
                List.of("TARGET-ACTIVE-TASKS:", "1:[0,1]"),
                List.of("TARGET-STANDBY-TASKS:", ""),
                List.of("TARGET-WARMUP-TASKS:", "")));
        // The member and process names as well as client-id are not deterministic, so we don't care about them.
        final List<Integer> dontCares = List.of(3, 6, 7);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--members", "--verbose"), expectedRows, dontCares, true);
        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--members"), expectedRows, dontCares, true);
    }

    private static Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    private StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = StreamsGroupCommandOptions.fromArgs(args);
        return new StreamsGroupCommand.StreamsGroupService(
            opts,
            Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private static void validateListOutput(
        List<String> args,
        List<String> expectedHeader,
        Set<List<String>> expectedRows
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> StreamsGroupCommand.main(args.toArray(new String[0])));
            out.set(output);

            String[] lines = output.split("\n");
            if (lines.length == 1 && lines[0].isEmpty()) lines = new String[]{};

            if (!expectedHeader.isEmpty() && lines.length > 0) {
                List<String> header = Arrays.asList(lines[0].split("\\s+"));
                if (!expectedHeader.equals(header)) return false;
            }

            Set<List<String>> groups = Arrays.stream(lines, expectedHeader.isEmpty() ? 0 : 1, lines.length)
                .map(line -> Arrays.asList(line.split("\\s+")))
                .collect(Collectors.toSet());
            return expectedRows.equals(groups);
        }, () -> String.format("Expected header=%s and groups=%s, but found:%n%s", expectedHeader, expectedRows, out.get()));
    }

    private static void validateDescribeOutput(
        List<String> args,
        List<String> expectedHeader,
        Set<List<String>> expectedRows,
        List<Integer> dontCareIndices
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> StreamsGroupCommand.main(args.toArray(new String[0])));
            out.set(output);

            String[] lines = output.split("\n");
            if (lines.length == 1 && lines[0].isEmpty()) lines = new String[]{};

            if (lines.length == 0) return false;
            List<String> header = Arrays.asList(lines[0].split("\\s+"));
            if (!expectedHeader.equals(header)) return false;

            Set<List<String>> groupDesc = Arrays.stream(Arrays.copyOfRange(lines, 1, lines.length))
                .map(line -> Arrays.asList(line.split("\\s+")))
                .collect(Collectors.toSet());
            if (groupDesc.size() != expectedRows.size()) return false;
            // clear the dontCare fields and then compare two sets
            return expectedRows.stream()
                .map(list -> {
                    List<String> listCloned = new ArrayList<>(list);
                    dontCareIndices.forEach(index -> listCloned.set(index, ""));
                    return listCloned;
                }).collect(Collectors.toSet())
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
        Set<List<List<String>>> expectedRows,
        List<Integer> dontCareIndices,
        boolean verbose
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> StreamsGroupCommand.main(args.toArray(new String[0])));
            out.set(output);

            String[] lines = output.split("\n");
            if (lines.length == 1 && lines[0].isEmpty()) lines = new String[]{};

            Set<List<List<String>>> actualOutput = new HashSet<>();
            int taskListSize = verbose ? 6 : 3;
            for (int i = 0; i < lines.length; i++) {
                List<String> header = Arrays.asList(lines[i].split("\\s+"));
                List<String> groupDesc = Arrays.asList(lines[++i].split("\\s+"));
                dontCareIndices.forEach(index -> groupDesc.set(index, ""));
                List<List<String>> tasks = Arrays.stream(Arrays.copyOfRange(lines, ++i, taskListSize + i))
                    .map(line -> {
                        String[] task = line.split("TASKS: ");
                        return List.of(task[0] + "TASKS:", task.length == 2 ? task[1].trim() : "");
                    }).toList();
                List<List<String>> outputList = new ArrayList<>(List.of(header, groupDesc));
                outputList.addAll(tasks);
                actualOutput.add(outputList);
                i += taskListSize;
            }

            return expectedRows.equals(actualOutput);
        }, () -> String.format("Expected description=%s but found:%n%s", expectedRows, out.get()));
    }
}
