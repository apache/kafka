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
        final List<String> expectedHeaders = List.of("GROUP", "TOPIC", "PARTITION", "OFFSET-LAG");
        final Set<List<String>> expectedRows = Set.of(List.of(APP_ID, "", "0", "0"), List.of(APP_ID, "", "1", "0"));
        final List<Integer> dontCares = List.of(1);


        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe"), expectedHeaders, expectedRows, 5, dontCares);

        // --describe --offsets has the same output as --describe
        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--offsets"), expectedHeaders, expectedRows, 5, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithVerboseOption() throws Exception {
        final List<String> expectedHeaders = List.of("GROUP", "TOPIC", "PARTITION", "LEADER-EPOCH", "OFFSET-LAG");
        final Set<List<String>> expectedRows = Set.of(List.of(APP_ID, "", "0", "2", "0"), List.of(APP_ID, "", "1", "2", "0"));
        final List<Integer> dontCares = List.of(1);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose"), expectedHeaders, expectedRows, 5, dontCares);
        // --describe --offsets has the same output as --describe
        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--offsets", "--verbose"), expectedHeaders, expectedRows, 5, dontCares);
        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--offsets"), expectedHeaders, expectedRows, 5, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithStateOption() throws Exception {
        final List<String> expectedHeaders = Arrays.asList("GROUP", "COORDINATOR", "(ID)", "STATE", "#MEMBERS");
        final Set<List<String>> expectedRows = Set.of(Arrays.asList(APP_ID, "", "", "Stable", "1"));
        final List<Integer> dontCares = List.of(1, 2);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--state"), expectedHeaders, expectedRows, 2, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithStateAndVerboseOptions() throws Exception {
        final List<String> expectedHeaders = Arrays.asList("GROUP", "COORDINATOR", "(ID)", "STATE", "GROUP-EPOCH", "TARGET-ASSIGNMENT-EPOCH", "#MEMBERS");
        final Set<List<String>> expectedRows = Set.of(Arrays.asList(APP_ID, "", "", "Stable", "2", "2", "1"));
        final List<Integer> dontCares = List.of(1, 2);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--state", "--verbose"), expectedHeaders, expectedRows, 2, dontCares);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--verbose", "--state"), expectedHeaders, expectedRows, 2, dontCares);
    }

    @Test
    public void testDescribeStreamsGroupWithMembersOption() throws Exception {
        final Set<MemberRows> expectedRows = Set.of(
            new MemberRows(
                List.of("GROUP", "MEMBER", "PROCESS", "CLIENT-ID"),
                List.of(APP_ID, "", "", ""),
                Arrays.stream("ACTIVE-TASKS: 0:[0,1] 1:[0,1]".split("\\s+")).toList(),
                List.of("STANDBY-TASKS:"),
                List.of("WARMUP-TASKS:")
            ));
        final List<Integer> dontCares = List.of(1, 2, 3);

        validateDescribeOutput(
            Arrays.asList("--bootstrap-server", cluster.bootstrapServers(), "--describe", "--members"), expectedRows, dontCares, false);
    }

    @Test
    public void testDescribeStreamsGroupWithMembersAndVerboseOptions() throws Exception {
        final Set<MemberRows> expectedRows = Set.of(new MemberRows(
            List.of("GROUP", "TARGET-ASSIGNMENT-EPOCH", "TOPOLOGY-EPOCH", "MEMBER", "MEMBER-PROTOCOL", "MEMBER-EPOCH", "PROCESS", "CLIENT-ID"),
            List.of(APP_ID, "2", "0", "", "streams", "2", "", ""),
            Arrays.stream("ACTIVE-TASKS: 0:[0,1] 1:[0,1]".split("\\s+")).toList(), List.of("STANDBY-TASKS:"), List.of("WARMUP-TASKS:"),
            Arrays.stream("TARGET-ACTIVE-TASKS: 0:[0,1] 1:[0,1]".split("\\s+")).toList(), List.of("TARGET-STANDBY-TASKS:"), List.of("TARGET-WARMUP-TASKS:")));
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
        int expectedSize,
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

            Set<List<String>> groupDesc = parseLines(lines, 1, dontCareIndices).stream().collect(Collectors.toSet());
            return expectedRows.equals(groupDesc) && lines.length == expectedSize;
        }, () -> String.format("Expected header=%s and groups=%s, but found:%n%s", expectedHeader, expectedRows, out.get()));
    }

    private static void validateDescribeOutput(
        List<String> args,
        Set<MemberRows> expectedRows,
        List<Integer> dontCareIndices,
        boolean verbose
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> StreamsGroupCommand.main(args.toArray(new String[0])));
            out.set(output);

            String[] lines = output.split("\n");
            if (lines.length == 1 && lines[0].isEmpty()) lines = new String[]{};

            List<String> header = Arrays.asList(lines[0].split("\\s+"));
            List<List<String>> groupDesc = parseLines(lines, 1, dontCareIndices);
            MemberRows memberRows = verbose ? new MemberRows(header, groupDesc.get(0), groupDesc.get(1), groupDesc.get(2), groupDesc.get(3), groupDesc.get(4), groupDesc.get(5), groupDesc.get(6))
                : new MemberRows(header, groupDesc.get(0), groupDesc.get(1), groupDesc.get(2), groupDesc.get(3));
            Set<MemberRows> actualRows = Set.of(memberRows);

            return expectedRows.equals(actualRows);
        }, () -> String.format("expected=%s, but found:%n%s", expectedRows, out.get()));
    }

    private static List<List<String>> parseLines(String[] lines, int index, List<Integer> dontCareIndices) {
        return Arrays.stream(lines, index, lines.length)
            .map(line -> line.isEmpty() ? Collections.<String>emptyList()
                : line.contains("TASKS:") ? Arrays.asList(line.split("\\s+"))
                : clearDontCares(Arrays.asList(line.split("\\s+")), dontCareIndices))
            .collect(Collectors.toList());
    }

    private static class MemberRows {
        List<String> headers;
        List<String> group;
        List<String> activeTasks;
        List<String> standbyTasks;
        List<String> warmupTasks;
        List<String> targetActiveTasks;
        List<String> targetStandbyTasks;
        List<String> targetWarmupTasks;

        public MemberRows(List<String> headers, List<String> group, List<String> activeTasks, List<String> standbyTasks, List<String> warmupTasks) {
            this.headers = headers;
            this.group = group;
            this.activeTasks = activeTasks;
            this.standbyTasks = standbyTasks;
            this.warmupTasks = warmupTasks;
        }

        public MemberRows(List<String> headers, List<String> group, List<String> activeTasks, List<String> standbyTasks, List<String> warmupTasks,
                          List<String> targetActiveTasks, List<String> targetStandbyTasks, List<String> targetWarmupTasks) {
            this.headers = headers;
            this.group = group;
            this.activeTasks = activeTasks;
            this.standbyTasks = standbyTasks;
            this.warmupTasks = warmupTasks;
            this.targetActiveTasks = targetActiveTasks;
            this.targetStandbyTasks = targetStandbyTasks;
            this.targetWarmupTasks = targetWarmupTasks;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MemberRows rows = (MemberRows) o;
            return headers.equals(rows.headers) &&
                group.equals(rows.group) &&
                activeTasks.equals(rows.activeTasks) &&
                standbyTasks.equals(rows.standbyTasks) &&
                warmupTasks.equals(rows.warmupTasks) &&
                Objects.equals(targetActiveTasks, rows.targetActiveTasks) &&
                Objects.equals(targetStandbyTasks, rows.targetStandbyTasks) &&
                Objects.equals(targetWarmupTasks, rows.targetWarmupTasks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(headers, group, activeTasks, standbyTasks, warmupTasks);
        }
    }

    /**
     * Replaces the dontCare field values with empty string
     * @param groupDesc the group to clear
     * @param dontCareIndices the indices to clear
     * @return the group description with the dontCareIndices cleared
     */
    private static List<String> clearDontCares(List<String> groupDesc, List<Integer> dontCareIndices) {
        for (Integer dontCareIndex : dontCareIndices) {
            groupDesc.set(dontCareIndex, "");
        }
        return groupDesc;
    }
}
