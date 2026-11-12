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
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
public class ListStreamsGroupTest {
    private static final String APP_ID = "streams-group-command-test";
    private static final String INPUT_TOPIC = "customInputTopic";
    private static final String OUTPUT_TOPIC = "customOutputTopic";

    @Test
    public void testListWithUnrecognizedNewOption() {
        String[] cgcArgs = new String[]{"--new-option", "--bootstrap-server", "localhost:9092", "--list"};
        Assertions.assertThrows(OptionException.class, () -> getStreamsGroupService(cgcArgs));
    }

    @ClusterTest
    public void testListStreamsGroupWithoutFilters(ClusterInstance cluster) throws Exception {
        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster);
             StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--list"})) {
            Set<String> expectedGroups = Set.of(APP_ID);

            final AtomicReference<Set> foundGroups = new AtomicReference<>();
            TestUtils.waitForCondition(() -> {
                foundGroups.set(new HashSet<>(service.listStreamsGroups()));
                return Objects.equals(expectedGroups, foundGroups.get());
            }, () -> "Expected --list to show streams groups " + expectedGroups + ", but found " + foundGroups.get() + ".");
        }
    }

    @ClusterTest
    public void testListStreamsGroupWithStates(ClusterInstance cluster) throws Exception {
        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster);
             StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--list", "--state"})) {
            Set<GroupListing> expectedListing = Set.of(
                new GroupListing(
                    APP_ID,
                    Optional.of(GroupType.STREAMS),
                    "streams",
                    Optional.of(GroupState.STABLE))
            );

            final AtomicReference<Set<GroupListing>> foundListing = new AtomicReference<>();

            TestUtils.waitForCondition(() -> {
                foundListing.set(new HashSet<>(service.listStreamsGroupsInStates(Set.of())));
                return Objects.equals(expectedListing, foundListing.get());
            }, () -> "Expected --list to show streams groups " + expectedListing + ", but found " + foundListing.get() + ".");
        }
    }

    @ClusterTest
    public void testListStreamsGroupWithSpecifiedStates(ClusterInstance cluster) throws Exception {
        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster);
             StreamsGroupCommand.StreamsGroupService stableService = getStreamsGroupService(new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--list", "--state", "stable"});
             StreamsGroupCommand.StreamsGroupService rebalanceService = getStreamsGroupService(new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--list", "--state", "PreparingRebalance"})) {
            Set<GroupListing> expectedStableListing = Set.of(
                new GroupListing(
                    APP_ID,
                    Optional.of(GroupType.STREAMS),
                    "streams",
                    Optional.of(GroupState.STABLE))
            );

            final AtomicReference<Set<GroupListing>> foundStableListing = new AtomicReference<>();
            TestUtils.waitForCondition(() -> {
                foundStableListing.set(new HashSet<>(stableService.listStreamsGroupsInStates(Set.of())));
                return Objects.equals(expectedStableListing, foundStableListing.get());
            }, () -> "Expected --list to show streams groups " + expectedStableListing + ", but found " + foundStableListing.get() + ".");

            Set<GroupListing> expectedRebalanceListing = Set.of();
            final AtomicReference<Set<GroupListing>> foundRebalanceListing = new AtomicReference<>();
            TestUtils.waitForCondition(() -> {
                foundRebalanceListing.set(new HashSet<>(rebalanceService.listStreamsGroupsInStates(Set.of(GroupState.PREPARING_REBALANCE))));
                return Objects.equals(expectedRebalanceListing, foundRebalanceListing.get());
            }, () -> "Expected --list to show streams groups " + expectedRebalanceListing + ", but found " + foundRebalanceListing.get() + ".");
        }
    }

    @ClusterTest
    public void testListStreamsGroupOutput(ClusterInstance cluster) throws Exception {
        cluster.createTopic(INPUT_TOPIC, 2, (short) 1);
        try (KafkaStreams ignored = startStreamsApp(cluster)) {
            validateListOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--list"),
                List.of(),
                Set.of(List.of(APP_ID))
            );

            validateListOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--list", "--state"),
                List.of("GROUP", "STATE"),
                Set.of(List.of(APP_ID, "Stable"))
            );

            validateListOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--list", "--state", "Stable"),
                List.of("GROUP", "STATE"),
                Set.of(List.of(APP_ID, "Stable"))
            );

            // Check case-insensitivity in state filter.
            validateListOutput(
                List.of("--bootstrap-server", cluster.bootstrapServers(), "--list", "--state", "stable"),
                List.of("GROUP", "STATE"),
                Set.of(List.of(APP_ID, "Stable"))
            );
        }
    }

    private KafkaStreams startStreamsApp(ClusterInstance cluster) throws InterruptedException {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProp(cluster));
        StreamsGroupCommandTestUtils.startApplicationAndWaitUntilRunning(streams);
        return streams;
    }

    private static Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> List.of(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    private Properties streamsProp(ClusterInstance cluster) {
        Properties streamsProp = new Properties();
        streamsProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsProp.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
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

    private static void validateListOutput(
        List<String> args,
        List<String> expectedHeader,
        Set<List<String>> expectedRows
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> assertEquals(0, StreamsGroupCommand.execute(args.toArray(new String[0]))));
            out.set(output);

            String[] lines = output.split("\n");
            if (lines.length == 1 && lines[0].isEmpty()) lines = new String[]{};

            if (!expectedHeader.isEmpty() && lines.length > 0) {
                List<String> header = List.of(lines[0].split("\\s+"));
                if (!expectedHeader.equals(header)) return false;
            }

            Set<List<String>> groups = Arrays.stream(lines, expectedHeader.isEmpty() ? 0 : 1, lines.length)
                .map(line -> List.of(line.split("\\s+")))
                .collect(Collectors.toSet());
            return expectedRows.equals(groups);
        }, () -> String.format("Expected header=%s and groups=%s, but found:%n%s", expectedHeader, expectedRows, out.get()));
    }
}
