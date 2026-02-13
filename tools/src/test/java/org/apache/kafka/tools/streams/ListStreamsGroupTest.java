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

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(600)
@Tag("integration")
public class ListStreamsGroupTest {

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
            Set<String> expectedGroups = Set.of(APP_ID);

            final AtomicReference<Set> foundGroups = new AtomicReference<>();
            TestUtils.waitForCondition(() -> {
                foundGroups.set(new HashSet<>(service.listStreamsGroups()));
                return Objects.equals(expectedGroups, foundGroups.get());
            }, () -> "Expected --list to show streams groups " + expectedGroups + ", but found " + foundGroups.get() + ".");

        }
    }

    @Test
    public void testListWithUnrecognizedNewOption() {
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
                foundListing.set(new HashSet<>(service.listStreamsGroupsInStates(Set.of())));
                return Objects.equals(expectedListing, foundListing.get());
            }, () -> "Expected --list to show streams groups " + expectedListing + ", but found " + foundListing.get() + ".");
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
                foundListing.set(new HashSet<>(service.listStreamsGroupsInStates(Set.of())));
                return Objects.equals(expectedListing, foundListing.get());
            }, () -> "Expected --list to show streams groups " + expectedListing + ", but found " + foundListing.get() + ".");
        }

        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--list", "--state", "PreparingRebalance"})) {
            Set<GroupListing> expectedListing = Set.of();

            final AtomicReference<Set<GroupListing>> foundListing = new AtomicReference<>();

            TestUtils.waitForCondition(() -> {
                foundListing.set(new HashSet<>(service.listStreamsGroupsInStates(Set.of(GroupState.PREPARING_REBALANCE))));
                return Objects.equals(expectedListing, foundListing.get());
            }, () -> "Expected --list to show streams groups " + expectedListing + ", but found " + foundListing.get() + ".");
        }
    }

    @Test
    public void testListStreamsGroupOutput() throws Exception {
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

    private static Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> List.of(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
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