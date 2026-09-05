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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import joptsimple.OptionException;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListConsumerGroupTest {
    private static final String TOPIC_PREFIX = "test.topic.";
    private static final String TOPIC_PARTITIONS_GROUP_PREFIX = "test.topic.partitions.group.";
    private static final String TOPIC_GROUP_PREFIX = "test.topic.group.";
    private static final String PROTOCOL_GROUP_PREFIX = "test.protocol.group.";
    private static final String DUMMY_BOOTSTRAP_SERVERS = "localhost:9092";

    private static List<GroupProtocol> supportedGroupProtocols(ClusterInstance clusterInstance) {
        return new ArrayList<>(clusterInstance.supportedGroupProtocols());
    }

    @ClusterTest(
        types = {Type.CO_KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "1000"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500")
        }
    )
    public void testListConsumerGroupsWithoutFilters(ClusterInstance clusterInstance) throws Exception {
        List<GroupProtocol> groupProtocols = supportedGroupProtocols(clusterInstance);
        for (int i = 0; i < groupProtocols.size(); i++) {
            GroupProtocol groupProtocol = groupProtocols.get(i);
            String topic = TOPIC_PREFIX + groupProtocol.name;
            String protocolGroup = PROTOCOL_GROUP_PREFIX + groupProtocol.name;
            String topicGroup = TOPIC_GROUP_PREFIX + i;
            String topicPartitionsGroup = TOPIC_PARTITIONS_GROUP_PREFIX + i;
            clusterInstance.createTopic(topic, 1, (short) 1);

            try (AutoCloseable topicPartitionsConsumerGroupExecutor = consumerGroupClosable(clusterInstance, topicPartitionsGroup, Set.of(new TopicPartition(topic, 0)));
                 AutoCloseable topicConsumerGroupExecutor = consumerGroupClosable(clusterInstance, GroupProtocol.CLASSIC, topicGroup, topic);
                 AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(clusterInstance, groupProtocol, protocolGroup, topic);
                 ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--list"})
            ) {
                Set<String> expectedGroups = Set.of(topicPartitionsGroup, topicGroup, protocolGroup);
                final AtomicReference<Set<String>> foundGroups = new AtomicReference<>(Set.of());

                TestUtils.waitForCondition(() -> {
                    foundGroups.set(new HashSet<>(service.listConsumerGroups()));
                    return Objects.equals(expectedGroups, foundGroups.get());
                }, () -> "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups.get() + ".");
            }

            removeConsumer(clusterInstance, Set.of(topicPartitionsGroup, topicGroup, protocolGroup));
            deleteTopic(clusterInstance, topic);
        }
    }

    @Test
    public void testListWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", DUMMY_BOOTSTRAP_SERVERS, "--list"};
        assertThrows(OptionException.class, () -> ConsumerGroupCommandOptions.fromArgs(cgcArgs));
    }

    @ClusterTest(
        types = {Type.CO_KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "1000"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500")
        }
    )
    public void testListConsumerGroupsWithStates(ClusterInstance clusterInstance) throws Exception {
        List<GroupProtocol> groupProtocols = supportedGroupProtocols(clusterInstance);
        for (int i = 0; i < groupProtocols.size(); i++) {
            GroupProtocol groupProtocol = groupProtocols.get(i);
            String topic = TOPIC_PREFIX + groupProtocol.name;
            String protocolGroup = PROTOCOL_GROUP_PREFIX + groupProtocol.name;
            String topicPartitionsGroup = TOPIC_PARTITIONS_GROUP_PREFIX + i;
            clusterInstance.createTopic(topic, 1, (short) 1);

            try (AutoCloseable topicPartitionsConsumerGroupExecutor = consumerGroupClosable(clusterInstance, topicPartitionsGroup, Set.of(new TopicPartition(topic, 0)));
                 AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(clusterInstance, groupProtocol, protocolGroup, topic);
                 ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state"})
            ) {
                Set<GroupListing> expectedListing = Set.of(
                        new GroupListing(
                                topicPartitionsGroup,
                                Optional.of(GroupType.CLASSIC),
                                "",
                                Optional.of(GroupState.EMPTY)
                        ),
                        new GroupListing(
                                protocolGroup,
                                Optional.of(GroupType.parse(groupProtocol.name())),
                                ConsumerProtocol.PROTOCOL_TYPE,
                                Optional.of(GroupState.STABLE)
                        )
                );

                assertGroupListing(
                        service,
                        Set.of(),
                        EnumSet.allOf(GroupState.class),
                        expectedListing
                );

                expectedListing = Set.of(
                        new GroupListing(
                                protocolGroup,
                                Optional.of(GroupType.parse(groupProtocol.name())),
                                ConsumerProtocol.PROTOCOL_TYPE,
                                Optional.of(GroupState.STABLE)
                        )
                );

                assertGroupListing(
                        service,
                        Set.of(),
                        Set.of(GroupState.STABLE),
                        expectedListing
                );

                assertGroupListing(
                        service,
                        Set.of(),
                        Set.of(GroupState.PREPARING_REBALANCE),
                        Set.of()
                );
            }

            removeConsumer(clusterInstance, Set.of(topicPartitionsGroup, protocolGroup));
            deleteTopic(clusterInstance, topic);
        }
    }

    @ClusterTest(
        types = {Type.CO_KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "1000"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500")
        }
    )
    public void testListConsumerGroupsWithTypesClassicProtocol(ClusterInstance clusterInstance) throws Exception {
        GroupProtocol groupProtocol = GroupProtocol.CLASSIC;
        String topic = TOPIC_PREFIX + groupProtocol.name;
        String protocolGroup = PROTOCOL_GROUP_PREFIX + groupProtocol.name;
        String topicPartitionsGroup = TOPIC_PARTITIONS_GROUP_PREFIX + "0";
        clusterInstance.createTopic(topic, 1, (short) 1);

        try (AutoCloseable topicPartitionsConsumerGroupExecutor = consumerGroupClosable(clusterInstance, topicPartitionsGroup, Set.of(new TopicPartition(topic, 0)));
             AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(clusterInstance, groupProtocol, protocolGroup, topic);
             ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state"})
        ) {
            Set<GroupListing> expectedListing = Set.of(
                    new GroupListing(
                            topicPartitionsGroup,
                            Optional.of(GroupType.CLASSIC),
                            "",
                            Optional.of(GroupState.EMPTY)
                    ),
                    new GroupListing(
                            protocolGroup,
                            Optional.of(GroupType.CLASSIC),
                            ConsumerProtocol.PROTOCOL_TYPE,
                            Optional.of(GroupState.STABLE)
                    )
            );

            // No filters explicitly mentioned. Expectation is that all groups are returned.
            assertGroupListing(
                    service,
                    Set.of(),
                    Set.of(),
                    expectedListing
            );

            // When group type is mentioned:
            // Old Group Coordinator returns empty listings if the type is not Classic.
            // New Group Coordinator returns groups according to the filter.
            assertGroupListing(
                    service,
                    Set.of(GroupType.CONSUMER),
                    Set.of(),
                    Set.of()
            );

            assertGroupListing(
                    service,
                    Set.of(GroupType.CLASSIC),
                    Set.of(),
                    expectedListing
            );
        }
    }

    @ClusterTest(
        types = {Type.CO_KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "1000"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500")
        }
    )
    public void testListConsumerGroupsWithTypesConsumerProtocol(ClusterInstance clusterInstance) throws Exception {
        GroupProtocol groupProtocol = GroupProtocol.CONSUMER;
        String topic = TOPIC_PREFIX + groupProtocol.name;
        String protocolGroup = PROTOCOL_GROUP_PREFIX + groupProtocol.name;
        String topicGroup = TOPIC_GROUP_PREFIX + "0";
        String topicPartitionsGroup = TOPIC_PARTITIONS_GROUP_PREFIX + "0";
        clusterInstance.createTopic(topic, 1, (short) 1);

        try (AutoCloseable topicPartitionsConsumerGroupExecutor = consumerGroupClosable(clusterInstance, topicPartitionsGroup, Set.of(new TopicPartition(topic, 0)));
             AutoCloseable topicConsumerGroupExecutor = consumerGroupClosable(clusterInstance, GroupProtocol.CLASSIC, topicGroup, topic);
             AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(clusterInstance, groupProtocol, protocolGroup, topic);
             ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--list"})
        ) {


            // No filters explicitly mentioned. Expectation is that all groups are returned.
            Set<GroupListing> expectedListing = Set.of(
                    new GroupListing(
                            topicPartitionsGroup,
                            Optional.of(GroupType.CLASSIC),
                            "",
                            Optional.of(GroupState.EMPTY)
                    ),
                    new GroupListing(
                            topicGroup,
                            Optional.of(GroupType.CLASSIC),
                            ConsumerProtocol.PROTOCOL_TYPE,
                            Optional.of(GroupState.STABLE)
                    ),
                    new GroupListing(
                            protocolGroup,
                            Optional.of(GroupType.CONSUMER),
                            ConsumerProtocol.PROTOCOL_TYPE,
                            Optional.of(GroupState.STABLE)
                    )
            );

            assertGroupListing(
                    service,
                    Set.of(),
                    Set.of(),
                    expectedListing
            );

            // When group type is mentioned:
            // New Group Coordinator returns groups according to the filter.
            expectedListing = Set.of(
                    new GroupListing(
                            protocolGroup,
                            Optional.of(GroupType.CONSUMER),
                            ConsumerProtocol.PROTOCOL_TYPE,
                            Optional.of(GroupState.STABLE)
                    )
            );

            assertGroupListing(
                    service,
                    Set.of(GroupType.CONSUMER),
                    Set.of(),
                    expectedListing
            );

            expectedListing = Set.of(
                    new GroupListing(
                            topicPartitionsGroup,
                            Optional.of(GroupType.CLASSIC),
                            "",
                            Optional.of(GroupState.EMPTY)
                    ),
                    new GroupListing(
                            topicGroup,
                            Optional.of(GroupType.CLASSIC),
                            ConsumerProtocol.PROTOCOL_TYPE,
                            Optional.of(GroupState.STABLE)
                    )
            );

            assertGroupListing(
                    service,
                    Set.of(GroupType.CLASSIC),
                    Set.of(),
                    expectedListing
            );
        }
    }

    @ClusterTest(
        types = {Type.CO_KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "1000"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500")
        }
    )
    public void testListGroupCommandClassicProtocol(ClusterInstance clusterInstance) throws Exception {
        GroupProtocol groupProtocol = GroupProtocol.CLASSIC;
        String topic = TOPIC_PREFIX + groupProtocol.name;
        String protocolGroup = PROTOCOL_GROUP_PREFIX + groupProtocol.name;
        String topicPartitionsGroup = TOPIC_PARTITIONS_GROUP_PREFIX + "0";
        clusterInstance.createTopic(topic, 1, (short) 1);

        try (AutoCloseable topicPartitionsConsumerGroupExecutor = consumerGroupClosable(clusterInstance, topicPartitionsGroup, Set.of(new TopicPartition(topic, 0)));
             AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(clusterInstance, groupProtocol, protocolGroup, topic)
        ) {
            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list"),
                    List.of(),
                    Set.of(
                            List.of(protocolGroup),
                            List.of(topicPartitionsGroup)
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state"),
                    List.of("GROUP", "STATE"),
                    Set.of(
                            List.of(protocolGroup, "Stable"),
                            List.of(topicPartitionsGroup, "Empty")
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type"),
                    List.of("GROUP", "TYPE"),
                    Set.of(
                            List.of(protocolGroup, "Classic"),
                            List.of(topicPartitionsGroup, "Classic")
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "--state"),
                    List.of("GROUP", "TYPE", "STATE"),
                    Set.of(
                            List.of(protocolGroup, "Classic", "Stable"),
                            List.of(topicPartitionsGroup, "Classic", "Empty")
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state", "Stable"),
                    List.of("GROUP", "STATE"),
                    Set.of(
                            List.of(protocolGroup, "Stable")
                    )
            );

            // Check case-insensitivity in state filter.
            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state", "stable"),
                    List.of("GROUP", "STATE"),
                    Set.of(
                            List.of(protocolGroup, "Stable")
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "Classic"),
                    List.of("GROUP", "TYPE"),
                    Set.of(
                            List.of(protocolGroup, "Classic"),
                            List.of(topicPartitionsGroup, "Classic")
                    )
            );

            // Check case-insensitivity in type filter.
            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "classic"),
                    List.of("GROUP", "TYPE"),
                    Set.of(
                            List.of(protocolGroup, "Classic"),
                            List.of(topicPartitionsGroup, "Classic")
                    )
            );
        }
    }

    @ClusterTest(
        types = {Type.CO_KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "1000"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
            @ClusterConfigProperty(key = CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500")
        }
    )
    public void testListGroupCommandConsumerProtocol(ClusterInstance clusterInstance) throws Exception {
        GroupProtocol groupProtocol = GroupProtocol.CONSUMER;
        String topic = TOPIC_PREFIX + groupProtocol.name;
        String protocolGroup = PROTOCOL_GROUP_PREFIX + groupProtocol.name;
        String topicPartitionsGroup = TOPIC_PARTITIONS_GROUP_PREFIX + "0";
        clusterInstance.createTopic(topic, 1, (short) 1);

        try (AutoCloseable topicPartitionsConsumerGroupExecutor = consumerGroupClosable(clusterInstance, topicPartitionsGroup, Set.of(new TopicPartition(topic, 0)));
             AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(clusterInstance, groupProtocol, protocolGroup, topic)
        ) {
            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list"),
                    List.of(),
                    Set.of(
                            List.of(protocolGroup),
                            List.of(topicPartitionsGroup)
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state"),
                    List.of("GROUP", "STATE"),
                    Set.of(
                            List.of(protocolGroup, "Stable"),
                            List.of(topicPartitionsGroup, "Empty")
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type"),
                    List.of("GROUP", "TYPE"),
                    Set.of(
                            List.of(protocolGroup, "Consumer"),
                            List.of(topicPartitionsGroup, "Classic")
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "--state"),
                    List.of("GROUP", "TYPE", "STATE"),
                    Set.of(
                            List.of(protocolGroup, "Consumer", "Stable"),
                            List.of(topicPartitionsGroup, "Classic", "Empty")
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "consumer"),
                    List.of("GROUP", "TYPE"),
                    Set.of(
                            List.of(protocolGroup, "Consumer")
                    )
            );

            validateListOutput(
                    List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "consumer", "--state", "Stable"),
                    List.of("GROUP", "TYPE", "STATE"),
                    Set.of(
                            List.of(protocolGroup, "Consumer", "Stable")
                    )
            );
        }
    }

    private static AutoCloseable consumerGroupClosable(ClusterInstance clusterInstance, GroupProtocol protocol, String groupId, String topicName) {
        Map<String, Object> configs = composeConfigs(clusterInstance, groupId, protocol.name);

        return ConsumerGroupCommandTestUtils.buildConsumers(
                1,
                false,
                topicName,
                () -> new KafkaConsumer<String, String>(configs)
        );
    }

    private static AutoCloseable consumerGroupClosable(ClusterInstance clusterInstance, String groupId, Set<TopicPartition> topicPartitions) {
        Map<String, Object> configs = composeConfigs(clusterInstance, groupId, GroupProtocol.CLASSIC.name);

        return ConsumerGroupCommandTestUtils.buildConsumers(
                1,
                topicPartitions,
                () -> new KafkaConsumer<String, String>(configs)
        );
    }

    private static Map<String, Object> composeConfigs(ClusterInstance clusterInstance, String groupId, String groupProtocol) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        configs.put(GROUP_ID_CONFIG, groupId);
        configs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(GROUP_PROTOCOL_CONFIG, groupProtocol);
        if (GroupProtocol.CLASSIC.name.equalsIgnoreCase(groupProtocol)) {
            configs.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        }
        return configs;
    }

    private static ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);
        return new ConsumerGroupCommand.ConsumerGroupService(
                opts,
                Map.of(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private static void removeConsumer(ClusterInstance clusterInstance, Set<String> groupIds) {
        try (Admin admin = clusterInstance.admin()) {
            assertDoesNotThrow(() -> admin.deleteConsumerGroups(groupIds).all().get());
        }
    }

    private static void deleteTopic(ClusterInstance clusterInstance, String topic) {
        try (Admin admin = clusterInstance.admin()) {
            assertDoesNotThrow(() -> admin.deleteTopics(Set.of(topic)).all().get());
        }
    }

    /**
     * Validates the consumer group listings returned against expected values using specified filters.
     *
     * @param service                The service to list consumer groups.
     * @param typeFilterSet          Filters for group types, empty for no filter.
     * @param groupStateFilterSet    Filters for group states, empty for no filter.
     * @param expectedListing        Expected consumer group listings.
     */
    private static void assertGroupListing(
        ConsumerGroupCommand.ConsumerGroupService service,
        Set<GroupType> typeFilterSet,
        Set<GroupState> groupStateFilterSet,
        Set<GroupListing> expectedListing
    ) throws Exception {
        final AtomicReference<Set<GroupListing>> foundListing = new AtomicReference<>(Set.of());
        TestUtils.waitForCondition(() -> {
            foundListing.set(new HashSet<>(service.listConsumerGroupsWithFilters(typeFilterSet, groupStateFilterSet)));
            return Objects.equals(expectedListing, foundListing.get());
        }, () -> "Expected to show groups " + expectedListing + ", but found " + foundListing.get() + ".");
    }

    /**
     * Validates that the output of the list command corresponds to the expected values.
     *
     * @param args              The arguments for the command line tool.
     * @param expectedHeader    The expected header as a list of strings; or an empty list
     *                          if a header is not expected.
     * @param expectedRows      The expected rows as a set of list of columns.
     * @throws InterruptedException
     */
    private static void validateListOutput(
        List<String> args,
        List<String> expectedHeader,
        Set<List<String>> expectedRows
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> ConsumerGroupCommand.main(args.toArray(new String[0])));
            out.set(output);

            int index = 0;
            String[] lines = output.split("\n");

            // Parse the header if one is expected.
            if (!expectedHeader.isEmpty()) {
                if (lines.length == 0) return false;
                List<String> header = Arrays.stream(lines[index++].split("\\s+")).toList();
                if (!expectedHeader.equals(header)) {
                    return false;
                }
            }

            // Parse the groups.
            Set<List<String>> groups = new HashSet<>();
            for (; index < lines.length; index++) {
                groups.add(Arrays.stream(lines[index].split("\\s+")).toList());
            }
            return expectedRows.equals(groups);
        }, () -> String.format("Expected header=%s and groups=%s, but found:%n%s", expectedHeader, expectedRows, out.get()));
    }

    @Test
    public void testConsumerGroupStatesFromString() {
        Set<GroupState> result = ConsumerGroupCommand.groupStatesFromString("Stable");
        assertEquals(Set.of(GroupState.STABLE), result);

        result = ConsumerGroupCommand.groupStatesFromString("Stable, PreparingRebalance");
        assertEquals(Set.of(GroupState.STABLE, GroupState.PREPARING_REBALANCE), result);

        result = ConsumerGroupCommand.groupStatesFromString("Dead,CompletingRebalance,");
        assertEquals(Set.of(GroupState.DEAD, GroupState.COMPLETING_REBALANCE), result);

        result = ConsumerGroupCommand.groupStatesFromString("stable");
        assertEquals(Set.of(GroupState.STABLE), result);

        result = ConsumerGroupCommand.groupStatesFromString("stable, assigning");
        assertEquals(Set.of(GroupState.STABLE, GroupState.ASSIGNING), result);

        result = ConsumerGroupCommand.groupStatesFromString("dead,reconciling,");
        assertEquals(Set.of(GroupState.DEAD, GroupState.RECONCILING), result);

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.groupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.groupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.groupStatesFromString("   ,   ,"));
    }

    @Test
    public void testConsumerGroupTypesFromString() {
        Set<GroupType> result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer");
        assertEquals(Set.of(GroupType.CONSUMER), result);

        result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer, classic");
        assertEquals(Set.of(GroupType.CONSUMER, GroupType.CLASSIC), result);

        result = ConsumerGroupCommand.consumerGroupTypesFromString("Consumer, Classic");
        assertEquals(Set.of(GroupType.CONSUMER, GroupType.CLASSIC), result);

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("Share"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("streams"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("  bad, generic"));

        assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("   ,   ,"));
    }
}
