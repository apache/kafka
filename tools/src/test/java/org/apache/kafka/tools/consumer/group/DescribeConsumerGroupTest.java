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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
public class DescribeConsumerGroupTest {
    private static final String TOPIC_PREFIX = "test.topic.";
    private static final String GROUP_PREFIX = "test.group.";
    private static final List<List<String>> DESCRIBE_TYPE_OFFSETS = Arrays.asList(Collections.singletonList(""), Collections.singletonList("--offsets"));
    private static final List<List<String>> DESCRIBE_TYPE_MEMBERS = Arrays.asList(Collections.singletonList("--members"), Arrays.asList("--members", "--verbose"));
    private static final List<List<String>> DESCRIBE_TYPE_STATE = Collections.singletonList(Collections.singletonList("--state"));
    private static final List<List<String>> DESCRIBE_TYPES = Stream.of(DESCRIBE_TYPE_OFFSETS, DESCRIBE_TYPE_MEMBERS, DESCRIBE_TYPE_STATE).flatMap(Collection::stream).collect(Collectors.toList());
    private ClusterInstance clusterInstance;

    private static List<ClusterConfig> generator() {
        return ConsumerGroupCommandTestUtils.generator();
    }

    @ClusterTemplate("generator")
    public void testDescribeNonExistingGroup(ClusterInstance clusterInstance) {
        String missingGroup = "missing.group";

        for (List<String> describeType : DESCRIBE_TYPES) {
            // note the group to be queried is a different (non-existing) group
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", missingGroup));
            cgcArgs.addAll(describeType);
            try (ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(cgcArgs.toArray(new String[0]))) {
                String output = ToolsTestUtils.grabConsoleOutput(describeGroups(service));
                assertTrue(output.contains("Consumer group '" + missingGroup + "' does not exist."),
                        "Expected error was not detected for describe option '" + String.join(" ", describeType) + "'");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeOffsetsOfNonExistingGroup(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        String missingGroup = "missing.group";
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 // note the group to be queried is a different (non-existing) group
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", missingGroup})
            ) {
                Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(missingGroup);
                assertTrue(res.getKey().map(s -> s.equals(GroupState.DEAD)).orElse(false) && res.getValue().map(Collection::isEmpty).orElse(false),
                        "Expected the state to be 'Dead', with no members in the group '" + missingGroup + "'.");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeMembersOfNonExistingGroup(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        String missingGroup = "missing.group";
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 // note the group to be queried is a different (non-existing) group
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", missingGroup})
            ) {
                Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(missingGroup, false);
                assertTrue(res.getKey().map(s -> s.equals(GroupState.DEAD)).orElse(false) && res.getValue().map(Collection::isEmpty).orElse(false),
                        "Expected the state to be 'Dead', with no members in the group '" + missingGroup + "'.");

                Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> res2 = service.collectGroupMembers(missingGroup, true);
                assertTrue(res2.getKey().map(s -> s.equals(GroupState.DEAD)).orElse(false) && res2.getValue().map(Collection::isEmpty).orElse(false),
                        "Expected the state to be 'Dead', with no members in the group '" + missingGroup + "' (verbose option).");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeStateOfNonExistingGroup(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        String missingGroup = "missing.group";
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 // note the group to be queried is a different (non-existing) group
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", missingGroup})
            ) {
                GroupInformation state = service.collectGroupState(missingGroup);
                assertTrue(Objects.equals(state.groupState, GroupState.DEAD) && state.numMembers == 0 &&
                                state.coordinator != null && clusterInstance.brokerIds().contains(state.coordinator.id()),
                        "Expected the state to be 'Dead', with no members in the group '" + missingGroup + "'."
                );
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeExistingGroup(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            createTopic(topic);
            for (List<String> describeType : DESCRIBE_TYPES) {
                String protocolGroup = GROUP_PREFIX + groupProtocol.name() + "." + String.join("", describeType);
                List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", protocolGroup));
                cgcArgs.addAll(describeType);
                try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, protocolGroup, topic, Collections.emptyMap());
                     ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(cgcArgs.toArray(new String[0]))
                ) {
                    TestUtils.waitForCondition(() -> {
                        Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                        return res.getKey().trim().split("\n").length == 2 && res.getValue().isEmpty() && checkArgsOutput(cgcArgs, res.getKey().trim().split("\n")[0]);
                    }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
                }
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeExistingGroups(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            createTopic(topic);

            // Create N single-threaded consumer groups from a single-partition topic
            List<AutoCloseable> protocolConsumerGroupExecutors = new ArrayList<>();
            try {
                List<String> groups = new ArrayList<>();
                for (List<String> describeType : DESCRIBE_TYPES) {
                    String group = GROUP_PREFIX + groupProtocol.name() + "." + String.join("", describeType);
                    groups.addAll(Arrays.asList("--group", group));
                    protocolConsumerGroupExecutors.add(consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap()));
                }

                int expectedNumLines = DESCRIBE_TYPES.size() * 2;

                for (List<String> describeType : DESCRIBE_TYPES) {
                    List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--describe"));
                    cgcArgs.addAll(groups);
                    cgcArgs.addAll(describeType);
                    try (ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(cgcArgs.toArray(new String[0]))) {
                        TestUtils.waitForCondition(() -> {
                            Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                            long numLines = Arrays.stream(res.getKey().trim().split("\n")).filter(line -> !line.isEmpty()).count();
                            return (numLines == expectedNumLines) && res.getValue().isEmpty() && checkArgsOutput(cgcArgs, res.getKey().trim().split("\n")[0]);
                        }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
                    }
                }
            } finally {
                for (AutoCloseable protocolConsumerGroupExecutor : protocolConsumerGroupExecutors) {
                    protocolConsumerGroupExecutor.close();
                }
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeAllExistingGroups(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            createTopic(topic);

            // Create N single-threaded consumer groups from a single-partition topic
            List<AutoCloseable> protocolConsumerGroupExecutors = new ArrayList<>();
            List<String> groups = new ArrayList<>();
            try {
                for (List<String> describeType : DESCRIBE_TYPES) {
                    String group = GROUP_PREFIX + groupProtocol.name() + "." + String.join("", describeType);
                    groups.add(group);
                    protocolConsumerGroupExecutors.add(consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap()));
                }
                int expectedNumLines = DESCRIBE_TYPES.size() * 2;
                for (List<String> describeType : DESCRIBE_TYPES) {
                    List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--all-groups"));
                    cgcArgs.addAll(describeType);
                    try (ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(cgcArgs.toArray(new String[0]))) {
                        TestUtils.waitForCondition(() -> {
                            Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                            long numLines = Arrays.stream(res.getKey().trim().split("\n")).filter(line -> !line.isEmpty()).count();
                            return (numLines == expectedNumLines) && res.getValue().isEmpty() && checkArgsOutput(cgcArgs, res.getKey().trim().split("\n")[0]);
                        }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
                    }
                }
            } finally {
                for (AutoCloseable protocolConsumerGroupExecutor : protocolConsumerGroupExecutors) {
                    protocolConsumerGroupExecutor.close();
                }
                // remove previous consumer groups, so we can have a clean cluster for next consumer group protocol test.
                deleteConsumerGroups(groups);
                deleteTopic(topic);
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeOffsetsOfExistingGroup(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> groupOffsets = service.collectGroupOffsets(group);
                    Optional<GroupState> state = groupOffsets.getKey();
                    Optional<Collection<PartitionAssignmentState>> assignments = groupOffsets.getValue();

                    Predicate<PartitionAssignmentState> isGrp = s -> Objects.equals(s.group, group);

                    boolean res = state.map(s -> s.equals(GroupState.STABLE)).orElse(false) &&
                            assignments.isPresent() &&
                            assignments.get().stream().filter(isGrp).count() == 1;

                    if (!res)
                        return false;

                    Optional<PartitionAssignmentState> maybePartitionState = assignments.get().stream().filter(isGrp).findFirst();
                    if (!maybePartitionState.isPresent())
                        return false;

                    PartitionAssignmentState partitionState = maybePartitionState.get();

                    return !partitionState.consumerId.map(s0 -> s0.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                            !partitionState.clientId.map(s0 -> s0.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                            !partitionState.host.map(h -> h.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false);
                }, "Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for group " + group + ".");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeMembersOfExistingGroup(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group});
                 Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))
            ) {
                TestUtils.waitForCondition(() -> {
                    ConsumerGroupDescription consumerGroupDescription = admin.describeConsumerGroups(Collections.singleton(group)).describedGroups().get(group).get();
                    return consumerGroupDescription.members().size() == 1 && consumerGroupDescription.members().iterator().next().assignment().topicPartitions().size() == 1;
                }, "Expected a 'Stable' group status, rows and valid member information for group " + group + ".");

                Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(group, true);

                assertTrue(res.getValue().isPresent());
                assertTrue(res.getValue().get().size() == 1 && res.getValue().get().iterator().next().assignment.size() == 1,
                        "Expected a topic partition assigned to the single group member for group " + group);
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeStateOfExistingGroup(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.singletonMap(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, groupProtocol == GroupProtocol.CONSUMER ? "range" : ""));
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    GroupInformation state = service.collectGroupState(group);
                    return Objects.equals(state.groupState, GroupState.STABLE) &&
                            state.numMembers == 1 &&
                            state.coordinator != null &&
                            clusterInstance.brokerIds().contains(state.coordinator.id());
                }, "Expected a 'Stable' group status, with one member for group " + group + ".");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeStateOfExistingGroupWithNonDefaultAssignor(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run one consumer in the group consuming from a single-partition topic
            AutoCloseable protocolConsumerGroupExecutor = null;
            try {
                String expectedName;
                if (groupProtocol.equals(GroupProtocol.CONSUMER)) {
                    protocolConsumerGroupExecutor = consumerGroupClosable(GroupProtocol.CONSUMER, group, topic, Collections.singletonMap(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "range"));
                    expectedName = RangeAssignor.RANGE_ASSIGNOR_NAME;
                } else {
                    protocolConsumerGroupExecutor = consumerGroupClosable(GroupProtocol.CLASSIC, group, topic, Collections.singletonMap(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName()));
                    expectedName = RoundRobinAssignor.ROUNDROBIN_ASSIGNOR_NAME;
                }

                try (ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})) {
                    TestUtils.waitForCondition(() -> {
                        GroupInformation state = service.collectGroupState(group);
                        return Objects.equals(state.groupState, GroupState.STABLE) &&
                                state.numMembers == 1 &&
                                Objects.equals(state.assignmentStrategy, expectedName) &&
                                state.coordinator != null &&
                                clusterInstance.brokerIds().contains(state.coordinator.id());
                    }, "Expected a 'Stable' group status, with one member and " + expectedName + " assignment strategy for group " + group + ".");
                }
            } finally {
                if (protocolConsumerGroupExecutor != null) {
                    protocolConsumerGroupExecutor.close();
                }
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeExistingGroupWithNoMembers(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            createTopic(topic);

            for (List<String> describeType : DESCRIBE_TYPES) {
                String group = GROUP_PREFIX + groupProtocol.name() + String.join("", describeType);
                List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group));
                cgcArgs.addAll(describeType);
                // run one consumer in the group consuming from a single-partition topic
                try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                     ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(cgcArgs.toArray(new String[0]))
                ) {
                    TestUtils.waitForCondition(() -> {
                        Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                        return res.getKey().trim().split("\n").length == 2 && res.getValue().isEmpty() && checkArgsOutput(cgcArgs, res.getKey().trim().split("\n")[0]);
                    }, "Expected describe group results with one data row for describe type '" + String.join(" ", describeType) + "'");
                    
                    protocolConsumerGroupExecutor.close();
                    TestUtils.waitForCondition(
                            () -> ToolsTestUtils.grabConsoleError(describeGroups(service)).contains("Consumer group '" + group + "' has no active members."),
                            "Expected no active member in describe group results with describe type " + String.join(" ", describeType));
                }
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeOffsetsOfExistingGroupWithNoMembers(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(group);
                    return res.getKey().map(s -> s.equals(GroupState.STABLE)).orElse(false)
                            && res.getValue().map(c -> c.stream().anyMatch(assignment -> Objects.equals(assignment.group, group) && assignment.offset.isPresent())).orElse(false);
                }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.");

                // stop the consumer so the group has no active member anymore
                protocolConsumerGroupExecutor.close();

                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> offsets = service.collectGroupOffsets(group);
                    Optional<GroupState> state = offsets.getKey();
                    Optional<Collection<PartitionAssignmentState>> assignments = offsets.getValue();
                    List<PartitionAssignmentState> testGroupAssignments = assignments.get().stream().filter(a -> Objects.equals(a.group, group)).collect(Collectors.toList());
                    PartitionAssignmentState assignment = testGroupAssignments.get(0);
                    return state.map(s -> s.equals(GroupState.EMPTY)).orElse(false) &&
                            testGroupAssignments.size() == 1 &&
                            assignment.consumerId.map(c -> c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) && // the member should be gone
                            assignment.clientId.map(c -> c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                            assignment.host.map(c -> c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false);
                }, "failed to collect group offsets");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeMembersOfExistingGroupWithNoMembers(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(group, false);
                    return res.getKey().map(s -> s.equals(GroupState.STABLE)).orElse(false)
                            && res.getValue().map(c -> c.stream().anyMatch(m -> Objects.equals(m.group, group))).orElse(false);
                }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.");

                // stop the consumer so the group has no active member anymore
                protocolConsumerGroupExecutor.close();

                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(group, false);
                    return res.getKey().map(s -> s.equals(GroupState.EMPTY)).orElse(false) && res.getValue().isPresent() && res.getValue().get().isEmpty();
                }, "Expected no member in describe group members results for group '" + group + "'");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeStateOfExistingGroupWithNoMembers(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    GroupInformation state = service.collectGroupState(group);
                    return Objects.equals(state.groupState, GroupState.STABLE) &&
                            state.numMembers == 1 &&
                            state.coordinator != null &&
                            clusterInstance.brokerIds().contains(state.coordinator.id());
                }, "Expected the group to initially become stable, and have a single member.");

                // stop the consumer so the group has no active member anymore
                protocolConsumerGroupExecutor.close();

                TestUtils.waitForCondition(() -> {
                    GroupInformation state = service.collectGroupState(group);
                    return Objects.equals(state.groupState, GroupState.EMPTY) && state.numMembers == 0;
                }, "Expected the group to become empty after the only member leaving.");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeWithConsumersWithoutAssignedPartitions(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            createTopic(topic);

            for (List<String> describeType : DESCRIBE_TYPES) {
                String group = GROUP_PREFIX + groupProtocol.name() + String.join("", describeType);
                List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group));
                cgcArgs.addAll(describeType);
                // run two consumers in the group consuming from a single-partition topic
                try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap(), 2);
                     ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(cgcArgs.toArray(new String[0]))
                ) {
                    TestUtils.waitForCondition(() -> {
                        Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                        int expectedNumRows = DESCRIBE_TYPE_MEMBERS.contains(describeType) ? 3 : 2;
                        return res.getValue().isEmpty() && res.getKey().trim().split("\n").length == expectedNumRows && checkArgsOutput(cgcArgs, res.getKey().trim().split("\n")[0]);
                    }, "Expected a single data row in describe group result with describe type '" + String.join(" ", describeType) + "'");
                }
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeOffsetsWithConsumersWithoutAssignedPartitions(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run two consumers in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap(), 2);
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(group);
                    return res.getKey().map(s -> s.equals(GroupState.STABLE)).isPresent() &&
                            res.getValue().isPresent() &&
                            res.getValue().get().stream().filter(s -> Objects.equals(s.group, group)).count() == 1 &&
                            res.getValue().get().stream().filter(x -> Objects.equals(x.group, group) && x.partition.isPresent()).count() == 1;
                }, "Expected rows for consumers with no assigned partitions in describe group results");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeMembersWithConsumersWithoutAssignedPartitions(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run two consumers in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap(), 2);
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(group, false);
                    return res.getKey().map(s -> s.equals(GroupState.STABLE)).orElse(false) &&
                            res.getValue().isPresent() &&
                            res.getValue().get().stream().filter(s -> Objects.equals(s.group, group)).count() == 2 &&
                            res.getValue().get().stream().filter(x -> Objects.equals(x.group, group) && x.numPartitions == 1).count() == 1 &&
                            res.getValue().get().stream().filter(x -> Objects.equals(x.group, group) && x.numPartitions == 0).count() == 1 &&
                            res.getValue().get().stream().allMatch(s -> s.assignment.isEmpty());
                }, "Expected rows for consumers with no assigned partitions in describe group results");

                Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(group, true);
                assertTrue(res.getKey().map(s -> s.equals(GroupState.STABLE)).orElse(false)
                                && res.getValue().map(c -> c.stream().anyMatch(s -> !s.assignment.isEmpty())).orElse(false),
                        "Expected additional columns in verbose version of describe members");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeStateWithConsumersWithoutAssignedPartitions(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run two consumers in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap(), 2);
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    GroupInformation state = service.collectGroupState(group);
                    return Objects.equals(state.groupState, GroupState.STABLE) && state.numMembers == 2;
                }, "Expected two consumers in describe group results");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeWithMultiPartitionTopicAndMultipleConsumers(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            createTopic(topic, 2);

            for (List<String> describeType : DESCRIBE_TYPES) {
                String group = GROUP_PREFIX + groupProtocol.name() + String.join("", describeType);
                List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group));
                cgcArgs.addAll(describeType);
                // run two consumers in the group consuming from a two-partition topic
                try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap(), 2);
                     ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(cgcArgs.toArray(new String[0]))
                ) {
                    TestUtils.waitForCondition(() -> {
                        Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                        int expectedNumRows = DESCRIBE_TYPE_STATE.contains(describeType) ? 2 : 3;
                        return res.getValue().isEmpty() && res.getKey().trim().split("\n").length == expectedNumRows && checkArgsOutput(cgcArgs, res.getKey().trim().split("\n")[0]);
                    }, "Expected a single data row in describe group result with describe type '" + String.join(" ", describeType) + "'");
                }
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeOffsetsWithMultiPartitionTopicAndMultipleConsumers(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic, 2);

            // run two consumers in the group consuming from a two-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap(), 2);
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(group);
                    return res.getKey().map(s -> s.equals(GroupState.STABLE)).orElse(false) &&
                            res.getValue().isPresent() &&
                            res.getValue().get().stream().filter(s -> Objects.equals(s.group, group)).count() == 2 &&
                            res.getValue().get().stream().filter(x -> Objects.equals(x.group, group) && x.partition.isPresent()).count() == 2 &&
                            res.getValue().get().stream().noneMatch(x -> Objects.equals(x.group, group) && !x.partition.isPresent());
                }, "Expected two rows (one row per consumer) in describe group results.");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeMembersWithMultiPartitionTopicAndMultipleConsumers(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic, 2);

            // run two consumers in the group consuming from a two-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap(), 2);
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(group, false);
                    return res.getKey().map(s -> s.equals(GroupState.STABLE)).orElse(false) &&
                            res.getValue().isPresent() &&
                            res.getValue().get().stream().filter(s -> Objects.equals(s.group, group)).count() == 2 &&
                            res.getValue().get().stream().filter(x -> Objects.equals(x.group, group) && x.numPartitions == 1).count() == 2 &&
                            res.getValue().get().stream().noneMatch(x -> Objects.equals(x.group, group) && x.numPartitions == 0);
                }, "Expected two rows (one row per consumer) in describe group members results.");

                Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(group, true);
                assertTrue(res.getKey().map(s -> s.equals(GroupState.STABLE)).orElse(false) && res.getValue().map(s -> s.stream().filter(x -> x.assignment.isEmpty()).count()).orElse(0L) == 0,
                        "Expected additional columns in verbose version of describe members");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeStateWithMultiPartitionTopicAndMultipleConsumers(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic, 2);

            // run two consumers in the group consuming from a two-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap(), 2);
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    GroupInformation state = service.collectGroupState(group);
                    return Objects.equals(state.groupState, GroupState.STABLE) && Objects.equals(state.group, group) && state.numMembers == 2;
                }, "Expected a stable group with two members in describe group state result.");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeSimpleConsumerGroup(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        // Ensure that the offsets of consumers which don't use group management are still displayed
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic, 2);

            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(GroupProtocol.CLASSIC, group, new HashSet<>(Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1))), Collections.emptyMap());
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(group);
                    return res.getKey().map(s -> s.equals(GroupState.EMPTY)).orElse(false)
                            && res.getValue().isPresent() && res.getValue().get().stream().filter(s -> Objects.equals(s.group, group)).count() == 2;
                }, "Expected a stable group with two members in describe group state result.");
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeGroupWithShortInitializationTimeout(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            createTopic(topic);

            // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
            // complete before the timeout expires
            List<String> describeType = DESCRIBE_TYPES.get(RANDOM.nextInt(DESCRIBE_TYPES.size()));
            String group = GROUP_PREFIX + groupProtocol.name() + String.join("", describeType);

            // set the group initialization timeout too low for the group to stabilize
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--timeout", "1", "--group", group));
            cgcArgs.addAll(describeType);

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(cgcArgs.toArray(new String[0]))
            ) {
                ExecutionException e = assertThrows(ExecutionException.class, service::describeGroups);
                assertInstanceOf(TimeoutException.class, e.getCause());
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeGroupOffsetsWithShortInitializationTimeout(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
            // complete before the timeout expires

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 // set the group initialization timeout too low for the group to stabilize
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group, "--timeout", "1"})
            ) {
                Throwable e = assertThrows(ExecutionException.class, () -> service.collectGroupOffsets(group));
                assertEquals(TimeoutException.class, e.getCause().getClass());
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeGroupMembersWithShortInitializationTimeout(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
            // complete before the timeout expires

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 // set the group initialization timeout too low for the group to stabilize
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group, "--timeout", "1"})
            ) {
                Throwable e = assertThrows(ExecutionException.class, () -> service.collectGroupMembers(group, false));
                assertEquals(TimeoutException.class, e.getCause().getClass());
                e = assertThrows(ExecutionException.class, () -> service.collectGroupMembers(group, true));
                assertEquals(TimeoutException.class, e.getCause().getClass());
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeGroupStateWithShortInitializationTimeout(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
            // complete before the timeout expires

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.emptyMap());
                 // set the group initialization timeout too low for the group to stabilize
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group, "--timeout", "1"})
            ) {
                Throwable e = assertThrows(ExecutionException.class, () -> service.collectGroupState(group));
                assertEquals(TimeoutException.class, e.getCause().getClass());
            }
        }
    }

    @ClusterTemplate("generator")
    public void testDescribeNonOffsetCommitGroup(ClusterInstance clusterInstance) throws Exception {
        this.clusterInstance = clusterInstance;
        for (GroupProtocol groupProtocol: clusterInstance.supportedGroupProtocols()) {
            String topic = TOPIC_PREFIX + groupProtocol.name();
            String group = GROUP_PREFIX + groupProtocol.name();
            createTopic(topic);

            // run one consumer in the group consuming from a single-partition topic
            try (AutoCloseable protocolConsumerGroupExecutor = consumerGroupClosable(groupProtocol, group, topic, Collections.singletonMap(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"));
                 ConsumerGroupCommand.ConsumerGroupService service = consumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--describe", "--group", group})
            ) {
                TestUtils.waitForCondition(() -> {
                    Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> groupOffsets = service.collectGroupOffsets(group);

                    Predicate<PartitionAssignmentState> isGrp = s -> Objects.equals(s.group, group);

                    boolean res = groupOffsets.getKey().map(s -> s.equals(GroupState.STABLE)).orElse(false) &&
                            groupOffsets.getValue().isPresent() &&
                            groupOffsets.getValue().get().stream().filter(isGrp).count() == 1;

                    if (!res)
                        return false;

                    Optional<PartitionAssignmentState> maybeAssignmentState = groupOffsets.getValue().get().stream().filter(isGrp).findFirst();
                    if (!maybeAssignmentState.isPresent())
                        return false;

                    PartitionAssignmentState assignmentState = maybeAssignmentState.get();

                    return assignmentState.consumerId.map(c -> !c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                            assignmentState.clientId.map(c -> !c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                            assignmentState.host.map(h -> !h.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false);
                }, "Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for non-offset-committing group " + group + ".");
            }
        }
    }

    @Test
    public void testDescribeWithUnrecognizedNewConsumerOption() {
        String group = GROUP_PREFIX +  "unrecognized";
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", "localhost:9092", "--describe", "--group", group};
        assertThrows(joptsimple.OptionException.class, () -> ConsumerGroupCommandOptions.fromArgs(cgcArgs));
    }

    @Test
    public void testDescribeWithMultipleSubActions() {
        String group = GROUP_PREFIX + "multiple.sub.actions";
        AtomicInteger exitStatus = new AtomicInteger(0);
        AtomicReference<String> exitMessage = new AtomicReference<>("");
        Exit.setExitProcedure((status, err) -> {
            exitStatus.set(status);
            exitMessage.set(err);
            throw new RuntimeException();
        });
        String[] cgcArgs = new String[]{"--bootstrap-server", "localhost:9092", "--describe", "--group", group, "--members", "--state"};
        try {
            assertThrows(RuntimeException.class, () -> ConsumerGroupCommand.main(cgcArgs));
        } finally {
            Exit.resetExitProcedure();
        }
        assertEquals(1, exitStatus.get());
        assertTrue(exitMessage.get().contains("Option [describe] takes at most one of these options"));
    }

    @Test
    public void testDescribeWithStateValue() {
        AtomicInteger exitStatus = new AtomicInteger(0);
        AtomicReference<String> exitMessage = new AtomicReference<>("");
        Exit.setExitProcedure((status, err) -> {
            exitStatus.set(status);
            exitMessage.set(err);
            throw new RuntimeException();
        });
        String[] cgcArgs = new String[]{"--bootstrap-server", "localhost:9092", "--describe", "--all-groups", "--state", "Stable"};
        try {
            assertThrows(RuntimeException.class, () -> ConsumerGroupCommand.main(cgcArgs));
        } finally {
            Exit.resetExitProcedure();
        }
        assertEquals(1, exitStatus.get());
        assertTrue(exitMessage.get().contains("Option [describe] does not take a value for [state]"));
    }

    @Test
    public void testPrintVersion() {
        ToolsTestUtils.MockExitProcedure exitProcedure = new ToolsTestUtils.MockExitProcedure();
        Exit.setExitProcedure(exitProcedure);
        try {
            String out = ToolsTestUtils.captureStandardOut(() -> ConsumerGroupCommandOptions.fromArgs(new String[]{"--version"}));
            assertEquals(0, exitProcedure.statusCode());
            assertEquals(AppInfoParser.getVersion(), out);
        } finally {
            Exit.resetExitProcedure();
        }
    }

    private static ConsumerGroupCommand.ConsumerGroupService consumerGroupService(String[] args) {
        return new ConsumerGroupCommand.ConsumerGroupService(
                ConsumerGroupCommandOptions.fromArgs(args),
                Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private void createTopic(String topic) {
        createTopic(topic, 1);
    }

    private void createTopic(String topic, int numPartitions) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            Assertions.assertDoesNotThrow(() -> admin.createTopics(Collections.singletonList(new NewTopic(topic, numPartitions, (short) 1))).topicId(topic).get());
        }
    }

    private void deleteConsumerGroups(Collection<String> groupIds) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            Assertions.assertDoesNotThrow(() -> admin.deleteConsumerGroups(groupIds).all().get());
        }
    }

    private void deleteTopic(String topic) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            Assertions.assertDoesNotThrow(() -> admin.deleteTopics(Collections.singletonList(topic)).topicNameValues().get(topic).get());
        }
    }

    private AutoCloseable consumerGroupClosable(GroupProtocol protocol, String groupId, Set<TopicPartition> topicPartitions, Map<String, Object> customConfigs) {
        Map<String, Object> configs = composeConfigs(
                groupId,
                protocol.name,
                customConfigs
        );
        return ConsumerGroupCommandTestUtils.buildConsumers(
                1,
                topicPartitions,
                () -> new KafkaConsumer<String, String>(configs)
        );
    }

    private AutoCloseable consumerGroupClosable(GroupProtocol protocol, String groupId, String topicName, Map<String, Object> customConfigs) {
        return consumerGroupClosable(protocol, groupId, topicName, customConfigs, 1);
    }

    private AutoCloseable consumerGroupClosable(GroupProtocol protocol, String groupId, String topicName, Map<String, Object> customConfigs, int numConsumers) {
        Map<String, Object> configs = composeConfigs(
                groupId,
                protocol.name,
                customConfigs
        );
        return ConsumerGroupCommandTestUtils.buildConsumers(
                numConsumers,
                false,
                topicName,
                () -> new KafkaConsumer<String, String>(configs)
        );
    }

    private Map<String, Object> composeConfigs(String groupId, String groupProtocol, Map<String, Object> customConfigs) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);

        configs.putAll(customConfigs);
        return configs;
    }

    private Runnable describeGroups(ConsumerGroupCommand.ConsumerGroupService service) {
        return () -> Assertions.assertDoesNotThrow(service::describeGroups);
    }

    private boolean checkArgsOutput(List<String> args, String output) {
        if (!output.contains("GROUP")) {
            return false;
        }

        if (args.contains("--members")) {
            return checkMembersArgsOutput(output, args.contains("--verbose"));
        }

        if (args.contains("--state")) {
            return checkStateArgsOutput(output);
        }

        // --offsets or no arguments
        AtomicBoolean result = new AtomicBoolean(true);
        List.of("TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID").forEach(key -> {
            if (!output.contains(key)) {
                result.set(false);
            }
        });
        return result.get();
    }

    private boolean checkMembersArgsOutput(String output, boolean verbose) {
        AtomicBoolean result = new AtomicBoolean(true);
        List<String> expectedKeys = verbose ?
            List.of("CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS", "ASSIGNMENT") :
            List.of("CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS");
        expectedKeys.forEach(key -> {
            if (!output.contains(key)) {
                result.set(false);
            }
        });
        return result.get();
    }

    private boolean checkStateArgsOutput(String output) {
        return output.contains("COORDINATOR (ID)") && output.contains("ASSIGNMENT-STRATEGY") && output.contains("STATE") && output.contains("#MEMBERS");
    }
}
