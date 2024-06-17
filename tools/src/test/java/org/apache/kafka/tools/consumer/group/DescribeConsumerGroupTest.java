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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.RANDOM;
import static org.apache.kafka.tools.ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class DescribeConsumerGroupTest extends ConsumerGroupCommandTest {
    private static final List<List<String>> DESCRIBE_TYPE_OFFSETS = Arrays.asList(Collections.singletonList(""), Collections.singletonList("--offsets"));
    private static final List<List<String>> DESCRIBE_TYPE_MEMBERS = Arrays.asList(Collections.singletonList("--members"), Arrays.asList("--members", "--verbose"));
    private static final List<List<String>> DESCRIBE_TYPE_STATE = Collections.singletonList(Collections.singletonList("--state"));
    private static final List<List<String>> DESCRIBE_TYPES;

    static {
        List<List<String>> describeTypes = new ArrayList<>();

        describeTypes.addAll(DESCRIBE_TYPE_OFFSETS);
        describeTypes.addAll(DESCRIBE_TYPE_MEMBERS);
        describeTypes.addAll(DESCRIBE_TYPE_STATE);

        DESCRIBE_TYPES = describeTypes;
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeNonExistingGroup(String quorum, String groupProtocol) {
        createOffsetsTopic(listenerName(), new Properties());
        String missingGroup = "missing.group";

        for (List<String> describeType : DESCRIBE_TYPES) {
            // note the group to be queried is a different (non-existing) group
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", missingGroup));
            cgcArgs.addAll(describeType);
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            String output = ToolsTestUtils.grabConsoleOutput(describeGroups(service));
            assertTrue(output.contains("Consumer group '" + missingGroup + "' does not exist."),
                "Expected error was not detected for describe option '" + String.join(" ", describeType) + "'");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeWithMultipleSubActions(String quorum) {
        AtomicInteger exitStatus = new AtomicInteger(0);
        AtomicReference<String> exitMessage = new AtomicReference<>("");
        Exit.setExitProcedure((status, err) -> {
            exitStatus.set(status);
            exitMessage.set(err);
            throw new RuntimeException();
        });
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP, "--members", "--state"};
        try {
            assertThrows(RuntimeException.class, () -> ConsumerGroupCommand.main(cgcArgs));
        } finally {
            Exit.resetExitProcedure();
        }
        assertEquals(1, exitStatus.get());
        assertTrue(exitMessage.get().contains("Option [describe] takes at most one of these options"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeWithStateValue(String quorum) {
        AtomicInteger exitStatus = new AtomicInteger(0);
        AtomicReference<String> exitMessage = new AtomicReference<>("");
        Exit.setExitProcedure((status, err) -> {
            exitStatus.set(status);
            exitMessage.set(err);
            throw new RuntimeException();
        });
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--all-groups", "--state", "Stable"};
        try {
            assertThrows(RuntimeException.class, () -> ConsumerGroupCommand.main(cgcArgs));
        } finally {
            Exit.resetExitProcedure();
        }
        assertEquals(1, exitStatus.get());
        assertTrue(exitMessage.get().contains("Option [describe] does not take a value for [state]"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testPrintVersion(String quorum) {
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

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeOffsetsOfNonExistingGroup(String quorum, String groupProtocol) throws Exception {
        String group = "missing.group";
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, groupProtocol);
        // note the group to be queried is a different (non-existing) group
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(group);
        assertTrue(res.getKey().map(s -> s.equals(ConsumerGroupState.DEAD)).orElse(false) && res.getValue().map(Collection::isEmpty).orElse(false),
            "Expected the state to be 'Dead', with no members in the group '" + group + "'.");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeMembersOfNonExistingGroup(String quorum, String groupProtocol) throws Exception {
        String group = "missing.group";
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, groupProtocol);
        // note the group to be queried is a different (non-existing) group
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(group, false);
        assertTrue(res.getKey().map(s -> s.equals(ConsumerGroupState.DEAD)).orElse(false) && res.getValue().map(Collection::isEmpty).orElse(false),
            "Expected the state to be 'Dead', with no members in the group '" + group + "'.");

        Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> res2 = service.collectGroupMembers(group, true);
        assertTrue(res2.getKey().map(s -> s.equals(ConsumerGroupState.DEAD)).orElse(false) && res2.getValue().map(Collection::isEmpty).orElse(false),
            "Expected the state to be 'Dead', with no members in the group '" + group + "' (verbose option).");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeStateOfNonExistingGroup(String quorum, String groupProtocol) throws Exception {
        String group = "missing.group";
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, groupProtocol);
        // note the group to be queried is a different (non-existing) group
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        GroupState state = service.collectGroupState(group);
        assertTrue(Objects.equals(state.state, ConsumerGroupState.DEAD) && state.numMembers == 0 &&
                state.coordinator != null && !brokers().filter(s -> s.config().brokerId() == state.coordinator.id()).isEmpty(),
            "Expected the state to be 'Dead', with no members in the group '" + group + "'."
        );
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeExistingGroup(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        for (List<String> describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            // run one consumer in the group consuming from a single-partition topic
            addConsumerGroupExecutor(1, TOPIC, group, groupProtocol);
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group));
            cgcArgs.addAll(describeType);
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                return res.getKey().trim().split("\n").length == 2 && res.getValue().isEmpty();
            }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeExistingGroups(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // Create N single-threaded consumer groups from a single-partition topic
        List<String> groups = new ArrayList<>();

        for (List<String> describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            addConsumerGroupExecutor(1, TOPIC, group, groupProtocol);
            groups.addAll(Arrays.asList("--group", group));
        }

        int expectedNumLines = DESCRIBE_TYPES.size() * 2;

        for (List<String> describeType : DESCRIBE_TYPES) {
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe"));
            cgcArgs.addAll(groups);
            cgcArgs.addAll(describeType);
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                long numLines = Arrays.stream(res.getKey().trim().split("\n")).filter(line -> !line.isEmpty()).count();
                return (numLines == expectedNumLines) && res.getValue().isEmpty();
            }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeAllExistingGroups(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // Create N single-threaded consumer groups from a single-partition topic
        for (List<String> describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            addConsumerGroupExecutor(1, TOPIC, group, groupProtocol);
        }

        int expectedNumLines = DESCRIBE_TYPES.size() * 2;

        for (List<String> describeType : DESCRIBE_TYPES) {
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--all-groups"));
            cgcArgs.addAll(describeType);
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                long numLines = Arrays.stream(res.getKey().trim().split("\n")).filter(s -> !s.isEmpty()).count();
                return (numLines == expectedNumLines) && res.getValue().isEmpty();
            }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeOffsetsOfExistingGroup(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>> groupOffsets = service.collectGroupOffsets(GROUP);
            Optional<ConsumerGroupState> state = groupOffsets.getKey();
            Optional<Collection<PartitionAssignmentState>> assignments = groupOffsets.getValue();

            Predicate<PartitionAssignmentState> isGrp = s -> Objects.equals(s.group, GROUP);

            boolean res = state.map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false) &&
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
        }, "Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for group " + GROUP + ".");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeMembersOfExistingGroup(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, groupProtocol);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> groupMembers = service.collectGroupMembers(GROUP, false);
            Optional<ConsumerGroupState> state = groupMembers.getKey();
            Optional<Collection<MemberAssignmentState>> assignments = groupMembers.getValue();

            Predicate<MemberAssignmentState> isGrp = s -> Objects.equals(s.group, GROUP);

            boolean res = state.map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false) &&
                assignments.isPresent() &&
                assignments.get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 1;

            if (!res)
                return false;

            Optional<MemberAssignmentState> maybeAssignmentState = assignments.get().stream().filter(isGrp).findFirst();
            if (!maybeAssignmentState.isPresent())
                return false;

            MemberAssignmentState assignmentState = maybeAssignmentState.get();

            return !Objects.equals(assignmentState.consumerId, ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
                !Objects.equals(assignmentState.clientId, ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
                !Objects.equals(assignmentState.host, ConsumerGroupCommand.MISSING_COLUMN_VALUE);
        }, "Expected a 'Stable' group status, rows and valid member information for group " + GROUP + ".");

        Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, true);

        if (res.getValue().isPresent()) {
            assertTrue(res.getValue().get().size() == 1 && res.getValue().get().iterator().next().assignment.size() == 1,
                "Expected a topic partition assigned to the single group member for group " + GROUP);
        } else {
            fail("Expected partition assignments for members of group " + GROUP);
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeStateOfExistingGroup(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(
            1,
            groupProtocol,
            // This is only effective when new protocol is used.
            Optional.of("range")
        );
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, ConsumerGroupState.STABLE) &&
                state.numMembers == 1 &&
                Objects.equals(state.assignmentStrategy, "range") &&
                state.coordinator != null &&
                brokers().count(s -> s.config().brokerId() == state.coordinator.id()) > 0;
        }, "Expected a 'Stable' group status, with one member and round robin assignment strategy for group " + GROUP + ".");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeStateOfExistingGroupWithNonDefaultAssignor(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        String expectedName;
        if (groupProtocol.equals("consumer")) {
            addConsumerGroupExecutor(1, groupProtocol, Optional.of("range"));
            expectedName = "range";
        } else {
            addConsumerGroupExecutor(1, TOPIC, GROUP, RoundRobinAssignor.class.getName(), Optional.empty(), Optional.empty(), false, groupProtocol);
            expectedName = "roundrobin";
        }
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, ConsumerGroupState.STABLE) &&
                state.numMembers == 1 &&
                Objects.equals(state.assignmentStrategy, expectedName) &&
                state.coordinator != null &&
                brokers().count(s -> s.config().brokerId() == state.coordinator.id()) > 0;
        }, "Expected a 'Stable' group status, with one member and " + expectedName + " assignment strategy for group " + GROUP + ".");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeExistingGroupWithNoMembers(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        for (List<String> describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            // run one consumer in the group consuming from a single-partition topic
            ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, group, groupProtocol);
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group));
            cgcArgs.addAll(describeType);
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                return res.getKey().trim().split("\n").length == 2 && res.getValue().isEmpty();
            }, "Expected describe group results with one data row for describe type '" + String.join(" ", describeType) + "'");

            // stop the consumer so the group has no active member anymore
            executor.shutdown();
            TestUtils.waitForCondition(
                () -> ToolsTestUtils.grabConsoleError(describeGroups(service)).contains("Consumer group '" + group + "' has no active members."),
                "Expected no active member in describe group results with describe type " + String.join(" ", describeType));
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeOffsetsOfExistingGroupWithNoMembers(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, GROUP, RangeAssignor.class.getName(), Optional.empty(), Optional.empty(), true, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            return res.getKey().map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false)
                && res.getValue().map(c -> c.stream().anyMatch(assignment -> Objects.equals(assignment.group, GROUP) && assignment.offset.isPresent())).orElse(false);
        }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.");

        // stop the consumer so the group has no active member anymore
        executor.shutdown();

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>> offsets = service.collectGroupOffsets(GROUP);
            Optional<ConsumerGroupState> state = offsets.getKey();
            Optional<Collection<PartitionAssignmentState>> assignments = offsets.getValue();
            List<PartitionAssignmentState> testGroupAssignments = assignments.get().stream().filter(a -> Objects.equals(a.group, GROUP)).collect(Collectors.toList());
            PartitionAssignmentState assignment = testGroupAssignments.get(0);
            return state.map(s -> s.equals(ConsumerGroupState.EMPTY)).orElse(false) &&
                testGroupAssignments.size() == 1 &&
                assignment.consumerId.map(c -> c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) && // the member should be gone
                assignment.clientId.map(c -> c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                assignment.host.map(c -> c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false);
        }, "failed to collect group offsets");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeMembersOfExistingGroupWithNoMembers(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, false);
            return res.getKey().map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false)
                && res.getValue().map(c -> c.stream().anyMatch(m -> Objects.equals(m.group, GROUP))).orElse(false);
        }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.");

        // stop the consumer so the group has no active member anymore
        executor.shutdown();

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, false);
            return res.getKey().map(s -> s.equals(ConsumerGroupState.EMPTY)).orElse(false) && res.getValue().isPresent() && res.getValue().get().isEmpty();
        }, "Expected no member in describe group members results for group '" + GROUP + "'");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeStateOfExistingGroupWithNoMembers(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run one consumer in the group consuming from a single-partition topic
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, ConsumerGroupState.STABLE) &&
                state.numMembers == 1 &&
                state.coordinator != null &&
                brokers().count(s -> s.config().brokerId() == state.coordinator.id()) > 0;
        }, "Expected the group '" + GROUP + "' to initially become stable, and have a single member.");

        // stop the consumer so the group has no active member anymore
        executor.shutdown();

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, ConsumerGroupState.EMPTY) && state.numMembers == 0;
        }, "Expected the group '" + GROUP + "' to become empty after the only member leaving.");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeWithConsumersWithoutAssignedPartitions(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        for (List<String> describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            // run two consumers in the group consuming from a single-partition topic
            addConsumerGroupExecutor(2, TOPIC, group, groupProtocol);
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group));
            cgcArgs.addAll(describeType);
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                int expectedNumRows = DESCRIBE_TYPE_MEMBERS.contains(describeType) ? 3 : 2;
                return res.getValue().isEmpty() && res.getKey().trim().split("\n").length == expectedNumRows;
            }, "Expected a single data row in describe group result with describe type '" + String.join(" ", describeType) + "'");
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeOffsetsWithConsumersWithoutAssignedPartitions(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run two consumers in the group consuming from a single-partition topic
        addConsumerGroupExecutor(2, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            return res.getKey().map(s -> s.equals(ConsumerGroupState.STABLE)).isPresent() &&
                res.getValue().isPresent() &&
                res.getValue().get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 1 &&
                res.getValue().get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.partition.isPresent()).count() == 1;
        }, "Expected rows for consumers with no assigned partitions in describe group results");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeMembersWithConsumersWithoutAssignedPartitions(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run two consumers in the group consuming from a single-partition topic
        addConsumerGroupExecutor(2, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, false);
            return res.getKey().map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false) &&
                res.getValue().isPresent() &&
                res.getValue().get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 2 &&
                res.getValue().get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.numPartitions == 1).count() == 1 &&
                res.getValue().get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.numPartitions == 0).count() == 1 &&
                res.getValue().get().stream().allMatch(s -> s.assignment.isEmpty());
        }, "Expected rows for consumers with no assigned partitions in describe group results");

        Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, true);
        assertTrue(res.getKey().map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false)
                && res.getValue().map(c -> c.stream().anyMatch(s -> !s.assignment.isEmpty())).orElse(false),
            "Expected additional columns in verbose version of describe members");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeStateWithConsumersWithoutAssignedPartitions(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        // run two consumers in the group consuming from a single-partition topic
        addConsumerGroupExecutor(2, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, ConsumerGroupState.STABLE) && state.numMembers == 2;
        }, "Expected two consumers in describe group results");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeWithMultiPartitionTopicAndMultipleConsumers(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        for (List<String> describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            // run two consumers in the group consuming from a two-partition topic
            addConsumerGroupExecutor(2, topic2, group, groupProtocol);
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group));
            cgcArgs.addAll(describeType);
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                int expectedNumRows = DESCRIBE_TYPE_STATE.contains(describeType) ? 2 : 3;
                return res.getValue().isEmpty() && res.getKey().trim().split("\n").length == expectedNumRows;
            }, "Expected a single data row in describe group result with describe type '" + String.join(" ", describeType) + "'");
        }
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeOffsetsWithMultiPartitionTopicAndMultipleConsumers(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        // run two consumers in the group consuming from a two-partition topic
        addConsumerGroupExecutor(2, topic2, GROUP, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            return res.getKey().map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false) &&
                res.getValue().isPresent() &&
                res.getValue().get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 2 &&
                res.getValue().get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.partition.isPresent()).count() == 2 &&
                res.getValue().get().stream().noneMatch(x -> Objects.equals(x.group, GROUP) && !x.partition.isPresent());
        }, "Expected two rows (one row per consumer) in describe group results.");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeMembersWithMultiPartitionTopicAndMultipleConsumers(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        // run two consumers in the group consuming from a two-partition topic
        addConsumerGroupExecutor(2, topic2, GROUP, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, false);
            return res.getKey().map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false) &&
                res.getValue().isPresent() &&
                res.getValue().get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 2 &&
                res.getValue().get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.numPartitions == 1).count() == 2 &&
                res.getValue().get().stream().noneMatch(x -> Objects.equals(x.group, GROUP) && x.numPartitions == 0);
        }, "Expected two rows (one row per consumer) in describe group members results.");

        Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, true);
        assertTrue(res.getKey().map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false) && res.getValue().map(s -> s.stream().filter(x -> x.assignment.isEmpty()).count()).orElse(0L) == 0,
            "Expected additional columns in verbose version of describe members");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeStateWithMultiPartitionTopicAndMultipleConsumers(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        // run two consumers in the group consuming from a two-partition topic
        addConsumerGroupExecutor(2, topic2, GROUP, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, ConsumerGroupState.STABLE) && Objects.equals(state.group, GROUP) && state.numMembers == 2;
        }, "Expected a stable group with two members in describe group state result.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft", "kraft+kip848"})
    public void testDescribeSimpleConsumerGroup(String quorum) throws Exception {
        // Ensure that the offsets of consumers which don't use group management are still displayed

        createOffsetsTopic(listenerName(), new Properties());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());
        addSimpleGroupExecutor(Arrays.asList(new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)), GROUP);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            return res.getKey().map(s -> s.equals(ConsumerGroupState.EMPTY)).orElse(false)
                && res.getValue().isPresent() && res.getValue().get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 2;
        }, "Expected a stable group with two members in describe group state result.");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeGroupWithShortInitializationTimeout(String quorum, String groupProtocol) {
        // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
        // complete before the timeout expires

        List<String> describeType = DESCRIBE_TYPES.get(RANDOM.nextInt(DESCRIBE_TYPES.size()));
        String group = GROUP + String.join("", describeType);
        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, groupProtocol);
        // set the group initialization timeout too low for the group to stabilize
        List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--timeout", "1", "--group", group));
        cgcArgs.addAll(describeType);
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

        ExecutionException e = assertThrows(ExecutionException.class, service::describeGroups);
        assertInstanceOf(TimeoutException.class, e.getCause());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeGroupOffsetsWithShortInitializationTimeout(String quorum, String groupProtocol) {
        // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
        // complete before the timeout expires

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, groupProtocol);

        // set the group initialization timeout too low for the group to stabilize
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP, "--timeout", "1"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Throwable e = assertThrows(ExecutionException.class, () -> service.collectGroupOffsets(GROUP));
        assertEquals(TimeoutException.class, e.getCause().getClass());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeGroupMembersWithShortInitializationTimeout(String quorum, String groupProtocol) {
        // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
        // complete before the timeout expires

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, groupProtocol);

        // set the group initialization timeout too low for the group to stabilize
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP, "--timeout", "1"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Throwable e = assertThrows(ExecutionException.class, () -> service.collectGroupMembers(GROUP, false));
        assertEquals(TimeoutException.class, e.getCause().getClass());
        e = assertThrows(ExecutionException.class, () -> service.collectGroupMembers(GROUP, true));
        assertEquals(TimeoutException.class, e.getCause().getClass());
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeGroupStateWithShortInitializationTimeout(String quorum, String groupProtocol) {
        // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
        // complete before the timeout expires

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, groupProtocol);

        // set the group initialization timeout too low for the group to stabilize
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP, "--timeout", "1"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Throwable e = assertThrows(ExecutionException.class, () -> service.collectGroupState(GROUP));
        assertEquals(TimeoutException.class, e.getCause().getClass());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeWithUnrecognizedNewConsumerOption(String quorum) {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        assertThrows(joptsimple.OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES)
    @MethodSource({"getTestQuorumAndGroupProtocolParametersAll"})
    public void testDescribeNonOffsetCommitGroup(String quorum, String groupProtocol) throws Exception {
        createOffsetsTopic(listenerName(), new Properties());

        Properties customProps = new Properties();
        // create a consumer group that never commits offsets
        customProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, TOPIC, GROUP, RangeAssignor.class.getName(), Optional.empty(), Optional.of(customProps), false, groupProtocol);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>> groupOffsets = service.collectGroupOffsets(GROUP);

            Predicate<PartitionAssignmentState> isGrp = s -> Objects.equals(s.group, GROUP);

            boolean res = groupOffsets.getKey().map(s -> s.equals(ConsumerGroupState.STABLE)).orElse(false) &&
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
        }, "Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for non-offset-committing group " + GROUP + ".");
    }

    private Runnable describeGroups(ConsumerGroupCommand.ConsumerGroupService service) {
        return () -> {
            try {
                service.describeGroups();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
