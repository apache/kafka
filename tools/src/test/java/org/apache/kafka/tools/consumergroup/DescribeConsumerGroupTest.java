/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools.consumergroup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.apache.kafka.tools.Tuple2;
import org.apache.kafka.tools.consumergroup.ConsumerGroupCommand.ConsumerGroupService;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class DescribeConsumerGroupTest extends ConsumerGroupCommandTest {
    private static final String[][] DESCRIBE_TYPE_OFFSETS = new String[][]{new String[]{""}, new String[]{"--offsets"}};
    private static final String[][] DESCRIBE_TYPE_MEMBERS = new String[][]{new String[]{"--members"}, new String[]{"--members", "--verbose"}};
    private static final String[][] DESCRIBE_TYPE_STATE = new String[][]{new String[]{"--state"}};
    private static final String[][] DESCRIBE_TYPES;

    static {
        List<String[]> describeTypes = new ArrayList<>();

        describeTypes.addAll(Arrays.asList(DESCRIBE_TYPE_OFFSETS));
        describeTypes.addAll(Arrays.asList(DESCRIBE_TYPE_MEMBERS));
        describeTypes.addAll(Arrays.asList(DESCRIBE_TYPE_STATE));

        DESCRIBE_TYPES = describeTypes.toArray(new String[0][0]);
    }

    @Test
    public void testDescribeNonExistingGroup() {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());
        String missingGroup = "missing.group";

        for (String[] describeType : DESCRIBE_TYPES) {
            // note the group to be queried is a different (non-existing) group
            List<String> cgcArgs = Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", missingGroup);
            cgcArgs.addAll(Arrays.asList(describeType));
            ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            String output = ToolsTestUtils.grabConsoleOutput(service::describeGroups);
            assertTrue(output.contains("Consumer group '" + missingGroup + "' does not exist."),
                "Expected error was not detected for describe option '" + String.join(" ", describeType) + "'");
        }
    }

    @Test
    public void testDescribeWithMultipleSubActions() {
        AtomicInteger exitStatus = new AtomicInteger(0);
        AtomicReference<String> exitMessage = new AtomicReference<>("");
        Exit.setExitProcedure((status, err) -> {
            exitStatus.set(status);
            exitMessage.set(err);
            throw new RuntimeException();
        });
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP, "--members", "--state"};
        try {
            ConsumerGroupCommand.main(cgcArgs);
        } catch (RuntimeException e) {
            //expected
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
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--all-groups", "--state", "Stable"};
        try {
            ConsumerGroupCommand.main(cgcArgs);
        } catch (RuntimeException e) {
            //expected
        } finally {
            Exit.resetExitProcedure();
        }
        assertEquals(1, exitStatus.get());
        assertTrue(exitMessage.get().contains("Option [describe] does not take a value for [state]"));
    }

    @Test
    public void testDescribeOffsetsOfNonExistingGroup() {
        String group = "missing.group";
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);
        // note the group to be queried is a different (non-existing) group
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(group);
        assertTrue(res.v1.map(s -> s.contains("Dead")).orElse(false) && res.v2.map(Collection::isEmpty).orElse(false),
            "Expected the state to be 'Dead', with no members in the group '" + group + "'.");
    }

    @Test
    public void testDescribeMembersOfNonExistingGroup() {
        String group = "missing.group";
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);
        // note the group to be queried is a different (non-existing) group
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(group, false);
        assertTrue(res.v1.map(s -> s.contains("Dead")).orElse(false) && res.v2.map(Collection::isEmpty).orElse(false),
            "Expected the state to be 'Dead', with no members in the group '" + group + "'.");

        Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res2 = service.collectGroupMembers(group, true);
        assertTrue(res2.v1.map(s -> s.contains("Dead")).orElse(false) && res2.v2.map(Collection::isEmpty).orElse(false),
            "Expected the state to be 'Dead', with no members in the group '" + group + "' (verbose option).");
    }

    @Test
    public void testDescribeStateOfNonExistingGroup() {
        String group = "missing.group";
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);
        // note the group to be queried is a different (non-existing) group
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        GroupState state = service.collectGroupState(group);
        assertTrue(Objects.equals(state.state, "Dead") && state.numMembers == 0 &&
                state.coordinator != null && servers().map(s -> s.config().brokerId()).toList().contains(state.coordinator.id()),
            "Expected the state to be 'Dead', with no members in the group '" + group + "'."
        );
    }

    @Test
    public void testDescribeExistingGroup() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        for (String[] describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            // run one consumer in the group consuming from a single-partition topic
            addConsumerGroupExecutor(1, TOPIC, group);
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group));
            cgcArgs.addAll(Arrays.asList(describeType));
            ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Tuple2<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(service::describeGroups);
                return res.v1.trim().split("\n").length == 2 && res.v2.isEmpty();
            }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
        }
    }

    @Test
    public void testDescribeExistingGroups() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // Create N single-threaded consumer groups from a single-partition topic
        List<String> groups = new ArrayList<>();

        for (String[] describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            addConsumerGroupExecutor(1, TOPIC, group);
            groups.addAll(Arrays.asList("--group", group));
        }

        int expectedNumLines = DESCRIBE_TYPES.length * 2;

        for (String[] describeType : DESCRIBE_TYPES) {
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe"));
            cgcArgs.addAll(groups);
            cgcArgs.addAll(Arrays.asList(describeType));
            ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Tuple2<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(service::describeGroups);
                long numLines = Arrays.stream(res.v1.trim().split("\n")).filter(line -> !line.isEmpty()).count();
                return (numLines == expectedNumLines) && res.v2.isEmpty();
            }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
        }
    }

    @Test
    public void testDescribeAllExistingGroups() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // Create N single-threaded consumer groups from a single-partition topic
        for (String[] describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            addConsumerGroupExecutor(1, TOPIC, group);
        }

        int expectedNumLines = DESCRIBE_TYPES.length * 2;

        for (String[] describeType : DESCRIBE_TYPES) {
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--all-groups"));
            cgcArgs.addAll(Arrays.asList(describeType));
            ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Tuple2<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(service::describeGroups);
                long numLines = Arrays.stream(res.v1.trim().split("\n")).filter(s -> !s.isEmpty()).count();
                return (numLines == expectedNumLines) && res.v2.isEmpty();
            }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
        }
    }

    @Test
    public void testDescribeOffsetsOfExistingGroup() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            Optional<String> state = res.v1;
            Optional<Collection<PartitionAssignmentState>> assignments = res.v2;

            Predicate<PartitionAssignmentState> isGrp = s -> Objects.equals(s.group, GROUP);

            return state.map(s -> s.contains("Stable")).orElse(false) &&
                assignments.isPresent() &&
                assignments.get().stream().filter(isGrp).count() == 1 &&
                !assignments.get().stream().filter(isGrp).findFirst().get().consumerId.map(s0 -> s0.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                !assignments.get().stream().filter(isGrp).findFirst().get().clientId.map(s0 -> s0.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                !assignments.get().stream().filter(isGrp).findFirst().get().host.map(h -> h.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false);
        }, "Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for group " + GROUP + ".");
    }

    @Test
    public void testDescribeMembersOfExistingGroup() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, false);
            Optional<String> state = res.v1;
            Optional<Collection<MemberAssignmentState>> assignments = res.v2;

            Predicate<MemberAssignmentState> isGrp = s -> Objects.equals(s.group, GROUP);

            return state.map(s -> s.contains("Stable")).orElse(false) &&
                assignments.isPresent() &&
                    assignments.get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 1 &&
                !Objects.equals(assignments.get().stream().filter(isGrp).findFirst().get().consumerId, ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
                !Objects.equals(assignments.get().stream().filter(isGrp).findFirst().get().clientId, ConsumerGroupCommand.MISSING_COLUMN_VALUE) &&
                !Objects.equals(assignments.get().stream().filter(isGrp).findFirst().get().host, ConsumerGroupCommand.MISSING_COLUMN_VALUE);
        }, "Expected a 'Stable' group status, rows and valid member information for group " + GROUP + ".");

        Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, true);

        if (res.v2.isPresent()) {
            assertTrue(res.v2.get().size() == 1 && res.v2.get().iterator().next().assignment.size() == 1,
                "Expected a topic partition assigned to the single group member for group " + GROUP);
        } else {
            fail("Expected partition assignments for members of group " + GROUP);
        }
    }

    @Test
    public void testDescribeStateOfExistingGroup() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, "Stable") &&
                state.numMembers == 1 &&
                Objects.equals(state.assignmentStrategy, "range") &&
                state.coordinator != null &&
                !servers().find(s -> s.config().brokerId() == state.coordinator.id()).isDefined();
        }, "Expected a 'Stable' group status, with one member and round robin assignment strategy for group " + GROUP + ".");
    }

    @Test
    public void testDescribeStateOfExistingGroupWithRoundRobinAssignor() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, TOPIC, GROUP, RoundRobinAssignor.class.getName(), Optional.empty(), false);
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, "Stable") &&
                state.numMembers == 1 &&
                Objects.equals(state.assignmentStrategy, "roundrobin") &&
                state.coordinator != null &&
                servers().find(s -> s.config().brokerId() == state.coordinator.id()).isDefined();
        }, "Expected a 'Stable' group status, with one member and round robin assignment strategy for group " + GROUP + ".");
    }

    @Test
    public void testDescribeExistingGroupWithNoMembers() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        for (String[] describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            // run one consumer in the group consuming from a single-partition topic
            ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, group);
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP));
            cgcArgs.addAll(Arrays.asList(describeType));
            ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Tuple2<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(service::describeGroups);
                return res.v1.trim().split("\n").length == 2 && res.v1.isEmpty();
            }, "Expected describe group results with one data row for describe type '" + String.join(" ", describeType) + "'");

            // stop the consumer so the group has no active member anymore
            executor.shutdown();
            TestUtils.waitForCondition(
                () -> ToolsTestUtils.grabConsoleError(service::describeGroups).contains("Consumer group '" + GROUP + "' has no active members."),
                "Expected no active member in describe group results with describe type " + String.join(" ", describeType));
        }
    }

    @Test
    public void testDescribeOffsetsOfExistingGroupWithNoMembers() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            return res.v1.map(s -> s.contains("Stable")).orElse(false)
                && res.v2.map(c -> c.stream().anyMatch(state -> Objects.equals(state.group, GROUP))).orElse(false);
        }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.");

        // stop the consumer so the group has no active member anymore
        executor.shutdown();

        Tuple2<Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>>, Boolean> res = ToolsTestUtils.computeUntilTrue(() -> service.collectGroupOffsets(GROUP), DEFAULT_MAX_WAIT_MS, 100, offsets -> {
            Optional<String> state = offsets.v1;
            Optional<Collection<PartitionAssignmentState>> assignments = offsets.v2;

            List<PartitionAssignmentState> testGroupAssignments =
                assignments.orElse(Collections.emptyList()).stream().filter(a -> Objects.equals(a.group, GROUP)).collect(Collectors.toList());
            PartitionAssignmentState assignment = testGroupAssignments.get(0);
            return state.map(s -> s.contains("Empty")).orElse(false) &&
                testGroupAssignments.size() == 1 &&
                assignment.consumerId.map(c -> c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) && // the member should be gone
                assignment.clientId.map(c -> c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                assignment.host.map(c -> c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false);
        });

        assertTrue(res.v2, "Expected no active member in describe group results, state: " + res.v1.v1 + ", assignments: " + res.v1.v2);
    }

    @Test
    public void testDescribeMembersOfExistingGroupWithNoMembers() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, false);
            return res.v1.map(s -> s.contains("Stable")).orElse(false)
                && res.v2.map(c -> c.stream().anyMatch(m -> Objects.equals(m.group, GROUP))).orElse(false);
        }, "Expected the group to initially become stable, and to find group in assignments after initial offset commit.");

        // stop the consumer so the group has no active member anymore
        executor.shutdown();

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, false);
            return res.v1.map(s -> s.contains("Empty")).orElse(false) && res.v2.isPresent() && res.v2.get().isEmpty();
        }, "Expected no member in describe group members results for group '" + GROUP + "'");
    }

    @Test
    public void testDescribeStateOfExistingGroupWithNoMembers() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run one consumer in the group consuming from a single-partition topic
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, "Stable") &&
                state.numMembers == 1 &&
                state.coordinator != null &&
                servers().find(s -> s.config().brokerId() == state.coordinator.id()).isDefined();
        }, "Expected the group '" + GROUP + "' to initially become stable, and have a single member.");

        // stop the consumer so the group has no active member anymore
        executor.shutdown();

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, "Empty") && state.numMembers == 0 && Objects.equals(state.assignmentStrategy, "");
        }, "Expected the group '" + GROUP + "' to become empty after the only member leaving.");
    }

    @Test
    public void testDescribeWithConsumersWithoutAssignedPartitions() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        for (String[] describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            // run two consumers in the group consuming from a single-partition topic
            addConsumerGroupExecutor(2, TOPIC, group);
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group));
            cgcArgs.addAll(Arrays.asList(describeType));
            ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Tuple2<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(service::describeGroups);
                int expectedNumRows = Arrays.asList(DESCRIBE_TYPE_MEMBERS).contains(describeType) ? 3 : 2;
                return res.v2.isEmpty() && res.v1.trim().split("\n").length == expectedNumRows;
            }, "Expected a single data row in describe group result with describe type '" + String.join(" ", describeType) + "'");
        }
    }

    @Test
    public void testDescribeOffsetsWithConsumersWithoutAssignedPartitions() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run two consumers in the group consuming from a single-partition topic
        addConsumerGroupExecutor(2);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            return res.v1.map(s -> s.contains("Stable")).isPresent() &&
                res.v2.isPresent() &&
                res.v2.get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 1 &&
                res.v2.get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.partition.isPresent()).count() == 1;
        }, "Expected rows for consumers with no assigned partitions in describe group results");
    }

    @Test
    public void testDescribeMembersWithConsumersWithoutAssignedPartitions() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run two consumers in the group consuming from a single-partition topic
        addConsumerGroupExecutor(2);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, false);
            return res.v1.map(s -> s.contains("Stable")).orElse(false) &&
                res.v2.isPresent() &&
                res.v2.get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 2 &&
                res.v2.get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.numPartitions == 1).count() == 1 &&
                res.v2.get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.numPartitions == 0).count() == 1 &&
                res.v2.get().stream().allMatch(s -> s.assignment.isEmpty());
        }, "Expected rows for consumers with no assigned partitions in describe group results");

        Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, true);
        assertTrue(res.v1.map(s -> s.contains("Stable")).orElse(false)
                && res.v2.map(c -> c.stream().anyMatch(s -> !s.assignment.isEmpty())).orElse(false),
            "Expected additional columns in verbose version of describe members");
    }

    @Test
    public void testDescribeStateWithConsumersWithoutAssignedPartitions() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        // run two consumers in the group consuming from a single-partition topic
        addConsumerGroupExecutor(2);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, "Stable") && state.numMembers == 2;
        }, "Expected two consumers in describe group results");
    }

    @Test
    public void testDescribeWithMultiPartitionTopicAndMultipleConsumers() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        for (String[] describeType : DESCRIBE_TYPES) {
            String group = GROUP + String.join("", describeType);
            // run two consumers in the group consuming from a two-partition topic
            addConsumerGroupExecutor(2, topic2, group);
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group));
            cgcArgs.addAll(Arrays.asList(describeType));
            ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

            TestUtils.waitForCondition(() -> {
                Tuple2<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(service::describeGroups);
                int expectedNumRows = Arrays.asList(DESCRIBE_TYPE_STATE).contains(describeType) ? 2 : 3;
                return res.v2.isEmpty() && res.v1.trim().split("\n").length == expectedNumRows;
            }, "Expected a single data row in describe group result with describe type '" + String.join(" ", describeType) + "'");
        }
    }

    @Test
    public void testDescribeOffsetsWithMultiPartitionTopicAndMultipleConsumers() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        // run two consumers in the group consuming from a two-partition topic
        addConsumerGroupExecutor(2, topic2, GROUP);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            return res.v1.map(s -> s.contains("Stable")).orElse(false) &&
                res.v2.isPresent() &&
                res.v2.get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 2 &&
                res.v2.get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.partition.isPresent()).count() == 2 &&
                res.v2.get().stream().noneMatch(x -> Objects.equals(x.group, GROUP) && !x.partition.isPresent());
        }, "Expected two rows (one row per consumer) in describe group results.");
    }

    @Test
    public void testDescribeMembersWithMultiPartitionTopicAndMultipleConsumers() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        // run two consumers in the group consuming from a two-partition topic
        addConsumerGroupExecutor(2, topic2, GROUP);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, false);
            return res.v1.map(s -> s.contains("Stable")).orElse(false) &&
                res.v2.isPresent() &&
                res.v2.get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 2 &&
                res.v2.get().stream().filter(x -> Objects.equals(x.group, GROUP) && x.numPartitions == 1).count() == 2 &&
                res.v2.get().stream().noneMatch(x -> Objects.equals(x.group, GROUP) && x.numPartitions == 0);
        }, "Expected two rows (one row per consumer) in describe group members results.");

        Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>> res = service.collectGroupMembers(GROUP, true);
        assertTrue(res.v1.map(s -> s.contains("Stable")).orElse(false) && res.v2.map(s -> s.stream().filter(x -> x.assignment.isEmpty()).count()).orElse(0L) == 0,
            "Expected additional columns in verbose version of describe members");
    }

    @Test
    public void testDescribeStateWithMultiPartitionTopicAndMultipleConsumers() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        // run two consumers in the group consuming from a two-partition topic
        addConsumerGroupExecutor(2, topic2, GROUP);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(GROUP);
            return Objects.equals(state.state, "Stable") && Objects.equals(state.group, GROUP) && state.numMembers == 2;
        }, "Expected a stable group with two members in describe group state result.");
    }

    @Test
    public void testDescribeSimpleConsumerGroup() throws Exception {
        // Ensure that the offsets of consumers which don't use group management are still displayed

        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());
        String topic2 = "foo2";
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());
        addSimpleGroupExecutor(Arrays.asList(new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)), GROUP);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            return res.v1.map(s -> s.contains("Empty")).orElse(false)
                && res.v2.isPresent() && res.v2.get().stream().filter(s -> Objects.equals(s.group, GROUP)).count() == 2;
        }, "Expected a stable group with two members in describe group state result.");
    }

    @Test
    public void testDescribeGroupWithShortInitializationTimeout() {
        // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
        // complete before the timeout expires

        String[] describeType = DESCRIBE_TYPES[RANDOM.nextInt(DESCRIBE_TYPES.length)];
        String group = GROUP + String.join("", describeType);
        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);
        // set the group initialization timeout too low for the group to stabilize
        List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--timeout", "1", "--group", group));
        cgcArgs.addAll(Arrays.asList(describeType));
        ConsumerGroupService service = getConsumerGroupService(cgcArgs.toArray(new String[0]));

        Throwable e = assertThrows(ExecutionException.class, () -> ToolsTestUtils.grabConsoleOutputAndError(service::describeGroups));
        assertEquals(TimeoutException.class, e.getCause().getClass());
    }

    @Test
    public void testDescribeGroupOffsetsWithShortInitializationTimeout() {
        // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
        // complete before the timeout expires

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);

        // set the group initialization timeout too low for the group to stabilize
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP, "--timeout", "1"};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Throwable e = assertThrows(ExecutionException.class, () -> service.collectGroupOffsets(GROUP));
        assertEquals(TimeoutException.class, e.getCause().getClass());
    }

    @Test
    public void testDescribeGroupMembersWithShortInitializationTimeout() {
        // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
        // complete before the timeout expires

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);

        // set the group initialization timeout too low for the group to stabilize
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP, "--timeout", "1"};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Throwable e = assertThrows(ExecutionException.class, () -> service.collectGroupMembers(GROUP, false));
        assertEquals(TimeoutException.class, e.getCause().getClass());
        e = assertThrows(ExecutionException.class, () -> service.collectGroupMembers(GROUP, true));
        assertEquals(TimeoutException.class, e.getCause().getClass());
    }

    @Test
    public void testDescribeGroupStateWithShortInitializationTimeout() {
        // Let creation of the offsets topic happen during group initialization to ensure that initialization doesn't
        // complete before the timeout expires

        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1);

        // set the group initialization timeout too low for the group to stabilize
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP, "--timeout", "1"};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        Throwable e = assertThrows(ExecutionException.class, () -> service.collectGroupState(GROUP));
        assertEquals(TimeoutException.class, e.getCause().getClass());
    }

    @Test
    public void testDescribeWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        assertThrows(joptsimple.OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @Test
    public void testDescribeNonOffsetCommitGroup() throws Exception {
        kafka.utils.TestUtils.createOffsetsTopic(zkClient(), servers());

        Properties customProps = new Properties();
        // create a consumer group that never commits offsets
        customProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // run one consumer in the group consuming from a single-partition topic
        addConsumerGroupExecutor(1, TOPIC, GROUP, RangeAssignor.class.getName(), Optional.of(customProps), false);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", GROUP};
        ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() -> {
            Predicate<PartitionAssignmentState> isGrp = s -> Objects.equals(s.group, GROUP);

            Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>> res = service.collectGroupOffsets(GROUP);
            return res.v1.map(s -> s.contains("Stable")).orElse(false) &&
                res.v2.isPresent() &&
                res.v2.get().stream().filter(isGrp).count() == 1 &&
                res.v2.get().stream().filter(isGrp).findFirst().get().consumerId.map(c -> !c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                res.v2.get().stream().filter(isGrp).findFirst().get().clientId.map(c -> !c.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false) &&
                res.v2.get().stream().filter(isGrp).findFirst().get().host.map(h -> !h.trim().equals(ConsumerGroupCommand.MISSING_COLUMN_VALUE)).orElse(false);
        }, "Expected a 'Stable' group status, rows and valid values for consumer id / client id / host columns in describe results for non-offset-committing group " + GROUP + ".");
    }
}
