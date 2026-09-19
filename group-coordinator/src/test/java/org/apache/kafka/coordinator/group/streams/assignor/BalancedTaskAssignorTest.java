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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.api.streams.assignor.TopologyDescriber;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BalancedTaskAssignorTest {

    private static final String SUBTOPOLOGY_1 = "test-subtopology1";
    private static final String SUBTOPOLOGY_2 = "test-subtopology2";

    private final BalancedTaskAssignor assignor = new BalancedTaskAssignor();

    @Test
    public void testToStringReturnsName() {
        assertEquals("balanced", assignor.name());
        assertEquals(assignor.name(), assignor.toString());
    }

    @Test
    public void shouldThrowWhenThereAreTasksButNoMembers() {
        assertThrows(TaskAssignorException.class, () -> assignor.assign(
            new GroupSpecImpl(Map.of(), AssignmentConfigsImpl.DEFAULT),
            statefulTopology(3, SUBTOPOLOGY_1)
        ));
    }

    @Test
    public void shouldAssignNothingWhenThereAreNoTasks() {
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members("member1", "process1", "member2", "process2"), AssignmentConfigsImpl.DEFAULT),
            statefulTopology(0, SUBTOPOLOGY_1)
        );

        assertEquals(2, result.members().size());
        assertEquals(new MemberAssignment(Map.of(), Map.of()), result.members().get("member1"));
        assertEquals(new MemberAssignment(Map.of(), Map.of()), result.members().get("member2"));
    }

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverProcessesWhereNumberOfProcessesIntegralDivisorOfNumberOfTasks() {
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members("member1", "process1", "member2", "process2", "member3", "process3"), AssignmentConfigsImpl.DEFAULT),
            statefulTopology(6, SUBTOPOLOGY_1)
        );

        assertEquals(2, activeTaskCount(result, "member1"));
        assertEquals(2, activeTaskCount(result, "member2"));
        assertEquals(2, activeTaskCount(result, "member3"));
        assertAllTasksAssignedOnce(result, statefulTopology(6, SUBTOPOLOGY_1));
        assertNoStandbyTasks(result);
    }

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverMembersWhereNumberOfMembersIntegralDivisorOfNumberOfTasks() {
        // process1 has two members, process2 one; so process1 gets twice as many tasks.
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members("member1_1", "process1", "member1_2", "process1", "member2_1", "process2"), AssignmentConfigsImpl.DEFAULT),
            statefulTopology(6, SUBTOPOLOGY_1)
        );

        assertEquals(2, activeTaskCount(result, "member1_1"));
        assertEquals(2, activeTaskCount(result, "member1_2"));
        assertEquals(2, activeTaskCount(result, "member2_1"));
        assertAllTasksAssignedOnce(result, statefulTopology(6, SUBTOPOLOGY_1));
    }

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverProcessesWhereNumberOfProcessesNotIntegralDivisorOfNumberOfTasks() {
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members("member1", "process1", "member2", "process2"), AssignmentConfigsImpl.DEFAULT),
            statefulTopology(5, SUBTOPOLOGY_1)
        );

        final int count1 = activeTaskCount(result, "member1");
        final int count2 = activeTaskCount(result, "member2");
        assertEquals(5, count1 + count2);
        assertTrue(Math.abs(count1 - count2) <= 1, "Expected an even split, got " + count1 + " and " + count2);
        assertAllTasksAssignedOnce(result, statefulTopology(5, SUBTOPOLOGY_1));
    }

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverUnevenlyDistributedMembers() {
        // process1 has three members, process2 has one. Round-robin would give each process four tasks, so the
        // balancing step has to move two tasks to process1 to even out the per-member load.
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members(
                "member1_1", "process1", "member1_2", "process1", "member1_3", "process1",
                "member2_1", "process2"
            ), AssignmentConfigsImpl.DEFAULT),
            statefulTopology(8, SUBTOPOLOGY_1)
        );

        assertEquals(2, activeTaskCount(result, "member1_1"));
        assertEquals(2, activeTaskCount(result, "member1_2"));
        assertEquals(2, activeTaskCount(result, "member1_3"));
        assertEquals(2, activeTaskCount(result, "member2_1"));
        assertAllTasksAssignedOnce(result, statefulTopology(8, SUBTOPOLOGY_1));
    }

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverProcessesWithMoreProcessesThanTasks() {
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members("member1", "process1", "member2", "process2", "member3", "process3", "member4", "process4"), AssignmentConfigsImpl.DEFAULT),
            statefulTopology(2, SUBTOPOLOGY_1)
        );

        // Which two of the four interchangeable processes receive a task is a tie-break, not a property.
        assertEquals(List.of(0, 0, 1, 1), sortedActiveTaskCounts(result, "member1", "member2", "member3", "member4"));
        assertAllTasksAssignedOnce(result, statefulTopology(2, SUBTOPOLOGY_1));
    }

    @Test
    public void shouldSpreadTasksOfEachSubtopologyOverProcesses() {
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members("member1", "process1", "member2", "process2"), AssignmentConfigsImpl.DEFAULT),
            statefulTopology(2, SUBTOPOLOGY_1, SUBTOPOLOGY_2)
        );

        // Each process gets one partition of each subtopology, never both partitions of the same subtopology.
        assertOnePartitionOfEachSubtopology(result, "member1", SUBTOPOLOGY_1, SUBTOPOLOGY_2);
        assertOnePartitionOfEachSubtopology(result, "member2", SUBTOPOLOGY_1, SUBTOPOLOGY_2);
        assertAllTasksAssignedOnce(result, statefulTopology(2, SUBTOPOLOGY_1, SUBTOPOLOGY_2));
    }

    @Test
    public void shouldNotDependOnPreviousAssignmentAcrossProcessesUnlikeStickyAssignor() {
        // The previous assignment groups the tasks by subtopology. The sticky assignor keeps that layout, because
        // both members are below quota; the balanced assignor recomputes the interleaved layout regardless.
        final Map<String, MemberMetadataAndStateImpl> members = mkMap(
            mkEntry("member1", memberWithTasks("process1", mkMap(mkEntry(SUBTOPOLOGY_1, Set.of(0, 1))), Map.of())),
            mkEntry("member2", memberWithTasks("process2", mkMap(mkEntry(SUBTOPOLOGY_2, Set.of(0, 1))), Map.of()))
        );
        final TopologyDescriber topology = statefulTopology(2, SUBTOPOLOGY_1, SUBTOPOLOGY_2);

        final GroupAssignment sticky = new StickyTaskAssignor().assign(new GroupSpecImpl(members, AssignmentConfigsImpl.DEFAULT), topology);
        assertEquals(mkMap(mkEntry(SUBTOPOLOGY_1, Set.of(0, 1))), sticky.members().get("member1").activeTasks());
        assertEquals(mkMap(mkEntry(SUBTOPOLOGY_2, Set.of(0, 1))), sticky.members().get("member2").activeTasks());

        final GroupAssignment balanced = assignor.assign(new GroupSpecImpl(members, AssignmentConfigsImpl.DEFAULT), topology);
        assertOnePartitionOfEachSubtopology(balanced, "member1", SUBTOPOLOGY_1, SUBTOPOLOGY_2);
        assertOnePartitionOfEachSubtopology(balanced, "member2", SUBTOPOLOGY_1, SUBTOPOLOGY_2);
        assertAllTasksAssignedOnce(balanced, topology);
    }

    @Test
    public void shouldKeepTasksOnTheirCurrentMemberWithinAProcess() {
        // Both members of the process are at quota with their current tasks, so nothing needs to move within the
        // process even though the tasks were dealt to the process in a different order.
        final Map<String, MemberMetadataAndStateImpl> members = mkMap(
            mkEntry("member1", memberWithTasks("process1", mkMap(mkEntry(SUBTOPOLOGY_1, Set.of(1, 3))), Map.of())),
            mkEntry("member2", memberWithTasks("process1", mkMap(mkEntry(SUBTOPOLOGY_1, Set.of(0, 2))), Map.of()))
        );

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, AssignmentConfigsImpl.DEFAULT),
            statefulTopology(4, SUBTOPOLOGY_1)
        );

        assertEquals(mkMap(mkEntry(SUBTOPOLOGY_1, Set.of(1, 3))), result.members().get("member1").activeTasks());
        assertEquals(mkMap(mkEntry(SUBTOPOLOGY_1, Set.of(0, 2))), result.members().get("member2").activeTasks());
    }

    @Test
    public void shouldMoveTasksOffAnOverloadedMemberWithinAProcess() {
        // member1 currently owns every task; it keeps two (its quota) and the other two move to member2.
        final Map<String, MemberMetadataAndStateImpl> members = mkMap(
            mkEntry("member1", memberWithTasks("process1", mkMap(mkEntry(SUBTOPOLOGY_1, Set.of(0, 1, 2, 3))), Map.of())),
            mkEntry("member2", memberWithTasks("process1", Map.of(), Map.of()))
        );

        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members, AssignmentConfigsImpl.DEFAULT),
            statefulTopology(4, SUBTOPOLOGY_1)
        );

        // member1 keeps two of its current tasks -- which two is a tie-break -- and member2 receives the other two.
        final Set<Integer> keptByMember1 = result.members().get("member1").activeTasks().get(SUBTOPOLOGY_1);
        final Set<Integer> movedToMember2 = result.members().get("member2").activeTasks().get(SUBTOPOLOGY_1);
        assertEquals(2, keptByMember1.size());
        assertEquals(2, movedToMember2.size());
        assertAllTasksAssignedOnce(result, statefulTopology(4, SUBTOPOLOGY_1));
    }

    @Test
    public void shouldAssignStandbysForStatefulTasks() {
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                members("member1", "process1", "member2", "process2", "member3", "process3"),
                AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(1)
            ),
            statefulTopology(3, SUBTOPOLOGY_1)
        );

        for (final String memberId : List.of("member1", "member2", "member3")) {
            assertEquals(1, activeTaskCount(result, memberId));
            assertEquals(1, standbyTaskCount(result, memberId));
        }
        assertAllTasksAssignedOnce(result, statefulTopology(3, SUBTOPOLOGY_1));
        assertStandbysAssigned(result, statefulTopology(3, SUBTOPOLOGY_1), 1);
    }

    @Test
    public void shouldNotAssignStandbysForStatelessTasks() {
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                members("member1", "process1", "member2", "process2"),
                AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(1)
            ),
            statelessTopology(4, SUBTOPOLOGY_1)
        );

        assertEquals(2, activeTaskCount(result, "member1"));
        assertEquals(2, activeTaskCount(result, "member2"));
        assertNoStandbyTasks(result);
    }

    @Test
    public void shouldNotAssignStandbyToTheProcessThatOwnsTheActiveTask() {
        // Two members per process: a standby must still land on the other process, not on the sibling member.
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                members("member1_1", "process1", "member1_2", "process1", "member2_1", "process2", "member2_2", "process2"),
                AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(1)
            ),
            statefulTopology(4, SUBTOPOLOGY_1)
        );

        final Set<Integer> process1Actives = mergeTasks(result, true, "member1_1", "member1_2").getOrDefault(SUBTOPOLOGY_1, Set.of());
        final Set<Integer> process1Standbys = mergeTasks(result, false, "member1_1", "member1_2").getOrDefault(SUBTOPOLOGY_1, Set.of());
        assertEquals(2, process1Actives.size());
        assertEquals(2, process1Standbys.size());
        assertTrue(process1Actives.stream().noneMatch(process1Standbys::contains),
            "process1 holds " + process1Actives + " as active and " + process1Standbys + " as standby");
        assertStandbysAssigned(result, statefulTopology(4, SUBTOPOLOGY_1), 1);
    }

    @Test
    public void shouldNotAssignAnyStandbysWithInsufficientCapacity() {
        // A single process cannot hold a standby of its own active tasks; the assignor warns and carries on.
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                members("member1", "process1", "member2", "process1"),
                AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(1)
            ),
            statefulTopology(4, SUBTOPOLOGY_1)
        );

        assertEquals(2, activeTaskCount(result, "member1"));
        assertEquals(2, activeTaskCount(result, "member2"));
        assertNoStandbyTasks(result);
    }

    @Test
    public void shouldAssignAsManyStandbysAsCapacityAllows() {
        // Two processes, two standby replicas requested: only one standby per task fits.
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(
                members("member1", "process1", "member2", "process2"),
                AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(2)
            ),
            statefulTopology(2, SUBTOPOLOGY_1)
        );

        assertEquals(1, activeTaskCount(result, "member1"));
        assertEquals(1, activeTaskCount(result, "member2"));
        assertStandbysAssigned(result, statefulTopology(2, SUBTOPOLOGY_1), 1);
    }

    @Test
    public void shouldDistributeStatelessTasksToBalanceTotalTaskLoad() {
        // Stateful placement leaves process1 (two members) and process2 (one member) at a load of one task per
        // member. The stateless tasks are then dealt by active load, so process1 gets two of them and process2 one.
        final GroupAssignment result = assignor.assign(
            new GroupSpecImpl(members("member1_1", "process1", "member1_2", "process1", "member2_1", "process2"), AssignmentConfigsImpl.DEFAULT),
            new MixedTopologyDescriber(3, 3)
        );

        assertEquals(2, activeTaskCount(result, "member1_1"));
        assertEquals(2, activeTaskCount(result, "member1_2"));
        assertEquals(2, activeTaskCount(result, "member2_1"));
        assertAllTasksAssignedOnce(result, new MixedTopologyDescriber(3, 3));
        assertNoStandbyTasks(result);
    }

    @Test
    public void shouldIgnoreWarmupTasksAndReportedOffsets() {
        // The balanced strategy does not use lag or warm-up information; the result must match the plain spec.
        final Map<String, MemberMetadataAndStateImpl> plainMembers = members("member1", "process1", "member2", "process2");
        final Map<String, MemberMetadataAndStateImpl> membersWithHints = mkMap(
            mkEntry("member1", new MemberMetadataAndStateImpl(
                Optional.of("instance1"),
                Optional.of("rack1"),
                "process1",
                Map.of("zone", "a"),
                Map.of(),
                Map.of(),
                mkMap(mkEntry(SUBTOPOLOGY_1, Set.of(1, 3))),
                mkMap(mkEntry(SUBTOPOLOGY_1, mkMap(mkEntry(1, 100L), mkEntry(3, 100L)))),
                mkMap(mkEntry(SUBTOPOLOGY_1, mkMap(mkEntry(1, 100L), mkEntry(3, 100L))))
            )),
            mkEntry("member2", new MemberMetadataAndStateImpl(
                Optional.of("instance2"),
                Optional.of("rack2"),
                "process2",
                Map.of("zone", "b"),
                Map.of(),
                Map.of(),
                Map.of(),
                mkMap(mkEntry(SUBTOPOLOGY_1, mkMap(mkEntry(0, 5L), mkEntry(2, 5L)))),
                mkMap(mkEntry(SUBTOPOLOGY_1, mkMap(mkEntry(0, 500L), mkEntry(2, 500L))))
            ))
        );
        final TopologyDescriber topology = statefulTopology(4, SUBTOPOLOGY_1);

        final GroupAssignment plain = assignor.assign(new GroupSpecImpl(plainMembers, AssignmentConfigsImpl.DEFAULT), topology);
        final GroupAssignment withHints = assignor.assign(new GroupSpecImpl(membersWithHints, AssignmentConfigsImpl.DEFAULT), topology);

        assertEquals(plain, withHints);
    }

    @Test
    public void shouldBeDeterministicRegardlessOfMemberOrder() {
        final Map<String, MemberMetadataAndStateImpl> ordered = new TreeMap<>(members(
            "member1", "process2", "member2", "process1", "member3", "process3", "member4", "process1"
        ));
        final Map<String, MemberMetadataAndStateImpl> reversed = new TreeMap<>((a, b) -> b.compareTo(a));
        reversed.putAll(ordered);
        final TopologyDescriber topology = new MixedTopologyDescriber(5, 3);

        assertEquals(
            assignor.assign(new GroupSpecImpl(ordered, AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(1)), topology),
            assignor.assign(new GroupSpecImpl(reversed, AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(1)), topology)
        );
    }

    @Test
    public void shouldProduceValidAndEvenAssignmentsForRandomInput() {
        final long seed = System.nanoTime();
        final Random random = new Random(seed);

        for (int iteration = 0; iteration < 50; iteration++) {
            final int numProcesses = 1 + random.nextInt(8);
            final int membersPerProcess = 1 + random.nextInt(4);
            final int numStatefulTasks = random.nextInt(20);
            final int numStatelessTasks = random.nextInt(20);
            final int numStandbyReplicas = random.nextInt(3);

            final Map<String, MemberMetadataAndStateImpl> members = new HashMap<>();
            for (int p = 0; p < numProcesses; p++) {
                for (int m = 0; m < membersPerProcess; m++) {
                    members.put("member" + p + "_" + m, member("process" + p));
                }
            }
            final TopologyDescriber topology = new MixedTopologyDescriber(numStatefulTasks, numStatelessTasks);
            final String context = String.format("seed=%d processes=%d membersPerProcess=%d stateful=%d stateless=%d standbys=%d",
                seed, numProcesses, membersPerProcess, numStatefulTasks, numStatelessTasks, numStandbyReplicas);

            final GroupAssignment result = assignor.assign(
                new GroupSpecImpl(members, AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(numStandbyReplicas)),
                topology
            );

            assertAllTasksAssignedOnce(result, topology);
            // Every process has the same capacity, so the number of active tasks per member may differ by at most one.
            final List<Integer> activeCounts = members.keySet().stream().map(memberId -> activeTaskCount(result, memberId)).toList();
            final int maxActive = activeCounts.stream().mapToInt(Integer::intValue).max().orElse(0);
            final int minActive = activeCounts.stream().mapToInt(Integer::intValue).min().orElse(0);
            assertTrue(maxActive - minActive <= 1, "Active tasks per member not even (" + context + "): " + activeCounts);

            // Standbys are bounded by the replicas requested and the number of other processes, and never share a
            // process with the active task or another standby of the same task.
            final int expectedStandbys = Math.min(numStandbyReplicas, numProcesses - 1);
            assertStandbysAssigned(result, topology, expectedStandbys);
            for (int p = 0; p < numProcesses; p++) {
                final String[] memberIds = new String[membersPerProcess];
                for (int m = 0; m < membersPerProcess; m++) {
                    memberIds[m] = "member" + p + "_" + m;
                }
                final Map<String, Set<Integer>> actives = mergeTasks(result, true, memberIds);
                final Map<String, Set<Integer>> standbys = mergeTasks(result, false, memberIds);
                for (final Map.Entry<String, Set<Integer>> entry : standbys.entrySet()) {
                    final Set<Integer> activePartitions = actives.getOrDefault(entry.getKey(), Set.of());
                    assertTrue(entry.getValue().stream().noneMatch(activePartitions::contains),
                        "process" + p + " holds a task as active and standby (" + context + ")");
                }
            }
        }
    }

    private static Map<String, MemberMetadataAndStateImpl> members(final String... memberIdsAndProcessIds) {
        final Map<String, MemberMetadataAndStateImpl> members = new HashMap<>();
        for (int i = 0; i < memberIdsAndProcessIds.length; i += 2) {
            members.put(memberIdsAndProcessIds[i], member(memberIdsAndProcessIds[i + 1]));
        }
        return members;
    }

    private static MemberMetadataAndStateImpl member(final String processId) {
        return memberWithTasks(processId, Map.of(), Map.of());
    }

    private static MemberMetadataAndStateImpl memberWithTasks(final String processId,
                                                              final Map<String, Set<Integer>> activeTasks,
                                                              final Map<String, Set<Integer>> standbyTasks) {
        return new MemberMetadataAndStateImpl(
            Optional.empty(),
            Optional.empty(),
            processId,
            Map.of(),
            activeTasks,
            standbyTasks,
            Map.of(),
            Map.of(),
            Map.of()
        );
    }

    private static TopologyDescriber statefulTopology(final int numTasks, final String... subtopologies) {
        return new UniformTopologyDescriber(numTasks, true, List.of(subtopologies));
    }

    private static TopologyDescriber statelessTopology(final int numTasks, final String... subtopologies) {
        return new UniformTopologyDescriber(numTasks, false, List.of(subtopologies));
    }

    private static int activeTaskCount(final GroupAssignment result, final String memberId) {
        final MemberAssignment member = result.members().get(memberId);
        assertNotNull(member);
        return member.activeTasks().values().stream().mapToInt(Set::size).sum();
    }

    private static List<Integer> sortedActiveTaskCounts(final GroupAssignment result, final String... memberIds) {
        return Stream.of(memberIds).map(memberId -> activeTaskCount(result, memberId)).sorted().toList();
    }

    /** The member holds exactly one partition of each of the given subtopologies and nothing else. */
    private static void assertOnePartitionOfEachSubtopology(final GroupAssignment result,
                                                            final String memberId,
                                                            final String... subtopologies) {
        final Map<String, Set<Integer>> activeTasks = result.members().get(memberId).activeTasks();
        assertEquals(Set.of(subtopologies), activeTasks.keySet(), memberId + " holds " + activeTasks);
        activeTasks.forEach((subtopology, partitions) ->
            assertEquals(1, partitions.size(), memberId + " holds " + partitions + " of " + subtopology));
    }

    private static int standbyTaskCount(final GroupAssignment result, final String memberId) {
        final MemberAssignment member = result.members().get(memberId);
        assertNotNull(member);
        return member.standbyTasks().values().stream().mapToInt(Set::size).sum();
    }

    private static Map<String, Set<Integer>> mergeTasks(final GroupAssignment result, final boolean active, final String... memberIds) {
        final Map<String, Set<Integer>> merged = new HashMap<>();
        for (final String memberId : memberIds) {
            final MemberAssignment member = result.members().get(memberId);
            assertNotNull(member);
            final Map<String, Set<Integer>> tasks = active ? member.activeTasks() : member.standbyTasks();
            tasks.forEach((subtopology, partitions) -> merged.computeIfAbsent(subtopology, s -> new HashSet<>()).addAll(partitions));
        }
        return merged;
    }

    private static void assertNoStandbyTasks(final GroupAssignment result) {
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            assertTrue(entry.getValue().standbyTasks().isEmpty(), entry.getKey() + " has standby tasks " + entry.getValue().standbyTasks());
        }
    }

    /** Every task of the topology is assigned as active task to exactly one member. */
    private static void assertAllTasksAssignedOnce(final GroupAssignment result, final TopologyDescriber topology) {
        final Map<TaskId, Integer> owners = new HashMap<>();
        for (final MemberAssignment member : result.members().values()) {
            member.activeTasks().forEach((subtopology, partitions) ->
                partitions.forEach(partition -> owners.merge(new TaskId(subtopology, partition), 1, Integer::sum)));
        }
        final Set<TaskId> expected = new HashSet<>();
        for (final String subtopology : topology.subtopologies()) {
            for (int partition = 0; partition < topology.maxNumInputPartitions(subtopology); partition++) {
                expected.add(new TaskId(subtopology, partition));
            }
        }
        assertEquals(expected, owners.keySet());
        owners.forEach((task, count) -> assertEquals(1, count, task + " is active on " + count + " members"));
    }

    /** Every stateful task has exactly the given number of standbys and no stateless task has any. */
    private static void assertStandbysAssigned(final GroupAssignment result, final TopologyDescriber topology, final int expectedStandbys) {
        final Map<TaskId, Integer> standbyCounts = new HashMap<>();
        for (final MemberAssignment member : result.members().values()) {
            member.standbyTasks().forEach((subtopology, partitions) ->
                partitions.forEach(partition -> standbyCounts.merge(new TaskId(subtopology, partition), 1, Integer::sum)));
        }
        final Map<TaskId, Integer> expected = new HashMap<>();
        if (expectedStandbys > 0) {
            for (final String subtopology : topology.subtopologies()) {
                if (topology.isStateful(subtopology)) {
                    for (int partition = 0; partition < topology.maxNumInputPartitions(subtopology); partition++) {
                        expected.put(new TaskId(subtopology, partition), expectedStandbys);
                    }
                }
            }
        }
        assertEquals(expected, standbyCounts);
        standbyCounts.keySet().forEach(task -> assertTrue(
            topology.isStateful(task.subtopologyId()), task + " is stateless but has standbys"));
    }

    private record UniformTopologyDescriber(int numTasks, boolean isStateful, List<String> subtopologies) implements TopologyDescriber {

        @Override
        public int maxNumInputPartitions(final String subtopologyId) throws NoSuchElementException {
            return numTasks;
        }

        @Override
        public boolean isStateful(final String subtopologyId) {
            return isStateful;
        }
    }

    /** One stateful subtopology followed by one stateless subtopology. */
    private record MixedTopologyDescriber(int numStatefulTasks, int numStatelessTasks) implements TopologyDescriber {

        @Override
        public List<String> subtopologies() {
            final List<String> subtopologies = new ArrayList<>();
            if (numStatefulTasks > 0) {
                subtopologies.add(SUBTOPOLOGY_1);
            }
            if (numStatelessTasks > 0) {
                subtopologies.add(SUBTOPOLOGY_2);
            }
            return subtopologies;
        }

        @Override
        public int maxNumInputPartitions(final String subtopologyId) throws NoSuchElementException {
            return subtopologyId.equals(SUBTOPOLOGY_1) ? numStatefulTasks : numStatelessTasks;
        }

        @Override
        public boolean isStateful(final String subtopologyId) {
            return subtopologyId.equals(SUBTOPOLOGY_1);
        }
    }
}
