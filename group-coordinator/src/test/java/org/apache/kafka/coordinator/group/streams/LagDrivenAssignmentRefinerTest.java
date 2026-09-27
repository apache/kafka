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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.coordinator.group.streams.topics.ConfiguredInternalTopic;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LagDrivenAssignmentRefinerTest {

    private static final String STATEFUL = "stateful-subtopology";
    private static final String STATELESS = "stateless-subtopology";
    private static final long ACCEPTABLE_RECOVERY_LAG = 100L;

    private final LagDrivenAssignmentRefiner refiner = new LagDrivenAssignmentRefiner();

    private static SortedMap<String, ConfiguredSubtopology> subtopologies() {
        final SortedMap<String, ConfiguredSubtopology> subtopologies = new TreeMap<>();
        subtopologies.put(STATEFUL, new ConfiguredSubtopology(
            4,
            Set.of("input"),
            Map.of(),
            Set.of(),
            Map.of("changelog", new ConfiguredInternalTopic("changelog", 4, Optional.empty(), Map.of()))
        ));
        subtopologies.put(STATELESS, new ConfiguredSubtopology(4, Set.of("input"), Map.of(), Set.of(), Map.of()));
        return subtopologies;
    }

    private static StreamsGroupMember member(
        final String memberId,
        final String processId,
        final TasksTuple assignedTasks
    ) {
        return member(memberId, processId, assignedTasks, TasksTuple.EMPTY);
    }

    private static StreamsGroupMember member(
        final String memberId,
        final String processId,
        final TasksTuple assignedTasks,
        final TasksTuple tasksPendingRevocation
    ) {
        return new StreamsGroupMember.Builder(memberId)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(1)
            .setState(MemberState.STABLE)
            .setProcessId(processId)
            .setRebalanceTimeoutMs(1500)
            .setTopologyEpoch(0)
            .setClientTags(Map.of())
            .setAssignedTasks(withEpochs(assignedTasks))
            .setTasksPendingRevocation(withEpochs(tasksPendingRevocation))
            .build();
    }

    private static TasksTupleWithEpochs withEpochs(final TasksTuple tasks) {
        final Map<String, Map<Integer, Integer>> activeWithEpochs = new HashMap<>();
        tasks.activeTasks().forEach((subtopologyId, partitionIds) -> {
            final Map<Integer, Integer> byPartition = new HashMap<>();
            partitionIds.forEach(partitionId -> byPartition.put(partitionId, 1));
            activeWithEpochs.put(subtopologyId, byPartition);
        });
        return new TasksTupleWithEpochs(activeWithEpochs, tasks.standbyTasks(), tasks.warmupTasks());
    }

    private static Map<String, MemberTaskOffsets> lag(
        final String memberId,
        final String subtopologyId,
        final int partitionId,
        final long lag
    ) {
        return Map.of(memberId, new MemberTaskOffsets(
            Map.of(subtopologyId, Map.of(partitionId, 1000L)),
            Map.of(subtopologyId, Map.of(partitionId, 1000L + lag))
        ));
    }

    private Map<String, TasksTuple> refine(
        final Map<String, StreamsGroupMember> members,
        final Map<String, TasksTuple> targetAssignment,
        final Map<String, MemberTaskOffsets> taskOffsets,
        final int numWarmupReplicas
    ) {
        return refiner.refine(
            members, targetAssignment, taskOffsets, subtopologies(), numWarmupReplicas, ACCEPTABLE_RECOVERY_LAG);
    }

    @Test
    public void shouldHandBackTargetAssignmentWhenNoTaskMoves() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2, 3))
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", targetAssignment.get("memberA")),
            "memberB", member("memberB", "processB", targetAssignment.get("memberB"))
        );

        // Nothing to derive, so the target assignment is handed back as-is rather than copied.
        assertSame(targetAssignment, refine(members, targetAssignment, Map.of(), 2));
    }

    @Test
    public void shouldWithholdMovingTaskAndWarmItUpOnItsNewOwner() {
        // The assignor moved 0_2 from memberA to memberB, which holds no state for it.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2, 3))
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1, 2))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 3)))
        );

        assertEquals(
            Map.of(
                "memberA", new TasksTuple(mkTasksMap(0, 1, 2), Map.of(), Map.of()),
                "memberB", new TasksTuple(mkTasksMap(3), Map.of(), mkTasksMap(2))
            ),
            refine(members, targetAssignment, Map.of(), 2)
        );
    }

    @Test
    public void shouldLetStatelessTaskMoveWithoutWarmingUp() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATELESS, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATELESS, 1))
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATELESS, 0, 1))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );

        assertSame(targetAssignment, refine(members, targetAssignment, Map.of(), 2));
    }

    @Test
    public void shouldLetTaskMoveWithinTheSameProcessWithoutWarmingUp() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "sharedProcess", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "sharedProcess", TasksTuple.EMPTY)
        );

        // The state directory is already on the process, so there is nothing to restore over the network.
        assertSame(targetAssignment, refine(members, targetAssignment, Map.of(), 2));
    }

    @Test
    public void shouldLetTaskMoveOnceItsNewOwnerHasCaughtUp() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 1)))
        );

        assertSame(
            targetAssignment,
            refine(members, targetAssignment, lag("memberB", STATEFUL, 1, ACCEPTABLE_RECOVERY_LAG), 2)
        );
    }

    @Test
    public void shouldKeepWithholdingWhileItsNewOwnerIsStillBehind() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 1)))
        );

        final Map<String, TasksTuple> refined = refine(
            members, targetAssignment, lag("memberB", STATEFUL, 1, ACCEPTABLE_RECOVERY_LAG + 1), 2);

        assertEquals(mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1)), refined.get("memberA"));
        assertEquals(mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 1)), refined.get("memberB"));
    }

    @Test
    public void shouldNotTreatUnknownOrCappedOffsetsAsCaughtUp() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 1)))
        );

        // An end-offset capped at MAX_VALUE would otherwise compute a hugely negative lag and read as caught up.
        final Map<String, MemberTaskOffsets> cappedEndOffset = Map.of("memberB", new MemberTaskOffsets(
            Map.of(STATEFUL, Map.of(1, 1000L)),
            Map.of(STATEFUL, Map.of(1, Long.MAX_VALUE))
        ));
        assertEquals(
            mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 1)),
            refine(members, targetAssignment, cappedEndOffset, 2).get("memberB")
        );

        // Only the end offset reported, so the lag is unknown.
        final Map<String, MemberTaskOffsets> endOffsetOnly = Map.of("memberB", new MemberTaskOffsets(
            Map.of(),
            Map.of(STATEFUL, Map.of(1, 1000L))
        ));
        assertEquals(
            mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 1)),
            refine(members, targetAssignment, endOffsetOnly, 2).get("memberB")
        );
    }

    @Test
    public void shouldNotWithholdTaskWhoseHandOverAlreadyStarted() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );
        // memberA was already told to revoke 0_1, so it is no longer running it -- taking it back now would leave the
        // task with nobody.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member(
                "memberA",
                "processA",
                mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1)),
                mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
            ),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 1)))
        );

        assertSame(targetAssignment, refine(members, targetAssignment, Map.of(), 2));
    }

    @Test
    public void shouldWithholdEveryMigrationButOnlyWarmUpAsManyAsTheBudgetAllows() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1, 2))
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1, 2))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );

        final Map<String, TasksTuple> refined = refine(members, targetAssignment, Map.of(), 1);

        // All three stay with memberA; only one of them is being warmed up, and which one is stable across steps
        // because migrations are ordered by task ID.
        assertEquals(mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1, 2)), refined.get("memberA"));
        assertEquals(mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0)), refined.get("memberB"));
    }

    @Test
    public void shouldDeferStandbyThatWouldCollideWithARoleHandedOutByTheRefinement() {
        // The target assignment moves 0_1 to memberB and gives memberA the standby for it. Both of those cannot hold
        // while memberA keeps running 0_1 as active and memberB warms it up.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", new TasksTuple(mkTasksMap(0), mkTasksMap(1), Map.of()),
            "memberB", new TasksTuple(mkTasksMap(1), mkTasksMap(0), Map.of())
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );

        final Map<String, TasksTuple> refined = refine(members, targetAssignment, Map.of(), 2);

        // memberA keeps 0_1 active, so its standby for it is dropped; memberB warms 0_1 up, so its standby for 0_0 --
        // which its process does not otherwise hold -- survives.
        assertEquals(new TasksTuple(mkTasksMap(0, 1), Map.of(), Map.of()), refined.get("memberA"));
        assertEquals(new TasksTuple(Map.of(), mkTasksMap(0), mkTasksMap(1)), refined.get("memberB"));
    }

    @Test
    public void shouldNeverLoseAnActiveTaskOrGiveOneMemberTwoRolesForIt() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", new TasksTuple(mkTasksMap(0, 1), mkTasksMap(2, 3), Map.of()),
            "memberB", new TasksTuple(mkTasksMap(2, 3), mkTasksMap(0, 1), Map.of())
        );
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1, 2, 3))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );

        final Map<String, TasksTuple> refined = refine(members, targetAssignment, Map.of(), 4);

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refined));
        refined.forEach((memberId, tasks) -> {
            assertTrue(disjoint(tasks.activeTasks(), tasks.standbyTasks()), memberId + " active/standby overlap");
            assertTrue(disjoint(tasks.activeTasks(), tasks.warmupTasks()), memberId + " active/warm-up overlap");
            assertTrue(disjoint(tasks.standbyTasks(), tasks.warmupTasks()), memberId + " standby/warm-up overlap");
        });
    }

    private static boolean disjoint(
        final Map<String, Set<Integer>> left,
        final Map<String, Set<Integer>> right
    ) {
        return left.entrySet().stream().noneMatch(entry ->
            right.getOrDefault(entry.getKey(), Set.of()).stream().anyMatch(entry.getValue()::contains));
    }

    private static Map<String, Set<Integer>> mkTasksMap(final Integer... partitionIds) {
        return Map.of(STATEFUL, Set.of(partitionIds));
    }
}
