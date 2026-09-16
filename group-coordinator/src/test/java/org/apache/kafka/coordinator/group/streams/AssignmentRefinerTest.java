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

import org.apache.kafka.coordinator.group.streams.assignor.TaskId;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredInternalTopic;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AssignmentRefinerTest {

    private static final String STATEFUL = "stateful-subtopology";
    private static final String STATELESS = "stateless-subtopology";
    private static final long ACCEPTABLE_RECOVERY_LAG = 100L;
    private static final TaskId STATEFUL_0 = new TaskId(STATEFUL, 0);
    private static final TaskId STATEFUL_1 = new TaskId(STATEFUL, 1);
    private static final TaskId STATEFUL_2 = new TaskId(STATEFUL, 2);
    private static final TaskId STATEFUL_3 = new TaskId(STATEFUL, 3);

    private static TasksTuple active(final Map<String, Set<Integer>> activeTasks) {
        return new TasksTuple(activeTasks, Map.of(), Map.of());
    }

    @Test
    public void shouldPreserveActiveTaskCountOfUnchangedAssignment() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of("0", Set.of(2)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, targetAssignment));
    }

    @Test
    public void shouldPreserveActiveTaskCountWhenATaskIsHeldBackWithItsCurrentOwner() {
        // What a refinement step does to stage a migration: the target assignment moves 0_2 to memberB, the refined
        // assignment leaves it with memberA while memberB warms it up. The active tasks themselves are unchanged.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of("0", Set.of(2)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1, 2))),
            "memberB", new TasksTuple(Map.of(), Map.of(), Map.of("0", Set.of(2)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldPreserveActiveTaskCountWhenStandbysAreDeferred() {
        // A refinement step may defer a standby to a later step, which is not a defect this check is about.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", new TasksTuple(Map.of("0", Set.of(0)), Map.of("0", Set.of(1)), Map.of()),
            "memberB", new TasksTuple(Map.of("0", Set.of(1)), Map.of("0", Set.of(0)), Map.of())
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0))),
            "memberB", active(Map.of("0", Set.of(1)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotPreserveActiveTaskCountWhenATaskWasDropped() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of("0", Set.of(2)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of())
        );

        assertFalse(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotPreserveActiveTaskCountWhenASubtopologyWasDroppedEntirely() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0), "1", Set.of(0)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0)))
        );

        assertFalse(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotPreserveActiveTaskCountWhenATaskWasHandedToTwoMembers() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0))),
            "memberB", active(Map.of("0", Set.of(1)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1))),
            "memberB", active(Map.of("0", Set.of(1)))
        );

        assertFalse(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotPreserveActiveTaskCountWhenATaskWasInvented() {
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0, 1)))
        );

        assertFalse(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldNotDetectADropAndADuplicateCancellingEachOtherOut() {
        // The accepted blind spot of counting: 0_0 was dropped and 0_1 handed to both members, so the count still
        // matches. It takes two coordinated mistakes in one derivation, and the exhaustive invariant is covered by the
        // refiner's own tests. If this check is ever strengthened, this test is what should fail.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0))),
            "memberB", active(Map.of("0", Set.of(1)))
        );
        final Map<String, TasksTuple> refinedAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(1))),
            "memberB", active(Map.of("0", Set.of(1)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refinedAssignment));
    }

    @Test
    public void shouldPreserveActiveTaskCountWhenTheTargetAssignmentItselfDuplicatesATask() {
        // A target assignment that places an active task twice is the assignor's defect, not the refinement's, so a
        // refinement that keeps it as-is is not blamed for it.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", active(Map.of("0", Set.of(0))),
            "memberB", active(Map.of("0", Set.of(0)))
        );

        assertTrue(AssignmentRefiner.preservesActiveTaskCount(targetAssignment, targetAssignment));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // isCaughtUp
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void shouldBeCaughtUpWhenTheLagIsWithinTheAcceptableRecoveryLag() {
        assertTrue(AssignmentRefinerImpl.isCaughtUp(offsets(1000L, 1050L), STATEFUL_0, ACCEPTABLE_RECOVERY_LAG));
    }

    @Test
    public void shouldBeCaughtUpWhenTheLagIsExactlyTheAcceptableRecoveryLag() {
        assertTrue(AssignmentRefinerImpl.isCaughtUp(offsets(1000L, 1100L), STATEFUL_0, ACCEPTABLE_RECOVERY_LAG));
    }

    @Test
    public void shouldNotBeCaughtUpWhenTheLagExceedsTheAcceptableRecoveryLag() {
        assertFalse(AssignmentRefinerImpl.isCaughtUp(offsets(1000L, 1101L), STATEFUL_0, ACCEPTABLE_RECOVERY_LAG));
    }

    @Test
    public void shouldBeCaughtUpWhenTheLagIsNegative() {
        // The offset is a position while the end offset is the last offset, so a fully restored task reports -1. That
        // is more than caught up, not a malformed report.
        assertTrue(AssignmentRefinerImpl.isCaughtUp(offsets(1000L, 999L), STATEFUL_0, ACCEPTABLE_RECOVERY_LAG));
    }

    @Test
    public void shouldNotBeCaughtUpWhenNothingWasReported() {
        assertFalse(AssignmentRefinerImpl.isCaughtUp(MemberTaskOffsets.EMPTY, STATEFUL_0, ACCEPTABLE_RECOVERY_LAG));
    }

    @Test
    public void shouldNotBeCaughtUpWhenOnlyTheEndOffsetWasReported() {
        final MemberTaskOffsets memberTaskOffsets = new MemberTaskOffsets(
            Map.of(),
            Map.of(STATEFUL, Map.of(0, 1000L))
        );

        assertFalse(AssignmentRefinerImpl.isCaughtUp(memberTaskOffsets, STATEFUL_0, ACCEPTABLE_RECOVERY_LAG));
    }

    @Test
    public void shouldNotBeCaughtUpWhenOnlyTheOffsetWasReported() {
        final MemberTaskOffsets memberTaskOffsets = new MemberTaskOffsets(
            Map.of(STATEFUL, Map.of(0, 1000L)),
            Map.of()
        );

        assertFalse(AssignmentRefinerImpl.isCaughtUp(memberTaskOffsets, STATEFUL_0, ACCEPTABLE_RECOVERY_LAG));
    }

    @Test
    public void shouldNotBeCaughtUpWhenTheRestoreHasNotStarted() {
        // Long.MAX_VALUE is the client's way of saying that it has not started restoring the task, so no lag can be
        // computed from it.
        assertFalse(AssignmentRefinerImpl.isCaughtUp(offsets(Long.MAX_VALUE, 1000L), STATEFUL_0, ACCEPTABLE_RECOVERY_LAG));
        assertFalse(AssignmentRefinerImpl.isCaughtUp(offsets(1000L, Long.MAX_VALUE), STATEFUL_0, ACCEPTABLE_RECOVERY_LAG));
    }

    @Test
    public void shouldNotBeCaughtUpWhenAnotherTaskWasReported() {
        assertFalse(AssignmentRefinerImpl.isCaughtUp(
            offsets(1000L, 1000L),
            new TaskId(STATEFUL, 1),
            ACCEPTABLE_RECOVERY_LAG
        ));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // indexCurrentAssignment
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void shouldIndexTheMemberProcessingATaskAsItsActiveHolder() {
        // No offsets reported for an active task means the restore has finished and the member is processing it.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1)))
        );

        final AssignmentRefinerImpl.CurrentAssignmentIndex index = index(members, Map.of());

        assertEquals(
            Map.of(
                STATEFUL_0, new AssignmentRefinerImpl.ActiveHolder("memberA", true),
                new TaskId(STATEFUL, 1), new AssignmentRefinerImpl.ActiveHolder("memberA", true)
            ),
            index.activeHolder()
        );
        assertEquals(Map.of(), index.taskCopies());
    }

    @Test
    public void shouldIndexAnActiveHolderThatIsStillRestoringAsNotProcessing() {
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)))
        );

        final AssignmentRefinerImpl.CurrentAssignmentIndex index =
            index(members, Map.of("memberA", offsets(500L, 10_000L)));

        assertEquals(
            Map.of(STATEFUL_0, new AssignmentRefinerImpl.ActiveHolder("memberA", false)),
            index.activeHolder()
        );
    }

    @Test
    public void shouldIndexAnActiveHolderWhoseRestoreHasNotStartedAsNotProcessing() {
        // Long.MAX_VALUE is the "restore not started" cap; it is still a report, so the task is still restoring.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)))
        );

        final AssignmentRefinerImpl.CurrentAssignmentIndex index =
            index(members, Map.of("memberA", offsets(Long.MAX_VALUE, Long.MAX_VALUE)));

        assertEquals(
            Map.of(STATEFUL_0, new AssignmentRefinerImpl.ActiveHolder("memberA", false)),
            index.activeHolder()
        );
    }

    @Test
    public void shouldNotIndexAnActiveHolderForATaskThatIsPendingRevocation() {
        // The member was told to give the task up, so it is not part of the current assignment to preserve, even
        // though it may still be running physically. The process it still occupies needs no tracking either: the
        // reconciler blocks a colliding placement until the revocation lands.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member(
                "memberA",
                "processA",
                TasksTuple.EMPTY,
                mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
            )
        );

        final AssignmentRefinerImpl.CurrentAssignmentIndex index = index(members, Map.of());

        assertEquals(Map.of(), index.activeHolder());
        assertEquals(Map.of(), index.taskCopies());
    }

    @Test
    public void shouldIndexStandbyAndWarmupHoldersWithWhetherTheyAreCaughtUp() {
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0)))
        );
        final Map<String, MemberTaskOffsets> taskOffsets = Map.of(
            "memberA", offsets(1000L, 1000L),
            "memberB", offsets(0L, 10_000L)
        );

        final AssignmentRefinerImpl.CurrentAssignmentIndex index = index(members, taskOffsets);

        assertEquals(Map.of(), index.activeHolder());
        assertEquals(
            Set.of(
                new AssignmentRefinerImpl.TaskCopy("memberA", "processA", TaskRole.STANDBY, true),
                new AssignmentRefinerImpl.TaskCopy("memberB", "processB", TaskRole.WARMUP, false)
            ),
            Set.copyOf(index.taskCopies().get(STATEFUL_0))
        );
    }

    @Test
    public void shouldNotIndexStatelessTasks() {
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATELESS, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATELESS, 0)))
        );

        final AssignmentRefinerImpl.CurrentAssignmentIndex index = index(members, Map.of());

        assertEquals(Map.of(), index.activeHolder());
        assertEquals(Map.of(), index.taskCopies());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // analyzeTasks
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void shouldDecideNothingWhenEveryTaskAlreadyRunsWhereItBelongs() {
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.stagedMigrations());
        assertEquals(List.of(), decisions.grantedTasks());
    }

    @Test
    public void shouldStageAMigrationToAMemberThatHoldsNoState() {
        // The headline case: a scale-out moves a stateful task to a cold member, so the task keeps running where it is
        // while the new owner restores it.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.grantedTasks());
        assertEquals(
            List.of(new AssignmentRefinerImpl.StagedMigration(
                STATEFUL_0,
                "memberA",
                "memberB",
                Optional.of("processB"),
                Optional.empty()
            )),
            decisions.stagedMigrations()
        );
    }

    @Test
    public void shouldGrantATaskWhoseHolderIsStillRestoringItRatherThanStageIt() {
        // Nothing is being processed, so staging would protect nothing: it would keep the task on a member that cannot
        // run it, throw that restore away when the migration completes, and possibly spend a warm-up slot on the way.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );
        // memberA reports restore progress for the active task, so it is not processing it; memberB's standby is a
        // long way behind, so without the restoring check this would stage and wait for it.
        final Map<String, MemberTaskOffsets> taskOffsets = Map.of(
            "memberA", offsets(500L, 10_000L),
            "memberB", offsets(0L, 10_000L)
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, taskOffsets);

        assertEquals(List.of(new AssignmentRefinerImpl.TaskGrant(STATEFUL_0, "memberB")), decisions.grantedTasks());
        assertEquals(List.of(), decisions.stagedMigrations());
    }

    @Test
    public void shouldGrantATaskWhoseHolderIsStillRestoringItEvenToAColdMember() {
        // The refiner does not weigh how far along the two members are: choosing the better-placed candidate is a
        // placement decision, so the assignor's choice stands even though it holds no state at all.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions =
            analyze(members, targetAssignment, Map.of("memberA", offsets(9_999L, 10_000L)));

        assertEquals(List.of(new AssignmentRefinerImpl.TaskGrant(STATEFUL_0, "memberB")), decisions.grantedTasks());
        assertEquals(List.of(), decisions.stagedMigrations());
    }

    @Test
    public void shouldDecideNothingWhenTheTaskIsAlreadyWithItsTargetOwnerButStillRestoring() {
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions =
            analyze(members, targetAssignment, Map.of("memberA", offsets(500L, 10_000L)));

        assertEquals(List.of(), decisions.grantedTasks());
        assertEquals(List.of(), decisions.stagedMigrations());
    }

    @Test
    public void shouldHoldATaskWithAHolderThatIsStillRestoringItWhenItsTargetOwnerIsGone() {
        // Leaving it out would make the holder revoke it and discard a restore that is part-way through, for no gain:
        // the task has nowhere else to go until the assignor names a member that still exists.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "departedMember", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions =
            analyze(members, targetAssignment, Map.of("memberA", offsets(500L, 10_000L)));

        assertEquals(List.of(), decisions.grantedTasks());
        assertEquals(
            List.of(new AssignmentRefinerImpl.StagedMigration(
                STATEFUL_0,
                "memberA",
                "departedMember",
                Optional.empty(),
                Optional.empty()
            )),
            decisions.stagedMigrations()
        );
    }

    @Test
    public void shouldStageAMigrationWhenTheHolderHasNotReportedAnyOffsetsYet() {
        // A member the coordinator has not heard from -- every member right after a failover -- reports nothing, so its
        // active tasks read as processing. That is the safe direction: we stage rather than hand the task over.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.grantedTasks());
        assertEquals(
            List.of(new AssignmentRefinerImpl.StagedMigration(
                STATEFUL_0,
                "memberA",
                "memberB",
                Optional.of("processB"),
                Optional.empty()
            )),
            decisions.stagedMigrations()
        );
    }

    @Test
    public void shouldGrantTheTaskWhenTheTargetOwnerAlreadyHoldsACaughtUpReplica() {
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(
            members,
            targetAssignment,
            Map.of("memberB", offsets(1000L, 1000L))
        );

        assertEquals(List.of(), decisions.stagedMigrations());
        assertEquals(
            List.of(new AssignmentRefinerImpl.TaskGrant(STATEFUL_0, "memberB")),
            decisions.grantedTasks()
        );
    }

    @Test
    public void shouldStageWhenTheReplicaOnTheTargetProcessIsNotCaughtUpYet() {
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(
            members,
            targetAssignment,
            Map.of("memberB", offsets(0L, 10_000L))
        );

        assertEquals(List.of(), decisions.grantedTasks());
        assertEquals(
            List.of(new AssignmentRefinerImpl.StagedMigration(
                STATEFUL_0,
                "memberA",
                "memberB",
                Optional.of("processB"),
                Optional.of(new AssignmentRefinerImpl.TaskCopy("memberB", "processB", TaskRole.STANDBY, false))
            )),
            decisions.stagedMigrations()
        );
    }

    @Test
    public void shouldGrantTheTaskWhenASiblingOnTheTargetProcessHoldsACaughtUpReplica() {
        // The sibling has to close the task before the target owner can open it, so the hand-over goes through the
        // checkpointed state directory rather than a network restore. Planting a warm-up task on the target owner is
        // not possible anyway, because its process already holds the task.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY),
            "memberC", member("memberC", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", TasksTuple.EMPTY
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(
            members,
            targetAssignment,
            Map.of("memberC", offsets(1000L, 1000L))
        );

        assertEquals(List.of(), decisions.stagedMigrations());
        assertEquals(
            List.of(new AssignmentRefinerImpl.TaskGrant(STATEFUL_0, "memberB")),
            decisions.grantedTasks()
        );
    }

    @Test
    public void shouldGrantTheTaskWhenItMovesBetweenTwoMembersOfOneProcess() {
        // A process must not hold the same task twice, so there is no way to warm the new owner up first. Staging such
        // a move would park it forever.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processA", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.stagedMigrations());
        assertEquals(
            List.of(new AssignmentRefinerImpl.TaskGrant(STATEFUL_0, "memberB")),
            decisions.grantedTasks()
        );
    }

    @Test
    public void shouldGrantATaskNobodyRunsEvenToAColdMember() {
        // There is no running task to protect, and choosing a warmer owner instead would be a placement decision,
        // which belongs to the assignor.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.stagedMigrations());
        assertEquals(
            List.of(new AssignmentRefinerImpl.TaskGrant(STATEFUL_0, "memberA")),
            decisions.grantedTasks()
        );
    }

    @Test
    public void shouldGrantATaskWhosePreviousOwnerWasAlreadyToldToRevokeIt() {
        // The hand-over is in flight: holding the task back now would take it away from a member that has already
        // stopped running it.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member(
                "memberA",
                "processA",
                TasksTuple.EMPTY,
                mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
            ),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.stagedMigrations());
        assertEquals(
            List.of(new AssignmentRefinerImpl.TaskGrant(STATEFUL_0, "memberB")),
            decisions.grantedTasks()
        );
    }

    @Test
    public void shouldDecideNothingForATaskTheTargetAssignmentNoLongerContains() {
        // A topology change removed the task. It belongs in nobody's slice, and its current holder revokes it the
        // ordinary way.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of("memberA", TasksTuple.EMPTY);

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.stagedMigrations());
        assertEquals(List.of(), decisions.grantedTasks());
    }

    @Test
    public void shouldNotStageStatelessTasks() {
        // A stateless task has nothing to restore, so the reconciler's ordinary revoke-and-grant is all its move needs.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATELESS, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATELESS, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.stagedMigrations());
        assertEquals(List.of(), decisions.grantedTasks());
    }

    @Test
    public void shouldReportAnInFlightWarmupOnTheTargetProcess() {
        // The migration is already under way from an earlier step: the target owner holds a warm-up task that has not
        // caught up yet. It stays staged, and the warm-up task is reported so that the budget pass keeps it rather than
        // planting a second one.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(
            List.of(new AssignmentRefinerImpl.StagedMigration(
                STATEFUL_0,
                "memberA",
                "memberB",
                Optional.of("processB"),
                Optional.of(new AssignmentRefinerImpl.TaskCopy("memberB", "processB", TaskRole.WARMUP, false))
            )),
            decisions.stagedMigrations()
        );
    }

    @Test
    public void shouldHoldATaskWhereItRunsWhenItsTargetOwnerIsGone() {
        // The target assignment still names a member the group has removed, because the assignor has not run again
        // yet. Nothing can be staged into that member, so the task stays where it is -- and the empty target process
        // tells the budget pass not to spend a slot on it.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "goneMember", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.grantedTasks());
        assertEquals(
            List.of(new AssignmentRefinerImpl.StagedMigration(
                STATEFUL_0,
                "memberA",
                "goneMember",
                Optional.empty(),
                Optional.empty()
            )),
            decisions.stagedMigrations()
        );
    }

    @Test
    public void shouldDecideNothingForAnUnownedTaskWhoseTargetOwnerIsGone() {
        // Granting it to a member that is no longer in the group would achieve nothing; the next assignor run places
        // the task somewhere real.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "goneMember", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(List.of(), decisions.stagedMigrations());
        assertEquals(List.of(), decisions.grantedTasks());
    }

    @Test
    public void shouldReturnDecisionsInCanonicalTaskOrder() {
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2, 0, 1))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2, 0, 1))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(members, targetAssignment, Map.of());

        assertEquals(
            List.of(STATEFUL_0, new TaskId(STATEFUL, 1), new TaskId(STATEFUL, 2)),
            decisions.stagedMigrations().stream().map(AssignmentRefinerImpl.StagedMigration::task).toList()
        );
    }

    @Test
    public void shouldDecideEachDivergingTaskExactlyOnce() {
        // The completeness invariant: every stateful task that is not already in place is either staged or granted,
        // never both and never neither.
        final Map<String, StreamsGroupMember> members = Map.of(
            // 0 stays put, 1 migrates to a cold member, 2 migrates to a caught-up member, 3 is unowned.
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1, 2))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 2)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1, 2, 3))
        );

        final AssignmentRefinerImpl.TaskDecisions decisions = analyze(
            members,
            targetAssignment,
            Map.of("memberB", offsets(2, 1000L, 1000L))
        );

        assertEquals(
            List.of(new TaskId(STATEFUL, 1)),
            decisions.stagedMigrations().stream().map(AssignmentRefinerImpl.StagedMigration::task).toList()
        );
        assertEquals(
            List.of(new TaskId(STATEFUL, 2), new TaskId(STATEFUL, 3)),
            decisions.grantedTasks().stream().map(AssignmentRefinerImpl.TaskGrant::task).toList()
        );
    }

    @Test
    public void shouldCountStatefulTasksOfEveryRoleTowardsProcessLoad() {
        // All three roles occupy the process: a standby and a warm-up read the changelog just as a restoring active
        // does, so a process full of replicas is not a good place to start another restore.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", new TasksTuple(
                Map.of(STATEFUL, Set.of(0)),
                Map.of(STATEFUL, Set.of(1)),
                Map.of(STATEFUL, Set.of(2))
            ))
        );

        assertEquals(
            Map.of("processA", new AssignmentRefinerImpl.ProcessLoad(3, 1)),
            load(members)
        );
    }

    @Test
    public void shouldNotCountStatelessTasksTowardsProcessLoad() {
        // A stateless task has no changelog, so it competes for nothing a warm-up needs. Counting it would rank a
        // process busy with work that does not compete as though it were a poor place to restore.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", new TasksTuple(
                Map.of(STATEFUL, Set.of(0), STATELESS, Set.of(0, 1, 2)),
                Map.of(),
                Map.of()
            ))
        );

        assertEquals(
            Map.of("processA", new AssignmentRefinerImpl.ProcessLoad(1, 1)),
            load(members)
        );
    }

    @Test
    public void shouldDivideProcessLoadByTheNumberOfMembersTheProcessRuns() {
        // Each member is one stream thread, so two members carrying four tasks between them are half as loaded as
        // one member carrying four.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA1", member("memberA1", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberA2", member("memberA2", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2, 3)))
        );

        assertEquals(
            Map.of("processA", new AssignmentRefinerImpl.ProcessLoad(4, 2)),
            load(members)
        );
    }

    @Test
    public void shouldNotCountTasksPendingRevocationTowardsProcessLoad() {
        // A task on its way out would overstate the load the process is about to carry, and the rest of the
        // derivation reads the granted half only.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member(
                "memberA",
                "processA",
                mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
                mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
            )
        );

        assertEquals(
            Map.of("processA", new AssignmentRefinerImpl.ProcessLoad(1, 1)),
            load(members)
        );
    }

    @Test
    public void shouldPlantAWarmupOnTheTargetOwnerOfAStagedMigration() {
        // The headline case: a scale-out stages the migration, and the budget funds a warm-up on the member the task
        // is moving to, so that it can be promoted in place once it has caught up.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_0, "memberB"), plan.warmupTasks());
        assertEquals(Set.of(), plan.borrowedMigrations());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldKeepFundingAWarmupThatIsAlreadyRestoring() {
        // Dropping a restore part-way through to start another one elsewhere would throw away the very work the
        // budget exists to buy, so a warm-up in flight keeps its slot.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_0, "memberB"), plan.warmupTasks());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldBorrowAStandbyOnTheTargetOwnerItselfInsteadOfSpendingASlot() {
        // The standby warms the migration as a side effect of being a standby, and is the very replica the promotion
        // then takes over in place. The target assignment must be relocating it elsewhere, so withholding that
        // relocation leaves the replica count where the target wants it.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_0), plan.borrowedMigrations());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldSpendASlotMovingAStandbyOnASiblingOntoTheTargetOwner() {
        // Borrowing where it sits would warm the migration but hand the task over through the sibling, which loses an
        // in-memory store: the sibling has to release the task before the target owner can hold anything, and only a
        // store that persists to disk survives that release. Moving the copy across pays the cost during warming
        // instead, where it merely delays convergence, and buys an in-place promotion for every store type.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB1", member("memberB1", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberB2", member("memberB2", "processB", TasksTuple.EMPTY)
        );
        // The assignor hands the active to memberB2, while the standby that could have warmed it sits on its sibling.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB1", TasksTuple.EMPTY,
            "memberB2", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_0, "memberB2"), plan.warmupTasks());
        assertEquals(Set.of(), plan.borrowedMigrations());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldBorrowASiblingStandbyWhereItSitsWhenTheBudgetCannotMoveIt() {
        // Warming through the sibling is worth more than not warming at all, and is what the migration would have
        // done anyway had the slot never been on offer. So such a candidate settles for the borrow rather than
        // parking -- unlike a migration whose destination process holds nothing, which has nothing to fall back on.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB1", member("memberB1", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberB2", member("memberB2", "processB", TasksTuple.EMPTY),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB1", TasksTuple.EMPTY,
            "memberB2", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );

        // processC is empty while processB already carries the standby, so the single slot goes to processC and the
        // sibling case is the one left unfunded.
        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_1, "memberC"), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_0), plan.borrowedMigrations());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldFundAFreshPlantAheadOfASiblingMoveEvenOnAHeavierProcess() {
        // A plant and a sibling move both cost a slot, but a plant that misses out parks (no progress at all) while
        // a sibling move that misses out still warms through the sibling it falls back to. So a plant takes a scarce
        // slot first -- even when its destination is more loaded than the sibling move's, since warming category outranks load.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            // processHeavy already runs two actives, so it is the more loaded destination (2 / 1 member = 2.0).
            "memberH", member("memberH", "processHeavy", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2, 3))),
            // processLight carries only the sibling standby, spread over two members (1 / 2 members = 0.5).
            "memberL1", member("memberL1", "processLight", TasksTuple.EMPTY),
            "memberL2", member("memberL2", "processLight", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 1)))
        );
        // STATEFUL_0 moves to the heavy process, which holds no copy of it -> a fresh plant. STATEFUL_1 moves to the
        // light process, whose sibling holds a not-caught-up standby -> a sibling move.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberH", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 2, 3)),
            "memberL1", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1)),
            "memberL2", TasksTuple.EMPTY
        );

        // One slot: under a load-only order the lighter processLight sibling move would win it; warming-first gives it
        // to the plant on the heavier processHeavy, and the sibling move falls back to a borrow.
        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_0, "memberH"), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_1), plan.borrowedMigrations());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldCountWarmupsFundedThisPassInTheSourceProcessLoad() {
        // The source-load tie-break is read live, not precomputed: a process can be one migration's source and
        // another's target, so funding a warm-up onto it mid-pass raises its load. Here processP is STATEFUL_1's
        // source and STATEFUL_2's target. STATEFUL_2 funds first (its target processP is the least loaded), which
        // raises processP's load; then STATEFUL_0 and STATEFUL_1 tie on warming and on target load (both go to
        // processR) and split on source load -- STATEFUL_1's source processP is now heavier than STATEFUL_0's
        // source processQ, so STATEFUL_1 takes the last slot. A precomputed (static) source load would leave the two
        // tied and hand the slot to STATEFUL_0 on the task-id fallback, so this pins the live reading.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberP1", member("memberP1", "processP", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))),
            "memberP2", member("memberP2", "processP", TasksTuple.EMPTY),
            "memberQ1", member("memberQ1", "processQ", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberQ2", member("memberQ2", "processQ", TasksTuple.EMPTY),
            "memberR", member("memberR", "processR", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 3))),
            "memberS", member("memberS", "processS", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2)))
        );
        // processP and processQ both start at load 0.5 (one active over two members); processR is the busier
        // destination for STATEFUL_0/1, so STATEFUL_2 -> processP is funded first.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberP1", TasksTuple.EMPTY,
            "memberP2", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2)),
            "memberQ1", TasksTuple.EMPTY,
            "memberR", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1, 3)),
            "memberS", TasksTuple.EMPTY
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 2);

        assertEquals(Map.of(STATEFUL_2, "memberP2", STATEFUL_1, "memberR"), plan.warmupTasks());
        assertEquals(Set.of(), plan.borrowedMigrations());
        assertEquals(Set.of(STATEFUL_0), plan.parkedMigrations());
    }

    @Test
    public void shouldNotFundAMigrationWhoseTargetMemberHasLeftTheGroup() {
        // Such a member cannot restore anything, so no slot may be spent on it. The task waits with its current owner
        // until the assignor names a member that still exists.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberGone", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_0), plan.parkedMigrations());
    }

    @Test
    public void shouldParkTheMigrationsTheBudgetCannotCover() {
        // Parking is never destructive: the task keeps running on its current owner and a later step picks it up once
        // a slot frees.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_0, "memberB"), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_1), plan.parkedMigrations());
    }

    @Test
    public void shouldNotFundAWarmupWhoseTaskWasReTargetedToAnotherProcess() {
        // The budget is recounted from zero every pass, so the stale warm-up does not go on holding a slot it no
        // longer earns: it is dropped and the plant on the new destination is funded in the very same pass.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0))),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        // The assignor has since re-targeted the task to a third process, which makes memberB's restore worthless.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", TasksTuple.EMPTY,
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_0, "memberC"), plan.warmupTasks());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldRelabelAWarmupOntoTheNewTargetOwnerWithinTheSameProcess() {
        // A warm-up exists only to be promoted on the member the task is moving to, so when the assignor re-targets
        // the active to a sibling member it has to follow. Leaving it behind would force the promotion through the
        // sibling-release path instead, which loses an in-memory store's state entirely.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB1", member("memberB1", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0))),
            "memberB2", member("memberB2", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB1", TasksTuple.EMPTY,
            "memberB2", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_0, "memberB2"), plan.warmupTasks());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldFreeTheSlotOfAWarmupWhoseTaskIsGrantedInTheSameStep() {
        // What justifies keeping a warm-up is a still-staged migration, not merely the task still being in the target
        // assignment: the moment its migration completes, the slot has to fund the next queued plant.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0))),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );
        // memberB's warm-up has caught up, so its migration is granted rather than staged this step.
        final Map<String, MemberTaskOffsets> taskOffsets = Map.of("memberB", offsets(9_950L, 10_000L));

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, taskOffsets, 1);

        assertEquals(Map.of(STATEFUL_1, "memberC"), plan.warmupTasks());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldFundTheMigrationWithTheLeastLoadedDestinationFirst() {
        // A lightly loaded destination restores faster, so its slot recycles sooner. Note the canonical order would
        // have picked the other migration, so this is the destination key deciding.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 2, 3))),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_1, "memberC"), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_0), plan.parkedMigrations());
    }

    @Test
    public void shouldFundTheMigrationRelievingTheBusierSourceWhenDestinationsTie() {
        // Of two migrations that could be funded, the one that relieves the busier process is worth more. The
        // canonical order would have picked the other one, so this is the source key deciding.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1, 2, 3))),
            "memberD", member("memberD", "processD", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1, 2)),
            "memberD", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 3))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_3, "memberC"), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_0), plan.parkedMigrations());
    }

    @Test
    public void shouldBreakAFullTieOnTheCanonicalTaskOrder() {
        // Same source, equally idle destinations: nothing distinguishes the two migrations, so the task order decides
        // purely so that the same inputs always produce the same assignment.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1)),
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_0, "memberC"), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_1), plan.parkedMigrations());
    }

    @Test
    public void shouldSpreadConcurrentPlantsAcrossProcessesRatherThanStackingThem() {
        // Each plant funded raises its destination's load before the next pick, so the second slot goes to the process
        // that is now lighter. Sorting once instead would have stacked both plants onto processB.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1, 2))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY),
            "memberC1", member("memberC1", "processC", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 3))),
            "memberC2", member("memberC2", "processC", TasksTuple.EMPTY)
        );
        // processB starts at 0 and processC at 0.5, so processB wins the first plant and is then at 1.0 -- above
        // processC, which therefore takes the second.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1)),
            "memberC1", TasksTuple.EMPTY,
            "memberC2", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 2);

        assertEquals(Map.of(STATEFUL_0, "memberB", STATEFUL_2, "memberC2"), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_1), plan.parkedMigrations());
    }

    @Test
    public void shouldNotCountAFundedSiblingMoveTowardsItsTargetProcessLoad() {
        // A sibling move relocates a copy within the destination process, so the process runs no more stateful tasks
        // after it than before, and its load -- which counts that copy where it sits today -- stays put. Raising it
        // would hand the next slot to a process that is in fact the busier one.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1, 2))),
            // processX spreads the two standbys that warm STATEFUL_0 and STATEFUL_1 over four members: 2 / 4 = 0.5.
            "memberX1", member("memberX1", "processX", TasksTuple.EMPTY),
            "memberX2", member("memberX2", "processX", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0, 1))),
            "memberX3", member("memberX3", "processX", TasksTuple.EMPTY),
            "memberX4", member("memberX4", "processX", TasksTuple.EMPTY),
            // processY carries the standby that warms STATEFUL_2 and the active of STATEFUL_3: 2 / 3 = 0.67.
            "memberY1", member("memberY1", "processY", TasksTuple.EMPTY),
            "memberY2", member("memberY2", "processY", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 2))),
            "memberY3", member("memberY3", "processY", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 3)))
        );
        // All three migrations are sibling moves: each destination process holds the task on a member next to its
        // target owner. STATEFUL_3 already runs where it belongs.
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberX1", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1)),
            "memberX2", TasksTuple.EMPTY,
            "memberX3", TasksTuple.EMPTY,
            "memberX4", TasksTuple.EMPTY,
            "memberY1", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 2)),
            "memberY2", TasksTuple.EMPTY,
            "memberY3", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 3))
        );

        // Two slots, and processX is the lighter destination for both of its migrations, so it takes both. Counting
        // the first move against processX would lift it to 0.75 and give the second slot to the heavier processY.
        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 2);

        assertEquals(Map.of(STATEFUL_0, "memberX1", STATEFUL_1, "memberX1"), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_2), plan.borrowedMigrations());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    @Test
    public void shouldEvictWarmupsInReverseFundingOrderWhenTheBudgetShrinks() {
        // Only a config change can lower the budget below the warm-ups already in flight. Which ones survive follows
        // the funding order rather than iteration order, so the outcome is reproducible.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0))),
            "memberC1", member("memberC1", "processC", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 1))),
            "memberC2", member("memberC2", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC1", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1)),
            "memberC2", TasksTuple.EMPTY
        );

        // processB carries its warm-up on one member and processC spreads its over two, so processC ranks first.
        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 1);

        assertEquals(Map.of(STATEFUL_1, "memberC1"), plan.warmupTasks());
        assertEquals(Set.of(STATEFUL_0), plan.parkedMigrations());
    }

    @Test
    public void shouldPlanNothingWhenTheWarmupBudgetIsZero() {
        // A budget of zero means the group does not stage migrations at all, so there is nothing to fund -- not even
        // the borrows, which the caller has already ruled out by returning the target assignment untouched.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 1)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final AssignmentRefinerImpl.WarmupPlan plan = plan(members, targetAssignment, Map.of(), 0);

        assertEquals(Map.of(), plan.warmupTasks());
        assertEquals(Set.of(), plan.borrowedMigrations());
        assertEquals(Set.of(), plan.parkedMigrations());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // filterStandbys
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void shouldWithholdAStandbyOnAProcessThatStillRunsTheTaskAsActive() {
        // The swap shape: the assignor moves the active to memberB and leaves a standby behind on memberA. While the
        // migration is staged the task keeps running on memberA, so the standby cannot be placed there as well.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        assertEquals(Map.of("memberA", Set.of(STATEFUL_0)), filter(members, targetAssignment, Map.of(), 1));
    }

    @Test
    public void shouldEmitTheStandbyOnTheMemberGrantingTheActiveAwayInTheSameStep() {
        // The demotion carve-out, and the reason the swap is efficient: memberA hands the active over and keeps a
        // standby in its place, which the client does by relabelling the task it already has. Emitted a step later,
        // that state would already be gone.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );
        // memberB's standby is caught up, so the migration is granted rather than staged.
        final Map<String, MemberTaskOffsets> taskOffsets = Map.of("memberB", offsets(100, 100));

        assertEquals(Map.of(), filter(members, targetAssignment, taskOffsets, 1));
    }

    @Test
    public void shouldWithholdAStandbyOnASiblingOfTheMemberGrantingTheActiveAway() {
        // The carve-out is about the member recycling its own task, so it does not extend to a sibling: that one
        // would be a second copy on the process until the hand-over finishes, and has to wait for it.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA1", member("memberA1", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberA2", member("memberA2", "processA", TasksTuple.EMPTY),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA1", TasksTuple.EMPTY,
            "memberA2", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );
        final Map<String, MemberTaskOffsets> taskOffsets = Map.of("memberB", offsets(100, 100));

        assertEquals(Map.of("memberA2", Set.of(STATEFUL_0)), filter(members, targetAssignment, taskOffsets, 1));
    }

    @Test
    public void shouldEmitAStandbyBlockedOnlyByAPendingRevocation() {
        // The filter reads the tasks members have been granted, never the ones they were told to give up: indexing
        // revocations for this rule alone would duplicate what the reconciler already enforces, which refuses to
        // grant a role for a task the process still physically holds. So this is emitted and the hand-over
        // serializes itself, at the cost of an extra heartbeat or two before the group settles.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member(
                "memberA",
                "processA",
                TasksTuple.EMPTY,
                mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
            )
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))
        );

        assertEquals(Map.of(), filter(members, targetAssignment, Map.of(), 1));
    }

    @Test
    public void shouldWithholdTheRelocatedStandbyOfABorrowedMigration() {
        // Borrowing keeps memberB's standby where it is and lets it serve as the warmer too. The relocated placement
        // the target assignment wants on memberC is what makes that free: granting it as well would leave three
        // copies where the target assignment asks for two.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))
        );

        assertEquals(Map.of("memberC", Set.of(STATEFUL_0)), filter(members, targetAssignment, Map.of(), 1));
    }

    @Test
    public void shouldEmitTheRelocatedStandbyOfASiblingMove() {
        // The mirror image of the borrow, and what the sibling move's slot pays for: the copy is moving off memberB1
        // onto memberB2 as a warm-up, so it stops being the replica the group is entitled to, and the relocated
        // placement is emitted to backfill it.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB1", member("memberB1", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberB2", member("memberB2", "processB", TasksTuple.EMPTY),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB1", TasksTuple.EMPTY,
            "memberB2", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))
        );

        assertEquals(Map.of(), filter(members, targetAssignment, Map.of(), 1));
    }

    @Test
    public void shouldNotWithholdARelocatedStandbyTheMemberAlreadyHolds() {
        // Only a placement the member does not have yet adds a replica. memberC already holds this one, so emitting
        // it changes nothing about the replica count and the borrowing rule has no reason to hold it back.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberC", member("memberC", "processC", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))
        );

        assertEquals(Map.of(), filter(members, targetAssignment, Map.of(), 1));
    }

    @Test
    public void shouldNotWithholdStandbysOfStatelessTasks() {
        // A stateless task has no state to restore, so it is never staged and never collides with anything the
        // refiner decides. Its placements flow through from the target assignment untouched.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATELESS, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATELESS, 0))
        );

        assertEquals(Map.of(), filter(members, targetAssignment, Map.of(), 1));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // assemble
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void shouldReturnTheTargetAssignmentItselfWhenNothingDiverges() {
        // A converged group is the overwhelmingly common case, and it costs nothing: with no migration to hold back
        // there is no patch, so the target assignment is handed straight back.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        assertSame(targetAssignment, assemble(members, targetAssignment, Map.of(), 1));
    }

    @Test
    public void shouldKeepTheActiveWithItsCurrentOwnerAndWithholdItFromTheTargetOwner() {
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        // memberB is withheld the active and planted with the warm-up instead; memberA keeps running the task.
        assertEquals(
            Map.of(
                "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
                "memberB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0))
            ),
            assemble(members, targetAssignment, Map.of(), 1)
        );
    }

    @Test
    public void shouldApplyNoPatchForAGrantedTask() {
        // The target assignment already places the task on its new owner and omits it from the old one, so letting
        // it through unchanged is the grant. Nothing is written down for it.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", TasksTuple.EMPTY),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        assertSame(targetAssignment, assemble(members, targetAssignment, Map.of(), 1));
    }

    @Test
    public void shouldKeepABorrowedStandbyWhereHistoryLeftIt() {
        // The target assignment is relocating memberB's standby to memberC, which is exactly why borrowing it is
        // free -- but that means it is not in the slice memberB would otherwise get. Without patching it back in,
        // the reconciler would revoke the very copy warming memberB, and the migration would finish cold.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))
        );

        assertEquals(
            Map.of(
                "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
                // the borrowed copy, kept: still the standby the group is entitled to, and the warmer as well
                "memberB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)),
                // the relocated placement, withheld so that the replica count does not move
                "memberC", TasksTuple.EMPTY
            ),
            assemble(members, targetAssignment, Map.of(), 1)
        );
    }

    @Test
    public void shouldMoveASiblingStandbyOntoTheTargetOwnerAndBackfillTheRelocatedOne() {
        // The mirror of the borrow. memberB1's copy is not kept, because it is moving onto memberB2 as a warm-up;
        // in exchange the relocated placement on memberC is emitted, which is what the spent slot pays for.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB1", member("memberB1", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberB2", member("memberB2", "processB", TasksTuple.EMPTY),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB1", TasksTuple.EMPTY,
            "memberB2", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))
        );

        assertEquals(
            Map.of(
                "memberA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
                "memberB1", TasksTuple.EMPTY,
                "memberB2", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0)),
                "memberC", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))
            ),
            assemble(members, targetAssignment, Map.of(), 1)
        );
    }

    @Test
    public void shouldEmitTheSwapAsOneStepOnceTheWarmerIsCaughtUp() {
        // The shape the whole design is built around: one step hands memberB the active and memberA the standby, so
        // both sides relabel what they already hold and no restore work is wasted.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0)))
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0)),
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );
        final Map<String, MemberTaskOffsets> taskOffsets = Map.of("memberB", offsets(100, 100));

        assertSame(targetAssignment, assemble(members, targetAssignment, taskOffsets, 1));
    }

    @Test
    public void shouldDropASubtopologyKeyWhoseLastTaskWasPatchedAway() {
        // Pruning is not tidiness: the coordinator decides whether a refinement step is due with a plain map
        // comparison, so a key left behind mapping to an empty set would read as a change on every heartbeat and
        // mint refinement steps forever.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))),
            "memberB", member("memberB", "processB", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0))
        );

        final TasksTuple memberB = assemble(members, targetAssignment, Map.of(), 1).get("memberB");

        // The withheld active was memberB's only task for the subtopology, so the key goes with it.
        assertEquals(Map.of(), memberB.activeTasks());
        assertTrue(memberB.sameTasks(withEpochs(mkTasksTuple(TaskRole.WARMUP, mkTasks(STATEFUL, 0)))));
    }

    @Test
    public void shouldPreserveTheActiveTaskCountThroughEveryDerivation() {
        // The wrapper ignores a refined assignment that drops or duplicates an active task, so a derivation that
        // trips this check would ship a refiner the coordinator silently discards.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB1", member("memberB1", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberB2", member("memberB2", "processB", TasksTuple.EMPTY),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB1", TasksTuple.EMPTY,
            "memberB2", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );

        for (int numWarmupReplicas = 0; numWarmupReplicas <= 3; numWarmupReplicas++) {
            assertTrue(
                AssignmentRefiner.preservesActiveTaskCount(
                    targetAssignment,
                    assemble(members, targetAssignment, Map.of(), numWarmupReplicas)
                ),
                "active task count not preserved at numWarmupReplicas=" + numWarmupReplicas
            );
        }
    }

    @Test
    public void shouldPlaceEachTasksActiveOnExactlyOneMember() {
        // The invariant the reconciler cannot recover from if it is broken: a task active on two members at once.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB", member("memberB", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", TasksTuple.EMPTY,
            "memberB", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );

        final Map<TaskId, String> activeOwners = new HashMap<>();
        assemble(members, targetAssignment, Map.of(), 1).forEach((memberId, tasks) ->
            tasks.activeTasks().forEach((subtopologyId, partitionIds) -> partitionIds.forEach(partitionId -> {
                final String previous = activeOwners.put(new TaskId(subtopologyId, partitionId), memberId);
                assertNull(previous, "task active on both " + previous + " and " + memberId);
            })));

        assertEquals(Set.of(STATEFUL_0, STATEFUL_1), activeOwners.keySet());
    }

    @Test
    public void shouldNeverPlaceATaskTwiceOnOneProcess() {
        // A process holds a given task in at most one role. The refiner relies on this rather than enforcing it, so
        // the derivation has to avoid producing an intermediate assignment that breaks it.
        final Map<String, StreamsGroupMember> members = Map.of(
            "memberA", member("memberA", "processA", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0, 1))),
            "memberB1", member("memberB1", "processB", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 0))),
            "memberB2", member("memberB2", "processB", TasksTuple.EMPTY),
            "memberC", member("memberC", "processC", TasksTuple.EMPTY)
        );
        final Map<String, TasksTuple> targetAssignment = Map.of(
            "memberA", mkTasksTuple(TaskRole.STANDBY, mkTasks(STATEFUL, 1)),
            "memberB1", TasksTuple.EMPTY,
            "memberB2", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 0)),
            "memberC", mkTasksTuple(TaskRole.ACTIVE, mkTasks(STATEFUL, 1))
        );

        final Set<String> seen = new HashSet<>();
        assemble(members, targetAssignment, Map.of(), 2).forEach((memberId, tasks) -> {
            final String processId = members.get(memberId).processId();
            Map.of(
                TaskRole.ACTIVE, tasks.activeTasks(),
                TaskRole.STANDBY, tasks.standbyTasks(),
                TaskRole.WARMUP, tasks.warmupTasks()
            ).forEach((role, byRole) -> byRole.forEach((subtopologyId, partitionIds) -> partitionIds.forEach(
                partitionId -> assertTrue(
                    seen.add(processId + "/" + subtopologyId + "/" + partitionId),
                    "process " + processId + " holds " + subtopologyId + "_" + partitionId + " more than once"
                ))));
        });
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------------------------------------------------

    private static AssignmentRefinerImpl.CurrentAssignmentIndex index(
        final Map<String, StreamsGroupMember> members,
        final Map<String, MemberTaskOffsets> taskOffsets
    ) {
        return AssignmentRefinerImpl.indexCurrentAssignment(
            members,
            taskOffsets,
            subtopologies(),
            ACCEPTABLE_RECOVERY_LAG
        );
    }

    private static AssignmentRefinerImpl.TaskDecisions analyze(
        final Map<String, StreamsGroupMember> members,
        final Map<String, TasksTuple> targetAssignment,
        final Map<String, MemberTaskOffsets> taskOffsets
    ) {
        return AssignmentRefinerImpl.analyzeTasks(
            index(members, taskOffsets),
            targetAssignment,
            members,
            subtopologies()
        );
    }

    private static Map<String, AssignmentRefinerImpl.ProcessLoad> load(
        final Map<String, StreamsGroupMember> members
    ) {
        return AssignmentRefinerImpl.indexProcessLoad(members, subtopologies());
    }

    private static AssignmentRefinerImpl.WarmupPlan plan(
        final Map<String, StreamsGroupMember> members,
        final Map<String, TasksTuple> targetAssignment,
        final Map<String, MemberTaskOffsets> taskOffsets,
        final int numWarmupReplicas
    ) {
        return AssignmentRefinerImpl.planWarmups(
            analyze(members, targetAssignment, taskOffsets),
            members,
            load(members),
            numWarmupReplicas
        );
    }

    private static SortedMap<String, SortedSet<TaskId>> filter(
        final Map<String, StreamsGroupMember> members,
        final Map<String, TasksTuple> targetAssignment,
        final Map<String, MemberTaskOffsets> taskOffsets,
        final int numWarmupReplicas
    ) {
        final AssignmentRefinerImpl.CurrentAssignmentIndex currentAssignment = index(members, taskOffsets);
        final AssignmentRefinerImpl.TaskDecisions decisions =
            AssignmentRefinerImpl.analyzeTasks(currentAssignment, targetAssignment, members, subtopologies());
        return AssignmentRefinerImpl.filterStandbys(
            targetAssignment,
            currentAssignment,
            decisions,
            AssignmentRefinerImpl.planWarmups(decisions, members, load(members), numWarmupReplicas),
            members,
            subtopologies()
        );
    }

    /**
     * The whole derivation, end to end -- which is what the go-live change will wire into {@code refine()}.
     */
    private static Map<String, TasksTuple> assemble(
        final Map<String, StreamsGroupMember> members,
        final Map<String, TasksTuple> targetAssignment,
        final Map<String, MemberTaskOffsets> taskOffsets,
        final int numWarmupReplicas
    ) {
        final AssignmentRefinerImpl.CurrentAssignmentIndex currentAssignment = index(members, taskOffsets);
        final AssignmentRefinerImpl.TaskDecisions decisions =
            AssignmentRefinerImpl.analyzeTasks(currentAssignment, targetAssignment, members, subtopologies());
        final AssignmentRefinerImpl.WarmupPlan warmupPlan =
            AssignmentRefinerImpl.planWarmups(decisions, members, load(members), numWarmupReplicas);
        return AssignmentRefinerImpl.assemble(
            targetAssignment,
            currentAssignment,
            decisions,
            warmupPlan,
            AssignmentRefinerImpl.filterStandbys(
                targetAssignment, currentAssignment, decisions, warmupPlan, members, subtopologies())
        );
    }

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

    private static MemberTaskOffsets offsets(final long offset, final long endOffset) {
        return offsets(0, offset, endOffset);
    }

    private static MemberTaskOffsets offsets(final int partitionId, final long offset, final long endOffset) {
        return new MemberTaskOffsets(
            Map.of(STATEFUL, Map.of(partitionId, offset)),
            Map.of(STATEFUL, Map.of(partitionId, endOffset))
        );
    }
}
