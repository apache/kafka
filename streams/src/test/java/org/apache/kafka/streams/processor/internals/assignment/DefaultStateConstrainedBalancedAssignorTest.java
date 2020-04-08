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
package org.apache.kafka.streams.processor.internals.assignment;

import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.RankedClient.tasksToCaughtUpClients;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DefaultStateConstrainedBalancedAssignorTest {

    private static final Set<UUID> TWO_CLIENTS = new HashSet<>(Arrays.asList(UUID_1, UUID_2));
    private static final Set<UUID> THREE_CLIENTS = new HashSet<>(Arrays.asList(UUID_1, UUID_2, UUID_3));

    @Test
    public void shouldAssignTaskToCaughtUpClient() {
        final long rankOfClient1 = 0;
        final long rankOfClient2 = Long.MAX_VALUE;
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2))
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToPreviouslyHostingClient() {
        final long rankOfClient1 = Long.MAX_VALUE;
        final long rankOfClient2 = Task.LATEST_OFFSET;
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2))
        );

        final List<TaskId> assignedTasksForClient1 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_0_1);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToPreviouslyHostingClientWhenOtherCaughtUpClientExists() {
        final long rankOfClient1 = 0;
        final long rankOfClient2 = Task.LATEST_OFFSET;
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2))
        );

        final List<TaskId> assignedTasksForClient1 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_0_1);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToCaughtUpClientThatIsFirstInSortOrder() {
        final long rankOfClient1 = 0;
        final long rankOfClient2 = 0;
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2))
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToMostCaughtUpClient() {
        final long rankOfClient1 = 3;
        final long rankOfClient2 = 5;
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2))
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksToCaughtUpClientsThatAreNotPreviousHosts() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = 0;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );
        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_1_2);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksToCaughtUpClientsThatAreNotPreviousHostsEvenIfNotRequiredByBalanceFactor() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = 0;
        final int balanceFactor = 2;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_1_2);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksToCaughtUpClientsEvenIfOneClientIsPreviousHostOfAll() {
        final long rankForTask01OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask01OnClient3 = 0;
        final long rankForTask12OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient2 = 0;
        final long rankForTask12OnClient3 = 0;
        final long rankForTask23OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask23OnClient2 = 0;
        final long rankForTask23OnClient3 = 0;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            threeStatefulTasksToThreeRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask01OnClient3,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask12OnClient3,
                rankForTask23OnClient1,
                rankForTask23OnClient2,
                rankForTask23OnClient3
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            THREE_CLIENTS,
            threeClientsToNumberOfStreamThreads(1, 1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_2_3);
        final List<TaskId> assignedTasksForClient3 = Collections.singletonList(TASK_1_2);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldMoveTask01FromClient1ToEvenlyDistributeTasksToCaughtUpClientsEvenIfOneClientIsPreviousHostOfBoth() {
        final long rankForTask01OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient2 = 100;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_1_2);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_0_1);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldMoveTask12FromClient2ToEvenlyDistributeTasksToCaughtUpClientsEvenIfOneClientIsPreviousHostOfBoth() {
        final long rankForTask01OnClient1 = 100;
        final long rankForTask01OnClient2 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = Task.LATEST_OFFSET;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_1_2);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_0_1);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignBothTasksToPreviousHostSinceBalanceFactorSatisfied() {
        final long rankForTask01OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient2 = 0;
        final int balanceFactor = 2;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_1, TASK_1_2);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignOneTaskToPreviousHostAndOtherTaskToMostCaughtUpClient() {
        final long rankForTask01OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 20;
        final long rankForTask12OnClient2 = 10;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_1_2);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignOneTaskToPreviousHostAndOtherTaskToLessCaughtUpClientDueToBalanceFactor() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient1 = 20;
        final long rankForTask12OnClient2 = 10;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_1_2);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_0_1);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignBothTasksToSameClientSincePreviousHostAndMostCaughtUpAndBalanceFactorSatisfied() {
        final long rankForTask01OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 10;
        final long rankForTask12OnClient2 = 20;
        final int balanceFactor = 2;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_1, TASK_1_2);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTasksToMostCaughtUpClient() {
        final long rankForTask01OnClient1 = 50;
        final long rankForTask01OnClient2 = 20;
        final long rankForTask12OnClient1 = 20;
        final long rankForTask12OnClient2 = 50;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_1_2);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_0_1);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksEvenIfClientsAreNotMostCaughtUpDueToBalanceFactor() {
        final long rankForTask01OnClient1 = 20;
        final long rankForTask01OnClient2 = 50;
        final long rankForTask01OnClient3 = 100;
        final long rankForTask12OnClient1 = 20;
        final long rankForTask12OnClient2 = 50;
        final long rankForTask12OnClient3 = 100;
        final long rankForTask23OnClient1 = 20;
        final long rankForTask23OnClient2 = 50;
        final long rankForTask23OnClient3 = 100;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            threeStatefulTasksToThreeRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask01OnClient3,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask12OnClient3,
                rankForTask23OnClient1,
                rankForTask23OnClient2,
                rankForTask23OnClient3
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            THREE_CLIENTS,
            threeClientsToNumberOfStreamThreads(1, 1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_2_3);
        final List<TaskId> assignedTasksForClient3 = Collections.singletonList(TASK_1_2);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignBothTasksToSameMostCaughtUpClientSinceBalanceFactorSatisfied() {
        final long rankForTask01OnClient1 = 40;
        final long rankForTask01OnClient2 = 30;
        final long rankForTask12OnClient1 = 20;
        final long rankForTask12OnClient2 = 10;
        final int balanceFactor = 2;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_0_1, TASK_1_2);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksOverClientsWithEqualRank() {
        final long rankForTask01OnClient1 = 40;
        final long rankForTask01OnClient2 = 40;
        final long rankForTask12OnClient1 = 40;
        final long rankForTask12OnClient2 = 40;
        final int balanceFactor = 2;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_1_2);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    /**
     * This test shows that in an assigment of one client the assumption that the set of tasks which are caught-up on
     * the given client is followed by the set of tasks that are not caught-up on the given client does NOT hold.
     * In fact, in this test, at some point during the execution of the algorithm the assignment for UUID_2
     * contains TASK_3_4 followed by TASK_2_3. TASK_2_3 is caught-up on UUID_2 whereas TASK_3_4 is not.
     */
    @Test
    public void shouldEvenlyDistributeTasksOrderOfCaughtUpAndNotCaughtUpTaskIsMixedUpInIntermediateResults() {
        final long rankForTask01OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask01OnClient3 = 100;
        final long rankForTask12OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient2 = 0;
        final long rankForTask12OnClient3 = 100;
        final long rankForTask23OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask23OnClient2 = 0;
        final long rankForTask23OnClient3 = 100;
        final long rankForTask34OnClient1 = 50;
        final long rankForTask34OnClient2 = 20;
        final long rankForTask34OnClient3 = 100;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            fourStatefulTasksToThreeRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask01OnClient3,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask12OnClient3,
                rankForTask23OnClient1,
                rankForTask23OnClient2,
                rankForTask23OnClient3,
                rankForTask34OnClient1,
                rankForTask34OnClient2,
                rankForTask34OnClient3
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            THREE_CLIENTS,
            threeClientsToNumberOfStreamThreads(1, 1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_1, TASK_1_2);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_2_3);
        final List<TaskId> assignedTasksForClient3 = Collections.singletonList(TASK_3_4);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksToTheCaughtUpClientEvenIfTheAssignmentIsUnbalanced() {
        final long rankForTask01OnClient1 = 60;
        final long rankForTask01OnClient2 = 50;
        final long rankForTask01OnClient3 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient1 = 40;
        final long rankForTask12OnClient2 = 30;
        final long rankForTask12OnClient3 = 0;
        final long rankForTask23OnClient1 = 10;
        final long rankForTask23OnClient2 = 20;
        final long rankForTask23OnClient3 = Task.LATEST_OFFSET;
        final long rankForTask34OnClient1 = 70;
        final long rankForTask34OnClient2 = 80;
        final long rankForTask34OnClient3 = 90;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            fourStatefulTasksToThreeRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask01OnClient3,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask12OnClient3,
                rankForTask23OnClient1,
                rankForTask23OnClient2,
                rankForTask23OnClient3,
                rankForTask34OnClient1,
                rankForTask34OnClient2,
                rankForTask34OnClient3
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            THREE_CLIENTS,
            threeClientsToNumberOfStreamThreads(1, 1, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_3_4);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_0_1, TASK_2_3, TASK_1_2);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldEvenlyDistributeTasksOverSameNumberOfStreamThreads() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = 0;
        final long rankForTask23OnClient1 = 0;
        final long rankForTask23OnClient2 = 0;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            threeStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask23OnClient1,
                rankForTask23OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 2),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_1_2, TASK_2_3);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksOnUnderProvisionedStreamThreads() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = 0;
        final long rankForTask23OnClient1 = 0;
        final long rankForTask23OnClient2 = 0;
        final long rankForTask34OnClient1 = 0;
        final long rankForTask34OnClient2 = 0;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            fourStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask23OnClient1,
                rankForTask23OnClient2,
                rankForTask34OnClient1,
                rankForTask34OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 2),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_1, TASK_2_3);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_1_2, TASK_3_4);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldDistributeTasksOverOverProvisionedStreamThreadsYieldingBalancedStreamThreadsAndClients() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = 0;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(2, 1),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_1_2);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldDistributeTasksOverOverProvisionedStreamThreadsYieldingBalancedStreamThreadsButUnbalancedClients() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = 0;
        final long rankForTask23OnClient1 = 0;
        final long rankForTask23OnClient2 = 0;
        final long rankForTask34OnClient1 = 0;
        final long rankForTask34OnClient2 = 0;
        final int balanceFactor = 1;

        final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates =
            fourStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask23OnClient1,
                rankForTask23OnClient2,
                rankForTask34OnClient1,
                rankForTask34OnClient2
            );

        final Map<UUID, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor().assign(
            statefulTasksToRankedCandidates,
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 4),
            tasksToCaughtUpClients(statefulTasksToRankedCandidates)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_1_2, TASK_3_4, TASK_2_3);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    private static Map<UUID, Integer> twoClientsToNumberOfStreamThreads(final int numberOfStreamThread1,
                                                                        final int numberOfStreamThread2) {
        return mkMap(
            mkEntry(UUID_1, numberOfStreamThread1),
            mkEntry(UUID_2, numberOfStreamThread2)
        );
    }

    private static Map<UUID, Integer> threeClientsToNumberOfStreamThreads(final int numberOfStreamThread1,
                                                                          final int numberOfStreamThread2,
                                                                          final int numberOfStreamThread3) {
        return mkMap(
            mkEntry(UUID_1, numberOfStreamThread1),
            mkEntry(UUID_2, numberOfStreamThread2),
            mkEntry(UUID_3, numberOfStreamThread3)
        );
    }

    private static SortedMap<TaskId, SortedSet<RankedClient>> oneStatefulTasksToTwoRankedClients(final long rankOfClient1,
                                                                                                 final long rankOfClient2) {
        final SortedSet<RankedClient> rankedClients01 = new TreeSet<>();
        rankedClients01.add(new RankedClient(UUID_1, rankOfClient1));
        rankedClients01.add(new RankedClient(UUID_2, rankOfClient2));
        return new TreeMap<>(
            mkMap(mkEntry(TASK_0_1, rankedClients01))
        );
    }

    private static SortedMap<TaskId, SortedSet<RankedClient>> twoStatefulTasksToTwoRankedClients(final long rankForTask01OnClient1,
                                                                                                 final long rankForTask01OnClient2,
                                                                                                 final long rankForTask12OnClient1,
                                                                                                 final long rankForTask12OnClient2) {
        final SortedSet<RankedClient> rankedClients01 = new TreeSet<>();
        rankedClients01.add(new RankedClient(UUID_1, rankForTask01OnClient1));
        rankedClients01.add(new RankedClient(UUID_2, rankForTask01OnClient2));
        final SortedSet<RankedClient> rankedClients12 = new TreeSet<>();
        rankedClients12.add(new RankedClient(UUID_1, rankForTask12OnClient1));
        rankedClients12.add(new RankedClient(UUID_2, rankForTask12OnClient2));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_0_1, rankedClients01),
                mkEntry(TASK_1_2, rankedClients12)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<RankedClient>> threeStatefulTasksToTwoRankedClients(final long rankForTask01OnClient1,
                                                                                                   final long rankForTask01OnClient2,
                                                                                                   final long rankForTask12OnClient1,
                                                                                                   final long rankForTask12OnClient2,
                                                                                                   final long rankForTask23OnClient1,
                                                                                                   final long rankForTask23OnClient2) {
        final SortedSet<RankedClient> rankedClients01 = new TreeSet<>();
        rankedClients01.add(new RankedClient(UUID_1, rankForTask01OnClient1));
        rankedClients01.add(new RankedClient(UUID_2, rankForTask01OnClient2));
        final SortedSet<RankedClient> rankedClients12 = new TreeSet<>();
        rankedClients12.add(new RankedClient(UUID_1, rankForTask12OnClient1));
        rankedClients12.add(new RankedClient(UUID_2, rankForTask12OnClient2));
        final SortedSet<RankedClient> rankedClients23 = new TreeSet<>();
        rankedClients23.add(new RankedClient(UUID_1, rankForTask23OnClient1));
        rankedClients23.add(new RankedClient(UUID_2, rankForTask23OnClient2));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_0_1, rankedClients01),
                mkEntry(TASK_1_2, rankedClients12),
                mkEntry(TASK_2_3, rankedClients23)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<RankedClient>> threeStatefulTasksToThreeRankedClients(final long rankForTask01OnClient1,
                                                                                                     final long rankForTask01OnClient2,
                                                                                                     final long rankForTask01OnClient3,
                                                                                                     final long rankForTask12OnClient1,
                                                                                                     final long rankForTask12OnClient2,
                                                                                                     final long rankForTask12OnClient3,
                                                                                                     final long rankForTask23OnClient1,
                                                                                                     final long rankForTask23OnClient2,
                                                                                                     final long rankForTask23OnClient3) {
        final SortedSet<RankedClient> rankedClients01 = new TreeSet<>();
        rankedClients01.add(new RankedClient(UUID_1, rankForTask01OnClient1));
        rankedClients01.add(new RankedClient(UUID_2, rankForTask01OnClient2));
        rankedClients01.add(new RankedClient(UUID_3, rankForTask01OnClient3));
        final SortedSet<RankedClient> rankedClients12 = new TreeSet<>();
        rankedClients12.add(new RankedClient(UUID_1, rankForTask12OnClient1));
        rankedClients12.add(new RankedClient(UUID_2, rankForTask12OnClient2));
        rankedClients12.add(new RankedClient(UUID_3, rankForTask12OnClient3));
        final SortedSet<RankedClient> rankedClients23 = new TreeSet<>();
        rankedClients23.add(new RankedClient(UUID_1, rankForTask23OnClient1));
        rankedClients23.add(new RankedClient(UUID_2, rankForTask23OnClient2));
        rankedClients23.add(new RankedClient(UUID_3, rankForTask23OnClient3));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_0_1, rankedClients01),
                mkEntry(TASK_1_2, rankedClients12),
                mkEntry(TASK_2_3, rankedClients23)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<RankedClient>> fourStatefulTasksToTwoRankedClients(final long rankForTask01OnClient1,
                                                                                                  final long rankForTask01OnClient2,
                                                                                                  final long rankForTask12OnClient1,
                                                                                                  final long rankForTask12OnClient2,
                                                                                                  final long rankForTask23OnClient1,
                                                                                                  final long rankForTask23OnClient2,
                                                                                                  final long rankForTask34OnClient1,
                                                                                                  final long rankForTask34OnClient2) {
        final SortedSet<RankedClient> rankedClients01 = new TreeSet<>();
        rankedClients01.add(new RankedClient(UUID_1, rankForTask01OnClient1));
        rankedClients01.add(new RankedClient(UUID_2, rankForTask01OnClient2));
        final SortedSet<RankedClient> rankedClients12 = new TreeSet<>();
        rankedClients12.add(new RankedClient(UUID_1, rankForTask12OnClient1));
        rankedClients12.add(new RankedClient(UUID_2, rankForTask12OnClient2));
        final SortedSet<RankedClient> rankedClients23 = new TreeSet<>();
        rankedClients23.add(new RankedClient(UUID_1, rankForTask23OnClient1));
        rankedClients23.add(new RankedClient(UUID_2, rankForTask23OnClient2));
        final SortedSet<RankedClient> rankedClients34 = new TreeSet<>();
        rankedClients34.add(new RankedClient(UUID_1, rankForTask34OnClient1));
        rankedClients34.add(new RankedClient(UUID_2, rankForTask34OnClient2));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_0_1, rankedClients01),
                mkEntry(TASK_1_2, rankedClients12),
                mkEntry(TASK_2_3, rankedClients23),
                mkEntry(TASK_3_4, rankedClients34)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<RankedClient>> fourStatefulTasksToThreeRankedClients(final long rankForTask01OnClient1,
                                                                                                    final long rankForTask01OnClient2,
                                                                                                    final long rankForTask01OnClient3,
                                                                                                    final long rankForTask12OnClient1,
                                                                                                    final long rankForTask12OnClient2,
                                                                                                    final long rankForTask12OnClient3,
                                                                                                    final long rankForTask23OnClient1,
                                                                                                    final long rankForTask23OnClient2,
                                                                                                    final long rankForTask23OnClient3,
                                                                                                    final long rankForTask34OnClient1,
                                                                                                    final long rankForTask34OnClient2,
                                                                                                    final long rankForTask34OnClient3) {
        final SortedSet<RankedClient> rankedClients01 = new TreeSet<>();
        rankedClients01.add(new RankedClient(UUID_1, rankForTask01OnClient1));
        rankedClients01.add(new RankedClient(UUID_2, rankForTask01OnClient2));
        rankedClients01.add(new RankedClient(UUID_3, rankForTask01OnClient3));
        final SortedSet<RankedClient> rankedClients12 = new TreeSet<>();
        rankedClients12.add(new RankedClient(UUID_1, rankForTask12OnClient1));
        rankedClients12.add(new RankedClient(UUID_2, rankForTask12OnClient2));
        rankedClients12.add(new RankedClient(UUID_3, rankForTask12OnClient3));
        final SortedSet<RankedClient> rankedClients23 = new TreeSet<>();
        rankedClients23.add(new RankedClient(UUID_1, rankForTask23OnClient1));
        rankedClients23.add(new RankedClient(UUID_2, rankForTask23OnClient2));
        rankedClients23.add(new RankedClient(UUID_3, rankForTask23OnClient3));
        final SortedSet<RankedClient> rankedClients34 = new TreeSet<>();
        rankedClients34.add(new RankedClient(UUID_1, rankForTask34OnClient1));
        rankedClients34.add(new RankedClient(UUID_2, rankForTask34OnClient2));
        rankedClients34.add(new RankedClient(UUID_3, rankForTask34OnClient3));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_0_1, rankedClients01),
                mkEntry(TASK_1_2, rankedClients12),
                mkEntry(TASK_2_3, rankedClients23),
                mkEntry(TASK_3_4, rankedClients34)
            )
        );
    }

    private static Map<UUID, List<TaskId>> expectedAssignmentForTwoClients(final List<TaskId> assignedTasksForClient1,
                                                                           final List<TaskId> assignedTasksForClient2) {
        return mkMap(
            mkEntry(UUID_1, assignedTasksForClient1),
            mkEntry(UUID_2, assignedTasksForClient2)
        );
    }

    private static Map<UUID, List<TaskId>> expectedAssignmentForThreeClients(final List<TaskId> assignedTasksForClient1,
                                                                             final List<TaskId> assignedTasksForClient2,
                                                                             final List<TaskId> assignedTasksForClient3) {
        return mkMap(
            mkEntry(UUID_1, assignedTasksForClient1),
            mkEntry(UUID_2, assignedTasksForClient2),
            mkEntry(UUID_3, assignedTasksForClient3)
        );
    }
}