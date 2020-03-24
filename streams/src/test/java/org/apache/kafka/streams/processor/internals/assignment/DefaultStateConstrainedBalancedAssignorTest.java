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


import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.assignment.StateConstrainedBalancedAssignor.ClientIdAndRank;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DefaultStateConstrainedBalancedAssignorTest {

    private static final TaskId TASK_01 = new TaskId(0, 1);
    private static final TaskId TASK_12 = new TaskId(1, 2);
    private static final TaskId TASK_23 = new TaskId(2, 3);
    private static final TaskId TASK_34 = new TaskId(3, 4);

    private static final String CLIENT_1 = "client1";
    private static final String CLIENT_2 = "client2";
    private static final String CLIENT_3 = "client3";

    private static final Set<String> TWO_CLIENTS = new HashSet<>(Arrays.asList(CLIENT_1, CLIENT_2));
    private static final Set<String> THREE_CLIENTS = new HashSet<>(Arrays.asList(CLIENT_1, CLIENT_2, CLIENT_3));

    @Test
    public void shouldAssignTaskToCaughtUpClient() {
        final long rankOfClient1 = 0;
        final long rankOfClient2 = Long.MAX_VALUE;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToPreviouslyHostingClient() {
        final long rankOfClient1 = Long.MAX_VALUE;
        final long rankOfClient2 = Task.LATEST_OFFSET;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToPreviouslyHostingClientWhenOtherCaughtUpClientExists() {
        final long rankOfClient1 = 0;
        final long rankOfClient2 = Task.LATEST_OFFSET;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToCaughtUpClientThatIsFirstInSortOrder() {
        final long rankOfClient1 = 0;
        final long rankOfClient2 = 0;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToMostCaughtUpClient() {
        final long rankOfClient1 = 3;
        final long rankOfClient2 = 5;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(rankOfClient1, rankOfClient2),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksToCaughtUpClientsThatAreNotPreviousHostsEvenIfNotRequiredByBalanceFactor() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = 0;
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
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
            ),
            balanceFactor,
            THREE_CLIENTS,
            threeClientsToNumberOfStreamThreads(1, 1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_23);
        final List<TaskId> assignedTasksForClient3 = Collections.singletonList(TASK_12);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldMoveTask12FromClient2ToEvenlyDistributeTasksToCaughtUpClientsEvenIfOneClientIsPreviousHostOfBoth() {
        final long rankForTask01OnClient1 = 100;
        final long rankForTask01OnClient2 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = Task.LATEST_OFFSET;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignBothTasksToPreviousHostSinceBalanceFactorSatisfied() {
        final long rankForTask01OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient2 = 0;
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_01, TASK_12);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignOneTaskToPreviousHostAndOtherTaskToLessCaughtUpClientDueToBalanceFactor() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = Task.LATEST_OFFSET;
        final long rankForTask12OnClient1 = 20;
        final long rankForTask12OnClient2 = 10;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignBothTasksToSameClientSincePreviousHostAndMostCaughtUpAndBalanceFactorSatisfied() {
        final long rankForTask01OnClient1 = Task.LATEST_OFFSET;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 10;
        final long rankForTask12OnClient2 = 20;
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_01, TASK_12);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
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
            ),
            balanceFactor,
            THREE_CLIENTS,
            threeClientsToNumberOfStreamThreads(1, 1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_23);
        final List<TaskId> assignedTasksForClient3 = Collections.singletonList(TASK_12);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_01, TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksOverClientsWithEqualRank() {
        final long rankForTask01OnClient1 = 40;
        final long rankForTask01OnClient2 = 40;
        final long rankForTask12OnClient1 = 40;
        final long rankForTask12OnClient2 = 40;
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    /**
     * This test shows that in an assigment of one client the assumption that the set of tasks which are caught-up on
     * the given client is followed by the set of tasks that are not caught-up on the given client does NOT hold.
     * In fact, in this test, at some point during the execution of the algorithm the assignment for CLIENT_2
     * contains TASK_34 followed by TASK_23. TASK_23 is caught-up on CLIENT_2 whereas TASK_34 is not.
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
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
            ),
            balanceFactor,
            THREE_CLIENTS,
            threeClientsToNumberOfStreamThreads(1, 1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_01, TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_23);
        final List<TaskId> assignedTasksForClient3 = Collections.singletonList(TASK_34);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
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
            ),
            balanceFactor,
            THREE_CLIENTS,
            threeClientsToNumberOfStreamThreads(1, 1, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_34);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_01, TASK_23, TASK_12);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            threeStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask23OnClient1,
                rankForTask23OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 2)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_12, TASK_23);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            fourStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask23OnClient1,
                rankForTask23OnClient2,
                rankForTask34OnClient1,
                rankForTask34OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 2)
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_01, TASK_23);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_12, TASK_34);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldDistributeTasksOverOverProvisionedStreamThreadsYieldingBalancedStreamThreadsAndClients() {
        final long rankForTask01OnClient1 = 0;
        final long rankForTask01OnClient2 = 0;
        final long rankForTask12OnClient1 = 0;
        final long rankForTask12OnClient2 = 0;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(2, 1)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
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

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            fourStatefulTasksToTwoRankedClients(
                rankForTask01OnClient1,
                rankForTask01OnClient2,
                rankForTask12OnClient1,
                rankForTask12OnClient2,
                rankForTask23OnClient1,
                rankForTask23OnClient2,
                rankForTask34OnClient1,
                rankForTask34OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS,
            twoClientsToNumberOfStreamThreads(1, 4)
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_12, TASK_34, TASK_23);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    private static Map<String, Integer> twoClientsToNumberOfStreamThreads(final int numberOfStreamThread1,
                                                                          final int numberOfStreamThread2) {
        return mkMap(
            mkEntry(CLIENT_1, numberOfStreamThread1),
            mkEntry(CLIENT_2, numberOfStreamThread2)
        );
    }
    
    private static Map<String, Integer> threeClientsToNumberOfStreamThreads(final int numberOfStreamThread1,
                                                                            final int numberOfStreamThread2,
                                                                            final int numberOfStreamThread3) {
        return mkMap(
            mkEntry(CLIENT_1, numberOfStreamThread1),
            mkEntry(CLIENT_2, numberOfStreamThread2),
            mkEntry(CLIENT_3, numberOfStreamThread3)
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> oneStatefulTasksToTwoRankedClients(final long rankOfClient1,
                                                                                                            final long rankOfClient2) {
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, rankOfClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, rankOfClient2));
        return new TreeMap<>(
            mkMap(mkEntry(TASK_01, rankedClients01))
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> twoStatefulTasksToTwoRankedClients(final long rankForTask01OnClient1,
                                                                                                            final long rankForTask01OnClient2,
                                                                                                            final long rankForTask12OnClient1,
                                                                                                            final long rankForTask12OnClient2) {
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, rankForTask01OnClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, rankForTask01OnClient2));
        final SortedSet<ClientIdAndRank<String>> rankedClients12 = new TreeSet<>();
        rankedClients12.add(ClientIdAndRank.make(CLIENT_1, rankForTask12OnClient1));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_2, rankForTask12OnClient2));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_01, rankedClients01),
                mkEntry(TASK_12, rankedClients12)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> threeStatefulTasksToTwoRankedClients(final long rankForTask01OnClient1,
                                                                                                              final long rankForTask01OnClient2,
                                                                                                              final long rankForTask12OnClient1,
                                                                                                              final long rankForTask12OnClient2,
                                                                                                              final long rankForTask23OnClient1,
                                                                                                              final long rankForTask23OnClient2) {
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, rankForTask01OnClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, rankForTask01OnClient2));
        final SortedSet<ClientIdAndRank<String>> rankedClients12 = new TreeSet<>();
        rankedClients12.add(ClientIdAndRank.make(CLIENT_1, rankForTask12OnClient1));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_2, rankForTask12OnClient2));
        final SortedSet<ClientIdAndRank<String>> rankedClients23 = new TreeSet<>();
        rankedClients23.add(ClientIdAndRank.make(CLIENT_1, rankForTask23OnClient1));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_2, rankForTask23OnClient2));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_01, rankedClients01),
                mkEntry(TASK_12, rankedClients12),
                mkEntry(TASK_23, rankedClients23)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> threeStatefulTasksToThreeRankedClients(final long rankForTask01OnClient1,
                                                                                                                final long rankForTask01OnClient2,
                                                                                                                final long rankForTask01OnClient3,
                                                                                                                final long rankForTask12OnClient1,
                                                                                                                final long rankForTask12OnClient2,
                                                                                                                final long rankForTask12OnClient3,
                                                                                                                final long rankForTask23OnClient1,
                                                                                                                final long rankForTask23OnClient2,
                                                                                                                final long rankForTask23OnClient3) {
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, rankForTask01OnClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, rankForTask01OnClient2));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_3, rankForTask01OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients12 = new TreeSet<>();
        rankedClients12.add(ClientIdAndRank.make(CLIENT_1, rankForTask12OnClient1));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_2, rankForTask12OnClient2));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_3, rankForTask12OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients23 = new TreeSet<>();
        rankedClients23.add(ClientIdAndRank.make(CLIENT_1, rankForTask23OnClient1));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_2, rankForTask23OnClient2));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_3, rankForTask23OnClient3));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_01, rankedClients01),
                mkEntry(TASK_12, rankedClients12),
                mkEntry(TASK_23, rankedClients23)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> fourStatefulTasksToTwoRankedClients(final long rankForTask01OnClient1,
                                                                                                             final long rankForTask01OnClient2,
                                                                                                             final long rankForTask12OnClient1,
                                                                                                             final long rankForTask12OnClient2,
                                                                                                             final long rankForTask23OnClient1,
                                                                                                             final long rankForTask23OnClient2,
                                                                                                             final long rankForTask34OnClient1,
                                                                                                             final long rankForTask34OnClient2) {
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, rankForTask01OnClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, rankForTask01OnClient2));
        final SortedSet<ClientIdAndRank<String>> rankedClients12 = new TreeSet<>();
        rankedClients12.add(ClientIdAndRank.make(CLIENT_1, rankForTask12OnClient1));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_2, rankForTask12OnClient2));
        final SortedSet<ClientIdAndRank<String>> rankedClients23 = new TreeSet<>();
        rankedClients23.add(ClientIdAndRank.make(CLIENT_1, rankForTask23OnClient1));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_2, rankForTask23OnClient2));
        final SortedSet<ClientIdAndRank<String>> rankedClients34 = new TreeSet<>();
        rankedClients34.add(ClientIdAndRank.make(CLIENT_1, rankForTask34OnClient1));
        rankedClients34.add(ClientIdAndRank.make(CLIENT_2, rankForTask34OnClient2));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_01, rankedClients01),
                mkEntry(TASK_12, rankedClients12),
                mkEntry(TASK_23, rankedClients23),
                mkEntry(TASK_34, rankedClients34)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> fourStatefulTasksToThreeRankedClients(final long rankForTask01OnClient1,
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
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, rankForTask01OnClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, rankForTask01OnClient2));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_3, rankForTask01OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients12 = new TreeSet<>();
        rankedClients12.add(ClientIdAndRank.make(CLIENT_1, rankForTask12OnClient1));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_2, rankForTask12OnClient2));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_3, rankForTask12OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients23 = new TreeSet<>();
        rankedClients23.add(ClientIdAndRank.make(CLIENT_1, rankForTask23OnClient1));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_2, rankForTask23OnClient2));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_3, rankForTask23OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients34 = new TreeSet<>();
        rankedClients34.add(ClientIdAndRank.make(CLIENT_1, rankForTask34OnClient1));
        rankedClients34.add(ClientIdAndRank.make(CLIENT_2, rankForTask34OnClient2));
        rankedClients34.add(ClientIdAndRank.make(CLIENT_3, rankForTask34OnClient3));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_01, rankedClients01),
                mkEntry(TASK_12, rankedClients12),
                mkEntry(TASK_23, rankedClients23),
                mkEntry(TASK_34, rankedClients34)
            )
        );
    }

    private static Map<String, List<TaskId>> expectedAssignmentForTwoClients(final List<TaskId> assignedTasksForClient1,
                                                                             final List<TaskId> assignedTasksForClient2) {
        return mkMap(
            mkEntry(CLIENT_1, assignedTasksForClient1),
            mkEntry(CLIENT_2, assignedTasksForClient2)
        );
    }

    private static Map<String, List<TaskId>> expectedAssignmentForThreeClients(final List<TaskId> assignedTasksForClient1,
                                                                               final List<TaskId> assignedTasksForClient2,
                                                                               final List<TaskId> assignedTasksForClient3) {
        return mkMap(
            mkEntry(CLIENT_1, assignedTasksForClient1),
            mkEntry(CLIENT_2, assignedTasksForClient2),
            mkEntry(CLIENT_3, assignedTasksForClient3)
        );
    }
}