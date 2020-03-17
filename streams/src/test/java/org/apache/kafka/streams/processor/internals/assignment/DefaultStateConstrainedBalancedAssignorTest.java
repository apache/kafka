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
        final long lagOfClient1 = 0;
        final long lagOfClient2 = Long.MAX_VALUE;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(lagOfClient1, lagOfClient2),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToPreviouslyHostingClient() {
        final long lagOfClient1 = Long.MAX_VALUE;
        final long lagOfClient2 = Task.LATEST_OFFSET;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(lagOfClient1, lagOfClient2),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToPreviouslyHostingClientWhenOtherCaughtUpClientExists() {
        final long lagOfClient1 = 0;
        final long lagOfClient2 = Task.LATEST_OFFSET;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(lagOfClient1, lagOfClient2),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToCaughtUpClientThatIsFirstInSortOrder() {
        final long lagOfClient1 = 0;
        final long lagOfClient2 = 0;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(lagOfClient1, lagOfClient2),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTaskToMostCaughtUpClient() {
        final long lagOfClient1 = 3;
        final long lagOfClient2 = 5;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            oneStatefulTasksToTwoRankedClients(lagOfClient1, lagOfClient2),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksToCaughtUpClientsThatAreNotPreviousHosts() {
        final long lagForTask01OnClient1 = 0;
        final long lagForTask01OnClient2 = 0;
        final long lagForTask12OnClient1 = 0;
        final long lagForTask12OnClient2 = 0;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1, 
                lagForTask01OnClient2, 
                lagForTask12OnClient1, 
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksToCaughtUpClientsThatAreNotPreviousHostsEvenIfNotRequiredByBalanceFactor() {
        final long lagForTask01OnClient1 = 0;
        final long lagForTask01OnClient2 = 0;
        final long lagForTask12OnClient1 = 0;
        final long lagForTask12OnClient2 = 0;
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksToCaughtUpClientsEvenIfOneClientIsPreviousHostOfAll() {
        final long lagForTask01OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask01OnClient2 = 0;
        final long lagForTask01OnClient3 = 0;
        final long lagForTask12OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask12OnClient2 = 0;
        final long lagForTask12OnClient3 = 0;
        final long lagForTask23OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask23OnClient2 = 0;
        final long lagForTask23OnClient3 = 0;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            threeStatefulTasksToThreeRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask01OnClient3,
                lagForTask12OnClient1,
                lagForTask12OnClient2,
                lagForTask12OnClient3,
                lagForTask23OnClient1,
                lagForTask23OnClient2,
                lagForTask23OnClient3
            ),
            balanceFactor,
            THREE_CLIENTS
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
    public void shouldEvenlyDistributeTasksToCaughtUpClientsEvenIfOneClientIsPreviousHostOfBoth1() {
        final long lagForTask01OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask01OnClient2 = 0;
        final long lagForTask12OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask12OnClient2 = 100;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksToCaughtUpClientsEvenIfOneClientIsPreviousHostOfBoth2() {
        final long lagForTask01OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask01OnClient2 = 100;
        final long lagForTask12OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask12OnClient2 = 0;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignBothTasksToPreviousHostSinceBalanceFactorSatisfied() {
        final long lagForTask01OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask01OnClient2 = 0;
        final long lagForTask12OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask12OnClient2 = 0;
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_01, TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignOneTaskToPreviousHostAndOtherTaskToMostCaughtUpClient() {
        final long lagForTask01OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask01OnClient2 = 0;
        final long lagForTask12OnClient1 = 20;
        final long lagForTask12OnClient2 = 10;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignOneTaskToPreviousHostAndOtherTaskToNotMostCaughtUpClientDueToBalanceFactor() {
        final long lagForTask01OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask01OnClient2 = 0;
        final long lagForTask12OnClient1 = 10;
        final long lagForTask12OnClient2 = 20;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignBothTasksToSameClientSincePreviousHostAndMostCaughtUpAndBalanceFactorSatisfied() {
        final long lagForTask01OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask01OnClient2 = 0;
        final long lagForTask12OnClient1 = 10;
        final long lagForTask12OnClient2 = 20;
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_01, TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.emptyList();
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldAssignTasksToMostCaughtUpClient() {
        final long lagForTask01OnClient1 = 50;
        final long lagForTask01OnClient2 = 20;
        final long lagForTask12OnClient1 = 20;
        final long lagForTask12OnClient2 = 50;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksEvenIfClientsAreNotMostCaughtUpDueToBalanceFactor() {
        final long lagForTask01OnClient1 = 20;
        final long lagForTask01OnClient2 = 50;
        final long lagForTask01OnClient3 = 100;
        final long lagForTask12OnClient1 = 20;
        final long lagForTask12OnClient2 = 50;
        final long lagForTask12OnClient3 = 100;
        final long lagForTask23OnClient1 = 20;
        final long lagForTask23OnClient2 = 50;
        final long lagForTask23OnClient3 = 100;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            threeStatefulTasksToThreeRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask01OnClient3,
                lagForTask12OnClient1,
                lagForTask12OnClient2,
                lagForTask12OnClient3,
                lagForTask23OnClient1,
                lagForTask23OnClient2,
                lagForTask23OnClient3
            ),
            balanceFactor,
            THREE_CLIENTS
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
        final long lagForTask01OnClient1 = 40;
        final long lagForTask01OnClient2 = 30;
        final long lagForTask12OnClient1 = 20;
        final long lagForTask12OnClient2 = 10;
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Collections.emptyList();
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_01, TASK_12);
        assertThat(assignment, is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2)));
    }

    @Test
    public void shouldEvenlyDistributeTasksOverClientsWithEqualRank() {
        final long lagForTask01OnClient1 = 40;
        final long lagForTask01OnClient2 = 40;
        final long lagForTask12OnClient1 = 40;
        final long lagForTask12OnClient2 = 40;
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            twoStatefulTasksToTwoRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask12OnClient1,
                lagForTask12OnClient2
            ),
            balanceFactor,
            TWO_CLIENTS
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
        final long lagForTask01OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask01OnClient2 = 0;
        final long lagForTask01OnClient3 = 100;
        final long lagForTask12OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask12OnClient2 = 0;
        final long lagForTask12OnClient3 = 100;
        final long lagForTask23OnClient1 = Task.LATEST_OFFSET;
        final long lagForTask23OnClient2 = 0;
        final long lagForTask23OnClient3 = 100;
        final long lagForTask34OnClient1 = 50;
        final long lagForTask34OnClient2 = 20;
        final long lagForTask34OnClient3 = 100;
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultStateConstrainedBalancedAssignor<String>().assign(
            fourStatefulTasksToThreeRankedClients(
                lagForTask01OnClient1,
                lagForTask01OnClient2,
                lagForTask01OnClient3,
                lagForTask12OnClient1,
                lagForTask12OnClient2,
                lagForTask12OnClient3,
                lagForTask23OnClient1,
                lagForTask23OnClient2,
                lagForTask23OnClient3,
                lagForTask34OnClient1,
                lagForTask34OnClient2,
                lagForTask34OnClient3
            ),
            balanceFactor,
            THREE_CLIENTS
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_01, TASK_12);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_23);
        final List<TaskId> assignedTasksForClient3 = Collections.singletonList(TASK_34);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> oneStatefulTasksToTwoRankedClients(final long lagOfClient1,
                                                                                                            final long lagOfClient2) {
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, lagOfClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, lagOfClient2));
        return new TreeMap<>(
            mkMap(mkEntry(TASK_01, rankedClients01))
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> twoStatefulTasksToTwoRankedClients(final long lagForTask01OnClient1,
                                                                                                            final long lagForTask01OnClient2,
                                                                                                            final long lagForTask12OnClient1,
                                                                                                            final long lagForTask12OnClient2) {
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, lagForTask01OnClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, lagForTask01OnClient2));
        final SortedSet<ClientIdAndRank<String>> rankedClients12 = new TreeSet<>();
        rankedClients12.add(ClientIdAndRank.make(CLIENT_1, lagForTask12OnClient1));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_2, lagForTask12OnClient2));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_01, rankedClients01),
                mkEntry(TASK_12, rankedClients12)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> threeStatefulTasksToThreeRankedClients(final long lagForTask01OnClient1,
                                                                                                                final long lagForTask01OnClient2,
                                                                                                                final long lagForTask01OnClient3,
                                                                                                                final long lagForTask12OnClient1,
                                                                                                                final long lagForTask12OnClient2,
                                                                                                                final long lagForTask12OnClient3,
                                                                                                                final long lagForTask23OnClient1,
                                                                                                                final long lagForTask23OnClient2,
                                                                                                                final long lagForTask23OnClient3) {
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, lagForTask01OnClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, lagForTask01OnClient2));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_3, lagForTask01OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients12 = new TreeSet<>();
        rankedClients12.add(ClientIdAndRank.make(CLIENT_1, lagForTask12OnClient1));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_2, lagForTask12OnClient2));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_3, lagForTask12OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients23 = new TreeSet<>();
        rankedClients23.add(ClientIdAndRank.make(CLIENT_1, lagForTask23OnClient1));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_2, lagForTask23OnClient2));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_3, lagForTask23OnClient3));
        return new TreeMap<>(
            mkMap(
                mkEntry(TASK_01, rankedClients01),
                mkEntry(TASK_12, rankedClients12),
                mkEntry(TASK_23, rankedClients23)
            )
        );
    }

    private static SortedMap<TaskId, SortedSet<ClientIdAndRank<String>>> fourStatefulTasksToThreeRankedClients(final long lagForTask01OnClient1,
                                                                                                               final long lagForTask01OnClient2,
                                                                                                               final long lagForTask01OnClient3,
                                                                                                               final long lagForTask12OnClient1,
                                                                                                               final long lagForTask12OnClient2,
                                                                                                               final long lagForTask12OnClient3,
                                                                                                               final long lagForTask23OnClient1,
                                                                                                               final long lagForTask23OnClient2,
                                                                                                               final long lagForTask23OnClient3,
                                                                                                               final long lagForTask34OnClient1,
                                                                                                               final long lagForTask34OnClient2,
                                                                                                               final long lagForTask34OnClient3) {
        final SortedSet<ClientIdAndRank<String>> rankedClients01 = new TreeSet<>();
        rankedClients01.add(ClientIdAndRank.make(CLIENT_1, lagForTask01OnClient1));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_2, lagForTask01OnClient2));
        rankedClients01.add(ClientIdAndRank.make(CLIENT_3, lagForTask01OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients12 = new TreeSet<>();
        rankedClients12.add(ClientIdAndRank.make(CLIENT_1, lagForTask12OnClient1));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_2, lagForTask12OnClient2));
        rankedClients12.add(ClientIdAndRank.make(CLIENT_3, lagForTask12OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients23 = new TreeSet<>();
        rankedClients23.add(ClientIdAndRank.make(CLIENT_1, lagForTask23OnClient1));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_2, lagForTask23OnClient2));
        rankedClients23.add(ClientIdAndRank.make(CLIENT_3, lagForTask23OnClient3));
        final SortedSet<ClientIdAndRank<String>> rankedClients34 = new TreeSet<>();
        rankedClients34.add(ClientIdAndRank.make(CLIENT_1, lagForTask34OnClient1));
        rankedClients34.add(ClientIdAndRank.make(CLIENT_2, lagForTask34OnClient2));
        rankedClients34.add(ClientIdAndRank.make(CLIENT_3, lagForTask34OnClient3));
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