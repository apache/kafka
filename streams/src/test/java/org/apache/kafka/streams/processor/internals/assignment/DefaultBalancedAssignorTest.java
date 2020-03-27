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
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DefaultBalancedAssignorTest {
    private static final TaskId TASK_00 = new TaskId(0, 0);
    private static final TaskId TASK_01 = new TaskId(0, 1);
    private static final TaskId TASK_02 = new TaskId(0, 2);
    private static final TaskId TASK_10 = new TaskId(1, 0);
    private static final TaskId TASK_11 = new TaskId(1, 1);
    private static final TaskId TASK_12 = new TaskId(1, 2);
    private static final TaskId TASK_20 = new TaskId(2, 0);
    private static final TaskId TASK_21 = new TaskId(2, 1);
    private static final TaskId TASK_22 = new TaskId(2, 2);

    private static final String CLIENT_1 = "client1";
    private static final String CLIENT_2 = "client2";
    private static final String CLIENT_3 = "client3";

    private static final SortedSet<String> TWO_CLIENTS = new TreeSet<>(Arrays.asList(CLIENT_1, CLIENT_2));
    private static final SortedSet<String> THREE_CLIENTS = new TreeSet<>(Arrays.asList(CLIENT_1, CLIENT_2, CLIENT_3));

    @Test
    public void shouldAssignTasksEvenlyOverClientsWhereNumberOfClientsIntegralDivisorOfNumberOfTasks() {
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultBalancedAssignor<String>().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_00,
                TASK_01,
                TASK_02,
                TASK_10,
                TASK_11,
                TASK_12,
                TASK_20,
                TASK_21,
                TASK_22
            ),
            threeClientsToNumberOfStreamThreads(1, 1, 1),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_00, TASK_10, TASK_20);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_01, TASK_11, TASK_21);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_02, TASK_12, TASK_22);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsWhereNumberOfClientsNotIntegralDivisorOfNumberOfTasks() {
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultBalancedAssignor<String>().assign(
            TWO_CLIENTS,
            mkSortedSet(
                TASK_00,
                TASK_01,
                TASK_02,
                TASK_10,
                TASK_11,
                TASK_12,
                TASK_20,
                TASK_21,
                TASK_22
            ),
            twoClientsToNumberOfStreamThreads(1, 1),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_00, TASK_02, TASK_11, TASK_20, TASK_22);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_01, TASK_10, TASK_12, TASK_21);
        assertThat(
            assignment,
            is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsWhereNumberOfStreamThreadsIntegralDivisorOfNumberOfTasks() {
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultBalancedAssignor<String>().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_00,
                TASK_01,
                TASK_02,
                TASK_10,
                TASK_11,
                TASK_12,
                TASK_20,
                TASK_21,
                TASK_22
            ),
            threeClientsToNumberOfStreamThreads(3, 3, 3),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_00, TASK_10, TASK_20);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_01, TASK_11, TASK_21);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_02, TASK_12, TASK_22);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsWhereNumberOfStreamThreadsNotIntegralDivisorOfNumberOfTasks() {
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultBalancedAssignor<String>().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_00,
                TASK_01,
                TASK_02,
                TASK_10,
                TASK_11,
                TASK_12,
                TASK_20,
                TASK_21,
                TASK_22
            ),
            threeClientsToNumberOfStreamThreads(2, 2, 2),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_00, TASK_10, TASK_20);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_01, TASK_11, TASK_21);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_02, TASK_12, TASK_22);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverUnevenlyDistributedStreamThreads() {
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultBalancedAssignor<String>().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_00,
                TASK_01,
                TASK_02,
                TASK_10,
                TASK_11,
                TASK_12,
                TASK_20,
                TASK_21,
                TASK_22
            ),
            threeClientsToNumberOfStreamThreads(1, 2, 3),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_10, TASK_20);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_01, TASK_11, TASK_21, TASK_00);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_02, TASK_12, TASK_22);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsWithLessClientsThanTasks() {
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultBalancedAssignor<String>().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_00,
                TASK_01
            ),
            threeClientsToNumberOfStreamThreads(1, 1, 1),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_00);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_01);
        final List<TaskId> assignedTasksForClient3 = Collections.emptyList();
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsAndStreamThreadsWithMoreStreamThreadsThanTasks() {
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultBalancedAssignor<String>().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_00,
                TASK_01,
                TASK_02,
                TASK_10,
                TASK_11,
                TASK_12,
                TASK_20,
                TASK_21,
                TASK_22
            ),
            threeClientsToNumberOfStreamThreads(6, 6, 6),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_00, TASK_10, TASK_20);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_01, TASK_11, TASK_21);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_02, TASK_12, TASK_22);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverStreamThreadsButBestEffortOverClients() {
        final int balanceFactor = 1;

        final Map<String, List<TaskId>> assignment = new DefaultBalancedAssignor<String>().assign(
            TWO_CLIENTS,
            mkSortedSet(
                TASK_00,
                TASK_01,
                TASK_02,
                TASK_10,
                TASK_11,
                TASK_12,
                TASK_20,
                TASK_21,
                TASK_22
            ),
            twoClientsToNumberOfStreamThreads(6, 2),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_00, TASK_02, TASK_11, TASK_20, TASK_22, TASK_01);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_10, TASK_12, TASK_21);
        assertThat(
            assignment,
            is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsButNotOverStreamThreadsBecauseBalanceFactorSatisfied() {
        final int balanceFactor = 2;

        final Map<String, List<TaskId>> assignment = new DefaultBalancedAssignor<String>().assign(
            TWO_CLIENTS,
            mkSortedSet(
                TASK_00,
                TASK_01,
                TASK_02,
                TASK_10,
                TASK_11,
                TASK_12,
                TASK_20,
                TASK_21,
                TASK_22
            ),
            twoClientsToNumberOfStreamThreads(6, 2),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_00, TASK_02, TASK_11, TASK_20, TASK_22);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_01, TASK_10, TASK_12, TASK_21);
        assertThat(
            assignment,
            is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2))
        );
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

    private static Map<String, List<TaskId>> expectedAssignmentForThreeClients(final List<TaskId> assignedTasksForClient1,
                                                                               final List<TaskId> assignedTasksForClient2,
                                                                               final List<TaskId> assignedTasksForClient3) {
        return mkMap(
            mkEntry(CLIENT_1, assignedTasksForClient1),
            mkEntry(CLIENT_2, assignedTasksForClient2),
            mkEntry(CLIENT_3, assignedTasksForClient3)
        );
    }

    private static Map<String, List<TaskId>> expectedAssignmentForTwoClients(final List<TaskId> assignedTasksForClient1,
                                                                             final List<TaskId> assignedTasksForClient2) {
        return mkMap(
            mkEntry(CLIENT_1, assignedTasksForClient1),
            mkEntry(CLIENT_2, assignedTasksForClient2)
        );
    }
}