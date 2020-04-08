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
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DefaultBalancedAssignorTest {
    private static final SortedSet<UUID> TWO_CLIENTS = new TreeSet<>(Arrays.asList(UUID_1, UUID_2));
    private static final SortedSet<UUID> THREE_CLIENTS = new TreeSet<>(Arrays.asList(UUID_1, UUID_2, UUID_3));

    @Test
    public void shouldAssignTasksEvenlyOverClientsWhereNumberOfClientsIntegralDivisorOfNumberOfTasks() {
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultBalancedAssignor().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_0_0,
                TASK_0_1,
                TASK_0_2,
                TASK_1_0,
                TASK_1_1,
                TASK_1_2,
                TASK_2_0,
                TASK_2_1,
                TASK_2_2
            ),
            threeClientsToNumberOfStreamThreads(1, 1, 1),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_0, TASK_1_0, TASK_2_0);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_0_1, TASK_1_1, TASK_2_1);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_0_2, TASK_1_2, TASK_2_2);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsWhereNumberOfClientsNotIntegralDivisorOfNumberOfTasks() {
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultBalancedAssignor().assign(
            TWO_CLIENTS,
            mkSortedSet(
                TASK_0_0,
                TASK_0_1,
                TASK_0_2,
                TASK_1_0,
                TASK_1_1,
                TASK_1_2,
                TASK_2_0,
                TASK_2_1,
                TASK_2_2
            ),
            twoClientsToNumberOfStreamThreads(1, 1),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_0, TASK_0_2, TASK_1_1, TASK_2_0, TASK_2_2);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_0_1, TASK_1_0, TASK_1_2, TASK_2_1);
        assertThat(
            assignment,
            is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsWhereNumberOfStreamThreadsIntegralDivisorOfNumberOfTasks() {
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultBalancedAssignor().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_0_0,
                TASK_0_1,
                TASK_0_2,
                TASK_1_0,
                TASK_1_1,
                TASK_1_2,
                TASK_2_0,
                TASK_2_1,
                TASK_2_2
            ),
            threeClientsToNumberOfStreamThreads(3, 3, 3),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_0, TASK_1_0, TASK_2_0);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_0_1, TASK_1_1, TASK_2_1);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_0_2, TASK_1_2, TASK_2_2);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsWhereNumberOfStreamThreadsNotIntegralDivisorOfNumberOfTasks() {
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultBalancedAssignor().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_0_0,
                TASK_0_1,
                TASK_0_2,
                TASK_1_0,
                TASK_1_1,
                TASK_1_2,
                TASK_2_0,
                TASK_2_1,
                TASK_2_2
            ),
            threeClientsToNumberOfStreamThreads(2, 2, 2),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_0, TASK_1_0, TASK_2_0);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_0_1, TASK_1_1, TASK_2_1);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_0_2, TASK_1_2, TASK_2_2);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverUnevenlyDistributedStreamThreads() {
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultBalancedAssignor().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_0_0,
                TASK_0_1,
                TASK_0_2,
                TASK_1_0,
                TASK_1_1,
                TASK_1_2,
                TASK_2_0,
                TASK_2_1,
                TASK_2_2
            ),
            threeClientsToNumberOfStreamThreads(1, 2, 3),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_1_0, TASK_2_0);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_0_1, TASK_1_1, TASK_2_1, TASK_0_0);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_0_2, TASK_1_2, TASK_2_2);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsWithLessClientsThanTasks() {
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultBalancedAssignor().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_0_0,
                TASK_0_1
            ),
            threeClientsToNumberOfStreamThreads(1, 1, 1),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Collections.singletonList(TASK_0_0);
        final List<TaskId> assignedTasksForClient2 = Collections.singletonList(TASK_0_1);
        final List<TaskId> assignedTasksForClient3 = Collections.emptyList();
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsAndStreamThreadsWithMoreStreamThreadsThanTasks() {
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultBalancedAssignor().assign(
            THREE_CLIENTS,
            mkSortedSet(
                TASK_0_0,
                TASK_0_1,
                TASK_0_2,
                TASK_1_0,
                TASK_1_1,
                TASK_1_2,
                TASK_2_0,
                TASK_2_1,
                TASK_2_2
            ),
            threeClientsToNumberOfStreamThreads(6, 6, 6),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_0, TASK_1_0, TASK_2_0);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_0_1, TASK_1_1, TASK_2_1);
        final List<TaskId> assignedTasksForClient3 = Arrays.asList(TASK_0_2, TASK_1_2, TASK_2_2);
        assertThat(
            assignment,
            is(expectedAssignmentForThreeClients(assignedTasksForClient1, assignedTasksForClient2, assignedTasksForClient3))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverStreamThreadsButBestEffortOverClients() {
        final int balanceFactor = 1;

        final Map<UUID, List<TaskId>> assignment = new DefaultBalancedAssignor().assign(
            TWO_CLIENTS,
            mkSortedSet(
                TASK_0_0,
                TASK_0_1,
                TASK_0_2,
                TASK_1_0,
                TASK_1_1,
                TASK_1_2,
                TASK_2_0,
                TASK_2_1,
                TASK_2_2
            ),
            twoClientsToNumberOfStreamThreads(6, 2),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_0, TASK_0_2, TASK_1_1, TASK_2_0, TASK_2_2,
            TASK_0_1);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_1_0, TASK_1_2, TASK_2_1);
        assertThat(
            assignment,
            is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2))
        );
    }

    @Test
    public void shouldAssignTasksEvenlyOverClientsButNotOverStreamThreadsBecauseBalanceFactorSatisfied() {
        final int balanceFactor = 2;

        final Map<UUID, List<TaskId>> assignment = new DefaultBalancedAssignor().assign(
            TWO_CLIENTS,
            mkSortedSet(
                TASK_0_0,
                TASK_0_1,
                TASK_0_2,
                TASK_1_0,
                TASK_1_1,
                TASK_1_2,
                TASK_2_0,
                TASK_2_1,
                TASK_2_2
            ),
            twoClientsToNumberOfStreamThreads(6, 2),
            balanceFactor
        );

        final List<TaskId> assignedTasksForClient1 = Arrays.asList(TASK_0_0, TASK_0_2, TASK_1_1, TASK_2_0, TASK_2_2);
        final List<TaskId> assignedTasksForClient2 = Arrays.asList(TASK_0_1, TASK_1_0, TASK_1_2, TASK_2_1);
        assertThat(
            assignment,
            is(expectedAssignmentForTwoClients(assignedTasksForClient1, assignedTasksForClient2))
        );
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

    private static Map<UUID, List<TaskId>> expectedAssignmentForThreeClients(final List<TaskId> assignedTasksForClient1,
                                                                               final List<TaskId> assignedTasksForClient2,
                                                                               final List<TaskId> assignedTasksForClient3) {
        return mkMap(
            mkEntry(UUID_1, assignedTasksForClient1),
            mkEntry(UUID_2, assignedTasksForClient2),
            mkEntry(UUID_3, assignedTasksForClient3)
        );
    }

    private static Map<UUID, List<TaskId>> expectedAssignmentForTwoClients(final List<TaskId> assignedTasksForClient1,
                                                                             final List<TaskId> assignedTasksForClient2) {
        return mkMap(
            mkEntry(UUID_1, assignedTasksForClient1),
            mkEntry(UUID_2, assignedTasksForClient2)
        );
    }
}