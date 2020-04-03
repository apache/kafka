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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.getMovements;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

public class TaskMovementTest {

    @Test
    public void shouldGetMovementsFromStateConstrainedToBalancedAssignment() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_2)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_0)),
            mkEntry(UUID_3, asList(TASK_0_2, TASK_1_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, asList(TASK_0_2, TASK_1_2))
        );
        final Queue<TaskMovement> expectedMovements = new LinkedList<>();
        expectedMovements.add(new TaskMovement(TASK_1_2, UUID_1, UUID_3));
        expectedMovements.add(new TaskMovement(TASK_1_0, UUID_2, UUID_1));
        expectedMovements.add(new TaskMovement(TASK_1_1, UUID_3, UUID_2));

        assertThat(getMovements(stateConstrainedAssignment, balancedAssignment, maxWarmupReplicas), equalTo(expectedMovements));
    }

    @Test
    public void shouldOnlyGetUpToMaxWarmupReplicaMovements() {
        final int maxWarmupReplicas = 1;
        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_2)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_0)),
            mkEntry(UUID_3, asList(TASK_0_2, TASK_1_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, asList(TASK_0_2, TASK_1_2))
        );
        final Queue<TaskMovement> expectedMovements = new LinkedList<>();
        expectedMovements.add(new TaskMovement(TASK_1_2, UUID_1, UUID_3));

        assertThat(getMovements(stateConstrainedAssignment, balancedAssignment, maxWarmupReplicas), equalTo(expectedMovements));
    }

    @Test
    public void shouldReturnEmptyMovementsWhenPassedEmptyTaskAssignments() {
        final int maxWarmupReplicas = 2;
        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, emptyList()),
            mkEntry(UUID_2, emptyList())
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, emptyList()),
            mkEntry(UUID_2, emptyList())
        );
        assertTrue(getMovements(stateConstrainedAssignment, balancedAssignment, maxWarmupReplicas).isEmpty());
    }

    @Test
    public void shouldReturnEmptyMovementsWhenPassedIdenticalTaskAssignments() {
        final int maxWarmupReplicas = 2;
        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_1))
        );
        assertTrue(getMovements(stateConstrainedAssignment, balancedAssignment, maxWarmupReplicas).isEmpty());
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfAssignmentsAreOfDifferentSize() {
        final int maxWarmupReplicas = 2;

        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_0_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_1))
        );
        assertThrows(IllegalStateException.class, () -> getMovements(stateConstrainedAssignment, balancedAssignment, maxWarmupReplicas));
    }
}
