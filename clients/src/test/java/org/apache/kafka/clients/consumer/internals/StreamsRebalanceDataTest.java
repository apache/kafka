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
package org.apache.kafka.clients.consumer.internals;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamsRebalanceDataTest {

    @Test
    public void testTaskIdEqualsAndHashCode() {
        final StreamsRebalanceData.TaskId task = new StreamsRebalanceData.TaskId("subtopologyId1", 1);
        final StreamsRebalanceData.TaskId taskEqual = new StreamsRebalanceData.TaskId(task.subtopologyId(), task.partitionId());
        final StreamsRebalanceData.TaskId taskUnequalSubtopology = new StreamsRebalanceData.TaskId(task.subtopologyId() + "1", task.partitionId());
        final StreamsRebalanceData.TaskId taskUnequalPartition = new StreamsRebalanceData.TaskId(task.subtopologyId(), task.partitionId() + 1);

        assertEquals(task, taskEqual);
        assertEquals(task.hashCode(), taskEqual.hashCode());
        assertNotEquals(task, taskUnequalSubtopology);
        assertNotEquals(task.hashCode(), taskUnequalSubtopology.hashCode());
        assertNotEquals(task, taskUnequalPartition);
        assertNotEquals(task.hashCode(), taskUnequalSubtopology.hashCode());
    }

    @Test
    public void taskIdShouldNotAcceptNulls() {
        final Exception exception = assertThrows(NullPointerException.class, () -> new StreamsRebalanceData.TaskId(null, 1));
        assertEquals("Subtopology ID cannot be null", exception.getMessage());
    }

    @Test
    public void testTaskIdCompareTo() {
        final StreamsRebalanceData.TaskId task = new StreamsRebalanceData.TaskId("subtopologyId1", 1);

        assertTrue(task.compareTo(new StreamsRebalanceData.TaskId(task.subtopologyId(), task.partitionId())) == 0);
        assertTrue(task.compareTo(new StreamsRebalanceData.TaskId(task.subtopologyId() + "1", task.partitionId())) < 0);
        assertTrue(task.compareTo(new StreamsRebalanceData.TaskId(task.subtopologyId(), task.partitionId() + 1)) < 0);
        assertTrue(new StreamsRebalanceData.TaskId(task.subtopologyId() + "1", task.partitionId()).compareTo(task) > 0);
        assertTrue(new StreamsRebalanceData.TaskId(task.subtopologyId(), task.partitionId() + 1).compareTo(task) > 0);
    }

    @Test
    public void emptyAssignmentShouldNotBeModifiable() {
        final StreamsRebalanceData.Assignment emptyAssignment = StreamsRebalanceData.Assignment.EMPTY;

        assertThrows(
            UnsupportedOperationException.class,
            () -> emptyAssignment.activeTasks().add(new StreamsRebalanceData.TaskId("subtopologyId1", 1))
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> emptyAssignment.standbyTasks().add(new StreamsRebalanceData.TaskId("subtopologyId1", 1))
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> emptyAssignment.warmupTasks().add(new StreamsRebalanceData.TaskId("subtopologyId1", 1))
        );
    }

    @Test
    public void assignmentShouldNotBeModifiable() {
        final StreamsRebalanceData.Assignment assignment = new StreamsRebalanceData.Assignment(
            new HashSet<>(Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 1))),
            new HashSet<>(Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 2))),
            new HashSet<>(Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 3)))
        );

        assertThrows(
            UnsupportedOperationException.class,
            () -> assignment.activeTasks().add(new StreamsRebalanceData.TaskId("subtopologyId2", 1))
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> assignment.standbyTasks().add(new StreamsRebalanceData.TaskId("subtopologyId2", 2))
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> assignment.warmupTasks().add(new StreamsRebalanceData.TaskId("subtopologyId2", 3))
        );
    }

    @Test
    public void assignmentShouldNotAcceptNulls() {
        final Exception exception1 = assertThrows(NullPointerException.class, () -> new StreamsRebalanceData.Assignment(null, Set.of(), Set.of()));
        assertEquals("Active tasks cannot be null", exception1.getMessage());
        final Exception exception2 = assertThrows(NullPointerException.class, () -> new StreamsRebalanceData.Assignment(Set.of(), null, Set.of()));
        assertEquals("Standby tasks cannot be null", exception2.getMessage());
        final Exception exception3 = assertThrows(NullPointerException.class, () -> new StreamsRebalanceData.Assignment(Set.of(), Set.of(), null));
        assertEquals("Warmup tasks cannot be null", exception3.getMessage());
    }

    @Test
    public void testAssignmentEqualsAndHashCode() {
        final StreamsRebalanceData.TaskId additionalTask = new StreamsRebalanceData.TaskId("subtopologyId2", 1);
        final StreamsRebalanceData.Assignment assignment = new StreamsRebalanceData.Assignment(
            Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 1)),
            Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 2)),
            Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 3))
        );
        final StreamsRebalanceData.Assignment assignmentEqual = new StreamsRebalanceData.Assignment(
            assignment.activeTasks(),
            assignment.standbyTasks(),
            assignment.warmupTasks()
        );
        Set<StreamsRebalanceData.TaskId> unequalActiveTasks = new HashSet<>(assignment.activeTasks());
        unequalActiveTasks.add(additionalTask);
        final StreamsRebalanceData.Assignment assignmentUnequalActiveTasks = new StreamsRebalanceData.Assignment(
            unequalActiveTasks,
            assignment.standbyTasks(),
            assignment.warmupTasks()
        );
        Set<StreamsRebalanceData.TaskId> unequalStandbyTasks = new HashSet<>(assignment.standbyTasks());
        unequalStandbyTasks.add(additionalTask);
        final StreamsRebalanceData.Assignment assignmentUnequalStandbyTasks = new StreamsRebalanceData.Assignment(
            assignment.activeTasks(),
            unequalStandbyTasks,
            assignment.warmupTasks()
        );
        Set<StreamsRebalanceData.TaskId> unequalWarmupTasks = new HashSet<>(assignment.warmupTasks());
        unequalWarmupTasks.add(additionalTask);
        final StreamsRebalanceData.Assignment assignmentUnequalWarmupTasks = new StreamsRebalanceData.Assignment(
            assignment.activeTasks(),
            assignment.standbyTasks(),
            unequalWarmupTasks
        );

        assertEquals(assignment, assignmentEqual);
        assertNotEquals(assignment, assignmentUnequalActiveTasks);
        assertNotEquals(assignment, assignmentUnequalStandbyTasks);
        assertNotEquals(assignment, assignmentUnequalWarmupTasks);
        assertEquals(assignment.hashCode(), assignmentEqual.hashCode());
        assertNotEquals(assignment.hashCode(), assignmentUnequalActiveTasks.hashCode());
        assertNotEquals(assignment.hashCode(), assignmentUnequalStandbyTasks.hashCode());
        assertNotEquals(assignment.hashCode(), assignmentUnequalWarmupTasks.hashCode());
    }

    @Test
    public void shouldCopyAssignment() {
        final StreamsRebalanceData.Assignment assignment = new StreamsRebalanceData.Assignment(
            Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 1)),
            Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 2)),
            Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 3))
        );

        final StreamsRebalanceData.Assignment copy = assignment.copy();

        assertEquals(assignment, copy);
        assertNotSame(assignment, copy);
    }

    @Test
    public void shouldCopyEmptyAssignment() {
        final StreamsRebalanceData.Assignment emptyAssignment = StreamsRebalanceData.Assignment.EMPTY;

        final StreamsRebalanceData.Assignment copy = emptyAssignment.copy();

        assertEquals(emptyAssignment, copy);
        assertNotSame(emptyAssignment, copy);
    }

    @Test
    public void subtopologyShouldNotAcceptNulls() {
        final Exception exception1 = assertThrows(
            NullPointerException.class,
            () -> new StreamsRebalanceData.Subtopology(null, Set.of(), Map.of(), Map.of(), List.of())
        );
        assertEquals("Subtopology ID cannot be null", exception1.getMessage());
        final Exception exception2 = assertThrows(
            NullPointerException.class,
            () -> new StreamsRebalanceData.Subtopology(Set.of(), null, Map.of(), Map.of(), List.of())
        );
        assertEquals("Repartition sink topics cannot be null", exception2.getMessage());
        final Exception exception3 = assertThrows(
            NullPointerException.class,
            () -> new StreamsRebalanceData.Subtopology(Set.of(), Set.of(), null, Map.of(), List.of())
        );
        assertEquals("Repartition source topics cannot be null", exception3.getMessage());
        final Exception exception4 = assertThrows(
            NullPointerException.class,
            () -> new StreamsRebalanceData.Subtopology(Set.of(), Set.of(), Map.of(), null, List.of())
        );
        assertEquals("State changelog topics cannot be null", exception4.getMessage());
        final Exception exception5 = assertThrows(
            NullPointerException.class,
            () -> new StreamsRebalanceData.Subtopology(Set.of(), Set.of(), Map.of(), Map.of(), null)
        );
        assertEquals("Co-partition groups cannot be null", exception5.getMessage());
    }

    @Test
    public void subtopologyShouldNotBeModifiable() {
        final StreamsRebalanceData.Subtopology subtopology = new StreamsRebalanceData.Subtopology(
            new HashSet<>(Set.of("sourceTopic1")),
            new HashSet<>(Set.of("repartitionSinkTopic1")),
            Map.of("repartitionSourceTopic1", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of()))
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
            Map.of("stateChangelogTopic1", new StreamsRebalanceData.TopicInfo(Optional.of(0), Optional.of((short) 1), Map.of()))
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
            new ArrayList<>(List.of(Set.of("sourceTopic1")))
        );

        assertThrows(
            UnsupportedOperationException.class,
            () -> subtopology.sourceTopics().add("sourceTopic2")
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> subtopology.repartitionSinkTopics().add("repartitionSinkTopic2")
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> subtopology.repartitionSourceTopics().put("repartitionSourceTopic2", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of()))
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> subtopology.stateChangelogTopics().put("stateChangelogTopic2", new StreamsRebalanceData.TopicInfo(Optional.of(0), Optional.of((short) 1), Map.of()))
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> subtopology.copartitionGroups().add(Set.of("sourceTopic2"))
        );
    }

    @Test
    public void topicInfoShouldNotAcceptNulls() {
        final Exception exception1 = assertThrows(
            NullPointerException.class,
            () -> new StreamsRebalanceData.TopicInfo(null, Optional.of((short) 1), Map.of())
        );
        assertEquals("Number of partitions cannot be null", exception1.getMessage());
        final Exception exception2 = assertThrows(
            NullPointerException.class,
            () -> new StreamsRebalanceData.TopicInfo(Optional.of(1), null, Map.of())
        );
        assertEquals("Replication factor cannot be null", exception2.getMessage());
        final Exception exception3 = assertThrows(
            NullPointerException.class,
            () -> new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), null)
        );
        assertEquals("Additional topic configs cannot be null", exception3.getMessage());
    }

    @Test
    public void streamsRebalanceDataShouldNotHaveModifiableSubtopologies() {
        final StreamsRebalanceData streamsRebalanceData = new StreamsRebalanceData(new HashMap<>());

        assertThrows(
            UnsupportedOperationException.class,
            () -> streamsRebalanceData.subtopologies().put("subtopologyId2", new StreamsRebalanceData.Subtopology(
                Set.of(),
                Set.of(),
                Map.of(),
                Map.of(),
                List.of()
            ))
        );
    }

    @Test
    public void streamsRebalanceDataShouldNotAcceptNulls() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsRebalanceData(null)
        );
        assertEquals("Subtopologies cannot be null", exception.getMessage());
    }

    @Test
    public void streamsRebalanceDataShouldBeConstructedWithEmptyAssignment() {
        final StreamsRebalanceData streamsRebalanceData = new StreamsRebalanceData(new HashMap<>());

        assertEquals(StreamsRebalanceData.Assignment.EMPTY, streamsRebalanceData.reconciledAssignment());
    }
}
