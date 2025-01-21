package org.apache.kafka.clients.consumer.internals;


import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    public void testTaskIdCompareTo() {
        final StreamsRebalanceData.TaskId task = new StreamsRebalanceData.TaskId("subtopologyId1", 1);

        assertTrue(task.compareTo(new StreamsRebalanceData.TaskId(task.subtopologyId(), task.partitionId())) == 0);
        assertTrue(task.compareTo(new StreamsRebalanceData.TaskId(task.subtopologyId() + "1", task.partitionId())) < 0);
        assertTrue(task.compareTo(new StreamsRebalanceData.TaskId(task.subtopologyId(), task.partitionId() + 1)) < 0);
        assertTrue(new StreamsRebalanceData.TaskId(task.subtopologyId() + "1", task.partitionId()).compareTo(task) > 0);
        assertTrue(new StreamsRebalanceData.TaskId(task.subtopologyId(), task.partitionId() + 1).compareTo(task) > 0);
    }

    @Test
    public void shouldNotModifyEmptyAssignment() {
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
    public void shouldNotModifyAssignment() {
        final StreamsRebalanceData.Assignment assignment = new StreamsRebalanceData.Assignment(
            Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 1)),
            Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 2)),
            Set.of(new StreamsRebalanceData.TaskId("subtopologyId1", 3))
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
}