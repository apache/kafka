package org.apache.kafka.clients.consumer.internals;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
}