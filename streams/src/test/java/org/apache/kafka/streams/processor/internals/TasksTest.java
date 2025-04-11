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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.State;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.standbyTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statefulTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statelessTask;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TasksTest {

    private static final TopicPartition TOPIC_PARTITION_A_0 = new TopicPartition("topicA", 0);
    private static final TopicPartition TOPIC_PARTITION_A_1 = new TopicPartition("topicA", 1);
    private static final TopicPartition TOPIC_PARTITION_B_0 = new TopicPartition("topicB", 0);
    private static final TopicPartition TOPIC_PARTITION_B_1 = new TopicPartition("topicB", 1);
    private static final TaskId TASK_0_0 = new TaskId(0, 0);
    private static final TaskId TASK_0_1 = new TaskId(0, 1);
    private static final TaskId TASK_1_0 = new TaskId(1, 0);
    private static final TaskId TASK_1_1 = new TaskId(1, 1);

    private final Tasks tasks = new Tasks(new LogContext());

    @Test
    public void shouldCheckStateWhenRemoveTask() {
        final StreamTask closedTask = statefulTask(TASK_0_0, Set.of(TOPIC_PARTITION_A_0)).inState(State.CLOSED).build();
        final StandbyTask suspendedTask = standbyTask(TASK_0_1, Set.of(TOPIC_PARTITION_A_1)).inState(State.SUSPENDED).build();
        final StreamTask runningTask = statelessTask(TASK_1_0).inState(State.RUNNING).build();

        tasks.addActiveTasks(Set.of(closedTask, runningTask));
        tasks.addStandbyTasks(Collections.singletonList(suspendedTask));

        assertDoesNotThrow(() -> tasks.removeTask(closedTask));
        assertDoesNotThrow(() -> tasks.removeTask(suspendedTask));
        assertThrows(IllegalStateException.class, () -> tasks.removeTask(runningTask));
    }

    @Test
    public void shouldKeepAddedTasks() {
        final StreamTask statefulTask = statefulTask(TASK_0_0, Set.of(TOPIC_PARTITION_A_0)).build();
        final StandbyTask standbyTask = standbyTask(TASK_0_1, Set.of(TOPIC_PARTITION_A_1)).build();
        final StreamTask statelessTask = statelessTask(TASK_1_0).build();

        tasks.addActiveTasks(Set.of(statefulTask, statelessTask));
        tasks.addStandbyTasks(Collections.singletonList(standbyTask));

        assertEquals(statefulTask, tasks.task(statefulTask.id()));
        assertEquals(statelessTask, tasks.task(statelessTask.id()));
        assertEquals(standbyTask, tasks.task(standbyTask.id()));

        assertEquals(Set.of(statefulTask, statelessTask), new HashSet<>(tasks.activeTasks()));
        assertEquals(Set.of(statefulTask, statelessTask, standbyTask), tasks.allTasks());
        assertEquals(Set.of(statefulTask, standbyTask), tasks.tasks(Set.of(statefulTask.id(), standbyTask.id())));
        assertEquals(Set.of(statefulTask.id(), statelessTask.id(), standbyTask.id()), tasks.allTaskIds());
        assertEquals(
            mkMap(
                mkEntry(statefulTask.id(), statefulTask),
                mkEntry(statelessTask.id(), statelessTask),
                mkEntry(standbyTask.id(), standbyTask)
            ),
            tasks.allTasksPerId());
        assertTrue(tasks.contains(statefulTask.id()));
        assertTrue(tasks.contains(statelessTask.id()));
        assertTrue(tasks.contains(statefulTask.id()));
    }

    @Test
    public void shouldDrainPendingTasksToCreate() {
        tasks.addPendingActiveTasksToCreate(mkMap(
            mkEntry(new TaskId(0, 0, "A"), Set.of(TOPIC_PARTITION_A_0)),
            mkEntry(new TaskId(0, 1, "A"), Set.of(TOPIC_PARTITION_A_1)),
            mkEntry(new TaskId(0, 0, "B"), Set.of(TOPIC_PARTITION_B_0)),
            mkEntry(new TaskId(0, 1, "B"), Set.of(TOPIC_PARTITION_B_1))
        ));

        tasks.addPendingStandbyTasksToCreate(mkMap(
            mkEntry(new TaskId(0, 0, "A"), Set.of(TOPIC_PARTITION_A_0)),
            mkEntry(new TaskId(0, 1, "A"), Set.of(TOPIC_PARTITION_A_1)),
            mkEntry(new TaskId(0, 0, "B"), Set.of(TOPIC_PARTITION_B_0)),
            mkEntry(new TaskId(0, 1, "B"), Set.of(TOPIC_PARTITION_B_1))
        ));

        assertEquals(mkMap(
            mkEntry(new TaskId(0, 0, "A"), Set.of(TOPIC_PARTITION_A_0)),
            mkEntry(new TaskId(0, 1, "A"), Set.of(TOPIC_PARTITION_A_1))
        ), tasks.drainPendingActiveTasksForTopologies(Set.of("A")));

        assertEquals(mkMap(
            mkEntry(new TaskId(0, 0, "A"), Set.of(TOPIC_PARTITION_A_0)),
            mkEntry(new TaskId(0, 1, "A"), Set.of(TOPIC_PARTITION_A_1))
        ), tasks.drainPendingStandbyTasksForTopologies(Set.of("A")));

        tasks.clearPendingTasksToCreate();

        assertEquals(Collections.emptyMap(), tasks.drainPendingActiveTasksForTopologies(Set.of("B")));
        assertEquals(Collections.emptyMap(), tasks.drainPendingStandbyTasksForTopologies(Set.of("B")));
    }

    @Test
    public void shouldVerifyIfPendingTaskToInitExist() {
        assertFalse(tasks.hasPendingTasksToInit());

        final StreamTask activeTask = statefulTask(TASK_0_0, Set.of(TOPIC_PARTITION_B_0)).build();
        tasks.addPendingTasksToInit(Collections.singleton(activeTask));
        assertTrue(tasks.hasPendingTasksToInit());

        final StandbyTask standbyTask = standbyTask(TASK_1_0, Set.of(TOPIC_PARTITION_A_1)).build();
        tasks.addPendingTasksToInit(Collections.singleton(standbyTask));
        assertTrue(tasks.hasPendingTasksToInit());

        assertTrue(tasks.hasPendingTasksToInit());

        final Set<Task> tasksToInit = tasks.drainPendingTasksToInit();
        assertEquals(2, tasksToInit.size());
        assertTrue(tasksToInit.contains(activeTask));
        assertTrue(tasksToInit.contains(standbyTask));
        assertFalse(tasks.hasPendingTasksToInit());
    }

    @Test
    public void shouldVerifyIfPendingActiveTaskToInitAreDrained() {
        final StreamTask activeTask1 = statefulTask(TASK_0_0, Set.of(TOPIC_PARTITION_B_0)).build();
        final StreamTask activeTask2 = statefulTask(TASK_0_1, Set.of(TOPIC_PARTITION_B_1)).build();
        final StandbyTask standbyTask1 = standbyTask(TASK_1_0, Set.of(TOPIC_PARTITION_A_0)).build();
        final StandbyTask standbyTask2 = standbyTask(TASK_1_1, Set.of(TOPIC_PARTITION_A_1)).build();
        tasks.addPendingTasksToInit(Set.of(activeTask1, activeTask2, standbyTask1, standbyTask2));

        final Set<Task> activeTasksToInit = tasks.drainPendingActiveTasksToInit();
        assertEquals(2, activeTasksToInit.size());
        assertTrue(activeTasksToInit.containsAll(Set.of(activeTask1, activeTask2)));
        assertFalse(activeTasksToInit.containsAll(Set.of(standbyTask1, standbyTask2)));
        assertEquals(2, tasks.pendingTasksToInit().size());
        assertTrue(tasks.hasPendingTasksToInit());
        assertTrue(tasks.pendingTasksToInit().containsAll(Set.of(standbyTask1, standbyTask2)));
    }

    @Test
    public void shouldAddFailedTask() {
        final StreamTask activeTask1 = statefulTask(TASK_0_0, Set.of(TOPIC_PARTITION_B_0)).build();
        final StreamTask activeTask2 = statefulTask(TASK_0_1, Set.of(TOPIC_PARTITION_B_1)).build();
        tasks.addTask(activeTask2);

        tasks.addFailedTask(activeTask1);

        assertEquals(activeTask1, tasks.task(TASK_0_0));
        assertEquals(activeTask2, tasks.task(TASK_0_1));
        assertTrue(tasks.allTasks().contains(activeTask1));
        assertTrue(tasks.allTasks().contains(activeTask2));
        assertFalse(tasks.allNonFailedTasks().contains(activeTask1));
        assertTrue(tasks.allNonFailedTasks().contains(activeTask2));
    }

    @Test
    public void shouldRemoveFailedTask() {
        final StreamTask activeTask1 = statefulTask(TASK_0_0, Set.of(TOPIC_PARTITION_B_0))
            .inState(State.SUSPENDED).build();
        tasks.addFailedTask(activeTask1);

        tasks.removeTask(activeTask1);
        assertFalse(tasks.allNonFailedTasks().contains(activeTask1));
        assertFalse(tasks.allTasks().contains(activeTask1));

        tasks.addTask(activeTask1);
        assertTrue(tasks.allNonFailedTasks().contains(activeTask1));
    }

    @Test
    public void shouldClearFailedTask() {
        final StreamTask activeTask1 = statefulTask(TASK_0_0, Set.of(TOPIC_PARTITION_B_0))
            .inState(State.SUSPENDED).build();
        tasks.addFailedTask(activeTask1);

        tasks.clear();
        assertFalse(tasks.allNonFailedTasks().contains(activeTask1));
        assertFalse(tasks.allTasks().contains(activeTask1));

        tasks.addTask(activeTask1);
        assertTrue(tasks.allNonFailedTasks().contains(activeTask1));
    }
}
