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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateUpdater.ExceptionAndTasks;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultStateUpdaterTest {

    private final static long CALL_TIMEOUT = 1000;
    private final static long VERIFICATION_TIMEOUT = 15000;
    private final static TopicPartition TOPIC_PARTITION_A_0 = new TopicPartition("topicA", 0);
    private final static TopicPartition TOPIC_PARTITION_B_0 = new TopicPartition("topicB", 0);
    private final static TopicPartition TOPIC_PARTITION_C_0 = new TopicPartition("topicC", 0);
    private final static TopicPartition TOPIC_PARTITION_D_0 = new TopicPartition("topicD", 0);
    private final static TaskId TASK_0_0 = new TaskId(0, 0);
    private final static TaskId TASK_0_2 = new TaskId(0, 2);
    private final static TaskId TASK_1_0 = new TaskId(1, 0);
    private final static TaskId TASK_1_1 = new TaskId(1, 1);

    private final ChangelogReader changelogReader = mock(ChangelogReader.class);
    private final java.util.function.Consumer<Set<TopicPartition>> offsetResetter = topicPartitions -> { };
    private final DefaultStateUpdater stateUpdater = new DefaultStateUpdater(changelogReader, offsetResetter, Time.SYSTEM);

    @AfterEach
    public void tearDown() {
        stateUpdater.shutdown(Duration.ofMinutes(1));
    }

    @Test
    public void shouldShutdownStateUpdater() {
        final StreamTask task = createStatelessTaskInStateRestoring(TASK_0_0);
        stateUpdater.add(task);

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verify(changelogReader).clear();
    }

    @Test
    public void shouldShutdownStateUpdaterAndRestart() {
        final StreamTask task1 = createStatelessTaskInStateRestoring(TASK_0_0);
        stateUpdater.add(task1);

        stateUpdater.shutdown(Duration.ofMinutes(1));

        final StreamTask task2 = createStatelessTaskInStateRestoring(TASK_1_0);
        stateUpdater.add(task2);

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verify(changelogReader, times(2)).clear();
    }

    @Test
    public void shouldThrowIfStatelessTaskNotInStateRestoring() {
        shouldThrowIfActiveTaskNotInStateRestoring(createStatelessTask(TASK_0_0));
    }

    @Test
    public void shouldThrowIfStatefulTaskNotInStateRestoring() {
        shouldThrowIfActiveTaskNotInStateRestoring(createActiveStatefulTask(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0)));
    }

    private void shouldThrowIfActiveTaskNotInStateRestoring(final StreamTask task) {
        shouldThrowIfTaskNotInGivenState(task, State.RESTORING);
    }

    @Test
    public void shouldThrowIfStandbyTaskNotInStateRunning() {
        final StandbyTask task = createStandbyTask(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_B_0));
        shouldThrowIfTaskNotInGivenState(task, State.RUNNING);
    }

    private void shouldThrowIfTaskNotInGivenState(final Task task, final State correctState) {
        for (final State state : State.values()) {
            if (state != correctState) {
                when(task.state()).thenReturn(state);
                assertThrows(IllegalStateException.class, () -> stateUpdater.add(task));
            }
        }
    }

    @Test
    public void shouldImmediatelyAddSingleStatelessTaskToRestoredTasks() throws Exception {
        final StreamTask task1 = createStatelessTaskInStateRestoring(TASK_0_0);
        shouldImmediatelyAddStatelessTasksToRestoredTasks(task1);
    }

    @Test
    public void shouldImmediatelyAddMultipleStatelessTasksToRestoredTasks() throws Exception {
        final StreamTask task1 = createStatelessTaskInStateRestoring(TASK_0_0);
        final StreamTask task2 = createStatelessTaskInStateRestoring(TASK_0_2);
        final StreamTask task3 = createStatelessTaskInStateRestoring(TASK_1_0);
        shouldImmediatelyAddStatelessTasksToRestoredTasks(task1, task2, task3);
    }

    private void shouldImmediatelyAddStatelessTasksToRestoredTasks(final StreamTask... tasks) throws Exception {
        for (final StreamTask task : tasks) {
            stateUpdater.add(task);
        }

        verifyRestoredActiveTasks(tasks);
        assertTrue(stateUpdater.getAllTasks().isEmpty());
    }

    @Test
    public void shouldRestoreSingleActiveStatefulTask() throws Exception {
        final StreamTask task =
            createActiveStatefulTaskInStateRestoring(TASK_0_0, Arrays.asList(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);

        stateUpdater.add(task);

        verifyRestoredActiveTasks(task);
        assertTrue(stateUpdater.getAllTasks().isEmpty());
        verify(changelogReader, times(1)).enforceRestoreActive();
        verify(changelogReader, atLeast(1)).restore(anyMap());
        verify(task).completeRestoration(offsetResetter);
        verify(changelogReader, never()).transitToUpdateStandby();
    }

    @Test
    public void shouldRestoreMultipleActiveStatefulTasks() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StreamTask task2 = createActiveStatefulTaskInStateRestoring(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StreamTask task3 = createActiveStatefulTaskInStateRestoring(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_C_0))
            .thenReturn(mkSet(TOPIC_PARTITION_C_0, TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_C_0, TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        verifyRestoredActiveTasks(task3, task1, task2);
        assertTrue(stateUpdater.getAllTasks().isEmpty());
        verify(changelogReader, times(3)).enforceRestoreActive();
        verify(changelogReader, atLeast(4)).restore(anyMap());
        verify(task3).completeRestoration(offsetResetter);
        verify(task1).completeRestoration(offsetResetter);
        verify(task2).completeRestoration(offsetResetter);
        verify(changelogReader, never()).transitToUpdateStandby();
    }

    @Test
    public void shouldUpdateSingleStandbyTask() throws Exception {
        final StandbyTask task = createStandbyTaskInStateRunning(
            TASK_0_0,
            Arrays.asList(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0)
        );
        shouldUpdateStandbyTasks(task);
    }

    @Test
    public void shouldUpdateMultipleStandbyTasks() throws Exception {
        final StandbyTask task1 = createStandbyTaskInStateRunning(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StandbyTask task2 = createStandbyTaskInStateRunning(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StandbyTask task3 = createStandbyTaskInStateRunning(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        shouldUpdateStandbyTasks(task1, task2, task3);
    }

    private void shouldUpdateStandbyTasks(final StandbyTask... tasks) throws Exception {
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);

        for (final StandbyTask task : tasks) {
            stateUpdater.add(task);
        }

        verifyUpdatingStandbyTasks(tasks);
        verify(changelogReader, times(1)).transitToUpdateStandby();
        verify(changelogReader, timeout(VERIFICATION_TIMEOUT).atLeast(1)).restore(anyMap());
        verify(changelogReader, never()).enforceRestoreActive();
    }

    @Test
    public void shouldRestoreActiveStatefulTasksAndUpdateStandbyTasks() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StreamTask task2 = createActiveStatefulTaskInStateRestoring(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StandbyTask task3 = createStandbyTaskInStateRunning(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        final StandbyTask task4 = createStandbyTaskInStateRunning(TASK_1_1, Collections.singletonList(TOPIC_PARTITION_D_0));
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);
        stateUpdater.add(task4);

        verifyRestoredActiveTasks(task2, task1);
        verify(task1).completeRestoration(offsetResetter);
        verify(task2).completeRestoration(offsetResetter);
        verify(changelogReader, atLeast(3)).restore(anyMap());
        verifyUpdatingStandbyTasks(task4, task3);
        final InOrder orderVerifier = inOrder(changelogReader, task1, task2);
        orderVerifier.verify(changelogReader, times(2)).enforceRestoreActive();
        orderVerifier.verify(changelogReader, times(1)).transitToUpdateStandby();
    }

    @Test
    public void shouldRestoreActiveStatefulTaskThenUpdateStandbyTaskAndAgainRestoreActiveStatefulTask() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StandbyTask task2 = createStandbyTaskInStateRunning(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        final StreamTask task3 = createActiveStatefulTaskInStateRestoring(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);

        stateUpdater.add(task1);
        stateUpdater.add(task2);

        verifyRestoredActiveTasks(task1);
        verify(task1).completeRestoration(offsetResetter);
        verifyUpdatingStandbyTasks(task2);
        final InOrder orderVerifier = inOrder(changelogReader);
        orderVerifier.verify(changelogReader, times(1)).enforceRestoreActive();
        orderVerifier.verify(changelogReader, times(1)).transitToUpdateStandby();

        stateUpdater.add(task3);

        verifyRestoredActiveTasks(task3);
        verify(task3).completeRestoration(offsetResetter);
        orderVerifier.verify(changelogReader, times(1)).enforceRestoreActive();
        orderVerifier.verify(changelogReader, times(1)).transitToUpdateStandby();
    }

    @Test
    public void shouldAddFailedTasksToQueueWhenRestoreThrowsStreamsExceptionWithoutTask() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StandbyTask task2 = createStandbyTaskInStateRunning(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final String expectedMessage = "The Streams were crossed!";
        final StreamsException expectedStreamsException = new StreamsException(expectedMessage);
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2)
        );
        doNothing().doThrow(expectedStreamsException).doNothing().when(changelogReader).restore(updatingTasks);

        stateUpdater.add(task1);
        stateUpdater.add(task2);

        final List<ExceptionAndTasks> failedTasks = getFailedTasks(1);
        assertEquals(1, failedTasks.size());
        final ExceptionAndTasks actualFailedTasks = failedTasks.get(0);
        assertEquals(2, actualFailedTasks.tasks.size());
        assertTrue(actualFailedTasks.tasks.containsAll(Arrays.asList(task1, task2)));
        assertTrue(actualFailedTasks.exception instanceof StreamsException);
        final StreamsException actualException = (StreamsException) actualFailedTasks.exception;
        assertFalse(actualException.taskId().isPresent());
        assertEquals(expectedMessage, actualException.getMessage());
        assertTrue(stateUpdater.getAllTasks().isEmpty());
    }

    @Test
    public void shouldAddFailedTasksToQueueWhenRestoreThrowsStreamsExceptionWithTask() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StreamTask task2 = createActiveStatefulTaskInStateRestoring(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StandbyTask task3 = createStandbyTaskInStateRunning(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        final String expectedMessage = "The Streams were crossed!";
        final StreamsException expectedStreamsException1 = new StreamsException(expectedMessage, task1.id());
        final StreamsException expectedStreamsException2 = new StreamsException(expectedMessage, task3.id());
        final Map<TaskId, Task> updatingTasksBeforeFirstThrow = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2),
            mkEntry(task3.id(), task3)
        );
        final Map<TaskId, Task> updatingTasksBeforeSecondThrow = mkMap(
            mkEntry(task2.id(), task2),
            mkEntry(task3.id(), task3)
        );
        doNothing()
            .doThrow(expectedStreamsException1)
            .when(changelogReader).restore(updatingTasksBeforeFirstThrow);
        doNothing()
            .doThrow(expectedStreamsException2)
            .when(changelogReader).restore(updatingTasksBeforeSecondThrow);

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        final List<ExceptionAndTasks> failedTasks = getFailedTasks(2);
        assertEquals(2, failedTasks.size());
        final ExceptionAndTasks actualFailedTasks1 = failedTasks.get(0);
        assertEquals(1, actualFailedTasks1.tasks.size());
        assertTrue(actualFailedTasks1.tasks.contains(task1));
        assertTrue(actualFailedTasks1.exception instanceof StreamsException);
        final StreamsException actualException1 = (StreamsException) actualFailedTasks1.exception;
        assertTrue(actualException1.taskId().isPresent());
        assertEquals(task1.id(), actualException1.taskId().get());
        assertEquals(expectedMessage, actualException1.getMessage());
        final ExceptionAndTasks actualFailedTasks2 = failedTasks.get(1);
        assertEquals(1, actualFailedTasks2.tasks.size());
        assertTrue(actualFailedTasks2.tasks.contains(task3));
        assertTrue(actualFailedTasks2.exception instanceof StreamsException);
        final StreamsException actualException2 = (StreamsException) actualFailedTasks2.exception;
        assertTrue(actualException2.taskId().isPresent());
        assertEquals(task3.id(), actualException2.taskId().get());
        assertEquals(expectedMessage, actualException2.getMessage());
        assertEquals(1, stateUpdater.getAllTasks().size());
        assertTrue(stateUpdater.getAllTasks().contains(task2));
    }

    @Test
    public void shouldAddFailedTasksToQueueWhenRestoreThrowsTaskCorruptedException() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StandbyTask task2 = createStandbyTaskInStateRunning(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StreamTask task3 = createActiveStatefulTaskInStateRestoring(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        final Set<TaskId> expectedTaskIds = mkSet(task1.id(), task2.id());
        final TaskCorruptedException taskCorruptedException = new TaskCorruptedException(expectedTaskIds);
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2),
            mkEntry(task3.id(), task3)
        );
        doNothing().doThrow(taskCorruptedException).doNothing().when(changelogReader).restore(updatingTasks);

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        final List<ExceptionAndTasks> failedTasks = getFailedTasks(1);
        assertEquals(1, failedTasks.size());
        final List<Task> expectedFailedTasks = Arrays.asList(task1, task2);
        final ExceptionAndTasks actualFailedTasks = failedTasks.get(0);
        assertEquals(2, actualFailedTasks.tasks.size());
        assertTrue(actualFailedTasks.tasks.containsAll(expectedFailedTasks));
        assertTrue(actualFailedTasks.exception instanceof TaskCorruptedException);
        final TaskCorruptedException actualException = (TaskCorruptedException) actualFailedTasks.exception;
        final Set<TaskId> corruptedTasks = actualException.corruptedTasks();
        assertTrue(corruptedTasks.containsAll(expectedFailedTasks.stream().map(Task::id).collect(Collectors.toList())));
        assertEquals(1, stateUpdater.getAllTasks().size());
        assertTrue(stateUpdater.getAllTasks().contains(task3));
    }

    @Test
    public void shouldAddFailedTasksToQueueWhenUncaughtExceptionIsThrown() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StandbyTask task2 = createStandbyTaskInStateRunning(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final IllegalStateException illegalStateException = new IllegalStateException("Nobody expects the Spanish inquisition!");
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2)
        );
        doThrow(illegalStateException).when(changelogReader).restore(updatingTasks);

        stateUpdater.add(task1);
        stateUpdater.add(task2);

        final List<ExceptionAndTasks> failedTasks = getFailedTasks(1);
        final List<Task> expectedFailedTasks = Arrays.asList(task1, task2);
        final ExceptionAndTasks actualFailedTasks = failedTasks.get(0);
        assertEquals(2, actualFailedTasks.tasks.size());
        assertTrue(actualFailedTasks.tasks.containsAll(expectedFailedTasks));
        assertTrue(actualFailedTasks.exception instanceof IllegalStateException);
        final IllegalStateException actualException = (IllegalStateException) actualFailedTasks.exception;
        assertEquals(actualException.getMessage(), illegalStateException.getMessage());
        assertTrue(stateUpdater.getAllTasks().isEmpty());
    }

    private void verifyRestoredActiveTasks(final StreamTask... tasks) throws Exception {
        final Set<StreamTask> expectedRestoredTasks = mkSet(tasks);
        final Set<StreamTask> restoredTasks = new HashSet<>();
        waitForCondition(
            () -> {
                restoredTasks.addAll(stateUpdater.getRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)));
                return restoredTasks.size() == expectedRestoredTasks.size();
            },
            VERIFICATION_TIMEOUT,
            "Did not get any restored active task within the given timeout!"
        );
        assertTrue(restoredTasks.containsAll(expectedRestoredTasks));
        assertEquals(expectedRestoredTasks.size(), restoredTasks.stream().filter(task -> task.state() == State.RESTORING).count());
    }

    private void verifyUpdatingStandbyTasks(final StandbyTask... tasks) throws Exception {
        final Set<StandbyTask> expectedStandbyTasks = mkSet(tasks);
        final Set<StandbyTask> standbyTasks = new HashSet<>();
        waitForCondition(
            () -> {
                standbyTasks.addAll(stateUpdater.getUpdatingStandbyTasks());
                return standbyTasks.size() == expectedStandbyTasks.size();
            },
            VERIFICATION_TIMEOUT,
            "Did not see all standby task within the given timeout!"
        );
        assertTrue(standbyTasks.containsAll(expectedStandbyTasks));
        assertEquals(expectedStandbyTasks.size(), standbyTasks.stream().filter(t -> t.state() == State.RUNNING).count());
    }

    private List<ExceptionAndTasks> getFailedTasks(final int expectedCount) throws Exception {
        final List<ExceptionAndTasks> failedTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                failedTasks.addAll(stateUpdater.getFailedTasksAndExceptions());
                return failedTasks.size() >= expectedCount;
            },
            VERIFICATION_TIMEOUT,
            "Did not get enough failed tasks within the given timeout!"
        );

        return failedTasks;
    }

    private StreamTask createActiveStatefulTaskInStateRestoring(final TaskId taskId,
                                                                final Collection<TopicPartition> changelogPartitions) {
        final StreamTask task = createActiveStatefulTask(taskId, changelogPartitions);
        when(task.state()).thenReturn(State.RESTORING);
        return task;
    }

    private StreamTask createActiveStatefulTask(final TaskId taskId,
                                                final Collection<TopicPartition> changelogPartitions) {
        final StreamTask task = mock(StreamTask.class);
        setupStatefulTask(task, taskId, changelogPartitions);
        when(task.isActive()).thenReturn(true);
        return task;
    }

    private StreamTask createStatelessTaskInStateRestoring(final TaskId taskId) {
        final StreamTask task = createStatelessTask(taskId);
        when(task.state()).thenReturn(State.RESTORING);
        return task;
    }

    private StreamTask createStatelessTask(final TaskId taskId) {
        final StreamTask task = mock(StreamTask.class);
        when(task.changelogPartitions()).thenReturn(Collections.emptySet());
        when(task.isActive()).thenReturn(true);
        when(task.id()).thenReturn(taskId);
        return task;
    }

    private StandbyTask createStandbyTaskInStateRunning(final TaskId taskId,
                                                        final Collection<TopicPartition> changelogPartitions) {
        final StandbyTask task = createStandbyTask(taskId, changelogPartitions);
        when(task.state()).thenReturn(State.RUNNING);
        return task;
    }

    private StandbyTask createStandbyTask(final TaskId taskId,
                                          final Collection<TopicPartition> changelogPartitions) {
        final StandbyTask task = mock(StandbyTask.class);
        setupStatefulTask(task, taskId, changelogPartitions);
        when(task.isActive()).thenReturn(false);
        return task;
    }

    private void setupStatefulTask(final Task task,
                                   final TaskId taskId,
                                   final Collection<TopicPartition> changelogPartitions) {
        when(task.changelogPartitions()).thenReturn(changelogPartitions);
        when(task.id()).thenReturn(taskId);
    }
}