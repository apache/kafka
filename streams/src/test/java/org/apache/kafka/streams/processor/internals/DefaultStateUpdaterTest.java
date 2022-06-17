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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
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
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verify(changelogReader, times(1)).enforceRestoreActive();
        verify(changelogReader, atLeast(3)).restore(anyMap());
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
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verify(changelogReader, times(3)).enforceRestoreActive();
        verify(changelogReader, atLeast(4)).restore(anyMap());
        verify(task3).completeRestoration(offsetResetter);
        verify(task1).completeRestoration(offsetResetter);
        verify(task2).completeRestoration(offsetResetter);
        verify(changelogReader, never()).transitToUpdateStandby();
    }

    @Test
    public void shouldDrainRestoredActiveTasks() throws Exception {
        assertTrue(stateUpdater.drainRestoredActiveTasks(Duration.ZERO).isEmpty());

        final StreamTask task1 = createStatelessTaskInStateRestoring(TASK_0_0);
        stateUpdater.add(task1);

        verifyDrainingRestoredActiveTasks(task1);

        final StreamTask task2 = createStatelessTaskInStateRestoring(TASK_1_1);
        final StreamTask task3 = createStatelessTaskInStateRestoring(TASK_1_0);
        final StreamTask task4 = createStatelessTaskInStateRestoring(TASK_0_2);
        stateUpdater.add(task2);
        stateUpdater.add(task3);
        stateUpdater.add(task4);

        verifyDrainingRestoredActiveTasks(task2, task3, task4);
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
        verifyRestoredActiveTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
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
        verifyUpdatingStandbyTasks(task4, task3);
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verify(task1).completeRestoration(offsetResetter);
        verify(task2).completeRestoration(offsetResetter);
        verify(changelogReader, atLeast(3)).restore(anyMap());
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

        verifyRestoredActiveTasks(task1, task3);
        verify(task3).completeRestoration(offsetResetter);
        orderVerifier.verify(changelogReader, times(1)).enforceRestoreActive();
        orderVerifier.verify(changelogReader, times(1)).transitToUpdateStandby();
    }

    @Test
    public void shouldRemoveActiveStatefulTask() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        shouldRemoveStatefulTask(task);
    }

    @Test
    public void shouldRemoveStandbyTask() throws Exception {
        final StandbyTask task = createStandbyTaskInStateRunning(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        shouldRemoveStatefulTask(task);
    }

    private void shouldRemoveStatefulTask(final Task task) throws Exception {
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);
        stateUpdater.add(task);

        stateUpdater.remove(TASK_0_0);

        verifyRemovedTasks(task);
        verifyRestoredActiveTasks();
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verify(changelogReader).unregister(Collections.singletonList(TOPIC_PARTITION_A_0));
    }

    @Test
    public void shouldNotRemoveActiveStatefulTaskFromRestoredActiveTasks() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        shouldNotRemoveTaskFromRestoredActiveTasks(task);
    }

    @Test
    public void shouldNotRemoveStatelessTaskFromRestoredActiveTasks() throws Exception {
        final StreamTask task = createStatelessTaskInStateRestoring(TASK_0_0);
        shouldNotRemoveTaskFromRestoredActiveTasks(task);
    }

    private void shouldNotRemoveTaskFromRestoredActiveTasks(final StreamTask task) throws Exception {
        final StreamTask controlTask = createActiveStatefulTaskInStateRestoring(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_B_0));
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.singleton(TOPIC_PARTITION_A_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);
        stateUpdater.add(task);
        stateUpdater.add(controlTask);
        verifyRestoredActiveTasks(task);

        stateUpdater.remove(task.id());
        stateUpdater.remove(controlTask.id());

        verifyRemovedTasks(controlTask);
        verifyRestoredActiveTasks(task);
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
    }

    @Test
    public void shouldNotRemoveActiveStatefulTaskFromFailedTasks() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        shouldNotRemoveTaskFromFailedTasks(task);
    }

    @Test
    public void shouldNotRemoveStandbyTaskFromFailedTasks() throws Exception {
        final StandbyTask task = createStandbyTaskInStateRunning(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        shouldNotRemoveTaskFromFailedTasks(task);
    }

    private void shouldNotRemoveTaskFromFailedTasks(final Task task) throws Exception {
        final StreamTask controlTask = createActiveStatefulTaskInStateRestoring(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StreamsException streamsException = new StreamsException("Something happened", task.id());
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);
        doNothing()
            .doThrow(streamsException)
            .doNothing()
            .when(changelogReader).restore(anyMap());
        stateUpdater.add(task);
        stateUpdater.add(controlTask);
        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task), streamsException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);

        stateUpdater.remove(task.id());
        stateUpdater.remove(controlTask.id());

        verifyRemovedTasks(controlTask);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks();
        verifyRestoredActiveTasks();
    }

    @Test
    public void shouldDrainRemovedTasks() throws Exception {
        assertTrue(stateUpdater.drainRemovedTasks().isEmpty());
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);

        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_B_0));
        stateUpdater.add(task1);
        stateUpdater.remove(task1.id());

        verifyDrainingRemovedTasks(task1);

        final StreamTask task2 = createActiveStatefulTaskInStateRestoring(TASK_1_1, Collections.singletonList(TOPIC_PARTITION_C_0));
        final StreamTask task3 = createActiveStatefulTaskInStateRestoring(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StreamTask task4 = createActiveStatefulTaskInStateRestoring(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_D_0));
        stateUpdater.add(task2);
        stateUpdater.remove(task2.id());
        stateUpdater.add(task3);
        stateUpdater.remove(task3.id());
        stateUpdater.add(task4);
        stateUpdater.remove(task4.id());

        verifyDrainingRemovedTasks(task2, task3, task4);
    }

    @Test
    public void shouldAddFailedTasksToQueueWhenRestoreThrowsStreamsExceptionWithoutTask() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StandbyTask task2 = createStandbyTaskInStateRunning(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final String exceptionMessage = "The Streams were crossed!";
        final StreamsException streamsException = new StreamsException(exceptionMessage);
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2)
        );
        doNothing().doThrow(streamsException).when(changelogReader).restore(updatingTasks);

        stateUpdater.add(task1);
        stateUpdater.add(task2);

        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task1, task2), streamsException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyRemovedTasks();
        verifyUpdatingTasks();
        verifyRestoredActiveTasks();
    }

    @Test
    public void shouldAddFailedTasksToQueueWhenRestoreThrowsStreamsExceptionWithTask() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StreamTask task2 = createActiveStatefulTaskInStateRestoring(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StandbyTask task3 = createStandbyTaskInStateRunning(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        final String exceptionMessage = "The Streams were crossed!";
        final StreamsException streamsException1 = new StreamsException(exceptionMessage, task1.id());
        final StreamsException streamsException2 = new StreamsException(exceptionMessage, task3.id());
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
            .doThrow(streamsException1)
            .when(changelogReader).restore(updatingTasksBeforeFirstThrow);
        doNothing()
            .doThrow(streamsException2)
            .when(changelogReader).restore(updatingTasksBeforeSecondThrow);

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        final ExceptionAndTasks expectedExceptionAndTasks1 = new ExceptionAndTasks(mkSet(task1), streamsException1);
        final ExceptionAndTasks expectedExceptionAndTasks2 = new ExceptionAndTasks(mkSet(task3), streamsException2);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks1, expectedExceptionAndTasks2);
        verifyUpdatingTasks(task2);
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
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

        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task1, task2), taskCorruptedException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks(task3);
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
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

        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task1, task2), illegalStateException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks();
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
    }

    @Test
    public void shouldDrainFailedTasksAndExceptions() throws Exception {
        assertTrue(stateUpdater.drainExceptionsAndFailedTasks().isEmpty());

        final StreamTask task1 = createActiveStatefulTaskInStateRestoring(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StreamTask task2 = createActiveStatefulTaskInStateRestoring(TASK_1_1, Collections.singletonList(TOPIC_PARTITION_C_0));
        final StreamTask task3 = createActiveStatefulTaskInStateRestoring(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StreamTask task4 = createActiveStatefulTaskInStateRestoring(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_D_0));
        final String exceptionMessage = "The Streams were crossed!";
        final StreamsException streamsException1 = new StreamsException(exceptionMessage, task1.id());
        final Map<TaskId, Task> updatingTasks1 = mkMap(
            mkEntry(task1.id(), task1)
        );
        doThrow(streamsException1)
            .when(changelogReader).restore(updatingTasks1);
        final StreamsException streamsException2 = new StreamsException(exceptionMessage, task2.id());
        final StreamsException streamsException3 = new StreamsException(exceptionMessage, task3.id());
        final StreamsException streamsException4 = new StreamsException(exceptionMessage, task4.id());
        final Map<TaskId, Task> updatingTasks2 = mkMap(
            mkEntry(task2.id(), task2),
            mkEntry(task3.id(), task3),
            mkEntry(task4.id(), task4)
        );
        doThrow(streamsException2).when(changelogReader).restore(updatingTasks2);
        final Map<TaskId, Task> updatingTasks3 = mkMap(
            mkEntry(task3.id(), task3),
            mkEntry(task4.id(), task4)
        );
        doThrow(streamsException3).when(changelogReader).restore(updatingTasks3);
        final Map<TaskId, Task> updatingTasks4 = mkMap(
            mkEntry(task4.id(), task4)
        );
        doThrow(streamsException4).when(changelogReader).restore(updatingTasks4);

        stateUpdater.add(task1);

        final ExceptionAndTasks expectedExceptionAndTasks1 = new ExceptionAndTasks(mkSet(task1), streamsException1);
        verifyDrainingExceptionsAndFailedTasks(expectedExceptionAndTasks1);

        stateUpdater.add(task2);
        stateUpdater.add(task3);
        stateUpdater.add(task4);

        final ExceptionAndTasks expectedExceptionAndTasks2 = new ExceptionAndTasks(mkSet(task2), streamsException2);
        final ExceptionAndTasks expectedExceptionAndTasks3 = new ExceptionAndTasks(mkSet(task3), streamsException3);
        final ExceptionAndTasks expectedExceptionAndTasks4 = new ExceptionAndTasks(mkSet(task4), streamsException4);
        verifyDrainingExceptionsAndFailedTasks(expectedExceptionAndTasks2, expectedExceptionAndTasks3, expectedExceptionAndTasks4);
    }

    private void verifyRestoredActiveTasks(final StreamTask... tasks) throws Exception {
        if (tasks.length == 0) {
            assertTrue(stateUpdater.getRestoredActiveTasks().isEmpty());
        } else {
            final Set<StreamTask> expectedRestoredTasks = mkSet(tasks);
            final Set<StreamTask> restoredTasks = new HashSet<>();
            waitForCondition(
                () -> {
                    restoredTasks.addAll(stateUpdater.getRestoredActiveTasks());
                    return restoredTasks.containsAll(expectedRestoredTasks);
                },
                VERIFICATION_TIMEOUT,
                "Did not get all restored active task within the given timeout!"
            );
            assertEquals(expectedRestoredTasks.size(), restoredTasks.size());
            assertTrue(restoredTasks.stream().allMatch(task -> task.state() == State.RESTORING));
        }
    }

    private void verifyDrainingRestoredActiveTasks(final StreamTask... tasks) throws Exception {
        final Set<StreamTask> expectedRestoredTasks = mkSet(tasks);
        final Set<StreamTask> restoredTasks = new HashSet<>();
        waitForCondition(
            () -> {
                restoredTasks.addAll(stateUpdater.drainRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)));
                return restoredTasks.containsAll(expectedRestoredTasks);
            },
            VERIFICATION_TIMEOUT,
            "Did not get all restored active task within the given timeout!"
        );
        assertEquals(expectedRestoredTasks.size(), restoredTasks.size());
        assertTrue(stateUpdater.drainRestoredActiveTasks(Duration.ZERO).isEmpty());
    }

    private void verifyUpdatingTasks(final Task... tasks) throws Exception {
        if (tasks.length == 0) {
            assertTrue(stateUpdater.getUpdatingTasks().isEmpty());
        } else {
            final Set<Task> expectedUpdatingTasks = mkSet(tasks);
            final Set<Task> updatingTasks = new HashSet<>();
            waitForCondition(
                () -> {
                    updatingTasks.addAll(stateUpdater.getUpdatingTasks());
                    return updatingTasks.containsAll(expectedUpdatingTasks);
                },
                VERIFICATION_TIMEOUT,
                "Did not get all updating task within the given timeout!"
            );
            assertEquals(expectedUpdatingTasks.size(), updatingTasks.size());
            assertTrue(updatingTasks.stream().allMatch(task -> task.state() == State.RESTORING));
        }
    }

    private void verifyUpdatingStandbyTasks(final StandbyTask... tasks) throws Exception {
        final Set<StandbyTask> expectedStandbyTasks = mkSet(tasks);
        final Set<StandbyTask> standbyTasks = new HashSet<>();
        waitForCondition(
            () -> {
                standbyTasks.addAll(stateUpdater.getUpdatingStandbyTasks());
                return standbyTasks.containsAll(expectedStandbyTasks);
            },
            VERIFICATION_TIMEOUT,
            "Did not see all standby task within the given timeout!"
        );
        assertEquals(expectedStandbyTasks.size(), standbyTasks.size());
        assertTrue(standbyTasks.stream().allMatch(task -> task.state() == State.RUNNING));
    }

    private void verifyRemovedTasks(final Task... tasks) throws Exception {
        if (tasks.length == 0) {
            assertTrue(stateUpdater.getRemovedTasks().isEmpty());
        } else {
            final Set<Task> expectedRemovedTasks = mkSet(tasks);
            final Set<Task> removedTasks = new HashSet<>();
            waitForCondition(
                () -> {
                    removedTasks.addAll(stateUpdater.getRemovedTasks());
                    return removedTasks.containsAll(mkSet(tasks));
                },
                VERIFICATION_TIMEOUT,
                "Did not get all removed task within the given timeout!"
            );
            assertEquals(expectedRemovedTasks.size(), removedTasks.size());
            assertTrue(removedTasks.stream()
                .allMatch(task -> task.isActive() && task.state() == State.RESTORING
                        || !task.isActive() && task.state() == State.RUNNING));
        }
    }

    private void verifyDrainingRemovedTasks(final Task... tasks) throws Exception {
        final Set<Task> expectedRemovedTasks = mkSet(tasks);
        final Set<Task> removedTasks = new HashSet<>();
        waitForCondition(
            () -> {
                removedTasks.addAll(stateUpdater.drainRemovedTasks());
                return removedTasks.containsAll(mkSet(tasks));
            },
            VERIFICATION_TIMEOUT,
            "Did not get all restored active task within the given timeout!"
        );
        assertEquals(expectedRemovedTasks.size(), removedTasks.size());
        assertTrue(stateUpdater.drainRemovedTasks().isEmpty());
    }

    private void verifyExceptionsAndFailedTasks(final ExceptionAndTasks... exceptionsAndTasks) throws Exception {
        final List<ExceptionAndTasks> expectedExceptionAndTasks = Arrays.asList(exceptionsAndTasks);
        final List<ExceptionAndTasks> failedTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                failedTasks.addAll(stateUpdater.getExceptionsAndFailedTasks());
                return failedTasks.containsAll(expectedExceptionAndTasks);
            },
            VERIFICATION_TIMEOUT,
            "Did not get all exceptions and failed tasks within the given timeout!"
        );
        assertEquals(expectedExceptionAndTasks.size(), failedTasks.size());
    }

    private void verifyDrainingExceptionsAndFailedTasks(final ExceptionAndTasks... exceptionsAndTasks) throws Exception {
        final List<ExceptionAndTasks> expectedExceptionAndTasks = Arrays.asList(exceptionsAndTasks);
        final List<ExceptionAndTasks> failedTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                failedTasks.addAll(stateUpdater.drainExceptionsAndFailedTasks());
                return failedTasks.containsAll(expectedExceptionAndTasks);
            },
            VERIFICATION_TIMEOUT,
            "Did not get all exceptions and failed tasks within the given timeout!"
        );
        assertEquals(expectedExceptionAndTasks.size(), failedTasks.size());
        assertTrue(stateUpdater.drainExceptionsAndFailedTasks().isEmpty());
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