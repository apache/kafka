package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultStateUpdaterTest {

    private final static long CALL_TIMEOUT = 1000;
    private final static long VERIFICATION_TIMEOUT = 15000;
    private final static TopicPartition TOPIC_PARTITION_A_0 = new TopicPartition("topicA", 0);
    private final static TopicPartition TOPIC_PARTITION_B_0 = new TopicPartition("topicB", 0);
    private final static TopicPartition TOPIC_PARTITION_C_0 = new TopicPartition("topicC", 0);
    private final static TaskId TASK_0_0 = new TaskId(0, 0);
    private final static TaskId TASK_1_0 = new TaskId(1, 0);
    private final static TaskId TASK_0_2 = new TaskId(0, 2);

    private final ChangelogReader changelogReader = mock(ChangelogReader.class);
    private final java.util.function.Consumer<Set<TopicPartition>> offsetResetter = (topicPartitions) -> {};
    private final DefaultStateUpdater stateUpdater = new DefaultStateUpdater(changelogReader, offsetResetter, Time.SYSTEM);

    @AfterEach
    public void tearDown() {
        stateUpdater.shutdown(Duration.ofMinutes(1));
    }

    @Test
    public void shouldShutdownStateUpdater() {
        final StreamTask task = createActiveStatefulTaskInStateCreated(TASK_0_0, Arrays.asList(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        stateUpdater.add(task);

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verify(changelogReader).clear();
    }

    @Test
    public void shouldShutdownStateUpdaterAndRestart() {
        final StreamTask task1 = createActiveStatelessTask(TASK_0_0);
        stateUpdater.add(task1);

        stateUpdater.shutdown(Duration.ofMinutes(1));

        final StreamTask task2 = createActiveStatelessTask(TASK_1_0);
        stateUpdater.add(task2);

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verify(changelogReader, times(2)).clear();
    }

    @Test
    public void shouldRestoreActiveStatefulTask() throws Exception {
        final StreamTask task =
            createActiveStatefulTaskInStateCreated(TASK_0_0, Arrays.asList(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);

        stateUpdater.add(task);

        final List<StreamTask> restoredTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                restoredTasks.addAll(stateUpdater.getRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)));
                return !restoredTasks.isEmpty();
            },
            VERIFICATION_TIMEOUT,
            "Did not get any restored active task within the given timeout!"
        );

        assertIterableEquals(Collections.singletonList(task), restoredTasks);
        verify(task).initializeIfNeeded();
        verify(changelogReader, times(3)).restore(anyMap());
        verify(task).completeRestoration(offsetResetter);
        // Todo: check for checkpoint creation before change to RUNNING state

    }

    @Test
    public void shouldAddMultipleActiveStatefulTasks() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StreamTask task2 = createActiveStatefulTaskInStateCreated(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StreamTask task3 = createActiveStatefulTaskInStateCreated(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_B_0))
            .thenReturn(mkSet(TOPIC_PARTITION_C_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        final List<StreamTask> restoredTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                restoredTasks.addAll(stateUpdater.getRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)));
                return restoredTasks.size() == 3;
            },
            VERIFICATION_TIMEOUT,
            "Did not get any restored active task within the given timeout!"
        );

        assertIterableEquals(Arrays.asList(task1, task2, task3), restoredTasks);
        verify(changelogReader).register(TOPIC_PARTITION_A_0, task1.stateManager());
        verify(changelogReader).register(TOPIC_PARTITION_B_0, task2.stateManager());
        verify(changelogReader).register(TOPIC_PARTITION_C_0, task3.stateManager());
        verify(changelogReader, times(4)).restore(anyMap());
        verify(changelogReader).unregister(Collections.singletonList((TOPIC_PARTITION_A_0)));
        verify(changelogReader).unregister(Collections.singletonList((TOPIC_PARTITION_B_0)));
        verify(changelogReader).unregister(Collections.singletonList((TOPIC_PARTITION_C_0)));
        verify(task1.stateManager()).checkpoint();
        verify(task2.stateManager()).checkpoint();
        verify(task3.stateManager()).checkpoint();
    }

    @Test
    public void shouldImmediatelyAddStatelessTaskToRestoredTasks() throws Exception {
        final StreamTask task = createActiveStatelessTask(TASK_0_0);

        stateUpdater.add(task);

        final List<StreamTask> restoredTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                restoredTasks.addAll(stateUpdater.getRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)));
                return !restoredTasks.isEmpty();
            },
            VERIFICATION_TIMEOUT,
            "Did not get any restored active task within the given timeout!"
        );

        assertIterableEquals(Collections.singletonList(task), restoredTasks);
        verify(changelogReader, never()).register(any(), any());
        verify(changelogReader, never()).unregister(any());
        verify(task.stateManager(), never()).checkpoint();
    }

    @Test
    public void shouldImmediatelyAddMultipleStatelessTasksToRestoredTasks() throws Exception {
        final StreamTask task1 = createActiveStatelessTask(TASK_0_0);
        final StreamTask task2 = createActiveStatelessTask(TASK_0_2);
        final StreamTask task3 = createActiveStatelessTask(TASK_1_0);

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        final List<StreamTask> restoredTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                restoredTasks.addAll(stateUpdater.getRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)));
                return restoredTasks.size() == 3;
            },
            VERIFICATION_TIMEOUT,
            "Did not get any restored active task within the given timeout!"
        );
        assertIterableEquals(Arrays.asList(task1, task2, task3), restoredTasks);
        verify(changelogReader, never()).register(any(), any());
        verify(changelogReader, never()).unregister(any());
        verify(task1.stateManager(), never()).checkpoint();
        verify(task2.stateManager(), never()).checkpoint();
        verify(task3.stateManager(), never()).checkpoint();
    }

    @Test
    public void shouldUpdateStandbyTask() throws Exception {
        final StandbyTask task = createStandbyTask(TASK_0_0, Arrays.asList(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);

        stateUpdater.add(task);

        waitForCondition(
            () -> !stateUpdater.getAllTasks().isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task within the given timeout!"
        );
        final Map<TaskId, Task> updatingTasks = mkMap(mkEntry(task.id(), task));
        verify(changelogReader, timeout(VERIFICATION_TIMEOUT).atLeast(2)).restore(updatingTasks);
        verify(changelogReader).register(TOPIC_PARTITION_A_0, task.stateManager());
        verify(changelogReader).register(TOPIC_PARTITION_B_0, task.stateManager());
    }

    @Test
    public void shouldUpdateMultipleStandbyTasks() throws Exception {
        final StandbyTask task1 = createStandbyTask(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StandbyTask task2 = createStandbyTask(TASK_0_2, Collections.singletonList(TOPIC_PARTITION_B_0));
        final StandbyTask task3 = createStandbyTask(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        final List<Task> allTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                allTasks.addAll(stateUpdater.getAllTasks());
                return allTasks.size() == 3;
            },
            VERIFICATION_TIMEOUT,
            "Did not see the given number of updating tasks within the given timeout!"
        );
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2),
            mkEntry(task3.id(), task3)
        );
        verify(changelogReader, timeout(VERIFICATION_TIMEOUT).atLeast(2)).restore(updatingTasks);
        verify(changelogReader).register(TOPIC_PARTITION_A_0, task1.stateManager());
        verify(changelogReader).register(TOPIC_PARTITION_B_0, task2.stateManager());
        verify(changelogReader).register(TOPIC_PARTITION_C_0, task3.stateManager());
    }

    @Test
    public void shouldAddAllTypesOfTasks() throws Exception {
        final StreamTask activeStatefulTask = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StreamTask activeStatelessTask = createActiveStatelessTask(TASK_0_2);
        final StandbyTask standbyTask = createStandbyTask(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_C_0));
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(Collections.singleton(TOPIC_PARTITION_A_0))
            .thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);

        stateUpdater.add(activeStatefulTask);
        stateUpdater.add(activeStatelessTask);
        stateUpdater.add(standbyTask);

        final List<Task> allTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                allTasks.addAll(stateUpdater.getAllTasks());
                return allTasks.size() == 3;
            },
            VERIFICATION_TIMEOUT,
            "Did not see the given number of updating tasks within the given timeout!"
        );
        final List<StreamTask> restoredTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                restoredTasks.addAll(stateUpdater.getRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)));
                return restoredTasks.size() == 2;
            },
            VERIFICATION_TIMEOUT,
            "Did not get any restored active task within the given timeout!"
        );
        assertIterableEquals(Arrays.asList(activeStatelessTask, activeStatefulTask), restoredTasks);
        verify(changelogReader, timeout(VERIFICATION_TIMEOUT).atLeast(3)).restore(anyMap());
        verify(changelogReader).register(TOPIC_PARTITION_A_0, activeStatefulTask.stateManager());
        verify(changelogReader).register(TOPIC_PARTITION_C_0, standbyTask.stateManager());
        verify(changelogReader).unregister(Collections.singletonList((TOPIC_PARTITION_A_0)));
        verify(activeStatefulTask.stateManager()).checkpoint();
        verify(activeStatelessTask.stateManager(), never()).checkpoint();
        verify(changelogReader, never()).unregister(Collections.singletonList((TOPIC_PARTITION_C_0)));
    }

    @Test
    public void shouldRemoveNotRestoredStatefulTask() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.add(task);
        waitForCondition(
            () -> !stateUpdater.getAllTasks().isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task within the given timeout!"
        );

        stateUpdater.remove(task);

        verify(changelogReader, timeout(VERIFICATION_TIMEOUT)).unregister(Collections.singletonList(TOPIC_PARTITION_A_0));
        verify(task.stateManager()).checkpoint();
        waitForCondition(
            () -> stateUpdater.getAllTasks().isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task removed within the given timeout!"
        );
    }

    @Test
    public void shouldRemoveRestoredStatefulTask() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        when(changelogReader.completedChangelogs()).thenReturn(Collections.singleton(TOPIC_PARTITION_A_0));
        when(changelogReader.allChangelogsCompleted()).thenReturn(true);
        stateUpdater.add(task);
        verify(changelogReader, timeout(VERIFICATION_TIMEOUT)).register(TOPIC_PARTITION_A_0, task.stateManager());
        verify(changelogReader, timeout(VERIFICATION_TIMEOUT)).unregister(Collections.singletonList(TOPIC_PARTITION_A_0));
        verify(task.stateManager()).checkpoint();
        waitForCondition(
            () -> !stateUpdater.getAllTasks().isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task within the given timeout!"
        );

        stateUpdater.remove(task);

        waitForCondition(
            () -> stateUpdater.getAllTasks().isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task removed within the given timeout!"
        );
        waitForCondition(
            () -> stateUpdater.getRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)).isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task removed within the given timeout!"
        );
    }

    @Test
    public void shouldRemoveStatelessTask() throws Exception {
        final StreamTask task = createActiveStatelessTask(TASK_0_0);
        stateUpdater.add(task);
        waitForCondition(
            () -> !stateUpdater.getAllTasks().isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task within the given timeout!"
        );

        stateUpdater.remove(task);

        waitForCondition(
            () -> stateUpdater.getAllTasks().isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task removed within the given timeout!"
        );
        waitForCondition(
            () -> stateUpdater.getRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)).isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task removed within the given timeout!"
        );
        verify(task.stateManager(), never()).checkpoint();
    }

    @Test
    public void shouldRemoveStandbyTask() throws Exception {
        final StandbyTask task = createStandbyTask(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        stateUpdater.add(task);
        verify(changelogReader, timeout(VERIFICATION_TIMEOUT)).register(TOPIC_PARTITION_A_0, task.stateManager());
        waitForCondition(
            () -> !stateUpdater.getAllTasks().isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task within the given timeout!"
        );

        stateUpdater.remove(task);

        verify(changelogReader, timeout(VERIFICATION_TIMEOUT)).unregister(Collections.singletonList(TOPIC_PARTITION_A_0));
        verify(task.stateManager()).checkpoint();
        waitForCondition(
            () -> stateUpdater.getAllTasks().isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task removed within the given timeout!"
        );
        waitForCondition(
            () -> stateUpdater.getRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)).isEmpty(),
            VERIFICATION_TIMEOUT,
            "Did not see updating task removed within the given timeout!"
        );
    }

    @Test
    public void shouldAddExceptionToExceptionQueueWhenRegisterThrows() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        setUpRegisterToThrow();
        shouldAddExceptionToExceptionQueueWhenRegisterThrows(task);
    }

    private void setUpRegisterToThrow() {
        doThrow(IllegalStateException.class).when(changelogReader).register(any(), any());
    }

    private void shouldAddExceptionToExceptionQueueWhenRegisterThrows(final Task task) throws Exception {
        stateUpdater.add(task);

        verifyRuntimeException();
        verify(task.stateManager(), never()).checkpoint();
    }

    @Test
    public void shouldAddExceptionToExceptionQueueWhenUnregisterThrowsDuringRemove() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        setUpUnregisterToThrow();
        stateUpdater.add(task);

        stateUpdater.remove(task);

        verifyRuntimeException();
        verify(task.stateManager()).checkpoint();
    }

    @Test
    public void shouldAddExceptionToExceptionQueueWhenUnregisterThrowsAfterRestorationCompletes() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        setUpUnregisterToThrow();
        when(changelogReader.completedChangelogs()).thenReturn(mkSet(TOPIC_PARTITION_A_0));
        when(changelogReader.allChangelogsCompleted()).thenReturn(true);
        stateUpdater.add(task);

        verifyRuntimeException();
        verify(task.stateManager()).checkpoint();
    }

    private void setUpUnregisterToThrow() {
        doThrow(IllegalStateException.class).when(changelogReader).unregister(any());
    }

    private void verifyRuntimeException() throws Exception {
        final List<RuntimeException> runtimeExceptions = getExceptions();

        assertEquals(1, runtimeExceptions.size());
        assertTrue(runtimeExceptions.get(0) instanceof IllegalStateException);
        assertTrue(stateUpdater.getAllTasks().isEmpty());
    }

    @Test
    public void shouldAddExceptionToExceptionQueueWhenRestoreThrowsStreamsExceptionMappedToATask() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final String expectedMessage = "The Streams were crossed!";
        final StreamsException expectedStreamsException = new StreamsException(expectedMessage, task.id());
        final Map<TaskId, Task> updatingTasks = mkMap(mkEntry(task.id(), task));
        doThrow(expectedStreamsException).when(changelogReader).restore(updatingTasks);

        stateUpdater.add(task);

        final List<RuntimeException> runtimeExceptions = getExceptions();

        assertEquals(1, runtimeExceptions.size());
        assertTrue(runtimeExceptions.get(0) instanceof StreamsException);
        assertTrue(((StreamsException) runtimeExceptions.get(0)).taskId().isPresent());
        assertEquals(expectedMessage, runtimeExceptions.get(0).getMessage());
        assertTrue(stateUpdater.getAllTasks().isEmpty());
        verify(task.stateManager()).checkpoint();
    }

    @Test
    public void shouldAddExceptionToExceptionQueueWhenRestoreThrowsStreamsExceptionNotMappedToATask() throws Exception {
        final StreamTask task = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final String expectedMessage = "The Streams were crossed!";
        final StreamsException expectedStreamsException = new StreamsException(expectedMessage);
        final Map<TaskId, Task> updatingTasks = mkMap(mkEntry(task.id(), task));
        doThrow(expectedStreamsException).doNothing().when(changelogReader).restore(updatingTasks);

        stateUpdater.add(task);

        final List<RuntimeException> runtimeExceptions = getExceptions();

        assertEquals(1, runtimeExceptions.size());
        assertTrue(runtimeExceptions.get(0) instanceof StreamsException);
        assertFalse(((StreamsException) runtimeExceptions.get(0)).taskId().isPresent());
    }

    @Test
    public void shouldAddExceptionToExceptionQueueWhenRestoreThrowsTaskCorruptedException() throws Exception {
        final StreamTask task1 = createActiveStatefulTaskInStateCreated(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_A_0));
        final StreamTask task2 = createActiveStatefulTaskInStateCreated(TASK_1_0, Collections.singletonList(TOPIC_PARTITION_B_0));
        final Set<TaskId> expectedTaskIds = mkSet(task1.id(), task2.id());
        final TaskCorruptedException taskCorruptedException = new TaskCorruptedException(expectedTaskIds);
        final Map<TaskId, Task> updatingTasks = mkMap(mkEntry(task1.id(), task1), mkEntry(task2.id(), task2));
        doThrow(taskCorruptedException).when(changelogReader).restore(updatingTasks);

        stateUpdater.add(task1);
        stateUpdater.add(task2);

        final List<RuntimeException> runtimeExceptions = getExceptions();

        assertEquals(1, runtimeExceptions.size());
        assertTrue(runtimeExceptions.get(0) instanceof TaskCorruptedException);
        assertEquals(expectedTaskIds, ((TaskCorruptedException) runtimeExceptions.get(0)).corruptedTasks());
        assertTrue(stateUpdater.getAllTasks().isEmpty());
    }

    private List<RuntimeException> getExceptions() throws Exception {
        final List<RuntimeException> runtimeExceptions = new ArrayList<>();
        waitForCondition(
            () -> {
                runtimeExceptions.addAll(stateUpdater.getExceptions());
                return !runtimeExceptions.isEmpty();
            },
            VERIFICATION_TIMEOUT,
            "Did not get any runtime exceptions within the given timeout!"
        );

        return runtimeExceptions;
    }

    @Test
    public void shouldThrowIfActiveStatefulTaskNotInRestoringState() {
        final StreamTask task = createActiveStatefulTask(TASK_0_0, Collections.singletonList(TOPIC_PARTITION_B_0));
        when(task.state()).thenReturn(State.CREATED);
        assertThrows(IllegalStateException.class, () -> stateUpdater.add(task));
    }

    private StreamTask createActiveStatefulTaskInStateCreated(final TaskId taskId,
                                                              final Collection<TopicPartition> changelogPartitions) {
        final StreamTask task = createActiveStatefulTask(taskId, changelogPartitions);
        when(task.state()).thenReturn(State.CREATED);
        return task;
    }

    private StreamTask createActiveStatefulTaskInStateSuspended(final TaskId taskId,
                                                                final Collection<TopicPartition> changelogPartitions) {
        final StreamTask task = createActiveStatefulTask(taskId, changelogPartitions);
        when(task.state()).thenReturn(State.SUSPENDED);
        return task;
    }

    private StreamTask createActiveStatefulTask(final TaskId taskId,
                                                final Collection<TopicPartition> changelogPartitions) {
        final StreamTask task = mock(StreamTask.class);
        setupStatefulTask(task, taskId, changelogPartitions);
        when(task.isActive()).thenReturn(true);
        return task;
    }

    private StreamTask createActiveStatelessTask(final TaskId taskId) {
        final StreamTask task = mock(StreamTask.class);
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        when(task.changelogPartitions()).thenReturn(Collections.emptySet());
        when(task.stateManager()).thenReturn(stateManager);
        when(task.isActive()).thenReturn(true);
        when(task.id()).thenReturn(taskId);
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
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        when(task.changelogPartitions()).thenReturn(changelogPartitions);
        when(task.stateManager()).thenReturn(stateManager);
        when(task.id()).thenReturn(taskId);
    }

}