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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.StateDirectory.TaskDirectory;
import org.apache.kafka.streams.processor.internals.StateUpdater.ExceptionAndTask;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.apache.kafka.streams.processor.internals.tasks.DefaultTaskManager;
import org.apache.kafka.streams.processor.internals.testutil.DummyStreamsConfig;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.intersection;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.union;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.standbyTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statefulTask;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TaskManagerTest {

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";

    private final TaskId taskId00 = new TaskId(0, 0);
    private final TopicPartition t1p0 = new TopicPartition(topic1, 0);
    private final TopicPartition t1p0changelog = new TopicPartition("changelog", 0);
    private final Set<TopicPartition> taskId00Partitions = Set.of(t1p0);
    private final Set<TopicPartition> taskId00ChangelogPartitions = Set.of(t1p0changelog);
    private final Map<TaskId, Set<TopicPartition>> taskId00Assignment = singletonMap(taskId00, taskId00Partitions);

    private final TaskId taskId01 = new TaskId(0, 1);
    private final TopicPartition t1p1 = new TopicPartition(topic1, 1);
    private final TopicPartition t2p2 = new TopicPartition(topic2, 1);
    private final TopicPartition t1p1changelog = new TopicPartition("changelog", 1);
    private final TopicPartition t1p1changelog2 = new TopicPartition("changelog2", 1);
    private final Set<TopicPartition> taskId01Partitions = Set.of(t1p1);
    private final Set<TopicPartition> taskId01ChangelogPartitions = Set.of(t1p1changelog);
    private final Map<TaskId, Set<TopicPartition>> taskId01Assignment = singletonMap(taskId01, taskId01Partitions);

    private final TaskId taskId02 = new TaskId(0, 2);
    private final TopicPartition t1p2 = new TopicPartition(topic1, 2);
    private final TopicPartition t1p2changelog = new TopicPartition("changelog", 2);
    private final Set<TopicPartition> taskId02Partitions = Set.of(t1p2);
    private final Set<TopicPartition> taskId02ChangelogPartitions = Set.of(t1p2changelog);

    private final TaskId taskId03 = new TaskId(0, 3);
    private final TopicPartition t1p3 = new TopicPartition(topic1, 3);
    private final TopicPartition t1p3changelog = new TopicPartition("changelog", 3);
    private final Set<TopicPartition> taskId03Partitions = Set.of(t1p3);
    private final Set<TopicPartition> taskId03ChangelogPartitions = Set.of(t1p3changelog);

    private final TaskId taskId04 = new TaskId(0, 4);
    private final TopicPartition t1p4 = new TopicPartition(topic1, 4);
    private final TopicPartition t1p4changelog = new TopicPartition("changelog", 4);
    private final Set<TopicPartition> taskId04Partitions = Set.of(t1p4);
    private final Set<TopicPartition> taskId04ChangelogPartitions = Set.of(t1p4changelog);

    private final TaskId taskId05 = new TaskId(0, 5);
    private final TopicPartition t1p5 = new TopicPartition(topic1, 5);
    private final TopicPartition t1p5changelog = new TopicPartition("changelog", 5);
    private final Set<TopicPartition> taskId05Partitions = Set.of(t1p5);
    private final Set<TopicPartition> taskId05ChangelogPartitions = Set.of(t1p5changelog);

    private final TaskId taskId10 = new TaskId(1, 0);
    private final TopicPartition t2p0 = new TopicPartition(topic2, 0);
    private final Set<TopicPartition> taskId10Partitions = Set.of(t2p0);
    private final Set<TopicPartition> assignment = singleton(new TopicPartition("assignment", 0));

    final java.util.function.Consumer<Set<TopicPartition>> noOpResetter = partitions -> { };

    @Mock
    private InternalTopologyBuilder topologyBuilder;
    @Mock
    private StateDirectory stateDirectory;
    @Mock
    private ChangelogReader changeLogReader;
    @Mock
    private Consumer<byte[], byte[]> consumer;
    @Mock
    private ActiveTaskCreator activeTaskCreator;
    @Mock
    private StandbyTaskCreator standbyTaskCreator;
    @Mock
    private Admin adminClient;
    @Mock
    private ProcessorStateManager stateManager;
    final StateUpdater stateUpdater = mock(StateUpdater.class);
    final DefaultTaskManager schedulingTaskManager = mock(DefaultTaskManager.class);

    private TaskManager taskManager;
    private TopologyMetadata topologyMetadata;
    private final Time time = new MockTime();

    @TempDir
    Path testFolder;

    @BeforeEach
    public void setUp() {
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, null);
    }

    private TaskManager setUpTaskManager(final ProcessingMode processingMode) {
        return setUpTaskManager(processingMode, null, false);
    }

    private TaskManager setUpTaskManager(final ProcessingMode processingMode, final TasksRegistry tasks) {
        return setUpTaskManager(processingMode, tasks, false);
    }

    private TaskManager setUpTaskManager(final ProcessingMode processingMode,
                                         final TasksRegistry tasks,
                                         final boolean processingThreadsEnabled) {
        topologyMetadata = new TopologyMetadata(topologyBuilder, new DummyStreamsConfig(processingMode));
        final TaskManager taskManager = new TaskManager(
            time,
            changeLogReader,
            ProcessId.randomProcessId(),
            "taskManagerTest",
            activeTaskCreator,
            standbyTaskCreator,
            tasks != null ? tasks : new Tasks(new LogContext()),
            topologyMetadata,
            adminClient,
            stateDirectory,
            stateUpdater,
            processingThreadsEnabled ? schedulingTaskManager : null
        );
        taskManager.setMainConsumer(consumer);
        return taskManager;
    }

    @Test
    public void shouldLockAllTasksOnCorruptionWithProcessingThreads() {
        final StreamTask activeTask1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.activeTaskIds()).thenReturn(Set.of(taskId00, taskId01));
        when(tasks.task(taskId00)).thenReturn(activeTask1);
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.handleCorruption(Set.of(taskId00));

        verify(consumer).assignment();
        verify(schedulingTaskManager).lockTasks(Set.of(taskId00, taskId01));
        verify(schedulingTaskManager).unlockTasks(Set.of(taskId00, taskId01));
    }

    @Test
    public void shouldLockCommitableTasksOnCorruptionWithProcessingThreads() {
        final StreamTask activeTask1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask activeTask2 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.commit(Set.of(activeTask1, activeTask2));

        verify(schedulingTaskManager).lockTasks(Set.of(taskId00, taskId01));
        verify(schedulingTaskManager).unlockTasks(Set.of(taskId00, taskId01));
    }

    @Test
    public void shouldLockActiveOnHandleAssignmentWithProcessingThreads() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTaskIds()).thenReturn(Set.of(taskId00, taskId01));
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.handleAssignment(
            mkMap(mkEntry(taskId00, taskId00Partitions)),
                mkMap(mkEntry(taskId01, taskId01Partitions))
        );

        verify(schedulingTaskManager).lockTasks(Set.of(taskId00, taskId01));
        verify(schedulingTaskManager).unlockTasks(Set.of(taskId00, taskId01));
    }

    @Test
    public void shouldLockAffectedTasksOnHandleRevocation() {
        final StreamTask activeTask1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask activeTask2 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasks()).thenReturn(Set.of(activeTask1, activeTask2));
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.handleRevocation(taskId01Partitions);

        verify(schedulingTaskManager).lockTasks(Set.of(taskId00, taskId01));
        verify(schedulingTaskManager).unlockTasks(Set.of(taskId00, taskId01));
    }

    @Test
    public void shouldLockTasksOnClose() {
        final StreamTask activeTask1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask activeTask2 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasks()).thenReturn(Set.of(activeTask1, activeTask2));
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.closeAndCleanUpTasks(Set.of(activeTask1), Set.of(), false);

        verify(schedulingTaskManager).lockTasks(Set.of(taskId00));
        verify(schedulingTaskManager).unlockTasks(Set.of(taskId00));
    }

    @Test
    public void shouldResumePollingForPartitionsWithAvailableSpaceForAllActiveTasks() {
        final StreamTask activeTask1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask activeTask2 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.activeTasks()).thenReturn(Set.of(activeTask1, activeTask2));

        taskManager.resumePollingForPartitionsWithAvailableSpace();

        verify(activeTask1).resumePollingForPartitionsWithAvailableSpace();
        verify(activeTask2).resumePollingForPartitionsWithAvailableSpace();
    }

    @Test
    public void shouldUpdateLagForAllActiveTasks() {
        final StreamTask activeTask1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask activeTask2 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.activeTasks()).thenReturn(Set.of(activeTask1, activeTask2));

        taskManager.updateLags();

        verify(activeTask1).updateLags();
        verify(activeTask2).updateLags();
    }

    @Test
    public void shouldRemoveUnusedActiveTaskFromStateUpdaterAndCloseCleanly() {
        final StreamTask activeTaskToClose = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(activeTaskToClose));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(activeTaskToClose.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(activeTaskToClose));

        taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap());

        verify(activeTaskToClose).suspend();
        verify(activeTaskToClose).closeClean();
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldRemoveUnusedFailedActiveTaskFromStateUpdaterAndCloseDirty() {
        final StreamTask activeTaskToClose = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(activeTaskToClose));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(activeTaskToClose.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(activeTaskToClose, new RuntimeException("KABOOM!")));

        taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap());

        verify(activeTaskToClose).prepareCommit(false);
        verify(activeTaskToClose).suspend();
        verify(activeTaskToClose).closeDirty();
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldRemoveUnusedStandbyTaskFromStateUpdaterAndCloseCleanly() {
        final StandbyTask standbyTaskToClose = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTaskToClose));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(standbyTaskToClose.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(standbyTaskToClose));

        taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap());

        verify(standbyTaskToClose).suspend();
        verify(standbyTaskToClose).closeClean();
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldRemoveUnusedFailedStandbyTaskFromStateUpdaterAndCloseDirty() {
        final StandbyTask standbyTaskToClose = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTaskToClose));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(standbyTaskToClose.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(standbyTaskToClose, new RuntimeException("KABOOM!")));

        taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap());

        verify(standbyTaskToClose).prepareCommit(false);
        verify(standbyTaskToClose).suspend();
        verify(standbyTaskToClose).closeDirty();
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldCollectFailedTaskFromStateUpdaterAndRethrow() {
        final StandbyTask failedStandbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(failedStandbyTask));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(failedStandbyTask.id())).thenReturn(future);
        final RuntimeException kaboom = new RuntimeException("KABOOM!");
        future.completeExceptionally(kaboom);
        when(stateUpdater.drainExceptionsAndFailedTasks())
            .thenReturn(singletonList(new ExceptionAndTask(new RuntimeException("KABOOM!"), failedStandbyTask)));

        final StreamsException exception = assertThrows(
            StreamsException.class,
            () -> taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap())
        );

        assertEquals("Encounter unexpected fatal error for task " + failedStandbyTask.id(), exception.getMessage());
        assertInstanceOf(RuntimeException.class, exception.getCause());
        assertEquals(kaboom.getMessage(), exception.getCause().getMessage());
        verify(tasks).addFailedTask(failedStandbyTask);
    }

    @Test
    public void shouldUpdateInputPartitionOfActiveTaskInStateUpdater() {
        final StreamTask activeTaskToUpdateInputPartitions = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final Set<TopicPartition> newInputPartitions = taskId02Partitions;
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(activeTaskToUpdateInputPartitions));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(activeTaskToUpdateInputPartitions.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(activeTaskToUpdateInputPartitions));

        taskManager.handleAssignment(
            mkMap(mkEntry(activeTaskToUpdateInputPartitions.id(), newInputPartitions)),
            Collections.emptyMap()
        );

        final InOrder updateInputPartitionsThenAddBack = inOrder(stateUpdater, activeTaskToUpdateInputPartitions);
        updateInputPartitionsThenAddBack.verify(activeTaskToUpdateInputPartitions)
            .updateInputPartitions(eq(newInputPartitions), any());
        updateInputPartitionsThenAddBack.verify(stateUpdater).add(activeTaskToUpdateInputPartitions);
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldRecycleActiveTaskInStateUpdater() {
        final StreamTask activeTaskToRecycle = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final StandbyTask recycledStandbyTask = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(activeTaskToRecycle));
        when(standbyTaskCreator.createStandbyTaskFromActive(activeTaskToRecycle, taskId03Partitions))
            .thenReturn(recycledStandbyTask);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(taskId03)).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(activeTaskToRecycle));

        taskManager.handleAssignment(
            Collections.emptyMap(),
            mkMap(mkEntry(activeTaskToRecycle.id(), activeTaskToRecycle.inputPartitions()))
        );

        verify(tasks).addPendingTasksToInit(Collections.singleton(recycledStandbyTask));
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldHandleExceptionThrownDuringRecyclingActiveTask() {
        final StreamTask activeTaskToRecycle = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(activeTaskToRecycle));
        when(standbyTaskCreator.createStandbyTaskFromActive(activeTaskToRecycle, activeTaskToRecycle.inputPartitions()))
            .thenThrow(new RuntimeException());
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(activeTaskToRecycle.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(activeTaskToRecycle));

        assertThrows(
            StreamsException.class,
            () -> taskManager.handleAssignment(
                Collections.emptyMap(),
                mkMap(mkEntry(activeTaskToRecycle.id(), activeTaskToRecycle.inputPartitions()))
            )
        );

        verify(stateUpdater, never()).add(any());
        verify(tasks, never()).addPendingTasksToInit(Collections.singleton(any()));
        verify(activeTaskToRecycle).closeDirty();
    }

    @Test
    public void shouldRecycleStandbyTaskInStateUpdater() {
        final StandbyTask standbyTaskToRecycle = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final StreamTask recycledActiveTask = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTaskToRecycle));
        when(activeTaskCreator.createActiveTaskFromStandby(standbyTaskToRecycle, taskId03Partitions, consumer))
            .thenReturn(recycledActiveTask);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(standbyTaskToRecycle.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(standbyTaskToRecycle));

        taskManager.handleAssignment(
            mkMap(mkEntry(standbyTaskToRecycle.id(), standbyTaskToRecycle.inputPartitions())),
            Collections.emptyMap()
        );

        verify(tasks).addPendingTasksToInit(Collections.singleton(recycledActiveTask));
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldHandleExceptionThrownDuringRecyclingStandbyTask() {
        final StandbyTask standbyTaskToRecycle = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTaskToRecycle));
        when(activeTaskCreator.createActiveTaskFromStandby(
            standbyTaskToRecycle,
            standbyTaskToRecycle.inputPartitions(),
            consumer))
            .thenThrow(new RuntimeException());
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(standbyTaskToRecycle.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(standbyTaskToRecycle));

        assertThrows(
            StreamsException.class,
            () -> taskManager.handleAssignment(
                mkMap(mkEntry(standbyTaskToRecycle.id(), standbyTaskToRecycle.inputPartitions())),
                Collections.emptyMap()
            )
        );

        verify(stateUpdater, never()).add(any());
        verify(tasks, never()).addPendingTasksToInit(Collections.singleton(any()));
        verify(standbyTaskToRecycle).closeDirty();
    }

    @Test
    public void shouldKeepReassignedActiveTaskInStateUpdater() {
        final StreamTask reassignedActiveTask = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(reassignedActiveTask));

        taskManager.handleAssignment(
            mkMap(mkEntry(reassignedActiveTask.id(), reassignedActiveTask.inputPartitions())),
            Collections.emptyMap()
        );

        verify(stateUpdater, never()).remove(reassignedActiveTask.id());
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldMoveReassignedSuspendedActiveTaskToStateUpdater() {
        final StreamTask reassignedActiveTask = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.SUSPENDED)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(reassignedActiveTask));

        taskManager.handleAssignment(
            mkMap(mkEntry(reassignedActiveTask.id(), reassignedActiveTask.inputPartitions())),
            Collections.emptyMap()
        );

        verify(tasks).removeTask(reassignedActiveTask);
        verify(stateUpdater).add(reassignedActiveTask);
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldAddFailedActiveTaskToRecycleDuringAssignmentToTaskRegistry() {
        final StreamTask failedActiveTaskToRecycle = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(failedActiveTaskToRecycle));
        final RuntimeException taskException = new RuntimeException("Nobody expects the Spanish inquisition!");
        when(stateUpdater.remove(failedActiveTaskToRecycle.id()))
            .thenReturn(CompletableFuture.completedFuture(
                new StateUpdater.RemovedTaskResult(failedActiveTaskToRecycle, taskException)
            ));

        final StreamsException exception = assertThrows(
            StreamsException.class,
            () -> taskManager.handleAssignment(
                Collections.emptyMap(),
                mkMap(mkEntry(failedActiveTaskToRecycle.id(), failedActiveTaskToRecycle.inputPartitions()))
            )
        );

        assertEquals("Encounter unexpected fatal error for task " + failedActiveTaskToRecycle.id(), exception.getMessage());
        assertEquals(taskException, exception.getCause());
        verify(tasks).addFailedTask(failedActiveTaskToRecycle);
        verify(tasks, never()).addTask(failedActiveTaskToRecycle);
        verify(tasks).allNonFailedTasks();
        verify(standbyTaskCreator, never()).createStandbyTaskFromActive(failedActiveTaskToRecycle, taskId03Partitions);
    }

    @Test
    public void shouldAddFailedStandbyTaskToRecycleDuringAssignmentToTaskRegistry() {
        final StandbyTask failedStandbyTaskToRecycle = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(failedStandbyTaskToRecycle));
        final RuntimeException taskException = new RuntimeException("Nobody expects the Spanish inquisition!");
        when(stateUpdater.remove(failedStandbyTaskToRecycle.id()))
            .thenReturn(CompletableFuture.completedFuture(
                new StateUpdater.RemovedTaskResult(failedStandbyTaskToRecycle, taskException)
            ));

        final StreamsException exception = assertThrows(
            StreamsException.class,
            () -> taskManager.handleAssignment(
                mkMap(mkEntry(failedStandbyTaskToRecycle.id(), failedStandbyTaskToRecycle.inputPartitions())),
                Collections.emptyMap()
            )
        );

        assertEquals("Encounter unexpected fatal error for task " + failedStandbyTaskToRecycle.id(), exception.getMessage());
        assertEquals(taskException, exception.getCause());
        verify(tasks).addFailedTask(failedStandbyTaskToRecycle);
        verify(tasks, never()).addTask(failedStandbyTaskToRecycle);
        verify(tasks).allNonFailedTasks();
        verify(activeTaskCreator, never()).createActiveTaskFromStandby(failedStandbyTaskToRecycle, taskId03Partitions, consumer);
    }

    @Test
    public void shouldAddFailedActiveTasksToReassignWithDifferentInputPartitionsDuringAssignmentToTaskRegistry() {
        final StreamTask failedActiveTaskToReassign = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(failedActiveTaskToReassign));
        final RuntimeException taskException = new RuntimeException("Nobody expects the Spanish inquisition!");
        when(stateUpdater.remove(failedActiveTaskToReassign.id()))
            .thenReturn(CompletableFuture.completedFuture(
                new StateUpdater.RemovedTaskResult(failedActiveTaskToReassign, taskException)
            ));

        final StreamsException exception = assertThrows(
            StreamsException.class,
            () -> taskManager.handleAssignment(
                mkMap(mkEntry(failedActiveTaskToReassign.id(), taskId00Partitions)),
                Collections.emptyMap()
            )
        );

        assertEquals("Encounter unexpected fatal error for task " + failedActiveTaskToReassign.id(), exception.getMessage());
        assertEquals(taskException, exception.getCause());
        verify(tasks).addFailedTask(failedActiveTaskToReassign);
        verify(tasks, never()).addTask(failedActiveTaskToReassign);
        verify(tasks).allNonFailedTasks();
        verify(tasks, never()).updateActiveTaskInputPartitions(failedActiveTaskToReassign, taskId00Partitions);
    }

    @Test
    public void shouldFirstHandleTasksInStateUpdaterThenSuspendedActiveTasksInTaskRegistry() {
        final StreamTask reassignedActiveTask1 = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.SUSPENDED)
            .withInputPartitions(taskId03Partitions).build();
        final StreamTask reassignedActiveTask2 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(reassignedActiveTask1));
        when(stateUpdater.tasks()).thenReturn(Set.of(reassignedActiveTask2));
        when(stateUpdater.remove(reassignedActiveTask2.id()))
            .thenReturn(CompletableFuture.completedFuture(new StateUpdater.RemovedTaskResult(reassignedActiveTask2)));

        taskManager.handleAssignment(
            mkMap(
                mkEntry(reassignedActiveTask1.id(), reassignedActiveTask1.inputPartitions()),
                mkEntry(reassignedActiveTask2.id(), taskId00Partitions)
            ),
            Collections.emptyMap()
        );

        final InOrder inOrder = inOrder(stateUpdater, tasks);
        inOrder.verify(stateUpdater).remove(reassignedActiveTask2.id());
        inOrder.verify(tasks).removeTask(reassignedActiveTask1);
        inOrder.verify(stateUpdater).add(reassignedActiveTask1);
    }

    @Test
    public void shouldNeverUpdateInputPartitionsOfStandbyTaskInStateUpdater() {
        final StandbyTask standbyTaskToUpdateInputPartitions = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTaskToUpdateInputPartitions));

        taskManager.handleAssignment(
            Collections.emptyMap(),
            mkMap(mkEntry(standbyTaskToUpdateInputPartitions.id(), taskId03Partitions))
        );
        verify(stateUpdater, never()).remove(standbyTaskToUpdateInputPartitions.id());
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldKeepReassignedStandbyTaskInStateUpdater() {
        final StandbyTask reassignedStandbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(reassignedStandbyTask));

        taskManager.handleAssignment(
            Collections.emptyMap(),
            mkMap(mkEntry(reassignedStandbyTask.id(), reassignedStandbyTask.inputPartitions()))
        );

        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldAssignMultipleTasksInStateUpdater() {
        final StreamTask activeTaskToClose = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final StandbyTask standbyTaskToRecycle = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final StreamTask recycledActiveTask = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(activeTaskToClose, standbyTaskToRecycle));
        final CompletableFuture<StateUpdater.RemovedTaskResult> futureForActiveTaskToClose = new CompletableFuture<>();
        when(stateUpdater.remove(activeTaskToClose.id())).thenReturn(futureForActiveTaskToClose);
        futureForActiveTaskToClose.complete(new StateUpdater.RemovedTaskResult(activeTaskToClose));
        when(activeTaskCreator.createActiveTaskFromStandby(standbyTaskToRecycle, taskId02Partitions, consumer))
            .thenReturn(recycledActiveTask);
        final CompletableFuture<StateUpdater.RemovedTaskResult> futureForStandbyTaskToRecycle = new CompletableFuture<>();
        when(stateUpdater.remove(standbyTaskToRecycle.id())).thenReturn(futureForStandbyTaskToRecycle);
        futureForStandbyTaskToRecycle.complete(new StateUpdater.RemovedTaskResult(standbyTaskToRecycle));

        taskManager.handleAssignment(
            mkMap(mkEntry(standbyTaskToRecycle.id(), standbyTaskToRecycle.inputPartitions())),
            Collections.emptyMap()
        );

        verify(tasks).addPendingTasksToInit(Collections.singleton(recycledActiveTask));
        verify(activeTaskToClose).suspend();
        verify(activeTaskToClose).closeClean();
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
    }

    @Test
    public void shouldReturnRunningTasksStateUpdaterTasksAndTasksToInitInAllTasks() {
        final StreamTask activeTaskToInit = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId03Partitions).build();
        final StreamTask runningActiveTask = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final StandbyTask standbyTaskInStateUpdater = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTaskInStateUpdater));
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId03, runningActiveTask)));
        when(tasks.pendingTasksToInit()).thenReturn(Set.of(activeTaskToInit));
        assertEquals(
            taskManager.allTasks(),
            mkMap(
                mkEntry(taskId03, runningActiveTask),
                mkEntry(taskId02, standbyTaskInStateUpdater),
                mkEntry(taskId01, activeTaskToInit)
            )
        );
    }

    @Test
    public void shouldNotReturnStateUpdaterTasksInOwnedTasks() {
        final StreamTask activeTask = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId03, activeTask)));
        assertEquals(taskManager.allOwnedTasks(), mkMap(mkEntry(taskId03, activeTask)));
    }

    @Test
    public void shouldCreateActiveTaskDuringAssignment() {
        final StreamTask activeTaskToBeCreated = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        final Set<Task> createdTasks = Set.of(activeTaskToBeCreated);
        final Map<TaskId, Set<TopicPartition>> tasksToBeCreated = mkMap(
            mkEntry(activeTaskToBeCreated.id(), activeTaskToBeCreated.inputPartitions()));
        when(activeTaskCreator.createTasks(consumer, tasksToBeCreated)).thenReturn(createdTasks);

        taskManager.handleAssignment(tasksToBeCreated, Collections.emptyMap());

        verify(tasks).addPendingTasksToInit(createdTasks);
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldCreateStandbyTaskDuringAssignment() {
        final StandbyTask standbyTaskToBeCreated = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        final Set<Task> createdTasks = Set.of(standbyTaskToBeCreated);
        when(standbyTaskCreator.createTasks(mkMap(
            mkEntry(standbyTaskToBeCreated.id(), standbyTaskToBeCreated.inputPartitions())))
        ).thenReturn(createdTasks);

        taskManager.handleAssignment(
            Collections.emptyMap(),
            mkMap(mkEntry(standbyTaskToBeCreated.id(), standbyTaskToBeCreated.inputPartitions()))
        );

        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(tasks).addPendingTasksToInit(createdTasks);
    }

    @Test
    public void shouldAddRecycledStandbyTasksFromActiveToPendingTasksToInit() {
        final StreamTask activeTaskToRecycle = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING).build();
        final StandbyTask standbyTask = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.CREATED).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(activeTaskToRecycle));
        when(standbyTaskCreator.createStandbyTaskFromActive(activeTaskToRecycle, taskId01Partitions))
            .thenReturn(standbyTask);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        taskManager.handleAssignment(emptyMap(), mkMap(mkEntry(taskId01, taskId01Partitions)));

        verify(activeTaskToRecycle).prepareCommit(true);
        verify(tasks).addPendingTasksToInit(Set.of(standbyTask));
        verify(tasks).removeTask(activeTaskToRecycle);
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldThrowDuringAssignmentIfStandbyTaskToRecycleIsFoundInTasksRegistry() {
        final StandbyTask standbyTaskToRecycle = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(standbyTaskToRecycle));
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        final IllegalStateException illegalStateException = assertThrows(
            IllegalStateException.class,
            () -> taskManager.handleAssignment(
                mkMap(mkEntry(standbyTaskToRecycle.id(), standbyTaskToRecycle.inputPartitions())),
                Collections.emptyMap()
            )
        );

        assertEquals(illegalStateException.getMessage(), "Standby tasks should only be managed by the state updater, " +
            "but standby task " + taskId03 + " is managed by the stream thread");
        verifyNoInteractions(activeTaskCreator);
    }

    @Test
    public void shouldAssignActiveTaskInTasksRegistryToBeClosedCleanly() {
        final StreamTask activeTaskToClose = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(activeTaskToClose));

        taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap());

        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(activeTaskToClose).prepareCommit(true);
        verify(activeTaskToClose).closeClean();
        verify(tasks).removeTask(activeTaskToClose);
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldThrowDuringAssignmentIfStandbyTaskToCloseIsFoundInTasksRegistry() {
        final StandbyTask standbyTaskToClose = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(standbyTaskToClose));

        final IllegalStateException illegalStateException = assertThrows(
            IllegalStateException.class,
            () -> taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap())
        );

        assertEquals(illegalStateException.getMessage(), "Standby tasks should only be managed by the state updater, " +
            "but standby task " + taskId03 + " is managed by the stream thread");
        verifyNoInteractions(activeTaskCreator);
    }

    @Test
    public void shouldAssignActiveTaskInTasksRegistryToUpdateInputPartitions() {
        final StreamTask activeTaskToUpdateInputPartitions = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final Set<TopicPartition> newInputPartitions = taskId02Partitions;
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(activeTaskToUpdateInputPartitions));
        when(tasks.updateActiveTaskInputPartitions(activeTaskToUpdateInputPartitions, newInputPartitions)).thenReturn(true);

        taskManager.handleAssignment(
            mkMap(mkEntry(activeTaskToUpdateInputPartitions.id(), newInputPartitions)),
            Collections.emptyMap()
        );

        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(activeTaskToUpdateInputPartitions).updateInputPartitions(eq(newInputPartitions), any());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldResumeActiveRunningTaskInTasksRegistry() {
        final StreamTask activeTaskToResume = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(activeTaskToResume));

        taskManager.handleAssignment(
            mkMap(mkEntry(activeTaskToResume.id(), activeTaskToResume.inputPartitions())),
            Collections.emptyMap()
        );

        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldResumeActiveSuspendedTaskInTasksRegistryAndAddToStateUpdater() {
        final StreamTask activeTaskToResume = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.SUSPENDED)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(activeTaskToResume));

        taskManager.handleAssignment(
            mkMap(mkEntry(activeTaskToResume.id(), activeTaskToResume.inputPartitions())),
            Collections.emptyMap()
        );

        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(activeTaskToResume).resume();
        verify(stateUpdater).add(activeTaskToResume);
        verify(tasks).removeTask(activeTaskToResume);
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldThrowDuringAssignmentIfStandbyTaskToUpdateInputPartitionsIsFoundInTasksRegistry() {
        final StandbyTask standbyTaskToUpdateInputPartitions = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final Set<TopicPartition> newInputPartitions = taskId03Partitions;
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(standbyTaskToUpdateInputPartitions));

        final IllegalStateException illegalStateException = assertThrows(
            IllegalStateException.class,
            () -> taskManager.handleAssignment(
                Collections.emptyMap(),
                mkMap(mkEntry(standbyTaskToUpdateInputPartitions.id(), newInputPartitions))
            )
        );

        assertEquals(illegalStateException.getMessage(), "Standby tasks should only be managed by the state updater, " +
            "but standby task " + taskId02 + " is managed by the stream thread");
        verifyNoInteractions(activeTaskCreator);
    }

    @Test
    public void shouldAssignMultipleTasksInTasksRegistry() {
        final StreamTask activeTaskToClose = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final StreamTask activeTaskToCreate = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(activeTaskToClose));

        taskManager.handleAssignment(
            mkMap(mkEntry(activeTaskToCreate.id(), activeTaskToCreate.inputPartitions())),
            Collections.emptyMap()
        );

        verify(activeTaskCreator).createTasks(
            consumer,
            mkMap(mkEntry(activeTaskToCreate.id(), activeTaskToCreate.inputPartitions()))
        );
        verify(activeTaskToClose).closeClean();
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldAddTasksToStateUpdater() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RESTORING).build();
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(task00, task01));
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, true);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verify(task00).initializeIfNeeded();
        verify(task01).initializeIfNeeded();
        verify(stateUpdater).add(task00);
        verify(stateUpdater).add(task01);
    }

    @Test
    public void shouldRetryInitializationWhenLockExceptionInStateUpdater() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RESTORING).build();
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(task00, task01));
        final LockException lockException = new LockException("Where are my keys??");
        doThrow(lockException).when(task00).initializeIfNeeded();
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, true);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verify(task00).initializeIfNeeded();
        verify(task01).initializeIfNeeded();
        verify(tasks).addPendingTasksToInit(
            argThat(tasksToInit -> tasksToInit.contains(task00) && !tasksToInit.contains(task01))
        );
        verify(stateUpdater, never()).add(task00);
        verify(stateUpdater).add(task01);
    }

    @Test
    public void shouldRetryInitializationWithBackoffWhenInitializationFails() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RESTORING).build();
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(task00, task01));
        doThrow(new LockException("Lock Exception!")).when(task00).initializeIfNeeded();
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, true);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        // task00 should not be initialized due to LockException, task01 should be initialized
        verify(task00).initializeIfNeeded();
        verify(task01).initializeIfNeeded();
        verify(tasks).addPendingTasksToInit(
            argThat(tasksToInit -> tasksToInit.contains(task00) && !tasksToInit.contains(task01))
        );
        verify(stateUpdater, never()).add(task00);
        verify(stateUpdater).add(task01);

        time.sleep(500);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        // task00 should not be initialized since the backoff period has not passed
        verify(task00, times(1)).initializeIfNeeded();
        verify(tasks, times(2)).addPendingTasksToInit(
            argThat(tasksToInit -> tasksToInit.contains(task00))
        );
        verify(stateUpdater, never()).add(task00);

        time.sleep(5000);

        // task00 should call initialize since the backoff period has passed
        doNothing().when(task00).initializeIfNeeded();
        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verify(task00, times(2)).initializeIfNeeded();
        verify(tasks, times(2)).addPendingTasksToInit(
            argThat(tasksToInit -> tasksToInit.contains(task00))
        );
        verify(stateUpdater).add(task00);
    }

    @Test
    public void shouldRethrowRuntimeExceptionInInitTask() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.CREATED).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(task00));
        final RuntimeException runtimeException = new RuntimeException("KABOOM!");
        doThrow(runtimeException).when(task00).initializeIfNeeded();
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, true);

        final StreamsException streamsException = assertThrows(
            StreamsException.class,
            () -> taskManager.checkStateUpdater(time.milliseconds(), noOpResetter)
        );
        verify(stateUpdater, never()).add(task00);
        verify(tasks).addFailedTask(task00);
        assertTrue(streamsException.taskId().isPresent());
        assertEquals(task00.id(), streamsException.taskId().get());
        assertEquals("Encounter unexpected fatal error for task 0_0", streamsException.getMessage());
        assertEquals(runtimeException, streamsException.getCause());
    }

    @Test
    public void shouldRethrowTaskCorruptedExceptionFromInitialization() {
        final StreamTask statefulTask0 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask statefulTask1 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId01Partitions).build();
        final StreamTask statefulTask2 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, tasks, true);
        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(statefulTask0, statefulTask1, statefulTask2));
        doThrow(new TaskCorruptedException(Collections.singleton(statefulTask0.id))).when(statefulTask0).initializeIfNeeded();
        doThrow(new TaskCorruptedException(Collections.singleton(statefulTask1.id))).when(statefulTask1).initializeIfNeeded();

        final TaskCorruptedException thrown = assertThrows(
            TaskCorruptedException.class,
            () -> taskManager.checkStateUpdater(time.milliseconds(), noOpResetter)
        );

        verify(tasks).addFailedTask(statefulTask0);
        verify(tasks).addFailedTask(statefulTask1);
        verify(stateUpdater).add(statefulTask2);
        assertEquals(Set.of(taskId00, taskId01), thrown.corruptedTasks());
        assertEquals("Tasks [0_1, 0_0] are corrupted and hence need to be re-initialized", thrown.getMessage());
    }

    @Test
    public void shouldReturnFalseFromCheckStateUpdaterIfActiveTasksAreRestoring() {
        when(stateUpdater.restoresActiveTasks()).thenReturn(true);
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        assertFalse(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));
    }

    @Test
    public void shouldReturnFalseFromCheckStateUpdaterIfActiveTasksAreNotRestoringAndNoPendingTaskToRecycleButPendingTasksToInit() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.hasPendingTasksToInit()).thenReturn(true);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        assertFalse(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));
    }

    @Test
    public void shouldReturnTrueFromCheckStateUpdaterIfActiveTasksAreNotRestoringAndNoPendingInit() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        assertTrue(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));
    }

    @Test
    public void shouldSuspendActiveTaskWithRevokedInputPartitionsInStateUpdater() {
        final StreamTask task = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setupForRevocationAndLost(Set.of(task), tasks);
        when(stateUpdater.tasks()).thenReturn(Set.of(task));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(task.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(task));

        taskManager.handleRevocation(task.inputPartitions());

        verify(task).suspend();
        verify(tasks).addTask(task);
        verify(stateUpdater).remove(task.id());
    }

    @Test
    public void shouldSuspendMultipleActiveTasksWithRevokedInputPartitionsInStateUpdater() {
        final StreamTask task1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask task2 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId01Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setupForRevocationAndLost(Set.of(task1, task2), tasks);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future1 = new CompletableFuture<>();
        when(stateUpdater.remove(task1.id())).thenReturn(future1);
        future1.complete(new StateUpdater.RemovedTaskResult(task1));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future2 = new CompletableFuture<>();
        when(stateUpdater.remove(task2.id())).thenReturn(future2);
        future2.complete(new StateUpdater.RemovedTaskResult(task2));

        taskManager.handleRevocation(union(HashSet::new, taskId00Partitions, taskId01Partitions));

        verify(task1).suspend();
        verify(tasks).addTask(task1);
        verify(task2).suspend();
        verify(tasks).addTask(task2);
    }

    @Test
    public void shouldNotSuspendActiveTaskWithoutRevokedInputPartitionsInStateUpdater() {
        final StreamTask task = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setupForRevocationAndLost(Set.of(task), tasks);

        taskManager.handleRevocation(taskId01Partitions);

        verify(task, never()).suspend();
        verify(tasks, never()).addTask(task);
        verify(stateUpdater, never()).remove(task.id());
    }

    @Test
    public void shouldNotRevokeStandbyTaskInStateUpdaterOnRevocation() {
        final StandbyTask task = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setupForRevocationAndLost(Set.of(task), tasks);

        taskManager.handleRevocation(taskId00Partitions);

        verify(task, never()).suspend();
        verify(tasks, never()).addTask(task);
        verify(stateUpdater, never()).remove(task.id());
    }

    @Test
    public void shouldThrowIfRevokingTasksInStateUpdaterFindsFailedTasks() {
        final StreamTask task1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask task2 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId01Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setupForRevocationAndLost(Set.of(task1, task2), tasks);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future1 = new CompletableFuture<>();
        when(stateUpdater.remove(task1.id())).thenReturn(future1);
        future1.complete(new StateUpdater.RemovedTaskResult(task1));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future2 = new CompletableFuture<>();
        when(stateUpdater.remove(task2.id())).thenReturn(future2);
        final RuntimeException taskException = new RuntimeException("Nobody expects the Spanish inquisition!");
        future2.complete(new StateUpdater.RemovedTaskResult(task2, taskException));

        final StreamsException thrownException = assertThrows(
            StreamsException.class,
            () -> taskManager.handleRevocation(union(HashSet::new, taskId00Partitions, taskId01Partitions))
        );

        assertEquals("Encounter unexpected fatal error for task " + task2.id(), thrownException.getMessage());
        assertEquals(thrownException.getCause(), taskException);
        verify(task1).suspend();
        verify(tasks).addTask(task1);
        verify(task2, never()).suspend();
        verify(tasks).addFailedTask(task2);
    }

    @Test
    public void shouldCloseCleanWhenRemoveAllActiveTasksFromStateUpdaterOnPartitionLost() {
        final StreamTask task1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final StandbyTask task2 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId01Partitions).build();
        final StreamTask task3 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setupForRevocationAndLost(Set.of(task1, task2, task3), tasks);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future1 = new CompletableFuture<>();
        when(stateUpdater.remove(task1.id())).thenReturn(future1);
        future1.complete(new StateUpdater.RemovedTaskResult(task1));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future3 = new CompletableFuture<>();
        when(stateUpdater.remove(task3.id())).thenReturn(future3);
        future3.complete(new StateUpdater.RemovedTaskResult(task3));

        taskManager.handleLostAll();

        verify(task1).suspend();
        verify(task1).closeClean();
        verify(task3).suspend();
        verify(task3).closeClean();
        verify(stateUpdater, never()).remove(task2.id());
    }

    @Test
    public void shouldCloseCleanTasksPendingInitOnPartitionLost() {
        final StreamTask task1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask task2 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.drainPendingActiveTasksToInit()).thenReturn(Set.of(task1, task2));
        final TaskManager taskManager = setupForRevocationAndLost(emptySet(), tasks);

        taskManager.handleLostAll();

        verify(task1).suspend();
        verify(task1).closeClean();
        verify(task2).suspend();
        verify(task2).closeClean();
    }

    @Test
    public void shouldCloseDirtyWhenRemoveFailedActiveTasksFromStateUpdaterOnPartitionLost() {
        final StreamTask task1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask task2 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setupForRevocationAndLost(Set.of(task1, task2), tasks);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future1 = new CompletableFuture<>();
        when(stateUpdater.remove(task1.id())).thenReturn(future1);
        future1.complete(new StateUpdater.RemovedTaskResult(task1, new StreamsException("Something happened")));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future3 = new CompletableFuture<>();
        when(stateUpdater.remove(task2.id())).thenReturn(future3);
        future3.complete(new StateUpdater.RemovedTaskResult(task2, new StreamsException("Something else happened")));

        taskManager.handleLostAll();

        verify(task1).prepareCommit(false);
        verify(task1).suspend();
        verify(task1).closeDirty();
        verify(task2).prepareCommit(false);
        verify(task2).suspend();
        verify(task2).closeDirty();
    }

    @Test
    public void shouldCloseTasksWhenRemoveFailedActiveTasksFromStateUpdaterOnPartitionLost() {
        final StreamTask task1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask task2 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId02Partitions).build();
        final StreamTask task3 = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.drainPendingActiveTasksToInit()).thenReturn(Set.of(task1));
        final TaskManager taskManager = setupForRevocationAndLost(Set.of(task2, task3), tasks);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future2 = new CompletableFuture<>();
        when(stateUpdater.remove(task2.id())).thenReturn(future2);
        future2.complete(new StateUpdater.RemovedTaskResult(task2, new StreamsException("Something happened")));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future3 = new CompletableFuture<>();
        when(stateUpdater.remove(task3.id())).thenReturn(future3);
        future3.complete(new StateUpdater.RemovedTaskResult(task3));

        taskManager.handleLostAll();

        verify(task1).suspend();
        verify(task1).closeClean();
        verify(task2).prepareCommit(false);
        verify(task2).suspend();
        verify(task2).closeDirty();
        verify(task3).suspend();
        verify(task3).closeClean();
    }

    private TaskManager setupForRevocationAndLost(final Set<Task> tasksInStateUpdater,
                                                  final TasksRegistry tasks) {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(tasksInStateUpdater);

        return taskManager;
    }

    @Test
    public void shouldTransitRestoredTaskToRunning() {
        final StreamTask task = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTransitionToRunningOfRestoredTask(Set.of(task), tasks);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verifyTransitionToRunningOfRestoredTask(Set.of(task), tasks);
    }

    @Test
    public void shouldTransitMultipleRestoredTasksToRunning() {
        final StreamTask task1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask task2 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId01Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTransitionToRunningOfRestoredTask(Set.of(task1, task2), tasks);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verifyTransitionToRunningOfRestoredTask(Set.of(task1, task2), tasks);
    }

    private void verifyTransitionToRunningOfRestoredTask(final Set<StreamTask> restoredTasks,
                                                         final TasksRegistry tasks) {
        for (final StreamTask restoredTask : restoredTasks) {
            verify(restoredTask).completeRestoration(noOpResetter);
            verify(restoredTask).clearTaskTimeout();
            verify(tasks).addTask(restoredTask);
            verify(consumer).resume(restoredTask.inputPartitions());
        }
    }

    @Test
    public void shouldHandleTimeoutExceptionInTransitRestoredTaskToRunning() {
        final StreamTask task = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTransitionToRunningOfRestoredTask(Set.of(task), tasks);
        final TimeoutException timeoutException = new TimeoutException();
        doThrow(timeoutException).when(task).completeRestoration(noOpResetter);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verify(task).maybeInitTaskTimeoutOrThrow(anyLong(), eq(timeoutException));
        verify(stateUpdater).add(task);
        verify(tasks, never()).addTask(task);
        verify(task, never()).clearTaskTimeout();
        verifyNoInteractions(consumer);
    }

    private TaskManager setUpTransitionToRunningOfRestoredTask(final Set<StreamTask> statefulTasks,
                                                               final TasksRegistry tasks) {
        when(stateUpdater.restoresActiveTasks()).thenReturn(true);
        when(stateUpdater.drainRestoredActiveTasks(any(Duration.class))).thenReturn(statefulTasks);

        return setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
    }

    @Test
    public void shouldReturnCorrectBooleanWhenTryingToCompleteRestoration() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE);
        when(stateUpdater.restoresActiveTasks()).thenReturn(false);
        assertTrue(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));
        when(stateUpdater.restoresActiveTasks()).thenReturn(true);
        assertFalse(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));
    }

    @Test
    public void shouldRethrowStreamsExceptionFromStateUpdater() {
        final StreamTask statefulTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamsException exception = new StreamsException("boom!");
        final ExceptionAndTask exceptionAndTasks = new ExceptionAndTask(exception, statefulTask);
        when(stateUpdater.hasExceptionsAndFailedTasks()).thenReturn(true);
        when(stateUpdater.drainExceptionsAndFailedTasks()).thenReturn(Collections.singletonList(exceptionAndTasks));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> taskManager.checkStateUpdater(time.milliseconds(), noOpResetter)
        );

        assertEquals(exception, thrown);
        assertEquals(statefulTask.id(), thrown.taskId().orElseThrow());
    }

    @Test
    public void shouldRethrowTaskCorruptedExceptionFromStateUpdater() {
        final StreamTask statefulTask0 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask statefulTask1 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId01Partitions).build();
        final ExceptionAndTask exceptionAndTasks0 =
            new ExceptionAndTask(new TaskCorruptedException(Collections.singleton(taskId00)), statefulTask0);
        final ExceptionAndTask exceptionAndTasks1 =
            new ExceptionAndTask(new TaskCorruptedException(Collections.singleton(taskId01)), statefulTask1);
        when(stateUpdater.hasExceptionsAndFailedTasks()).thenReturn(true);
        when(stateUpdater.drainExceptionsAndFailedTasks()).thenReturn(Arrays.asList(exceptionAndTasks0, exceptionAndTasks1));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        final TaskCorruptedException thrown = assertThrows(
            TaskCorruptedException.class,
            () -> taskManager.checkStateUpdater(time.milliseconds(), noOpResetter)
        );

        assertEquals(Set.of(taskId00, taskId01), thrown.corruptedTasks());
        assertEquals("Tasks [0_1, 0_0] are corrupted and hence need to be re-initialized", thrown.getMessage());
    }

    @Test
    public void shouldAddSubscribedTopicsFromAssignmentToTopologyMetadata() {
        final Map<TaskId, Set<TopicPartition>> activeTasksAssignment = mkMap(
            mkEntry(taskId01, Set.of(t1p1)),
            mkEntry(taskId02, Set.of(t1p2, t2p2))
        );
        final Map<TaskId, Set<TopicPartition>> standbyTasksAssignment = mkMap(
            mkEntry(taskId03, Set.of(t1p3)),
            mkEntry(taskId04, Set.of(t1p4))
        );
        when(standbyTaskCreator.createTasks(standbyTasksAssignment)).thenReturn(Collections.emptySet());

        taskManager.handleAssignment(activeTasksAssignment, standbyTasksAssignment);

        verify(topologyBuilder).addSubscribedTopicsFromAssignment(eq(Set.of(t1p1, t1p2, t2p2)), anyString());
        verify(topologyBuilder, never()).addSubscribedTopicsFromAssignment(eq(Set.of(t1p3, t1p4)), anyString());
        verify(activeTaskCreator).createTasks(any(), eq(activeTasksAssignment));
    }

    @Test
    public void shouldNotLockAnythingIfStateDirIsEmpty() {
        when(stateDirectory.listNonEmptyTaskDirectories()).thenReturn(new ArrayList<>());

        taskManager.handleRebalanceStart(singleton("topic"));

        assertTrue(taskManager.lockedTaskDirectories().isEmpty());
    }

    @Test
    public void shouldTryToLockValidTaskDirsAtRebalanceStart() throws Exception {
        expectLockObtainedFor(taskId01);
        expectLockFailedFor(taskId10);
        expectDirectoryNotEmpty(taskId01);

        makeTaskFolders(
            taskId01.toString(),
            taskId10.toString(),
            "dummy"
        );
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.lockedTaskDirectories(), is(singleton(taskId01)));
    }

    @Test
    public void shouldUnlockEmptyDirsAtRebalanceStart() throws Exception {
        expectLockObtainedFor(taskId01, taskId10);
        expectDirectoryNotEmpty(taskId01);
        when(stateDirectory.directoryForTaskIsEmpty(taskId10)).thenReturn(true);

        makeTaskFolders(taskId01.toString(), taskId10.toString());
        taskManager.handleRebalanceStart(singleton("topic"));

        verify(stateDirectory).unlock(taskId10);
        assertThat(taskManager.lockedTaskDirectories(), is(singleton(taskId01)));
    }

    @Test
    public void shouldNotPauseReadyTasksOnRebalanceComplete() {
        final StreamTask statefulTask0 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(statefulTask0));
        final Set<TopicPartition> assigned = Set.of(t1p0, t1p1);
        when(consumer.assignment()).thenReturn(assigned);

        taskManager.handleRebalanceComplete();

        verify(consumer).pause(Set.of(t1p1));
    }

    @Test
    public void shouldReleaseLockForUnassignedTasksAfterRebalance() throws Exception {
        final StreamTask runningStatefulTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask restoringStatefulTask = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId01Partitions).build();
        final StandbyTask standbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId00, runningStatefulTask)));
        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTask, restoringStatefulTask));
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(runningStatefulTask));
        expectLockObtainedFor(taskId00, taskId01, taskId02, taskId03);
        expectDirectoryNotEmpty(taskId00, taskId01, taskId02, taskId03);
        makeTaskFolders(
            taskId00.toString(),
            taskId01.toString(),
            taskId02.toString(),
            taskId03.toString()
        );

        final Set<TopicPartition> assigned = Set.of(t1p0, t1p1, t1p2);
        when(consumer.assignment()).thenReturn(assigned);

        taskManager.handleRebalanceStart(singleton("topic"));
        taskManager.handleRebalanceComplete();

        verify(consumer).pause(Set.of(t1p1, t1p2));
        verify(stateDirectory).unlock(taskId03);
        assertThat(taskManager.lockedTaskDirectories(), is(Set.of(taskId00, taskId01, taskId02)));
    }

    @Test
    public void shouldComputeOffsetSumForRunningStatefulTask() {
        final StreamTask runningStatefulTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING).build();
        final long changelogOffsetOfRunningTask = Task.LATEST_OFFSET;
        when(runningStatefulTask.changelogOffsets())
            .thenReturn(mkMap(mkEntry(t1p0changelog, changelogOffsetOfRunningTask)));
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId00, runningStatefulTask)));

        assertThat(
            taskManager.taskOffsetSums(),
            is(mkMap(mkEntry(taskId00, changelogOffsetOfRunningTask)))
        );
    }

    @Test
    public void shouldComputeOffsetSumForRestoringActiveTask() throws Exception {
        final StreamTask restoringStatefulTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING).build();
        final long changelogOffset = 42L;
        when(restoringStatefulTask.changelogOffsets()).thenReturn(mkMap(mkEntry(t1p0changelog, changelogOffset)));
        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        final Map<TopicPartition, Long> changelogOffsetInCheckpoint = mkMap(mkEntry(t1p0changelog, 24L));
        writeCheckpointFile(taskId00, changelogOffsetInCheckpoint);
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(restoringStatefulTask));
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.taskOffsetSums(), is(mkMap(mkEntry(taskId00, changelogOffset))));
    }

    @Test
    public void shouldComputeOffsetSumForRestoringStandbyTask() throws Exception {
        final StandbyTask restoringStandbyTask = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING).build();
        final long changelogOffset = 42L;
        when(restoringStandbyTask.changelogOffsets()).thenReturn(mkMap(mkEntry(t1p0changelog, changelogOffset)));
        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        final Map<TopicPartition, Long> changelogOffsetInCheckpoint = mkMap(mkEntry(t1p0changelog, 24L));
        writeCheckpointFile(taskId00, changelogOffsetInCheckpoint);
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(restoringStandbyTask));
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.taskOffsetSums(), is(mkMap(mkEntry(taskId00, changelogOffset))));
    }

    @Test
    public void shouldComputeOffsetSumForRunningStatefulTaskAndRestoringTask() {
        final StreamTask runningStatefulTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING).build();
        final StreamTask restoringStatefulTask = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING).build();
        final StandbyTask restoringStandbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING).build();
        final long changelogOffsetOfRunningTask = Task.LATEST_OFFSET;
        final long changelogOffsetOfRestoringStatefulTask = 24L;
        final long changelogOffsetOfRestoringStandbyTask = 84L;
        when(runningStatefulTask.changelogOffsets())
            .thenReturn(mkMap(mkEntry(t1p0changelog, changelogOffsetOfRunningTask)));
        when(restoringStatefulTask.changelogOffsets())
            .thenReturn(mkMap(mkEntry(t1p1changelog, changelogOffsetOfRestoringStatefulTask)));
        when(restoringStandbyTask.changelogOffsets())
            .thenReturn(mkMap(mkEntry(t1p2changelog, changelogOffsetOfRestoringStandbyTask)));
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId00, runningStatefulTask)));
        when(stateUpdater.tasks()).thenReturn(Set.of(restoringStandbyTask, restoringStatefulTask));

        assertThat(
            taskManager.taskOffsetSums(),
            is(mkMap(
                mkEntry(taskId00, changelogOffsetOfRunningTask),
                mkEntry(taskId01, changelogOffsetOfRestoringStatefulTask),
                mkEntry(taskId02, changelogOffsetOfRestoringStandbyTask)
            ))
        );
    }

    @Test
    public void shouldSkipUnknownOffsetsWhenComputingOffsetSum() {
        final StreamTask restoringStatefulTask = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING).build();
        final long changelogOffsetOfRestoringStandbyTask = 84L;
        when(restoringStatefulTask.changelogOffsets())
            .thenReturn(mkMap(
                mkEntry(t1p1changelog, changelogOffsetOfRestoringStandbyTask),
                mkEntry(t1p1changelog2, OffsetCheckpoint.OFFSET_UNKNOWN)
            ));
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.tasks()).thenReturn(Set.of(restoringStatefulTask));

        assertThat(
            taskManager.taskOffsetSums(),
            is(mkMap(
                mkEntry(taskId01, changelogOffsetOfRestoringStandbyTask)
            ))
        );
    }

    @Test
    public void shouldComputeOffsetSumForUnassignedTaskWeCanLock() throws Exception {
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), 5L),
            mkEntry(new TopicPartition("changelog", 1), 10L)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, 15L));

        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        writeCheckpointFile(taskId00, changelogOffsets);

        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.taskOffsetSums(), is(expectedOffsetSums));
    }

    @Test
    public void shouldComputeOffsetSumFromCheckpointFileForUninitializedTask() throws Exception {
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), 5L),
            mkEntry(new TopicPartition("changelog", 1), 10L)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, 15L));

        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        writeCheckpointFile(taskId00, changelogOffsets);

        taskManager.handleRebalanceStart(singleton("topic"));
        final StateMachineTask uninitializedTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singleton(uninitializedTask));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());

        assertThat(uninitializedTask.state(), is(State.CREATED));

        assertThat(taskManager.taskOffsetSums(), is(expectedOffsetSums));
    }

    @Test
    public void shouldComputeOffsetSumFromCheckpointFileForClosedTask() throws Exception {
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), 5L),
            mkEntry(new TopicPartition("changelog", 1), 10L)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, 15L));

        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        writeCheckpointFile(taskId00, changelogOffsets);

        final StateMachineTask closedTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);

        taskManager.handleRebalanceStart(singleton("topic"));

        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singleton(closedTask));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());

        closedTask.suspend();
        closedTask.closeClean();
        assertThat(closedTask.state(), is(State.CLOSED));

        assertThat(taskManager.taskOffsetSums(), is(expectedOffsetSums));
    }
    
    @Test
    public void shouldNotReportOffsetSumsForTaskWeCantLock() throws Exception {
        expectLockFailedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        taskManager.handleRebalanceStart(singleton("topic"));
        assertTrue(taskManager.lockedTaskDirectories().isEmpty());

        assertTrue(taskManager.taskOffsetSums().isEmpty());
    }

    @Test
    public void shouldNotReportOffsetSumsAndReleaseLockForUnassignedTaskWithoutCheckpoint() throws Exception {
        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        expectDirectoryNotEmpty(taskId00);
        when(stateDirectory.checkpointFileFor(taskId00)).thenReturn(getCheckpointFile(taskId00));
        taskManager.handleRebalanceStart(singleton("topic"));

        assertTrue(taskManager.taskOffsetSums().isEmpty());
    }

    @Test
    public void shouldPinOffsetSumToLongMaxValueInCaseOfOverflow() throws Exception {
        final long largeOffset = Long.MAX_VALUE / 2;
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 1), largeOffset),
            mkEntry(new TopicPartition("changelog", 2), largeOffset),
            mkEntry(new TopicPartition("changelog", 3), largeOffset)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, Long.MAX_VALUE));

        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        writeCheckpointFile(taskId00, changelogOffsets);
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.taskOffsetSums(), is(expectedOffsetSums));
    }

    @Test
    public void shouldReInitializeStreamsProducerOnHandleLostAllIfEosV2Enabled() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2);

        taskManager.handleLostAll();

        verify(activeTaskCreator).reInitializeProducer();
    }

    @Test
    public void shouldReAddRevivedTasksToStateUpdater() {
        final StreamTask corruptedActiveTask = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final StandbyTask corruptedStandbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.task(taskId03)).thenReturn(corruptedActiveTask);
        when(tasks.task(taskId02)).thenReturn(corruptedStandbyTask);

        taskManager.handleCorruption(Set.of(corruptedActiveTask.id(), corruptedStandbyTask.id()));

        final InOrder activeTaskOrder = inOrder(corruptedActiveTask);
        activeTaskOrder.verify(corruptedActiveTask).closeDirty();
        activeTaskOrder.verify(corruptedActiveTask).revive();
        final InOrder standbyTaskOrder = inOrder(corruptedStandbyTask);
        standbyTaskOrder.verify(corruptedStandbyTask).closeDirty();
        standbyTaskOrder.verify(corruptedStandbyTask).revive();
        verify(tasks).removeTask(corruptedActiveTask);
        verify(tasks).removeTask(corruptedStandbyTask);
        verify(tasks).addPendingTasksToInit(Set.of(corruptedActiveTask));
        verify(tasks).addPendingTasksToInit(Set.of(corruptedStandbyTask));
        verify(consumer).assignment();
    }

    @Test
    public void shouldNotCommitNonCorruptedRestoringActiveTasksAndNotCommitRunningStandbyTasks() {
        final StreamTask activeRestoringTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RESTORING).build();
        final StandbyTask standbyTask = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING).build();
        final StreamTask corruptedTask = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId02, corruptedTask)));
        when(tasks.task(taskId02)).thenReturn(corruptedTask);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(consumer.assignment()).thenReturn(intersection(HashSet::new, taskId00Partitions, taskId01Partitions, taskId02Partitions));

        taskManager.handleCorruption(Set.of(taskId02));

        verify(activeRestoringTask, never()).commitNeeded();
        verify(activeRestoringTask, never()).prepareCommit(true);
        verify(activeRestoringTask, never()).postCommit(anyBoolean());
        verify(standbyTask, never()).commitNeeded();
        verify(standbyTask, never()).prepareCommit(true);
        verify(standbyTask, never()).postCommit(anyBoolean());
    }

    @Test
    public void shouldSuspendAllRevokedActiveTasksAndPropagateSuspendException() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("task 0_1 suspend boom!");
            }
        };

        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);

        taskManager.addTask(task00);
        taskManager.addTask(task01);
        taskManager.addTask(task02);


        final RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> taskManager.handleRevocation(union(HashSet::new, taskId01Partitions, taskId02Partitions)));
        assertThat(thrown.getCause().getMessage(), is("task 0_1 suspend boom!"));

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.SUSPENDED));
        assertThat(task02.state(), is(Task.State.SUSPENDED));

        verifyNoInteractions(activeTaskCreator);
    }

    @Test
    public void shouldShutDownStateUpdaterAndCloseFailedTasksDirty() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final StreamTask failedStatefulTask = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING).build();
        final StandbyTask failedStandbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING).build();
        when(stateUpdater.drainExceptionsAndFailedTasks())
            .thenReturn(Arrays.asList(
                new ExceptionAndTask(new RuntimeException(), failedStatefulTask),
                new ExceptionAndTask(new RuntimeException(), failedStandbyTask))
            );
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        taskManager.shutdown(true);

        verify(activeTaskCreator).close();
        verify(stateUpdater).shutdown(Duration.ofMillis(Long.MAX_VALUE));
        verify(failedStatefulTask).prepareCommit(false);
        verify(failedStatefulTask).suspend();
        verify(failedStatefulTask).closeDirty();
    }

    @Test
    public void shouldShutdownSchedulingTaskManager() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        taskManager.shutdown(true);

        verify(schedulingTaskManager).shutdown(Duration.ofMillis(Long.MAX_VALUE));
    }

    @Test
    public void shouldShutDownStateUpdaterAndCloseDirtyTasksFailedDuringRemoval() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final StreamTask removedStatefulTask = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING).build();
        final StandbyTask removedStandbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING).build();
        final StreamTask removedFailedStatefulTask = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING).build();
        final StandbyTask removedFailedStandbyTask = standbyTask(taskId04, taskId04ChangelogPartitions)
            .inState(State.RUNNING).build();
        final StreamTask removedFailedStatefulTaskDuringRemoval = statefulTask(taskId05, taskId05ChangelogPartitions)
            .inState(State.RESTORING).build();
        final StandbyTask removedFailedStandbyTaskDuringRemoval = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING).build();
        when(stateUpdater.tasks())
            .thenReturn(Set.of(
                removedStatefulTask,
                removedStandbyTask,
                removedFailedStatefulTask,
                removedFailedStandbyTask,
                removedFailedStatefulTaskDuringRemoval,
                removedFailedStandbyTaskDuringRemoval
            ));
        final CompletableFuture<StateUpdater.RemovedTaskResult> futureForRemovedStatefulTask = new CompletableFuture<>();
        final CompletableFuture<StateUpdater.RemovedTaskResult> futureForRemovedStandbyTask = new CompletableFuture<>();
        final CompletableFuture<StateUpdater.RemovedTaskResult> futureForRemovedFailedStatefulTask = new CompletableFuture<>();
        final CompletableFuture<StateUpdater.RemovedTaskResult> futureForRemovedFailedStandbyTask = new CompletableFuture<>();
        final CompletableFuture<StateUpdater.RemovedTaskResult> futureForRemovedFailedStatefulTaskDuringRemoval = new CompletableFuture<>();
        final CompletableFuture<StateUpdater.RemovedTaskResult> futureForRemovedFailedStandbyTaskDuringRemoval = new CompletableFuture<>();
        when(stateUpdater.remove(removedStatefulTask.id())).thenReturn(futureForRemovedStatefulTask);
        when(stateUpdater.remove(removedStandbyTask.id())).thenReturn(futureForRemovedStandbyTask);
        when(stateUpdater.remove(removedFailedStatefulTask.id())).thenReturn(futureForRemovedFailedStatefulTask);
        when(stateUpdater.remove(removedFailedStandbyTask.id())).thenReturn(futureForRemovedFailedStandbyTask);
        when(stateUpdater.remove(removedFailedStatefulTaskDuringRemoval.id()))
            .thenReturn(futureForRemovedFailedStatefulTaskDuringRemoval);
        when(stateUpdater.remove(removedFailedStandbyTaskDuringRemoval.id()))
            .thenReturn(futureForRemovedFailedStandbyTaskDuringRemoval);
        when(stateUpdater.drainExceptionsAndFailedTasks()).thenReturn(Arrays.asList(
            new ExceptionAndTask(new StreamsException("KABOOM!"), removedFailedStatefulTaskDuringRemoval),
            new ExceptionAndTask(new StreamsException("KABOOM!"), removedFailedStandbyTaskDuringRemoval)
        ));
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        futureForRemovedStatefulTask.complete(new StateUpdater.RemovedTaskResult(removedStatefulTask));
        futureForRemovedStandbyTask.complete(new StateUpdater.RemovedTaskResult(removedStandbyTask));
        futureForRemovedFailedStatefulTask
            .complete(new StateUpdater.RemovedTaskResult(removedFailedStatefulTask, new StreamsException("KABOOM!")));
        futureForRemovedFailedStandbyTask
            .complete(new StateUpdater.RemovedTaskResult(removedFailedStandbyTask, new StreamsException("KABOOM!")));
        futureForRemovedFailedStatefulTaskDuringRemoval
            .completeExceptionally(new StreamsException("KABOOM!"));
        futureForRemovedFailedStandbyTaskDuringRemoval
            .completeExceptionally(new StreamsException("KABOOM!"));

        taskManager.shutdown(true);

        verify(stateUpdater).shutdown(Duration.ofMillis(Long.MAX_VALUE));
        verify(tasks).addTask(removedStatefulTask);
        verify(tasks).addTask(removedStandbyTask);
        verify(removedFailedStatefulTask).prepareCommit(false);
        verify(removedFailedStatefulTask).suspend();
        verify(removedFailedStatefulTask).closeDirty();
        verify(removedFailedStandbyTask).prepareCommit(false);
        verify(removedFailedStandbyTask).suspend();
        verify(removedFailedStandbyTask).closeDirty();
        verify(removedFailedStatefulTaskDuringRemoval).prepareCommit(false);
        verify(removedFailedStatefulTaskDuringRemoval).suspend();
        verify(removedFailedStatefulTaskDuringRemoval).closeDirty();
        verify(removedFailedStandbyTaskDuringRemoval).prepareCommit(false);
        verify(removedFailedStandbyTaskDuringRemoval).suspend();
        verify(removedFailedStandbyTaskDuringRemoval).closeDirty();
    }

    @Test
    public void shouldHandleRebalanceEvents() {
        when(consumer.assignment()).thenReturn(assignment);
        when(stateDirectory.listNonEmptyTaskDirectories()).thenReturn(new ArrayList<>());
        assertThat(taskManager.rebalanceInProgress(), is(false));
        taskManager.handleRebalanceStart(emptySet());
        assertThat(taskManager.rebalanceInProgress(), is(true));
        taskManager.handleRebalanceComplete();
        assertThat(taskManager.rebalanceInProgress(), is(false));
        verify(consumer).pause(assignment);
    }

    @Test
    public void shouldCommitViaConsumerIfEosDisabled() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        task01.setCommittableOffsetsAndMetadata(offsets);
        task01.setCommitNeeded();
        taskManager.addTask(task01);

        taskManager.commitAll();

        verify(consumer).commitSync(offsets);
    }

    @Test
    public void shouldCommitViaProducerIfEosV2Enabled() {
        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.streamsProducer()).thenReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT02 = singletonMap(t1p2, new OffsetAndMetadata(1L, null));
        final Map<TopicPartition, OffsetAndMetadata> allOffsets = new HashMap<>();
        allOffsets.putAll(offsetsT01);
        allOffsets.putAll(offsetsT02);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2);

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        task01.setCommittableOffsetsAndMetadata(offsetsT01);
        task01.setCommitNeeded();
        taskManager.addTask(task01);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);
        task02.setCommittableOffsetsAndMetadata(offsetsT02);
        task02.setCommitNeeded();
        taskManager.addTask(task02);

        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("appId"));

        taskManager.commitAll();

        verify(producer).commitTransaction(allOffsets, new ConsumerGroupMetadata("appId"));
        verifyNoMoreInteractions(producer);
    }

    @Test
    public void shouldNotFailOnTimeoutException() {
        final AtomicReference<TimeoutException> timeoutException = new AtomicReference<>();
        timeoutException.set(new TimeoutException("Skip me!"));

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        task00.transitionTo(State.RESTORING);
        task00.transitionTo(State.RUNNING);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public boolean process(final long wallClockTime) {
                final TimeoutException exception = timeoutException.get();
                if (exception != null) {
                    throw exception;
                }
                return true;
            }
        };
        task01.transitionTo(State.RESTORING);
        task01.transitionTo(State.RUNNING);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);
        task02.transitionTo(State.RESTORING);
        task02.transitionTo(State.RUNNING);

        taskManager.addTask(task00);
        taskManager.addTask(task01);
        taskManager.addTask(task02);

        task00.addRecords(
            t1p0,
            Arrays.asList(
                getConsumerRecord(t1p0, 0L),
                getConsumerRecord(t1p0, 1L)
            )
        );
        task01.addRecords(
            t1p1,
            Arrays.asList(
                getConsumerRecord(t1p1, 0L),
                getConsumerRecord(t1p1, 1L)
            )
        );
        task02.addRecords(
            t1p2,
            Arrays.asList(
                getConsumerRecord(t1p2, 0L),
                getConsumerRecord(t1p2, 1L)
            )
        );

        // should only process 2 records, because task01 throws TimeoutException
        assertThat(taskManager.process(1, time), is(2));
        assertThat(task01.timeout, equalTo(time.milliseconds()));

        //  retry without error
        timeoutException.set(null);
        assertThat(taskManager.process(1, time), is(3));
        assertThat(task01.timeout, equalTo(null));

        // there should still be one record for task01 to be processed
        assertThat(taskManager.process(1, time), is(1));
    }

    @Test
    public void shouldTransmitProducerMetrics() {
        final MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        final Metric testMetric = new KafkaMetric(
            new Object(),
            testMetricName,
            (Measurable) (config, now) -> 0,
            null,
            new MockTime());
        final Map<MetricName, Metric> dummyProducerMetrics = singletonMap(testMetricName, testMetric);

        when(activeTaskCreator.producerMetrics()).thenReturn(dummyProducerMetrics);

        assertThat(taskManager.producerMetrics(), is(dummyProducerMetrics));
    }

    private void expectLockObtainedFor(final TaskId... tasks) {
        for (final TaskId task : tasks) {
            when(stateDirectory.lock(task)).thenReturn(true);
        }
    }

    private void expectLockFailedFor(final TaskId... tasks) {
        for (final TaskId task : tasks) {
            when(stateDirectory.lock(task)).thenReturn(false);
        }
    }

    private void expectDirectoryNotEmpty(final TaskId... tasks) {
        for (final TaskId taskId : tasks) {
            when(stateDirectory.directoryForTaskIsEmpty(taskId)).thenReturn(false);
        }
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnCommitFailed() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task01.setCommittableOffsetsAndMetadata(offsets);
        task01.setCommitNeeded();
        taskManager.addTask(task01);

        doThrow(new CommitFailedException()).when(consumer).commitSync(offsets);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> taskManager.commitAll()
        );

        assertThat(thrown.getCause(), instanceOf(CommitFailedException.class));
        assertThat(
            thrown.getMessage(),
            equalTo("Consumer committing offsets failed, indicating the corresponding thread is no longer part of the group;" +
                " it means all tasks belonging to this thread should be migrated.")
        );
        assertThat(task01.state(), is(Task.State.CREATED));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotFailForTimeoutExceptionOnConsumerCommit() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);

        task00.setCommittableOffsetsAndMetadata(taskId00Partitions.stream().collect(Collectors.toMap(p -> p, p -> new OffsetAndMetadata(0))));
        task01.setCommittableOffsetsAndMetadata(taskId00Partitions.stream().collect(Collectors.toMap(p -> p, p -> new OffsetAndMetadata(0))));

        doThrow(new TimeoutException("KABOOM!")).doNothing().when(consumer).commitSync(any(Map.class));

        task00.setCommitNeeded();

        assertThat(taskManager.commit(Set.of(task00, task01)), equalTo(0));
        assertThat(task00.timeout, equalTo(time.milliseconds()));
        assertNull(task01.timeout);

        assertThat(taskManager.commit(Set.of(task00, task01)), equalTo(1));
        assertNull(task00.timeout);
        assertNull(task01.timeout);

        verify(consumer, times(2)).commitSync(any(Map.class));
    }

    @Test
    public void shouldThrowTaskCorruptedExceptionForTimeoutExceptionOnCommitWithEosV2() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2);

        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.streamsProducer()).thenReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        final Map<TopicPartition, OffsetAndMetadata> allOffsets = new HashMap<>(offsetsT00);
        allOffsets.putAll(offsetsT01);

        doThrow(new TimeoutException("KABOOM!")).doNothing().when(producer).commitTransaction(allOffsets, null);

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        task00.setCommittableOffsetsAndMetadata(offsetsT00);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        task01.setCommittableOffsetsAndMetadata(offsetsT01);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);

        task00.setCommitNeeded();
        task01.setCommitNeeded();

        final TaskCorruptedException exception = assertThrows(
            TaskCorruptedException.class,
            () -> taskManager.commit(Set.of(task00, task01, task02))
        );
        assertThat(
            exception.corruptedTasks(),
            equalTo(Set.of(taskId00, taskId01))
        );

        verify(consumer).groupMetadata();
    }

    @Test
    public void shouldStreamsExceptionOnCommitError() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task01.setCommittableOffsetsAndMetadata(offsets);
        task01.setCommitNeeded();
        taskManager.addTask(task01);

        doThrow(new KafkaException()).when(consumer).commitSync(offsets);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> taskManager.commitAll()
        );

        assertThat(thrown.getCause(), instanceOf(KafkaException.class));
        assertThat(thrown.getMessage(), equalTo("Error encountered committing offsets via consumer"));
        assertThat(task01.state(), is(Task.State.CREATED));
    }

    @Test
    public void shouldFailOnCommitFatal() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task01.setCommittableOffsetsAndMetadata(offsets);
        task01.setCommitNeeded();
        taskManager.addTask(task01);

        doThrow(new RuntimeException("KABOOM")).when(consumer).commitSync(offsets);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.commitAll()
        );

        assertThat(thrown.getMessage(), equalTo("KABOOM"));
        assertThat(task01.state(), is(Task.State.CREATED));
    }

    @Test
    public void shouldRecycleStartupTasksFromStateDirectoryAsActive() {
        final Tasks taskRegistry = new Tasks(new LogContext());
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, taskRegistry, true);
        final StandbyTask startupTask = standbyTask(taskId00, taskId00ChangelogPartitions).build();

        final StreamTask activeTask = statefulTask(taskId00, taskId00ChangelogPartitions).build();
        when(activeTaskCreator.createActiveTaskFromStandby(eq(startupTask), eq(taskId00Partitions), any()))
                .thenReturn(activeTask);

        when(stateDirectory.hasStartupTasks()).thenReturn(true, false);
        when(stateDirectory.removeStartupTask(taskId00)).thenReturn(startupTask, (Task) null);

        taskManager.handleAssignment(taskId00Assignment, Collections.emptyMap());

        // ensure we used our existing startup Task directly as a Standby
        assertTrue(taskRegistry.hasPendingTasksToInit());
        assertEquals(Collections.singleton(activeTask), taskRegistry.drainPendingTasksToInit());

        // we're using a mock StateUpdater here, so now that we've drained the task from the queue of startup tasks to init
        // let's "add" it to our mock StateUpdater
        when(stateUpdater.tasks()).thenReturn(Collections.singleton(activeTask));
        when(stateUpdater.standbyTasks()).thenReturn(Collections.emptySet());

        // ensure we recycled our existing startup Standby into an Active task
        verify(activeTaskCreator).createActiveTaskFromStandby(eq(startupTask), eq(taskId00Partitions), any());

        // ensure we didn't construct any new Tasks
        verify(activeTaskCreator).createTasks(any(), eq(Collections.emptyMap()));
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
        verifyNoMoreInteractions(activeTaskCreator);
        verifyNoMoreInteractions(standbyTaskCreator);

        // verify the recycled task is now being used as an assigned Active
        assertEquals(Collections.singletonMap(taskId00, activeTask), taskManager.activeTaskMap());
        assertEquals(Collections.emptyMap(), taskManager.standbyTaskMap());
    }

    @Test
    public void shouldUseStartupTasksFromStateDirectoryAsStandby() {
        final Tasks taskRegistry = new Tasks(new LogContext());
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, taskRegistry, true);
        final StandbyTask startupTask = standbyTask(taskId00, taskId00ChangelogPartitions).build();

        when(stateDirectory.hasStartupTasks()).thenReturn(true, true, false);
        when(stateDirectory.removeStartupTask(taskId00)).thenReturn(startupTask, (Task) null);

        assertFalse(taskRegistry.hasPendingTasksToInit());

        taskManager.handleAssignment(Collections.emptyMap(), taskId00Assignment);

        // ensure we used our existing startup Task directly as a Standby
        assertTrue(taskRegistry.hasPendingTasksToInit());
        assertEquals(Collections.singleton(startupTask), taskRegistry.drainPendingTasksToInit());

        // we're using a mock StateUpdater here, so now that we've drained the task from the queue of startup tasks to init
        // let's "add" it to our mock StateUpdater
        when(stateUpdater.tasks()).thenReturn(Collections.singleton(startupTask));
        when(stateUpdater.standbyTasks()).thenReturn(Collections.singleton(startupTask));

        // ensure we didn't construct any new Tasks, or recycle an existing Task; we only used the one we already have
        verify(activeTaskCreator).createTasks(any(), eq(Collections.emptyMap()));
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
        verifyNoMoreInteractions(activeTaskCreator);
        verifyNoMoreInteractions(standbyTaskCreator);

        // verify the startup Standby is now being used as an assigned Standby
        assertEquals(Collections.emptyMap(), taskManager.activeTaskMap());
        assertEquals(Collections.singletonMap(taskId00, startupTask), taskManager.standbyTaskMap());
    }

    @Test
    public void shouldStartStateUpdaterOnInit() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE);
        taskManager.init();
        verify(stateUpdater).start();
    }

    private static KafkaFutureImpl<DeletedRecords> completedFuture() {
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        futureDeletedRecords.complete(null);
        return futureDeletedRecords;
    }

    private void makeTaskFolders(final String... names) throws Exception {
        final ArrayList<TaskDirectory> taskFolders = new ArrayList<>(names.length);
        for (int i = 0; i < names.length; i++) {
            final String name = names[i];
            final Path path = testFolder.resolve(name).toAbsolutePath();
            if (Files.notExists(path)) {
                Files.createDirectories(path);
            }
            taskFolders.add(new TaskDirectory(path.toFile(), null));
        }
        when(stateDirectory.listNonEmptyTaskDirectories()).thenReturn(taskFolders);
    }

    private void writeCheckpointFile(final TaskId task, final Map<TopicPartition, Long> offsets) throws Exception {
        final File checkpointFile = getCheckpointFile(task);
        final Path checkpointFilePath = checkpointFile.toPath();
        Files.createFile(checkpointFilePath);
        new OffsetCheckpoint(checkpointFile).write(offsets);
        lenient().when(stateDirectory.checkpointFileFor(task)).thenReturn(checkpointFile);
        expectDirectoryNotEmpty(task);
    }

    private File getCheckpointFile(final TaskId task) {
        return new File(new File(testFolder.toAbsolutePath().toString(), task.toString()), StateManagerUtil.CHECKPOINT_FILE_NAME);
    }

    private static ConsumerRecord<byte[], byte[]> getConsumerRecord(final TopicPartition topicPartition, final long offset) {
        return new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), offset, null, null);
    }

    private static class StateMachineTask extends AbstractTask implements Task {
        private final boolean active;

        // TODO: KAFKA-12569 clean up usage of these flags and use the new commitCompleted flag where appropriate
        private boolean commitNeeded = false;
        private boolean commitRequested = false;
        private boolean commitPrepared = false;
        private boolean commitCompleted = false;
        private Map<TopicPartition, OffsetAndMetadata> committableOffsets = Collections.emptyMap();
        private Map<TopicPartition, Long> purgeableOffsets;
        private Map<TopicPartition, Long> changelogOffsets = Collections.emptyMap();
        private Set<TopicPartition> partitionsForOffsetReset = Collections.emptySet();
        private Long timeout = null;

        private final Map<TopicPartition, LinkedList<ConsumerRecord<byte[], byte[]>>> queue = new HashMap<>();

        StateMachineTask(final TaskId id,
                         final Set<TopicPartition> partitions,
                         final boolean active,
                         final ProcessorStateManager processorStateManager) {
            super(id, null, null, processorStateManager, partitions, (new TopologyConfig(new DummyStreamsConfig())).getTaskConfig(), "test-task", StateMachineTask.class);
            this.active = active;
        }

        @Override
        public void initializeIfNeeded() {
            if (state() == State.CREATED) {
                transitionTo(State.RESTORING);
                if (!active) {
                    transitionTo(State.RUNNING);
                }
            }
        }

        @Override
        public void addPartitionsForOffsetReset(final Set<TopicPartition> partitionsForOffsetReset) {
            this.partitionsForOffsetReset = partitionsForOffsetReset;
        }

        @Override
        public void completeRestoration(final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
            if (state() == State.RUNNING) {
                return;
            }
            transitionTo(State.RUNNING);
        }

        public void setCommitNeeded() {
            commitNeeded = true;
        }

        @Override
        public boolean commitNeeded() {
            return commitNeeded;
        }

        public void setCommitRequested() {
            commitRequested = true;
        }

        @Override
        public boolean commitRequested() {
            return commitRequested;
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> prepareCommit(final boolean clean) {
            commitPrepared = true;

            if (commitNeeded) {
                if (!clean) {
                    return null;
                }
                return committableOffsets;
            } else {
                return Collections.emptyMap();
            }
        }

        @Override
        public void postCommit(final boolean enforceCheckpoint) {
            commitNeeded = false;
            commitCompleted = true;
        }

        @Override
        public void suspend() {
            if (state() == State.CLOSED) {
                throw new IllegalStateException("Illegal state " + state() + " while suspending active task " + id);
            } else if (state() == State.SUSPENDED) {
                // do nothing
            } else {
                transitionTo(State.SUSPENDED);
            }
        }

        @Override
        public void resume() {
            if (state() == State.SUSPENDED) {
                transitionTo(State.RUNNING);
            }
        }

        @Override
        public void revive() {
            //TODO: KAFKA-12569 move clearing of commit-required statuses to closeDirty/Clean/AndRecycle methods
            commitNeeded = false;
            commitRequested = false;
            super.revive();
        }

        @Override
        public void maybeInitTaskTimeoutOrThrow(final long currentWallClockMs,
                                                final Exception cause) {
            timeout = currentWallClockMs;
        }

        @Override
        public void clearTaskTimeout() {
            timeout = null;
        }

        @Override
        public void recordRestoration(final Time time, final long numRecords, final boolean initRemaining) {
            // do nothing
        }

        @Override
        public void closeClean() {
            transitionTo(State.CLOSED);
        }

        @Override
        public void closeDirty() {
            transitionTo(State.CLOSED);
        }

        @Override
        public void prepareRecycle() {
            transitionTo(State.CLOSED);
        }

        @Override
        public void resumePollingForPartitionsWithAvailableSpace() {
            // noop
        }

        @Override
        public void updateLags() {
            // noop
        }

        @Override
        public void updateInputPartitions(final Set<TopicPartition> topicPartitions, final Map<String, List<String>> allTopologyNodesToSourceTopics) {
            inputPartitions = topicPartitions;
        }

        void setCommittableOffsetsAndMetadata(final Map<TopicPartition, OffsetAndMetadata> committableOffsets) {
            if (!active) {
                throw new IllegalStateException("Cannot set CommittableOffsetsAndMetadate for StandbyTasks");
            }
            this.committableOffsets = committableOffsets;
        }

        @Override
        public StateStore store(final String name) {
            return null;
        }

        @Override
        public Set<TopicPartition> changelogPartitions() {
            return changelogOffsets.keySet();
        }

        public boolean isActive() {
            return active;
        }

        void setPurgeableOffsets(final Map<TopicPartition, Long> purgeableOffsets) {
            this.purgeableOffsets = purgeableOffsets;
        }

        @Override
        public Map<TopicPartition, Long> purgeableOffsets() {
            return purgeableOffsets;
        }

        void setChangelogOffsets(final Map<TopicPartition, Long> changelogOffsets) {
            this.changelogOffsets = changelogOffsets;
        }

        @Override
        public Map<TopicPartition, Long> changelogOffsets() {
            return changelogOffsets;
        }

        @Override
        public Map<TopicPartition, Long> committedOffsets() {
            return Collections.emptyMap();
        }

        @Override
        public Map<TopicPartition, Long> highWaterMark() {
            return Collections.emptyMap();
        }

        @Override
        public Optional<Long> timeCurrentIdlingStarted() {
            return Optional.empty();
        }

        @Override
        public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
            if (isActive()) {
                final Deque<ConsumerRecord<byte[], byte[]>> partitionQueue =
                    queue.computeIfAbsent(partition, k -> new LinkedList<>());

                for (final ConsumerRecord<byte[], byte[]> record : records) {
                    partitionQueue.add(record);
                }
            } else {
                throw new IllegalStateException("Can't add records to an inactive task.");
            }
        }

        @Override
        public boolean process(final long wallClockTime) {
            if (isActive() && state() == State.RUNNING) {
                for (final LinkedList<ConsumerRecord<byte[], byte[]>> records : queue.values()) {
                    final ConsumerRecord<byte[], byte[]> record = records.poll();
                    if (record != null) {
                        return true;
                    }
                }
                return false;
            } else {
                throw new IllegalStateException("Can't process an inactive or non-running task.");
            }
        }
    }
}
