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
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
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
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.StateDirectory.TaskDirectory;
import org.apache.kafka.streams.processor.internals.StateUpdater.ExceptionAndTask;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.apache.kafka.streams.processor.internals.tasks.DefaultTaskManager;
import org.apache.kafka.streams.processor.internals.testutil.DummyStreamsConfig;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;

import org.apache.logging.log4j.Level;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.intersection;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.union;
import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.standbyTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statefulTask;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
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
    final StateUpdater stateUpdater = mock(StateUpdater.class);
    final DefaultTaskManager schedulingTaskManager = mock(DefaultTaskManager.class);

    private TaskManager taskManager;
    private TopologyMetadata topologyMetadata;
    private final Time time = new MockTime();

    @TempDir
    Path testFolder;

    @BeforeEach
    public void setUp() {
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, null, false);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId03, activeTask)));
        assertEquals(taskManager.allRunningTasks(), mkMap(mkEntry(taskId03, activeTask)));
    }

    @Test
    public void shouldCreateActiveTaskDuringAssignment() {
        final StreamTask activeTaskToBeCreated = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        final IllegalStateException illegalStateException = assertThrows(
            IllegalStateException.class,
            () -> taskManager.handleAssignment(
                mkMap(mkEntry(standbyTaskToRecycle.id(), standbyTaskToRecycle.inputPartitions())),
                Collections.emptyMap()
            )
        );

        assertEquals("Standby tasks should only be managed by the state updater, " +
            "but standby task " + taskId03 + " is managed by the stream thread", illegalStateException.getMessage());
        verifyNoInteractions(activeTaskCreator);
    }

    @Test
    public void shouldAssignActiveTaskInTasksRegistryToBeClosedCleanly() {
        final StreamTask activeTaskToClose = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(standbyTaskToClose));

        final IllegalStateException illegalStateException = assertThrows(
            IllegalStateException.class,
            () -> taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap())
        );

        assertEquals("Standby tasks should only be managed by the state updater, " +
            "but standby task " + taskId03 + " is managed by the stream thread", illegalStateException.getMessage());
        verifyNoInteractions(activeTaskCreator);
    }

    @Test
    public void shouldAssignActiveTaskInTasksRegistryToUpdateInputPartitions() {
        final StreamTask activeTaskToUpdateInputPartitions = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final Set<TopicPartition> newInputPartitions = taskId02Partitions;
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(standbyTaskToUpdateInputPartitions));

        final IllegalStateException illegalStateException = assertThrows(
            IllegalStateException.class,
            () -> taskManager.handleAssignment(
                Collections.emptyMap(),
                mkMap(mkEntry(standbyTaskToUpdateInputPartitions.id(), newInputPartitions))
            )
        );

        assertEquals("Standby tasks should only be managed by the state updater, " +
            "but standby task " + taskId02 + " is managed by the stream thread", illegalStateException.getMessage());
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, false);

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
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, false);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verify(task00).initializeIfNeeded();
        verify(task01).initializeIfNeeded();
        verify(task00, never()).clearTaskTimeout();
        verify(task01).clearTaskTimeout();
        verify(tasks).addPendingTasksToInit(
            argThat(tasksToInit -> tasksToInit.contains(task00) && !tasksToInit.contains(task01))
        );
        verify(stateUpdater, never()).add(task00);
        verify(stateUpdater).add(task01);
    }

    @Test
    public void shouldRetryInitializationWhenTimeoutExceptionInStateUpdater() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RESTORING).build();
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(task00, task01));
        final TimeoutException timeoutException = new TimeoutException("Timed out!");
        doThrow(timeoutException).when(task00).initializeIfNeeded();
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, false);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verify(task00).initializeIfNeeded();
        verify(task01).initializeIfNeeded();
        verify(task00).maybeInitTaskTimeoutOrThrow(anyLong(), eq(timeoutException));
        verify(task00, never()).clearTaskTimeout();
        verify(task01).clearTaskTimeout();
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
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, false);

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
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, false);

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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, tasks, false);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertFalse(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));
    }

    @Test
    public void shouldReturnFalseFromCheckStateUpdaterIfActiveTasksAreNotRestoringAndNoPendingTaskToRecycleButPendingTasksToInit() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.hasPendingTasksToInit()).thenReturn(true);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertFalse(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));
    }

    @Test
    public void shouldReturnTrueFromCheckStateUpdaterIfActiveTasksAreNotRestoringAndNoPendingInit() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
            verify(restoredTask, atLeastOnce()).clearTaskTimeout();
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

    @Test
    public void shouldAddFailedRestoredTasksBackToStateUpdaterOnException() {
        final StreamTask task1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask task2 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId01Partitions).build();
        final StreamTask task3 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId02Partitions).build();

        // Use LinkedHashSet to ensure predictable iteration order
        final Set<StreamTask> restoredTasks = new java.util.LinkedHashSet<>();
        restoredTasks.add(task1);
        restoredTasks.add(task2);
        restoredTasks.add(task3);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTransitionToRunningOfRestoredTask(restoredTasks, tasks);

        // task1 completes successfully, task2 throws StreamsException from maybeInitTaskTimeoutOrThrow
        // task3 is never processed because task2 throws
        final TimeoutException timeoutException = new TimeoutException();
        doThrow(timeoutException).when(task2).completeRestoration(noOpResetter);
        doThrow(new StreamsException("Task timeout exceeded", task2.id())).when(task2).maybeInitTaskTimeoutOrThrow(anyLong(), eq(timeoutException));

        assertThrows(StreamsException.class, () -> taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));

        // task1 should be successfully transitioned
        verify(tasks).addTask(task1);
        verify(consumer).resume(task1.inputPartitions());
        verify(task1).clearTaskTimeout();

        // task2 should be added back to state updater once in the finally block
        // (the add in the catch block doesn't execute because maybeInitTaskTimeoutOrThrow throws)
        verify(stateUpdater).add(task2);
        verify(tasks, never()).addTask(task2);
        verify(task2, never()).clearTaskTimeout();

        // task3 should also be added back to state updater in the finally block
        verify(stateUpdater).add(task3);
        verify(tasks, never()).addTask(task3);
        verify(task3, never()).clearTaskTimeout();
    }

    private TaskManager setUpTransitionToRunningOfRestoredTask(final Set<StreamTask> statefulTasks,
                                                               final TasksRegistry tasks) {
        when(stateUpdater.restoresActiveTasks()).thenReturn(true);
        when(stateUpdater.drainRestoredActiveTasks(any(Duration.class))).thenReturn(statefulTasks);

        return setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
    }

    @Test
    public void shouldReturnCorrectBooleanWhenTryingToCompleteRestoration() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, null, false);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

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
    public void shouldPauseAllTopicsOnRebalanceComplete() {
        final Set<TopicPartition> assigned = Set.of(t1p0, t1p1);
        when(consumer.assignment()).thenReturn(assigned);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, null);
        taskManager.handleRebalanceComplete();

        verify(consumer).pause(assigned);
    }

    @Test
    public void shouldNotPauseReadyTasksOnRebalanceComplete() {
        final StreamTask statefulTask0 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(t1p0changelog, changelogOffsetOfRunningTask)
        );
        when(runningStatefulTask.changelogOffsets()).thenReturn(changelogOffsets);
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId00, runningStatefulTask)));

        assertThat(
            taskManager.taskOffsetSums(),
            is(mkMap(mkEntry(taskId00, changelogOffsetOfRunningTask)))
        );
    }

    @Test
    public void shouldComputeOffsetSumForNonRunningActiveTask() throws Exception {
        final StreamTask restoringStatefulTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING).build();
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), 5L),
            mkEntry(new TopicPartition("changelog", 1), 10L)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(
            mkEntry(taskId00, 15L)
        );
        when(restoringStatefulTask.changelogOffsets())
            .thenReturn(changelogOffsets);
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
        when(stateUpdater.tasks()).thenReturn(Set.of(restoringStatefulTask));

        assertThat(taskManager.taskOffsetSums(), is(expectedOffsetSums));
    }

    @Test
    public void shouldComputeOffsetSumForRestoringActiveTask() throws Exception {
        final StreamTask restoringStatefulTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING).build();
        final long changelogOffset = 42L;
        final Map<TaskId, Long> expectedOffsetSums = mkMap(
            mkEntry(taskId00, changelogOffset)
        );
        when(restoringStatefulTask.changelogOffsets()).thenReturn(mkMap(mkEntry(t1p0changelog, changelogOffset)));
        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        final Map<TopicPartition, Long> changelogOffsetInCheckpoint = mkMap(mkEntry(t1p0changelog, 24L));
        writeCheckpointFile(taskId00, changelogOffsetInCheckpoint);
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
        when(stateUpdater.tasks()).thenReturn(Set.of(restoringStatefulTask));
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.taskOffsetSums(), is(expectedOffsetSums));
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, false);
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId01, restoringStatefulTask)));
        when(stateUpdater.tasks()).thenReturn(Set.of(restoringStatefulTask));

        assertThat(
            taskManager.taskOffsetSums(),
            is(mkMap(
                mkEntry(taskId01, changelogOffsetOfRestoringStandbyTask)
            ))
        );
    }

    @Test
    public void shouldComputeOffsetSumForStandbyTask() throws Exception {
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), 5L),
            mkEntry(new TopicPartition("changelog", 1), 10L)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, 15L));

        final StandbyTask standbyTask = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();
        when(standbyTask.changelogOffsets()).thenReturn(changelogOffsets);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, false);

        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTask));

        expectLockObtainedFor(taskId00);
        expectDirectoryNotEmpty(taskId00);
        makeTaskFolders(taskId00.toString());

        taskManager.handleRebalanceStart(singleton("topic"));
        taskManager.handleAssignment(emptyMap(), taskId00Assignment);

        assertThat(taskManager.taskOffsetSums(), is(expectedOffsetSums));
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

    @ParameterizedTest
    @EnumSource(value = State.class, names = {"CREATED", "CLOSED"})
    public void shouldComputeOffsetSumFromCheckpointFileForCreatedAndClosedTasks(final State state) throws Exception {
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), 5L),
            mkEntry(new TopicPartition("changelog", 1), 10L)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, 15L));

        final StreamTask task = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(state)
            .withInputPartitions(taskId00Partitions)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId00, task)));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        writeCheckpointFile(taskId00, changelogOffsets);

        taskManager.handleRebalanceStart(singleton("topic"));

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
    public void shouldCloseActiveUnassignedSuspendedTasksWhenClosingRevokedTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.SUSPENDED)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleAssignment(emptyMap(), emptyMap());

        verify(task00).prepareCommit(true);
        verify(task00).closeClean();
        verify(tasks).removeTask(task00);
    }

    @Test
    public void shouldCloseDirtyActiveUnassignedTasksWhenErrorCleanClosingTask() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.SUSPENDED)
            .build();

        doThrow(new RuntimeException("KABOOM!")).when(task00).closeClean();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allNonFailedTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );

        verify(task00).closeClean();
        verify(task00).closeDirty();
        verify(tasks).removeTask(task00);
        assertThat(
            thrown.getMessage(),
            is("Encounter unexpected fatal error for task 0_0")
        );
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldCloseActiveTasksWhenHandlingLostTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00, task01));
        when(tasks.allTaskIds()).thenReturn(Set.of(taskId00, taskId01));

        final ArrayList<TaskDirectory> taskFolders = new ArrayList<>(2);
        taskFolders.add(new TaskDirectory(testFolder.resolve(taskId00.toString()).toFile(), null));
        taskFolders.add(new TaskDirectory(testFolder.resolve(taskId01.toString()).toFile(), null));

        when(stateDirectory.listNonEmptyTaskDirectories())
            .thenReturn(taskFolders)
            .thenReturn(new ArrayList<>());

        expectLockObtainedFor(taskId00, taskId01);
        expectDirectoryNotEmpty(taskId00, taskId01);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleRebalanceStart(emptySet());
        assertThat(taskManager.lockedTaskDirectories(), is(Set.of(taskId00, taskId01)));

        // this should close only active tasks as zombies
        taskManager.handleLostAll();

        // close of active task
        verify(task00).prepareCommit(false);
        verify(task00).suspend();
        verify(task00).closeDirty();
        verify(tasks).removeTask(task00);

        // standby task not closed
        verify(task01, never()).prepareCommit(anyBoolean());
        verify(task01, never()).suspend();
        verify(task01, never()).closeDirty();
        verify(task01, never()).closeClean();
        verify(tasks, never()).removeTask(task01);

        // The locked task map will not be cleared.
        assertThat(taskManager.lockedTaskDirectories(), is(Set.of(taskId00, taskId01)));

        taskManager.handleRebalanceStart(emptySet());

        assertThat(taskManager.lockedTaskDirectories(), is(emptySet()));
    }

    @Test
    public void shouldReInitializeStreamsProducerOnHandleLostAllIfEosV2Enabled() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, null, false);

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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
    public void shouldReviveCorruptTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.task(taskId00)).thenReturn(task00);
        when(tasks.allTasksPerId()).thenReturn(singletonMap(taskId00, task00));
        when(tasks.activeTaskIds()).thenReturn(Set.of(taskId00));

        when(task00.prepareCommit(false)).thenReturn(emptyMap());
        doNothing().when(task00).postCommit(anyBoolean());
        when(task00.changelogPartitions()).thenReturn(taskId00ChangelogPartitions);

        when(consumer.assignment()).thenReturn(taskId00Partitions);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleCorruption(singleton(taskId00));

        verify(task00).prepareCommit(false);
        verify(task00).postCommit(true);
        verify(task00).addPartitionsForOffsetReset(taskId00Partitions);
        verify(task00).closeDirty();
        verify(task00).revive();
        verify(tasks).removeTask(task00);
        verify(tasks).addPendingTasksToInit(Set.of(task00));
        verify(consumer, never()).commitSync(emptyMap());
    }

    @Test
    public void shouldReviveCorruptTasksEvenIfTheyCannotCloseClean() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.task(taskId00)).thenReturn(task00);
        when(tasks.allTasksPerId()).thenReturn(singletonMap(taskId00, task00));
        when(tasks.activeTaskIds()).thenReturn(Set.of(taskId00));

        when(task00.prepareCommit(false)).thenReturn(emptyMap());
        when(task00.changelogPartitions()).thenReturn(taskId00ChangelogPartitions);
        doThrow(new RuntimeException("oops")).when(task00).suspend();

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleCorruption(singleton(taskId00));

        verify(task00).prepareCommit(false);
        verify(task00).suspend();
        verify(task00, never()).postCommit(anyBoolean()); // postCommit is NOT called
        verify(task00).closeDirty();
        verify(task00).revive();
        verify(tasks).removeTask(task00);
        verify(tasks).addPendingTasksToInit(Set.of(task00));
        verify(task00).addPartitionsForOffsetReset(emptySet());
    }

    @Test
    public void shouldCommitNonCorruptedTasksOnTaskCorruptedException() {
        final StreamTask corruptedTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final StreamTask nonCorruptedTask = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.task(taskId00)).thenReturn(corruptedTask);
        when(tasks.allTasksPerId()).thenReturn(mkMap(
            mkEntry(taskId00, corruptedTask),
            mkEntry(taskId01, nonCorruptedTask)
        ));
        when(tasks.activeTaskIds()).thenReturn(Set.of(taskId00, taskId01));

        when(nonCorruptedTask.commitNeeded()).thenReturn(true);
        when(nonCorruptedTask.prepareCommit(true)).thenReturn(emptyMap());
        when(corruptedTask.prepareCommit(false)).thenReturn(emptyMap());
        doNothing().when(corruptedTask).postCommit(anyBoolean());

        when(consumer.assignment()).thenReturn(taskId00Partitions);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleCorruption(Set.of(taskId00));

        verify(nonCorruptedTask).prepareCommit(true);
        verify(nonCorruptedTask, never()).addPartitionsForOffsetReset(any());
        verify(corruptedTask).addPartitionsForOffsetReset(taskId00Partitions);
        verify(corruptedTask).postCommit(true);

        // check that we should not commit empty map either
        verify(consumer, never()).commitSync(emptyMap());
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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
    public void shouldCleanAndReviveCorruptedStandbyTasksBeforeCommittingNonCorruptedTasks() {
        final StandbyTask corruptedStandby = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask runningNonCorruptedActive = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.task(taskId00)).thenReturn(corruptedStandby);
        when(tasks.allTasksPerId()).thenReturn(mkMap(
            mkEntry(taskId00, corruptedStandby),
            mkEntry(taskId01, runningNonCorruptedActive)
        ));
        when(tasks.activeTaskIds()).thenReturn(Set.of(taskId01));

        when(runningNonCorruptedActive.commitNeeded()).thenReturn(true);
        when(runningNonCorruptedActive.prepareCommit(true))
            .thenThrow(new TaskMigratedException("You dropped out of the group!", new RuntimeException()));

        when(corruptedStandby.changelogPartitions()).thenReturn(taskId00ChangelogPartitions);
        when(corruptedStandby.prepareCommit(false)).thenReturn(emptyMap());
        doNothing().when(corruptedStandby).suspend();
        doNothing().when(corruptedStandby).postCommit(anyBoolean());

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertThrows(TaskMigratedException.class, () -> taskManager.handleCorruption(singleton(taskId00)));

        // verifying the entire task lifecycle
        final InOrder taskOrder = inOrder(corruptedStandby, runningNonCorruptedActive);
        taskOrder.verify(corruptedStandby).prepareCommit(false);
        taskOrder.verify(corruptedStandby).suspend();
        taskOrder.verify(corruptedStandby).postCommit(true);
        taskOrder.verify(corruptedStandby).closeDirty();
        taskOrder.verify(corruptedStandby).revive();
        taskOrder.verify(runningNonCorruptedActive).prepareCommit(true);

        verify(tasks).removeTask(corruptedStandby);
        verify(tasks).addPendingTasksToInit(Set.of(corruptedStandby));
    }

    @Test
    public void shouldNotAttemptToCommitInHandleCorruptedDuringARebalance() {
        final StreamTask corruptedActive = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final StreamTask uncorruptedActive = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.task(taskId00)).thenReturn(corruptedActive);
        when(tasks.allTasksPerId()).thenReturn(mkMap(
            mkEntry(taskId00, corruptedActive),
            mkEntry(taskId01, uncorruptedActive)
        ));
        when(tasks.activeTaskIds()).thenReturn(Set.of(taskId00, taskId01));

        when(uncorruptedActive.commitNeeded()).thenReturn(true);
        when(uncorruptedActive.prepareCommit(true)).thenReturn(emptyMap());

        when(corruptedActive.prepareCommit(false)).thenReturn(emptyMap());
        doNothing().when(corruptedActive).postCommit(anyBoolean());

        when(consumer.assignment()).thenReturn(taskId00Partitions);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleRebalanceStart(singleton(topic1));
        assertThat(taskManager.rebalanceInProgress(), is(true));

        taskManager.handleCorruption(singleton(taskId00));

        verify(uncorruptedActive, never()).prepareCommit(anyBoolean());
        verify(uncorruptedActive, never()).postCommit(anyBoolean());

        verify(corruptedActive).postCommit(true);
        verify(corruptedActive).addPartitionsForOffsetReset(taskId00Partitions);
        verify(consumer, never()).commitSync(emptyMap());
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitDuringHandleCorruptedWithEOS() {
        final StreamTask corruptedActive = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        // this task will time out during commit
        final StreamTask uncorruptedActive = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.task(taskId00)).thenReturn(corruptedActive);
        when(tasks.allTasksPerId()).thenReturn(mkMap(
            mkEntry(taskId00, corruptedActive),
            mkEntry(taskId01, uncorruptedActive)
        ));
        when(tasks.activeTaskIds()).thenReturn(Set.of(taskId00, taskId01));

        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.streamsProducer()).thenReturn(producer);
        final ConsumerGroupMetadata groupMetadata = mock(ConsumerGroupMetadata.class);
        
        when(consumer.groupMetadata()).thenReturn(groupMetadata);
        when(consumer.assignment()).thenReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions));

        // mock uncorrupted task to indicate that it needs commit and will return offsets
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        when(tasks.tasks(singleton(taskId01))).thenReturn(Set.of(uncorruptedActive));
        when(uncorruptedActive.commitNeeded()).thenReturn(true);
        when(uncorruptedActive.prepareCommit(true)).thenReturn(offsets);
        when(uncorruptedActive.prepareCommit(false)).thenReturn(emptyMap());
        when(uncorruptedActive.changelogPartitions()).thenReturn(taskId01ChangelogPartitions);
        doNothing().when(uncorruptedActive).suspend();
        doNothing().when(uncorruptedActive).closeDirty();
        doNothing().when(uncorruptedActive).revive();
        doNothing().when(uncorruptedActive).markChangelogAsCorrupted(taskId01ChangelogPartitions);

        // corrupted task doesn't need commit
        when(corruptedActive.commitNeeded()).thenReturn(false);
        when(corruptedActive.prepareCommit(false)).thenReturn(emptyMap());
        when(corruptedActive.changelogPartitions()).thenReturn(taskId00ChangelogPartitions);
        doNothing().when(corruptedActive).suspend();
        doNothing().when(corruptedActive).postCommit(true);
        doNothing().when(corruptedActive).closeDirty();
        doNothing().when(corruptedActive).revive();

        doThrow(new TimeoutException()).when(producer).commitTransaction(offsets, groupMetadata);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, tasks);

        taskManager.handleCorruption(singleton(taskId00));

        // 1. verify corrupted task was closed dirty and revived
        final InOrder corruptedOrder = inOrder(corruptedActive, tasks);
        corruptedOrder.verify(corruptedActive).prepareCommit(false);
        corruptedOrder.verify(corruptedActive).suspend();
        corruptedOrder.verify(corruptedActive).postCommit(true);
        corruptedOrder.verify(corruptedActive).closeDirty();
        corruptedOrder.verify(tasks).removeTask(corruptedActive);
        corruptedOrder.verify(corruptedActive).revive();
        corruptedOrder.verify(tasks).addPendingTasksToInit(Set.of(corruptedActive));

        // 2. verify uncorrupted task attempted commit, failed with timeout, then was closed dirty and revived
        final InOrder uncorruptedOrder = inOrder(uncorruptedActive, producer, tasks);
        uncorruptedOrder.verify(uncorruptedActive).prepareCommit(true);
        uncorruptedOrder.verify(producer).commitTransaction(offsets, groupMetadata); // tries to commit, throws TimeoutException
        uncorruptedOrder.verify(uncorruptedActive).suspend();
        uncorruptedOrder.verify(uncorruptedActive).postCommit(true);
        uncorruptedOrder.verify(uncorruptedActive).closeDirty();
        uncorruptedOrder.verify(tasks).removeTask(uncorruptedActive);
        uncorruptedOrder.verify(uncorruptedActive).revive();
        uncorruptedOrder.verify(tasks).addPendingTasksToInit(Set.of(uncorruptedActive));

        // verify both tasks had their input partitions reset
        verify(corruptedActive).addPartitionsForOffsetReset(taskId00Partitions);
        verify(uncorruptedActive).addPartitionsForOffsetReset(taskId01Partitions);
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitWithAlos() {
        final StreamTask corruptedActive = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        // this task will time out during commit
        final StreamTask uncorruptedActive = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.task(taskId00)).thenReturn(corruptedActive);
        when(tasks.allTasksPerId()).thenReturn(mkMap(
            mkEntry(taskId00, corruptedActive),
            mkEntry(taskId01, uncorruptedActive)
        ));
        when(tasks.activeTaskIds()).thenReturn(Set.of(taskId00, taskId01));
        when(tasks.activeTasks()).thenReturn(Set.of(corruptedActive, uncorruptedActive));

        // we need to mock uncorrupted task to indicate that it needs commit and will return offsets
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        when(uncorruptedActive.commitNeeded()).thenReturn(true);
        when(uncorruptedActive.prepareCommit(true)).thenReturn(offsets);
        when(uncorruptedActive.changelogPartitions()).thenReturn(taskId01ChangelogPartitions);
        doNothing().when(uncorruptedActive).suspend();
        doNothing().when(uncorruptedActive).closeDirty();
        doNothing().when(uncorruptedActive).revive();

        // corrupted task doesn't need commit
        when(corruptedActive.commitNeeded()).thenReturn(false);
        when(corruptedActive.prepareCommit(false)).thenReturn(emptyMap());
        when(corruptedActive.changelogPartitions()).thenReturn(taskId00ChangelogPartitions);
        doNothing().when(corruptedActive).suspend();
        doNothing().when(corruptedActive).postCommit(anyBoolean());
        doNothing().when(corruptedActive).closeDirty();
        doNothing().when(corruptedActive).revive();

        doThrow(new TimeoutException()).when(consumer).commitSync(offsets);
        when(consumer.assignment()).thenReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleCorruption(singleton(taskId00));

        // 1. verify corrupted task was closed dirty and revived
        final InOrder corruptedOrder = inOrder(corruptedActive, tasks);
        corruptedOrder.verify(corruptedActive).prepareCommit(false);
        corruptedOrder.verify(corruptedActive).suspend();
        corruptedOrder.verify(corruptedActive).postCommit(true);
        corruptedOrder.verify(corruptedActive).closeDirty();
        corruptedOrder.verify(tasks).removeTask(corruptedActive);
        corruptedOrder.verify(corruptedActive).revive();
        corruptedOrder.verify(tasks).addPendingTasksToInit(Set.of(corruptedActive));

        // 2. verify uncorrupted task attempted commit, failed with timeout, then was closed dirty and revived
        final InOrder uncorruptedOrder = inOrder(uncorruptedActive, consumer, tasks);
        uncorruptedOrder.verify(uncorruptedActive).prepareCommit(true);
        uncorruptedOrder.verify(consumer).commitSync(offsets); // attempt commit, throws TimeoutException
        uncorruptedOrder.verify(uncorruptedActive).prepareCommit(false);
        uncorruptedOrder.verify(uncorruptedActive).suspend();
        uncorruptedOrder.verify(uncorruptedActive).closeDirty();
        uncorruptedOrder.verify(tasks).removeTask(uncorruptedActive);
        uncorruptedOrder.verify(uncorruptedActive).revive();
        uncorruptedOrder.verify(tasks).addPendingTasksToInit(Set.of(uncorruptedActive));

        // verify both tasks had their input partitions reset
        verify(corruptedActive).addPartitionsForOffsetReset(taskId00Partitions);
        verify(uncorruptedActive).addPartitionsForOffsetReset(taskId01Partitions);
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitDuringRevocationWithAlos() {
        // task being revoked - needs commit
        final StreamTask revokedActiveTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        // unrevoked task that needs commit - this will also be affected by timeout
        final StreamTask unrevokedActiveTaskWithCommit = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        // unrevoked task without commit needed - this should stay RUNNING
        final StreamTask unrevokedActiveTaskWithoutCommit = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(revokedActiveTask, unrevokedActiveTaskWithCommit, unrevokedActiveTaskWithoutCommit));

        when(consumer.assignment()).thenReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions, taskId02Partitions));

        // revoked task needs commit
        final Map<TopicPartition, OffsetAndMetadata> revokedTaskOffsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        when(revokedActiveTask.commitNeeded()).thenReturn(true);
        when(revokedActiveTask.prepareCommit(true)).thenReturn(revokedTaskOffsets);
        when(revokedActiveTask.changelogPartitions()).thenReturn(taskId00ChangelogPartitions);
        doNothing().when(revokedActiveTask).suspend();
        doNothing().when(revokedActiveTask).closeDirty();
        doNothing().when(revokedActiveTask).revive();

        // unrevoked task with commit also takes part in commit
        final Map<TopicPartition, OffsetAndMetadata> unrevokedTaskOffsets = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        when(unrevokedActiveTaskWithCommit.commitNeeded()).thenReturn(true);
        when(unrevokedActiveTaskWithCommit.prepareCommit(true)).thenReturn(unrevokedTaskOffsets);
        when(unrevokedActiveTaskWithCommit.changelogPartitions()).thenReturn(taskId01ChangelogPartitions);
        doNothing().when(unrevokedActiveTaskWithCommit).suspend();
        doNothing().when(unrevokedActiveTaskWithCommit).closeDirty();
        doNothing().when(unrevokedActiveTaskWithCommit).revive();

        // unrevoked task without commit needed
        when(unrevokedActiveTaskWithoutCommit.commitNeeded()).thenReturn(false);

        // mock timeout during commit - all offsets from tasks needing commit
        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(revokedTaskOffsets);
        expectedCommittedOffsets.putAll(unrevokedTaskOffsets);
        doThrow(new TimeoutException()).when(consumer).commitSync(expectedCommittedOffsets);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleRevocation(taskId00Partitions);

        // 1. verify that the revoked task was suspended, closed dirty, and revived
        final InOrder revokedOrder = inOrder(revokedActiveTask, tasks);
        revokedOrder.verify(revokedActiveTask).prepareCommit(true);
        revokedOrder.verify(revokedActiveTask).suspend();
        revokedOrder.verify(revokedActiveTask).closeDirty();
        revokedOrder.verify(tasks).removeTask(revokedActiveTask);
        revokedOrder.verify(revokedActiveTask).revive();
        revokedOrder.verify(tasks).addPendingTasksToInit(argThat(set -> set.contains(revokedActiveTask)));

        // 2. verify that the unrevoked task with commit also tried to commit and was closed dirty due to timeout
        final InOrder unrevokedOrder = inOrder(unrevokedActiveTaskWithCommit, consumer, tasks);
        unrevokedOrder.verify(unrevokedActiveTaskWithCommit).prepareCommit(true);
        unrevokedOrder.verify(consumer).commitSync(expectedCommittedOffsets); // timeout thrown here
        unrevokedOrder.verify(unrevokedActiveTaskWithCommit).suspend();
        unrevokedOrder.verify(unrevokedActiveTaskWithCommit).closeDirty();
        unrevokedOrder.verify(tasks).removeTask(unrevokedActiveTaskWithCommit);
        unrevokedOrder.verify(unrevokedActiveTaskWithCommit).revive();
        unrevokedOrder.verify(tasks).addPendingTasksToInit(argThat(set -> set.contains(unrevokedActiveTaskWithCommit)));

        // 3. verify that the unrevoked task without commit needed was not affected
        verify(unrevokedActiveTaskWithoutCommit, never()).prepareCommit(anyBoolean());
        verify(unrevokedActiveTaskWithoutCommit, never()).suspend();
        verify(unrevokedActiveTaskWithoutCommit, never()).closeDirty();

        // input partitions were reset for affected tasks
        verify(revokedActiveTask).addPartitionsForOffsetReset(taskId00Partitions);
        verify(unrevokedActiveTaskWithCommit).addPartitionsForOffsetReset(taskId01Partitions);
        verify(unrevokedActiveTaskWithoutCommit, never()).addPartitionsForOffsetReset(any());
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitDuringRevocationWithEOS() {
        // task being revoked - needs commit
        final StreamTask revokedActiveTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        // unrevoked task that needs commit - this will also be affected by timeout
        final StreamTask unrevokedActiveTaskWithCommit = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        // unrevoked task without commit needed - this should remain RUNNING
        final StreamTask unrevokedActiveTaskWithoutCommit = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(revokedActiveTask, unrevokedActiveTaskWithCommit, unrevokedActiveTaskWithoutCommit));
        when(tasks.tasks(Set.of(taskId00, taskId01))).thenReturn(Set.of(revokedActiveTask, unrevokedActiveTaskWithCommit));

        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.streamsProducer()).thenReturn(producer);
        final ConsumerGroupMetadata groupMetadata = mock(ConsumerGroupMetadata.class);
        when(consumer.groupMetadata()).thenReturn(groupMetadata);
        when(consumer.assignment()).thenReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions, taskId02Partitions));

        // revoked task needs commit
        final Map<TopicPartition, OffsetAndMetadata> revokedTaskOffsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        when(revokedActiveTask.commitNeeded()).thenReturn(true);
        when(revokedActiveTask.prepareCommit(true)).thenReturn(revokedTaskOffsets);
        when(revokedActiveTask.changelogPartitions()).thenReturn(taskId00ChangelogPartitions);
        doNothing().when(revokedActiveTask).suspend();
        doNothing().when(revokedActiveTask).closeDirty();
        doNothing().when(revokedActiveTask).revive();
        doNothing().when(revokedActiveTask).markChangelogAsCorrupted(taskId00ChangelogPartitions);

        // unrevoked task with commit also takes part in EOS-v2 commit
        final Map<TopicPartition, OffsetAndMetadata> unrevokedTaskOffsets = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        when(unrevokedActiveTaskWithCommit.commitNeeded()).thenReturn(true);
        when(unrevokedActiveTaskWithCommit.prepareCommit(true)).thenReturn(unrevokedTaskOffsets);
        when(unrevokedActiveTaskWithCommit.changelogPartitions()).thenReturn(taskId01ChangelogPartitions);
        doNothing().when(unrevokedActiveTaskWithCommit).suspend();
        doNothing().when(unrevokedActiveTaskWithCommit).closeDirty();
        doNothing().when(unrevokedActiveTaskWithCommit).revive();
        doNothing().when(unrevokedActiveTaskWithCommit).markChangelogAsCorrupted(taskId01ChangelogPartitions);

        // unrevoked task without commit needed
        when(unrevokedActiveTaskWithoutCommit.commitNeeded()).thenReturn(false);

        // mock timeout during commit - all offsets from tasks needing commit
        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(revokedTaskOffsets);
        expectedCommittedOffsets.putAll(unrevokedTaskOffsets);
        doThrow(new TimeoutException()).when(producer).commitTransaction(expectedCommittedOffsets, groupMetadata);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, tasks);

        taskManager.handleRevocation(taskId00Partitions);

        // 1. verify that the revoked task was suspended, closed dirty, and revived
        final InOrder revokedOrder = inOrder(revokedActiveTask, tasks);
        revokedOrder.verify(revokedActiveTask).prepareCommit(true);
        revokedOrder.verify(revokedActiveTask).suspend();
        revokedOrder.verify(revokedActiveTask).closeDirty();
        revokedOrder.verify(tasks).removeTask(revokedActiveTask);
        revokedOrder.verify(revokedActiveTask).revive();
        revokedOrder.verify(tasks).addPendingTasksToInit(argThat(set -> set.contains(revokedActiveTask)));

        // 2. verify that the unrevoked task with commit also tried to commit and was closed dirty due to timeout
        final InOrder unrevokedOrder = inOrder(unrevokedActiveTaskWithCommit, producer, tasks);
        unrevokedOrder.verify(unrevokedActiveTaskWithCommit).prepareCommit(true);
        unrevokedOrder.verify(producer).commitTransaction(expectedCommittedOffsets, groupMetadata); // timeout thrown here
        unrevokedOrder.verify(unrevokedActiveTaskWithCommit).suspend();
        unrevokedOrder.verify(unrevokedActiveTaskWithCommit).closeDirty();
        unrevokedOrder.verify(tasks).removeTask(unrevokedActiveTaskWithCommit);
        unrevokedOrder.verify(unrevokedActiveTaskWithCommit).revive();
        unrevokedOrder.verify(tasks).addPendingTasksToInit(argThat(set -> set.contains(unrevokedActiveTaskWithCommit)));

        // 3. verify that the unrevoked task without commit needed was not affected
        verify(unrevokedActiveTaskWithoutCommit, never()).prepareCommit(anyBoolean());
        verify(unrevokedActiveTaskWithoutCommit, never()).suspend();
        verify(unrevokedActiveTaskWithoutCommit, never()).closeDirty();

        // verify input partitions were reset for affected tasks
        verify(revokedActiveTask).addPartitionsForOffsetReset(taskId00Partitions);
        verify(unrevokedActiveTaskWithCommit).addPartitionsForOffsetReset(taskId01Partitions);
        verify(unrevokedActiveTaskWithoutCommit, never()).addPartitionsForOffsetReset(any());
    }

    @Test
    public void shouldCloseStandbyUnassignedTasksWhenCreatingNewTasks() {
        final StandbyTask task00 = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.drainPendingTasksToInit()).thenReturn(emptySet());

        taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(stateUpdater.tasks()).thenReturn(Set.of(task00));

        // mock future for removing task from StateUpdater
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(task00.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(task00));

        taskManager.handleAssignment(emptyMap(), emptyMap());

        verify(stateUpdater).remove(task00.id());
        verify(task00).suspend();
        verify(task00).closeClean();

        verify(activeTaskCreator).createTasks(any(), eq(emptyMap()));
        verify(standbyTaskCreator).createTasks(emptyMap());
    }

    @Test
    public void shouldAddNonResumedSuspendedTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);

        when(tasks.allNonFailedTasks()).thenReturn(Set.of(task00));

        when(tasks.drainPendingTasksToInit()).thenReturn(emptySet());
        when(tasks.hasPendingTasksToInit()).thenReturn(false);

        taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(stateUpdater.tasks()).thenReturn(Set.of(task01));
        when(stateUpdater.restoresActiveTasks()).thenReturn(false);
        when(stateUpdater.hasExceptionsAndFailedTasks()).thenReturn(false);

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);

        // checkStateUpdater should return true (all tasks ready, no pending work)
        assertTrue(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));

        verify(stateUpdater, never()).add(any(Task.class));
        verify(activeTaskCreator).createTasks(any(), eq(emptyMap()));
        verify(standbyTaskCreator).createTasks(emptyMap());

        // verify idempotence
        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertTrue(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));
        verify(stateUpdater, never()).add(any(Task.class));
    }

    @Test
    public void shouldUpdateInputPartitionsAfterRebalance() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final Set<TopicPartition> newPartitionsSet = Set.of(t1p1);

        when(tasks.allNonFailedTasks()).thenReturn(Set.of(task00));
        when(tasks.drainPendingTasksToInit()).thenReturn(emptySet());
        when(tasks.hasPendingTasksToInit()).thenReturn(false);
        when(tasks.updateActiveTaskInputPartitions(task00, newPartitionsSet)).thenReturn(true);

        taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(stateUpdater.tasks()).thenReturn(emptySet());
        when(stateUpdater.restoresActiveTasks()).thenReturn(false);
        when(stateUpdater.hasExceptionsAndFailedTasks()).thenReturn(false);

        final Map<TaskId, Set<TopicPartition>> taskIdSetMap = singletonMap(taskId00, newPartitionsSet);
        taskManager.handleAssignment(taskIdSetMap, emptyMap());

        verify(task00).updateInputPartitions(eq(newPartitionsSet), any());
        assertTrue(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter));
        assertThat(task00.state(), is(Task.State.RUNNING));
        verify(activeTaskCreator).createTasks(any(), eq(emptyMap()));
        verify(standbyTaskCreator).createTasks(emptyMap());
    }

    @Test
    public void shouldAddNewActiveTasks() {
        // task in created state
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId00Partitions)
            .build();

        final Map<TaskId, Set<TopicPartition>> assignment = taskId00Assignment;
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        // first, we need to handle assignment -- creates tasks and adds to pending initialization
        when(activeTaskCreator.createTasks(any(), eq(assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(assignment, emptyMap());

        verify(tasks).addPendingTasksToInit(singletonList(task00));

        // next, drain pending tasks, initialize them, and then add to stateupdater
        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(task00));

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verify(task00).initializeIfNeeded();
        verify(stateUpdater).add(task00);

        // last, drain the restored tasks from stateupdater and transition to running
        when(stateUpdater.restoresActiveTasks()).thenReturn(true);
        when(stateUpdater.drainRestoredActiveTasks(any(Duration.class))).thenReturn(Set.of(task00));

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verifyTransitionToRunningOfRestoredTask(Set.of(task00), tasks);
    }

    @Test
    public void shouldNotCompleteRestorationIfTasksCannotInitialize() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.CREATED)
            .build();
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.CREATED)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions)
        );

        when(activeTaskCreator.createTasks(any(), eq(assignment)))
            .thenReturn(asList(task00, task01));
        taskManager.handleAssignment(assignment, emptyMap());

        verify(tasks).addPendingTasksToInit(asList(task00, task01));

        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(task00, task01));
        final LockException lockException = new LockException("can't lock");
        final TimeoutException timeoutException = new TimeoutException("timeout during init");
        doThrow(lockException).when(task00).initializeIfNeeded();
        doThrow(timeoutException).when(task01).initializeIfNeeded();
        when(tasks.hasPendingTasksToInit()).thenReturn(true);

        final boolean restorationComplete = taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        assertFalse(restorationComplete);
        verify(task00).initializeIfNeeded();
        verify(task01).initializeIfNeeded();
        verify(task00, never()).maybeInitTaskTimeoutOrThrow(anyLong(), any());
        verify(task01).maybeInitTaskTimeoutOrThrow(anyLong(), eq(timeoutException));
        verify(task00, never()).clearTaskTimeout();
        verify(task01, never()).clearTaskTimeout();
        verify(tasks).addPendingTasksToInit(Collections.singleton(task00));
        verify(tasks).addPendingTasksToInit(Collections.singleton(task01));
        verify(stateUpdater, never()).add(task00);
        verify(stateUpdater, never()).add(task01);
        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldNotCompleteRestorationIfTaskCannotCompleteRestoration() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RESTORING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(stateUpdater.restoresActiveTasks()).thenReturn(true);
        when(stateUpdater.drainRestoredActiveTasks(any(Duration.class))).thenReturn(Set.of(task00));
        final TimeoutException timeoutException = new TimeoutException("timeout!");
        doThrow(timeoutException).when(task00).completeRestoration(any());

        final boolean restorationComplete = taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        assertFalse(restorationComplete);
        verify(task00).completeRestoration(any());
        verify(stateUpdater).add(task00);
        verify(tasks, never()).addTask(task00);
        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldSuspendActiveTasksDuringRevocation() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00));

        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));

        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenReturn(offsets);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleRevocation(taskId00Partitions);

        verify(task00).prepareCommit(true);
        verify(task00).postCommit(true);
        verify(task00).suspend();
    }

    @Test
    public void shouldCommitAllActiveTasksThatNeedCommittingOnHandleRevocationWithEosV2() {
        // task being revoked, needs commit
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        // unrevoked task that needs commit, this should also be committed with EOS-v2
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        // unrevoked task that doesn't need commit, should not be committed
        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING)
            .build();

        // standby task should not be committed
        final StandbyTask task10 = standbyTask(taskId10, emptySet())
            .withInputPartitions(taskId10Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);

        when(tasks.allTasks()).thenReturn(Set.of(task00, task01, task02, task10));

        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.streamsProducer()).thenReturn(producer);
        final ConsumerGroupMetadata groupMetadata = mock(ConsumerGroupMetadata.class);
        when(consumer.groupMetadata()).thenReturn(groupMetadata);

        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenReturn(offsets00);
        doNothing().when(task00).postCommit(anyBoolean());
        doNothing().when(task00).suspend();

        final Map<TopicPartition, OffsetAndMetadata> offsets01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsets01);
        doNothing().when(task01).postCommit(anyBoolean());

        // task02 does not need commit
        when(task02.commitNeeded()).thenReturn(false);

        // standby task should not take part in commit
        when(task10.commitNeeded()).thenReturn(false);

        // expected committed offsets, only task00 and task01 (both need commit)
        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(offsets00);
        expectedCommittedOffsets.putAll(offsets01);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, tasks);

        taskManager.handleRevocation(taskId00Partitions);

        // Verify the commit transaction was called with offsets from task00 and task01
        verify(producer).commitTransaction(expectedCommittedOffsets, groupMetadata);

        // Verify task00 (revoked) was suspended and committed
        verify(task00).prepareCommit(true);
        verify(task00).postCommit(true);
        verify(task00).suspend();

        // Verify task01 (unrevoked but needs commit) was also committed
        verify(task01).prepareCommit(true);
        verify(task01).postCommit(false);

        // Verify task02 (doesn't need commit) was not committed
        verify(task02, never()).prepareCommit(anyBoolean());
        verify(task02, never()).postCommit(anyBoolean());

        // Verify standby task10 was not committed
        verify(task10, never()).prepareCommit(anyBoolean());
        verify(task10, never()).postCommit(anyBoolean());
    }

    @Test
    public void shouldCommitAllNeededTasksOnHandleRevocation() {
        // revoked task that needs commit
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenReturn(offsets00);

        // non revoked task that needs commit
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();
        final Map<TopicPartition, OffsetAndMetadata> offsets01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsets01);

        // non revoked task that does NOT need commit
        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING)
            .build();
        when(task02.commitNeeded()).thenReturn(false);

        // standby task (not be affected by revocation)
        final StandbyTask task03 = standbyTask(taskId03, taskId03ChangelogPartitions)
            .withInputPartitions(taskId03Partitions)
            .inState(State.RUNNING)
            .build();

        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(offsets00);
        expectedCommittedOffsets.putAll(offsets01);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00, task01, task02, task03));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleRevocation(taskId00Partitions);

        // both tasks needing commit had prepareCommit called
        verify(task00).prepareCommit(true);
        verify(task01).prepareCommit(true);
        verify(task02, never()).prepareCommit(anyBoolean());
        verify(task03, never()).prepareCommit(anyBoolean());

        verify(consumer).commitSync(expectedCommittedOffsets);

        // revoked task suspended
        verify(task00).suspend();
        verify(task00).postCommit(true);

        // non-revoked task with commit was also post-committed (but not suspended)
        verify(task01).postCommit(false);
        verify(task01, never()).suspend();

        // task02 and task03 should not be affected
        verify(task02, never()).postCommit(anyBoolean());
        verify(task02, never()).suspend();
        verify(task03, never()).postCommit(anyBoolean());
        verify(task03, never()).suspend();
    }

    @ParameterizedTest
    @EnumSource(ProcessingMode.class)
    public void shouldNotCommitIfNoRevokedTasksNeedCommitting(final ProcessingMode processingMode) {
        // task00 being revoked, no commit needed
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        // task01 NOT being revoked, commit needed
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        // task02 NOT being revoked, no commit needed
        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00, task01, task02));

        when(task00.commitNeeded()).thenReturn(false);
        when(task01.commitNeeded()).thenReturn(true); // only task01 needs commit
        when(task02.commitNeeded()).thenReturn(false);

        final TaskManager taskManager = setUpTaskManager(processingMode, tasks);

        taskManager.handleRevocation(taskId00Partitions);

        verify(task00, never()).prepareCommit(anyBoolean());
        verify(task01, never()).prepareCommit(anyBoolean());
        verify(task02, never()).prepareCommit(anyBoolean());

        verify(task00).suspend();
        verify(task01, never()).suspend();
        verify(task02, never()).suspend();
    }

    @Test
    public void shouldNotCommitOnHandleAssignmentIfNoTaskClosed() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(tasks.allNonFailedTasks()).thenReturn(Set.of(task00));
        when(stateUpdater.tasks()).thenReturn(Set.of(task01));

        final Map<TaskId, Set<TopicPartition>> assignmentActive = singletonMap(taskId00, taskId00Partitions);
        final Map<TaskId, Set<TopicPartition>> assignmentStandby = singletonMap(taskId01, taskId01Partitions);

        taskManager.handleAssignment(assignmentActive, assignmentStandby);

        // active task stays in task manager
        verify(tasks, never()).removeTask(task00);
        verify(task00, never()).prepareCommit(anyBoolean());
        verify(task00, never()).postCommit(anyBoolean());

        // standby task not removed from state updater
        verify(stateUpdater, never()).remove(task01.id());
        verify(task01, never()).prepareCommit(anyBoolean());
        verify(task01, never()).postCommit(anyBoolean());

        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldNotCommitOnHandleAssignmentIfOnlyStandbyTaskClosed() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(tasks.allNonFailedTasks()).thenReturn(Set.of(task00));
        when(stateUpdater.tasks()).thenReturn(Set.of(task01));

        // mock to remove standby task from state updater
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(task01.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(task01));

        final Map<TaskId, Set<TopicPartition>> assignmentActive = singletonMap(taskId00, taskId00Partitions);

        taskManager.handleAssignment(assignmentActive, Collections.emptyMap());

        verify(task00, never()).prepareCommit(anyBoolean());
        verify(task00, never()).postCommit(anyBoolean());

        verify(stateUpdater).remove(task01.id());
        verify(task01).suspend();
        verify(task01).closeClean();

        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldNotCommitCreatedTasksOnRevocationOrClosure() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId00Partitions)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(activeTaskCreator.createTasks(consumer, taskId00Assignment))
            .thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        verify(tasks).addPendingTasksToInit(singletonList(task00));

        // when handle revocation is called, the tasks in pendingTasksToInit are NOT affected
        // by revocation. They remain in the pending queue untouched
        taskManager.handleRevocation(taskId00Partitions);

        // tasks in pendingTasksToInit are not managed by handleRevocation
        verify(task00, never()).suspend();
        verify(task00, never()).prepareCommit(anyBoolean());

        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(task00));

        // this calls handleTasksPendingInitialization()
        // which drains pendingTasksToInit and closes those tasks
        taskManager.handleAssignment(emptyMap(), emptyMap());

        // close clean without ever being committed
        verify(task00).closeClean();
        verify(task00, never()).prepareCommit(anyBoolean());
    }

    @Test
    public void shouldPassUpIfExceptionDuringSuspend() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        doThrow(new RuntimeException("KABOOM!")).when(task00).suspend();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertThrows(RuntimeException.class, () -> taskManager.handleRevocation(taskId00Partitions));

        verify(task00).suspend();
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdownWithAlos() {
        shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdown(ProcessingMode.AT_LEAST_ONCE);
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdownWithExactlyOnceV2() {
        when(activeTaskCreator.streamsProducer()).thenReturn(mock(StreamsProducer.class));
        shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdown(ProcessingMode.EXACTLY_ONCE_V2);
    }

    private void shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdown(final ProcessingMode processingMode) {

        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();
        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(processingMode, tasks);

        doThrow(new TaskMigratedException("migrated", new RuntimeException("cause")))
            .when(task01).suspend();
        doThrow(new RuntimeException("oops"))
            .when(task02).suspend();

        when(tasks.activeTasks()).thenReturn(Set.of(task00, task01, task02));

        final RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> taskManager.shutdown(true)
        );
        assertThat(exception.getCause().getMessage(), is("oops"));

        // Verify tasks that threw exceptions were closed dirty
        verify(task00).prepareCommit(true);
        verify(task00).suspend();
        verify(task00).closeClean();
        verify(task01).prepareCommit(true);
        verify(task01, times(2)).suspend();
        verify(task01).closeDirty();
        verify(task02).prepareCommit(true);
        verify(task02, times(2)).suspend();
        verify(task02).closeDirty();

        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator).close();
        verify(stateUpdater).shutdown(Duration.ofMinutes(1L));
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateStreamsProducerExceptionsOnCleanShutdown() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        doThrow(new RuntimeException("whatever")).when(activeTaskCreator).close();

        when(tasks.activeTasks()).thenReturn(Set.of(task00));

        final RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> taskManager.shutdown(true)
        );

        assertThat(exception.getMessage(), is("whatever"));

        verify(task00).prepareCommit(true);
        verify(task00).suspend();
        verify(task00).closeClean();
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator).close();
        verify(stateUpdater).shutdown(Duration.ofMinutes(1L));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCloseTasksIfStateUpdaterTimesOutOnRemove() throws Exception {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, false);

        when(stateUpdater.tasks()).thenReturn(singleton(task00));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = mock(CompletableFuture.class);
        when(stateUpdater.remove(eq(taskId00))).thenReturn(future);
        when(future.get(anyLong(), any())).thenThrow(new java.util.concurrent.TimeoutException());

        taskManager.shutdown(true);

        verify(task00).closeDirty();
    }

    @Test
    public void shouldPropagateSuspendExceptionWhenRevokingStandbyTask() {
        final StandbyTask task00 = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions)
            .build();

        doThrow(new RuntimeException("task 0_1 suspend boom!")).when(task01).suspend();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, tasks);

        when(stateUpdater.tasks()).thenReturn(Set.of(task00, task01));

        // task01 is revoked, task00 stays
        final CompletableFuture<StateUpdater.RemovedTaskResult> futureTask01 = new CompletableFuture<>();
        when(stateUpdater.remove(task01.id())).thenReturn(futureTask01);
        futureTask01.complete(new StateUpdater.RemovedTaskResult(task01));

        final RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> taskManager.handleAssignment(
                Collections.emptyMap(),
                singletonMap(taskId00, taskId00Partitions)
            ));
        assertThat(thrown.getCause().getMessage(), is("task 0_1 suspend boom!"));

        verify(task01, times(2)).suspend();
        verify(task01).closeDirty();
        verify(stateUpdater, never()).remove(task00.id());
        verify(task00, never()).suspend();
        verify(task00, never()).prepareCommit(anyBoolean());
        verify(task00, never()).closeClean();
        verify(task00, never()).closeDirty();
    }

    @Test
    public void shouldSuspendAllRevokedActiveTasksAndPropagateSuspendException() {
        // will not be revoked
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();

        // will be revoked and throws exception during suspend
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions).build();
        doThrow(new RuntimeException("task 0_1 suspend boom!")).when(task01).suspend();

        // will be revoked with no exception
        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(tasks.allTasks()).thenReturn(Set.of(task00, task01, task02));

        final RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> taskManager.handleRevocation(union(HashSet::new, taskId01Partitions, taskId02Partitions)));

        assertThat(thrown.getCause().getMessage(), is("task 0_1 suspend boom!"));

        verify(task01).suspend();
        verify(task02).suspend();
        verify(task00, never()).suspend();
        verifyNoInteractions(activeTaskCreator);
    }

    @Test
    public void shouldCloseActiveTasksAndIgnoreExceptionsOnUncleanShutdown() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions)
            .build();
        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        doThrow(new TaskMigratedException("migrated", new RuntimeException("cause")))
            .when(task01).suspend();
        doThrow(new RuntimeException("oops"))
            .when(task02).suspend();
        doThrow(new RuntimeException("whatever")).when(activeTaskCreator).close();

        when(tasks.allTasks()).thenReturn(Set.of(task00, task01, task02));
        when(tasks.activeTasks()).thenReturn(Set.of(task00, task01, task02));

        taskManager.shutdown(false);

        verify(task00).prepareCommit(false);
        verify(task00).suspend();
        verify(task00).closeDirty();
        verify(task00, never()).closeClean();
        verify(task01).prepareCommit(false);
        verify(task01).suspend();
        verify(task01).closeDirty();
        verify(task01, never()).closeClean();
        verify(task02).prepareCommit(false);
        verify(task02).suspend();
        verify(task02).closeDirty();
        verify(task02, never()).closeClean();
        verify(tasks).clear();

        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator).close();
        verify(stateUpdater).shutdown(Duration.ofMinutes(1L));
    }

    @Test
    public void shouldCloseStandbyTasksOnShutdown() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final StandbyTask standbyTask00 = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTask00)).thenReturn(Set.of());
        when(stateUpdater.standbyTasks()).thenReturn(Set.of(standbyTask00));

        final CompletableFuture<StateUpdater.RemovedTaskResult> futureForStandbyTask = new CompletableFuture<>();
        when(stateUpdater.remove(taskId00)).thenReturn(futureForStandbyTask);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        futureForStandbyTask.complete(new StateUpdater.RemovedTaskResult(standbyTask00)); // simulate successful removal

        taskManager.shutdown(true);

        verify(stateUpdater).shutdown(Duration.ofMinutes(1L));

        verify(tasks).addTask(standbyTask00);

        verify(standbyTask00).prepareCommit(true);
        verify(standbyTask00).postCommit(true);
        verify(standbyTask00).suspend();
        verify(standbyTask00).closeClean();

        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator).close();
        verifyNoInteractions(consumer);
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
            )
            .thenReturn(Collections.emptyList());
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.shutdown(true);

        verify(activeTaskCreator).close();
        verify(stateUpdater).shutdown(Duration.ofMinutes(1L));
        verify(failedStatefulTask).prepareCommit(false);
        verify(failedStatefulTask).suspend();
        verify(failedStatefulTask).closeDirty();
    }

    @Test
    public void shouldShutdownSchedulingTaskManager() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        taskManager.shutdown(true);

        verify(schedulingTaskManager).shutdown(Duration.ofMinutes(5L));
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
                removedFailedStandbyTaskDuringRemoval)
            ).thenReturn(Collections.emptySet());
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
        when(stateUpdater.drainExceptionsAndFailedTasks())
                .thenReturn(Arrays.asList(
                    new ExceptionAndTask(new StreamsException("KABOOM!"), removedFailedStatefulTaskDuringRemoval),
                    new ExceptionAndTask(new StreamsException("KABOOM!"), removedFailedStandbyTaskDuringRemoval))
                ).thenReturn(Collections.emptyList());
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
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

        verify(stateUpdater).shutdown(Duration.ofMinutes(1L));
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
    public void shouldInitializeNewStandbyTasks() {
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId01Partitions)
            .build();

        final Map<TaskId, Set<TopicPartition>> assignment = taskId01Assignment;
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(standbyTaskCreator.createTasks(assignment)).thenReturn(singletonList(task01));

        taskManager.handleAssignment(emptyMap(), assignment);

        verify(tasks).addPendingTasksToInit(singletonList(task01));

        when(tasks.drainPendingTasksToInit()).thenReturn(Set.of(task01));

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verify(task01).initializeIfNeeded();
        verify(stateUpdater).add(task01);
        verifyNoInteractions(consumer);
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
    public void shouldCommitActiveAndStandbyTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));

        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenReturn(offsets);
        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(emptyMap());

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00, task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertThat(taskManager.commitAll(), equalTo(2));

        verify(task00, times(2)).commitNeeded();
        verify(task00).prepareCommit(true);
        verify(task00).postCommit(false);
        verify(task01, times(2)).commitNeeded();
        verify(task01).prepareCommit(true);
        verify(task01).postCommit(false);
        verify(consumer).commitSync(offsets);
    }

    @Test
    public void shouldCommitProvidedTasksIfNeeded() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();
        final Map<TopicPartition, OffsetAndMetadata> offsetsTask00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));

        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();
        final Map<TopicPartition, OffsetAndMetadata> offsetsTask01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));

        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING)
            .build();

        final StandbyTask task03 = standbyTask(taskId03, taskId03ChangelogPartitions)
            .withInputPartitions(taskId03Partitions)
            .inState(State.RUNNING)
            .build();

        final StandbyTask task04 = standbyTask(taskId04, taskId04ChangelogPartitions)
            .withInputPartitions(taskId04Partitions)
            .inState(State.RUNNING)
            .build();

        final StandbyTask task05 = standbyTask(taskId05, taskId05ChangelogPartitions)
            .withInputPartitions(taskId05Partitions)
            .inState(State.RUNNING)
            .build();

        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenReturn(offsetsTask00);
        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsetsTask01);
        when(task02.commitNeeded()).thenReturn(false);
        when(task03.commitNeeded()).thenReturn(true);
        when(task03.prepareCommit(true)).thenReturn(emptyMap());
        when(task04.commitNeeded()).thenReturn(true);
        when(task04.prepareCommit(true)).thenReturn(emptyMap());
        when(task05.commitNeeded()).thenReturn(false);

        final TasksRegistry tasks = mock(TasksRegistry.class);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertThat(taskManager.commit(Set.of(task00, task02, task03, task05)), equalTo(2));

        verify(task00, times(2)).commitNeeded();
        verify(task00).prepareCommit(true);
        verify(task00).postCommit(false);
        verify(task01, never()).prepareCommit(anyBoolean());
        verify(task01, never()).postCommit(anyBoolean());
        verify(task02, atLeastOnce()).commitNeeded();
        verify(task02, never()).prepareCommit(anyBoolean());
        verify(task02, never()).postCommit(anyBoolean());
        verify(task03, times(2)).commitNeeded();
        verify(task03).prepareCommit(true);
        verify(task03).postCommit(false);
        verify(task04, never()).prepareCommit(anyBoolean());
        verify(task04, never()).postCommit(anyBoolean());
        verify(task05, atLeastOnce()).commitNeeded();
        verify(task05, never()).prepareCommit(anyBoolean());
        verify(task05, never()).postCommit(anyBoolean());
        verify(consumer).commitSync(offsetsTask00);
    }

    @Test
    public void shouldNotCommitOffsetsIfOnlyStandbyTasksAssigned() {
        final StandbyTask task00 = standbyTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenReturn(emptyMap());

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertThat(taskManager.commitAll(), equalTo(1));

        verify(task00, times(2)).commitNeeded();
        verify(task00).prepareCommit(true);
        verify(task00).postCommit(false);
        verify(consumer, never()).commitSync(any(Map.class));
    }

    @Test
    public void shouldNotCommitActiveAndStandbyTasksWhileRebalanceInProgress() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        when(task00.commitNeeded()).thenReturn(true);
        when(task01.commitNeeded()).thenReturn(true);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00, task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.handleRebalanceStart(emptySet());

        assertThat(
            taskManager.commitAll(),
            equalTo(-1) // sentinel indicating that nothing was done because a rebalance is in progress
        );

        assertThat(
            taskManager.maybeCommitActiveTasksPerUserRequested(),
            equalTo(-1) // sentinel indicating that nothing was done because a rebalance is in progress
        );
    }

    @Test
    public void shouldCommitViaConsumerIfEosDisabled() {
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));

        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsets);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertThat(taskManager.commitAll(), equalTo(1));

        verify(task01, times(2)).commitNeeded();
        verify(task01).prepareCommit(true);
        verify(task01).postCommit(false);
        verify(consumer).commitSync(offsets);
    }

    @Test
    public void shouldCommitViaProducerIfEosV2Enabled() {
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task01, task02));

        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.streamsProducer()).thenReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT02 = singletonMap(t1p2, new OffsetAndMetadata(1L, null));
        final Map<TopicPartition, OffsetAndMetadata> allOffsets = new HashMap<>();
        allOffsets.putAll(offsetsT01);
        allOffsets.putAll(offsetsT02);

        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsetsT01);
        doNothing().when(task01).postCommit(false);

        when(task02.commitNeeded()).thenReturn(true);
        when(task02.prepareCommit(true)).thenReturn(offsetsT02);
        doNothing().when(task02).postCommit(false);

        final ConsumerGroupMetadata groupMetadata = mock(ConsumerGroupMetadata.class);
        when(consumer.groupMetadata()).thenReturn(groupMetadata);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, tasks);

        taskManager.commitAll();

        verify(producer).commitTransaction(allOffsets, groupMetadata);
        verify(task01, times(2)).commitNeeded();
        verify(task01).prepareCommit(true);
        verify(task01).postCommit(false);
        verify(task02, times(2)).commitNeeded();
        verify(task02).prepareCommit(true);
        verify(task02).postCommit(false);
        verifyNoMoreInteractions(producer);
    }

    @Test
    public void shouldPropagateExceptionFromActiveCommit() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenThrow(new RuntimeException("opsh."));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        final RuntimeException thrown =
            assertThrows(RuntimeException.class, taskManager::commitAll);
        assertThat(thrown.getMessage(), equalTo("opsh."));

        verify(task00).commitNeeded();
        verify(task00).prepareCommit(true);
    }

    @Test
    public void shouldPropagateExceptionFromStandbyCommit() {
        final StandbyTask task01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenThrow(new RuntimeException("opsh."));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        final RuntimeException thrown =
            assertThrows(RuntimeException.class, () -> taskManager.commitAll());
        assertThat(thrown.getMessage(), equalTo("opsh."));

        verify(task01).commitNeeded();
        verify(task01).prepareCommit(true);
    }

    @Test
    public void shouldSendPurgeData() {
        when(adminClient.deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(5L))))
            .thenReturn(new DeleteRecordsResult(singletonMap(t1p1, completedFuture())));
        when(adminClient.deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(17L))))
            .thenReturn(new DeleteRecordsResult(singletonMap(t1p1, completedFuture())));

        final InOrder inOrder = inOrder(adminClient);

        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        when(task00.purgeableOffsets())
            .thenReturn(new HashMap<>())
            .thenReturn(singletonMap(t1p1, 5L))
            .thenReturn(singletonMap(t1p1, 17L));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.maybePurgeCommittedRecords(); // no-op
        taskManager.maybePurgeCommittedRecords(); // sends purge for offset 5L
        taskManager.maybePurgeCommittedRecords(); // sends purge for offset 17L

        inOrder.verify(adminClient).deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(5L)));
        inOrder.verify(adminClient).deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(17L)));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldNotSendPurgeDataIfPreviousNotDone() {
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        when(adminClient.deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(5L))))
            .thenReturn(new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords)));

        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        when(task00.purgeableOffsets())
            .thenReturn(new HashMap<>())
            .thenReturn(singletonMap(t1p1, 5L))
            .thenReturn(singletonMap(t1p1, 17L));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.maybePurgeCommittedRecords();
        taskManager.maybePurgeCommittedRecords();

        // this call should be a no-op.
        // because the previous deleteRecords request
        // has not completed yet, so no new request is sent.
        taskManager.maybePurgeCommittedRecords();
    }

    @Test
    public void shouldIgnorePurgeDataErrors() {
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords));
        futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));
        when(adminClient.deleteRecords(any())).thenReturn(deleteRecordsResult);

        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        when(task00.purgeableOffsets()).thenReturn(singletonMap(t1p1, 5L));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        taskManager.maybePurgeCommittedRecords();
        taskManager.maybePurgeCommittedRecords();
    }

    @Test
    public void shouldMaybeCommitAllActiveTasksThatNeedCommit() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();
        final Map<TopicPartition, OffsetAndMetadata> offsets0 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));

        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();
        final Map<TopicPartition, OffsetAndMetadata> offsets1 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));

        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING)
            .build();

        final StreamTask task03 = statefulTask(taskId03, taskId03ChangelogPartitions)
            .withInputPartitions(taskId03Partitions)
            .inState(State.RUNNING)
            .build();

        // for task00 both commitRequested AND commitNeeded - so it should trigger commit
        when(task00.commitRequested()).thenReturn(true);
        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenReturn(offsets0);

        // for task01 only commitNeeded (no commitRequested) so it gets committed when triggered
        when(task01.commitRequested()).thenReturn(false);
        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsets1);

        // for task02 only commitRequested (no commitNeeded), so does not get committed
        when(task02.commitRequested()).thenReturn(true);
        when(task02.commitNeeded()).thenReturn(false);

        // for task03 both commitRequested AND commitNeeded, so should trigger commit
        when(task03.commitRequested()).thenReturn(true);
        when(task03.commitNeeded()).thenReturn(true);
        when(task03.prepareCommit(true)).thenReturn(emptyMap());

        // expected committed offsets only for task00 and task01 (task03 has empty offsets)
        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(offsets0);
        expectedCommittedOffsets.putAll(offsets1);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00, task01, task02, task03));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        // maybeCommitActiveTasksPerUserRequested checks if any task has both commitRequested AND commitNeeded
        // If found, commits all active running tasks that have commitNeeded
        // Returns count of committed tasks: task00, task01, and task03 (3 tasks)
        assertThat(taskManager.maybeCommitActiveTasksPerUserRequested(), equalTo(3));

        // Verify commit flow for tasks that needed commit
        verify(task00, atLeastOnce()).commitNeeded();
        verify(task00).prepareCommit(true);
        verify(task00).postCommit(false);

        verify(task01, atLeastOnce()).commitNeeded();
        verify(task01).prepareCommit(true);
        verify(task01).postCommit(false);

        verify(task03, atLeastOnce()).commitNeeded();
        verify(task03).prepareCommit(true);
        verify(task03).postCommit(false);

        // task02 should not be committed (no commitNeeded)
        verify(task02, never()).prepareCommit(anyBoolean());

        // Consumer should commit combined offsets from task00 and task01
        verify(consumer).commitSync(expectedCommittedOffsets);
    }

    @Test
    public void shouldProcessActiveTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions)
            .build();

        // simulate processing records from the queue
        when(task00.process(anyLong()))
            .thenReturn(true)   // record 1
            .thenReturn(true)   // record 2
            .thenReturn(true)   // record 3
            .thenReturn(true)   // record 4
            .thenReturn(true)   // record 5
            .thenReturn(true)   // record 6
            .thenReturn(false); // no more records

        when(task01.process(anyLong()))
            .thenReturn(true)   // record 1
            .thenReturn(true)   // record 2
            .thenReturn(true)   // record 3
            .thenReturn(true)   // record 4
            .thenReturn(true)   // record 5
            .thenReturn(false); // no more records

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.activeTasks()).thenReturn(Set.of(task00, task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        // check that we should be processing at most max num records
        assertThat(taskManager.process(3, time), is(6));

        // check that if there's no records processable, we would stop early
        assertThat(taskManager.process(3, time), is(5));
        assertThat(taskManager.process(3, time), is(0));
    }

    @Test
    public void shouldNotFailOnTimeoutException() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();
        // throws TimeoutException on first call, then processes 2 records
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions)
            .build();
        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions)
            .build();

        when(task00.process(anyLong()))
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);

        when(task01.process(anyLong()))
            .thenThrow(new TimeoutException("Skip me!"))  // throws TimeoutException
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);

        when(task02.process(anyLong()))
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.activeTasks()).thenReturn(Set.of(task00, task01, task02));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        // should only process 2 records, because task01 throws TimeoutException
        assertThat(taskManager.process(1, time), is(2));
        verify(task01).maybeInitTaskTimeoutOrThrow(anyLong(), any(TimeoutException.class));

        //  retry without error
        assertThat(taskManager.process(1, time), is(3));
        verify(task01).clearTaskTimeout();

        // there should still be one record for task01 to be processed
        assertThat(taskManager.process(1, time), is(1));
    }

    @Test
    public void shouldPropagateTaskMigratedExceptionsInProcessActiveTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        when(task00.process(anyLong()))
            .thenThrow(new TaskMigratedException("migrated", new RuntimeException("cause")));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.activeTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertThrows(TaskMigratedException.class, () -> taskManager.process(1, time));
    }

    @Test
    public void shouldWrapRuntimeExceptionsInProcessActiveTasksAndSetTaskId() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        when(task00.process(anyLong())).thenThrow(new RuntimeException("oops"));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.activeTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        final StreamsException exception = assertThrows(StreamsException.class, () -> taskManager.process(1, time));
        assertThat(exception.taskId().isPresent(), is(true));
        assertThat(exception.taskId().get(), is(taskId00));
        assertThat(exception.getCause().getMessage(), is("oops"));
    }

    @Test
    public void shouldPropagateTaskMigratedExceptionsInPunctuateActiveTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        when(task00.maybePunctuateStreamTime())
            .thenThrow(new TaskMigratedException("migrated", new RuntimeException("cause")));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.activeTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertThrows(TaskMigratedException.class, taskManager::punctuate);
    }

    @Test
    public void shouldPropagateKafkaExceptionsInPunctuateActiveTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        when(task00.maybePunctuateStreamTime()).thenThrow(new KafkaException("oops"));

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.activeTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        assertThrows(KafkaException.class, taskManager::punctuate);
    }

    @Test
    public void shouldPunctuateActiveTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        when(task00.maybePunctuateStreamTime()).thenReturn(true);
        when(task00.maybePunctuateSystemTime()).thenReturn(true);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.activeTasks()).thenReturn(Set.of(task00));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        // one for stream and one for system time
        assertThat(taskManager.punctuate(), equalTo(2));

        verify(task00).maybePunctuateStreamTime();
        verify(task00).maybePunctuateSystemTime();
    }

    @Test
    public void shouldReturnFalseWhenThereAreStillNonRunningTasks() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        // mock that the state updater is still restoring active tasks
        when(stateUpdater.restoresActiveTasks()).thenReturn(true);

        assertThat(taskManager.checkStateUpdater(time.milliseconds(), noOpResetter), is(false));

        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldHaveRemainingPartitionsUncleared() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();

        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        when(task00.prepareCommit(false)).thenReturn(offsets);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);
        when(tasks.allTasks()).thenReturn(Set.of(task00));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            appender.setClassLogger(TaskManager.class, Level.DEBUG);

            taskManager.handleRevocation(Set.of(t1p0, new TopicPartition("unknown", 0)));

            verify(task00).suspend();

            final List<String> messages = appender.getMessages();
            assertThat(
                messages,
                hasItem("taskManagerTestThe following revoked partitions [unknown-0] are missing " +
                    "from the current task partitions. It could potentially be due to race " +
                    "condition of consumer detecting the heartbeat failure, or the " +
                    "tasks have been cleaned up by the handleAssignment callback.")
            );
        }
    }

    @Test
    public void shouldThrowTaskMigratedWhenAllTaskCloseExceptionsAreTaskMigrated() {
        final StandbyTask migratedTask01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions)
            .build();
        final StandbyTask migratedTask02 = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions)
            .build();

        doThrow(new TaskMigratedException("t1 close exception", new RuntimeException()))
            .when(migratedTask01).suspend();
        doThrow(new TaskMigratedException("t2 close exception", new RuntimeException()))
            .when(migratedTask02).suspend();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(stateUpdater.tasks()).thenReturn(Set.of(migratedTask01, migratedTask02));

        // mock futures for removing tasks from StateUpdater
        final CompletableFuture<StateUpdater.RemovedTaskResult> future01 = new CompletableFuture<>();
        when(stateUpdater.remove(taskId01)).thenReturn(future01);
        future01.complete(new StateUpdater.RemovedTaskResult(migratedTask01));

        final CompletableFuture<StateUpdater.RemovedTaskResult> future02 = new CompletableFuture<>();
        when(stateUpdater.remove(taskId02)).thenReturn(future02);
        future02.complete(new StateUpdater.RemovedTaskResult(migratedTask02));

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );
        // The task map orders tasks based on topic group id and partition, so here
        // t1 should always be the first.
        assertThat(
            thrown.getMessage(),
            equalTo("t2 close exception; it means all tasks belonging to this thread should be migrated.")
        );
        verify(migratedTask01, times(2)).suspend();
        verify(migratedTask02, times(2)).suspend();
        verify(stateUpdater).remove(taskId01);
        verify(stateUpdater).remove(taskId02);
    }

    @Test
    public void shouldThrowRuntimeExceptionWhenEncounteredUnknownExceptionDuringTaskClose() {
        final StandbyTask migratedTask01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions)
            .build();
        final StandbyTask migratedTask02 = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions)
            .build();

        doThrow(new TaskMigratedException("t1 close exception", new RuntimeException()))
            .when(migratedTask01).suspend();
        doThrow(new IllegalStateException("t2 illegal state exception", new RuntimeException()))
            .when(migratedTask02).suspend();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(stateUpdater.tasks()).thenReturn(Set.of(migratedTask01, migratedTask02));

        // mock futures for removing tasks from StateUpdater
        final CompletableFuture<StateUpdater.RemovedTaskResult> future01 = new CompletableFuture<>();
        when(stateUpdater.remove(taskId01)).thenReturn(future01);
        future01.complete(new StateUpdater.RemovedTaskResult(migratedTask01));

        final CompletableFuture<StateUpdater.RemovedTaskResult> future02 = new CompletableFuture<>();
        when(stateUpdater.remove(taskId02)).thenReturn(future02);
        future02.complete(new StateUpdater.RemovedTaskResult(migratedTask02));

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );
        // Fatal exception thrown first.
        assertThat(thrown.getMessage(), equalTo("Encounter unexpected fatal error for task 0_2"));

        assertThat(thrown.getCause().getMessage(), equalTo("t2 illegal state exception"));

        verify(migratedTask01, times(2)).suspend();
        verify(migratedTask02, times(2)).suspend();
        verify(stateUpdater).remove(taskId01);
        verify(stateUpdater).remove(taskId02);
    }

    @Test
    public void shouldThrowSameKafkaExceptionWhenEncounteredDuringTaskClose() {
        final StandbyTask migratedTask01 = standbyTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions)
            .build();
        final StandbyTask migratedTask02 = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions)
            .build();

        doThrow(new TaskMigratedException("t1 close exception", new RuntimeException()))
            .when(migratedTask01).suspend();
        doThrow(new KafkaException("Kaboom for t2!", new RuntimeException()))
            .when(migratedTask02).suspend();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(stateUpdater.tasks()).thenReturn(Set.of(migratedTask01, migratedTask02));

        // mock futures for removing tasks from StateUpdater
        final CompletableFuture<StateUpdater.RemovedTaskResult> future01 = new CompletableFuture<>();
        when(stateUpdater.remove(taskId01)).thenReturn(future01);
        future01.complete(new StateUpdater.RemovedTaskResult(migratedTask01));

        final CompletableFuture<StateUpdater.RemovedTaskResult> future02 = new CompletableFuture<>();
        when(stateUpdater.remove(taskId02)).thenReturn(future02);
        future02.complete(new StateUpdater.RemovedTaskResult(migratedTask02));

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );

        assertThat(thrown.taskId().isPresent(), is(true));
        assertThat(thrown.taskId().get(), is(taskId02));

        // Expecting the original Kafka exception wrapped in the StreamsException.
        assertThat(thrown.getCause().getMessage(), equalTo("Kaboom for t2!"));

        verify(migratedTask01, times(2)).suspend();
        verify(migratedTask02, times(2)).suspend();
        verify(stateUpdater).remove(taskId01);
        verify(stateUpdater).remove(taskId02);
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
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));

        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsets);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        doThrow(new CommitFailedException()).when(consumer).commitSync(offsets);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            taskManager::commitAll
        );

        assertThat(thrown.getCause(), instanceOf(CommitFailedException.class));
        assertThat(
            thrown.getMessage(),
            equalTo("Consumer committing offsets failed, indicating the corresponding thread is no longer part of the group;" +
                " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotFailForTimeoutExceptionOnConsumerCommit() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final Map<TopicPartition, OffsetAndMetadata> offsets = taskId00Partitions.stream()
            .collect(Collectors.toMap(p -> p, p -> new OffsetAndMetadata(0)));

        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenReturn(offsets);
        when(task01.commitNeeded()).thenReturn(false);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        doThrow(new TimeoutException("KABOOM!")).doNothing().when(consumer).commitSync(any(Map.class));

        assertThat(taskManager.commit(Set.of(task00, task01)), equalTo(0));
        verify(task00).maybeInitTaskTimeoutOrThrow(anyLong(), any(TimeoutException.class));

        assertThat(taskManager.commit(Set.of(task00, task01)), equalTo(1));
        verify(task00).clearTaskTimeout();

        verify(consumer, times(2)).commitSync(any(Map.class));
    }

    @Test
    public void shouldThrowTaskCorruptedExceptionForTimeoutExceptionOnCommitWithEosV2() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();
        final StreamTask task02 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING)
            .build();

        final Map<TopicPartition, OffsetAndMetadata> offsetsT00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        final Map<TopicPartition, OffsetAndMetadata> allOffsets = new HashMap<>(offsetsT00);
        allOffsets.putAll(offsetsT01);

        when(task00.commitNeeded()).thenReturn(true);
        when(task00.prepareCommit(true)).thenReturn(offsetsT00);
        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsetsT01);
        when(task02.commitNeeded()).thenReturn(false);

        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.streamsProducer()).thenReturn(producer);
        final ConsumerGroupMetadata groupMetadata = mock(ConsumerGroupMetadata.class);
        when(consumer.groupMetadata()).thenReturn(groupMetadata);

        doThrow(new TimeoutException("KABOOM!")).when(producer).commitTransaction(allOffsets, groupMetadata);

        final TasksRegistry tasks = mock(TasksRegistry.class);

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, tasks);

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
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));

        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsets);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        doThrow(new KafkaException()).when(consumer).commitSync(offsets);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            taskManager::commitAll
        );

        assertThat(thrown.getCause(), instanceOf(KafkaException.class));
        assertThat(thrown.getMessage(), equalTo("Error encountered committing offsets via consumer"));

        verify(task01).commitNeeded();
        verify(task01).prepareCommit(true);
    }

    @Test
    public void shouldFailOnCommitFatal() {
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));

        when(task01.commitNeeded()).thenReturn(true);
        when(task01.prepareCommit(true)).thenReturn(offsets);

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        doThrow(new RuntimeException("KABOOM")).when(consumer).commitSync(offsets);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            taskManager::commitAll
        );

        assertThat(thrown.getMessage(), equalTo("KABOOM"));

        verify(task01).commitNeeded();
        verify(task01).prepareCommit(true);
    }

    @Test
    public void shouldSuspendAllTasksButSkipCommitIfSuspendingFailsDuringRevocation() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions)
            .build();
        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId01Partitions)
            .build();

        doThrow(new RuntimeException("KABOOM!")).when(task00).suspend();

        final TasksRegistry tasks = mock(TasksRegistry.class);

        when(tasks.allTasks()).thenReturn(Set.of(task00, task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.handleRevocation(union(HashSet::new, taskId00Partitions, taskId01Partitions)));

        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));

        // verify both tasks had suspend called
        verify(task00).suspend();
        verify(task01).suspend();

        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldConvertActiveTaskToStandbyTask() {
        final StreamTask activeTaskToRecycle = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StandbyTask recycledStandbyTask = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(activeTaskCreator.createTasks(consumer, taskId00Assignment)).thenReturn(singletonList(activeTaskToRecycle));
        when(standbyTaskCreator.createStandbyTaskFromActive(activeTaskToRecycle, taskId00Partitions))
            .thenReturn(recycledStandbyTask);

        // create active task
        taskManager.handleAssignment(taskId00Assignment, Collections.emptyMap());

        // convert active to standby
        when(stateUpdater.tasks()).thenReturn(Set.of(activeTaskToRecycle));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(activeTaskToRecycle.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(activeTaskToRecycle));

        taskManager.handleAssignment(Collections.emptyMap(), taskId00Assignment);

        verify(activeTaskCreator).createTasks(consumer, emptyMap());
        verify(standbyTaskCreator, times(2)).createTasks(Collections.emptyMap());
        verify(standbyTaskCreator).createStandbyTaskFromActive(activeTaskToRecycle, taskId00Partitions);
        verify(tasks).addPendingTasksToInit(Collections.singleton(recycledStandbyTask));
    }

    @Test
    public void shouldConvertStandbyTaskToActiveTask() {
        final StandbyTask standbyTaskToRecycle = standbyTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask recycledActiveTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(standbyTaskCreator.createTasks(taskId00Assignment)).thenReturn(singletonList(standbyTaskToRecycle));
        when(activeTaskCreator.createActiveTaskFromStandby(standbyTaskToRecycle, taskId00Partitions, consumer))
            .thenReturn(recycledActiveTask);

        // create standby task
        taskManager.handleAssignment(Collections.emptyMap(), taskId00Assignment);

        // convert standby to active
        when(stateUpdater.tasks()).thenReturn(Set.of(standbyTaskToRecycle));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(standbyTaskToRecycle.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(standbyTaskToRecycle));

        taskManager.handleAssignment(taskId00Assignment, Collections.emptyMap());

        verify(activeTaskCreator, times(2)).createTasks(consumer, emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
        verify(activeTaskCreator).createActiveTaskFromStandby(standbyTaskToRecycle, taskId00Partitions, consumer);
        verify(tasks).addPendingTasksToInit(Collections.singleton(recycledActiveTask));
    }

    @Test
    public void shouldListNotPausedTasks() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RUNNING)
            .build();

        final StreamTask task01 = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING)
            .build();

        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(Set.of(task00, task01));

        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks);

        when(stateUpdater.tasks()).thenReturn(Collections.emptySet());

        assertEquals(2, taskManager.notPausedTasks().size());
        assertTrue(taskManager.notPausedTasks().containsKey(taskId00));
        assertTrue(taskManager.notPausedTasks().containsKey(taskId01));

        topologyMetadata.pauseTopology(UNNAMED_TOPOLOGY);

        assertEquals(0, taskManager.notPausedTasks().size());
    }

    @Test
    public void shouldRecycleStartupTasksFromStateDirectoryAsActive() {
        final Tasks taskRegistry = new Tasks(new LogContext());
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, taskRegistry);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, taskRegistry);
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, null);
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
}
