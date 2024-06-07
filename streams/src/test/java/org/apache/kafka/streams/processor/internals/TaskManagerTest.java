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
import org.apache.kafka.streams.processor.internals.StateDirectory.TaskDirectory;
import org.apache.kafka.streams.processor.internals.StateUpdater.ExceptionAndTask;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.apache.kafka.streams.processor.internals.tasks.DefaultTaskManager;
import org.apache.kafka.streams.processor.internals.testutil.DummyStreamsConfig;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.union;
import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.standbyTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statefulTask;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TaskManagerTest {

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";

    private final TaskId taskId00 = new TaskId(0, 0);
    private final TopicPartition t1p0 = new TopicPartition(topic1, 0);
    private final TopicPartition t1p0changelog = new TopicPartition("changelog", 0);
    private final Set<TopicPartition> taskId00Partitions = mkSet(t1p0);
    private final Set<TopicPartition> taskId00ChangelogPartitions = mkSet(t1p0changelog);
    private final Map<TaskId, Set<TopicPartition>> taskId00Assignment = singletonMap(taskId00, taskId00Partitions);

    private final TaskId taskId01 = new TaskId(0, 1);
    private final TopicPartition t1p1 = new TopicPartition(topic1, 1);
    private final TopicPartition t2p2 = new TopicPartition(topic2, 1);
    private final TopicPartition t1p1changelog = new TopicPartition("changelog", 1);
    private final Set<TopicPartition> taskId01Partitions = mkSet(t1p1);
    private final Set<TopicPartition> taskId01ChangelogPartitions = mkSet(t1p1changelog);
    private final Map<TaskId, Set<TopicPartition>> taskId01Assignment = singletonMap(taskId01, taskId01Partitions);

    private final TaskId taskId02 = new TaskId(0, 2);
    private final TopicPartition t1p2 = new TopicPartition(topic1, 2);
    private final TopicPartition t1p2changelog = new TopicPartition("changelog", 2);
    private final Set<TopicPartition> taskId02Partitions = mkSet(t1p2);
    private final Set<TopicPartition> taskId02ChangelogPartitions = mkSet(t1p2changelog);

    private final TaskId taskId03 = new TaskId(0, 3);
    private final TopicPartition t1p3 = new TopicPartition(topic1, 3);
    private final TopicPartition t1p3changelog = new TopicPartition("changelog", 3);
    private final Set<TopicPartition> taskId03Partitions = mkSet(t1p3);
    private final Set<TopicPartition> taskId03ChangelogPartitions = mkSet(t1p3changelog);

    private final TaskId taskId04 = new TaskId(0, 4);
    private final TopicPartition t1p4 = new TopicPartition(topic1, 4);
    private final TopicPartition t1p4changelog = new TopicPartition("changelog", 4);
    private final Set<TopicPartition> taskId04Partitions = mkSet(t1p4);
    private final Set<TopicPartition> taskId04ChangelogPartitions = mkSet(t1p4changelog);

    private final TaskId taskId05 = new TaskId(0, 5);
    private final TopicPartition t1p5 = new TopicPartition(topic1, 5);
    private final TopicPartition t1p5changelog = new TopicPartition("changelog", 5);
    private final Set<TopicPartition> taskId05Partitions = mkSet(t1p5);
    private final Set<TopicPartition> taskId05ChangelogPartitions = mkSet(t1p5changelog);

    private final TaskId taskId10 = new TaskId(1, 0);
    private final TopicPartition t2p0 = new TopicPartition(topic2, 0);
    private final Set<TopicPartition> taskId10Partitions = mkSet(t2p0);
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
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ProcessorStateManager.StateStoreMetadata stateStore;
    final StateUpdater stateUpdater = mock(StateUpdater.class);
    final DefaultTaskManager schedulingTaskManager = mock(DefaultTaskManager.class);

    private TaskManager taskManager;
    private TopologyMetadata topologyMetadata;
    private final Time time = new MockTime();

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Rule
    public final MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

    @Before
    public void setUp() {
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, null, false);
    }

    private TaskManager setUpTaskManager(final ProcessingMode processingMode, final boolean stateUpdaterEnabled) {
        return setUpTaskManager(processingMode, null, stateUpdaterEnabled, false);
    }

    private TaskManager setUpTaskManager(final ProcessingMode processingMode, final TasksRegistry tasks, final boolean stateUpdaterEnabled) {
        return setUpTaskManager(processingMode, tasks, stateUpdaterEnabled, false);
    }

    private TaskManager setUpTaskManager(final ProcessingMode processingMode,
                                         final TasksRegistry tasks,
                                         final boolean stateUpdaterEnabled,
                                         final boolean processingThreadsEnabled) {
        topologyMetadata = new TopologyMetadata(topologyBuilder, new DummyStreamsConfig(processingMode));
        final TaskManager taskManager = new TaskManager(
            time,
            changeLogReader,
            UUID.randomUUID(),
            "taskManagerTest",
            activeTaskCreator,
            standbyTaskCreator,
            tasks != null ? tasks : new Tasks(new LogContext()),
            topologyMetadata,
            adminClient,
            stateDirectory,
            stateUpdaterEnabled ? stateUpdater : null,
            processingThreadsEnabled ? schedulingTaskManager : null
        );
        taskManager.setMainConsumer(consumer);
        return taskManager;
    }

    @Test
    public void shouldClassifyExistingTasksWithoutStateUpdater() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, false);
        final Map<TaskId, Set<TopicPartition>> runningActiveTasks = mkMap(mkEntry(taskId01, mkSet(t1p1)));
        final Map<TaskId, Set<TopicPartition>> standbyTasks = mkMap(mkEntry(taskId02, mkSet(t2p2)));
        final Map<TaskId, Set<TopicPartition>> restoringActiveTasks = mkMap(mkEntry(taskId03, mkSet(t1p3)));
        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>(runningActiveTasks);
        activeTasks.putAll(restoringActiveTasks);
        handleAssignment(runningActiveTasks, standbyTasks, restoringActiveTasks);

        taskManager.handleAssignment(activeTasks, standbyTasks);

        verifyNoInteractions(stateUpdater);
    }

    @Test
    public void shouldNotUpdateExistingStandbyTaskIfStandbyIsReassignedWithSameInputPartitionWithoutStateUpdater() {
        final StandbyTask standbyTask = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        updateExistingStandbyTaskIfStandbyIsReassignedWithoutStateUpdater(standbyTask, taskId03Partitions);
        verify(standbyTask, never()).updateInputPartitions(eq(taskId03Partitions), any());
    }

    @Test
    public void shouldUpdateExistingStandbyTaskIfStandbyIsReassignedWithDifferentInputPartitionWithoutStateUpdater() {
        final StandbyTask standbyTask = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        updateExistingStandbyTaskIfStandbyIsReassignedWithoutStateUpdater(standbyTask, taskId04Partitions);
        verify(standbyTask).updateInputPartitions(eq(taskId04Partitions), any());
    }

    private void updateExistingStandbyTaskIfStandbyIsReassignedWithoutStateUpdater(final Task standbyTask,
                                                                                   final Set<TopicPartition> newInputPartition) {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(mkSet(standbyTask));
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, false);

        taskManager.handleAssignment(
            Collections.emptyMap(),
            mkMap(mkEntry(standbyTask.id(), newInputPartition))
        );

        verify(standbyTask).resume();
    }

    @Test
    public void shouldLockAllTasksOnCorruptionWithProcessingThreads() {
        final StreamTask activeTask1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true, true);
        when(tasks.activeTaskIds()).thenReturn(mkSet(taskId00, taskId01));
        when(tasks.task(taskId00)).thenReturn(activeTask1);
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.handleCorruption(mkSet(taskId00));

        verify(consumer).assignment();
        verify(schedulingTaskManager).lockTasks(mkSet(taskId00, taskId01));
        verify(schedulingTaskManager).unlockTasks(mkSet(taskId00, taskId01));
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true, true);
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.commit(mkSet(activeTask1, activeTask2));

        verify(schedulingTaskManager).lockTasks(mkSet(taskId00, taskId01));
        verify(schedulingTaskManager).unlockTasks(mkSet(taskId00, taskId01));
    }

    @Test
    public void shouldLockActiveOnHandleAssignmentWithProcessingThreads() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true, true);
        when(tasks.allTaskIds()).thenReturn(mkSet(taskId00, taskId01));
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.handleAssignment(
            mkMap(mkEntry(taskId00, taskId00Partitions)),
                mkMap(mkEntry(taskId01, taskId01Partitions))
        );

        verify(schedulingTaskManager).lockTasks(mkSet(taskId00, taskId01));
        verify(schedulingTaskManager).unlockTasks(mkSet(taskId00, taskId01));
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true, true);
        when(tasks.allTasks()).thenReturn(mkSet(activeTask1, activeTask2));
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.handleRevocation(taskId01Partitions);

        verify(schedulingTaskManager).lockTasks(mkSet(taskId00, taskId01));
        verify(schedulingTaskManager).unlockTasks(mkSet(taskId00, taskId01));
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
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true, true);
        when(tasks.allTasks()).thenReturn(mkSet(activeTask1, activeTask2));
        final KafkaFuture<Void> mockFuture = KafkaFuture.completedFuture(null);
        when(schedulingTaskManager.lockTasks(any())).thenReturn(mockFuture);

        taskManager.closeAndCleanUpTasks(mkSet(activeTask1), mkSet(), false);

        verify(schedulingTaskManager).lockTasks(mkSet(taskId00));
        verify(schedulingTaskManager).unlockTasks(mkSet(taskId00));
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
        when(tasks.activeTasks()).thenReturn(mkSet(activeTask1, activeTask2));

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
        when(tasks.activeTasks()).thenReturn(mkSet(activeTask1, activeTask2));

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
        when(stateUpdater.getTasks()).thenReturn(mkSet(activeTaskToClose));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(activeTaskToClose.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(activeTaskToClose));

        taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap());

        verify(activeTaskToClose).suspend();
        verify(activeTaskToClose).closeClean();
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(activeTaskToClose.id());
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(activeTaskToClose));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(activeTaskToClose.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(activeTaskToClose, new RuntimeException("KABOOM!")));

        taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap());

        verify(activeTaskToClose).prepareCommit();
        verify(activeTaskToClose).suspend();
        verify(activeTaskToClose).closeDirty();
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(activeTaskToClose.id());
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(standbyTaskToClose));
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(standbyTaskToClose));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();
        when(stateUpdater.remove(standbyTaskToClose.id())).thenReturn(future);
        future.complete(new StateUpdater.RemovedTaskResult(standbyTaskToClose, new RuntimeException("KABOOM!")));

        taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap());

        verify(standbyTaskToClose).prepareCommit();
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(failedStandbyTask));
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
        verify(tasks).addTask(failedStandbyTask);
    }

    @Test
    public void shouldUpdateInputPartitionOfActiveTaskInStateUpdater() {
        final StreamTask activeTaskToUpdateInputPartitions = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId03Partitions).build();
        final Set<TopicPartition> newInputPartitions = taskId02Partitions;
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.getTasks()).thenReturn(mkSet(activeTaskToUpdateInputPartitions));
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(activeTaskToRecycle));
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(activeTaskToRecycle));
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(standbyTaskToRecycle));
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(standbyTaskToRecycle));
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(reassignedActiveTask));

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
        when(tasks.allTasks()).thenReturn(mkSet(reassignedActiveTask));

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
    public void shouldNeverUpdateInputPartitionsOfStandbyTaskInStateUpdater() {
        final StandbyTask standbyTaskToUpdateInputPartitions = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.getTasks()).thenReturn(mkSet(standbyTaskToUpdateInputPartitions));

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
        when(stateUpdater.getTasks()).thenReturn(mkSet(reassignedStandbyTask));

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
        when(stateUpdater.getTasks()).thenReturn(mkSet(activeTaskToClose, standbyTaskToRecycle));
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
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(activeTaskToClose.id());
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

        when(stateUpdater.getTasks()).thenReturn(mkSet(standbyTaskInStateUpdater));
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId03, runningActiveTask)));
        when(tasks.pendingTasksToInit()).thenReturn(mkSet(activeTaskToInit));
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
        final StandbyTask standbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
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
        final Set<Task> createdTasks = mkSet(activeTaskToBeCreated);
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
        final Set<Task> createdTasks = mkSet(standbyTaskToBeCreated);
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
    public void shouldAddRecycledStandbyTaskfromActiveToPendingTasksToInitWithStateUpdaterEnabled() {
        final StreamTask activeTaskToRecycle = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING).build();
        final StandbyTask standbyTask = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.CREATED).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(mkSet(activeTaskToRecycle));
        when(standbyTaskCreator.createStandbyTaskFromActive(activeTaskToRecycle, taskId01Partitions))
            .thenReturn(standbyTask);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);

        taskManager.handleAssignment(emptyMap(), mkMap(mkEntry(taskId01, taskId01Partitions)));

        verify(activeTaskToRecycle).prepareCommit();
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(activeTaskToRecycle.id());
        verify(tasks).addPendingTasksToInit(mkSet(standbyTask));
        verify(tasks).removeTask(activeTaskToRecycle);
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldAddRecycledStandbyTaskfromActiveToTaskRegistryWithStateUpdaterDisabled() {
        final StreamTask activeTaskToRecycle = statefulTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING).build();
        final StandbyTask standbyTask = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.CREATED).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(mkSet(activeTaskToRecycle));
        when(standbyTaskCreator.createStandbyTaskFromActive(activeTaskToRecycle, taskId01Partitions))
            .thenReturn(standbyTask);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, false);

        taskManager.handleAssignment(emptyMap(), mkMap(mkEntry(taskId01, taskId01Partitions)));

        verify(activeTaskToRecycle).prepareCommit();
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(activeTaskToRecycle.id());
        verify(tasks).replaceActiveWithStandby(standbyTask);
        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldThrowDuringAssignmentIfStandbyTaskToRecycleIsFoundInTasksRegistryWithStateUpdaterEnabled() {
        final StandbyTask standbyTaskToRecycle = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasks()).thenReturn(mkSet(standbyTaskToRecycle));
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
    public void shouldAssignActiveTaskInTasksRegistryToBeClosedCleanlyWithStateUpdaterEnabled() {
        final StreamTask activeTaskToClose = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasks()).thenReturn(mkSet(activeTaskToClose));

        taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap());

        verify(activeTaskCreator).createTasks(consumer, Collections.emptyMap());
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(activeTaskToClose.id());
        verify(activeTaskToClose).prepareCommit();
        verify(activeTaskToClose).closeClean();
        verify(tasks).removeTask(activeTaskToClose);
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
    }

    @Test
    public void shouldThrowDuringAssignmentIfStandbyTaskToCloseIsFoundInTasksRegistryWithStateUpdaterEnabled() {
        final StandbyTask standbyTaskToClose = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasks()).thenReturn(mkSet(standbyTaskToClose));

        final IllegalStateException illegalStateException = assertThrows(
            IllegalStateException.class,
            () -> taskManager.handleAssignment(Collections.emptyMap(), Collections.emptyMap())
        );

        assertEquals(illegalStateException.getMessage(), "Standby tasks should only be managed by the state updater, " +
            "but standby task " + taskId03 + " is managed by the stream thread");
        verifyNoInteractions(activeTaskCreator);
    }

    @Test
    public void shouldAssignActiveTaskInTasksRegistryToUpdateInputPartitionsWithStateUpdaterEnabled() {
        final StreamTask activeTaskToUpdateInputPartitions = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final Set<TopicPartition> newInputPartitions = taskId02Partitions;
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasks()).thenReturn(mkSet(activeTaskToUpdateInputPartitions));
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
    public void shouldResumeActiveRunningTaskInTasksRegistryWithStateUpdaterEnabled() {
        final StreamTask activeTaskToResume = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasks()).thenReturn(mkSet(activeTaskToResume));

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
        when(tasks.allTasks()).thenReturn(mkSet(activeTaskToResume));

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
    public void shouldThrowDuringAssignmentIfStandbyTaskToUpdateInputPartitionsIsFoundInTasksRegistryWithStateUpdaterEnabled() {
        final StandbyTask standbyTaskToUpdateInputPartitions = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final Set<TopicPartition> newInputPartitions = taskId03Partitions;
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasks()).thenReturn(mkSet(standbyTaskToUpdateInputPartitions));

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
    public void shouldAssignMultipleTasksInTasksRegistryWithStateUpdaterEnabled() {
        final StreamTask activeTaskToClose = statefulTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId03Partitions).build();
        final StreamTask activeTaskToCreate = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasks()).thenReturn(mkSet(activeTaskToClose));

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
        when(tasks.drainPendingTasksToInit()).thenReturn(mkSet(task00, task01));
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
        when(tasks.drainPendingTasksToInit()).thenReturn(mkSet(task00, task01));
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
    public void shouldRethrowRuntimeExceptionInInitTaskWithStateUpdater() {
        final StreamTask task00 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.CREATED).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.drainPendingTasksToInit()).thenReturn(mkSet(task00));
        final RuntimeException runtimeException = new RuntimeException("KABOOM!");
        doThrow(runtimeException).when(task00).initializeIfNeeded();
        taskManager = setUpTaskManager(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE, tasks, true);

        final StreamsException streamsException = assertThrows(
            StreamsException.class,
            () -> taskManager.checkStateUpdater(time.milliseconds(), noOpResetter)
        );
        verify(stateUpdater, never()).add(task00);
        verify(tasks).addTask(task00);
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
        when(tasks.drainPendingTasksToInit()).thenReturn(mkSet(statefulTask0, statefulTask1, statefulTask2));
        doThrow(new TaskCorruptedException(Collections.singleton(statefulTask0.id))).when(statefulTask0).initializeIfNeeded();
        doThrow(new TaskCorruptedException(Collections.singleton(statefulTask1.id))).when(statefulTask1).initializeIfNeeded();

        final TaskCorruptedException thrown = assertThrows(
            TaskCorruptedException.class,
            () -> taskManager.checkStateUpdater(time.milliseconds(), noOpResetter)
        );

        verify(tasks).addTask(statefulTask0);
        verify(tasks).addTask(statefulTask1);
        verify(stateUpdater).add(statefulTask2);
        assertEquals(mkSet(taskId00, taskId01), thrown.corruptedTasks());
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
        final TaskManager taskManager = setupForRevocationAndLost(mkSet(task), tasks);
        when(stateUpdater.getTasks()).thenReturn(mkSet(task));
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
        final TaskManager taskManager = setupForRevocationAndLost(mkSet(task1, task2), tasks);
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
        final TaskManager taskManager = setupForRevocationAndLost(mkSet(task), tasks);

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
        final TaskManager taskManager = setupForRevocationAndLost(mkSet(task), tasks);

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
        final TaskManager taskManager = setupForRevocationAndLost(mkSet(task1, task2), tasks);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future1 = new CompletableFuture<>();
        when(stateUpdater.remove(task1.id())).thenReturn(future1);
        future1.complete(new StateUpdater.RemovedTaskResult(task1));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future2 = new CompletableFuture<>();
        when(stateUpdater.remove(task2.id())).thenReturn(future2);
        final StreamsException streamsException = new StreamsException("Something happened");
        future2.complete(new StateUpdater.RemovedTaskResult(task2, streamsException));

        final StreamsException thrownException = assertThrows(
            StreamsException.class,
            () -> taskManager.handleRevocation(union(HashSet::new, taskId00Partitions, taskId01Partitions))
        );

        assertEquals(thrownException, streamsException);
        verify(task1).suspend();
        verify(tasks).addTask(task1);
        verify(task2, never()).suspend();
        verify(tasks).addTask(task2);
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
        final TaskManager taskManager = setupForRevocationAndLost(mkSet(task1, task2, task3), tasks);
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
    public void shouldCloseDirtyWhenRemoveFailedActiveTasksFromStateUpdaterOnPartitionLost() {
        final StreamTask task1 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask task2 = statefulTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId02Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setupForRevocationAndLost(mkSet(task1, task2), tasks);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future1 = new CompletableFuture<>();
        when(stateUpdater.remove(task1.id())).thenReturn(future1);
        future1.complete(new StateUpdater.RemovedTaskResult(task1, new StreamsException("Something happened")));
        final CompletableFuture<StateUpdater.RemovedTaskResult> future3 = new CompletableFuture<>();
        when(stateUpdater.remove(task2.id())).thenReturn(future3);
        future3.complete(new StateUpdater.RemovedTaskResult(task2));

        taskManager.handleLostAll();

        verify(task1).prepareCommit();
        verify(task1).suspend();
        verify(task1).closeDirty();
        verify(task2).suspend();
        verify(task2).closeClean();
    }

    private TaskManager setupForRevocationAndLost(final Set<Task> tasksInStateUpdater,
                                                  final TasksRegistry tasks) {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(stateUpdater.getTasks()).thenReturn(tasksInStateUpdater);

        return taskManager;
    }

    @Test
    public void shouldTransitRestoredTaskToRunning() {
        final StreamTask task = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTransitionToRunningOfRestoredTask(mkSet(task), tasks);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verifyTransitionToRunningOfRestoredTask(mkSet(task), tasks);
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
        final TaskManager taskManager = setUpTransitionToRunningOfRestoredTask(mkSet(task1, task2), tasks);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verifyTransitionToRunningOfRestoredTask(mkSet(task1, task2), tasks);
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
        final TaskManager taskManager = setUpTransitionToRunningOfRestoredTask(mkSet(task), tasks);
        final TimeoutException timeoutException = new TimeoutException();
        doThrow(timeoutException).when(task).completeRestoration(noOpResetter);

        taskManager.checkStateUpdater(time.milliseconds(), noOpResetter);

        verify(task).maybeInitTaskTimeoutOrThrow(anyLong(), eq(timeoutException));
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
    public void shouldReturnCorrectBooleanWhenTryingToCompleteRestorationWithStateUpdater() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, true);
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
        assertEquals(statefulTask.id(), thrown.taskId().get());
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

        assertEquals(mkSet(taskId00, taskId01), thrown.corruptedTasks());
        assertEquals("Tasks [0_1, 0_0] are corrupted and hence need to be re-initialized", thrown.getMessage());
    }
    public void shouldAddSubscribedTopicsFromAssignmentToTopologyMetadata() {
        final Map<TaskId, Set<TopicPartition>> activeTasksAssignment = mkMap(
            mkEntry(taskId01, mkSet(t1p1)),
            mkEntry(taskId02, mkSet(t1p2, t2p2))
        );
        final Map<TaskId, Set<TopicPartition>> standbyTasksAssignment = mkMap(
            mkEntry(taskId03, mkSet(t1p3)),
            mkEntry(taskId04, mkSet(t1p4))
        );
        when(standbyTaskCreator.createTasks(standbyTasksAssignment)).thenReturn(Collections.emptySet());

        taskManager.handleAssignment(activeTasksAssignment, standbyTasksAssignment);

        verify(topologyBuilder).addSubscribedTopicsFromAssignment(eq(mkSet(t1p1, t1p2, t2p2)), anyString());
        verify(topologyBuilder, never()).addSubscribedTopicsFromAssignment(eq(mkSet(t1p3, t1p4)), anyString());
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
    public void shouldPauseAllTopicsWithoutStateUpdaterOnRebalanceComplete() {
        final Set<TopicPartition> assigned = mkSet(t1p0, t1p1);
        when(consumer.assignment()).thenReturn(assigned);

        taskManager.handleRebalanceComplete();

        verify(consumer).pause(assigned);
    }

    @Test
    public void shouldNotPauseReadyTasksWithStateUpdaterOnRebalanceComplete() {
        final StreamTask statefulTask0 = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasks()).thenReturn(mkSet(statefulTask0));
        final Set<TopicPartition> assigned = mkSet(t1p0, t1p1);
        when(consumer.assignment()).thenReturn(assigned);

        taskManager.handleRebalanceComplete();

        verify(consumer).pause(mkSet(t1p1));
    }

    @Test
    public void shouldReleaseLockForUnassignedTasksAfterRebalance() throws Exception {
        expectLockObtainedFor(taskId00, taskId01, taskId02);
        expectDirectoryNotEmpty(taskId00, taskId01, taskId02);

        makeTaskFolders(
            taskId00.toString(),  // active task
            taskId01.toString(),  // standby task
            taskId02.toString()   // unassigned but able to lock
        );
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.lockedTaskDirectories(), is(mkSet(taskId00, taskId01, taskId02)));

        handleAssignment(taskId00Assignment, taskId01Assignment, emptyMap());

        taskManager.handleRebalanceComplete();
        assertThat(taskManager.lockedTaskDirectories(), is(mkSet(taskId00, taskId01)));

        verify(stateDirectory).unlock(taskId02);
        verify(consumer).pause(assignment);
    }

    @Test
    public void shouldReleaseLockForUnassignedTasksAfterRebalanceWithStateUpdater() throws Exception {
        final StreamTask runningStatefulTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId00Partitions).build();
        final StreamTask restoringStatefulTask = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING)
            .withInputPartitions(taskId01Partitions).build();
        final StandbyTask standbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING)
            .withInputPartitions(taskId02Partitions).build();
        final StandbyTask unassignedStandbyTask = standbyTask(taskId03, taskId03ChangelogPartitions)
            .inState(State.CREATED)
            .withInputPartitions(taskId03Partitions).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true);
        when(tasks.allTasksPerId()).thenReturn(mkMap(mkEntry(taskId00, runningStatefulTask)));
        when(stateUpdater.getTasks()).thenReturn(mkSet(standbyTask, restoringStatefulTask));
        when(tasks.allTasks()).thenReturn(mkSet(runningStatefulTask));
        expectLockObtainedFor(taskId00, taskId01, taskId02, taskId03);
        expectDirectoryNotEmpty(taskId00, taskId01, taskId02, taskId03);
        makeTaskFolders(
            taskId00.toString(),
            taskId01.toString(),
            taskId02.toString(),
            taskId03.toString()
        );

        final Set<TopicPartition> assigned = mkSet(t1p0, t1p1, t1p2);
        when(consumer.assignment()).thenReturn(assigned);

        taskManager.handleRebalanceStart(singleton("topic"));
        taskManager.handleRebalanceComplete();

        verify(consumer).pause(mkSet(t1p1, t1p2));
        verify(stateDirectory).unlock(taskId03);
        assertThat(taskManager.lockedTaskDirectories(), is(mkSet(taskId00, taskId01, taskId02)));
    }

    @Test
    public void shouldReportLatestOffsetAsOffsetSumForRunningTask() throws Exception {
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), Task.LATEST_OFFSET),
            mkEntry(new TopicPartition("changelog", 1), Task.LATEST_OFFSET)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, Task.LATEST_OFFSET));

        computeOffsetSumAndVerify(changelogOffsets, expectedOffsetSums);
    }

    @Test
    public void shouldComputeOffsetSumForNonRunningActiveTask() throws Exception {
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), 5L),
            mkEntry(new TopicPartition("changelog", 1), 10L)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, 15L));

        computeOffsetSumAndVerify(changelogOffsets, expectedOffsetSums);
    }

    @Test
    public void shouldComputeOffsetSumForRestoringActiveTaskWithStateUpdater() throws Exception {
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(restoringStatefulTask));
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.getTaskOffsetSums(), is(mkMap(mkEntry(taskId00, changelogOffset))));
    }

    @Test
    public void shouldComputeOffsetSumForRestoringStandbyTaskWithStateUpdater() throws Exception {
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(restoringStandbyTask));
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.getTaskOffsetSums(), is(mkMap(mkEntry(taskId00, changelogOffset))));
    }

    @Test
    public void shouldComputeOffsetSumForRunningStatefulTaskAndRestoringTaskWithStateUpdater() {
        final StreamTask runningStatefulTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .inState(State.RUNNING).build();
        final StreamTask restoringStatefulTask = statefulTask(taskId01, taskId01ChangelogPartitions)
            .inState(State.RESTORING).build();
        final StandbyTask restoringStandbyTask = standbyTask(taskId02, taskId02ChangelogPartitions)
            .inState(State.RUNNING).build();
        final long changelogOffsetOfRunningTask = 42L;
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
        when(stateUpdater.getTasks()).thenReturn(mkSet(restoringStandbyTask, restoringStatefulTask));

        assertThat(
            taskManager.getTaskOffsetSums(),
            is(mkMap(
                mkEntry(taskId00, changelogOffsetOfRunningTask),
                mkEntry(taskId01, changelogOffsetOfRestoringStatefulTask),
                mkEntry(taskId02, changelogOffsetOfRestoringStandbyTask)
            ))
        );
    }

    @Test
    public void shouldSkipUnknownOffsetsWhenComputingOffsetSum() throws Exception {
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), OffsetCheckpoint.OFFSET_UNKNOWN),
            mkEntry(new TopicPartition("changelog", 1), 10L)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, 10L));

        computeOffsetSumAndVerify(changelogOffsets, expectedOffsetSums);
    }

    private void computeOffsetSumAndVerify(final Map<TopicPartition, Long> changelogOffsets,
                                           final Map<TaskId, Long> expectedOffsetSums) throws Exception {
        expectLockObtainedFor(taskId00);
        expectDirectoryNotEmpty(taskId00);
        makeTaskFolders(taskId00.toString());

        taskManager.handleRebalanceStart(singleton("topic"));
        final StateMachineTask restoringTask = handleAssignment(
            emptyMap(),
            emptyMap(),
            taskId00Assignment
        ).get(taskId00);
        restoringTask.setChangelogOffsets(changelogOffsets);

        assertThat(taskManager.getTaskOffsetSums(), is(expectedOffsetSums));
    }

    @Test
    public void shouldComputeOffsetSumForStandbyTask() throws Exception {
        final Map<TopicPartition, Long> changelogOffsets = mkMap(
            mkEntry(new TopicPartition("changelog", 0), 5L),
            mkEntry(new TopicPartition("changelog", 1), 10L)
        );
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, 15L));

        expectLockObtainedFor(taskId00);
        expectDirectoryNotEmpty(taskId00);
        makeTaskFolders(taskId00.toString());

        taskManager.handleRebalanceStart(singleton("topic"));
        final StateMachineTask restoringTask = handleAssignment(
            emptyMap(),
            taskId00Assignment,
            emptyMap()
        ).get(taskId00);
        restoringTask.setChangelogOffsets(changelogOffsets);

        assertThat(taskManager.getTaskOffsetSums(), is(expectedOffsetSums));
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

        assertThat(taskManager.getTaskOffsetSums(), is(expectedOffsetSums));
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

        assertThat(taskManager.getTaskOffsetSums(), is(expectedOffsetSums));
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

        assertThat(taskManager.getTaskOffsetSums(), is(expectedOffsetSums));
    }
    
    @Test
    public void shouldNotReportOffsetSumsForTaskWeCantLock() throws Exception {
        expectLockFailedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        taskManager.handleRebalanceStart(singleton("topic"));
        assertTrue(taskManager.lockedTaskDirectories().isEmpty());

        assertTrue(taskManager.getTaskOffsetSums().isEmpty());
    }

    @Test
    public void shouldNotReportOffsetSumsAndReleaseLockForUnassignedTaskWithoutCheckpoint() throws Exception {
        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        expectDirectoryNotEmpty(taskId00);
        when(stateDirectory.checkpointFileFor(taskId00)).thenReturn(getCheckpointFile(taskId00));
        taskManager.handleRebalanceStart(singleton("topic"));

        assertTrue(taskManager.getTaskOffsetSums().isEmpty());
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

        assertThat(taskManager.getTaskOffsetSums(), is(expectedOffsetSums));
    }

    @Test
    public void shouldCloseActiveUnassignedSuspendedTasksWhenClosingRevokedTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        // first `handleAssignment`
        when(consumer.assignment()).thenReturn(assignment);

        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));

        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId00);
    }

    @Test
    public void shouldCloseDirtyActiveUnassignedTasksWhenErrorCleanClosingTask() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void closeClean() {
                throw new RuntimeException("KABOOM!");
            }
        };

        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.handleRevocation(taskId00Partitions);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(
            thrown.getMessage(),
            is("Encounter unexpected fatal error for task 0_0")
        );
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId00);
    }

    @Test
    public void shouldCloseActiveTasksWhenHandlingLostTasks() throws Exception {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager);

        // `handleAssignment`
        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));
        when(standbyTaskCreator.createTasks(taskId01Assignment)).thenReturn(singletonList(task01));

        final ArrayList<TaskDirectory> taskFolders = new ArrayList<>(2);
        taskFolders.add(new TaskDirectory(testFolder.newFolder(taskId00.toString()), null));
        taskFolders.add(new TaskDirectory(testFolder.newFolder(taskId01.toString()), null));

        when(stateDirectory.listNonEmptyTaskDirectories())
            .thenReturn(taskFolders)
            .thenReturn(new ArrayList<>());

        expectLockObtainedFor(taskId00, taskId01);
        expectDirectoryNotEmpty(taskId00, taskId01);

        taskManager.handleRebalanceStart(emptySet());
        assertThat(taskManager.lockedTaskDirectories(), Matchers.is(mkSet(taskId00, taskId01)));

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        // `handleLostAll`
        taskManager.handleLostAll();
        assertThat(task00.commitPrepared, is(true));
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), is(singletonMap(taskId01, task01)));

        // The locked task map will not be cleared.
        assertThat(taskManager.lockedTaskDirectories(), is(mkSet(taskId00, taskId01)));

        taskManager.handleRebalanceStart(emptySet());

        assertThat(taskManager.lockedTaskDirectories(), is(emptySet()));
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId00);
    }

    @Test
    public void shouldReInitializeThreadProducerOnHandleLostAllIfEosV2Enabled() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, false);

        taskManager.handleLostAll();

        verify(activeTaskCreator).reInitializeThreadProducer();
    }

    @Test
    public void shouldThrowWhenHandlingClosingTasksOnProducerCloseError() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        // `handleAssignment`
        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        // `handleAssignment`
        doThrow(new RuntimeException("KABOOM!")).when(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId00);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );

        assertThat(
            thrown.getMessage(),
            is("Encounter unexpected fatal error for task 0_0")
        );
        assertThat(thrown.getCause(), instanceOf(RuntimeException.class));
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
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

        taskManager.handleCorruption(mkSet(corruptedActiveTask.id(), corruptedStandbyTask.id()));

        final InOrder activeTaskOrder = inOrder(corruptedActiveTask);
        activeTaskOrder.verify(corruptedActiveTask).closeDirty();
        activeTaskOrder.verify(corruptedActiveTask).revive();
        final InOrder standbyTaskOrder = inOrder(corruptedStandbyTask);
        standbyTaskOrder.verify(corruptedStandbyTask).closeDirty();
        standbyTaskOrder.verify(corruptedStandbyTask).revive();
        verify(tasks).removeTask(corruptedActiveTask);
        verify(tasks).removeTask(corruptedStandbyTask);
        verify(tasks).addPendingTasksToInit(mkSet(corruptedActiveTask));
        verify(tasks).addPendingTasksToInit(mkSet(corruptedStandbyTask));
        verify(consumer).assignment();
    }

    @Test
    public void shouldReviveCorruptTasks() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        final AtomicBoolean enforcedCheckpoint = new AtomicBoolean(false);
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void postCommit(final boolean enforceCheckpoint) {
                if (enforceCheckpoint) {
                    enforcedCheckpoint.set(true);
                }
                super.postCommit(enforceCheckpoint);
            }
        };

        // `handleAssignment`
        when(consumer.assignment())
            .thenReturn(assignment)
            .thenReturn(taskId00Partitions);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), tp -> assertThat(tp, is(empty()))), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        task00.setChangelogOffsets(singletonMap(t1p0, 0L));
        taskManager.handleCorruption(singleton(taskId00));

        assertThat(task00.commitPrepared, is(true));
        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task00.partitionsForOffsetReset, equalTo(taskId00Partitions));
        assertThat(enforcedCheckpoint.get(), is(true));
        assertThat(taskManager.activeTaskMap(), is(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());

        verify(stateManager).markChangelogAsCorrupted(taskId00Partitions);
    }

    @Test
    public void shouldReviveCorruptTasksEvenIfTheyCannotCloseClean() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("oops");
            }
        };

        when(consumer.assignment())
            .thenReturn(assignment)
            .thenReturn(taskId00Partitions);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), tp -> assertThat(tp, is(empty()))), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        task00.setChangelogOffsets(singletonMap(t1p0, 0L));
        taskManager.handleCorruption(singleton(taskId00));
        assertThat(task00.commitPrepared, is(true));
        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task00.partitionsForOffsetReset, equalTo(taskId00Partitions));
        assertThat(taskManager.activeTaskMap(), is(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());

        verify(stateManager).markChangelogAsCorrupted(taskId00Partitions);
    }

    @Test
    public void shouldCommitNonCorruptedTasksOnTaskCorruptedException() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        final StateMachineTask corruptedTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask nonCorruptedTask = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);

        final Map<TaskId, Set<TopicPartition>> firstAssignment = new HashMap<>(taskId00Assignment);
        firstAssignment.putAll(taskId01Assignment);

        // `handleAssignment`
        when(activeTaskCreator.createTasks(any(), eq(firstAssignment)))
            .thenReturn(asList(corruptedTask, nonCorruptedTask));

        when(consumer.assignment())
            .thenReturn(assignment)
            .thenReturn(taskId00Partitions);

        taskManager.handleAssignment(firstAssignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), tp -> assertThat(tp, is(empty()))), is(true));

        assertThat(nonCorruptedTask.state(), is(Task.State.RUNNING));
        nonCorruptedTask.setCommitNeeded();

        corruptedTask.setChangelogOffsets(singletonMap(t1p0, 0L));
        taskManager.handleCorruption(singleton(taskId00));

        assertTrue(nonCorruptedTask.commitPrepared);
        assertThat(nonCorruptedTask.partitionsForOffsetReset, equalTo(Collections.emptySet()));
        assertThat(corruptedTask.partitionsForOffsetReset, equalTo(taskId00Partitions));

        // check that we should not commit empty map either
        verify(consumer, never()).commitSync(emptyMap());
        verify(stateManager).markChangelogAsCorrupted(taskId00Partitions);
    }

    @Test
    public void shouldNotCommitNonRunningNonCorruptedTasks() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        final StateMachineTask corruptedTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask nonRunningNonCorruptedTask = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);

        nonRunningNonCorruptedTask.setCommitNeeded();

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>(taskId00Assignment);
        assignment.putAll(taskId01Assignment);

        // `handleAssignment`
        when(activeTaskCreator.createTasks(any(), eq(assignment)))
            .thenReturn(asList(corruptedTask, nonRunningNonCorruptedTask));
        when(consumer.assignment()).thenReturn(taskId00Partitions);

        taskManager.handleAssignment(assignment, emptyMap());

        corruptedTask.setChangelogOffsets(singletonMap(t1p0, 0L));
        taskManager.handleCorruption(singleton(taskId00));

        assertThat(nonRunningNonCorruptedTask.state(), is(Task.State.CREATED));
        assertThat(nonRunningNonCorruptedTask.partitionsForOffsetReset, equalTo(Collections.emptySet()));
        assertThat(corruptedTask.partitionsForOffsetReset, equalTo(taskId00Partitions));

        assertFalse(nonRunningNonCorruptedTask.commitPrepared);
        verify(stateManager).markChangelogAsCorrupted(taskId00Partitions);
    }

    @Test
    public void shouldNotCommitNonCorruptedRestoringActiveTasksAndNotCommitRunningStandbyTasksWithStateUpdaterEnabled() {
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

        taskManager.handleCorruption(mkSet(taskId02));

        verify(activeRestoringTask, never()).commitNeeded();
        verify(activeRestoringTask, never()).prepareCommit();
        verify(activeRestoringTask, never()).postCommit(anyBoolean());
        verify(standbyTask, never()).commitNeeded();
        verify(standbyTask, never()).prepareCommit();
        verify(standbyTask, never()).postCommit(anyBoolean());
    }

    @Test
    public void shouldNotCommitNonCorruptedRestoringActiveTasksAndCommitRunningStandbyTasksWithStateUpdaterDisabled() {
        final StreamTask activeRestoringTask = statefulTask(taskId00, taskId00ChangelogPartitions)
            .withInputPartitions(taskId00Partitions)
            .inState(State.RESTORING).build();
        final StandbyTask standbyTask = standbyTask(taskId01, taskId01ChangelogPartitions)
            .withInputPartitions(taskId01Partitions)
            .inState(State.RUNNING).build();
        when(standbyTask.commitNeeded()).thenReturn(true);
        final StreamTask corruptedTask = statefulTask(taskId02, taskId02ChangelogPartitions)
            .withInputPartitions(taskId02Partitions)
            .inState(State.RUNNING).build();
        final TasksRegistry tasks = mock(TasksRegistry.class);
        when(tasks.allTasksPerId()).thenReturn(mkMap(
            mkEntry(taskId00, activeRestoringTask),
            mkEntry(taskId01, standbyTask),
            mkEntry(taskId02, corruptedTask)
        ));
        when(tasks.task(taskId02)).thenReturn(corruptedTask);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, false);
        when(consumer.assignment()).thenReturn(intersection(HashSet::new, taskId00Partitions, taskId01Partitions, taskId02Partitions));

        taskManager.handleCorruption(mkSet(taskId02));

        verify(activeRestoringTask, never()).commitNeeded();
        verify(activeRestoringTask, never()).prepareCommit();
        verify(activeRestoringTask, never()).postCommit(anyBoolean());
        verify(standbyTask).prepareCommit();
        verify(standbyTask).postCommit(anyBoolean());
    }

    @Test
    public void shouldCleanAndReviveCorruptedStandbyTasksBeforeCommittingNonCorruptedTasks() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        final StateMachineTask corruptedStandby = new StateMachineTask(taskId00, taskId00Partitions, false, stateManager);
        final StateMachineTask runningNonCorruptedActive = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
                throw new TaskMigratedException("You dropped out of the group!", new RuntimeException());
            }
        };

        // handleAssignment
        when(activeTaskCreator.createTasks(any(), eq(taskId01Assignment)))
            .thenReturn(singleton(runningNonCorruptedActive));
        when(standbyTaskCreator.createTasks(taskId00Assignment)).thenReturn(singleton(corruptedStandby));

        when(consumer.assignment()).thenReturn(assignment);

        taskManager.handleAssignment(taskId01Assignment, taskId00Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        // make sure this will be committed and throw
        assertThat(runningNonCorruptedActive.state(), is(Task.State.RUNNING));
        assertThat(corruptedStandby.state(), is(Task.State.RUNNING));

        runningNonCorruptedActive.setCommitNeeded();

        corruptedStandby.setChangelogOffsets(singletonMap(t1p0, 0L));
        assertThrows(TaskMigratedException.class, () -> taskManager.handleCorruption(singleton(taskId00)));


        assertThat(corruptedStandby.commitPrepared, is(true));
        assertThat(corruptedStandby.state(), is(Task.State.CREATED));
        verify(stateManager).markChangelogAsCorrupted(taskId00Partitions);
    }

    @Test
    public void shouldNotAttemptToCommitInHandleCorruptedDuringARebalance() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        when(stateDirectory.listNonEmptyTaskDirectories()).thenReturn(new ArrayList<>());

        final StateMachineTask corruptedActive = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);

        // make sure this will attempt to be committed and throw
        final StateMachineTask uncorruptedActive = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        uncorruptedActive.setCommitNeeded();

        // handleAssignment
        final Map<TaskId, Set<TopicPartition>> firstAssignement = new HashMap<>();
        firstAssignement.putAll(taskId00Assignment);
        firstAssignement.putAll(taskId01Assignment);
        when(activeTaskCreator.createTasks(any(), eq(firstAssignement)))
            .thenReturn(asList(corruptedActive, uncorruptedActive));

        when(consumer.assignment())
            .thenReturn(assignment)
            .thenReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions));

        uncorruptedActive.setCommittableOffsetsAndMetadata(offsets);

        taskManager.handleAssignment(firstAssignement, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(uncorruptedActive.state(), is(Task.State.RUNNING));

        assertThat(uncorruptedActive.commitPrepared, is(false));
        assertThat(uncorruptedActive.commitNeeded, is(true));
        assertThat(uncorruptedActive.commitCompleted, is(false));

        taskManager.handleRebalanceStart(singleton(topic1));
        assertThat(taskManager.rebalanceInProgress(), is(true));
        taskManager.handleCorruption(singleton(taskId00));

        assertThat(uncorruptedActive.commitPrepared, is(false));
        assertThat(uncorruptedActive.commitNeeded, is(true));
        assertThat(uncorruptedActive.commitCompleted, is(false));

        assertThat(uncorruptedActive.state(), is(State.RUNNING));
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitWithALOS() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        final StateMachineTask corruptedActive = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask uncorruptedActive = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
                fail("Should not try to mark changelogs as corrupted for uncorrupted task");
            }
        };
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        uncorruptedActive.setCommittableOffsetsAndMetadata(offsets);

        // handleAssignment
        final Map<TaskId, Set<TopicPartition>> firstAssignment = new HashMap<>();
        firstAssignment.putAll(taskId00Assignment);
        firstAssignment.putAll(taskId01Assignment);
        when(activeTaskCreator.createTasks(any(), eq(firstAssignment)))
            .thenReturn(asList(corruptedActive, uncorruptedActive));

        when(consumer.assignment())
            .thenReturn(assignment)
            .thenReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions));

        doThrow(new TimeoutException()).when(consumer).commitSync(offsets);

        taskManager.handleAssignment(firstAssignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(uncorruptedActive.state(), is(Task.State.RUNNING));
        assertThat(corruptedActive.state(), is(Task.State.RUNNING));

        // make sure this will be committed and throw
        uncorruptedActive.setCommitNeeded();
        corruptedActive.setChangelogOffsets(singletonMap(t1p0, 0L));

        assertThat(uncorruptedActive.commitPrepared, is(false));
        assertThat(uncorruptedActive.commitNeeded, is(true));
        assertThat(uncorruptedActive.commitCompleted, is(false));
        assertThat(corruptedActive.commitPrepared, is(false));
        assertThat(corruptedActive.commitNeeded, is(false));
        assertThat(corruptedActive.commitCompleted, is(false));

        taskManager.handleCorruption(singleton(taskId00));

        assertThat(uncorruptedActive.commitPrepared, is(true));
        assertThat(uncorruptedActive.commitNeeded, is(false));
        assertThat(uncorruptedActive.commitCompleted, is(false)); //if not corrupted, we should close dirty without committing
        assertThat(corruptedActive.commitPrepared, is(true));
        assertThat(corruptedActive.commitNeeded, is(false));
        assertThat(corruptedActive.commitCompleted, is(true)); //if corrupted, should enforce checkpoint with corrupted tasks removed

        assertThat(corruptedActive.state(), is(Task.State.CREATED));
        assertThat(uncorruptedActive.state(), is(Task.State.CREATED));
        verify(stateManager).markChangelogAsCorrupted(taskId00Partitions);
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitDuringHandleCorruptedWithEOS() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, false);
        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.threadProducer()).thenReturn(producer);
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        final AtomicBoolean corruptedTaskChangelogMarkedAsCorrupted = new AtomicBoolean(false);
        final StateMachineTask corruptedActiveTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
                super.markChangelogAsCorrupted(partitions);
                corruptedTaskChangelogMarkedAsCorrupted.set(true);
            }
        };

        final AtomicBoolean uncorruptedTaskChangelogMarkedAsCorrupted = new AtomicBoolean(false);
        final StateMachineTask uncorruptedActiveTask = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
                super.markChangelogAsCorrupted(partitions);
                uncorruptedTaskChangelogMarkedAsCorrupted.set(true);
            }
        };
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        uncorruptedActiveTask.setCommittableOffsetsAndMetadata(offsets);

        // handleAssignment
        final Map<TaskId, Set<TopicPartition>> firstAssignment = new HashMap<>();
        firstAssignment.putAll(taskId00Assignment);
        firstAssignment.putAll(taskId01Assignment);
        when(activeTaskCreator.createTasks(any(), eq(firstAssignment)))
            .thenReturn(asList(corruptedActiveTask, uncorruptedActiveTask));

        when(consumer.assignment())
            .thenReturn(assignment)
            .thenReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions));

        final ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata("appId");
        when(consumer.groupMetadata()).thenReturn(groupMetadata);

        doThrow(new TimeoutException()).when(producer).commitTransaction(offsets, groupMetadata);

        taskManager.handleAssignment(firstAssignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(uncorruptedActiveTask.state(), is(Task.State.RUNNING));
        assertThat(corruptedActiveTask.state(), is(Task.State.RUNNING));

        // make sure this will be committed and throw
        uncorruptedActiveTask.setCommitNeeded();

        final Map<TopicPartition, Long> corruptedActiveTaskChangelogOffsets = singletonMap(t1p0changelog, 0L);
        corruptedActiveTask.setChangelogOffsets(corruptedActiveTaskChangelogOffsets);
        final Map<TopicPartition, Long> uncorruptedActiveTaskChangelogOffsets = singletonMap(t1p1changelog, 0L);
        uncorruptedActiveTask.setChangelogOffsets(uncorruptedActiveTaskChangelogOffsets);

        assertThat(uncorruptedActiveTask.commitPrepared, is(false));
        assertThat(uncorruptedActiveTask.commitNeeded, is(true));
        assertThat(uncorruptedActiveTask.commitCompleted, is(false));
        assertThat(corruptedActiveTask.commitPrepared, is(false));
        assertThat(corruptedActiveTask.commitNeeded, is(false));
        assertThat(corruptedActiveTask.commitCompleted, is(false));

        taskManager.handleCorruption(singleton(taskId00));

        assertThat(uncorruptedActiveTask.commitPrepared, is(true));
        assertThat(uncorruptedActiveTask.commitNeeded, is(false));
        assertThat(uncorruptedActiveTask.commitCompleted, is(true)); //if corrupted due to timeout on commit, should enforce checkpoint with corrupted tasks removed
        assertThat(corruptedActiveTask.commitPrepared, is(true));
        assertThat(corruptedActiveTask.commitNeeded, is(false));
        assertThat(corruptedActiveTask.commitCompleted, is(true)); //if corrupted, should enforce checkpoint with corrupted tasks removed

        assertThat(corruptedActiveTask.state(), is(Task.State.CREATED));
        assertThat(uncorruptedActiveTask.state(), is(Task.State.CREATED));
        assertThat(corruptedTaskChangelogMarkedAsCorrupted.get(), is(true));
        assertThat(uncorruptedTaskChangelogMarkedAsCorrupted.get(), is(true));
        verify(stateManager).markChangelogAsCorrupted(taskId00ChangelogPartitions);
        verify(stateManager).markChangelogAsCorrupted(taskId01ChangelogPartitions);
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitDuringRevocationWithALOS() {
        final StateMachineTask revokedActiveTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        revokedActiveTask.setCommittableOffsetsAndMetadata(offsets00);
        revokedActiveTask.setCommitNeeded();

        final StateMachineTask unrevokedActiveTaskWithCommitNeeded = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
                fail("Should not try to mark changelogs as corrupted for uncorrupted task");
            }
        };
        final Map<TopicPartition, OffsetAndMetadata> offsets01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        unrevokedActiveTaskWithCommitNeeded.setCommittableOffsetsAndMetadata(offsets01);
        unrevokedActiveTaskWithCommitNeeded.setCommitNeeded();

        final StateMachineTask unrevokedActiveTaskWithoutCommitNeeded = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);

        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(offsets00);
        expectedCommittedOffsets.putAll(offsets01);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
        );

        when(consumer.assignment())
            .thenReturn(assignment)
            .thenReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions, taskId02Partitions));

        when(activeTaskCreator.createTasks(any(), eq(assignmentActive)))
            .thenReturn(asList(revokedActiveTask, unrevokedActiveTaskWithCommitNeeded, unrevokedActiveTaskWithoutCommitNeeded));

        doThrow(new TimeoutException()).when(consumer).commitSync(expectedCommittedOffsets);

        taskManager.handleAssignment(assignmentActive, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(revokedActiveTask.state(), is(Task.State.RUNNING));
        assertThat(unrevokedActiveTaskWithCommitNeeded.state(), is(State.RUNNING));
        assertThat(unrevokedActiveTaskWithoutCommitNeeded.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);

        assertThat(revokedActiveTask.state(), is(State.SUSPENDED));
        assertThat(unrevokedActiveTaskWithCommitNeeded.state(), is(State.CREATED));
        assertThat(unrevokedActiveTaskWithoutCommitNeeded.state(), is(State.RUNNING));
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitDuringRevocationWithEOS() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, false);
        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.threadProducer()).thenReturn(producer);
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        final StateMachineTask revokedActiveTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> revokedActiveTaskOffsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        revokedActiveTask.setCommittableOffsetsAndMetadata(revokedActiveTaskOffsets);
        revokedActiveTask.setCommitNeeded();

        final AtomicBoolean unrevokedTaskChangelogMarkedAsCorrupted = new AtomicBoolean(false);
        final StateMachineTask unrevokedActiveTask = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
                super.markChangelogAsCorrupted(partitions);
                unrevokedTaskChangelogMarkedAsCorrupted.set(true);
            }
        };
        final Map<TopicPartition, OffsetAndMetadata> unrevokedTaskOffsets = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        unrevokedActiveTask.setCommittableOffsetsAndMetadata(unrevokedTaskOffsets);
        unrevokedActiveTask.setCommitNeeded();

        final StateMachineTask unrevokedActiveTaskWithoutCommitNeeded = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);

        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(revokedActiveTaskOffsets);
        expectedCommittedOffsets.putAll(unrevokedTaskOffsets);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
            );

        when(consumer.assignment())
            .thenReturn(assignment)
            .thenReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions, taskId02Partitions));

        when(activeTaskCreator.createTasks(any(), eq(assignmentActive)))
            .thenReturn(asList(revokedActiveTask, unrevokedActiveTask, unrevokedActiveTaskWithoutCommitNeeded));

        final ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata("appId");
        when(consumer.groupMetadata()).thenReturn(groupMetadata);

        doThrow(new TimeoutException()).when(producer).commitTransaction(expectedCommittedOffsets, groupMetadata);

        taskManager.handleAssignment(assignmentActive, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(revokedActiveTask.state(), is(Task.State.RUNNING));
        assertThat(unrevokedActiveTask.state(), is(Task.State.RUNNING));
        assertThat(unrevokedActiveTaskWithoutCommitNeeded.state(), is(State.RUNNING));

        final Map<TopicPartition, Long> revokedActiveTaskChangelogOffsets = singletonMap(t1p0changelog, 0L);
        revokedActiveTask.setChangelogOffsets(revokedActiveTaskChangelogOffsets);
        final Map<TopicPartition, Long> unrevokedActiveTaskChangelogOffsets = singletonMap(t1p1changelog, 0L);
        unrevokedActiveTask.setChangelogOffsets(unrevokedActiveTaskChangelogOffsets);

        taskManager.handleRevocation(taskId00Partitions);

        assertThat(unrevokedTaskChangelogMarkedAsCorrupted.get(), is(true));
        assertThat(revokedActiveTask.state(), is(State.SUSPENDED));
        assertThat(unrevokedActiveTask.state(), is(State.CREATED));
        assertThat(unrevokedActiveTaskWithoutCommitNeeded.state(), is(State.RUNNING));
        verify(stateManager).markChangelogAsCorrupted(taskId00ChangelogPartitions);
        verify(stateManager).markChangelogAsCorrupted(taskId01ChangelogPartitions);
    }

    @Test
    public void shouldCloseStandbyUnassignedTasksWhenCreatingNewTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false, stateManager);

        when(consumer.assignment()).thenReturn(assignment);
        when(standbyTaskCreator.createTasks(taskId00Assignment)).thenReturn(singletonList(task00));

        taskManager.handleAssignment(emptyMap(), taskId00Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
    }

    @Test
    public void shouldAddNonResumedSuspendedTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager);

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));
        when(standbyTaskCreator.createTasks(taskId01Assignment)).thenReturn(singletonList(task01));

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        // expect these calls twice (because we're going to tryToCompleteRestoration twice)
        verify(activeTaskCreator).createTasks(any(), eq(emptyMap()));
        verify(consumer, times(2)).assignment();
        verify(consumer, times(2)).resume(assignment);
    }

    @Test
    public void shouldUpdateInputPartitionsAfterRebalance() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        final Set<TopicPartition> newPartitionsSet = mkSet(t1p1);
        final Map<TaskId, Set<TopicPartition>> taskIdSetMap = singletonMap(taskId00, newPartitionsSet);
        taskManager.handleAssignment(taskIdSetMap, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertEquals(newPartitionsSet, task00.inputPartitions());
        // expect these calls twice (because we're going to tryToCompleteRestoration twice)
        verify(consumer, times(2)).resume(assignment);
        verify(consumer, times(2)).assignment();
        verify(activeTaskCreator).createTasks(any(), eq(emptyMap()));
    }

    @Test
    public void shouldAddNewActiveTasks() {
        final Map<TaskId, Set<TopicPartition>> assignment = taskId00Assignment;
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);

        when(activeTaskCreator.createTasks(any(), eq(assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration(time.milliseconds(), noOpResetter -> { });

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(changeLogReader).enforceRestoreActive();
        verify(consumer).assignment();
        verify(consumer).resume(eq(emptySet()));
    }

    @Test
    public void shouldNotCompleteRestorationIfTasksCannotInitialize() {
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void initializeIfNeeded() {
                throw new LockException("can't lock");
            }
        };
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public void initializeIfNeeded() {
                throw new TimeoutException("timed out");
            }
        };

        when(activeTaskCreator.createTasks(any(), eq(assignment))).thenReturn(asList(task00, task01));

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.CREATED));

        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(false));

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.CREATED));
        assertThat(
            taskManager.activeTaskMap(),
            Matchers.equalTo(mkMap(mkEntry(taskId00, task00), mkEntry(taskId01, task01)))
        );
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(changeLogReader).enforceRestoreActive();
        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldNotCompleteRestorationIfTaskCannotCompleteRestoration() {
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void completeRestoration(final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
                throw new TimeoutException("timeout!");
            }
        };

        when(activeTaskCreator.createTasks(any(), eq(assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(false));

        assertThat(task00.state(), is(Task.State.RESTORING));
        assertThat(
            taskManager.activeTaskMap(),
            Matchers.equalTo(mkMap(mkEntry(taskId00, task00)))
        );
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(changeLogReader).enforceRestoreActive();
        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldSuspendActiveTasksDuringRevocation() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));
    }

    @Test
    public void shouldCommitAllActiveTasksThatNeedCommittingOnHandleRevocationWithEosV2() {
        final StreamsProducer producer = mock(StreamsProducer.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, false);

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets00);
        task00.setCommitNeeded();

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        task01.setCommittableOffsetsAndMetadata(offsets01);
        task01.setCommitNeeded();

        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets02 = singletonMap(t1p2, new OffsetAndMetadata(2L, null));
        task02.setCommittableOffsetsAndMetadata(offsets02);

        final StateMachineTask task10 = new StateMachineTask(taskId10, taskId10Partitions, false, stateManager);

        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(offsets00);
        expectedCommittedOffsets.putAll(offsets01);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
        );

        final Map<TaskId, Set<TopicPartition>> assignmentStandby = mkMap(
            mkEntry(taskId10, taskId10Partitions)
        );
        when(consumer.assignment()).thenReturn(assignment);

        when(activeTaskCreator.createTasks(any(), eq(assignmentActive)))
            .thenReturn(asList(task00, task01, task02));

        when(activeTaskCreator.threadProducer()).thenReturn(producer);
        when(standbyTaskCreator.createTasks(assignmentStandby))
            .thenReturn(singletonList(task10));

        final ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata("appId");
        when(consumer.groupMetadata()).thenReturn(groupMetadata);

        task00.committedOffsets();
        task01.committedOffsets();
        task02.committedOffsets();
        task10.committedOffsets();

        taskManager.handleAssignment(assignmentActive, assignmentStandby);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(task02.state(), is(Task.State.RUNNING));
        assertThat(task10.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);

        assertThat(task00.commitNeeded, is(false));
        assertThat(task01.commitNeeded, is(false));
        assertThat(task02.commitPrepared, is(false));
        assertThat(task10.commitPrepared, is(false));

        verify(producer).commitTransaction(expectedCommittedOffsets, groupMetadata);
    }

    @Test
    public void shouldCommitAllNeededTasksOnHandleRevocation() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets00);
        task00.setCommitNeeded();

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        task01.setCommittableOffsetsAndMetadata(offsets01);
        task01.setCommitNeeded();

        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets02 = singletonMap(t1p2, new OffsetAndMetadata(2L, null));
        task02.setCommittableOffsetsAndMetadata(offsets02);

        final StateMachineTask task10 = new StateMachineTask(taskId10, taskId10Partitions, false, stateManager);

        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(offsets00);
        expectedCommittedOffsets.putAll(offsets01);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
        );

        final Map<TaskId, Set<TopicPartition>> assignmentStandby = mkMap(
            mkEntry(taskId10, taskId10Partitions)
        );
        when(consumer.assignment()).thenReturn(assignment);

        when(activeTaskCreator.createTasks(any(), eq(assignmentActive)))
            .thenReturn(asList(task00, task01, task02));
        when(standbyTaskCreator.createTasks(assignmentStandby))
            .thenReturn(singletonList(task10));

        taskManager.handleAssignment(assignmentActive, assignmentStandby);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(task02.state(), is(Task.State.RUNNING));
        assertThat(task10.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);

        assertThat(task00.commitNeeded, is(false));
        assertThat(task00.commitPrepared, is(true));
        assertThat(task00.commitNeeded, is(false));
        assertThat(task01.commitPrepared, is(true));
        assertThat(task02.commitPrepared, is(false));
        assertThat(task10.commitPrepared, is(false));

        verify(consumer).commitSync(expectedCommittedOffsets);
    }

    @Test
    public void shouldNotCommitOnHandleAssignmentIfNoTaskClosed() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets00);
        task00.setCommitNeeded();

        final StateMachineTask task10 = new StateMachineTask(taskId10, taskId10Partitions, false, stateManager);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = singletonMap(taskId00, taskId00Partitions);
        final Map<TaskId, Set<TopicPartition>> assignmentStandby = singletonMap(taskId10, taskId10Partitions);

        when(consumer.assignment()).thenReturn(assignment);

        when(activeTaskCreator.createTasks(any(), eq(assignmentActive))).thenReturn(singleton(task00));
        when(standbyTaskCreator.createTasks(assignmentStandby)).thenReturn(singletonList(task10));

        taskManager.handleAssignment(assignmentActive, assignmentStandby);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task10.state(), is(Task.State.RUNNING));

        taskManager.handleAssignment(assignmentActive, assignmentStandby);

        assertThat(task00.commitNeeded, is(true));
        assertThat(task10.commitPrepared, is(false));
    }

    @Test
    public void shouldNotCommitOnHandleAssignmentIfOnlyStandbyTaskClosed() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets00);
        task00.setCommitNeeded();

        final StateMachineTask task10 = new StateMachineTask(taskId10, taskId10Partitions, false, stateManager);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = singletonMap(taskId00, taskId00Partitions);
        final Map<TaskId, Set<TopicPartition>> assignmentStandby = singletonMap(taskId10, taskId10Partitions);

        when(consumer.assignment()).thenReturn(assignment);

        when(activeTaskCreator.createTasks(any(), eq(assignmentActive))).thenReturn(singleton(task00));
        when(standbyTaskCreator.createTasks(assignmentStandby)).thenReturn(singletonList(task10));

        taskManager.handleAssignment(assignmentActive, assignmentStandby);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task10.state(), is(Task.State.RUNNING));

        taskManager.handleAssignment(assignmentActive, Collections.emptyMap());

        assertThat(task00.commitNeeded, is(true));
    }

    @Test
    public void shouldNotCommitCreatedTasksOnRevocationOrClosure() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);

        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));

        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), is(Task.State.CLOSED));
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId00);
    }

    @Test
    public void shouldPassUpIfExceptionDuringSuspend() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("KABOOM!");
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));
        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        assertThrows(RuntimeException.class, () -> taskManager.handleRevocation(taskId00Partitions));
        assertThat(task00.state(), is(Task.State.SUSPENDED));
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdownWithAlos() {
        shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdown(ProcessingMode.AT_LEAST_ONCE);
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdownWithExactlyOnceV1() {
        when(activeTaskCreator.streamsProducerForTask(any())).thenReturn(mock(StreamsProducer.class));
        shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdown(ProcessingMode.EXACTLY_ONCE_ALPHA);
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdownWithExactlyOnceV2() {
        when(activeTaskCreator.threadProducer()).thenReturn(mock(StreamsProducer.class));
        shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdown(ProcessingMode.EXACTLY_ONCE_V2);
    }

    private void shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdown(final ProcessingMode processingMode) {
        final TaskManager taskManager = setUpTaskManager(processingMode, null, false);

        final TopicPartition changelog = new TopicPartition("changelog", 0);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions),
            mkEntry(taskId03, taskId03Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public Set<TopicPartition> changelogPartitions() {
                return singleton(changelog);
            }
        };
        final AtomicBoolean closedDirtyTask01 = new AtomicBoolean(false);
        final AtomicBoolean closedDirtyTask02 = new AtomicBoolean(false);
        final AtomicBoolean closedDirtyTask03 = new AtomicBoolean(false);
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("migrated", new RuntimeException("cause"));
            }

            @Override
            public void closeDirty() {
                super.closeDirty();
                closedDirtyTask01.set(true);
            }
        };
        final Task task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("oops");
            }

            @Override
            public void closeDirty() {
                super.closeDirty();
                closedDirtyTask02.set(true);
            }
        };
        final Task task03 = new StateMachineTask(taskId03, taskId03Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("oops");
            }

            @Override
            public void closeDirty() {
                super.closeDirty();
                closedDirtyTask03.set(true);
            }
        };

        when(activeTaskCreator.createTasks(any(), eq(assignment)))
            .thenReturn(asList(task00, task01, task02, task03));

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.CREATED));
        assertThat(task02.state(), is(Task.State.CREATED));
        assertThat(task03.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration(time.milliseconds(), null);

        assertThat(task00.state(), is(Task.State.RESTORING));
        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(task02.state(), is(Task.State.RUNNING));
        assertThat(task03.state(), is(Task.State.RUNNING));
        assertThat(
            taskManager.activeTaskMap(),
            Matchers.equalTo(
                mkMap(
                    mkEntry(taskId00, task00),
                    mkEntry(taskId01, task01),
                    mkEntry(taskId02, task02),
                    mkEntry(taskId03, task03)
                )
            )
        );
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(changeLogReader).enforceRestoreActive();
        verify(changeLogReader).completedChangelogs();

        final RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> taskManager.shutdown(true)
        );
        assertThat(exception.getCause().getMessage(), is("oops"));

        assertThat(closedDirtyTask01.get(), is(true));
        assertThat(closedDirtyTask02.get(), is(true));
        assertThat(closedDirtyTask03.get(), is(true));
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(task01.state(), is(Task.State.CLOSED));
        assertThat(task02.state(), is(Task.State.CLOSED));
        assertThat(task03.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator, times(4)).closeAndRemoveTaskProducerIfNeeded(any());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator).closeThreadProducerIfNeeded();
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateTaskProducerExceptionsOnCleanShutdown() {
        final TopicPartition changelog = new TopicPartition("changelog", 0);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions)
        );
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public Set<TopicPartition> changelogPartitions() {
                return singleton(changelog);
            }
        };
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        when(activeTaskCreator.createTasks(any(), eq(assignment))).thenReturn(singletonList(task00));
        doThrow(new RuntimeException("whatever"))
            .when(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId00);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration(time.milliseconds(), null);

        assertThat(task00.state(), is(Task.State.RESTORING));
        assertThat(
            taskManager.activeTaskMap(),
            Matchers.equalTo(
                mkMap(
                    mkEntry(taskId00, task00)
                )
            )
        );
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(changeLogReader).enforceRestoreActive();
        verify(changeLogReader).completedChangelogs();

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> taskManager.shutdown(true));

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(exception.getCause().getMessage(), is("whatever"));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId00);
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator).closeThreadProducerIfNeeded();
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateThreadProducerExceptionsOnCleanShutdown() {
        final TopicPartition changelog = new TopicPartition("changelog", 0);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public Set<TopicPartition> changelogPartitions() {
                return singleton(changelog);
            }
        };

        when(activeTaskCreator.createTasks(any(), eq(assignment))).thenReturn(singletonList(task00));
        doThrow(new RuntimeException("whatever")).when(activeTaskCreator).closeThreadProducerIfNeeded();

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration(time.milliseconds(), null);

        assertThat(task00.state(), is(Task.State.RESTORING));
        assertThat(
            taskManager.activeTaskMap(),
            Matchers.equalTo(
                mkMap(
                    mkEntry(taskId00, task00)
                )
            )
        );
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(changeLogReader).enforceRestoreActive();
        verify(changeLogReader).completedChangelogs();

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> taskManager.shutdown(true));

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(exception.getMessage(), is("whatever"));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId00);
    }

    @Test
    public void shouldOnlyCommitRevokedStandbyTaskAndPropagatePrepareCommitException() {
        setUpTaskManager(ProcessingMode.EXACTLY_ONCE_ALPHA, false);

        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false, stateManager);

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
                throw new RuntimeException("task 0_1 prepare commit boom!");
            }
        };
        task01.setCommitNeeded();

        taskManager.addTask(task00);
        taskManager.addTask(task01);

        final RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> taskManager.handleAssignment(
                Collections.emptyMap(),
                singletonMap(taskId00, taskId00Partitions)
            ));
        assertThat(thrown.getCause().getMessage(), is("task 0_1 prepare commit boom!"));

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.CLOSED));

        // All the tasks involving in the commit should already be removed.
        assertThat(taskManager.allTasks(), is(Collections.singletonMap(taskId00, task00)));
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
    public void shouldCloseActiveTasksAndIgnoreExceptionsOnUncleanShutdown() {
        final TopicPartition changelog = new TopicPartition("changelog", 0);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public Set<TopicPartition> changelogPartitions() {
                return singleton(changelog);
            }
        };
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("migrated", new RuntimeException("cause"));
            }
        };
        final Task task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("oops");
            }
        };

        when(activeTaskCreator.createTasks(any(), eq(assignment))).thenReturn(asList(task00, task01, task02));
        doThrow(new RuntimeException("whatever")).when(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(any());
        doThrow(new RuntimeException("whatever all")).when(activeTaskCreator).closeThreadProducerIfNeeded();

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.CREATED));
        assertThat(task02.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration(time.milliseconds(), null);

        assertThat(task00.state(), is(Task.State.RESTORING));
        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(task02.state(), is(Task.State.RUNNING));
        assertThat(
            taskManager.activeTaskMap(),
            Matchers.equalTo(
                mkMap(
                    mkEntry(taskId00, task00),
                    mkEntry(taskId01, task01),
                    mkEntry(taskId02, task02)
                )
            )
        );
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(changeLogReader).enforceRestoreActive();
        verify(changeLogReader).completedChangelogs();

        taskManager.shutdown(false);

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(task01.state(), is(Task.State.CLOSED));
        assertThat(task02.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator, times(3)).closeAndRemoveTaskProducerIfNeeded(any());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator).closeThreadProducerIfNeeded();
    }

    @Test
    public void shouldCloseStandbyTasksOnShutdown() {
        final Map<TaskId, Set<TopicPartition>> assignment = singletonMap(taskId00, taskId00Partitions);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false, stateManager);

        // `handleAssignment`
        when(standbyTaskCreator.createTasks(assignment)).thenReturn(singletonList(task00));

        taskManager.handleAssignment(emptyMap(), assignment);
        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration(time.milliseconds(), null);
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));

        taskManager.shutdown(true);
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator).closeThreadProducerIfNeeded();
        // `tryToCompleteRestoration`
        verify(consumer).assignment();
        verify(consumer).resume(eq(emptySet()));
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

        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(failedStatefulTask.id());
        verify(activeTaskCreator).closeThreadProducerIfNeeded();
        verify(stateUpdater).shutdown(Duration.ofMillis(Long.MAX_VALUE));
        verify(failedStatefulTask).prepareCommit();
        verify(failedStatefulTask).suspend();
        verify(failedStatefulTask).closeDirty();
    }

    @Test
    public void shouldShutdownSchedulingTaskManager() {
        final TasksRegistry tasks = mock(TasksRegistry.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.AT_LEAST_ONCE, tasks, true, true);

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
        when(stateUpdater.getTasks())
            .thenReturn(mkSet(
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
        verify(removedFailedStatefulTask).prepareCommit();
        verify(removedFailedStatefulTask).suspend();
        verify(removedFailedStatefulTask).closeDirty();
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId03);
        verify(removedFailedStandbyTask).prepareCommit();
        verify(removedFailedStandbyTask).suspend();
        verify(removedFailedStandbyTask).closeDirty();
        verify(activeTaskCreator, never()).closeAndRemoveTaskProducerIfNeeded(taskId04);
        verify(removedFailedStatefulTaskDuringRemoval).prepareCommit();
        verify(removedFailedStatefulTaskDuringRemoval).suspend();
        verify(removedFailedStatefulTaskDuringRemoval).closeDirty();
        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId05);
        verify(removedFailedStandbyTaskDuringRemoval).prepareCommit();
        verify(removedFailedStandbyTaskDuringRemoval).suspend();
        verify(removedFailedStandbyTaskDuringRemoval).closeDirty();
        verify(activeTaskCreator, never()).closeAndRemoveTaskProducerIfNeeded(taskId00);
    }

    @Test
    public void shouldInitializeNewActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        when(consumer.assignment()).thenReturn(assignment);

        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment)))
            .thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // verifies that we actually resume the assignment at the end of restoration.
        verify(consumer).resume(assignment);
    }

    @Test
    public void shouldInitialiseNewStandbyTasks() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager);

        when(consumer.assignment()).thenReturn(assignment);
        when(standbyTaskCreator.createTasks(taskId01Assignment)).thenReturn(singletonList(task01));

        taskManager.handleAssignment(emptyMap(), taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.equalTo(singletonMap(taskId01, task01)));
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
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager);

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment)))
            .thenReturn(singletonList(task00));
        when(standbyTaskCreator.createTasks(taskId01Assignment))
            .thenReturn(singletonList(task01));

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();
        task01.setCommitNeeded();

        assertThat(taskManager.commitAll(), equalTo(2));
        assertThat(task00.commitNeeded, is(false));
        assertThat(task01.commitNeeded, is(false));

        verify(consumer).commitSync(offsets);
    }

    @Test
    public void shouldCommitProvidedTasksIfNeeded() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);
        final StateMachineTask task03 = new StateMachineTask(taskId03, taskId03Partitions, false, stateManager);
        final StateMachineTask task04 = new StateMachineTask(taskId04, taskId04Partitions, false, stateManager);
        final StateMachineTask task05 = new StateMachineTask(taskId05, taskId05Partitions, false, stateManager);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
        );
        final Map<TaskId, Set<TopicPartition>> assignmentStandby = mkMap(
            mkEntry(taskId03, taskId03Partitions),
            mkEntry(taskId04, taskId04Partitions),
            mkEntry(taskId05, taskId05Partitions)
        );

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(assignmentActive)))
            .thenReturn(Arrays.asList(task00, task01, task02));
        when(standbyTaskCreator.createTasks(assignmentStandby))
            .thenReturn(Arrays.asList(task03, task04, task05));

        taskManager.handleAssignment(assignmentActive, assignmentStandby);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();
        task01.setCommitNeeded();
        task03.setCommitNeeded();
        task04.setCommitNeeded();

        assertThat(taskManager.commit(mkSet(task00, task02, task03, task05)), equalTo(2));
        assertThat(task00.commitNeeded, is(false));
        assertThat(task01.commitNeeded, is(true));
        assertThat(task02.commitNeeded, is(false));
        assertThat(task03.commitNeeded, is(false));
        assertThat(task04.commitNeeded, is(true));
        assertThat(task05.commitNeeded, is(false));
    }

    @Test
    public void shouldNotCommitOffsetsIfOnlyStandbyTasksAssigned() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, false, stateManager);

        when(consumer.assignment()).thenReturn(assignment);
        when(standbyTaskCreator.createTasks(taskId00Assignment)).thenReturn(singletonList(task00));

        taskManager.handleAssignment(Collections.emptyMap(), taskId00Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();

        assertThat(taskManager.commitAll(), equalTo(1));
        assertThat(task00.commitNeeded, is(false));
    }

    @Test
    public void shouldNotCommitActiveAndStandbyTasksWhileRebalanceInProgress() throws Exception {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager);

        makeTaskFolders(taskId00.toString(), taskId01.toString());
        expectDirectoryNotEmpty(taskId00, taskId01);
        expectLockObtainedFor(taskId00, taskId01);
        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment)))
            .thenReturn(singletonList(task00));
        when(standbyTaskCreator.createTasks(taskId01Assignment))
            .thenReturn(singletonList(task01));

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();
        task01.setCommitNeeded();

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
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        task01.setCommittableOffsetsAndMetadata(offsets);
        task01.setCommitNeeded();
        taskManager.addTask(task01);

        taskManager.commitAll();

        verify(consumer).commitSync(offsets);
    }

    @Test
    public void shouldCommitViaProducerIfEosAlphaEnabled() {
        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.streamsProducerForTask(any(TaskId.class)))
            .thenReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT02 = singletonMap(t1p2, new OffsetAndMetadata(1L, null));

        shouldCommitViaProducerIfEosEnabled(ProcessingMode.EXACTLY_ONCE_ALPHA, offsetsT01, offsetsT02);

        verify(producer).commitTransaction(offsetsT01, new ConsumerGroupMetadata("appId"));
        verify(producer).commitTransaction(offsetsT02, new ConsumerGroupMetadata("appId"));
        verifyNoMoreInteractions(producer);
    }

    @Test
    public void shouldCommitViaProducerIfEosV2Enabled() {
        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.threadProducer()).thenReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT02 = singletonMap(t1p2, new OffsetAndMetadata(1L, null));
        final Map<TopicPartition, OffsetAndMetadata> allOffsets = new HashMap<>();
        allOffsets.putAll(offsetsT01);
        allOffsets.putAll(offsetsT02);

        shouldCommitViaProducerIfEosEnabled(ProcessingMode.EXACTLY_ONCE_V2, offsetsT01, offsetsT02);

        verify(producer).commitTransaction(allOffsets, new ConsumerGroupMetadata("appId"));
        verifyNoMoreInteractions(producer);
    }

    private void shouldCommitViaProducerIfEosEnabled(final ProcessingMode processingMode,
                                                     final Map<TopicPartition, OffsetAndMetadata> offsetsT01,
                                                     final Map<TopicPartition, OffsetAndMetadata> offsetsT02) {
        final TaskManager taskManager = setUpTaskManager(processingMode, false);

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
    }

    @Test
    public void shouldPropagateExceptionFromActiveCommit() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
                throw new RuntimeException("opsh.");
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();

        final RuntimeException thrown =
            assertThrows(RuntimeException.class, () -> taskManager.commitAll());
        assertThat(thrown.getMessage(), equalTo("opsh."));
    }

    @Test
    public void shouldPropagateExceptionFromStandbyCommit() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
                throw new RuntimeException("opsh.");
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(standbyTaskCreator.createTasks(taskId01Assignment)).thenReturn(singletonList(task01));

        taskManager.handleAssignment(emptyMap(), taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task01.state(), is(Task.State.RUNNING));

        task01.setCommitNeeded();

        final RuntimeException thrown =
            assertThrows(RuntimeException.class, () -> taskManager.commitAll());
        assertThat(thrown.getMessage(), equalTo("opsh."));
    }

    @Test
    public void shouldSendPurgeData() {
        when(adminClient.deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(5L))))
            .thenReturn(new DeleteRecordsResult(singletonMap(t1p1, completedFuture())));
        when(adminClient.deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(17L))))
            .thenReturn(new DeleteRecordsResult(singletonMap(t1p1, completedFuture())));

        final InOrder inOrder = inOrder(adminClient);

        final Map<TopicPartition, Long> purgableOffsets = new HashMap<>();
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public Map<TopicPartition, Long> purgeableOffsets() {
                return purgableOffsets;
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        purgableOffsets.put(t1p1, 5L);
        taskManager.maybePurgeCommittedRecords();

        purgableOffsets.put(t1p1, 17L);
        taskManager.maybePurgeCommittedRecords();

        inOrder.verify(adminClient).deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(5L)));
        inOrder.verify(adminClient).deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(17L)));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldNotSendPurgeDataIfPreviousNotDone() {
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        when(adminClient.deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(5L))))
            .thenReturn(new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords)));

        final Map<TopicPartition, Long> purgableOffsets = new HashMap<>();
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public Map<TopicPartition, Long> purgeableOffsets() {
                return purgableOffsets;
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        purgableOffsets.put(t1p1, 5L);
        taskManager.maybePurgeCommittedRecords();

        // this call should be a no-op.
        // this is verified, as there is no expectation on adminClient for this second call,
        // so it would fail verification if we invoke the admin client again.
        purgableOffsets.put(t1p1, 17L);
        taskManager.maybePurgeCommittedRecords();
    }

    @Test
    public void shouldIgnorePurgeDataErrors() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);

        when(consumer.assignment()).thenReturn(assignment);

        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords));
        futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));
        when(adminClient.deleteRecords(any())).thenReturn(deleteRecordsResult);

        taskManager.addTask(task00);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        task00.setPurgeableOffsets(singletonMap(t1p1, 5L));

        taskManager.maybePurgeCommittedRecords();
        taskManager.maybePurgeCommittedRecords();
    }

    @Test
    public void shouldMaybeCommitAllActiveTasksThatNeedCommit() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets0 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets0);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets1 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        task01.setCommittableOffsetsAndMetadata(offsets1);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets2 = singletonMap(t1p2, new OffsetAndMetadata(2L, null));
        task02.setCommittableOffsetsAndMetadata(offsets2);
        final StateMachineTask task03 = new StateMachineTask(taskId03, taskId03Partitions, true, stateManager);
        final StateMachineTask task04 = new StateMachineTask(taskId10, taskId10Partitions, false, stateManager);

        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(offsets0);
        expectedCommittedOffsets.putAll(offsets1);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions),
            mkEntry(taskId03, taskId03Partitions)
        );

        final Map<TaskId, Set<TopicPartition>> assignmentStandby = mkMap(
            mkEntry(taskId10, taskId10Partitions)
        );

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(assignmentActive)))
            .thenReturn(asList(task00, task01, task02, task03));
        when(standbyTaskCreator.createTasks(assignmentStandby))
            .thenReturn(singletonList(task04));

        taskManager.handleAssignment(assignmentActive, assignmentStandby);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(task02.state(), is(Task.State.RUNNING));
        assertThat(task03.state(), is(Task.State.RUNNING));
        assertThat(task04.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();
        task00.setCommitRequested();

        task01.setCommitNeeded();

        task02.setCommitRequested();

        task03.setCommitNeeded();
        task03.setCommitRequested();

        task04.setCommitNeeded();
        task04.setCommitRequested();

        assertThat(taskManager.maybeCommitActiveTasksPerUserRequested(), equalTo(3));

        verify(consumer).commitSync(expectedCommittedOffsets);
    }

    @Test
    public void shouldProcessActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);

        final Map<TaskId, Set<TopicPartition>> firstAssignment = new HashMap<>();
        firstAssignment.put(taskId00, taskId00Partitions);
        firstAssignment.put(taskId01, taskId01Partitions);

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(firstAssignment)))
            .thenReturn(Arrays.asList(task00, task01));

        taskManager.handleAssignment(firstAssignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        task00.addRecords(
            t1p0,
            Arrays.asList(
                getConsumerRecord(t1p0, 0L),
                getConsumerRecord(t1p0, 1L),
                getConsumerRecord(t1p0, 2L),
                getConsumerRecord(t1p0, 3L),
                getConsumerRecord(t1p0, 4L),
                getConsumerRecord(t1p0, 5L)
            )
        );
        task01.addRecords(
            t1p1,
            Arrays.asList(
                getConsumerRecord(t1p1, 0L),
                getConsumerRecord(t1p1, 1L),
                getConsumerRecord(t1p1, 2L),
                getConsumerRecord(t1p1, 3L),
                getConsumerRecord(t1p1, 4L)
            )
        );

        // check that we should be processing at most max num records
        assertThat(taskManager.process(3, time), is(6));

        // check that if there's no records processable, we would stop early
        assertThat(taskManager.process(3, time), is(5));
        assertThat(taskManager.process(3, time), is(0));
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
    public void shouldPropagateTaskMigratedExceptionsInProcessActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public boolean process(final long wallClockTime) {
                throw new TaskMigratedException("migrated", new RuntimeException("cause"));
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        final TopicPartition partition = taskId00Partitions.iterator().next();
        task00.addRecords(partition, singletonList(getConsumerRecord(partition, 0L)));

        assertThrows(TaskMigratedException.class, () -> taskManager.process(1, time));
    }

    @Test
    public void shouldWrapRuntimeExceptionsInProcessActiveTasksAndSetTaskId() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public boolean process(final long wallClockTime) {
                throw new RuntimeException("oops");
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment)))
            .thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        final TopicPartition partition = taskId00Partitions.iterator().next();
        task00.addRecords(partition, singletonList(getConsumerRecord(partition, 0L)));

        final StreamsException exception = assertThrows(StreamsException.class, () -> taskManager.process(1, time));
        assertThat(exception.taskId().isPresent(), is(true));
        assertThat(exception.taskId().get(), is(taskId00));
        assertThat(exception.getCause().getMessage(), is("oops"));
    }

    @Test
    public void shouldPropagateTaskMigratedExceptionsInPunctuateActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public boolean maybePunctuateStreamTime() {
                throw new TaskMigratedException("migrated", new RuntimeException("cause"));
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        assertThrows(TaskMigratedException.class, () -> taskManager.punctuate());
    }

    @Test
    public void shouldPropagateKafkaExceptionsInPunctuateActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public boolean maybePunctuateStreamTime() {
                throw new KafkaException("oops");
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        assertThrows(KafkaException.class, () -> taskManager.punctuate());
    }

    @Test
    public void shouldPunctuateActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public boolean maybePunctuateStreamTime() {
                return true;
            }

            @Override
            public boolean maybePunctuateSystemTime() {
                return true;
            }
        };

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        // one for stream and one for system time
        assertThat(taskManager.punctuate(), equalTo(2));
    }

    @Test
    public void shouldReturnFalseWhenThereAreStillNonRunningTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public Set<TopicPartition> changelogPartitions() {
                return singleton(new TopicPartition("fake", 0));
            }
        };

        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(false));
        assertThat(task00.state(), is(Task.State.RESTORING));
        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldHaveRemainingPartitionsUncleared() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        when(consumer.assignment()).thenReturn(assignment);
        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(task00));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            appender.setClassLoggerToDebug(TaskManager.class);
            taskManager.handleAssignment(taskId00Assignment, emptyMap());
            assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
            assertThat(task00.state(), is(Task.State.RUNNING));

            taskManager.handleRevocation(mkSet(t1p0, new TopicPartition("unknown", 0)));
            assertThat(task00.state(), is(Task.State.SUSPENDED));

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
        final StateMachineTask migratedTask01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("t1 close exception", new RuntimeException());
            }
        };

        final StateMachineTask migratedTask02 = new StateMachineTask(taskId02, taskId02Partitions, false, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("t2 close exception", new RuntimeException());
            }
        };
        taskManager.addTask(migratedTask01);
        taskManager.addTask(migratedTask02);

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
    }

    @Test
    public void shouldThrowRuntimeExceptionWhenEncounteredUnknownExceptionDuringTaskClose() {
        final StateMachineTask migratedTask01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("t1 close exception", new RuntimeException());
            }
        };

        final StateMachineTask migratedTask02 = new StateMachineTask(taskId02, taskId02Partitions, false, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new IllegalStateException("t2 illegal state exception", new RuntimeException());
            }
        };
        taskManager.addTask(migratedTask01);
        taskManager.addTask(migratedTask02);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );
        // Fatal exception thrown first.
        assertThat(thrown.getMessage(), equalTo("Encounter unexpected fatal error for task 0_2"));

        assertThat(thrown.getCause().getMessage(), equalTo("t2 illegal state exception"));
    }

    @Test
    public void shouldThrowSameKafkaExceptionWhenEncounteredDuringTaskClose() {
        final StateMachineTask migratedTask01 = new StateMachineTask(taskId01, taskId01Partitions, false, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("t1 close exception", new RuntimeException());
            }
        };

        final StateMachineTask migratedTask02 = new StateMachineTask(taskId02, taskId02Partitions, false, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new KafkaException("Kaboom for t2!", new RuntimeException());
            }
        };
        taskManager.addTask(migratedTask01);
        taskManager.addTask(migratedTask02);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );

        assertThat(thrown.taskId().isPresent(), is(true));
        assertThat(thrown.taskId().get(), is(taskId02));

        // Expecting the original Kafka exception wrapped in the StreamsException.
        assertThat(thrown.getCause().getMessage(), equalTo("Kaboom for t2!"));
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

    private Map<TaskId, StateMachineTask> handleAssignment(final Map<TaskId, Set<TopicPartition>> runningActiveAssignment,
                                                           final Map<TaskId, Set<TopicPartition>> standbyAssignment,
                                                           final Map<TaskId, Set<TopicPartition>> restoringActiveAssignment) {
        final Set<Task> runningTasks = runningActiveAssignment.entrySet().stream()
                                           .map(t -> new StateMachineTask(t.getKey(), t.getValue(), true, stateManager))
                                           .collect(Collectors.toSet());
        final Set<Task> standbyTasks = standbyAssignment.entrySet().stream()
                                           .map(t -> new StateMachineTask(t.getKey(), t.getValue(), false, stateManager))
                                           .collect(Collectors.toSet());
        final Set<Task> restoringTasks = restoringActiveAssignment.entrySet().stream()
                                           .map(t -> new StateMachineTask(t.getKey(), t.getValue(), true, stateManager))
                                           .collect(Collectors.toSet());
        // give the restoring tasks some uncompleted changelog partitions so they'll stay in restoring
        restoringTasks.forEach(t -> ((StateMachineTask) t).setChangelogOffsets(singletonMap(new TopicPartition("changelog", 0), 0L)));

        // Initially assign only the active tasks we want to complete restoration
        final Map<TaskId, Set<TopicPartition>> allActiveTasksAssignment = new HashMap<>(runningActiveAssignment);
        allActiveTasksAssignment.putAll(restoringActiveAssignment);
        final Set<Task> allActiveTasks = new HashSet<>(runningTasks);
        allActiveTasks.addAll(restoringTasks);

        when(standbyTaskCreator.createTasks(standbyAssignment)).thenReturn(standbyTasks);
        when(activeTaskCreator.createTasks(any(), eq(allActiveTasksAssignment))).thenReturn(allActiveTasks);

        lenient().when(consumer.assignment()).thenReturn(assignment);

        taskManager.handleAssignment(allActiveTasksAssignment, standbyAssignment);
        taskManager.tryToCompleteRestoration(time.milliseconds(), null);

        final Map<TaskId, StateMachineTask> allTasks = new HashMap<>();

        // Just make sure all tasks ended up in the expected state
        for (final Task task : runningTasks) {
            assertThat(task.state(), is(Task.State.RUNNING));
            allTasks.put(task.id(), (StateMachineTask) task);
        }
        for (final Task task : restoringTasks) {
            assertThat(task.state(), is(Task.State.RESTORING));
            allTasks.put(task.id(), (StateMachineTask) task);
        }
        for (final Task task : standbyTasks) {
            assertThat(task.state(), is(Task.State.RUNNING));
            allTasks.put(task.id(), (StateMachineTask) task);
        }
        return allTasks;
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

        assertThat(taskManager.commit(mkSet(task00, task01)), equalTo(0));
        assertThat(task00.timeout, equalTo(time.milliseconds()));
        assertNull(task01.timeout);

        assertThat(taskManager.commit(mkSet(task00, task01)), equalTo(1));
        assertNull(task00.timeout);
        assertNull(task01.timeout);

        verify(consumer, times(2)).commitSync(any(Map.class));
    }

    @Test
    public void shouldNotFailForTimeoutExceptionOnCommitWithEosAlpha() {
        final Tasks tasks = mock(Tasks.class);
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_ALPHA, tasks, false);

        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.streamsProducerForTask(any(TaskId.class))).thenReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));

        doThrow(new TimeoutException("KABOOM!"))
            .doNothing()
            .doNothing()
            .doNothing()
            .when(producer).commitTransaction(offsetsT00, null);
        doNothing()
            .doNothing()
            .when(producer).commitTransaction(offsetsT01, null);

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        task00.setCommittableOffsetsAndMetadata(offsetsT00);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        task01.setCommittableOffsetsAndMetadata(offsetsT01);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true, stateManager);
        when(tasks.allTasks()).thenReturn(mkSet(task00, task01, task02));

        task00.setCommitNeeded();
        task01.setCommitNeeded();

        final TaskCorruptedException exception = assertThrows(
            TaskCorruptedException.class,
            () -> taskManager.commit(mkSet(task00, task01, task02))
        );
        assertThat(
            exception.corruptedTasks(),
            equalTo(Collections.singleton(taskId00))
        );

        verify(consumer, times(2)).groupMetadata();
    }

    @Test
    public void shouldThrowTaskCorruptedExceptionForTimeoutExceptionOnCommitWithEosV2() {
        final TaskManager taskManager = setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2, false);

        final StreamsProducer producer = mock(StreamsProducer.class);
        when(activeTaskCreator.threadProducer()).thenReturn(producer);

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
            () -> taskManager.commit(mkSet(task00, task01, task02))
        );
        assertThat(
            exception.corruptedTasks(),
            equalTo(mkSet(taskId00, taskId01))
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
    public void shouldSuspendAllTasksButSkipCommitIfSuspendingFailsDuringRevocation() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("KABOOM!");
            }
        };
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>(taskId00Assignment);
        assignment.putAll(taskId01Assignment);
        when(activeTaskCreator.createTasks(any(), eq(assignment))).thenReturn(asList(task00, task01));

        taskManager.handleAssignment(assignment, Collections.emptyMap());

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.handleRevocation(asList(t1p0, t1p1)));

        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
        assertThat(task00.state(), is(Task.State.SUSPENDED));
        assertThat(task01.state(), is(Task.State.SUSPENDED));
        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldConvertActiveTaskToStandbyTask() {
        final StreamTask activeTask = mock(StreamTask.class);
        when(activeTask.id()).thenReturn(taskId00);
        when(activeTask.inputPartitions()).thenReturn(taskId00Partitions);
        when(activeTask.isActive()).thenReturn(true);

        final StandbyTask standbyTask = mock(StandbyTask.class);
        when(standbyTask.id()).thenReturn(taskId00);

        when(activeTaskCreator.createTasks(any(), eq(taskId00Assignment))).thenReturn(singletonList(activeTask));
        when(standbyTaskCreator.createStandbyTaskFromActive(any(), eq(taskId00Partitions))).thenReturn(standbyTask);

        taskManager.handleAssignment(taskId00Assignment, Collections.emptyMap());
        taskManager.handleAssignment(Collections.emptyMap(), taskId00Assignment);

        verify(activeTaskCreator).closeAndRemoveTaskProducerIfNeeded(taskId00);
        verify(activeTaskCreator).createTasks(any(), eq(emptyMap()));
        verify(standbyTaskCreator, times(2)).createTasks(Collections.emptyMap());
        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldConvertStandbyTaskToActiveTask() {
        final StandbyTask standbyTask = mock(StandbyTask.class);
        when(standbyTask.id()).thenReturn(taskId00);
        when(standbyTask.isActive()).thenReturn(false);
        when(standbyTask.prepareCommit()).thenReturn(Collections.emptyMap());

        final StreamTask activeTask = mock(StreamTask.class);
        when(activeTask.id()).thenReturn(taskId00);
        when(activeTask.inputPartitions()).thenReturn(taskId00Partitions);
        when(standbyTaskCreator.createTasks(taskId00Assignment)).thenReturn(singletonList(standbyTask));
        when(activeTaskCreator.createActiveTaskFromStandby(eq(standbyTask), eq(taskId00Partitions), any()))
            .thenReturn(activeTask);

        taskManager.handleAssignment(Collections.emptyMap(), taskId00Assignment);
        taskManager.handleAssignment(taskId00Assignment, Collections.emptyMap());

        verify(activeTaskCreator, times(2)).createTasks(any(), eq(emptyMap()));
        verify(standbyTaskCreator).createTasks(Collections.emptyMap());
        verifyNoInteractions(consumer);
    }

    @Test
    public void shouldListNotPausedTasks() {
        handleAssignment(taskId00Assignment, taskId01Assignment, emptyMap());

        assertEquals(taskManager.notPausedTasks().size(), 2);

        topologyMetadata.pauseTopology(UNNAMED_TOPOLOGY);

        assertEquals(taskManager.notPausedTasks().size(), 0);
    }

    private static KafkaFutureImpl<DeletedRecords> completedFuture() {
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        futureDeletedRecords.complete(null);
        return futureDeletedRecords;
    }

    private void makeTaskFolders(final String... names) throws Exception {
        final ArrayList<TaskDirectory> taskFolders = new ArrayList<>(names.length);
        for (int i = 0; i < names.length; ++i) {
            taskFolders.add(new TaskDirectory(testFolder.newFolder(names[i]), null));
        }
        when(stateDirectory.listNonEmptyTaskDirectories()).thenReturn(taskFolders);
    }

    private void writeCheckpointFile(final TaskId task, final Map<TopicPartition, Long> offsets) throws Exception {
        final File checkpointFile = getCheckpointFile(task);
        Files.createFile(checkpointFile.toPath());
        new OffsetCheckpoint(checkpointFile).write(offsets);
        lenient().when(stateDirectory.checkpointFileFor(task)).thenReturn(checkpointFile);
        expectDirectoryNotEmpty(task);
    }

    private File getCheckpointFile(final TaskId task) {
        return new File(new File(testFolder.getRoot(), task.toString()), StateManagerUtil.CHECKPOINT_FILE_NAME);
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
        public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
            commitPrepared = true;

            if (commitNeeded) {
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
        public StateStore getStore(final String name) {
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
