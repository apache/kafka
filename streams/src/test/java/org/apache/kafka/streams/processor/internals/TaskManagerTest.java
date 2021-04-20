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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamThread.ProcessingMode;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.union;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.resetToStrict;
import static org.easymock.EasyMock.verify;
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

@RunWith(EasyMockRunner.class)
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
    private final TopicPartition t1p1changelog = new TopicPartition("changelog", 1);
    private final Set<TopicPartition> taskId01Partitions = mkSet(t1p1);
    private final Set<TopicPartition> taskId01ChangelogPartitions = mkSet(t1p1changelog);
    private final Map<TaskId, Set<TopicPartition>> taskId01Assignment = singletonMap(taskId01, taskId01Partitions);

    private final TaskId taskId02 = new TaskId(0, 2);
    private final TopicPartition t1p2 = new TopicPartition(topic1, 2);
    private final Set<TopicPartition> taskId02Partitions = mkSet(t1p2);

    private final TaskId taskId03 = new TaskId(0, 3);
    private final TopicPartition t1p3 = new TopicPartition(topic1, 3);
    private final Set<TopicPartition> taskId03Partitions = mkSet(t1p3);

    private final TaskId taskId04 = new TaskId(0, 4);
    private final TopicPartition t1p4 = new TopicPartition(topic1, 4);
    private final Set<TopicPartition> taskId04Partitions = mkSet(t1p4);

    private final TaskId taskId05 = new TaskId(0, 5);
    private final TopicPartition t1p5 = new TopicPartition(topic1, 5);
    private final Set<TopicPartition> taskId05Partitions = mkSet(t1p5);

    private final TaskId taskId10 = new TaskId(1, 0);
    private final TopicPartition t2p0 = new TopicPartition(topic2, 0);
    private final Set<TopicPartition> taskId10Partitions = mkSet(t2p0);

    @Mock(type = MockType.STRICT)
    private InternalTopologyBuilder topologyBuilder;
    @Mock(type = MockType.DEFAULT)
    private StateDirectory stateDirectory;
    @Mock(type = MockType.NICE)
    private ChangelogReader changeLogReader;
    @Mock(type = MockType.STRICT)
    private Consumer<byte[], byte[]> consumer;
    @Mock(type = MockType.STRICT)
    private ActiveTaskCreator activeTaskCreator;
    @Mock(type = MockType.NICE)
    private StandbyTaskCreator standbyTaskCreator;
    @Mock(type = MockType.NICE)
    private Admin adminClient;

    private TaskManager taskManager;
    private final Time time = new MockTime();

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        setUpTaskManager(StreamThread.ProcessingMode.AT_LEAST_ONCE);
    }

    private void setUpTaskManager(final StreamThread.ProcessingMode processingMode) {
        taskManager = new TaskManager(
            time,
            changeLogReader,
            UUID.randomUUID(),
            "taskManagerTest",
            new StreamsMetricsImpl(new Metrics(), "clientId", StreamsConfig.METRICS_LATEST, time),
            activeTaskCreator,
            standbyTaskCreator,
            topologyBuilder,
            adminClient,
            stateDirectory,
            processingMode
        );
        taskManager.setMainConsumer(consumer);
    }

    @Test
    public void shouldIdempotentlyUpdateSubscriptionFromActiveAssignment() {
        final TopicPartition newTopicPartition = new TopicPartition("topic2", 1);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(mkEntry(taskId01, mkSet(t1p1, newTopicPartition)));

        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(emptyList());
        topologyBuilder.addSubscribedTopicsFromAssignment(eq(asList(t1p1, newTopicPartition)), anyString());
        expectLastCall();
        replay(activeTaskCreator, topologyBuilder);

        taskManager.handleAssignment(assignment, emptyMap());

        verify(activeTaskCreator, topologyBuilder);
    }

    @Test
    public void shouldNotLockAnythingIfStateDirIsEmpty() {
        expect(stateDirectory.listNonEmptyTaskDirectories()).andReturn(new File[0]).once();

        replay(stateDirectory);
        taskManager.handleRebalanceStart(singleton("topic"));

        verify(stateDirectory);
        assertTrue(taskManager.lockedTaskDirectories().isEmpty());
    }

    @Test
    public void shouldTryToLockValidTaskDirsAtRebalanceStart() throws Exception {
        expectLockObtainedFor(taskId01);
        expectLockFailedFor(taskId10);

        makeTaskFolders(
            taskId01.toString(),
            taskId10.toString(),
            "dummy"
        );
        replay(stateDirectory);
        taskManager.handleRebalanceStart(singleton("topic"));

        verify(stateDirectory);
        assertThat(taskManager.lockedTaskDirectories(), is(singleton(taskId01)));
    }

    @Test
    public void shouldReleaseLockForUnassignedTasksAfterRebalance() throws Exception {
        expectLockObtainedFor(taskId00, taskId01, taskId02);
        expectUnlockFor(taskId02);

        makeTaskFolders(
            taskId00.toString(),  // active task
            taskId01.toString(),  // standby task
            taskId02.toString()   // unassigned but able to lock
        );
        replay(stateDirectory);
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.lockedTaskDirectories(), is(mkSet(taskId00, taskId01, taskId02)));

        handleAssignment(taskId00Assignment, taskId01Assignment, emptyMap());
        reset(consumer);
        expectConsumerAssignmentPaused(consumer);
        replay(consumer);

        taskManager.handleRebalanceComplete();
        assertThat(taskManager.lockedTaskDirectories(), is(mkSet(taskId00, taskId01)));
        verify(stateDirectory);
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
        makeTaskFolders(taskId00.toString());
        replay(stateDirectory);

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
        makeTaskFolders(taskId00.toString());
        replay(stateDirectory);

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

        replay(stateDirectory);
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
        replay(stateDirectory);

        taskManager.handleRebalanceStart(singleton("topic"));
        final StateMachineTask uninitializedTask = new StateMachineTask(taskId00, taskId00Partitions, true);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andStubReturn(singleton(uninitializedTask));
        replay(activeTaskCreator);
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
        replay(stateDirectory);

        final StateMachineTask closedTask = new StateMachineTask(taskId00, taskId00Partitions, true);

        taskManager.handleRebalanceStart(singleton("topic"));
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andStubReturn(singleton(closedTask));
        replay(activeTaskCreator);
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
        replay(stateDirectory);
        taskManager.handleRebalanceStart(singleton("topic"));
        assertTrue(taskManager.lockedTaskDirectories().isEmpty());

        assertTrue(taskManager.getTaskOffsetSums().isEmpty());
    }

    @Test
    public void shouldNotReportOffsetSumsAndReleaseLockForUnassignedTaskWithoutCheckpoint() throws Exception {
        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        expect(stateDirectory.checkpointFileFor(taskId00)).andReturn(getCheckpointFile(taskId00));
        replay(stateDirectory);
        taskManager.handleRebalanceStart(singleton("topic"));

        assertTrue(taskManager.getTaskOffsetSums().isEmpty());
        verify(stateDirectory);
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
        replay(stateDirectory);
        taskManager.handleRebalanceStart(singleton("topic"));

        assertThat(taskManager.getTaskOffsetSums(), is(expectedOffsetSums));
    }

    @Test
    public void shouldCloseActiveUnassignedSuspendedTasksWhenClosingRevokedTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        // first `handleAssignment`
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andStubReturn(singletonList(task00));
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andStubReturn(emptyList());
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();
        expect(standbyTaskCreator.createTasks(anyObject())).andStubReturn(emptyList());
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        // `handleRevocation`
        consumer.commitSync(offsets);
        expectLastCall();

        // second `handleAssignment`
        consumer.commitSync(offsets);
        expectLastCall();

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));

        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
    }

    @Test
    public void shouldCloseDirtyActiveUnassignedTasksWhenErrorCleanClosingTask() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public void closeClean() {
                throw new RuntimeException("KABOOM!");
            }
        };

        // first `handleAssignment`
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andStubReturn(singletonList(task00));
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andStubReturn(emptyList());
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();
        expect(standbyTaskCreator.createTasks(anyObject())).andStubReturn(emptyList());
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.handleRevocation(taskId00Partitions);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(
            thrown.getMessage(),
            is("Unexpected failure to close 1 task(s) [[0_0]]. First unexpected exception (for task 0_0) follows.")
        );
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldCloseActiveTasksWhenHandlingLostTasks() throws Exception {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        // `handleAssignment`
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andStubReturn(singletonList(task00));
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment))).andStubReturn(singletonList(task01));
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        makeTaskFolders(taskId00.toString(), taskId01.toString());
        expectLockObtainedFor(taskId00, taskId01);

        // The second attempt will return empty tasks.
        makeTaskFolders();
        expectLockObtainedFor();
        replay(stateDirectory);

        taskManager.handleRebalanceStart(emptySet());
        assertThat(taskManager.lockedTaskDirectories(), Matchers.is(mkSet(taskId00, taskId01)));

        // `handleLostAll`
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

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
    }

    @Test
    public void shouldReInitializeThreadProducerOnHandleLostAllIfEosV2Enabled() {
        activeTaskCreator.reInitializeThreadProducer();
        expectLastCall();
        replay(activeTaskCreator);

        setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2);

        taskManager.handleLostAll();

        verify(activeTaskCreator);
    }

    @Test
    public void shouldThrowWhenHandlingClosingTasksOnProducerCloseError() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        // `handleAssignment`
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andStubReturn(singletonList(task00));
        expect(standbyTaskCreator.createTasks(anyObject())).andStubReturn(emptyList());
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        // `handleAssignment`
        consumer.commitSync(offsets);
        expectLastCall();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall().andThrow(new RuntimeException("KABOOM!"));

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

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
            is("Unexpected failure to close 1 task(s) [[0_0]]. First unexpected exception (for task 0_0) follows.")
        );
        assertThat(thrown.getCause(), instanceOf(RuntimeException.class));
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldReviveCorruptTasks() {
        final ProcessorStateManager stateManager = EasyMock.createStrictMock(ProcessorStateManager.class);
        stateManager.markChangelogAsCorrupted(taskId00Partitions);
        EasyMock.expectLastCall().once();
        replay(stateManager);

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
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andStubReturn(singletonList(task00));
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();
        expect(consumer.assignment()).andReturn(taskId00Partitions);
        replay(activeTaskCreator, topologyBuilder, consumer, changeLogReader);

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

        verify(stateManager);
        verify(consumer);
    }

    @Test
    public void shouldReviveCorruptTasksEvenIfTheyCannotCloseClean() {
        final ProcessorStateManager stateManager = EasyMock.createStrictMock(ProcessorStateManager.class);
        stateManager.markChangelogAsCorrupted(taskId00Partitions);
        replay(stateManager);

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("oops");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andStubReturn(singletonList(task00));
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();
        expect(consumer.assignment()).andReturn(taskId00Partitions);
        replay(activeTaskCreator, topologyBuilder, consumer, changeLogReader);

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

        verify(stateManager);
        verify(consumer);
    }

    @Test
    public void shouldCommitNonCorruptedTasksOnTaskCorruptedException() {
        final ProcessorStateManager stateManager = EasyMock.createStrictMock(ProcessorStateManager.class);
        stateManager.markChangelogAsCorrupted(taskId00Partitions);
        replay(stateManager);

        final StateMachineTask corruptedTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask nonCorruptedTask = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>(taskId00Assignment);
        assignment.putAll(taskId01Assignment);

        // `handleAssignment`
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment)))
            .andStubReturn(asList(corruptedTask, nonCorruptedTask));
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();
        expectRestoreToBeCompleted(consumer, changeLogReader);
        consumer.commitSync(eq(emptyMap()));
        expect(consumer.assignment()).andReturn(taskId00Partitions);
        replay(activeTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), tp -> assertThat(tp, is(empty()))), is(true));

        assertThat(nonCorruptedTask.state(), is(Task.State.RUNNING));
        nonCorruptedTask.setCommitNeeded();

        corruptedTask.setChangelogOffsets(singletonMap(t1p0, 0L));
        taskManager.handleCorruption(singleton(taskId00));

        assertTrue(nonCorruptedTask.commitPrepared);
        assertThat(nonCorruptedTask.partitionsForOffsetReset, equalTo(Collections.emptySet()));
        assertThat(corruptedTask.partitionsForOffsetReset, equalTo(taskId00Partitions));

        verify(consumer);
    }

    @Test
    public void shouldNotCommitNonRunningNonCorruptedTasks() {
        final ProcessorStateManager stateManager = EasyMock.createStrictMock(ProcessorStateManager.class);
        stateManager.markChangelogAsCorrupted(taskId00Partitions);
        replay(stateManager);

        final StateMachineTask corruptedTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);
        final StateMachineTask nonRunningNonCorruptedTask = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);

        nonRunningNonCorruptedTask.setCommitNeeded();

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>(taskId00Assignment);
        assignment.putAll(taskId01Assignment);

        // `handleAssignment`
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment)))
            .andStubReturn(asList(corruptedTask, nonRunningNonCorruptedTask));
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();
        expect(consumer.assignment()).andReturn(taskId00Partitions);
        replay(activeTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        corruptedTask.setChangelogOffsets(singletonMap(t1p0, 0L));
        taskManager.handleCorruption(singleton(taskId00));

        assertThat(nonRunningNonCorruptedTask.state(), is(Task.State.CREATED));
        assertThat(nonRunningNonCorruptedTask.partitionsForOffsetReset, equalTo(Collections.emptySet()));
        assertThat(corruptedTask.partitionsForOffsetReset, equalTo(taskId00Partitions));

        verify(activeTaskCreator);
        assertFalse(nonRunningNonCorruptedTask.commitPrepared);
        verify(consumer);
    }

    @Test
    public void shouldCleanAndReviveCorruptedStandbyTasksBeforeCommittingNonCorruptedTasks() {
        final ProcessorStateManager stateManager = EasyMock.createStrictMock(ProcessorStateManager.class);
        stateManager.markChangelogAsCorrupted(taskId00Partitions);
        replay(stateManager);

        final StateMachineTask corruptedStandby = new StateMachineTask(taskId00, taskId00Partitions, false, stateManager);
        final StateMachineTask runningNonCorruptedActive = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
                throw new TaskMigratedException("You dropped out of the group!", new RuntimeException());
            }
        };

        // handleAssignment
        expect(standbyTaskCreator.createTasks(eq(taskId00Assignment))).andStubReturn(singleton(corruptedStandby));
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId01Assignment))).andStubReturn(singleton(runningNonCorruptedActive));
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        expectRestoreToBeCompleted(consumer, changeLogReader);

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

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
        verify(consumer);
    }

    @Test
    public void shouldNotAttemptToCommitInHandleCorruptedDuringARebalance() {
        final ProcessorStateManager stateManager = EasyMock.createNiceMock(ProcessorStateManager.class);
        expect(stateDirectory.listNonEmptyTaskDirectories()).andStubReturn(new File[0]);

        final StateMachineTask corruptedActive = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);

        // make sure this will attempt to be committed and throw
        final StateMachineTask uncorruptedActive = new StateMachineTask(taskId01, taskId01Partitions, true, stateManager);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        uncorruptedActive.setCommitNeeded();

        // handleAssignment
        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.putAll(taskId00Assignment);
        assignment.putAll(taskId01Assignment);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(asList(corruptedActive, uncorruptedActive));
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();
        topologyBuilder.addSubscribedTopicsFromMetadata(eq(singleton(topic1)), anyObject());
        expectLastCall().anyTimes();

        expectRestoreToBeCompleted(consumer, changeLogReader);

        expect(consumer.assignment()).andStubReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions));

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader, stateDirectory, stateManager);

        uncorruptedActive.setCommittableOffsetsAndMetadata(offsets);

        taskManager.handleAssignment(assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(uncorruptedActive.state(), is(Task.State.RUNNING));

        assertThat(uncorruptedActive.commitPrepared, is(false));
        assertThat(uncorruptedActive.commitNeeded, is(true));
        assertThat(uncorruptedActive.commitCompleted, is(false));

        taskManager.handleRebalanceStart(singleton(topic1));
        assertThat(taskManager.isRebalanceInProgress(), is(true));
        taskManager.handleCorruption(singleton(taskId00));

        assertThat(uncorruptedActive.commitPrepared, is(false));
        assertThat(uncorruptedActive.commitNeeded, is(true));
        assertThat(uncorruptedActive.commitCompleted, is(false));

        assertThat(uncorruptedActive.state(), is(State.RUNNING));
        verify(consumer);
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitWithALOS() {
        final ProcessorStateManager stateManager = EasyMock.createStrictMock(ProcessorStateManager.class);
        stateManager.markChangelogAsCorrupted(taskId00Partitions);
        replay(stateManager);

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
        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.putAll(taskId00Assignment);
        assignment.putAll(taskId01Assignment);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(asList(corruptedActive, uncorruptedActive));
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        expectRestoreToBeCompleted(consumer, changeLogReader);

        consumer.commitSync(offsets);
        expectLastCall().andThrow(new TimeoutException());

        expect(consumer.assignment()).andStubReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions));

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());
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
        verify(consumer);
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitDuringHandleCorruptedWithEOS() {
        setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2);
        final StreamsProducer producer = mock(StreamsProducer.class);
        expect(activeTaskCreator.threadProducer()).andStubReturn(producer);
        final ProcessorStateManager stateManager = EasyMock.createMock(ProcessorStateManager.class);

        final AtomicBoolean corruptedTaskChangelogMarkedAsCorrupted = new AtomicBoolean(false);
        final StateMachineTask corruptedActiveTask = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
                super.markChangelogAsCorrupted(partitions);
                corruptedTaskChangelogMarkedAsCorrupted.set(true);
            }
        };
        stateManager.markChangelogAsCorrupted(taskId00ChangelogPartitions);

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
        stateManager.markChangelogAsCorrupted(taskId01ChangelogPartitions);

        // handleAssignment
        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.putAll(taskId00Assignment);
        assignment.putAll(taskId01Assignment);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(asList(corruptedActiveTask, uncorruptedActiveTask));
        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        expectRestoreToBeCompleted(consumer, changeLogReader);

        final ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata("appId");
        expect(consumer.groupMetadata()).andReturn(groupMetadata);
        producer.commitTransaction(offsets, groupMetadata);
        expectLastCall().andThrow(new TimeoutException());

        expect(consumer.assignment()).andStubReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions));

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader, stateManager, producer);

        taskManager.handleAssignment(assignment, emptyMap());
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
        verify(consumer);
    }

    @Test
    public void shouldCloseAndReviveUncorruptedTasksWhenTimeoutExceptionThrownFromCommitDuringRevocationWithALOS() {
        final StateMachineTask revokedActiveTask = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        revokedActiveTask.setCommittableOffsetsAndMetadata(offsets00);
        revokedActiveTask.setCommitNeeded();

        final StateMachineTask unrevokedActiveTaskWithCommitNeeded = new StateMachineTask(taskId01, taskId01Partitions, true) {
            @Override
            public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
                fail("Should not try to mark changelogs as corrupted for uncorrupted task");
            }
        };
        final Map<TopicPartition, OffsetAndMetadata> offsets01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        unrevokedActiveTaskWithCommitNeeded.setCommittableOffsetsAndMetadata(offsets01);
        unrevokedActiveTaskWithCommitNeeded.setCommitNeeded();

        final StateMachineTask unrevokedActiveTaskWithoutCommitNeeded = new StateMachineTask(taskId02, taskId02Partitions, true);

        final Map<TopicPartition, OffsetAndMetadata> expectedCommittedOffsets = new HashMap<>();
        expectedCommittedOffsets.putAll(offsets00);
        expectedCommittedOffsets.putAll(offsets01);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
        );

        expectRestoreToBeCompleted(consumer, changeLogReader);

        expect(activeTaskCreator.createTasks(anyObject(), eq(assignmentActive))).andReturn(asList(revokedActiveTask, unrevokedActiveTaskWithCommitNeeded, unrevokedActiveTaskWithoutCommitNeeded));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();
        consumer.commitSync(expectedCommittedOffsets);
        expectLastCall().andThrow(new TimeoutException());
        expect(consumer.assignment()).andStubReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions, taskId02Partitions));

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

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
        setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2);
        final StreamsProducer producer = mock(StreamsProducer.class);
        expect(activeTaskCreator.threadProducer()).andStubReturn(producer);
        final ProcessorStateManager stateManager = EasyMock.createMock(ProcessorStateManager.class);

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

        stateManager.markChangelogAsCorrupted(taskId00ChangelogPartitions);
        stateManager.markChangelogAsCorrupted(taskId01ChangelogPartitions);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
            );

        expectRestoreToBeCompleted(consumer, changeLogReader);

        expect(activeTaskCreator.createTasks(anyObject(), eq(assignmentActive))).andReturn(asList(revokedActiveTask, unrevokedActiveTask, unrevokedActiveTaskWithoutCommitNeeded));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();

        final ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata("appId");
        expect(consumer.groupMetadata()).andReturn(groupMetadata);
        producer.commitTransaction(expectedCommittedOffsets, groupMetadata);
        expectLastCall().andThrow(new TimeoutException());

        expect(consumer.assignment()).andStubReturn(union(HashSet::new, taskId00Partitions, taskId01Partitions, taskId02Partitions));

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader, producer, stateManager);

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
    }

    @Test
    public void shouldCloseStandbyUnassignedTasksWhenCreatingNewTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(standbyTaskCreator.createTasks(eq(taskId00Assignment))).andStubReturn(singletonList(task00));
        consumer.commitSync(Collections.emptyMap());
        expectLastCall();
        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

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
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        // expect these calls twice (because we're going to tryToCompleteRestoration twice)
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment))).andReturn(singletonList(task01));
        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        verify(activeTaskCreator);
    }

    @Test
    public void shouldUpdateInputPartitionsAfterRebalance() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        // expect these calls twice (because we're going to tryToCompleteRestoration twice)
        expectRestoreToBeCompleted(consumer, changeLogReader, false);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));
        replay(activeTaskCreator, consumer, changeLogReader);


        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        final Set<TopicPartition> newPartitionsSet = mkSet(t1p1);
        final Map<TaskId, Set<TopicPartition>> taskIdSetMap = singletonMap(taskId00, newPartitionsSet);
        taskManager.handleAssignment(taskIdSetMap, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertEquals(newPartitionsSet, task00.inputPartitions());
        verify(activeTaskCreator, consumer, changeLogReader);
    }

    @Test
    public void shouldAddNewActiveTasks() {
        final Map<TaskId, Set<TopicPartition>> assignment = taskId00Assignment;
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(consumer.assignment()).andReturn(emptySet());
        consumer.resume(eq(emptySet()));
        expectLastCall();
        changeLogReader.enforceRestoreActive();
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(singletonList(task00));
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andStubReturn(emptyList());
        replay(consumer, activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration(time.milliseconds(), noOpResetter -> { });

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator);
    }

    @Test
    public void shouldNotCompleteRestorationIfTasksCannotInitialize() {
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public void initializeIfNeeded() {
                throw new LockException("can't lock");
            }
        };
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, true) {
            @Override
            public void initializeIfNeeded() {
                throw new TimeoutException("timed out");
            }
        };

        consumer.commitSync(Collections.emptyMap());
        expectLastCall();
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(consumer.assignment()).andReturn(emptySet());
        consumer.resume(eq(emptySet()));
        expectLastCall();
        changeLogReader.enforceRestoreActive();
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(asList(task00, task01));
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andStubReturn(emptyList());
        replay(consumer, activeTaskCreator, standbyTaskCreator, changeLogReader);

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
        verify(activeTaskCreator);
    }

    @Test
    public void shouldNotCompleteRestorationIfTaskCannotCompleteRestoration() {
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public void completeRestoration(final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
                throw new TimeoutException("timeout!");
            }
        };

        consumer.commitSync(Collections.emptyMap());
        expectLastCall();
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(consumer.assignment()).andReturn(emptySet());
        consumer.resume(eq(emptySet()));
        expectLastCall();
        changeLogReader.enforceRestoreActive();
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(singletonList(task00));
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andStubReturn(emptyList());
        replay(consumer, activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(false));

        assertThat(task00.state(), is(Task.State.RESTORING));
        assertThat(
            taskManager.activeTaskMap(),
            Matchers.equalTo(mkMap(mkEntry(taskId00, task00)))
        );
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator);
    }

    @Test
    public void shouldSuspendActiveTasksDuringRevocation() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));
        consumer.commitSync(offsets);
        expectLastCall();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));
    }

    @Test
    public void shouldCommitAllActiveTasksThatNeedCommittingOnHandleRevocationWithEosV2() {
        final StreamsProducer producer = mock(StreamsProducer.class);
        setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2);

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets00);
        task00.setCommitNeeded();

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        task01.setCommittableOffsetsAndMetadata(offsets01);
        task01.setCommitNeeded();

        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets02 = singletonMap(t1p2, new OffsetAndMetadata(2L, null));
        task02.setCommittableOffsetsAndMetadata(offsets02);

        final StateMachineTask task10 = new StateMachineTask(taskId10, taskId10Partitions, false);

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
        expectRestoreToBeCompleted(consumer, changeLogReader);

        expect(activeTaskCreator.createTasks(anyObject(), eq(assignmentActive)))
            .andReturn(asList(task00, task01, task02));

        expect(activeTaskCreator.threadProducer()).andReturn(producer);
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expect(standbyTaskCreator.createTasks(eq(assignmentStandby)))
            .andReturn(singletonList(task10));

        final ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata("appId");
        expect(consumer.groupMetadata()).andReturn(groupMetadata);
        producer.commitTransaction(expectedCommittedOffsets, groupMetadata);
        expectLastCall();

        task00.committedOffsets();
        EasyMock.expectLastCall();
        task01.committedOffsets();
        EasyMock.expectLastCall();
        task02.committedOffsets();
        EasyMock.expectLastCall();
        task10.committedOffsets();
        EasyMock.expectLastCall();

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

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
    }

    @Test
    public void shouldCommitAllNeededTasksOnHandleRevocation() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets00);
        task00.setCommitNeeded();

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        task01.setCommittableOffsetsAndMetadata(offsets01);
        task01.setCommitNeeded();

        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets02 = singletonMap(t1p2, new OffsetAndMetadata(2L, null));
        task02.setCommittableOffsetsAndMetadata(offsets02);

        final StateMachineTask task10 = new StateMachineTask(taskId10, taskId10Partitions, false);

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
        expectRestoreToBeCompleted(consumer, changeLogReader);

        expect(activeTaskCreator.createTasks(anyObject(), eq(assignmentActive)))
            .andReturn(asList(task00, task01, task02));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();
        expect(standbyTaskCreator.createTasks(eq(assignmentStandby)))
            .andReturn(singletonList(task10));
        consumer.commitSync(expectedCommittedOffsets);
        expectLastCall();

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

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
    }

    @Test
    public void shouldNotCommitOnHandleAssignmentIfNoTaskClosed() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets00);
        task00.setCommitNeeded();

        final StateMachineTask task10 = new StateMachineTask(taskId10, taskId10Partitions, false);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = singletonMap(taskId00, taskId00Partitions);
        final Map<TaskId, Set<TopicPartition>> assignmentStandby = singletonMap(taskId10, taskId10Partitions);

        expectRestoreToBeCompleted(consumer, changeLogReader);

        expect(activeTaskCreator.createTasks(anyObject(), eq(assignmentActive))).andReturn(singleton(task00));
        expect(standbyTaskCreator.createTasks(eq(assignmentStandby))).andReturn(singletonList(task10));

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

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
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets00);
        task00.setCommitNeeded();

        final StateMachineTask task10 = new StateMachineTask(taskId10, taskId10Partitions, false);

        final Map<TaskId, Set<TopicPartition>> assignmentActive = singletonMap(taskId00, taskId00Partitions);
        final Map<TaskId, Set<TopicPartition>> assignmentStandby = singletonMap(taskId10, taskId10Partitions);

        expectRestoreToBeCompleted(consumer, changeLogReader);

        expect(activeTaskCreator.createTasks(anyObject(), eq(assignmentActive))).andReturn(singleton(task00));
        expect(standbyTaskCreator.createTasks(eq(assignmentStandby))).andReturn(singletonList(task10));

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(assignmentActive, assignmentStandby);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task10.state(), is(Task.State.RUNNING));

        taskManager.handleAssignment(assignmentActive, Collections.emptyMap());

        assertThat(task00.commitNeeded, is(true));
    }

    @Test
    public void shouldNotCommitCreatedTasksOnRevocationOrClosure() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId00));
        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));

        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), is(Task.State.CLOSED));
    }

    @Test
    public void shouldPassUpIfExceptionDuringSuspend() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("KABOOM!");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);
        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        assertThrows(RuntimeException.class, () -> taskManager.handleRevocation(taskId00Partitions));
        assertThat(task00.state(), is(Task.State.SUSPENDED));

        verify(consumer);
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdown() {
        final TopicPartition changelog = new TopicPartition("changelog", 0);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions),
            mkEntry(taskId03, taskId03Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Collection<TopicPartition> changelogPartitions() {
                return singletonList(changelog);
            }
        };
        final AtomicBoolean closedDirtyTask01 = new AtomicBoolean(false);
        final AtomicBoolean closedDirtyTask02 = new AtomicBoolean(false);
        final AtomicBoolean closedDirtyTask03 = new AtomicBoolean(false);
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, true) {
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
        final Task task02 = new StateMachineTask(taskId02, taskId02Partitions, true) {
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
        final Task task03 = new StateMachineTask(taskId03, taskId03Partitions, true) {
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

        resetToStrict(changeLogReader);
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment)))
            .andStubReturn(asList(task00, task01, task02, task03));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId00));
        expectLastCall();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId01));
        expectLastCall();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId02));
        expectLastCall();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId03));
        expectLastCall();
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall();
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andStubReturn(emptyList());
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

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

        final RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> taskManager.shutdown(true)
        );
        assertThat(exception.getMessage(), equalTo("Unexpected exception while closing task"));
        assertThat(exception.getCause().getMessage(), is("migrated; it means all tasks belonging to this thread should be migrated."));
        assertThat(exception.getCause().getCause().getMessage(), is("cause"));

        assertThat(closedDirtyTask01.get(), is(true));
        assertThat(closedDirtyTask02.get(), is(true));
        assertThat(closedDirtyTask03.get(), is(true));
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(task01.state(), is(Task.State.CLOSED));
        assertThat(task02.state(), is(Task.State.CLOSED));
        assertThat(task03.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator, changeLogReader);
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateTaskProducerExceptionsOnCleanShutdown() {
        final TopicPartition changelog = new TopicPartition("changelog", 0);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions)
        );
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Collection<TopicPartition> changelogPartitions() {
                return singletonList(changelog);
            }
        };
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        resetToStrict(changeLogReader);
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(singletonList(task00));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId00));
        expectLastCall().andThrow(new RuntimeException("whatever"));
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall();
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andStubReturn(emptyList());
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

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

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> taskManager.shutdown(true));

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(exception.getMessage(), is("Unexpected exception while closing task"));
        assertThat(exception.getCause().getMessage(), is("whatever"));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator, changeLogReader);
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateThreadProducerExceptionsOnCleanShutdown() {
        final TopicPartition changelog = new TopicPartition("changelog", 0);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Collection<TopicPartition> changelogPartitions() {
                return singletonList(changelog);
            }
        };

        resetToStrict(changeLogReader);
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(singletonList(task00));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId00));
        expectLastCall();
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall().andThrow(new RuntimeException("whatever"));
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andStubReturn(emptyList());
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

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

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> taskManager.shutdown(true));

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(exception.getMessage(), is("Unexpected exception while closing task"));
        assertThat(exception.getCause().getMessage(), is("whatever"));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator, changeLogReader);
    }

    @Test
    public void shouldOnlyCommitRevokedStandbyTaskAndPropagatePrepareCommitException() {
        setUpTaskManager(StreamThread.ProcessingMode.EXACTLY_ONCE_ALPHA);

        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false);

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false) {
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
        assertThat(taskManager.tasks(), is(Collections.singletonMap(taskId00, task00)));
    }

    @Test
    public void shouldSuspendAllRevokedActiveTasksAndPropagateSuspendException() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("task 0_1 suspend boom!");
            }
        };

        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);

        taskManager.addTask(task00);
        taskManager.addTask(task01);
        taskManager.addTask(task02);

        replay(activeTaskCreator);

        final RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> taskManager.handleRevocation(union(HashSet::new, taskId01Partitions, taskId02Partitions)));
        assertThat(thrown.getCause().getMessage(), is("task 0_1 suspend boom!"));

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.SUSPENDED));
        assertThat(task02.state(), is(Task.State.SUSPENDED));

        verify(activeTaskCreator);
    }

    @Test
    public void shouldCloseActiveTasksAndIgnoreExceptionsOnUncleanShutdown() {
        final TopicPartition changelog = new TopicPartition("changelog", 0);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
        );
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Collection<TopicPartition> changelogPartitions() {
                return singletonList(changelog);
            }
        };
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, true) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("migrated", new RuntimeException("cause"));
            }
        };
        final Task task02 = new StateMachineTask(taskId02, taskId02Partitions, true) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("oops");
            }
        };

        resetToStrict(changeLogReader);
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andStubReturn(asList(task00, task01, task02));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId00));
        expectLastCall().andThrow(new RuntimeException("whatever 0"));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId01));
        expectLastCall().andThrow(new RuntimeException("whatever 1"));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId02));
        expectLastCall().andThrow(new RuntimeException("whatever 2"));
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall().andThrow(new RuntimeException("whatever all"));
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andStubReturn(emptyList());
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

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

        taskManager.shutdown(false);

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(task01.state(), is(Task.State.CLOSED));
        assertThat(task02.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator, changeLogReader);
    }

    @Test
    public void shouldCloseStandbyTasksOnShutdown() {
        final Map<TaskId, Set<TopicPartition>> assignment = singletonMap(taskId00, taskId00Partitions);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false);

        // `handleAssignment`
        expect(standbyTaskCreator.createTasks(eq(assignment))).andStubReturn(singletonList(task00));

        // `tryToCompleteRestoration`
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(consumer.assignment()).andReturn(emptySet());
        consumer.resume(eq(emptySet()));
        expectLastCall();

        // `shutdown`
        consumer.commitSync(Collections.emptyMap());
        expectLastCall();
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall();

        replay(consumer, activeTaskCreator, standbyTaskCreator, changeLogReader);

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
        verify(activeTaskCreator);
    }

    @Test
    public void shouldInitializeNewActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));
        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // verifies that we actually resume the assignment at the end of restoration.
        verify(consumer);
    }

    @Test
    public void shouldInitializeNewStandbyTasks() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment)))
            .andStubReturn(singletonList(task01));

        replay(standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(emptyMap(), taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.equalTo(singletonMap(taskId01, task01)));
    }

    @Test
    public void shouldHandleRebalanceEvents() {
        final Set<TopicPartition> assignment = singleton(new TopicPartition("assignment", 0));
        expect(consumer.assignment()).andReturn(assignment);
        consumer.pause(assignment);
        expectLastCall();
        expect(stateDirectory.listNonEmptyTaskDirectories()).andReturn(new File[0]);
        replay(consumer, stateDirectory);
        assertThat(taskManager.isRebalanceInProgress(), is(false));
        taskManager.handleRebalanceStart(emptySet());
        assertThat(taskManager.isRebalanceInProgress(), is(true));
        taskManager.handleRebalanceComplete();
        assertThat(taskManager.isRebalanceInProgress(), is(false));
    }

    @Test
    public void shouldCommitActiveAndStandbyTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment)))
            .andStubReturn(singletonList(task01));
        consumer.commitSync(offsets);
        expectLastCall();

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();
        task01.setCommitNeeded();

        assertThat(taskManager.commitAll(), equalTo(2));
        assertThat(task00.commitNeeded, is(false));
        assertThat(task01.commitNeeded, is(false));
    }

    @Test
    public void shouldCommitProvidedTasksIfNeeded() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);
        final StateMachineTask task03 = new StateMachineTask(taskId03, taskId03Partitions, false);
        final StateMachineTask task04 = new StateMachineTask(taskId04, taskId04Partitions, false);
        final StateMachineTask task05 = new StateMachineTask(taskId05, taskId05Partitions, false);

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

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignmentActive)))
            .andStubReturn(Arrays.asList(task00, task01, task02));
        expect(standbyTaskCreator.createTasks(eq(assignmentStandby)))
            .andStubReturn(Arrays.asList(task03, task04, task05));

        consumer.commitSync(eq(emptyMap()));

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

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
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(standbyTaskCreator.createTasks(eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));
        expectLastCall();

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(Collections.emptyMap(), taskId00Assignment);
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();

        assertThat(taskManager.commitAll(), equalTo(1));
        assertThat(task00.commitNeeded, is(false));
    }

    @Test
    public void shouldNotCommitActiveAndStandbyTasksWhileRebalanceInProgress() throws Exception {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        makeTaskFolders(taskId00.toString(), task01.toString());
        expectLockObtainedFor(taskId00, taskId01);
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment)))
            .andStubReturn(singletonList(task01));

        replay(activeTaskCreator, standbyTaskCreator, stateDirectory, consumer, changeLogReader);

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
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        task01.setCommittableOffsetsAndMetadata(offsets);
        task01.setCommitNeeded();
        taskManager.addTask(task01);

        consumer.commitSync(offsets);
        expectLastCall();
        replay(consumer);

        taskManager.commitAll();

        verify(consumer);
    }

    @Test
    public void shouldCommitViaProducerIfEosAlphaEnabled() {
        final StreamsProducer producer = mock(StreamsProducer.class);
        expect(activeTaskCreator.streamsProducerForTask(anyObject(TaskId.class)))
            .andReturn(producer)
            .andReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT02 = singletonMap(t1p2, new OffsetAndMetadata(1L, null));

        producer.commitTransaction(offsetsT01, new ConsumerGroupMetadata("appId"));
        expectLastCall();
        producer.commitTransaction(offsetsT02, new ConsumerGroupMetadata("appId"));
        expectLastCall();

        shouldCommitViaProducerIfEosEnabled(StreamThread.ProcessingMode.EXACTLY_ONCE_ALPHA, producer, offsetsT01, offsetsT02);
    }

    @Test
    public void shouldCommitViaProducerIfEosV2Enabled() {
        final StreamsProducer producer = mock(StreamsProducer.class);
        expect(activeTaskCreator.threadProducer()).andReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT02 = singletonMap(t1p2, new OffsetAndMetadata(1L, null));
        final Map<TopicPartition, OffsetAndMetadata> allOffsets = new HashMap<>();
        allOffsets.putAll(offsetsT01);
        allOffsets.putAll(offsetsT02);

        producer.commitTransaction(allOffsets, new ConsumerGroupMetadata("appId"));
        expectLastCall();

        shouldCommitViaProducerIfEosEnabled(StreamThread.ProcessingMode.EXACTLY_ONCE_V2, producer, offsetsT01, offsetsT02);
    }

    private void shouldCommitViaProducerIfEosEnabled(final StreamThread.ProcessingMode processingMode,
                                                     final StreamsProducer producer,
                                                     final Map<TopicPartition, OffsetAndMetadata> offsetsT01,
                                                     final Map<TopicPartition, OffsetAndMetadata> offsetsT02) {
        setUpTaskManager(processingMode);

        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        task01.setCommittableOffsetsAndMetadata(offsetsT01);
        task01.setCommitNeeded();
        taskManager.addTask(task01);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);
        task02.setCommittableOffsetsAndMetadata(offsetsT02);
        task02.setCommitNeeded();
        taskManager.addTask(task02);

        reset(consumer);
        expect(consumer.groupMetadata()).andStubReturn(new ConsumerGroupMetadata("appId"));
        replay(activeTaskCreator, consumer, producer);

        taskManager.commitAll();

        verify(producer, consumer);
    }

    @Test
    public void shouldPropagateExceptionFromActiveCommit() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
                throw new RuntimeException("opsh.");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);

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
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false) {
            @Override
            public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
                throw new RuntimeException("opsh.");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment)))
            .andStubReturn(singletonList(task01));

        replay(standbyTaskCreator, consumer, changeLogReader);

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
        resetToStrict(adminClient);
        expect(adminClient.deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(5L))))
            .andReturn(new DeleteRecordsResult(singletonMap(t1p1, completedFuture())));
        expect(adminClient.deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(17L))))
            .andReturn(new DeleteRecordsResult(singletonMap(t1p1, completedFuture())));
        replay(adminClient);

        final Map<TopicPartition, Long> purgableOffsets = new HashMap<>();
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Map<TopicPartition, Long> purgeableOffsets() {
                return purgableOffsets;
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        purgableOffsets.put(t1p1, 5L);
        taskManager.maybePurgeCommittedRecords();

        purgableOffsets.put(t1p1, 17L);
        taskManager.maybePurgeCommittedRecords();

        verify(adminClient);
    }

    @Test
    public void shouldNotSendPurgeDataIfPreviousNotDone() {
        resetToStrict(adminClient);
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        expect(adminClient.deleteRecords(singletonMap(t1p1, RecordsToDelete.beforeOffset(5L))))
            .andReturn(new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords)));
        replay(adminClient);

        final Map<TopicPartition, Long> purgableOffsets = new HashMap<>();
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Map<TopicPartition, Long> purgeableOffsets() {
                return purgableOffsets;
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);

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

        verify(adminClient);
    }

    @Test
    public void shouldIgnorePurgeDataErrors() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords));
        futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));
        expect(adminClient.deleteRecords(anyObject())).andReturn(deleteRecordsResult).times(2);

        replay(activeTaskCreator, adminClient, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        task00.setPurgeableOffsets(singletonMap(t1p1, 5L));

        taskManager.maybePurgeCommittedRecords();
        taskManager.maybePurgeCommittedRecords();

        verify(adminClient);
    }

    @Test
    public void shouldMaybeCommitAllActiveTasksThatNeedCommit() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets0 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets0);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets1 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        task01.setCommittableOffsetsAndMetadata(offsets1);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets2 = singletonMap(t1p2, new OffsetAndMetadata(2L, null));
        task02.setCommittableOffsetsAndMetadata(offsets2);
        final StateMachineTask task03 = new StateMachineTask(taskId03, taskId03Partitions, true);
        final StateMachineTask task04 = new StateMachineTask(taskId10, taskId10Partitions, false);

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

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignmentActive)))
            .andStubReturn(asList(task00, task01, task02, task03));
        expect(standbyTaskCreator.createTasks(eq(assignmentStandby)))
            .andStubReturn(singletonList(task04));
        consumer.commitSync(expectedCommittedOffsets);
        expectLastCall();

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

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
    }

    @Test
    public void shouldProcessActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.put(taskId00, taskId00Partitions);
        assignment.put(taskId01, taskId01Partitions);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment)))
            .andStubReturn(Arrays.asList(task00, task01));

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());
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

        // check that if there's no records proccssible, we would stop early
        assertThat(taskManager.process(3, time), is(5));
        assertThat(taskManager.process(3, time), is(0));
    }

    @Test
    public void shouldNotFailOnTimeoutException() {
        final AtomicReference<TimeoutException> timeoutException = new AtomicReference<>();
        timeoutException.set(new TimeoutException("Skip me!"));

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        task00.transitionTo(State.RESTORING);
        task00.transitionTo(State.RUNNING);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true) {
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
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);
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
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public boolean process(final long wallClockTime) {
                throw new TaskMigratedException("migrated", new RuntimeException("cause"));
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        final TopicPartition partition = taskId00Partitions.iterator().next();
        task00.addRecords(partition, singletonList(getConsumerRecord(partition, 0L)));

        assertThrows(TaskMigratedException.class, () -> taskManager.process(1, time));
    }

    @Test
    public void shouldPropagateRuntimeExceptionsInProcessActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public boolean process(final long wallClockTime) {
                throw new RuntimeException("oops");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        final TopicPartition partition = taskId00Partitions.iterator().next();
        task00.addRecords(partition, singletonList(getConsumerRecord(partition, 0L)));

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> taskManager.process(1, time));
        assertThat(exception.getMessage(), is("oops"));
    }

    @Test
    public void shouldPropagateTaskMigratedExceptionsInPunctuateActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public boolean maybePunctuateStreamTime() {
                throw new TaskMigratedException("migrated", new RuntimeException("cause"));
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        assertThrows(TaskMigratedException.class, () -> taskManager.punctuate());
    }

    @Test
    public void shouldPropagateKafkaExceptionsInPunctuateActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public boolean maybePunctuateStreamTime() {
                throw new KafkaException("oops");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        assertThrows(KafkaException.class, () -> taskManager.punctuate());
    }

    @Test
    public void shouldPunctuateActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public boolean maybePunctuateStreamTime() {
                return true;
            }

            @Override
            public boolean maybePunctuateSystemTime() {
                return true;
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        // one for stream and one for system time
        assertThat(taskManager.punctuate(), equalTo(2));
    }

    @Test
    public void shouldReturnFalseWhenThereAreStillNonRunningTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Collection<TopicPartition> changelogPartitions() {
                return singletonList(new TopicPartition("fake", 0));
            }
        };

        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andStubReturn(singletonList(task00));

        replay(activeTaskCreator, changeLogReader, consumer);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(false));
        assertThat(task00.state(), is(Task.State.RESTORING));
        // this could be a bit mysterious; we're verifying _no_ interactions on the consumer,
        // since the taskManager should _not_ resume the assignment while we're still in RESTORING
        verify(consumer);
    }

    @Test
    public void shouldHaveRemainingPartitionsUncleared() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task00.setCommittableOffsetsAndMetadata(offsets);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));
        consumer.commitSync(offsets);
        expectLastCall();

        replay(activeTaskCreator, consumer, changeLogReader);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            taskManager.handleAssignment(taskId00Assignment, emptyMap());
            assertThat(taskManager.tryToCompleteRestoration(time.milliseconds(), null), is(true));
            assertThat(task00.state(), is(Task.State.RUNNING));

            taskManager.handleRevocation(mkSet(t1p0, new TopicPartition("unknown", 0)));
            assertThat(task00.state(), is(Task.State.SUSPENDED));

            final List<String> messages = appender.getMessages();
            assertThat(
                messages,
                hasItem("taskManagerTestThe following partitions [unknown-0] are missing " +
                    "from the task partitions. It could potentially due to race " +
                    "condition of consumer detecting the heartbeat failure, or the " +
                    "tasks have been cleaned up by the handleAssignment callback.")
            );
        }
    }

    @Test
    public void shouldThrowTaskMigratedWhenAllTaskCloseExceptionsAreTaskMigrated() {
        final StateMachineTask migratedTask01 = new StateMachineTask(taskId01, taskId01Partitions, false) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("t1 close exception", new RuntimeException());
            }
        };

        final StateMachineTask migratedTask02 = new StateMachineTask(taskId02, taskId02Partitions, false) {
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
            equalTo("t1 close exception; it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowRuntimeExceptionWhenEncounteredUnknownExceptionDuringTaskClose() {
        final StateMachineTask migratedTask01 = new StateMachineTask(taskId01, taskId01Partitions, false) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("t1 close exception", new RuntimeException());
            }
        };

        final StateMachineTask migratedTask02 = new StateMachineTask(taskId02, taskId02Partitions, false) {
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
        assertThat(thrown.getMessage(), equalTo("Unexpected failure to close 2 task(s) [[0_1, 0_2]]. " +
                                                    "First unexpected exception (for task 0_2) follows."));

        assertThat(thrown.getCause().getMessage(), equalTo("t2 illegal state exception"));
    }

    @Test
    public void shouldThrowSameKafkaExceptionWhenEncounteredDuringTaskClose() {
        final StateMachineTask migratedTask01 = new StateMachineTask(taskId01, taskId01Partitions, false) {
            @Override
            public void suspend() {
                super.suspend();
                throw new TaskMigratedException("t1 close exception", new RuntimeException());
            }
        };

        final StateMachineTask migratedTask02 = new StateMachineTask(taskId02, taskId02Partitions, false) {
            @Override
            public void suspend() {
                super.suspend();
                throw new KafkaException("Kaboom for t2!", new RuntimeException());
            }
        };
        taskManager.addTask(migratedTask01);
        taskManager.addTask(migratedTask02);

        final KafkaException thrown = assertThrows(
            KafkaException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );

        // Expecting the original Kafka exception instead of a wrapped one.
        assertThat(thrown.getMessage(), equalTo("Kaboom for t2!"));

        assertThat(thrown.getCause().getMessage(), equalTo(null));
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

        expect(activeTaskCreator.producerMetrics()).andReturn(dummyProducerMetrics);
        replay(activeTaskCreator);

        assertThat(taskManager.producerMetrics(), is(dummyProducerMetrics));
    }

    private Map<TaskId, StateMachineTask> handleAssignment(final Map<TaskId, Set<TopicPartition>> runningActiveAssignment,
                                                           final Map<TaskId, Set<TopicPartition>> standbyAssignment,
                                                           final Map<TaskId, Set<TopicPartition>> restoringActiveAssignment) {
        final Set<Task> runningTasks = runningActiveAssignment.entrySet().stream()
                                           .map(t -> new StateMachineTask(t.getKey(), t.getValue(), true))
                                           .collect(Collectors.toSet());
        final Set<Task> standbyTasks = standbyAssignment.entrySet().stream()
                                           .map(t -> new StateMachineTask(t.getKey(), t.getValue(), false))
                                           .collect(Collectors.toSet());
        final Set<Task> restoringTasks = restoringActiveAssignment.entrySet().stream()
                                           .map(t -> new StateMachineTask(t.getKey(), t.getValue(), true))
                                           .collect(Collectors.toSet());
        // give the restoring tasks some uncompleted changelog partitions so they'll stay in restoring
        restoringTasks.forEach(t -> ((StateMachineTask) t).setChangelogOffsets(singletonMap(new TopicPartition("changelog", 0), 0L)));

        // Initially assign only the active tasks we want to complete restoration
        final Map<TaskId, Set<TopicPartition>> allActiveTasksAssignment = new HashMap<>(runningActiveAssignment);
        allActiveTasksAssignment.putAll(restoringActiveAssignment);
        final Set<Task> allActiveTasks = new HashSet<>(runningTasks);
        allActiveTasks.addAll(restoringTasks);

        expect(standbyTaskCreator.createTasks(eq(standbyAssignment))).andStubReturn(standbyTasks);
        expect(activeTaskCreator.createTasks(anyObject(), eq(allActiveTasksAssignment))).andStubReturn(allActiveTasks);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

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

    private void expectLockObtainedFor(final TaskId... tasks) throws Exception {
        for (final TaskId task : tasks) {
            expect(stateDirectory.lock(task)).andReturn(true).once();
        }
    }

    private void expectLockFailedFor(final TaskId... tasks) throws Exception {
        for (final TaskId task : tasks) {
            expect(stateDirectory.lock(task)).andReturn(false).once();
        }
    }

    private void expectUnlockFor(final TaskId... tasks) throws Exception {
        for (final TaskId task : tasks) {
            stateDirectory.unlock(task);
            expectLastCall();
        }
    }

    private static void expectConsumerAssignmentPaused(final Consumer<byte[], byte[]> consumer) {
        final Set<TopicPartition> assignment = singleton(new TopicPartition("assignment", 0));
        expect(consumer.assignment()).andReturn(assignment);
        consumer.pause(assignment);
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnCommitFailed() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task01.setCommittableOffsetsAndMetadata(offsets);
        task01.setCommitNeeded();
        taskManager.addTask(task01);

        consumer.commitSync(offsets);
        expectLastCall().andThrow(new CommitFailedException());
        replay(consumer);

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
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);

        consumer.commitSync(anyObject(Map.class));
        expectLastCall().andThrow(new TimeoutException("KABOOM!"));
        consumer.commitSync(anyObject(Map.class));
        expectLastCall();
        replay(consumer);

        task00.setCommitNeeded();

        assertThat(taskManager.commit(mkSet(task00, task01)), equalTo(0));
        assertThat(task00.timeout, equalTo(time.milliseconds()));
        assertNull(task01.timeout);

        assertThat(taskManager.commit(mkSet(task00, task01)), equalTo(1));
        assertNull(task00.timeout);
        assertNull(task01.timeout);
    }

    @Test
    public void shouldNotFailForTimeoutExceptionOnCommitWithEosAlpha() {
        setUpTaskManager(ProcessingMode.EXACTLY_ONCE_ALPHA);

        final StreamsProducer producer = mock(StreamsProducer.class);
        expect(activeTaskCreator.streamsProducerForTask(anyObject(TaskId.class)))
            .andReturn(producer)
            .andReturn(producer)
            .andReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));

        producer.commitTransaction(offsetsT00, null);
        expectLastCall().andThrow(new TimeoutException("KABOOM!"));
        producer.commitTransaction(offsetsT00, null);
        expectLastCall();

        producer.commitTransaction(offsetsT01, null);
        expectLastCall();
        producer.commitTransaction(offsetsT01, null);
        expectLastCall();

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        task00.setCommittableOffsetsAndMetadata(offsetsT00);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        task01.setCommittableOffsetsAndMetadata(offsetsT01);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);

        expect(consumer.groupMetadata()).andStubReturn(null);
        replay(producer, activeTaskCreator, consumer);

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
    }

    @Test
    public void shouldThrowTaskCorruptedExceptionForTimeoutExceptionOnCommitWithEosV2() {
        setUpTaskManager(ProcessingMode.EXACTLY_ONCE_V2);

        final StreamsProducer producer = mock(StreamsProducer.class);
        expect(activeTaskCreator.threadProducer())
            .andReturn(producer)
            .andReturn(producer);

        final Map<TopicPartition, OffsetAndMetadata> offsetsT00 = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        final Map<TopicPartition, OffsetAndMetadata> offsetsT01 = singletonMap(t1p1, new OffsetAndMetadata(1L, null));
        final Map<TopicPartition, OffsetAndMetadata> allOffsets = new HashMap<>(offsetsT00);
        allOffsets.putAll(offsetsT01);

        producer.commitTransaction(allOffsets, null);
        expectLastCall().andThrow(new TimeoutException("KABOOM!"));
        producer.commitTransaction(allOffsets, null);
        expectLastCall();

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        task00.setCommittableOffsetsAndMetadata(offsetsT00);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        task01.setCommittableOffsetsAndMetadata(offsetsT01);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);

        expect(consumer.groupMetadata()).andStubReturn(null);
        replay(producer, activeTaskCreator, consumer);

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
    }

    @Test
    public void shouldStreamsExceptionOnCommitError() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task01.setCommittableOffsetsAndMetadata(offsets);
        task01.setCommitNeeded();
        taskManager.addTask(task01);

        consumer.commitSync(offsets);
        expectLastCall().andThrow(new KafkaException());
        replay(consumer);

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
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        final Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p0, new OffsetAndMetadata(0L, null));
        task01.setCommittableOffsetsAndMetadata(offsets);
        task01.setCommitNeeded();
        taskManager.addTask(task01);

        consumer.commitSync(offsets);
        expectLastCall().andThrow(new RuntimeException("KABOOM"));
        replay(consumer);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.commitAll()
        );

        assertThat(thrown.getMessage(), equalTo("KABOOM"));
        assertThat(task01.state(), is(Task.State.CREATED));
    }

    @Test
    public void shouldSuspendAllTasksButSkipCommitIfSuspendingFailsDuringRevocation() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public void suspend() {
                super.suspend();
                throw new RuntimeException("KABOOM!");
            }
        };
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>(taskId00Assignment);
        assignment.putAll(taskId01Assignment);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment)))
            .andReturn(asList(task00, task01));
        replay(activeTaskCreator, consumer);

        taskManager.handleAssignment(assignment, Collections.emptyMap());

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> taskManager.handleRevocation(asList(t1p0, t1p1)));

        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
        assertThat(task00.state(), is(Task.State.SUSPENDED));
        assertThat(task01.state(), is(Task.State.SUSPENDED));
    }

    @Test
    public void shouldConvertActiveTaskToStandbyTask() {
        final StreamTask activeTask = mock(StreamTask.class);
        expect(activeTask.id()).andStubReturn(taskId00);
        expect(activeTask.inputPartitions()).andStubReturn(taskId00Partitions);
        expect(activeTask.isActive()).andStubReturn(true);
        expect(activeTask.prepareCommit()).andStubReturn(Collections.emptyMap());

        final StandbyTask standbyTask = mock(StandbyTask.class);
        expect(standbyTask.id()).andStubReturn(taskId00);

        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(activeTask));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall().anyTimes();

        expect(standbyTaskCreator.createStandbyTaskFromActive(anyObject(), eq(taskId00Partitions)))
            .andReturn(standbyTask);

        replay(activeTask, standbyTask, activeTaskCreator, standbyTaskCreator, consumer);

        taskManager.handleAssignment(taskId00Assignment, Collections.emptyMap());
        taskManager.handleAssignment(Collections.emptyMap(), taskId00Assignment);

        verify(activeTaskCreator, standbyTaskCreator);
    }

    @Test
    public void shouldConvertStandbyTaskToActiveTask() {
        final StandbyTask standbyTask = mock(StandbyTask.class);
        expect(standbyTask.id()).andStubReturn(taskId00);
        expect(standbyTask.isActive()).andStubReturn(false);
        expect(standbyTask.prepareCommit()).andStubReturn(Collections.emptyMap());
        standbyTask.suspend();
        expectLastCall().anyTimes();
        standbyTask.postCommit(true);
        expectLastCall().anyTimes();

        final StreamTask activeTask = mock(StreamTask.class);
        expect(activeTask.id()).andStubReturn(taskId00);
        expect(activeTask.inputPartitions()).andStubReturn(taskId00Partitions);

        expect(standbyTaskCreator.createTasks(eq(taskId00Assignment)))
            .andReturn(singletonList(standbyTask));

        expect(activeTaskCreator.createActiveTaskFromStandby(anyObject(), eq(taskId00Partitions), anyObject()))
            .andReturn(activeTask);

        replay(standbyTask, activeTask, standbyTaskCreator, activeTaskCreator, consumer);

        taskManager.handleAssignment(Collections.emptyMap(), taskId00Assignment);
        taskManager.handleAssignment(taskId00Assignment, Collections.emptyMap());

        verify(standbyTaskCreator, activeTaskCreator);
    }

    private static void expectRestoreToBeCompleted(final Consumer<byte[], byte[]> consumer,
                                                   final ChangelogReader changeLogReader) {
        expectRestoreToBeCompleted(consumer, changeLogReader, true);
    }

    private static void expectRestoreToBeCompleted(final Consumer<byte[], byte[]> consumer,
                                                   final ChangelogReader changeLogReader,
                                                   final boolean changeLogUpdateRequired) {
        final Set<TopicPartition> assignment = singleton(new TopicPartition("assignment", 0));
        expect(consumer.assignment()).andReturn(assignment);
        consumer.resume(assignment);
        expectLastCall();
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet()).times(changeLogUpdateRequired ? 1 : 0, 1);
    }

    private static KafkaFutureImpl<DeletedRecords> completedFuture() {
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        futureDeletedRecords.complete(null);
        return futureDeletedRecords;
    }

    private void makeTaskFolders(final String... names) throws Exception {
        final File[] taskFolders = new File[names.length];
        for (int i = 0; i < names.length; ++i) {
            taskFolders[i] = testFolder.newFolder(names[i]);
        }
        expect(stateDirectory.listNonEmptyTaskDirectories()).andReturn(taskFolders).once();
    }

    private void writeCheckpointFile(final TaskId task, final Map<TopicPartition, Long> offsets) throws Exception {
        final File checkpointFile = getCheckpointFile(task);
        assertThat(checkpointFile.createNewFile(), is(true));
        new OffsetCheckpoint(checkpointFile).write(offsets);
        expect(stateDirectory.checkpointFileFor(task)).andReturn(checkpointFile);
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
                         final boolean active) {
            this(id, partitions, active, null);
        }

        StateMachineTask(final TaskId id,
                         final Set<TopicPartition> partitions,
                         final boolean active,
                         final ProcessorStateManager processorStateManager) {
            super(id, null, null, processorStateManager, partitions, 0L, "test-task", StateMachineTask.class);
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
        public void closeClean() {
            transitionTo(State.CLOSED);
        }

        @Override
        public void closeDirty() {
            transitionTo(State.CLOSED);
        }

        @Override
        public void closeCleanAndRecycleState() {
            transitionTo(State.CLOSED);
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
        public Collection<TopicPartition> changelogPartitions() {
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
        public void updateCommittedOffsets(final TopicPartition topicPartition, final Long offset) {
            Objects.requireNonNull(topicPartition);
            assertThat("It must be from an owned topic", inputPartitions.contains(topicPartition));
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
