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

import java.util.HashSet;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
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
import java.io.IOException;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.resetToStrict;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class TaskManagerTest {

    private final String topic1 = "topic1";

    private final TaskId taskId00 = new TaskId(0, 0);
    private final TopicPartition t1p0 = new TopicPartition(topic1, 0);
    private final Set<TopicPartition> taskId00Partitions = mkSet(t1p0);
    private final Map<TaskId, Set<TopicPartition>> taskId00Assignment = singletonMap(taskId00, taskId00Partitions);

    private final TaskId taskId01 = new TaskId(0, 1);
    private final TopicPartition t1p1 = new TopicPartition(topic1, 1);
    private final Set<TopicPartition> taskId01Partitions = mkSet(t1p1);
    private final Map<TaskId, Set<TopicPartition>> taskId01Assignment = singletonMap(taskId01, taskId01Partitions);

    private final TaskId taskId02 = new TaskId(0, 2);
    private final TopicPartition t1p2 = new TopicPartition(topic1, 2);
    private final Set<TopicPartition> taskId02Partitions = mkSet(t1p2);

    private final TaskId taskId10 = new TaskId(1, 0);

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

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "clientId", StreamsConfig.METRICS_LATEST);
        taskManager = new TaskManager(
            changeLogReader,
            UUID.randomUUID(),
            "taskManagerTest",
            streamsMetrics,
            activeTaskCreator,
            standbyTaskCreator,
            topologyBuilder,
            adminClient,
            stateDirectory
        );
        taskManager.setMainConsumer(consumer);
    }

    @Test
    public void shouldIdempotentlyUpdateSubscriptionFromActiveAssignment() {
        final TopicPartition newTopicPartition = new TopicPartition("topic2", 1);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(mkEntry(taskId01, mkSet(t1p1, newTopicPartition)));

        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(emptyList()).anyTimes();

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
    public void shouldTryToLockValidTaskDirsAtRebalanceStart() throws IOException {
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
    public void shouldReleaseLockForUnassignedTasksAfterRebalance() throws IOException {
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
    public void shouldReportLatestOffsetAsOffsetSumForRunningTask() throws IOException {
        final Map<TaskId, Long> expectedOffsetSums = mkMap(mkEntry(taskId00, Task.LATEST_OFFSET));

        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        replay(stateDirectory);

        taskManager.handleRebalanceStart(singleton("topic"));
        handleAssignment(taskId00Assignment, emptyMap(), emptyMap());

        assertThat(taskManager.getTaskOffsetSums(), is(expectedOffsetSums));
    }

    @Test
    public void shouldComputeOffsetSumForNonRunningActiveTask() throws IOException {
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
            emptyMap(),
            taskId00Assignment
        ).get(taskId00);
        restoringTask.setChangelogOffsets(changelogOffsets);

        assertThat(taskManager.getTaskOffsetSums(), is(expectedOffsetSums));
    }

    @Test
    public void shouldComputeOffsetSumForStandbyTask() throws IOException {
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
    public void shouldComputeOffsetSumForUnassignedTaskWeCanLock() throws IOException {
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
    public void shouldNotReportOffsetSumsForTaskWeCantLock() throws IOException {
        expectLockFailedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        replay(stateDirectory);
        taskManager.handleRebalanceStart(singleton("topic"));
        assertTrue(taskManager.lockedTaskDirectories().isEmpty());

        assertTrue(taskManager.getTaskOffsetSums().isEmpty());
    }

    @Test
    public void shouldNotReportOffsetSumsAndReleaseLockForUnassignedTaskWithoutCheckpoint() throws IOException {
        expectLockObtainedFor(taskId00);
        makeTaskFolders(taskId00.toString());
        expect(stateDirectory.checkpointFileFor(taskId00)).andReturn(getCheckpointFile(taskId00));
        replay(stateDirectory);
        taskManager.handleRebalanceStart(singleton("topic"));

        assertTrue(taskManager.getTaskOffsetSums().isEmpty());
        verify(stateDirectory);
    }

    @Test
    public void shouldPinOffsetSumToLongMaxValueInCaseOfOverflow() throws IOException {
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
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();
        expect(standbyTaskCreator.createTasks(anyObject())).andReturn(emptyList()).anyTimes();

        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());

        assertThat(taskManager.tryToCompleteRestoration(), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));
        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
    }

    @Test
    public void shouldCloseActiveTasksWhenHandlingLostTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment))).andReturn(singletonList(task01)).anyTimes();

        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);

        assertThat(taskManager.tryToCompleteRestoration(), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));
        taskManager.handleLostAll();
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), is(singletonMap(taskId01, task01)));
    }

    @Test
    public void shouldReviveCorruptTasks() {
        final ProcessorStateManager stateManager = EasyMock.createStrictMock(ProcessorStateManager.class);
        stateManager.markChangelogAsCorrupted(taskId00Partitions);
        replay(stateManager);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();
        expect(standbyTaskCreator.createTasks(anyObject())).andReturn(emptyList()).anyTimes();

        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());

        assertThat(taskManager.tryToCompleteRestoration(), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        taskManager.handleCorruption(singletonMap(taskId00, taskId00Partitions));
        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(taskManager.activeTaskMap(), is(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(stateManager);
    }

    @Test
    public void shouldReviveCorruptTasksEvenIfTheyCannotCloseClean() {
        final ProcessorStateManager stateManager = EasyMock.createStrictMock(ProcessorStateManager.class);
        stateManager.markChangelogAsCorrupted(taskId00Partitions);
        replay(stateManager);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true, stateManager) {
            @Override
            public void closeClean() {
                throw new RuntimeException("oops");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId00);
        expectLastCall();
        expect(standbyTaskCreator.createTasks(anyObject())).andReturn(emptyList()).anyTimes();

        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());

        assertThat(taskManager.tryToCompleteRestoration(), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        taskManager.handleCorruption(singletonMap(taskId00, taskId00Partitions));
        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(taskManager.activeTaskMap(), is(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(stateManager);
    }

    @Test
    public void shouldCloseStandbyUnassignedTasksWhenCreatingNewTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(standbyTaskCreator.createTasks(eq(taskId00Assignment))).andReturn(singletonList(task00)).anyTimes();
        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);
        taskManager.handleAssignment(emptyMap(), taskId00Assignment);
        assertThat(taskManager.tryToCompleteRestoration(), is(true));
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
        assertThat(taskManager.tryToCompleteRestoration(), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        verify(activeTaskCreator);
    }

    @Test
    public void shouldAddNewActiveTasks() {
        final Map<TaskId, Set<TopicPartition>> assignment = taskId00Assignment;
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(consumer.assignment()).andReturn(emptySet());
        consumer.resume(eq(emptySet()));
        expectLastCall();
        changeLogReader.transitToRestoreActive();
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        replay(consumer, activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration();

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

        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(consumer.assignment()).andReturn(emptySet());
        consumer.resume(eq(emptySet()));
        expectLastCall();
        changeLogReader.transitToRestoreActive();
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(asList(task00, task01)).anyTimes();
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        replay(consumer, activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.CREATED));

        assertThat(taskManager.tryToCompleteRestoration(), is(false));

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
            public void completeRestoration() {
                throw new TimeoutException("timeout!");
            }
        };

        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(consumer.assignment()).andReturn(emptySet());
        consumer.resume(eq(emptySet()));
        expectLastCall();
        changeLogReader.transitToRestoreActive();
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        replay(consumer, activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        assertThat(taskManager.tryToCompleteRestoration(), is(false));

        assertThat(task00.state(), is(Task.State.RESTORING));
        assertThat(
            taskManager.activeTaskMap(),
            Matchers.equalTo(mkMap(mkEntry(taskId00, task00)))
        );
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator);
    }

    @Test
    public void shouldSuspendActiveTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);
        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));
    }

    @Test
    public void shouldPassUpIfExceptionDuringSuspend() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public void suspend() {
                throw new RuntimeException("KABOOM!");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);
        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        assertThrows(RuntimeException.class, () -> taskManager.handleRevocation(taskId00Partitions));
        assertThat(task00.state(), is(Task.State.RUNNING));
    }

    @Test
    public void shouldCloseActiveTasksAndPropagateExceptionsOnCleanShutdown() {
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
            public void closeClean() {
                throw new TaskMigratedException("migrated", new RuntimeException("cause"));
            }
        };
        final Task task02 = new StateMachineTask(taskId02, taskId02Partitions, true) {
            @Override
            public void closeClean() {
                throw new RuntimeException("oops");
            }
        };

        resetToStrict(changeLogReader);
        changeLogReader.transitToRestoreActive();
        expectLastCall();
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        // make sure we also remove the changelog partitions from the changelog reader
        changeLogReader.remove(eq(singletonList(changelog)));
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(asList(task00, task01, task02)).anyTimes();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId00));
        expectLastCall();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId01));
        expectLastCall();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId02));
        expectLastCall();
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall();
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.CREATED));
        assertThat(task02.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration();

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

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> taskManager.shutdown(true));

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(task01.state(), is(Task.State.CLOSED));
        assertThat(task02.state(), is(Task.State.CLOSED));
        assertThat(exception.getMessage(), is("Unexpected exception while closing task"));
        assertThat(exception.getCause().getMessage(), is("oops"));
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
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Collection<TopicPartition> changelogPartitions() {
                return singletonList(changelog);
            }
        };

        resetToStrict(changeLogReader);
        changeLogReader.transitToRestoreActive();
        expectLastCall();
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        // make sure we also remove the changelog partitions from the changelog reader
        changeLogReader.remove(eq(singletonList(changelog)));
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId00));
        expectLastCall().andThrow(new RuntimeException("whatever"));
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall();
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration();

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
        changeLogReader.transitToRestoreActive();
        expectLastCall();
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        // make sure we also remove the changelog partitions from the changelog reader
        changeLogReader.remove(eq(singletonList(changelog)));
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId00));
        expectLastCall();
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall().andThrow(new RuntimeException("whatever"));
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration();

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
            public void closeClean() {
                throw new TaskMigratedException("migrated", new RuntimeException("cause"));
            }
        };
        final Task task02 = new StateMachineTask(taskId02, taskId02Partitions, true) {
            @Override
            public void closeClean() {
                throw new RuntimeException("oops");
            }
        };

        resetToStrict(changeLogReader);
        changeLogReader.transitToRestoreActive();
        expectLastCall();
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        // make sure we also remove the changelog partitions from the changelog reader
        changeLogReader.remove(eq(singletonList(changelog)));
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(asList(task00, task01, task02)).anyTimes();
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId00));
        expectLastCall().andThrow(new RuntimeException("whatever 0"));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId01));
        expectLastCall().andThrow(new RuntimeException("whatever 1"));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(eq(taskId02));
        expectLastCall().andThrow(new RuntimeException("whatever 2"));
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall().andThrow(new RuntimeException("whatever all"));
        expect(standbyTaskCreator.createTasks(eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.CREATED));
        assertThat(task01.state(), is(Task.State.CREATED));
        assertThat(task02.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration();

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

        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
        expect(consumer.assignment()).andReturn(emptySet());
        consumer.resume(eq(emptySet()));
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        activeTaskCreator.closeThreadProducerIfNeeded();
        expectLastCall();
        expect(standbyTaskCreator.createTasks(eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        replay(consumer, activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(emptyMap(), assignment);

        assertThat(task00.state(), is(Task.State.CREATED));

        taskManager.tryToCompleteRestoration();

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
            .andReturn(singletonList(task00)).anyTimes();
        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
            .andReturn(singletonList(task01)).anyTimes();

        replay(standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(emptyMap(), taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment)))
            .andReturn(singletonList(task01)).anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();
        task01.setCommitNeeded();

        assertThat(taskManager.commitAll(), equalTo(2));
    }

    @Test
    public void shouldNotCommitActiveAndStandbyTasksWhileRebalanceInProgress() throws IOException {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        makeTaskFolders(taskId00.toString(), task01.toString());
        expectLockObtainedFor(taskId00, taskId01);
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment)))
            .andReturn(singletonList(task01)).anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, stateDirectory, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
    public void shouldPropagateExceptionFromActiveCommit() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public void commit() {
                throw new RuntimeException("opsh.");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
            public void commit() {
                throw new RuntimeException("opsh.");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(standbyTaskCreator.createTasks(eq(taskId01Assignment)))
            .andReturn(singletonList(task01)).anyTimes();

        replay(standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(emptyMap(), taskId01Assignment);
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
            .andReturn(singletonList(task00)).anyTimes();

        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords));
        futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));
        expect(adminClient.deleteRecords(anyObject())).andReturn(deleteRecordsResult).times(2);

        replay(activeTaskCreator, adminClient, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        task00.setPurgeableOffsets(singletonMap(t1p1, 5L));

        taskManager.maybePurgeCommittedRecords();
        taskManager.maybePurgeCommittedRecords();

        verify(adminClient);
    }

    @Test
    public void shouldMaybeCommitActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, true);
        final StateMachineTask task02 = new StateMachineTask(taskId02, taskId02Partitions, true);

        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(
            mkEntry(taskId00, taskId00Partitions),
            mkEntry(taskId01, taskId01Partitions),
            mkEntry(taskId02, taskId02Partitions)
        );

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment)))
            .andReturn(asList(task00, task01, task02)).anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);


        taskManager.handleAssignment(assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));
        assertThat(task02.state(), is(Task.State.RUNNING));

        task00.setCommitNeeded();
        task00.setCommitRequested();

        task01.setCommitNeeded();

        task02.setCommitRequested();

        assertThat(taskManager.maybeCommitActiveTasksPerUserRequested(), equalTo(1));
    }

    @Test
    public void shouldProcessActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        final TopicPartition partition = taskId00Partitions.iterator().next();
        task00.addRecords(
            partition,
            singletonList(new ConsumerRecord<>(partition.topic(), partition.partition(), 0L, null, null))
        );

        assertThat(taskManager.process(0L), is(1));
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
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        final TopicPartition partition = taskId00Partitions.iterator().next();
        task00.addRecords(
            partition,
            singletonList(new ConsumerRecord<>(partition.topic(), partition.partition(), 0L, null, null))
        );

        assertThrows(TaskMigratedException.class, () -> taskManager.process(0L));
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
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

        assertThat(task00.state(), is(Task.State.RUNNING));

        final TopicPartition partition = taskId00Partitions.iterator().next();
        task00.addRecords(
            partition,
            singletonList(new ConsumerRecord<>(partition.topic(), partition.partition(), 0L, null, null))
        );

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> taskManager.process(0L));
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
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

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
            .andReturn(singletonList(task00)).anyTimes();

        replay(activeTaskCreator, changeLogReader, consumer);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(false));
        assertThat(task00.state(), is(Task.State.RESTORING));
        // this could be a bit mysterious; we're verifying _no_ interactions on the consumer,
        // since the taskManager should _not_ resume the assignment while we're still in RESTORING
        verify(consumer);
    }

    @Test
    public void shouldHaveRemainingPartitionsUncleared() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);
        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        assertThat(taskManager.tryToCompleteRestoration(), is(true));
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(mkSet(t1p0, new TopicPartition("unknown", 0)));
        assertThat(task00.state(), is(Task.State.SUSPENDED));

        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("taskManagerTestThe following partitions [unknown-0] are missing " +
                                         "from the task partitions. It could potentially due to race " +
                                         "condition of consumer detecting the heartbeat failure, or the " +
                                         "tasks have been cleaned up by the handleAssignment callback."));
    }

    @Test
    public void shouldThrowTaskMigratedWhenAllTaskCloseExceptionsAreTaskMigrated() {
        final StateMachineTask migratedTask01 = new StateMachineTask(taskId01, taskId01Partitions, false) {
            @Override
            public void closeClean() {
                throw new TaskMigratedException("t1 close exception", new RuntimeException());
            }
        };

        final StateMachineTask migratedTask02 = new StateMachineTask(taskId02, taskId02Partitions, false) {
            @Override
            public void closeClean() {
                throw new TaskMigratedException("t2 close exception", new RuntimeException());
            }
        };
        taskManager.tasks().put(taskId01, migratedTask01);
        taskManager.tasks().put(taskId02, migratedTask02);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> taskManager.handleAssignment(emptyMap(), emptyMap())
        );
        // The task map orders tasks based on topic group id and partition, so here
        // t1 should always be the first.
        assertThat(thrown.getMessage(), equalTo("t1 close exception; it means all tasks belonging to this thread should be migrated."));
    }

    @Test
    public void shouldThrowRuntimeExceptionWhenEncounteredUnknownExceptionDuringTaskClose() {
        final StateMachineTask migratedTask01 = new StateMachineTask(taskId01, taskId01Partitions, false) {
            @Override
            public void closeClean() {
                throw new TaskMigratedException("t1 close exception", new RuntimeException());
            }
        };

        final StateMachineTask migratedTask02 = new StateMachineTask(taskId02, taskId02Partitions, false) {
            @Override
            public void closeClean() {
                throw new IllegalStateException("t2 illegal state exception", new RuntimeException());
            }
        };
        taskManager.tasks().put(taskId01, migratedTask01);
        taskManager.tasks().put(taskId02, migratedTask02);

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
            public void closeClean() {
                throw new TaskMigratedException("t1 close exception", new RuntimeException());
            }
        };

        final StateMachineTask migratedTask02 = new StateMachineTask(taskId02, taskId02Partitions, false) {
            @Override
            public void closeClean() {
                throw new KafkaException("Kaboom for t2!", new RuntimeException());
            }
        };
        taskManager.tasks().put(taskId01, migratedTask01);
        taskManager.tasks().put(taskId02, migratedTask02);

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

        // Initially assign only the active tasks we want to complete restoration
        final Map<TaskId, Set<TopicPartition>> allActiveTasksAssignment = new HashMap<>(runningActiveAssignment);
        allActiveTasksAssignment.putAll(restoringActiveAssignment);
        final Set<Task> allActiveTasks = new HashSet<>(runningTasks);
        allActiveTasks.addAll(restoringTasks);

        expect(activeTaskCreator.createTasks(anyObject(), eq(runningActiveAssignment))).andStubReturn(runningTasks);
        expect(standbyTaskCreator.createTasks(eq(standbyAssignment))).andStubReturn(standbyTasks);
        expect(activeTaskCreator.createTasks(anyObject(), eq(allActiveTasksAssignment))).andStubReturn(allActiveTasks);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(runningActiveAssignment, standbyAssignment);
        assertThat(taskManager.tryToCompleteRestoration(), is(true));

        taskManager.handleAssignment(allActiveTasksAssignment, standbyAssignment);

        final Map<TaskId, StateMachineTask> allTasks = new HashMap<>();

        // Just make sure all tasks ended up in the expected state
        for (final Task task : runningTasks) {
            assertThat(task.state(), is(Task.State.RUNNING));
            allTasks.put(task.id(), (StateMachineTask) task);
        }
        for (final Task task : restoringTasks) {
            assertThat(task.state(), not(Task.State.RUNNING));
            allTasks.put(task.id(), (StateMachineTask) task);
        }
        for (final Task task : standbyTasks) {
            assertThat(task.state(), is(Task.State.RUNNING));
            allTasks.put(task.id(), (StateMachineTask) task);
        }
        return allTasks;
    }

    private void expectLockObtainedFor(final TaskId... tasks) throws IOException {
        for (final TaskId task : tasks) {
            expect(stateDirectory.lock(task)).andReturn(true).once();
        }
    }

    private void expectLockFailedFor(final TaskId... tasks) throws IOException {
        for (final TaskId task : tasks) {
            expect(stateDirectory.lock(task)).andReturn(false).once();
        }
    }

    private void expectUnlockFor(final TaskId... tasks) throws IOException {
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

    private static void expectRestoreToBeCompleted(final Consumer<byte[], byte[]> consumer,
                                                   final ChangelogReader changeLogReader) {
        final Set<TopicPartition> assignment = singleton(new TopicPartition("assignment", 0));
        expect(consumer.assignment()).andReturn(assignment);
        consumer.resume(assignment);
        expectLastCall();
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
    }

    private static KafkaFutureImpl<DeletedRecords> completedFuture() {
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        futureDeletedRecords.complete(null);
        return futureDeletedRecords;
    }

    private void makeTaskFolders(final String... names) throws IOException {
        final File[] taskFolders = new File[names.length];
        for (int i = 0; i < names.length; ++i) {
            taskFolders[i] = testFolder.newFolder(names[i]);
        }
        expect(stateDirectory.listNonEmptyTaskDirectories()).andReturn(taskFolders).once();
    }

    private void writeCheckpointFile(final TaskId task, final Map<TopicPartition, Long> offsets) throws IOException {
        final File checkpointFile = getCheckpointFile(task);
        assertThat(checkpointFile.createNewFile(), is(true));
        new OffsetCheckpoint(checkpointFile).write(offsets);
        expect(stateDirectory.checkpointFileFor(task)).andReturn(checkpointFile);
    }

    private File getCheckpointFile(final TaskId task) {
        return new File(new File(testFolder.getRoot(), task.toString()), StateManagerUtil.CHECKPOINT_FILE_NAME);
    }

    private static class StateMachineTask extends AbstractTask implements Task {
        private final boolean active;
        private boolean commitNeeded = false;
        private boolean commitRequested = false;
        private Map<TopicPartition, Long> purgeableOffsets;
        private Map<TopicPartition, Long> changelogOffsets;
        private Map<TopicPartition, LinkedList<ConsumerRecord<byte[], byte[]>>> queue = new HashMap<>();

        StateMachineTask(final TaskId id,
                         final Set<TopicPartition> partitions,
                         final boolean active) {
            this(id, partitions, active, null);
        }

        StateMachineTask(final TaskId id,
                         final Set<TopicPartition> partitions,
                         final boolean active,
                         final ProcessorStateManager processorStateManager) {
            super(id, null, null, processorStateManager, partitions);
            this.active = active;
        }

        @Override
        public void initializeIfNeeded() {
            if (state() == State.CREATED) {
                transitionTo(State.RESTORING);
            }
        }

        @Override
        public void completeRestoration() {
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
        public void commit() {}

        @Override
        public void suspend() {
            transitionTo(State.SUSPENDED);
        }

        @Override
        public void resume() {
            if (state() == State.SUSPENDED) {
                transitionTo(State.RUNNING);
            }
        }

        @Override
        public void closeClean() {
            transitionTo(State.CLOSING);
            transitionTo(State.CLOSED);
        }

        @Override
        public void closeDirty() {
            transitionTo(State.CLOSING);
            transitionTo(State.CLOSED);
        }

        @Override
        public StateStore getStore(final String name) {
            return null;
        }

        @Override
        public Collection<TopicPartition> changelogPartitions() {
            return emptyList();
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
