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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
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

    @Mock(type = MockType.STRICT)
    private InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates;
    @Mock(type = MockType.STRICT)
    private InternalTopologyBuilder topologyBuilder;
    @Mock(type = MockType.NICE)
    private StateDirectory stateDirectory;
    @Mock(type = MockType.NICE)
    private ChangelogReader changeLogReader;
    @Mock(type = MockType.NICE)
    private StreamsMetadataState streamsMetadataState;
    @Mock(type = MockType.NICE)
    private Consumer<byte[], byte[]> restoreConsumer;
    @Mock(type = MockType.NICE)
    private Consumer<byte[], byte[]> consumer;
    @Mock(type = MockType.STRICT)
    private StreamThread.AbstractTaskCreator<Task> activeTaskCreator;
    @Mock(type = MockType.NICE)
    private StreamThread.AbstractTaskCreator<Task> standbyTaskCreator;
    @Mock(type = MockType.NICE)
    private Admin adminClient;
    @Mock(type = MockType.NICE)
    private StreamTask streamTask;
    @Mock(type = MockType.NICE)
    private StandbyTask standbyTask;

    private TaskManager taskManager;

    private final String topic2 = "topic2";
    private final TopicPartition t1p2 = new TopicPartition(topic1, 2);
    private final TopicPartition t1p3 = new TopicPartition(topic1, 3);
    private final TopicPartition t2p1 = new TopicPartition(topic2, 1);
    private final TopicPartition t2p2 = new TopicPartition(topic2, 2);
    private final TopicPartition t2p3 = new TopicPartition(topic2, 3);

    private final TaskId task02 = new TaskId(0, 2);
    private final TaskId task03 = new TaskId(0, 3);
    private final TaskId task11 = new TaskId(1, 1);

    private final Set<TaskId> revokedTasks = new HashSet<>();
    private final List<TopicPartition> revokedPartitions = new ArrayList<>();
    private final List<TopicPartition> revokedChangelogs = emptyList();

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        taskManager = new TaskManager(changeLogReader,
                                      UUID.randomUUID(),
                                      "",
                                      restoreConsumer,
                                      activeTaskCreator,
                                      standbyTaskCreator,
                                      topologyBuilder,
                                      adminClient);
        taskManager.setConsumer(consumer);
        revokedChangelogs.clear();
    }

    private void replay() {
        EasyMock.replay(changeLogReader,
                        restoreConsumer,
                        consumer,
                        activeTaskCreator,
                        standbyTaskCreator,
                        adminClient);
    }

    @Test
    public void shouldUpdateSubscriptionFromAssignmentOfNewTopic2() {
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(mkEntry(taskId01, mkSet(t1p1, t2p1)));

        expect(activeTaskCreator.builder()).andReturn(topologyBuilder).anyTimes();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(emptyList()).anyTimes();
        expect(standbyTaskCreator.createTasks(anyObject(), anyObject())).andReturn(emptyList()).anyTimes();

        topologyBuilder.addSubscribedTopics(anyObject(), anyString());
        EasyMock.expectLastCall();

        EasyMock.expectLastCall().once();

        EasyMock.replay(activeTaskCreator,
                        standbyTaskCreator,
                        topologyBuilder,
                        subscriptionUpdates);

        taskManager.handleAssignment(assignment, emptyMap());

        verify(activeTaskCreator,
               standbyTaskCreator,
               topologyBuilder,
               subscriptionUpdates);
    }

    @Test
    public void shouldNotUpdateSubscriptionFromAssignmentOfExistingTopic1() {
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(mkEntry(taskId01, mkSet(t1p1)));

        expect(activeTaskCreator.builder()).andReturn(topologyBuilder).anyTimes();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(emptyList()).anyTimes();
        expect(standbyTaskCreator.createTasks(anyObject(), anyObject())).andReturn(emptyList()).anyTimes();

        topologyBuilder.addSubscribedTopics(anyObject(), anyString());
        EasyMock.expectLastCall();

        EasyMock.replay(activeTaskCreator,
                        standbyTaskCreator,
                        topologyBuilder,
                        subscriptionUpdates);

        taskManager.handleAssignment(assignment, emptyMap());

        verify(activeTaskCreator,
               standbyTaskCreator,
               topologyBuilder,
               subscriptionUpdates);
    }

    @Test
    public void shouldReturnCachedTaskIdsFromDirectory() throws IOException {
        final File[] taskFolders = asList(testFolder.newFolder("0_1"),
                                          testFolder.newFolder("0_2"),
                                          testFolder.newFolder("0_3"),
                                          testFolder.newFolder("1_1"),
                                          testFolder.newFolder("dummy")).toArray(new File[0]);

        assertTrue((new File(taskFolders[0], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile());
        assertTrue((new File(taskFolders[1], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile());
        assertTrue((new File(taskFolders[3], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile());

        expect(activeTaskCreator.stateDirectory()).andReturn(stateDirectory).once();
        expect(stateDirectory.listTaskDirectories()).andReturn(taskFolders).once();

        EasyMock.replay(activeTaskCreator, stateDirectory);

        final Set<TaskId> tasks = taskManager.cachedTasksIds();

        verify(activeTaskCreator, stateDirectory);

        assertThat(tasks, equalTo(Utils.mkSet(taskId01, task02, task11)));
    }

    @Test
    public void shouldCloseActiveUnAssignedSuspendedTasksWhenClosingRevokedTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        expect(standbyTaskCreator.createTasks(anyObject(), anyObject())).andReturn(emptyList()).anyTimes();

        topologyBuilder.addSubscribedTopics(anyObject(), anyString());
        EasyMock.expectLastCall().anyTimes();

        EasyMock.replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.updateNewAndRestoringTasks();
        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));
        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), Matchers.is(Task.State.SUSPENDED));
        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), Matchers.is(Task.State.CLOSED));
        assertThat(taskManager.activeTasks(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTasks(), Matchers.anEmptyMap());
    }

    @Test
    public void shouldCloseStandbyUnassignedTasksWhenCreatingNewTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false);

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(standbyTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00)).anyTimes();
        EasyMock.replay(activeTaskCreator, standbyTaskCreator, changeLogReader);
        taskManager.handleAssignment(emptyMap(), taskId00Assignment);
        taskManager.updateNewAndRestoringTasks();
        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));
        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), Matchers.is(Task.State.CLOSED));
        assertThat(taskManager.activeTasks(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTasks(), Matchers.anEmptyMap());
    }

    @Test
    public void shouldAddNonResumedSuspendedTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));
        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);

        EasyMock.replay(activeTaskCreator, changeLogReader);
        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.updateNewAndRestoringTasks();
        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.updateNewAndRestoringTasks();
        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));

        verify(activeTaskCreator);
    }

    @Test
    public void shouldAddNewActiveTasks() {
        final Map<TaskId, Set<TopicPartition>> assignment = singletonMap(taskId00, taskId00Partitions);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(standbyTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        EasyMock.replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), Matchers.is(Task.State.CREATED));
        assertThat(taskManager.activeTasks(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTasks(), Matchers.anEmptyMap());
        verify(activeTaskCreator);
    }

    @Ignore
    @Test
    public void shouldNotAddResumedActiveTasks() {
        throw new RuntimeException();
//        checkOrder(active, true);
//        expect(active.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
//        replay();
//
//        // Need to call this twice so task manager doesn't consider all partitions "new"
//        taskManager.setAssignmentMetadata(taskId0Assignment, emptyMap());
//        taskManager.setAssignmentMetadata(taskId0Assignment, emptyMap());
//        taskManager.setPartitionsToTaskId(taskId0PartitionToTaskId);
//        taskManager.createTasks(taskId0Partitions);
//
//        // should be no calls to activeTaskCreator and no calls to active.addNewTasks(..)
//        verify(active, activeTaskCreator);
    }

    @Ignore
    @Test
    public void shouldPauseActivePartitions() {
        throw new RuntimeException();
//        expect(streamTask.id()).andReturn(taskId0);
//        EasyMock.replay(streamTask);
//        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId0Assignment)))
//            .andReturn(singletonList(streamTask));
//
//        expect(consumer.assignment()).andReturn(taskId0Partitions).times(2);
//        consumer.pause(taskId0Partitions);
//        expectLastCall();
//        replay();
//
//        taskManager.setAssignmentMetadata(taskId0Assignment, emptyMap());
//        taskManager.setPartitionsToTaskId(taskId0PartitionToTaskId);
//        taskManager.createTasks(taskId0Partitions);
//        verify(consumer);
    }

    @Ignore
    @Test
    public void shouldSuspendActiveTasks() {
        throw new RuntimeException();
//        expect(active.suspendOrCloseTasks(revokedTasks, revokedChangelogs)).andReturn(null);
//        expect(restoreConsumer.assignment()).andReturn(Collections.emptySet());
//        replay();

//        taskManager.handleRevocation(revokedPartitions);
//        verify(active);
    }

    @Ignore
    @Test
    @SuppressWarnings("unchecked")
    public void shouldUnassignChangelogPartitionsOnSuspend() {
        throw new RuntimeException();
//        expect(active.suspendOrCloseTasks(revokedTasks, new ArrayList<>()))
//            .andAnswer(() -> {
//                ((List) EasyMock.getCurrentArguments()[1]).add(t1p0);
//                return null;
//            });
//        expect(restoreConsumer.assignment()).andReturn(Collections.singleton(t1p0));
//
//        restoreConsumer.assign(Collections.emptySet());
//        expectLastCall();
//        replay();
//
//        taskManager.handleRevocation(Collections.emptySet());
//        verify(restoreConsumer);
    }

    @Ignore
    @Test
    public void shouldThrowStreamsExceptionAtEndIfExceptionDuringSuspend() {
        throw new RuntimeException();
//        expect(active.suspendOrCloseTasks(revokedTasks, revokedChangelogs)).andReturn(new RuntimeException(""));
//
//        replay();
//        try {
//            taskManager.handleRevocation(revokedPartitions);
//            fail("Should have thrown streams exception");
//        } catch (final StreamsException e) {
//            // expected
//        }
//        verify(restoreConsumer, active, standby);
    }

    @Ignore
    @Test
    public void shouldCloseActiveTasksOnShutdown() {
        throw new RuntimeException();
//        active.shutdown(true);
//        expectLastCall();
//        replay();
//
//        taskManager.shutdown(true);
//        verify(active);
    }

    @Ignore
    @Test
    public void shouldCloseStandbyTasksOnShutdown() {
        throw new RuntimeException();
//        standby.shutdown(false);
//        expectLastCall();
//        replay();
//
//        taskManager.shutdown(false);
//        verify(standby);
    }

    @Ignore
    @Test
    public void shouldUnassignChangelogPartitionsOnShutdown() {
        throw new RuntimeException();
//        restoreConsumer.unsubscribe();
//        expectLastCall();
//        replay();
//
//        taskManager.shutdown(true);
//        verify(restoreConsumer);
    }

    @Ignore
    @Test
    public void shouldInitializeNewActiveTasks() {
        throw new RuntimeException();
//        active.initializeNewTasks();
//        expectLastCall();
//        replay();
//
//        taskManager.updateNewAndRestoringTasks();
//        verify(active);
    }

    @Ignore
    @Test
    public void shouldInitializeNewStandbyTasks() {
        throw new RuntimeException();
//        standby.initializeNewTasks();
//        expectLastCall();
//        replay();
//
//        taskManager.updateNewAndRestoringTasks();
//        verify(standby);
    }

    @Ignore
    @Test
    public void shouldResumeRestoredPartitions() {
        throw new RuntimeException();
//        expect(active.allTasksRunning()).andReturn(true).once();
//        expect(consumer.assignment()).andReturn(taskId0Partitions);
//        expect(standby.running()).andReturn(Collections.emptySet());
//
//        consumer.resume(taskId0Partitions);
//        expectLastCall();
//        replay();
//
//        taskManager.updateNewAndRestoringTasks();
//        verify(consumer);
    }

    @Test
    public void shouldAssignStandbyPartitionsWhenAllActiveTasksAreRunning() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final Task task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));
        expect(standbyTaskCreator.createTasks(anyObject(), eq(taskId01Assignment))).andReturn(singletonList(task01));
        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        restoreConsumer.assign(taskId00Partitions);
        EasyMock.expectLastCall();

        EasyMock.replay(activeTaskCreator, standbyTaskCreator, changeLogReader, restoreConsumer);
        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.updateNewAndRestoringTasks();
        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));

        verify(restoreConsumer);
    }

    @Ignore
    @Test
    public void shouldReturnTrueWhenActiveAndStandbyTasksAreRunning() {
        throw new RuntimeException();
//        mockAssignStandbyPartitions(1L);
//        expect(standby.allTasksRunning()).andReturn(true);
//        replay();
//
//        assertTrue(taskManager.updateNewAndRestoringTasks());
    }

    @Ignore
    @Test
    public void shouldReturnFalseWhenOnlyActiveTasksAreRunning() {
        throw new RuntimeException();
//        mockAssignStandbyPartitions(1L);
//        expect(standby.allTasksRunning()).andReturn(false);
//        replay();
//
//        assertFalse(taskManager.updateNewAndRestoringTasks());
    }

    @Ignore
    @Test
    public void shouldSeekToCheckpointedOffsetOnStandbyPartitionsWhenOffsetGreaterThanEqualTo0() {
        throw new RuntimeException();
//        mockAssignStandbyPartitions(1L);
//        restoreConsumer.seek(t1p0, 1L);
//        expectLastCall();
//        replay();

//        taskManager.updateNewAndRestoringTasks();
//        verify(restoreConsumer);
    }

    @Ignore
    @Test
    public void shouldSeekToBeginningIfOffsetIsLessThan0() {
        throw new RuntimeException();
//        mockAssignStandbyPartitions(-1L);
//        restoreConsumer.seekToBeginning(taskId0Partitions);
//        expectLastCall();
//        replay();
//
//        taskManager.updateNewAndRestoringTasks();
//        verify(restoreConsumer);
    }

    @Ignore
    @Test
    public void shouldCommitActiveAndStandbyTasks() {
        throw new RuntimeException();
//        expect(active.commit()).andReturn(1);
//        expect(standby.commit()).andReturn(2);
//
//        replay();
//
//        assertThat(taskManager.commitAll(), equalTo(3));
//        verify(active, standby);
    }

    @Ignore
    @Test
    public void shouldPropagateExceptionFromActiveCommit() {
        throw new RuntimeException();
//        // upgrade to strict mock to ensure no calls
//        checkOrder(standby, true);
//        active.commit();
//        expectLastCall().andThrow(new RuntimeException(""));
//        replay();
//
//        try {
//            taskManager.commitAll();
//            fail("should have thrown first exception");
//        } catch (final Exception e) {
//            // ok
//        }
//        verify(active, standby);
    }

    @Ignore
    @Test
    public void shouldPropagateExceptionFromStandbyCommit() {
        throw new RuntimeException();
//        expect(standby.commit()).andThrow(new RuntimeException(""));
//        replay();
//
//        try {
//            taskManager.commitAll();
//            fail("should have thrown exception");
//        } catch (final Exception e) {
//            // ok
//        }
//        verify(standby);
    }

    @Ignore
    @Test
    public void shouldSendPurgeData() {
        throw new RuntimeException();
//        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
//        final Map<TopicPartition, RecordsToDelete> recordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
//        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, (KafkaFuture<DeletedRecords>) futureDeletedRecords));
//
//        futureDeletedRecords.complete(null);
//
//        expect(active.recordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).times(2);
//        expect(adminClient.deleteRecords(recordsToDelete)).andReturn(deleteRecordsResult).times(2);
//        replay();
//
//        taskManager.maybePurgeCommittedRecords();
//        taskManager.maybePurgeCommittedRecords();
//        verify(active, adminClient);
    }

    @Ignore
    @Test
    public void shouldNotSendPurgeDataIfPreviousNotDone() {
        throw new RuntimeException();
//        final KafkaFuture<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
//        final Map<TopicPartition, RecordsToDelete> recordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
//        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, futureDeletedRecords));
//
//        expect(active.recordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).once();
//        expect(adminClient.deleteRecords(recordsToDelete)).andReturn(deleteRecordsResult).once();
//        replay();
//
//        taskManager.maybePurgeCommittedRecords();
//        // second call should be no-op as the previous one is not done yet
//        taskManager.maybePurgeCommittedRecords();
//        verify(active, adminClient);
    }

    @Ignore
    @Test
    public void shouldIgnorePurgeDataErrors() {
        throw new RuntimeException();
//        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
//        final Map<TopicPartition, RecordsToDelete> recordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
//        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, (KafkaFuture<DeletedRecords>) futureDeletedRecords));
//
//        futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));
//
//        expect(active.recordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).times(2);
//        expect(adminClient.deleteRecords(recordsToDelete)).andReturn(deleteRecordsResult).times(2);
//        replay();
//
//        taskManager.maybePurgeCommittedRecords();
//        taskManager.maybePurgeCommittedRecords();
//        verify(active, adminClient);
    }

    @Ignore
    @Test
    public void shouldMaybeCommitActiveTasks() {
        throw new RuntimeException();
//        expect(active.maybeCommitPerUserRequested()).andReturn(5);
//        replay();
//
//        assertThat(taskManager.maybeCommitActiveTasksPerUserRequested(), equalTo(5));
//        verify(active);
    }

    @Ignore
    @Test
    public void shouldProcessActiveTasks() {
        throw new RuntimeException();
//        expect(active.process(0L)).andReturn(10);
//        replay();
//
//        assertThat(taskManager.process(0L), equalTo(10));
//        verify(active);
    }

    @Ignore
    @Test
    public void shouldPunctuateActiveTasks() {
        throw new RuntimeException();
//        expect(active.punctuate()).andReturn(20);
//        replay();
//
//        assertThat(taskManager.punctuate(), equalTo(20));
//        verify(active);
    }

    // TODO K9113: the following three tests needs to be fixed once thread calling restore is cleaned
    @Ignore
    @Test
    public void shouldRestoreStateFromChangeLogReader() {
        throw new RuntimeException();
//        expect(active.hasRestoringTasks()).andReturn(true).once();
//        expect(restoreConsumer.assignment()).andReturn(taskId0Partitions).once();
//        active.updateRestored(taskId0Partitions);
//        expectLastCall();
//        replay();
//
//        taskManager.updateNewAndRestoringTasks();
//        verify(changeLogReader, active);
    }

    @Ignore
    @Test
    public void shouldReturnFalseWhenThereAreStillNonRunningTasks() {
        throw new RuntimeException();
//        expect(active.allTasksRunning()).andReturn(false);
//        replay();
//
//        assertFalse(taskManager.updateNewAndRestoringTasks());
    }

    @Ignore
    @Test
    public void shouldNotResumeConsumptionUntilAllStoresRestored() {
        throw new RuntimeException();
//        expect(active.allTasksRunning()).andReturn(false);
//
//        final Consumer<byte[], byte[]> consumer = EasyMock.createStrictMock(Consumer.class);
//        taskManager.setConsumer(consumer);
//        EasyMock.replay(active, consumer, changeLogReader);
//
//        // shouldn't invoke `resume` method in consumer
//        taskManager.updateNewAndRestoringTasks();
//        verify(consumer);
    }

    @Ignore
    @Test
    public void shouldUpdateTasksFromPartitionAssignment() {
        throw new RuntimeException();
//        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
//        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
//
//        taskManager.setAssignmentMetadata(activeTasks, standbyTasks);
//        assertTrue(taskManager.assignedActiveTasks().isEmpty());
//
//        // assign two active tasks with two partitions each
//        activeTasks.put(task01, new HashSet<>(asList(t1p1, t2p1)));
//        activeTasks.put(task02, new HashSet<>(asList(t1p2, t2p2)));
//
//        // assign one standby task with two partitions
//        standbyTasks.put(task03, new HashSet<>(asList(t1p3, t2p3)));
//        taskManager.setAssignmentMetadata(activeTasks, standbyTasks);
//
//        assertThat(taskManager.assignedActiveTasks(), equalTo(activeTasks));
//        assertThat(taskManager.assignedStandbyTasks(), equalTo(standbyTasks));
    }

/*        private void mockAssignStandbyPartitions(final long offset) {
        expect(active.hasRestoringTasks()).andReturn(true).once();
        final StandbyTask task = EasyMock.createNiceMock(StandbyTask.class);
        expect(active.allTasksRunning()).andReturn(true);
        expect(standby.running()).andReturn(singletonList(task));
        expect(task.checkpointedOffsets()).andReturn(Collections.singletonMap(t1p0, offset));
        restoreConsumer.assign(taskId00Partitions);

        expectLastCall();
        EasyMock.replay(task);
    }*/

    private void mockTopologyBuilder() {
        expect(activeTaskCreator.builder()).andReturn(topologyBuilder).anyTimes();
        expect(topologyBuilder.sourceTopicPattern()).andReturn(Pattern.compile("abc"));
        expect(topologyBuilder.subscriptionUpdates()).andReturn(subscriptionUpdates);
    }

    private static class StateMachineTask implements Task {
        private final TaskId id;
        private final Set<TopicPartition> partitions;
        private final boolean active;
        private State state = State.CREATED;

        StateMachineTask(final TaskId id,
                         final Set<TopicPartition> partitions,
                         final boolean active) {
            this.id = id;
            this.partitions = partitions;
            this.active = active;
        }

        @Override
        public State state() {
            return state;
        }

        @Override
        public void transitionTo(final State newState) {
            State.validateTransition(state, newState);
            state = newState;
        }

        @Override
        public void initializeIfNeeded() {
            if (state() == State.CREATED) {
                transitionTo(State.RESTORING);
            }
        }

        @Override
        public void startRunning() {
            transitionTo(State.RUNNING);
        }

        @Override
        public boolean commitNeeded() {
            return false;
        }

        @Override
        public void commit() {

        }

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
            if (!active) {
                transitionTo(State.SUSPENDED);
            }
            transitionTo(State.CLOSED);
        }

        @Override
        public void closeDirty() {
            if (!active) {
                transitionTo(State.SUSPENDED);
            }
            transitionTo(State.CLOSED);
        }

        @Override
        public StateStore getStore(final String name) {
            return null;
        }

        @Override
        public String applicationId() {
            return null;
        }

        @Override
        public ProcessorTopology topology() {
            return null;
        }

        @Override
        public ProcessorContext context() {
            return null;
        }

        @Override
        public TaskId id() {
            return id;
        }

        @Override
        public Set<TopicPartition> partitions() {
            return partitions;
        }

        @Override
        public Collection<TopicPartition> changelogPartitions() {
            return emptyList();
        }

        @Override
        public boolean hasStateStores() {
            return false;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public String toString(final String indent) {
            return null;
        }
    }
}
