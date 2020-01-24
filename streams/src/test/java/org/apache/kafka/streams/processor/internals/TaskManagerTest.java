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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;
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

    private static final String topic1 = "topic1";

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

    @Mock(type = MockType.STRICT)
    private InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates;
    @Mock(type = MockType.STRICT)
    private InternalTopologyBuilder topologyBuilder;
    @Mock(type = MockType.NICE)
    private StateDirectory stateDirectory;
    @Mock(type = MockType.NICE)
    private ChangelogReader changeLogReader;
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

    private TaskManager taskManager;

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        taskManager = new TaskManager(changeLogReader,
                                      UUID.randomUUID(),
                                      "",
                                      activeTaskCreator,
                                      standbyTaskCreator,
                                      topologyBuilder,
                                      adminClient);
        taskManager.setConsumer(consumer);
    }

    @Test
    public void shouldUpdateSubscriptionFromAssignmentOfNewTopic2() {
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(mkEntry(taskId01, mkSet(t1p1, new TopicPartition("topic2", 1))));

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

        final Set<TaskId> tasks = taskManager.tasksOnLocalStorage();

        verify(activeTaskCreator, stateDirectory);

        assertThat(tasks, equalTo(Utils.mkSet(taskId01, taskId02, new TaskId(1, 1))));
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
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
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
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
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
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        verify(activeTaskCreator);
    }

    @Test
    public void shouldSuspendActiveTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));
        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);

        EasyMock.replay(activeTaskCreator, changeLogReader);
        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.updateNewAndRestoringTasks();
        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), Matchers.is(Task.State.SUSPENDED));
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

    @Test
    public void shouldCloseActiveTasksOnShutdown() {
        final Map<TaskId, Set<TopicPartition>> assignment = singletonMap(taskId00, taskId00Partitions);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        activeTaskCreator.close();
        EasyMock.expectLastCall();
        expect(standbyTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        EasyMock.replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), Matchers.is(Task.State.CREATED));
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());

        taskManager.shutdown(true);

        assertThat(task00.state(), Matchers.is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator);
    }

    @Test
    public void shouldCloseStandbyTasksOnShutdown() {
        final Map<TaskId, Set<TopicPartition>> assignment = singletonMap(taskId00, taskId00Partitions);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false);

//        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        activeTaskCreator.close();
        EasyMock.expectLastCall();
        expect(standbyTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        EasyMock.replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(emptyMap(), assignment);

        assertThat(task00.state(), Matchers.is(Task.State.CREATED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));

        taskManager.shutdown(true);

        assertThat(task00.state(), Matchers.is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator);
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

    @Test
    public void shouldInitializeNewActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();

        EasyMock.replay(activeTaskCreator, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.updateNewAndRestoringTasks();

        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
    }

    @Test
    public void shouldInitializeNewStandbyTasks() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(standbyTaskCreator.createTasks(anyObject(), eq(taskId01Assignment)))
            .andReturn(singletonList(task01)).anyTimes();

        EasyMock.replay(standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(emptyMap(), taskId01Assignment);
        taskManager.updateNewAndRestoringTasks();

        assertThat(task01.state(), Matchers.is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.equalTo(singletonMap(taskId01, task01)));
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

    @Test
    public void shouldCommitActiveAndStandbyTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();
        expect(standbyTaskCreator.createTasks(anyObject(), eq(taskId01Assignment)))
            .andReturn(singletonList(task01)).anyTimes();

        EasyMock.replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        taskManager.updateNewAndRestoringTasks();

        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));
        assertThat(task01.state(), Matchers.is(Task.State.RUNNING));

        task00.setCommitNeeded();
        task01.setCommitNeeded();

        assertThat(taskManager.commitAll(), equalTo(2));
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

    @Test
    public void shouldIgnorePurgeDataErrors() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();

        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords));
        futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));
        expect(adminClient.deleteRecords(anyObject())).andReturn(deleteRecordsResult).times(2);

        EasyMock.replay(activeTaskCreator, changeLogReader, adminClient);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.updateNewAndRestoringTasks();

        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));

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

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment)))
            .andReturn(asList(task00, task01, task02)).anyTimes();

        EasyMock.replay(activeTaskCreator, standbyTaskCreator, changeLogReader);


        taskManager.handleAssignment(assignment, emptyMap());
        taskManager.updateNewAndRestoringTasks();

        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));
        assertThat(task01.state(), Matchers.is(Task.State.RUNNING));
        assertThat(task02.state(), Matchers.is(Task.State.RUNNING));

        task00.setCommitNeeded();
        task00.setCommitRequested();

        task01.setCommitNeeded();

        task02.setCommitRequested();

        assertThat(taskManager.maybeCommitActiveTasksPerUserRequested(), equalTo(1));
    }

    @Ignore
    @Test
    public void shouldProcessActiveTasks() {
        final StateMachineTask task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expect(changeLogReader.completedChangelogs()).andReturn(taskId00Partitions);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();

        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords));
        futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));
        expect(adminClient.deleteRecords(anyObject())).andReturn(deleteRecordsResult).times(2);

        EasyMock.replay(activeTaskCreator, changeLogReader, adminClient);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.updateNewAndRestoringTasks();

        assertThat(task00.state(), Matchers.is(Task.State.RUNNING));

        taskManager.process(0L);
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
<<<<<<< HEAD
        expect(standby.running()).andReturn(singletonList(task));
        expect(task.checkpointedOffsets()).andReturn(Collections.singletonMap(t1p0, offset));
        restoreConsumer.assign(taskId00Partitions);
=======
        expect(standby.running()).andReturn(Collections.singletonList(task));
        expect(task.restoredOffsets()).andReturn(Collections.singletonMap(t1p0, offset));
        restoreConsumer.assign(taskId0Partitions);
>>>>>>> 8e72390d66fc25cc06a0bb11c172a876bf6106f6

        expectLastCall();
        EasyMock.replay(task);
    }*/

    private void mockTopologyBuilder() {
        expect(activeTaskCreator.builder()).andReturn(topologyBuilder).anyTimes();
        expect(topologyBuilder.sourceTopicPattern()).andReturn(Pattern.compile("abc"));
        expect(topologyBuilder.subscriptionUpdates()).andReturn(subscriptionUpdates);
    }

    private static class StateMachineTask extends AbstractTask implements Task {
        private final TaskId id;
        private final Set<TopicPartition> partitions;
        private final boolean active;
        private boolean commitNeeded = false;
        private boolean commitRequested = false;
        private Map<TopicPartition, Long> purgeableOffsets;

        StateMachineTask(final TaskId id,
                         final Set<TopicPartition> partitions,
                         final boolean active) {
            this.id = id;
            this.partitions = partitions;
            this.active = active;
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

        public void setCommitNeeded() {
            this.commitNeeded = true;
        }

        @Override
        public boolean commitNeeded() {
            return this.commitNeeded;
        }

        public void setCommitRequested() {
            this.commitRequested = true;
        }

        @Override
        public boolean commitRequested() {
            return commitRequested;
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
            transitionTo(State.CLOSED);
        }

        @Override
        public void closeDirty() {
            transitionTo(State.CLOSED);
        }

        @Override
        public StateStore getStore(final String name) {
            return null;
        }

        @Override
        public TaskId id() {
            return id;
        }

        @Override
        public Set<TopicPartition> inputPartitions() {
            return partitions;
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
        public Map<TopicPartition, Long> purgableOffsets() {
            return purgeableOffsets;
        }

        @Override
        public Map<TopicPartition, Long> changelogOffsets() {
            return null;
        }
    }
}
