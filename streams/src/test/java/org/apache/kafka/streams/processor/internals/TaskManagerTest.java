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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.streams.errors.StreamsException;
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
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
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
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThrows;

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


    @Mock(type = MockType.STRICT)
    private InternalTopologyBuilder topologyBuilder;
    @Mock(type = MockType.NICE)
    private StateDirectory stateDirectory;
    @Mock(type = MockType.NICE)
    private ChangelogReader changeLogReader;
    @Mock(type = MockType.STRICT)
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
    public void shouldIdempotentlyUpdateSubscriptionFromActiveAssignment() {
        final TopicPartition newTopicPartition = new TopicPartition("topic2", 1);
        final Map<TaskId, Set<TopicPartition>> assignment = mkMap(mkEntry(taskId01, mkSet(t1p1, newTopicPartition)));

        expect(activeTaskCreator.builder()).andReturn(topologyBuilder).anyTimes();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(emptyList()).anyTimes();

        topologyBuilder.addSubscribedTopicsFromAssignment(eq(asList(t1p1, newTopicPartition)), anyString());
        expectLastCall();

        replay(activeTaskCreator,
                        topologyBuilder);

        taskManager.handleAssignment(assignment, emptyMap());

        verify(activeTaskCreator, topologyBuilder);
    }

    @Test
    public void shouldReturnCachedTaskIdsFromDirectory() throws IOException {
        final File[] taskFolders = asList(testFolder.newFolder("0_1"),
                                          testFolder.newFolder("0_2"),
                                          testFolder.newFolder("0_3"),
                                          testFolder.newFolder("1_1"),
                                          testFolder.newFolder("dummy")).toArray(new File[0]);

        assertThat((new File(taskFolders[0], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile(), is(true));
        assertThat((new File(taskFolders[1], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile(), is(true));
        assertThat((new File(taskFolders[3], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile(), is(true));

        expect(activeTaskCreator.stateDirectory()).andReturn(stateDirectory).once();
        expect(stateDirectory.listTaskDirectories()).andReturn(taskFolders).once();

        replay(activeTaskCreator, stateDirectory);

        final Set<TaskId> tasks = taskManager.tasksOnLocalStorage();

        verify(activeTaskCreator, stateDirectory);

        assertThat(tasks, equalTo(mkSet(taskId01, taskId02, new TaskId(1, 1))));
    }

    @Test
    public void shouldCloseActiveUnAssignedSuspendedTasksWhenClosingRevokedTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        expect(standbyTaskCreator.createTasks(anyObject(), anyObject())).andReturn(emptyList()).anyTimes();

        topologyBuilder.addSubscribedTopicsFromAssignment(anyObject(), anyString());
        expectLastCall().anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, topologyBuilder, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());

        taskManager.checkForCompletedRestoration();
        assertThat(task00.state(), is(Task.State.RUNNING));
        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));
        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
    }

    @Test
    public void shouldCloseStandbyUnassignedTasksWhenCreatingNewTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(standbyTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00)).anyTimes();
        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);
        taskManager.handleAssignment(emptyMap(), taskId00Assignment);
        taskManager.checkForCompletedRestoration();
        assertThat(task00.state(), is(Task.State.RUNNING));
        taskManager.handleAssignment(emptyMap(), emptyMap());
        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
    }

    @Test
    public void shouldAddNonResumedSuspendedTasks() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        // expect these calls twice (because we're going to checkForCompletedRestoration twice)
        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));
        replay(activeTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.checkForCompletedRestoration();
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.checkForCompletedRestoration();
        assertThat(task00.state(), is(Task.State.RUNNING));

        verify(activeTaskCreator);
    }

    private static void expectRestoreToBeCompleted(final Consumer<byte[], byte[]> consumer, final ChangelogReader changeLogReader) {
        expect(consumer.assignment()).andReturn(emptySet());
        consumer.resume(emptySet());
        expectLastCall();
        expect(changeLogReader.completedChangelogs()).andReturn(emptySet());
    }

    @Test
    public void shouldAddNewActiveTasks() {
        final Map<TaskId, Set<TopicPartition>> assignment = singletonMap(taskId00, taskId00Partitions);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true);

        changeLogReader.transitToRestoreActive();
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        expect(standbyTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.RESTORING));
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
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
        taskManager.checkForCompletedRestoration();
        assertThat(task00.state(), is(Task.State.RUNNING));

        taskManager.handleRevocation(taskId00Partitions);
        assertThat(task00.state(), is(Task.State.SUSPENDED));
    }

    @Test
    public void shouldThrowStreamsExceptionAtEndIfExceptionDuringSuspend() {
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public void suspend() {
                throw new RuntimeException("asdf");
            }
        };

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment))).andReturn(singletonList(task00));

        replay(activeTaskCreator, consumer, changeLogReader);
        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.checkForCompletedRestoration();
        assertThat(task00.state(), is(Task.State.RUNNING));

        assertThrows(StreamsException.class, () -> taskManager.handleRevocation(taskId00Partitions));
        // TODO K9113: is it safe for the task to still be RUNNING, or should we trap it in SUSPENDING or something?
        assertThat(task00.state(), is(Task.State.RUNNING));
    }

    @Test
    public void shouldCloseActiveTasksOnShutdown() {
        final TopicPartition changelog = new TopicPartition("changelog", 0);
        final Map<TaskId, Set<TopicPartition>> assignment = singletonMap(taskId00, taskId00Partitions);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, true) {
            @Override
            public Collection<TopicPartition> changelogPartitions() {
                return singletonList(changelog);
            }
        };

        EasyMock.resetToStrict(changeLogReader);
        changeLogReader.transitToRestoreActive();
        // make sure we also remove the changelog partitions from the changelog reader
        changeLogReader.remove(eq(singletonList(changelog)));
        expectLastCall();
        expect(activeTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        activeTaskCreator.close();
        expectLastCall();
        expect(standbyTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(assignment, emptyMap());

        assertThat(task00.state(), is(Task.State.RESTORING));
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());

        taskManager.shutdown(true);

        assertThat(task00.state(), is(Task.State.CLOSED));
        assertThat(taskManager.activeTaskMap(), Matchers.anEmptyMap());
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
        // the active task creator should also get closed (so that it closes the thread producer if applicable)
        verify(activeTaskCreator, changeLogReader);
    }

    @Test
    public void shouldCloseStandbyTasksOnShutdown() {
        final Map<TaskId, Set<TopicPartition>> assignment = singletonMap(taskId00, taskId00Partitions);
        final Task task00 = new StateMachineTask(taskId00, taskId00Partitions, false);

        expect(activeTaskCreator.createTasks(anyObject(), eq(emptyMap()))).andReturn(emptyList()).anyTimes();
        activeTaskCreator.close();
        expectLastCall();
        expect(standbyTaskCreator.createTasks(anyObject(), eq(assignment))).andReturn(singletonList(task00)).anyTimes();
        replay(activeTaskCreator, standbyTaskCreator, changeLogReader);

        taskManager.handleAssignment(emptyMap(), assignment);

        assertThat(task00.state(), is(Task.State.RESTORING));
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
        taskManager.checkForCompletedRestoration();

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(taskManager.activeTaskMap(), Matchers.equalTo(singletonMap(taskId00, task00)));
        assertThat(taskManager.standbyTaskMap(), Matchers.anEmptyMap());
    }

    @Test
    public void shouldInitializeNewStandbyTasks() {
        final StateMachineTask task01 = new StateMachineTask(taskId01, taskId01Partitions, false);

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(standbyTaskCreator.createTasks(anyObject(), eq(taskId01Assignment)))
            .andReturn(singletonList(task01)).anyTimes();

        replay(standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(emptyMap(), taskId01Assignment);
        taskManager.checkForCompletedRestoration();

        assertThat(task01.state(), is(Task.State.RUNNING));
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

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();
        expect(standbyTaskCreator.createTasks(anyObject(), eq(taskId01Assignment)))
            .andReturn(singletonList(task01)).anyTimes();

        replay(activeTaskCreator, standbyTaskCreator, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, taskId01Assignment);
        taskManager.checkForCompletedRestoration();

        assertThat(task00.state(), is(Task.State.RUNNING));
        assertThat(task01.state(), is(Task.State.RUNNING));

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

        expectRestoreToBeCompleted(consumer, changeLogReader);
        expect(activeTaskCreator.createTasks(anyObject(), eq(taskId00Assignment)))
            .andReturn(singletonList(task00)).anyTimes();

        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(singletonMap(t1p1, futureDeletedRecords));
        futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));
        expect(adminClient.deleteRecords(anyObject())).andReturn(deleteRecordsResult).times(2);

        replay(activeTaskCreator, adminClient, consumer, changeLogReader);

        taskManager.handleAssignment(taskId00Assignment, emptyMap());
        taskManager.checkForCompletedRestoration();

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
        taskManager.checkForCompletedRestoration();

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
        taskManager.checkForCompletedRestoration();

        assertThat(task00.state(), is(Task.State.RUNNING));

        final TopicPartition partition = taskId00Partitions.iterator().next();
        task00.addRecords(
            partition,
            singletonList(new ConsumerRecord<>(partition.topic(), partition.partition(), 0L, null, null))
        );

        assertThat(taskManager.process(0L), is(1));
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
        taskManager.checkForCompletedRestoration();

        assertThat(task00.state(), is(Task.State.RUNNING));

        // one for stream and one for system time
        assertThat(taskManager.punctuate(), equalTo(2));
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
        assertThat(taskManager.checkForCompletedRestoration(), is(false));
        assertThat(task00.state(), is(Task.State.RESTORING));
        // this could be a bit mysterious; we're verifying _no_ interactions on the consumer,
        // since the taskManager should _not_ resume the assignment while we're still in RESTORING
        verify(consumer);
    }

    private static class StateMachineTask extends AbstractTask implements Task {
        private final TaskId id;
        private final Set<TopicPartition> partitions;
        private final boolean active;
        private boolean commitNeeded = false;
        private boolean commitRequested = false;
        private Map<TopicPartition, Long> purgeableOffsets;
        private Map<TopicPartition, LinkedList<ConsumerRecord<byte[], byte[]>>> queue = new HashMap<>();

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
        public void completeInitializationAfterRestore() {
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
