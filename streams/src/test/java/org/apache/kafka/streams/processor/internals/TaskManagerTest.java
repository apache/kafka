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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.easymock.EasyMock.checkOrder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EasyMockRunner.class)
public class TaskManagerTest {

    private final TaskId taskId0 = new TaskId(0, 0);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final Set<TopicPartition> taskId0Partitions = Utils.mkSet(t1p0);
    private final Map<TaskId, Set<TopicPartition>> taskId0Assignment = Collections.singletonMap(taskId0, taskId0Partitions);

    @Mock(type = MockType.NICE)
    private ChangelogReader changeLogReader;
    @Mock(type = MockType.NICE)
    private Consumer<byte[], byte[]> restoreConsumer;
    @Mock(type = MockType.NICE)
    private Consumer<byte[], byte[]> consumer;
    @Mock(type = MockType.NICE)
    private StreamThread.AbstractTaskCreator activeTaskCreator;
    @Mock(type = MockType.NICE)
    private StreamThread.AbstractTaskCreator standbyTaskCreator;
    @Mock(type = MockType.NICE)
    private ThreadMetadataProvider threadMetadataProvider;
    @Mock(type = MockType.NICE)
    private Task firstTask;
    @Mock(type = MockType.NICE)
    private AssignedTasks active;
    @Mock(type = MockType.NICE)
    private AssignedTasks standby;

    private TaskManager taskManager;


    @Before
    public void setUp() throws Exception {
        taskManager = new TaskManager(changeLogReader,
                                      "",
                                      restoreConsumer,
                                      activeTaskCreator,
                                      standbyTaskCreator,
                                      active,
                                      standby);
        taskManager.setThreadMetadataProvider(threadMetadataProvider);
        taskManager.setConsumer(consumer);
    }

    private void replay() {
        EasyMock.replay(changeLogReader,
                        restoreConsumer,
                        consumer,
                        activeTaskCreator,
                        standbyTaskCreator,
                        threadMetadataProvider,
                        active,
                        standby);
    }

    @Test
    public void shouldCloseActiveUnAssignedSuspendedTasksWhenCreatingNewTasks() {
        mockSingleActiveTask();
        active.closeNonAssignedSuspendedTasks(taskId0Assignment);
        expectLastCall();
        replay();

        taskManager.createTasks(taskId0Partitions);

        verify(active);
    }

    @Test
    public void shouldCloseStandbyUnAssignedSuspendedTasksWhenCreatingNewTasks() {
        mockSingleActiveTask();
        standby.closeNonAssignedSuspendedTasks(taskId0Assignment);
        expectLastCall();
        replay();

        taskManager.createTasks(taskId0Partitions);

        verify(active);
    }

    @Test
    public void shouldResetChangeLogReaderOnCreateTasks() {
        mockSingleActiveTask();
        changeLogReader.reset();
        EasyMock.expectLastCall();
        replay();

        taskManager.createTasks(taskId0Partitions);
        verify(changeLogReader);
    }

    @Test
    public void shouldAddNonResumedActiveTasks() {
        mockSingleActiveTask();
        EasyMock.expect(active.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(false);
        active.addNewTask(EasyMock.same(firstTask));
        replay();

        taskManager.createTasks(taskId0Partitions);

        verify(activeTaskCreator, active);
    }

    @Test
    public void shouldNotAddResumedActiveTasks() {
        checkOrder(active, true);
        mockThreadMetadataProvider(Collections.<TaskId, Set<TopicPartition>>emptyMap(), taskId0Assignment);
        EasyMock.expect(active.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
        replay();

        taskManager.createTasks(taskId0Partitions);

        // should be no calls to activeTaskCreator and no calls to active.addNewTasks(..)
        verify(active, activeTaskCreator);
    }

    @Test
    public void shouldAddNonResumedStandbyTasks() {
        mockStandbyTaskExpectations();
        EasyMock.expect(standby.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(false);
        standby.addNewTask(EasyMock.same(firstTask));
        replay();

        taskManager.createTasks(taskId0Partitions);

        verify(standbyTaskCreator, active);
    }

    @Test
    public void shouldNotAddResumedStandbyTasks() {
        checkOrder(active, true);
        mockThreadMetadataProvider(taskId0Assignment, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        EasyMock.expect(standby.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
        replay();

        taskManager.createTasks(taskId0Partitions);

        // should be no calls to standbyTaskCreator and no calls to standby.addNewTasks(..)
        verify(standby, standbyTaskCreator);
    }


    @Test
    public void shouldPauseActiveUninitializedPartitions() {
        mockSingleActiveTask();
        EasyMock.expect(active.uninitializedPartitions()).andReturn(taskId0Partitions);
        consumer.pause(taskId0Partitions);
        EasyMock.expectLastCall();
        replay();

        taskManager.createTasks(taskId0Partitions);
        verify(consumer);
    }

    @Test
    public void shouldSuspendActiveTasks() {
        EasyMock.expect(active.suspend()).andReturn(null);
        replay();

        taskManager.suspendTasksAndState();
        verify(active);
    }

    @Test
    public void shouldSuspendStandbyTasks() {
        EasyMock.expect(standby.suspend()).andReturn(null);
        replay();

        taskManager.suspendTasksAndState();
        verify(standby);
    }

    @Test
    public void shouldUnassignChangelogPartitionsOnSuspend() {
        restoreConsumer.assign(Collections.<TopicPartition>emptyList());
        EasyMock.expectLastCall();
        replay();

        taskManager.suspendTasksAndState();
        verify(restoreConsumer);
    }

    @Test
    public void shouldThrowStreamsExceptionAtEndIfExceptionDuringSuspend() {
        EasyMock.expect(active.suspend()).andReturn(new RuntimeException(""));
        EasyMock.expect(standby.suspend()).andReturn(new RuntimeException(""));
        EasyMock.expectLastCall();
        restoreConsumer.assign(Collections.<TopicPartition>emptyList());

        replay();
        try {
            taskManager.suspendTasksAndState();
            fail("Should have thrown streams exception");
        } catch (StreamsException e) {
            // expected
        }
        verify(restoreConsumer, active, standby);
    }

    @Test
    public void shouldCloseActiveTasksOnShutdown() {
        active.close(true);
        EasyMock.expectLastCall();
        replay();

        taskManager.shutdown(true);
        verify(active);
    }

    @Test
    public void shouldCloseStandbyTasksOnShutdown() {
        standby.close(false);
        EasyMock.expectLastCall();
        replay();

        taskManager.shutdown(false);
        verify(standby);
    }

    @Test
    public void shouldUnassignChangelogPartitionsOnShutdown() {
        restoreConsumer.assign(Collections.<TopicPartition>emptyList());
        EasyMock.expectLastCall();
        replay();

        taskManager.shutdown(true);
        verify(restoreConsumer);
    }

    @Test
    public void shouldCloseThreadMetadataProviderOnShutdown() {
        threadMetadataProvider.close();
        EasyMock.expectLastCall();
        replay();

        taskManager.shutdown(true);
        verify(threadMetadataProvider);
    }

    @Test
    public void shouldNotPropagateExceptionsOnShutdown() {
        threadMetadataProvider.close();
        EasyMock.expectLastCall().andThrow(new RuntimeException());
        replay();

        taskManager.shutdown(false);
    }

    @Test
    public void shouldInitializeNewActiveTasks() {
        EasyMock.expect(active.initializeNewTasks()).andReturn(new HashSet<TopicPartition>());
        EasyMock.expect(active.updateRestored(EasyMock.<Collection<TopicPartition>>anyObject())).
                andReturn(Collections.<TopicPartition>emptySet());
        EasyMock.expectLastCall();
        replay();
        taskManager.updateNewAndRestoringTasks();
        verify(active);
    }

    @Test
    public void shouldInitializeNewStandbyTasks() {
        EasyMock.expect(standby.initializeNewTasks()).andReturn(new HashSet<TopicPartition>());
        EasyMock.expect(active.initializeNewTasks()).andReturn(new HashSet<TopicPartition>());
        EasyMock.expect(active.updateRestored(EasyMock.<Collection<TopicPartition>>anyObject())).
                andReturn(Collections.<TopicPartition>emptySet());
        EasyMock.expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(standby);
    }

    @Test
    public void shouldRestoreStateFromChangeLogReader() {
        EasyMock.expect(active.initializeNewTasks()).andReturn(new HashSet<TopicPartition>());
        EasyMock.expect(changeLogReader.restore(active)).andReturn(taskId0Partitions);
        EasyMock.expect(active.updateRestored(taskId0Partitions)).
                andReturn(Collections.<TopicPartition>emptySet());

        replay();
        taskManager.updateNewAndRestoringTasks();
        verify(changeLogReader, active);
    }

    @Test
    public void shouldResumeRestoredPartitions() {
        EasyMock.expect(active.initializeNewTasks()).andReturn(new HashSet<TopicPartition>());
        EasyMock.expect(changeLogReader.restore(active)).andReturn(taskId0Partitions);
        EasyMock.expect(active.updateRestored(taskId0Partitions)).
                andReturn(taskId0Partitions);

        consumer.resume(taskId0Partitions);
        EasyMock.expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(consumer);
    }

    @Test
    public void shouldAssignStandbyPartitionsWhenAllActiveTasksAreRunning() {
        mockAssignStandbyPartitions(1L);
        replay();

        assertTrue(taskManager.updateNewAndRestoringTasks());
        verify(restoreConsumer);
    }

    @Test
    public void shouldReturnFalseWhenThereAreStillNonRunningTasks() {
        EasyMock.expect(active.initializeNewTasks()).andReturn(new HashSet<TopicPartition>());
        EasyMock.expect(active.allTasksRunning()).andReturn(false);
        EasyMock.expect(active.updateRestored(EasyMock.<Collection<TopicPartition>>anyObject())).
                andReturn(Collections.<TopicPartition>emptySet());
        replay();

        assertFalse(taskManager.updateNewAndRestoringTasks());
    }

    @Test
    public void shouldSeekToCheckpointedOffsetOnStandbyPartitionsWhenOffsetGreaterThanEqualTo0() {
        mockAssignStandbyPartitions(1L);
        restoreConsumer.seek(t1p0, 1L);
        EasyMock.expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(restoreConsumer);
    }

    @Test
    public void shouldSeekToBeginningIfOffsetIsLessThan0() {
        mockAssignStandbyPartitions(-1L);
        restoreConsumer.seekToBeginning(taskId0Partitions);
        EasyMock.expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(restoreConsumer);
    }

    @Test
    public void shouldCommitActiveAndStandbyTasks() {
        EasyMock.expect(active.commit()).andReturn(1);
        EasyMock.expect(standby.commit()).andReturn(2);

        replay();

        assertThat(taskManager.commitAll(), equalTo(3));
        verify(active, standby);
    }

    @Test
    public void shouldPropagateExceptionFromActiveCommit() {
        // upgrade to strict mock to ensure no calls
        checkOrder(standby, true);
        active.commit();
        EasyMock.expectLastCall().andThrow(new RuntimeException(""));
        replay();

        try {
            taskManager.commitAll();
            fail("should have thrown first exception");
        } catch (Exception e) {
            // ok
        }
        verify(active, standby);
    }

    @Test
    public void shouldPropagateExceptionFromStandbyCommit() {
        EasyMock.expect(standby.commit()).andThrow(new RuntimeException(""));
        replay();

        try {
            taskManager.commitAll();
            fail("should have thrown exception");
        } catch (Exception e) {
            // ok
        }
        verify(standby);
    }

    @Test
    public void shouldMaybeCommitActiveTasks() {
        EasyMock.expect(active.maybeCommit()).andReturn(5);
        replay();

        assertThat(taskManager.maybeCommitActiveTasks(), equalTo(5));
        verify(active);
    }

    @Test
    public void shouldProcessActiveTasks() {
        EasyMock.expect(active.process()).andReturn(10);
        replay();

        assertThat(taskManager.process(), equalTo(10));
        verify(active);
    }

    @Test
    public void shouldPunctuateActiveTasks() {
        EasyMock.expect(active.punctuate()).andReturn(20);
        replay();

        assertThat(taskManager.punctuate(), equalTo(20));
        verify(active);
    }

    @Test
    public void shouldResumeConsumptionOfInitializedPartitions() {
        final Set<TopicPartition> resumed = Collections.singleton(new TopicPartition("topic", 0));
        EasyMock.expect(active.initializeNewTasks()).andReturn(resumed);
        EasyMock.expect(active.updateRestored(EasyMock.<Collection<TopicPartition>>anyObject())).
                andReturn(Collections.<TopicPartition>emptySet());
        consumer.resume(resumed);
        EasyMock.expectLastCall();

        EasyMock.replay(active, consumer);

        taskManager.updateNewAndRestoringTasks();
        EasyMock.verify(consumer);
    }

    private void mockAssignStandbyPartitions(final long offset) {
        final Task task = EasyMock.createNiceMock(Task.class);
        EasyMock.expect(active.initializeNewTasks()).andReturn(new HashSet<TopicPartition>());
        EasyMock.expect(active.allTasksRunning()).andReturn(true);
        EasyMock.expect(active.updateRestored(EasyMock.<Collection<TopicPartition>>anyObject())).
                andReturn(Collections.<TopicPartition>emptySet());
        EasyMock.expect(standby.running()).andReturn(Collections.singletonList(task));
        EasyMock.expect(task.checkpointedOffsets()).andReturn(Collections.singletonMap(t1p0, offset));
        restoreConsumer.assign(taskId0Partitions);

        EasyMock.expectLastCall();
        EasyMock.replay(task);
    }



    private void mockStandbyTaskExpectations() {
        mockThreadMetadataProvider(taskId0Assignment, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        expect(standbyTaskCreator.createTasks(EasyMock.<Consumer<byte[], byte[]>>anyObject(),
                                                   EasyMock.eq(taskId0Assignment)))
                .andReturn(Collections.singletonList(firstTask));

    }

    @SuppressWarnings("unchecked")
    private void mockSingleActiveTask() {
        mockThreadMetadataProvider(Collections.<TaskId, Set<TopicPartition>>emptyMap(), taskId0Assignment);

        expect(activeTaskCreator.createTasks(EasyMock.anyObject(Consumer.class),
                                                  EasyMock.eq(taskId0Assignment)))
                .andReturn(Collections.singletonList(firstTask));

    }

    private void mockThreadMetadataProvider(final Map<TaskId, Set<TopicPartition>> standbyAssignment,
                                            final Map<TaskId, Set<TopicPartition>> activeAssignment) {
        expect(threadMetadataProvider.standbyTasks())
                .andReturn(standbyAssignment)
                .anyTimes();
        expect(threadMetadataProvider.activeTasks())
                .andReturn(activeAssignment)
                .anyTimes();
    }

}