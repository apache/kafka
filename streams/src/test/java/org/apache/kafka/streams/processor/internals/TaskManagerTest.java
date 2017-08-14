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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.easymock.EasyMock.checkOrder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.fail;

@RunWith(EasyMockRunner.class)
public class TaskManagerTest {

    private final Time time = new MockTime();
    private final TaskId taskId0 = new TaskId(0, 0);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final TopicPartition t1p1 = new TopicPartition("t1", 1);
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

    private TaskManager taskManager;


    @Before
    public void setUp() throws Exception {
        taskManager = new TaskManager(changeLogReader, time, "", restoreConsumer, activeTaskCreator, standbyTaskCreator);
        taskManager.setThreadMetadataProvider(threadMetadataProvider);
        taskManager.setConsumer(consumer);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCloseActiveUnAssignedSuspendedTasksBeforeCreatingNewTasksWhenTaskPartitionAssignmentHasChanged() {
        final Set<TopicPartition> secondPartitionAssignment = Utils.mkSet(t1p0, t1p1);
        final Map<TaskId, Set<TopicPartition>> secondAssignment = Collections.singletonMap(taskId0, secondPartitionAssignment);

        mockSingleActiveTask();
        expect(activeTaskCreator.retryWithBackoff(EasyMock.anyObject(Consumer.class),
                                                           EasyMock.eq(secondAssignment),
                                                           EasyMock.eq(time.milliseconds())))
                .andReturn(Collections.singletonMap(firstTask, secondPartitionAssignment));

        expect(threadMetadataProvider.activeTasks())
                .andReturn(secondAssignment);

        firstTask.closeSuspended(true, null);
        expectLastCall();

        replay(threadMetadataProvider, activeTaskCreator, standbyTaskCreator, firstTask);

        taskManager.createTasks(taskId0Partitions);
        taskManager.suspendTasksAndState();

        taskManager.createTasks(secondPartitionAssignment);

        verify(activeTaskCreator, firstTask);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void shouldCloseStandbyTaskIfFailureOnSuspend() {
        checkOrder(firstTask, true);
        mockStandbyTaskExpectations(Collections.<TopicPartition, Long>emptyMap());
        verifyTaskIsClosedOnSuspendFailure(Collections.<TopicPartition>emptySet());
    }

    @Test
    public void shouldCloseActiveTaskIfFailureOnSuspend() {
        checkOrder(firstTask, true);
        mockSingleActiveTask();
        verifyTaskIsClosedOnSuspendFailure(taskId0Partitions);
    }

    @Test
    public void shouldInitializeRestoreConsumerWithOffsetsFromStandbyTasks() {
        mockStandbyTaskExpectations(Collections.singletonMap(t1p0, 0L));
        restoreConsumer.assign(EasyMock.eq(taskId0Partitions));
        expectLastCall();
        replay(threadMetadataProvider, firstTask, activeTaskCreator, standbyTaskCreator, restoreConsumer);

        taskManager.createTasks(Collections.<TopicPartition>emptySet());

        EasyMock.verify(restoreConsumer);
    }

    @Test
    public void shouldNotCloseSuspendedTasksTwice() {
        mockSingleActiveTask();
        expect(threadMetadataProvider.activeTasks())
                .andReturn(Collections.<TaskId, Set<TopicPartition>>emptyMap());
        firstTask.suspend();
        expectLastCall();
        firstTask.closeSuspended(true, null);
        expectLastCall();

        replay(threadMetadataProvider, activeTaskCreator, standbyTaskCreator, firstTask);

        taskManager.createTasks(taskId0Partitions);
        taskManager.suspendTasksAndState();

        taskManager.createTasks(Collections.<TopicPartition>emptySet());

        verify(firstTask);
    }

    @Test
    public void shouldNotCloseActiveTaskOnCommitFailedExceptionDuringTaskSuspend() {
        checkOrder(firstTask, true);
        mockSingleActiveTask();
        firstTask.suspend();
        expectLastCall().andThrow(new CommitFailedException());

        replay(threadMetadataProvider, firstTask, activeTaskCreator, standbyTaskCreator);

        taskManager.createTasks(taskId0Partitions);

        taskManager.suspendTasksAndState();
        verify(firstTask);
    }


    @SuppressWarnings("unchecked")
    private void mockStandbyTaskExpectations(final Map<TopicPartition, Long> checkpoint) {
        expect(threadMetadataProvider.standbyTasks())
                .andReturn(taskId0Assignment)
                .anyTimes();
        expect(threadMetadataProvider.activeTasks())
                .andStubReturn(Collections.<TaskId, Set<TopicPartition>>emptyMap());

        expect(standbyTaskCreator.retryWithBackoff(EasyMock.anyObject(Consumer.class),
                                                   EasyMock.eq(taskId0Assignment),
                                                   EasyMock.eq(time.milliseconds())))
                .andReturn(Collections.singletonMap(firstTask, taskId0Partitions));

        stubTaskCreator(activeTaskCreator);

        expect(firstTask.checkpointedOffsets())
                .andReturn(checkpoint)
                .anyTimes();
    }

    @SuppressWarnings("unchecked")
    private void mockSingleActiveTask() {
        expect(threadMetadataProvider.standbyTasks())
                .andReturn(Collections.<TaskId, Set<TopicPartition>>emptyMap())
                .anyTimes();
        expect(threadMetadataProvider.activeTasks())
                .andReturn(taskId0Assignment);

        expect(activeTaskCreator.retryWithBackoff(EasyMock.anyObject(Consumer.class),
                                                  EasyMock.eq(taskId0Assignment),
                                                  EasyMock.eq(time.milliseconds())))
                .andReturn(Collections.singletonMap(firstTask, taskId0Partitions));

        stubTaskCreator(standbyTaskCreator);

        expect(firstTask.id()).andStubReturn(taskId0);
        expect(firstTask.partitions()).andStubReturn(taskId0Partitions);
    }

    private void verifyTaskIsClosedOnSuspendFailure(final Set<TopicPartition> assignment) {
        firstTask.suspend();
        expectLastCall().andThrow(new RuntimeException("KABOOM!"));
        firstTask.close(false);
        expectLastCall();
        replay(threadMetadataProvider, firstTask, activeTaskCreator, standbyTaskCreator);

        taskManager.createTasks(assignment);

        try {
            taskManager.suspendTasksAndState();
            fail("should have thrown StreamsException");
        } catch (final StreamsException e) {
            // pass
        }
        verify(firstTask);
    }

    @SuppressWarnings("unchecked")
    private void stubTaskCreator(final StreamThread.AbstractTaskCreator taskCreator) {
        expect(taskCreator.retryWithBackoff(EasyMock.anyObject(Consumer.class),
                                                   EasyMock.anyObject(Map.class),
                                                   EasyMock.anyLong()))
                .andReturn(Collections.<Task, Set<TopicPartition>>emptyMap())
                .anyTimes();
    }

}