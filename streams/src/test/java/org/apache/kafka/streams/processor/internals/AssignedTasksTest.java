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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AssignedTasksTest {

    private final AssignedTasks<AbstractTask> assignedTasks = new AssignedTasks<>("log", "task");
    private final AbstractTask t1 = EasyMock.createMock(AbstractTask.class);
    private final AbstractTask t2 = EasyMock.createMock(AbstractTask.class);
    private final TopicPartition tp1 = new TopicPartition("t1", 0);
    private final TopicPartition tp2 = new TopicPartition("t2", 0);
    private final TopicPartition changeLog1 = new TopicPartition("cl1", 0);
    private final TopicPartition changeLog2 = new TopicPartition("cl2", 0);
    private final TaskId taskId1 = new TaskId(0, 0);
    private final TaskId taskId2 = new TaskId(1, 0);

    @Before
    public void before() {
        EasyMock.expect(t1.id()).andReturn(taskId1).anyTimes();
        EasyMock.expect(t2.id()).andReturn(taskId2).anyTimes();
    }

    @Test
    public void shouldGetPartitionsFromNewTasksThatHaveStateStores() {
        EasyMock.expect(t1.hasStateStores()).andReturn(true);
        EasyMock.expect(t2.hasStateStores()).andReturn(true);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
        EasyMock.replay(t1, t2);

        assignedTasks.addNewTask(t1);
        assignedTasks.addNewTask(t2);

        final Set<TopicPartition> partitions = assignedTasks.uninitializedPartitions();
        assertThat(partitions, equalTo(Utils.mkSet(tp1, tp2)));
        EasyMock.verify(t1, t2);
    }

    @Test
    public void shouldNotGetPartitionsFromNewTasksWithoutStateStores() {
        EasyMock.expect(t1.hasStateStores()).andReturn(false);
        EasyMock.expect(t2.hasStateStores()).andReturn(false);
        EasyMock.replay(t1, t2);

        assignedTasks.addNewTask(t1);
        assignedTasks.addNewTask(t2);

        final Set<TopicPartition> partitions = assignedTasks.uninitializedPartitions();
        assertTrue(partitions.isEmpty());
        EasyMock.verify(t1, t2);
    }

    @Test
    public void shouldInitializeNewTasks() {
        EasyMock.expect(t1.initialize()).andReturn(false);
        EasyMock.replay(t1);

        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();

        EasyMock.verify(t1);
    }

    @Test
    public void shouldMoveInitializedTasksNeedingRestoreToRestoring() {
        EasyMock.expect(t1.initialize()).andReturn(false);
        EasyMock.expect(t2.initialize()).andReturn(true);
        EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.<TopicPartition>emptyList());

        EasyMock.replay(t1, t2);

        assignedTasks.addNewTask(t1);
        assignedTasks.addNewTask(t2);

        assignedTasks.initializeNewTasks();

        Collection<AbstractTask> restoring = assignedTasks.restoring();
        assertThat(restoring.size(), equalTo(1));
        assertSame(restoring.iterator().next(), t1);
    }

    @Test
    public void shouldMoveInitializedTasksThatDontNeedRestoringToRunning() {
        EasyMock.expect(t2.initialize()).andReturn(true);
        EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.<TopicPartition>emptyList());

        EasyMock.replay(t2);

        assignedTasks.addNewTask(t2);
        assignedTasks.initializeNewTasks();

        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId2)));
    }

    @Test
    public void shouldTransitionFullyRestoredTasksToRunning() {
        final Set<TopicPartition> task1Partitions = Utils.mkSet(tp1);
        EasyMock.expect(t1.initialize()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(task1Partitions).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Utils.mkSet(changeLog1, changeLog2)).anyTimes();
        EasyMock.replay(t1);

        assignedTasks.addNewTask(t1);

        assignedTasks.initializeNewTasks();

        assertTrue(assignedTasks.updateRestored(Utils.mkSet(changeLog1)).isEmpty());
        Set<TopicPartition> partitions = assignedTasks.updateRestored(Utils.mkSet(changeLog2));
        assertThat(partitions, equalTo(task1Partitions));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
    }

    @Test
    public void shouldSuspendRunningTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        suspendTask();

        assertThat(assignedTasks.previousTasks(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotSuspendUnInitializedTasks() {
        EasyMock.replay(t1);

        assignedTasks.addNewTask(t1);
        assignedTasks.suspend();

        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotSuspendSuspendedTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        suspendTask();
        assignedTasks.suspend();
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnSuspendWhenRuntimeException() {
        mockInitializedTask();
        t1.suspend();
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!"));
        t1.close(false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assertThat(suspendTask(), not(nullValue()));
        assertTrue(assignedTasks.previousTasks().isEmpty());
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnSuspendWhenProducerFencedException() {
        mockInitializedTask();
        t1.suspend();
        EasyMock.expectLastCall().andThrow(new ProducerFencedException("KABOOM!"));
        t1.close(false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());
        assertTrue(assignedTasks.previousTasks().isEmpty());
        EasyMock.verify(t1);
    }

    private void mockInitializedTask() {
        EasyMock.expect(t1.initialize()).andReturn(true);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.<TopicPartition>emptyList());
    }

    @Test
    public void shouldResumeMatchingSuspendedTasks() {
        mockRunningTaskSuspension();
        t1.resume();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        suspendTask();

        assertTrue(assignedTasks.maybeResumeSuspendedTask(taskId1, Collections.singleton(tp1)));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        assertTrue(assignedTasks.previousTasks().isEmpty());
        EasyMock.verify(t1);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void shouldApplyActionToRunningTasks() {
        StreamThread.TaskAction taskAction = EasyMock.createMock(StreamThread.TaskAction.class);
        taskAction.apply(EasyMock.anyObject(AbstractTask.class));
        EasyMock.expectLastCall();
        mockInitializedTask();

        EasyMock.replay(t1, taskAction);

        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();

        assignedTasks.applyToRunningTasks(taskAction, false);

        EasyMock.verify(taskAction);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotApplyActionToNotRunningTasks() {
        final StreamThread.TaskAction taskAction = EasyMock.createMock(StreamThread.TaskAction.class);
        EasyMock.expect(t1.initialize()).andReturn(false);

        EasyMock.replay(t1, taskAction);

        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();

        assignedTasks.applyToRunningTasks(taskAction, false);

        EasyMock.verify(taskAction);
    }

    @SuppressWarnings({"unchecked", "ThrowableNotThrown"})
    @Test
    public void shouldCloseTaskOnApplyToRunningIfProducerFencedException() {
        final StreamThread.TaskAction taskAction = EasyMock.createMock(StreamThread.TaskAction.class);
        taskAction.apply(EasyMock.anyObject(AbstractTask.class));
        EasyMock.expectLastCall().andThrow(new ProducerFencedException("BOOM!"));
        mockInitializedTask();
        t1.close(false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1, taskAction);

        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();

        assignedTasks.applyToRunningTasks(taskAction, false);
        assertTrue(assignedTasks.running().isEmpty());
        EasyMock.verify(t1, taskAction);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReturnExceptionAndNotCloseTaskOnApplyToRunningWhenRuntimeException() {
        final StreamThread.TaskAction taskAction = EasyMock.createMock(StreamThread.TaskAction.class);
        taskAction.apply(EasyMock.anyObject(AbstractTask.class));
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!"));
        EasyMock.expect(taskAction.name()).andReturn("name");
        mockInitializedTask();
        EasyMock.replay(t1, taskAction);

        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();

        assertThat(assignedTasks.applyToRunningTasks(taskAction, false), not(nullValue()));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1, taskAction);
    }

    private RuntimeException suspendTask() {
        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();
        return assignedTasks.suspend();
    }

    private void mockRunningTaskSuspension() {
        EasyMock.expect(t1.initialize()).andReturn(true);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1)).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.<TopicPartition>emptyList()).anyTimes();
        t1.suspend();
        EasyMock.expectLastCall();
    }

}