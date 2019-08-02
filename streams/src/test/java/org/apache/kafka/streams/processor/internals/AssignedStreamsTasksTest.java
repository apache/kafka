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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TaskMigratedException;
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
import static org.junit.Assert.fail;

public class AssignedStreamsTasksTest {

    private final StreamTask t1 = EasyMock.createMock(StreamTask.class);
    private final StreamTask t2 = EasyMock.createMock(StreamTask.class);
    private final TopicPartition tp1 = new TopicPartition("t1", 0);
    private final TopicPartition tp2 = new TopicPartition("t2", 0);
    private final TopicPartition changeLog1 = new TopicPartition("cl1", 0);
    private final TopicPartition changeLog2 = new TopicPartition("cl2", 0);
    private final TaskId taskId1 = new TaskId(0, 0);
    private final TaskId taskId2 = new TaskId(1, 0);
    private AssignedStreamsTasks assignedTasks;

    @Before
    public void before() {
        assignedTasks = new AssignedStreamsTasks(new LogContext("log "));
        EasyMock.expect(t1.id()).andReturn(taskId1).anyTimes();
        EasyMock.expect(t2.id()).andReturn(taskId2).anyTimes();
    }

    @Test
    public void shouldInitializeNewTasks() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
        EasyMock.replay(t1);

        addAndInitTask();

        EasyMock.verify(t1);
    }

    @Test
    public void shouldMoveInitializedTasksNeedingRestoreToRestoring() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
        EasyMock.expect(t2.initializeStateStores()).andReturn(true);
        t2.initializeTopology();
        EasyMock.expectLastCall().once();
        final Set<TopicPartition> t2partitions = Collections.singleton(tp2);
        EasyMock.expect(t2.partitions()).andReturn(t2partitions);
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.emptyList());

        EasyMock.replay(t1, t2);

        assignedTasks.addNewTask(t1);
        assignedTasks.addNewTask(t2);

        assignedTasks.initializeNewTasks();

        final Collection<StreamTask> restoring = assignedTasks.restoringTasks();
        assertThat(restoring.size(), equalTo(1));
        assertSame(restoring.iterator().next(), t1);
    }

    @Test
    public void shouldMoveInitializedTasksThatDontNeedRestoringToRunning() {
        EasyMock.expect(t2.initializeStateStores()).andReturn(true);
        t2.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.emptyList());

        EasyMock.replay(t2);

        assignedTasks.addNewTask(t2);
        assignedTasks.initializeNewTasks();

        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId2)));
    }

    @Test
    public void shouldTransitionFullyRestoredTasksToRunning() {
        final Set<TopicPartition> task1Partitions = Utils.mkSet(tp1);
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(task1Partitions).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Utils.mkSet(changeLog1, changeLog2)).anyTimes();
        EasyMock.expect(t1.hasStateStores()).andReturn(true).anyTimes();
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.replay(t1);

        addAndInitTask();

        assignedTasks.updateRestored(Utils.mkSet(changeLog1));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.<TaskId>emptySet()));
        assignedTasks.updateRestored(Utils.mkSet(changeLog2));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
    }

    @Test
    public void shouldSuspendRunningTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());

        assertThat(assignedTasks.previousTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseRestoringTasks() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
        t1.closeStateManager(true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();
        assertThat(assignedTasks.closeAllRestoringTasks(), nullValue());

        EasyMock.verify(t1);
    }

    @Test
    public void shouldClosedUnInitializedTasksOnSuspend() {
        t1.close(false, false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assignedTasks.addNewTask(t1);
        assertThat(assignedTasks.suspend(), nullValue());

        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotSuspendSuspendedTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());
        assertThat(assignedTasks.suspend(), nullValue());
        EasyMock.verify(t1);
    }


    @Test
    public void shouldCloseTaskOnSuspendWhenRuntimeException() {
        mockTaskInitialization();
        t1.suspend();
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!"));
        t1.close(false, false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assertThat(suspendTask(), not(nullValue()));
        assertThat(assignedTasks.previousTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnSuspendIfTaskMigratedException() {
        mockTaskInitialization();
        t1.suspend();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());
        assertTrue(assignedTasks.previousTaskIds().isEmpty());
        EasyMock.verify(t1);
    }

    @Test
    public void shouldResumeMatchingSuspendedTasks() {
        mockRunningTaskSuspension();
        t1.resume();
        EasyMock.expectLastCall();
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());

        assertTrue(assignedTasks.maybeResumeSuspendedTask(taskId1, Collections.singleton(tp1)));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnResumeSuspendedIfTaskMigratedException() {
        mockRunningTaskSuspension();
        t1.resume();
        t1.initializeTopology();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());

        try {
            assignedTasks.maybeResumeSuspendedTask(taskId1, Collections.singleton(tp1));
            fail("Should have thrown TaskMigratedException.");
        } catch (final TaskMigratedException expected) { /* ignore */ }

        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    private void mockTaskInitialization() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(true);
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptyList());
    }

    @Test
    public void shouldCommitRunningTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();

        assignedTasks.commit();
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnCommitIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.commit();
            fail("Should have thrown TaskMigratedException.");
        } catch (final TaskMigratedException expected) { /* ignore */ }

        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldThrowExceptionOnCommitWhenNotCommitFailedOrProducerFenced() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new RuntimeException(""));
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.commit();
            fail("Should have thrown exception");
        } catch (final Exception e) {
            // ok
        }
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCommitRunningTasksIfNeeded() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitRequested()).andReturn(true);
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.maybeCommitPerUserRequested(), equalTo(1));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnMaybeCommitIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitRequested()).andReturn(true);
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.maybeCommitPerUserRequested();
            fail("Should have thrown TaskMigratedException.");
        } catch (final TaskMigratedException expected) { /* ignore */ }

        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnProcessesIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.isProcessable(0L)).andReturn(true);
        t1.process();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.process(0L);
            fail("Should have thrown TaskMigratedException.");
        } catch (final TaskMigratedException expected) { /* ignore */ }

        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotProcessUnprocessableTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.isProcessable(0L)).andReturn(false);
        EasyMock.replay(t1);
        addAndInitTask();

        assertThat(assignedTasks.process(0L), equalTo(0));

        EasyMock.verify(t1);
    }

    @Test
    public void shouldAlwaysProcessProcessableTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.isProcessable(0L)).andReturn(true);
        EasyMock.expect(t1.process()).andReturn(true).once();

        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.process(0L), equalTo(1));

        EasyMock.verify(t1);
    }

    @Test
    public void shouldPunctuateRunningTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(true);
        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.punctuate(), equalTo(2));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnMaybePunctuateStreamTimeIfTaskMigratedException() {
        mockTaskInitialization();
        t1.maybePunctuateStreamTime();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.punctuate();
            fail("Should have thrown TaskMigratedException.");
        } catch (final TaskMigratedException expected) { /* ignore */ }

        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnMaybePunctuateSystemTimeIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        t1.maybePunctuateSystemTime();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.punctuate();
            fail("Should have thrown TaskMigratedException.");
        } catch (final TaskMigratedException expected) { /* ignore */ }
        EasyMock.verify(t1);
    }

    @Test
    public void shouldReturnNumberOfPunctuations() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(false);
        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.punctuate(), equalTo(1));
        EasyMock.verify(t1);
    }

    private void addAndInitTask() {
        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();
    }

    private RuntimeException suspendTask() {
        addAndInitTask();
        return assignedTasks.suspend();
    }

    private void mockRunningTaskSuspension() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(true);
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t1.hasStateStores()).andReturn(false).anyTimes();
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1)).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptyList()).anyTimes();
        t1.suspend();
        EasyMock.expectLastCall();
    }


}