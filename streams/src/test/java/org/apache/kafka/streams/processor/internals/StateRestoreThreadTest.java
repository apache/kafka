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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.StateMachineTask;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class StateRestoreThreadTest {

    private final Time time = new MockTime();
    private final String topic = "topic";
    private final int partition = 0;
    private final TopicPartition tp = new TopicPartition(topic, partition);
    private final TaskId taskId = new TaskId(0, partition);
    private final String clientId = "state-restore-thread-test";
    private final ChangelogReader reader = new MockChangelogReader();

    private final StateRestoreThread restoreThread = new StateRestoreThread(time, clientId, reader);

    final ProcessorStateManager stateManager = EasyMock.createStrictMock(ProcessorStateManager.class);

    @Test
    public void shouldRegisterActiveTaskChangelogs() {
        final StateMachineTask task = new StateMachineTask(taskId, Collections.singleton(tp), true, stateManager);
        task.setChangelogOffsets(Collections.singletonMap(tp, 0L));

        restoreThread.addInitializedTasks(Collections.singletonList(task));

        restoreThread.runOnce();

        assertTrue(((MockChangelogReader) restoreThread.changelogReader()).isPartitionRegistered(tp));

        restoreThread.addClosedTasks(Collections.singletonList(task));

        restoreThread.runOnce();

        assertFalse(((MockChangelogReader) restoreThread.changelogReader()).isPartitionRegistered(tp));
    }

    @Test
    public void shouldRegisterStandbyTaskChangelogs() {
        final StateMachineTask task = new StateMachineTask(taskId, Collections.emptySet(), false, stateManager);
        task.setChangelogOffsets(Collections.singletonMap(tp, 0L));

        restoreThread.addInitializedTasks(Collections.singletonList(task));

        restoreThread.runOnce();

        assertTrue(((MockChangelogReader) restoreThread.changelogReader()).isPartitionRegistered(tp));

        restoreThread.addClosedTasks(Collections.singletonList(task));

        restoreThread.runOnce();

        assertFalse(((MockChangelogReader) restoreThread.changelogReader()).isPartitionRegistered(tp));
    }

    @Test
    public void shouldClearChangelogsUponShutdown() throws InterruptedException {
        final StateMachineTask task = new StateMachineTask(taskId, Collections.singleton(tp), true, stateManager);
        task.setChangelogOffsets(Collections.singletonMap(tp, 0L));

        restoreThread.addInitializedTasks(Collections.singletonList(task));

        restoreThread.start();

        TestUtils.waitForCondition(() -> ((MockChangelogReader) restoreThread.changelogReader()).isPartitionRegistered(tp),
                "Should registered the changelog within timeout");

        restoreThread.shutdown(1000);

        TestUtils.waitForCondition(() -> !((MockChangelogReader) restoreThread.changelogReader()).isPartitionRegistered(tp),
                "Should unregistered the changelog within timeout");
    }

    @Test
    public void shouldReportTaskCorruptedException() throws InterruptedException {
        final TaskCorruptedException exception = new TaskCorruptedException(Collections.singletonMap(taskId, Collections.singleton(tp)));
        final ChangelogReader reader = new MockChangelogReader() {
            @Override
            public int restore() {
                throw exception;
            }
        };
        final StateRestoreThread restoreThread = new StateRestoreThread(time, clientId, reader);
        final StateMachineTask task = new StateMachineTask(taskId, Collections.emptySet(), true, stateManager);
        task.setChangelogOffsets(Collections.singletonMap(tp, 0L));

        restoreThread.addInitializedTasks(Collections.singletonList(task));

        restoreThread.start();

        TestUtils.waitForCondition(() -> restoreThread.pollNextExceptionIfAny() == exception,
                "Should reported the exception within timeout");

        assertTrue(restoreThread.isAlive());
        assertTrue(restoreThread.isRunning());

        restoreThread.shutdown(1000);
    }

    @Test
    public void shouldReportFatalStreamsException() throws InterruptedException {
        final StreamsException exception = new StreamsException("kaboom!");
        final ChangelogReader reader = new MockChangelogReader() {
            @Override
            public int restore() {
                throw exception;
            }
        };
        final StateRestoreThread restoreThread = new StateRestoreThread(time, clientId, reader);
        final StateMachineTask task = new StateMachineTask(taskId, Collections.emptySet(), true, stateManager);
        task.setChangelogOffsets(Collections.singletonMap(tp, 0L));

        restoreThread.addInitializedTasks(Collections.singletonList(task));

        restoreThread.start();

        TestUtils.waitForCondition(() -> restoreThread.pollNextExceptionIfAny() == exception,
                "Should reported the exception within timeout");

        assertFalse(restoreThread.isAlive());
        assertFalse(restoreThread.isRunning());

        restoreThread.shutdown(1000);
    }

    @Test
    public void shouldPreferFatalRuntimeException() {
        final RuntimeException fatal = new IllegalStateException("kaboom!");
        final TaskCorruptedException corrupted = new TaskCorruptedException(Collections.emptyMap());

        restoreThread.setFatalException(fatal);
        restoreThread.addTaskCorruptedException(corrupted);

        assertEquals(fatal, restoreThread.pollNextExceptionIfAny());

        // fatal exception would not clear itself once set
        assertEquals(fatal, restoreThread.pollNextExceptionIfAny());
    }

    @Test
    public void shouldMergeTaskCorruptedException() {
        final TopicPartition t2p = new TopicPartition("topic2", partition);
        final TopicPartition tp2 = new TopicPartition(topic, 1);
        final TopicPartition t2p2 = new TopicPartition("topic2", 1);
        final TaskId taskId2 = new TaskId(0, 1);
        final TaskCorruptedException e1 = new TaskCorruptedException(
            Collections.singletonMap(taskId, Collections.singleton(tp)));
        final TaskCorruptedException e2 = new TaskCorruptedException(
                Collections.singletonMap(taskId, Collections.singleton(t2p)));
        final TaskCorruptedException e3 = new TaskCorruptedException(
            Collections.singletonMap(taskId2, Collections.singleton(tp2)));
        final TaskCorruptedException e4 = new TaskCorruptedException(
                Collections.singletonMap(taskId2, Collections.singleton(t2p2)));

        restoreThread.addTaskCorruptedException(e1);
        restoreThread.addTaskCorruptedException(e2);
        restoreThread.addTaskCorruptedException(e3);
        restoreThread.addTaskCorruptedException(e4);

        final RuntimeException e = restoreThread.pollNextExceptionIfAny();

        assertTrue(e instanceof TaskCorruptedException);
        assertEquals(2, ((TaskCorruptedException) e).corruptedTaskWithChangelogs().size());
        assertEquals(Utils.mkSet(tp, t2p), ((TaskCorruptedException) e).corruptedTaskWithChangelogs().get(taskId));
        assertEquals(Utils.mkSet(tp2, t2p2), ((TaskCorruptedException) e).corruptedTaskWithChangelogs().get(taskId2));

        // transient exception would be cleared once returned
        assertNull(restoreThread.pollNextExceptionIfAny());
    }

    @Test
    public void shouldIgnoreExceptionWhenInterruptedWhileShutdown() {
        final StreamsException exception = new StreamsException("kaboom!", new InterruptException("interrupted"));
        final AtomicBoolean throwException = new AtomicBoolean(false);
        final ChangelogReader reader = new MockChangelogReader() {
            @Override
            public int restore() {
                if (throwException.get())
                    throw exception;
                else
                    return super.restore();
            }
        };
        final StateRestoreThread restoreThread = new StateRestoreThread(time, clientId, reader);
        final StateMachineTask task = new StateMachineTask(taskId, Collections.emptySet(), true, stateManager);
        task.setChangelogOffsets(Collections.singletonMap(tp, 0L));

        restoreThread.addInitializedTasks(Collections.singletonList(task));

        restoreThread.start();

        restoreThread.setIsRunning(false);
        throwException.set(true);

        assertNull(restoreThread.pollNextExceptionIfAny());
    }
}
