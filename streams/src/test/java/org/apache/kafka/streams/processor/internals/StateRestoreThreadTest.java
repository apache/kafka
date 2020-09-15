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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.StateMachineTask;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
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

        restoreThread.addClosedTasks(Collections.singletonMap(task, Collections.singleton(tp)));

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

        restoreThread.addClosedTasks(Collections.singletonMap(task, Collections.singleton(tp)));

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
}
