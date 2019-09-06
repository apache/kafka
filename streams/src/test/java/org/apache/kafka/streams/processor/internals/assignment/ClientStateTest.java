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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientStateTest {

    private final ClientState client = new ClientState(1);

    @Test
    public void shouldHaveNotReachedCapacityWhenAssignedTasksLessThanCapacity() {
        assertFalse(client.reachedCapacity());
    }

    @Test
    public void shouldHaveReachedCapacityWhenAssignedTasksGreaterThanOrEqualToCapacity() {
        client.assign(new TaskId(0, 1), true);
        assertTrue(client.reachedCapacity());
    }


    @Test
    public void shouldAddActiveTasksToBothAssignedAndActive() {
        final TaskId tid = new TaskId(0, 1);

        client.assign(tid, true);
        assertThat(client.activeTasks(), equalTo(Collections.singleton(tid)));
        assertThat(client.assignedTasks(), equalTo(Collections.singleton(tid)));
        assertThat(client.assignedTaskCount(), equalTo(1));
        assertThat(client.standbyTasks().size(), equalTo(0));
    }

    @Test
    public void shouldAddStandbyTasksToBothStandbyAndActive() {
        final TaskId tid = new TaskId(0, 1);

        client.assign(tid, false);
        assertThat(client.assignedTasks(), equalTo(Collections.singleton(tid)));
        assertThat(client.standbyTasks(), equalTo(Collections.singleton(tid)));
        assertThat(client.assignedTaskCount(), equalTo(1));
        assertThat(client.activeTasks().size(), equalTo(0));
    }

    @Test
    public void shouldAddPreviousActiveTasksToPreviousAssignedAndPreviousActive() {
        final TaskId tid1 = new TaskId(0, 1);
        final TaskId tid2 = new TaskId(0, 2);

        client.addPreviousActiveTasks(Utils.mkSet(tid1, tid2));
        assertThat(client.previousActiveTasks(), equalTo(Utils.mkSet(tid1, tid2)));
        assertThat(client.previousAssignedTasks(), equalTo(Utils.mkSet(tid1, tid2)));
    }

    @Test
    public void shouldAddPreviousStandbyTasksToPreviousAssigned() {
        final TaskId tid1 = new TaskId(0, 1);
        final TaskId tid2 = new TaskId(0, 2);

        client.addPreviousStandbyTasks(Utils.mkSet(tid1, tid2));
        assertThat(client.previousActiveTasks().size(), equalTo(0));
        assertThat(client.previousAssignedTasks(), equalTo(Utils.mkSet(tid1, tid2)));
    }

    @Test
    public void shouldHaveAssignedTaskIfActiveTaskAssigned() {
        final TaskId tid = new TaskId(0, 2);

        client.assign(tid, true);
        assertTrue(client.hasAssignedTask(tid));
    }

    @Test
    public void shouldHaveAssignedTaskIfStandbyTaskAssigned() {
        final TaskId tid = new TaskId(0, 2);

        client.assign(tid, false);
        assertTrue(client.hasAssignedTask(tid));
    }

    @Test
    public void shouldNotHaveAssignedTaskIfTaskNotAssigned() {

        client.assign(new TaskId(0, 2), true);
        assertFalse(client.hasAssignedTask(new TaskId(0, 3)));
    }

    @Test
    public void shouldHaveMoreAvailableCapacityWhenCapacityTheSameButFewerAssignedTasks() {
        final ClientState c2 = new ClientState(1);
        client.assign(new TaskId(0, 1), true);
        assertTrue(c2.hasMoreAvailableCapacityThan(client));
        assertFalse(client.hasMoreAvailableCapacityThan(c2));
    }

    @Test
    public void shouldHaveMoreAvailableCapacityWhenCapacityHigherAndSameAssignedTaskCount() {
        final ClientState c2 = new ClientState(2);
        assertTrue(c2.hasMoreAvailableCapacityThan(client));
        assertFalse(client.hasMoreAvailableCapacityThan(c2));
    }

    @Test
    public void shouldUseMultiplesOfCapacityToDetermineClientWithMoreAvailableCapacity() {
        final ClientState c2 = new ClientState(2);

        for (int i = 0; i < 7; i++) {
            c2.assign(new TaskId(0, i), true);
        }

        for (int i = 7; i < 11; i++) {
            client.assign(new TaskId(0, i), true);
        }

        assertTrue(c2.hasMoreAvailableCapacityThan(client));
    }

    @Test
    public void shouldHaveMoreAvailableCapacityWhenCapacityIsTheSameButAssignedTasksIsLess() {
        final ClientState c1 = new ClientState(3);
        final ClientState c2 = new ClientState(3);
        for (int i = 0; i < 4; i++) {
            c1.assign(new TaskId(0, i), true);
            c2.assign(new TaskId(0, i), true);
        }
        c2.assign(new TaskId(0, 5), true);
        assertTrue(c1.hasMoreAvailableCapacityThan(c2));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionIfCapacityOfThisClientStateIsZero() {
        final ClientState c1 = new ClientState(0);
        c1.hasMoreAvailableCapacityThan(new ClientState(1));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionIfCapacityOfOtherClientStateIsZero() {
        final ClientState c1 = new ClientState(1);
        c1.hasMoreAvailableCapacityThan(new ClientState(0));
    }

    @Test
    public void shouldHaveUnfulfilledQuotaWhenActiveTaskSizeLessThanCapacityTimesTasksPerThread() {
        final ClientState client = new ClientState(1);
        client.assign(new TaskId(0, 1), true);
        assertTrue(client.hasUnfulfilledQuota(2));
    }

    @Test
    public void shouldNotHaveUnfulfilledQuotaWhenActiveTaskSizeGreaterEqualThanCapacityTimesTasksPerThread() {
        final ClientState client = new ClientState(1);
        client.assign(new TaskId(0, 1), true);
        assertFalse(client.hasUnfulfilledQuota(1));
    }

}