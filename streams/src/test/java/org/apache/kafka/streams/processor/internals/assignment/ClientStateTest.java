/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientStateTest {

    private final ClientState<Integer> client = new ClientState<>(1);

    @Test
    public void shouldHaveNotReachedCapacityWhenAssignedTasksLessThanCapacity() throws Exception {
        assertFalse(client.reachedCapacity());
    }

    @Test
    public void shouldHaveReachedCapacityWhenAssignedTasksGreaterThanOrEqualToCapacity() throws Exception {
        client.assign(1, true);
        assertTrue(client.reachedCapacity());
    }


    @Test
    public void shouldAddActiveTasksToBothAssignedAndActive() throws Exception {
        client.assign(1, true);
        assertThat(client.activeTasks(), equalTo(Collections.singleton(1)));
        assertThat(client.assignedTasks(), equalTo(Collections.singleton(1)));
        assertThat(client.assignedTaskCount(), equalTo(1));
        assertThat(client.standbyTasks().size(), equalTo(0));
    }

    @Test
    public void shouldAddStandbyTasksToBothStandbyAndActive() throws Exception {
        client.assign(1, false);
        assertThat(client.assignedTasks(), equalTo(Collections.singleton(1)));
        assertThat(client.standbyTasks(), equalTo(Collections.singleton(1)));
        assertThat(client.assignedTaskCount(), equalTo(1));
        assertThat(client.activeTasks().size(), equalTo(0));
    }

    @Test
    public void shouldAddPreviousActiveTasksToPreviousAssignedAndPreviousActive() throws Exception {
        client.addPreviousActiveTasks(Utils.mkSet(1, 2));
        assertThat(client.previousActiveTasks(), equalTo(Utils.mkSet(1, 2)));
        assertThat(client.previousAssignedTasks(), equalTo(Utils.mkSet(1, 2)));
    }

    @Test
    public void shouldAddPreviousStandbyTasksToPreviousAssigned() throws Exception {
        client.addPreviousStandbyTasks(Utils.mkSet(1, 2));
        assertThat(client.previousActiveTasks().size(), equalTo(0));
        assertThat(client.previousAssignedTasks(), equalTo(Utils.mkSet(1, 2)));
    }

    @Test
    public void shouldHaveAssignedTaskIfActiveTaskAssigned() throws Exception {
        client.assign(2, true);
        assertTrue(client.hasAssignedTask(2));
    }

    @Test
    public void shouldHaveAssignedTaskIfStandbyTaskAssigned() throws Exception {
        client.assign(2, false);
        assertTrue(client.hasAssignedTask(2));
    }

    @Test
    public void shouldNotHaveAssignedTaskIfTaskNotAssigned() throws Exception {
        client.assign(2, true);
        assertFalse(client.hasAssignedTask(3));
    }

    @Test
    public void shouldHaveMoreAvailableCapacityWhenCapacityTheSameButFewerAssignedTasks() throws Exception {
        final ClientState<Integer> c2 = new ClientState<>(1);
        client.assign(1, true);
        assertTrue(c2.hasMoreAvailableCapacityThan(client));
        assertFalse(client.hasMoreAvailableCapacityThan(c2));
    }

    @Test
    public void shouldHaveMoreAvailableCapacityWhenCapacityHigherAndSameAssignedTaskCount() throws Exception {
        final ClientState<Integer> c2 = new ClientState<>(2);
        assertTrue(c2.hasMoreAvailableCapacityThan(client));
        assertFalse(client.hasMoreAvailableCapacityThan(c2));
    }

    @Test
    public void shouldUseMultiplesOfCapacityToDetermineClientWithMoreAvailableCapacity() throws Exception {
        final ClientState<Integer> c2 = new ClientState<>(2);

        for (int i = 0; i < 7; i++) {
            c2.assign(i, true);
        }

        for (int i = 7; i < 11; i++) {
            client.assign(i, true);
        }

        assertTrue(c2.hasMoreAvailableCapacityThan(client));
    }

    @Test
    public void shouldHaveMoreAvailableCapacityWhenCapacityIsTheSameButAssignedTasksIsLess() throws Exception {
        final ClientState<Integer> c1 = new ClientState<>(3);
        final ClientState<Integer> c2 = new ClientState<>(3);
        for (int i = 0; i < 4; i++) {
            c1.assign(i, true);
            c2.assign(i, true);
        }
        c2.assign(5, true);
        assertTrue(c1.hasMoreAvailableCapacityThan(c2));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionIfCapacityOfThisClientStateIsZero() throws Exception {
        final ClientState<Integer> c1 = new ClientState<>(0);
        c1.hasMoreAvailableCapacityThan(new ClientState<Integer>(1));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionIfCapacityOfOtherClientStateIsZero() throws Exception {
        final ClientState<Integer> c1 = new ClientState<>(1);
        c1.hasMoreAvailableCapacityThan(new ClientState<Integer>(0));
    }


}