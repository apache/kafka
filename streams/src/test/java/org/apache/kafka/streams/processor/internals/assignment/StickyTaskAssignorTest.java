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
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertTrue;

public class StickyTaskAssignorTest {

    private final TaskId task00 = new TaskId(0, 0);
    private final TaskId task01 = new TaskId(0, 1);
    private final TaskId task02 = new TaskId(0, 2);
    private final TaskId task03 = new TaskId(0, 3);
    private final Map<Integer, ClientState<TaskId>> clients = new TreeMap<>();
    private final Integer p1 = 1;
    private final Integer p2 = 2;
    private final Integer p3 = 3;
    private final Integer p4 = 4;

    @Test
    public void shouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount() throws Exception {
        createClient(p1, 1);
        createClient(p2, 1);
        createClient(p3, 1);

        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(0);

        for (final Integer processId : clients.keySet()) {
            assertThat(clients.get(processId).activeTaskCount(), equalTo(1));
        }
    }

    @Test
    public void shouldNotMigrateActiveTaskToOtherProcess() throws Exception {
        createClientWithPreviousActiveTasks(p1, 1, task00);
        createClientWithPreviousActiveTasks(p2, 1, task01);

        final StickyTaskAssignor firstAssignor = createTaskAssignor(task00, task01, task02);
        firstAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), hasItems(task00));
        assertThat(clients.get(p2).activeTasks(), hasItems(task01));
        assertThat(allActiveTasks(), equalTo(Arrays.asList(task00, task01, task02)));

        clients.clear();

        // flip the previous active tasks assignment around.
        createClientWithPreviousActiveTasks(p1, 1, task01);
        createClientWithPreviousActiveTasks(p2, 1, task02);

        final StickyTaskAssignor secondAssignor = createTaskAssignor(task00, task01, task02);
        secondAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), hasItems(task01));
        assertThat(clients.get(p2).activeTasks(), hasItems(task02));
        assertThat(allActiveTasks(), equalTo(Arrays.asList(task00, task01, task02)));
    }

    @Test
    public void shouldMigrateActiveTasksToNewProcessWithoutChangingAllAssignments() throws Exception {
        createClientWithPreviousActiveTasks(p1, 1, task00, task02);
        createClientWithPreviousActiveTasks(p2, 1, task01);
        createClient(p3, 1);

        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);

        assertThat(clients.get(p2).activeTasks(), equalTo(Collections.singleton(task01)));
        assertThat(clients.get(p1).activeTasks().size(), equalTo(1));
        assertThat(clients.get(p3).activeTasks().size(), equalTo(1));
        assertThat(allActiveTasks(), equalTo(Arrays.asList(task00, task01, task02)));
    }

    @Test
    public void shouldAssignBasedOnCapacity() throws Exception {
        createClient(p1, 1);
        createClient(p2, 2);
        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);
        assertThat(clients.get(p1).activeTasks().size(), equalTo(1));
        assertThat(clients.get(p2).activeTasks().size(), equalTo(2));
    }

    @Test
    public void shouldKeepActiveTaskStickynessWhenMoreClientThanActiveTasks() {
        final int p5 = 5;
        createClientWithPreviousActiveTasks(p1, 1, task00);
        createClientWithPreviousActiveTasks(p2, 1, task02);
        createClientWithPreviousActiveTasks(p3, 1, task01);
        createClient(p4, 1);
        createClient(p5, 1);

        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), equalTo(Collections.singleton(task00)));
        assertThat(clients.get(p2).activeTasks(), equalTo(Collections.singleton(task02)));
        assertThat(clients.get(p3).activeTasks(), equalTo(Collections.singleton(task01)));

        // change up the assignment and make sure it is still sticky
        clients.clear();
        createClient(p1, 1);
        createClientWithPreviousActiveTasks(p2, 1, task00);
        createClient(p3, 1);
        createClientWithPreviousActiveTasks(p4, 1, task02);
        createClientWithPreviousActiveTasks(p5, 1, task01);

        final StickyTaskAssignor secondAssignor = createTaskAssignor(task00, task01, task02);
        secondAssignor.assign(0);

        assertThat(clients.get(p2).activeTasks(), equalTo(Collections.singleton(task00)));
        assertThat(clients.get(p4).activeTasks(), equalTo(Collections.singleton(task02)));
        assertThat(clients.get(p5).activeTasks(), equalTo(Collections.singleton(task01)));


    }

    @Test
    public void shouldAssignTasksToClientWithPreviousStandbyTasks() throws Exception {
        final ClientState<TaskId> client1 = createClient(p1, 1);
        client1.addPreviousStandbyTasks(Utils.mkSet(task02));
        final ClientState<TaskId> client2 = createClient(p2, 1);
        client2.addPreviousStandbyTasks(Utils.mkSet(task01));
        final ClientState<TaskId> client3 = createClient(p3, 1);
        client3.addPreviousStandbyTasks(Utils.mkSet(task00));

        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), equalTo(Collections.singleton(task02)));
        assertThat(clients.get(p2).activeTasks(), equalTo(Collections.singleton(task01)));
        assertThat(clients.get(p3).activeTasks(), equalTo(Collections.singleton(task00)));
    }

    @Test
    public void shouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks() throws Exception {
        final ClientState<TaskId> c1 = createClientWithPreviousActiveTasks(p1, 1, task00);
        c1.addPreviousStandbyTasks(Utils.mkSet(task01));
        final ClientState<TaskId> c2 = createClientWithPreviousActiveTasks(p2, 2, task02);
        c2.addPreviousStandbyTasks(Utils.mkSet(task01));

        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), equalTo(Collections.singleton(task00)));
        assertThat(clients.get(p2).activeTasks(), equalTo(Utils.mkSet(task02, task01)));
    }

    @Test
    public void shouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssingedTo() throws Exception {
        createClientWithPreviousActiveTasks(p1, 1, task00);
        createClientWithPreviousActiveTasks(p2, 1, task01);
        createClientWithPreviousActiveTasks(p3, 1, task02);
        createClientWithPreviousActiveTasks(p4, 1, task03);

        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02, task03);
        taskAssignor.assign(1);

        assertThat(clients.get(p1).standbyTasks(), not(hasItems(task00)));
        assertTrue(clients.get(p1).standbyTasks().size() <= 2);
        assertThat(clients.get(p2).standbyTasks(), not(hasItems(task01)));
        assertTrue(clients.get(p2).standbyTasks().size() <= 2);
        assertThat(clients.get(p3).standbyTasks(), not(hasItems(task02)));
        assertTrue(clients.get(p3).standbyTasks().size() <= 2);
        assertThat(clients.get(p4).standbyTasks(), not(hasItems(task03)));
        assertTrue(clients.get(p4).standbyTasks().size() <= 2);

        int nonEmptyStandbyTaskCount = 0;
        for (final Integer client : clients.keySet()) {
            nonEmptyStandbyTaskCount += clients.get(client).standbyTasks().isEmpty() ? 0 : 1;
        }

        assertTrue(nonEmptyStandbyTaskCount >= 3);
        assertThat(allStandbyTasks(), equalTo(Arrays.asList(task00, task01, task02, task03)));
    }



    @Test
    public void shouldAssignMultipleReplicasOfStandbyTask() throws Exception {
        createClientWithPreviousActiveTasks(p1, 1, task00);
        createClientWithPreviousActiveTasks(p2, 1, task01);
        createClientWithPreviousActiveTasks(p3, 1, task02);

        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(2);

        assertThat(clients.get(p1).standbyTasks(), equalTo(Utils.mkSet(task01, task02)));
        assertThat(clients.get(p2).standbyTasks(), equalTo(Utils.mkSet(task02, task00)));
        assertThat(clients.get(p3).standbyTasks(), equalTo(Utils.mkSet(task00, task01)));
    }

    @Test
    public void shouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned() throws Exception {
        createClient(p1, 1);
        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00);
        taskAssignor.assign(1);
        assertThat(clients.get(p1).standbyTasks().size(), equalTo(0));
    }

    @Test
    public void shouldAssignActiveAndStandbyTasks() throws Exception {
        createClient(p1, 1);
        createClient(p2, 1);
        createClient(p3, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(1);

        assertThat(allActiveTasks(), equalTo(Arrays.asList(task00, task01, task02)));
        assertThat(allStandbyTasks(), equalTo(Arrays.asList(task00, task01, task02)));
    }


    @Test
    public void shouldAssignAtLeastOneTaskToEachClientIfPossible() throws Exception {
        createClient(p1, 3);
        createClient(p2, 1);
        createClient(p3, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(0);
        assertThat(clients.get(p1).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(p2).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(p3).assignedTaskCount(), equalTo(1));
    }

    @Test
    public void shouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks() throws Exception {
        createClient(p1, 1);
        createClient(p2, 1);
        createClient(p3, 1);
        createClient(p4, 1);
        createClient(5, 1);
        createClient(6, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(0);

        assertThat(allActiveTasks(), equalTo(Arrays.asList(task00, task01, task02)));
    }

    @Test
    public void shouldBalanceActiveAndStandbyTasksAcrossAvailableClients() throws Exception {
        createClient(p1, 1);
        createClient(p2, 1);
        createClient(p3, 1);
        createClient(p4, 1);
        createClient(5, 1);
        createClient(6, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(1);

        for (final ClientState<TaskId> clientState : clients.values()) {
            assertThat(clientState.assignedTaskCount(), equalTo(1));
        }
    }

    @Test
    public void shouldAssignMoreTasksToClientWithMoreCapacity() throws Exception {
        createClient(p2, 2);
        createClient(p1, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00,
                                                                            task01,
                                                                            task02,
                                                                            new TaskId(1, 0),
                                                                            new TaskId(1, 1),
                                                                            new TaskId(1, 2),
                                                                            new TaskId(2, 0),
                                                                            new TaskId(2, 1),
                                                                            new TaskId(2, 2),
                                                                            new TaskId(3, 0),
                                                                            new TaskId(3, 1),
                                                                            new TaskId(3, 2));

        taskAssignor.assign(0);
        assertThat(clients.get(p2).assignedTaskCount(), equalTo(8));
        assertThat(clients.get(p1).assignedTaskCount(), equalTo(4));
    }


    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHosts() throws Exception {
        createClient(p1, 1);
        createClient(p2, 1);
        createClient(p3, 1);
        createClient(p4, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(1);

        for (int i = p1; i <= p4; i++) {
            final Set<TaskId> taskIds = clients.get(i).assignedTasks();
            for (int j = p1; j <= p4; j++) {
                if (j != i) {
                    assertThat("clients shouldn't have same task assignment", clients.get(j).assignedTasks(),
                               not(equalTo(taskIds)));
                }
            }

        }
    }

    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousActiveTasks() throws Exception {
        createClientWithPreviousActiveTasks(p1, 1, task01, task02);
        createClientWithPreviousActiveTasks(p2, 1, task03);
        createClientWithPreviousActiveTasks(p3, 1, task00);
        createClient(p4, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(1);

        for (int i = p1; i <= p4; i++) {
            final Set<TaskId> taskIds = clients.get(i).assignedTasks();
            for (int j = p1; j <= p4; j++) {
                if (j != i) {
                    assertThat("clients shouldn't have same task assignment", clients.get(j).assignedTasks(),
                               not(equalTo(taskIds)));
                }
            }

        }
    }

    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousStandbyTasks() throws Exception {
        final ClientState<TaskId> c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task02);
        c1.addPreviousStandbyTasks(Utils.mkSet(task03, task00));
        final ClientState<TaskId> c2 = createClientWithPreviousActiveTasks(p2, 1, task03, task00);
        c2.addPreviousStandbyTasks(Utils.mkSet(task01, task02));

        createClient(p3, 1);
        createClient(p4, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(1);

        for (int i = p1; i <= p4; i++) {
            final Set<TaskId> taskIds = clients.get(i).assignedTasks();
            for (int j = p1; j <= p4; j++) {
                if (j != i) {
                    assertThat("clients shouldn't have same task assignment", clients.get(j).assignedTasks(),
                               not(equalTo(taskIds)));
                }
            }

        }
    }

    @Test
    public void shouldReBalanceTasksAcrossAllClientsWhenCapacityAndTaskCountTheSame() throws Exception {
        createClientWithPreviousActiveTasks(p3, 1, task00, task01, task02, task03);
        createClient(p1, 1);
        createClient(p2, 1);
        createClient(p4, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(0);

        assertThat(clients.get(p1).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(p2).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(p3).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(p4).assignedTaskCount(), equalTo(1));
    }

    @Test
    public void shouldReBalanceTasksAcrossClientsWhenCapacityLessThanTaskCount() throws Exception {
        createClientWithPreviousActiveTasks(p3, 1, task00, task01, task02, task03);
        createClient(p1, 1);
        createClient(p2, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(0);

        assertThat(clients.get(p3).assignedTaskCount(), equalTo(2));
        assertThat(clients.get(p1).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(p2).assignedTaskCount(), equalTo(1));
    }

    @Test
    public void shouldRebalanceTasksToClientsBasedOnCapacity() throws Exception {
        createClientWithPreviousActiveTasks(p2, 1, task00, task03, task02);
        createClient(p3, 2);
        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task02, task03);
        taskAssignor.assign(0);
        assertThat(clients.get(p2).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(p3).assignedTaskCount(), equalTo(2));
    }

    @Test
    public void shouldMoveMinimalNumberOfTasksWhenPreviouslyAboveCapacityAndNewClientAdded() throws Exception {
        final Set<TaskId> p1PrevTasks = Utils.mkSet(task00, task02);
        final Set<TaskId> p2PrevTasks = Utils.mkSet(task01, task03);

        createClientWithPreviousActiveTasks(p1, 1, task00, task02);
        createClientWithPreviousActiveTasks(p2, 1, task01, task03);
        createClientWithPreviousActiveTasks(p3, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(0);

        final Set<TaskId> p3ActiveTasks = clients.get(p3).activeTasks();
        assertThat(p3ActiveTasks.size(), equalTo(1));
        if (p1PrevTasks.removeAll(p3ActiveTasks)) {
            assertThat(clients.get(p2).activeTasks(), equalTo(p2PrevTasks));
        } else {
            assertThat(clients.get(p1).activeTasks(), equalTo(p1PrevTasks));
        }
    }

    @Test
    public void shouldNotMoveAnyTasksWhenNewTasksAdded() throws Exception {
        final TaskId task04 = new TaskId(0, 4);
        final TaskId task05 = new TaskId(0, 5);

        createClientWithPreviousActiveTasks(p1, 1, task00, task01);
        createClientWithPreviousActiveTasks(p2, 1, task02, task03);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task03, task01, task04, task02, task00, task05);
        taskAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), hasItems(task00, task01));
        assertThat(clients.get(p2).activeTasks(), hasItems(task02, task03));
    }

    @Test
    public void shouldAssignNewTasksToNewClientWhenPreviousTasksAssignedToOldClients() throws Exception {
        final TaskId task04 = new TaskId(0, 4);
        final TaskId task05 = new TaskId(0, 5);

        createClientWithPreviousActiveTasks(p1, 1, task02, task01);
        createClientWithPreviousActiveTasks(p2, 1, task00, task03);
        createClient(p3, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task03, task01, task04, task02, task00, task05);
        taskAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), hasItems(task02, task01));
        assertThat(clients.get(p2).activeTasks(), hasItems(task00, task03));
        assertThat(clients.get(p3).activeTasks(), hasItems(task04, task05));
    }

    private StickyTaskAssignor<Integer> createTaskAssignor(final TaskId... tasks) {
        return new StickyTaskAssignor<>(clients,
                                        new HashSet<>(Arrays.asList(tasks)));
    }

    private List<TaskId> allActiveTasks() {
        final List<TaskId> allActive = new ArrayList<>();
        for (final ClientState<TaskId> client : clients.values()) {
            allActive.addAll(client.activeTasks());
        }
        Collections.sort(allActive);
        return allActive;
    }

    private List<TaskId> allStandbyTasks() {
        final List<TaskId> tasks = new ArrayList<>();
        for (final ClientState<TaskId> client : clients.values()) {
            tasks.addAll(client.standbyTasks());
        }
        Collections.sort(tasks);
        return tasks;
    }

    private ClientState<TaskId> createClient(final Integer processId, final int capacity) {
        return createClientWithPreviousActiveTasks(processId, capacity);
    }

    private ClientState<TaskId> createClientWithPreviousActiveTasks(final Integer processId, final int capacity, final TaskId... taskIds) {
        final ClientState<TaskId> clientState = new ClientState<>(capacity);
        clientState.addPreviousActiveTasks(Utils.mkSet(taskIds));
        clients.put(processId, clientState);
        return clientState;
    }

}
