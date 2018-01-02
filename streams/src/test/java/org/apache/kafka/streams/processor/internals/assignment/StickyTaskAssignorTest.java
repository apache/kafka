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
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertTrue;

public class StickyTaskAssignorTest {

    private final TaskId task00 = new TaskId(0, 0);
    private final TaskId task01 = new TaskId(0, 1);
    private final TaskId task02 = new TaskId(0, 2);
    private final TaskId task03 = new TaskId(0, 3);
    private final TaskId task04 = new TaskId(0, 4);
    private final TaskId task05 = new TaskId(0, 5);
    private final Map<Integer, ClientState> clients = new TreeMap<>();
    private final Integer p1 = 1;
    private final Integer p2 = 2;
    private final Integer p3 = 3;
    private final Integer p4 = 4;

    @Test
    public void shouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount() {
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
    public void shouldNotMigrateActiveTaskToOtherProcess() {
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
    public void shouldMigrateActiveTasksToNewProcessWithoutChangingAllAssignments() {
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
    public void shouldAssignBasedOnCapacity() {
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
    public void shouldAssignTasksToClientWithPreviousStandbyTasks() {
        final ClientState client1 = createClient(p1, 1);
        client1.addPreviousStandbyTasks(Utils.mkSet(task02));
        final ClientState client2 = createClient(p2, 1);
        client2.addPreviousStandbyTasks(Utils.mkSet(task01));
        final ClientState client3 = createClient(p3, 1);
        client3.addPreviousStandbyTasks(Utils.mkSet(task00));

        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), equalTo(Collections.singleton(task02)));
        assertThat(clients.get(p2).activeTasks(), equalTo(Collections.singleton(task01)));
        assertThat(clients.get(p3).activeTasks(), equalTo(Collections.singleton(task00)));
    }

    @Test
    public void shouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks() {
        final ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task00);
        c1.addPreviousStandbyTasks(Utils.mkSet(task01));
        final ClientState c2 = createClientWithPreviousActiveTasks(p2, 2, task02);
        c2.addPreviousStandbyTasks(Utils.mkSet(task01));

        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), equalTo(Collections.singleton(task00)));
        assertThat(clients.get(p2).activeTasks(), equalTo(Utils.mkSet(task02, task01)));
    }

    @Test
    public void shouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssingedTo() {
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
    public void shouldAssignMultipleReplicasOfStandbyTask() {
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
    public void shouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned() {
        createClient(p1, 1);
        final StickyTaskAssignor taskAssignor = createTaskAssignor(task00);
        taskAssignor.assign(1);
        assertThat(clients.get(p1).standbyTasks().size(), equalTo(0));
    }

    @Test
    public void shouldAssignActiveAndStandbyTasks() {
        createClient(p1, 1);
        createClient(p2, 1);
        createClient(p3, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(1);

        assertThat(allActiveTasks(), equalTo(Arrays.asList(task00, task01, task02)));
        assertThat(allStandbyTasks(), equalTo(Arrays.asList(task00, task01, task02)));
    }


    @Test
    public void shouldAssignAtLeastOneTaskToEachClientIfPossible() {
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
    public void shouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks() {
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
    public void shouldBalanceActiveAndStandbyTasksAcrossAvailableClients() {
        createClient(p1, 1);
        createClient(p2, 1);
        createClient(p3, 1);
        createClient(p4, 1);
        createClient(5, 1);
        createClient(6, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(1);

        for (final ClientState clientState : clients.values()) {
            assertThat(clientState.assignedTaskCount(), equalTo(1));
        }
    }

    @Test
    public void shouldAssignMoreTasksToClientWithMoreCapacity() {
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
    public void shouldNotHaveSameAssignmentOnAnyTwoHosts() {
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
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousActiveTasks() {
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
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousStandbyTasks() {
        final ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task02);
        c1.addPreviousStandbyTasks(Utils.mkSet(task03, task00));
        final ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task03, task00);
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
    public void shouldReBalanceTasksAcrossAllClientsWhenCapacityAndTaskCountTheSame() {
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
    public void shouldReBalanceTasksAcrossClientsWhenCapacityLessThanTaskCount() {
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
    public void shouldRebalanceTasksToClientsBasedOnCapacity() {
        createClientWithPreviousActiveTasks(p2, 1, task00, task03, task02);
        createClient(p3, 2);
        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task02, task03);
        taskAssignor.assign(0);
        assertThat(clients.get(p2).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(p3).assignedTaskCount(), equalTo(2));
    }

    @Test
    public void shouldMoveMinimalNumberOfTasksWhenPreviouslyAboveCapacityAndNewClientAdded() {
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
    public void shouldNotMoveAnyTasksWhenNewTasksAdded() {
        createClientWithPreviousActiveTasks(p1, 1, task00, task01);
        createClientWithPreviousActiveTasks(p2, 1, task02, task03);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task03, task01, task04, task02, task00, task05);
        taskAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), hasItems(task00, task01));
        assertThat(clients.get(p2).activeTasks(), hasItems(task02, task03));
    }

    @Test
    public void shouldAssignNewTasksToNewClientWhenPreviousTasksAssignedToOldClients() {

        createClientWithPreviousActiveTasks(p1, 1, task02, task01);
        createClientWithPreviousActiveTasks(p2, 1, task00, task03);
        createClient(p3, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task03, task01, task04, task02, task00, task05);
        taskAssignor.assign(0);

        assertThat(clients.get(p1).activeTasks(), hasItems(task02, task01));
        assertThat(clients.get(p2).activeTasks(), hasItems(task00, task03));
        assertThat(clients.get(p3).activeTasks(), hasItems(task04, task05));
    }

    @Test
    public void shouldAssignTasksNotPreviouslyActiveToNewClient() {
        final TaskId task10 = new TaskId(0, 10);
        final TaskId task11 = new TaskId(0, 11);
        final TaskId task12 = new TaskId(1, 2);
        final TaskId task13 = new TaskId(1, 3);
        final TaskId task20 = new TaskId(2, 0);
        final TaskId task21 = new TaskId(2, 1);
        final TaskId task22 = new TaskId(2, 2);
        final TaskId task23 = new TaskId(2, 3);

        final ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task12, task13);
        c1.addPreviousStandbyTasks(Utils.mkSet(task00, task11, task20, task21, task23));
        final ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task00, task11, task22);
        c2.addPreviousStandbyTasks(Utils.mkSet(task01, task10, task02, task20, task03, task12, task21, task13, task23));
        final ClientState c3 = createClientWithPreviousActiveTasks(p3, 1, task20, task21, task23);
        c3.addPreviousStandbyTasks(Utils.mkSet(task02, task12));

        final ClientState newClient = createClient(p4, 1);
        newClient.addPreviousStandbyTasks(Utils.mkSet(task00, task10, task01, task02, task11, task20, task03, task12, task21, task13, task22, task23));

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task10, task01, task02, task11, task20, task03, task12, task21, task13, task22, task23);
        taskAssignor.assign(0);

        assertThat(c1.activeTasks(), equalTo(Utils.mkSet(task01, task12, task13)));
        assertThat(c2.activeTasks(), equalTo(Utils.mkSet(task00, task11, task22)));
        assertThat(c3.activeTasks(), equalTo(Utils.mkSet(task20, task21, task23)));
        assertThat(newClient.activeTasks(), equalTo(Utils.mkSet(task02, task03, task10)));
    }

    @Test
    public void shouldAssignTasksNotPreviouslyActiveToMultipleNewClients() {
        final TaskId task10 = new TaskId(0, 10);
        final TaskId task11 = new TaskId(0, 11);
        final TaskId task12 = new TaskId(1, 2);
        final TaskId task13 = new TaskId(1, 3);
        final TaskId task20 = new TaskId(2, 0);
        final TaskId task21 = new TaskId(2, 1);
        final TaskId task22 = new TaskId(2, 2);
        final TaskId task23 = new TaskId(2, 3);

        final ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task12, task13);
        c1.addPreviousStandbyTasks(Utils.mkSet(task00, task11, task20, task21, task23));
        final ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task00, task11, task22);
        c2.addPreviousStandbyTasks(Utils.mkSet(task01, task10, task02, task20, task03, task12, task21, task13, task23));

        final ClientState bounce1 = createClient(p3, 1);
        bounce1.addPreviousStandbyTasks(Utils.mkSet(task20, task21, task23));

        final ClientState bounce2 = createClient(p4, 1);
        bounce2.addPreviousStandbyTasks(Utils.mkSet(task02, task03, task10));

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task10, task01, task02, task11, task20, task03, task12, task21, task13, task22, task23);
        taskAssignor.assign(0);

        assertThat(c1.activeTasks(), equalTo(Utils.mkSet(task01, task12, task13)));
        assertThat(c2.activeTasks(), equalTo(Utils.mkSet(task00, task11, task22)));
        assertThat(bounce1.activeTasks(), equalTo(Utils.mkSet(task20, task21, task23)));
        assertThat(bounce2.activeTasks(), equalTo(Utils.mkSet(task02, task03, task10)));
    }

    @Test
    public void shouldAssignTasksToNewClient() {
        createClientWithPreviousActiveTasks(p1, 1, task01, task02);
        createClient(p2, 1);
        createTaskAssignor(task01, task02).assign(0);
        assertThat(clients.get(p1).activeTaskCount(), equalTo(1));
    }

    @Test
    public void shouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingClients() {
        final ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task00, task01, task02);
        final ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task03, task04, task05);
        final ClientState newClient = createClient(p3, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task01, task02, task03, task04, task05);
        taskAssignor.assign(0);
        assertThat(c1.activeTasks(), not(hasItem(task03)));
        assertThat(c1.activeTasks(), not(hasItem(task04)));
        assertThat(c1.activeTasks(), not(hasItem(task05)));
        assertThat(c1.activeTaskCount(), equalTo(2));
        assertThat(c2.activeTasks(), not(hasItems(task00)));
        assertThat(c2.activeTasks(), not(hasItems(task01)));
        assertThat(c2.activeTasks(), not(hasItems(task02)));
        assertThat(c2.activeTaskCount(), equalTo(2));
        assertThat(newClient.activeTaskCount(), equalTo(2));
    }

    @Test
    public void shouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingAndBouncedClients() {
        final TaskId task06 = new TaskId(0, 6);
        final ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task00, task01, task02, task06);
        final ClientState c2 = createClient(p2, 1);
        c2.addPreviousStandbyTasks(Utils.mkSet(task03, task04, task05));
        final ClientState newClient = createClient(p3, 1);

        final StickyTaskAssignor<Integer> taskAssignor = createTaskAssignor(task00, task01, task02, task03, task04, task05, task06);
        taskAssignor.assign(0);
        assertThat(c1.activeTasks(), not(hasItem(task03)));
        assertThat(c1.activeTasks(), not(hasItem(task04)));
        assertThat(c1.activeTasks(), not(hasItem(task05)));
        assertThat(c1.activeTaskCount(), equalTo(3));
        assertThat(c2.activeTasks(), not(hasItems(task00)));
        assertThat(c2.activeTasks(), not(hasItems(task01)));
        assertThat(c2.activeTasks(), not(hasItems(task02)));
        assertThat(c2.activeTaskCount(), equalTo(2));
        assertThat(newClient.activeTaskCount(), equalTo(2));
    }

    private StickyTaskAssignor<Integer> createTaskAssignor(final TaskId... tasks) {
        final List<TaskId> taskIds = Arrays.asList(tasks);
        Collections.shuffle(taskIds);
        return new StickyTaskAssignor<>(clients,
                                        new HashSet<>(taskIds));
    }

    private List<TaskId> allActiveTasks() {
        final List<TaskId> allActive = new ArrayList<>();
        for (final ClientState client : clients.values()) {
            allActive.addAll(client.activeTasks());
        }
        Collections.sort(allActive);
        return allActive;
    }

    private List<TaskId> allStandbyTasks() {
        final List<TaskId> tasks = new ArrayList<>();
        for (final ClientState client : clients.values()) {
            tasks.addAll(client.standbyTasks());
        }
        Collections.sort(tasks);
        return tasks;
    }

    private ClientState createClient(final Integer processId, final int capacity) {
        return createClientWithPreviousActiveTasks(processId, capacity);
    }

    private ClientState createClientWithPreviousActiveTasks(final Integer processId, final int capacity, final TaskId... taskIds) {
        final ClientState clientState = new ClientState(capacity);
        clientState.addPreviousActiveTasks(Utils.mkSet(taskIds));
        clients.put(processId, clientState);
        return clientState;
    }

}
