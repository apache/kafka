/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.kafka.common.utils.Utils.mkList;
import static org.apache.kafka.common.utils.Utils.mkSet;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TaskAssignorTest {

    @Test
    public void testAssignWithoutStandby() {
        HashMap<Integer, ClientState<Integer>> states = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            states.put(i, new ClientState<Integer>(1d));
        }
        Set<Integer> tasks;
        Map<Integer, ClientState<Integer>> assignments;
        int numActiveTasks;
        int numAssignedTasks;

        // # of clients and # of tasks are equal.
        tasks = mkSet(0, 1, 2, 3, 4, 5);
        assignments = TaskAssignor.assign(states, tasks, 0, "TaskAssignorTest-TestAssignWithoutStandby");
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState<Integer> assignment : assignments.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertEquals(1, assignment.activeTasks.size());
            assertEquals(1, assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size(), numAssignedTasks);

        // # of clients < # of tasks
        tasks = mkSet(0, 1, 2, 3, 4, 5, 6, 7);
        assignments = TaskAssignor.assign(states, tasks, 0, "TaskAssignorTest-TestAssignWithoutStandby");
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState<Integer> assignment : assignments.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertTrue(1 <= assignment.activeTasks.size());
            assertTrue(2 >= assignment.activeTasks.size());
            assertTrue(1 <= assignment.assignedTasks.size());
            assertTrue(2 >= assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size(), numAssignedTasks);

        // # of clients > # of tasks
        tasks = mkSet(0, 1, 2, 3);
        assignments = TaskAssignor.assign(states, tasks, 0, "TaskAssignorTest-TestAssignWithoutStandby");
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState<Integer> assignment : assignments.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertTrue(0 <= assignment.activeTasks.size());
            assertTrue(1 >= assignment.activeTasks.size());
            assertTrue(0 <= assignment.assignedTasks.size());
            assertTrue(1 >= assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size(), numAssignedTasks);
    }

    @Test
    public void testAssignWithStandby() {
        HashMap<Integer, ClientState<Integer>> states = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            states.put(i, new ClientState<Integer>(1d));
        }
        Set<Integer> tasks;
        Map<Integer, ClientState<Integer>> assignments;
        int numActiveTasks;
        int numAssignedTasks;

        // # of clients and # of tasks are equal.
        tasks = mkSet(0, 1, 2, 3, 4, 5);

        // 1 standby replicas.
        numActiveTasks = 0;
        numAssignedTasks = 0;
        assignments = TaskAssignor.assign(states, tasks, 1, "TaskAssignorTest-TestAssignWithStandby");
        for (ClientState<Integer> assignment : assignments.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertEquals(1, assignment.activeTasks.size());
            assertEquals(2, assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size() * 2, numAssignedTasks);

        // # of clients < # of tasks
        tasks = mkSet(0, 1, 2, 3, 4, 5, 6, 7);

        // 1 standby replicas.
        assignments = TaskAssignor.assign(states, tasks, 1, "TaskAssignorTest-TestAssignWithStandby");
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState<Integer> assignment : assignments.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertTrue(1 <= assignment.activeTasks.size());
            assertTrue(2 >= assignment.activeTasks.size());
            assertTrue(2 <= assignment.assignedTasks.size());
            assertTrue(3 >= assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size() * 2, numAssignedTasks);

        // # of clients > # of tasks
        tasks = mkSet(0, 1, 2, 3);

        // 1 standby replicas.
        assignments = TaskAssignor.assign(states, tasks, 1, "TaskAssignorTest-TestAssignWithStandby");
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState<Integer> assignment : assignments.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertTrue(0 <= assignment.activeTasks.size());
            assertTrue(1 >= assignment.activeTasks.size());
            assertTrue(1 <= assignment.assignedTasks.size());
            assertTrue(2 >= assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size() * 2, numAssignedTasks);

        // # of clients >> # of tasks
        tasks = mkSet(0, 1);

        // 1 standby replicas.
        assignments = TaskAssignor.assign(states, tasks, 1, "TaskAssignorTest-TestAssignWithStandby");
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState<Integer> assignment : assignments.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertTrue(0 <= assignment.activeTasks.size());
            assertTrue(1 >= assignment.activeTasks.size());
            assertTrue(0 <= assignment.assignedTasks.size());
            assertTrue(1 >= assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size() * 2, numAssignedTasks);

        // 2 standby replicas.
        assignments = TaskAssignor.assign(states, tasks, 2, "TaskAssignorTest-TestAssignWithStandby");
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState<Integer> assignment : assignments.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertTrue(0 <= assignment.activeTasks.size());
            assertTrue(1 >= assignment.activeTasks.size());
            assertTrue(1 == assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size() * 3, numAssignedTasks);

        // 3 standby replicas.
        assignments = TaskAssignor.assign(states, tasks, 3, "TaskAssignorTest-TestAssignWithStandby");
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState<Integer> assignment : assignments.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertTrue(0 <= assignment.activeTasks.size());
            assertTrue(1 >= assignment.activeTasks.size());
            assertTrue(1 <= assignment.assignedTasks.size());
            assertTrue(2 >= assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size() * 4, numAssignedTasks);
    }

    @Test
    public void testStickiness() {
        List<Integer> tasks;
        Map<Integer, ClientState<Integer>> states;
        Map<Integer, ClientState<Integer>> assignments;
        int i;

        // # of clients and # of tasks are equal.
        tasks = mkList(0, 1, 2, 3, 4, 5);
        Collections.shuffle(tasks);
        states = new HashMap<>();
        i = 0;
        for (int task : tasks) {
            ClientState<Integer> state = new ClientState<>(1d);
            state.prevActiveTasks.add(task);
            state.prevAssignedTasks.add(task);
            states.put(i++, state);
        }
        assignments = TaskAssignor.assign(states, mkSet(0, 1, 2, 3, 4, 5), 0, "TaskAssignorTest-TestStickiness");
        for (int client : states.keySet()) {
            Set<Integer> oldActive = states.get(client).prevActiveTasks;
            Set<Integer> oldAssigned = states.get(client).prevAssignedTasks;
            Set<Integer> newActive = assignments.get(client).activeTasks;
            Set<Integer> newAssigned = assignments.get(client).assignedTasks;

            assertEquals(oldActive, newActive);
            assertEquals(oldAssigned, newAssigned);
        }

        // # of clients > # of tasks
        tasks = mkList(0, 1, 2, 3, -1, -1);
        Collections.shuffle(tasks);
        states = new HashMap<>();
        i = 0;
        for (int task : tasks) {
            ClientState<Integer> state = new ClientState<>(1d);
            if (task >= 0) {
                state.prevActiveTasks.add(task);
                state.prevAssignedTasks.add(task);
            }
            states.put(i++, state);
        }
        assignments = TaskAssignor.assign(states, mkSet(0, 1, 2, 3), 0, "TaskAssignorTest-TestStickiness");
        for (int client : states.keySet()) {
            Set<Integer> oldActive = states.get(client).prevActiveTasks;
            Set<Integer> oldAssigned = states.get(client).prevAssignedTasks;
            Set<Integer> newActive = assignments.get(client).activeTasks;
            Set<Integer> newAssigned = assignments.get(client).assignedTasks;

            assertEquals(oldActive, newActive);
            assertEquals(oldAssigned, newAssigned);
        }

        // # of clients < # of tasks
        List<Set<Integer>> taskSets = mkList(mkSet(0, 1), mkSet(2, 3), mkSet(4, 5), mkSet(6, 7), mkSet(8, 9), mkSet(10, 11));
        Collections.shuffle(taskSets);
        states = new HashMap<>();
        i = 0;
        for (Set<Integer> taskSet : taskSets) {
            ClientState<Integer> state = new ClientState<>(1d);
            state.prevActiveTasks.addAll(taskSet);
            state.prevAssignedTasks.addAll(taskSet);
            states.put(i++, state);
        }
        assignments = TaskAssignor.assign(states, mkSet(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), 0, "TaskAssignorTest-TestStickiness");
        for (int client : states.keySet()) {
            Set<Integer> oldActive = states.get(client).prevActiveTasks;
            Set<Integer> oldAssigned = states.get(client).prevAssignedTasks;
            Set<Integer> newActive = assignments.get(client).activeTasks;
            Set<Integer> newAssigned = assignments.get(client).assignedTasks;

            Set<Integer> intersection = new HashSet<>();

            intersection.addAll(oldActive);
            intersection.retainAll(newActive);
            assertTrue(intersection.size() > 0);

            intersection.clear();
            intersection.addAll(oldAssigned);
            intersection.retainAll(newAssigned);
            assertTrue(intersection.size() > 0);
        }
    }

}
