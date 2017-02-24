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

import org.apache.kafka.streams.processor.TaskId;
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

    private static Map<Integer, ClientState> copyStates(Map<Integer, ClientState> states) {
        Map<Integer, ClientState> copy = new HashMap<>();
        for (Map.Entry<Integer, ClientState> entry : states.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().copy());
        }

        return copy;
    }

    @Test
    public void testAssignWithoutStandby() {
        HashMap<Integer, ClientState> statesWithNoPrevTasks = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            statesWithNoPrevTasks.put(i, new ClientState(1d));
        }
        Set<TaskId> tasks;
        int numActiveTasks;
        int numAssignedTasks;

        Map<Integer, ClientState> states;

        // # of clients and # of tasks are equal.
        states = copyStates(statesWithNoPrevTasks);
        tasks = mkSet(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3),
                new TaskId(4, 4), new TaskId(5, 5));
        TaskAssignor.assign(states, tasks, 0);
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState assignment : states.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertEquals(1, assignment.activeTasks.size());
            assertEquals(1, assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size(), numAssignedTasks);

        // # of clients < # of tasks
        tasks = mkSet(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3),
                new TaskId(4, 4), new TaskId(5, 5), new TaskId(6, 6), new TaskId(7, 7));
        states = copyStates(statesWithNoPrevTasks);
        TaskAssignor.assign(states, tasks, 0);
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState assignment : states.values()) {
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
        tasks = mkSet(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3));
        states = copyStates(statesWithNoPrevTasks);
        TaskAssignor.assign(states, tasks, 0);
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState assignment : states.values()) {
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
        HashMap<Integer, ClientState> statesWithNoPrevTasks = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            statesWithNoPrevTasks.put(i, new ClientState(1d));
        }
        Set<TaskId> tasks;
        Map<Integer, ClientState> states;
        int numActiveTasks;
        int numAssignedTasks;

        // # of clients and # of tasks are equal.
        tasks = mkSet(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3),
                new TaskId(4, 4), new TaskId(5, 5));

        // 1 standby replicas.
        numActiveTasks = 0;
        numAssignedTasks = 0;
        states = copyStates(statesWithNoPrevTasks);
        TaskAssignor.assign(states, tasks, 1);
        for (ClientState assignment : states.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertEquals(1, assignment.activeTasks.size());
            assertEquals(2, assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size() * 2, numAssignedTasks);

        // # of clients < # of tasks
        tasks = mkSet(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3),
                new TaskId(4, 4), new TaskId(5, 5), new TaskId(6, 6), new TaskId(7, 7));

        // 1 standby replicas.
        states = copyStates(statesWithNoPrevTasks);
        TaskAssignor.assign(states, tasks, 1);
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState assignment : states.values()) {
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
        tasks = mkSet(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3));

        // 1 standby replicas.
        states = copyStates(statesWithNoPrevTasks);
        TaskAssignor.assign(states, tasks, 1);
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState assignment : states.values()) {
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
        tasks = mkSet(new TaskId(0, 0), new TaskId(1, 1));

        // 1 standby replicas.
        states = copyStates(statesWithNoPrevTasks);
        TaskAssignor.assign(states, tasks, 1);
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState assignment : states.values()) {
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
        states = copyStates(statesWithNoPrevTasks);
        TaskAssignor.assign(states, tasks, 2);
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState assignment : states.values()) {
            numActiveTasks += assignment.activeTasks.size();
            numAssignedTasks += assignment.assignedTasks.size();
            assertTrue(0 <= assignment.activeTasks.size());
            assertTrue(1 >= assignment.activeTasks.size());
            assertTrue(1 == assignment.assignedTasks.size());
        }
        assertEquals(tasks.size(), numActiveTasks);
        assertEquals(tasks.size() * 3, numAssignedTasks);

        // 3 standby replicas.
        states = copyStates(statesWithNoPrevTasks);
        TaskAssignor.assign(states, tasks, 3);
        numActiveTasks = 0;
        numAssignedTasks = 0;
        for (ClientState assignment : states.values()) {
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
        List<TaskId> tasks;
        Map<Integer, ClientState> statesWithPrevTasks;
        Map<Integer, ClientState> assignments;
        int i;

        // # of clients and # of tasks are equal.
        Map<Integer, ClientState> states;
        tasks = mkList(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3),
                new TaskId(4, 4), new TaskId(5, 5));
        Collections.shuffle(tasks);
        statesWithPrevTasks = new HashMap<>();
        i = 0;
        for (TaskId task : tasks) {
            ClientState state = new ClientState(1d);
            state.prevActiveTasks.add(task);
            state.prevAssignedTasks.add(task);
            statesWithPrevTasks.put(i++, state);
        }
        states = copyStates(statesWithPrevTasks);
        TaskAssignor.assign(states, mkSet(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3),
                new TaskId(4, 4), new TaskId(5, 5)), 0);
        for (int client : states.keySet()) {
            Set<TaskId> oldActive = statesWithPrevTasks.get(client).prevActiveTasks;
            Set<TaskId> oldAssigned = statesWithPrevTasks.get(client).prevAssignedTasks;
            Set<TaskId> newActive = states.get(client).activeTasks;
            Set<TaskId> newAssigned = states.get(client).assignedTasks;

            assertEquals(oldActive, newActive);
            assertEquals(oldAssigned, newAssigned);
        }

        // # of clients > # of tasks
        tasks = mkList(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3),
                new TaskId(4, 4), new TaskId(5, 5), new TaskId(-1, -1), new TaskId(-1, -1));
        Collections.shuffle(tasks);
        statesWithPrevTasks = new HashMap<>();
        i = 0;
        for (TaskId task : tasks) {
            ClientState state = new ClientState(1d);
            if (task.topicGroupId >= 0) {
                state.prevActiveTasks.add(task);
                state.prevAssignedTasks.add(task);
            }
            statesWithPrevTasks.put(i++, state);
        }
        states = copyStates(statesWithPrevTasks);
        TaskAssignor.assign(states, mkSet(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2),
                new TaskId(3, 3)), 0);
        for (int client : states.keySet()) {
            Set<TaskId> oldActive = statesWithPrevTasks.get(client).prevActiveTasks;
            Set<TaskId> oldAssigned = statesWithPrevTasks.get(client).prevAssignedTasks;
            Set<TaskId> newActive = states.get(client).activeTasks;
            Set<TaskId> newAssigned = states.get(client).assignedTasks;

            assertEquals(oldActive, newActive);
            assertEquals(oldAssigned, newAssigned);
        }

        // # of clients < # of tasks
        List<Set<TaskId>> taskSets = mkList(mkSet(new TaskId(0, 0), new TaskId(1, 1)),
                mkSet(new TaskId(2, 2), new TaskId(3, 3)),
                mkSet(new TaskId(4, 4), new TaskId(5, 5)),
                mkSet(new TaskId(6, 6), new TaskId(7, 7)),
                mkSet(new TaskId(8, 8), new TaskId(9, 9)),
                mkSet(new TaskId(10, 10), new TaskId(11, 11)));
        Collections.shuffle(taskSets);
        statesWithPrevTasks = new HashMap<>();
        i = 0;
        for (Set<TaskId> taskSet : taskSets) {
            ClientState state = new ClientState(1d);
            state.prevActiveTasks.addAll(taskSet);
            state.prevAssignedTasks.addAll(taskSet);
            statesWithPrevTasks.put(i++, state);
        }
        states = copyStates(statesWithPrevTasks);
        TaskAssignor.assign(states, mkSet(new TaskId(0, 0), new TaskId(1, 1), new TaskId(2, 2), new TaskId(3, 3),
                new TaskId(4, 4), new TaskId(5, 5), new TaskId(6, 6), new TaskId(7, 7)
                , new TaskId(8, 8), new TaskId(9, 9), new TaskId(10, 10), new TaskId(11, 11)), 0);
        for (int client : states.keySet()) {
            Set<TaskId> oldActive = statesWithPrevTasks.get(client).prevActiveTasks;
            Set<TaskId> oldAssigned = statesWithPrevTasks.get(client).prevAssignedTasks;
            Set<TaskId> newActive = states.get(client).activeTasks;
            Set<TaskId> newAssigned = states.get(client).assignedTasks;

            Set<TaskId> intersection = new HashSet<>();

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
