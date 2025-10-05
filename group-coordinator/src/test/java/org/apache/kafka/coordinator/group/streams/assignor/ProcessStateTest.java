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

package org.apache.kafka.coordinator.group.streams.assignor;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessStateTest {

    @Test
    void shouldAddActiveTaskToMember() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        TaskId taskId = new TaskId("id1", 0);

        processState.addTask("member1", taskId, true);

        assertTrue(processState.assignedActiveTasks().contains(taskId));
        assertEquals(1, processState.memberToTaskCounts().get("member1"));
    }

    @Test
    void shouldAddStandbyTaskToMember() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        TaskId taskId = new TaskId("id1", 0);

        processState.addTask("member1", taskId, false);

        assertTrue(processState.assignedStandbyTasks().contains(taskId));
        assertEquals(1, processState.memberToTaskCounts().get("member1"));
    }

    @Test
    void shouldIncreaseCapacity() {
        ProcessState processState = new ProcessState("process1");

        processState.addMember("member1");

        assertEquals(1, processState.capacity());
    }

    @Test
    void shouldReturnTrueWhenCapacityIsAvailable() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");

        assertTrue(processState.hasCapacity());
    }

    @Test
    void shouldReturnFalseWhenCapacityIsFull() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        processState.addTask("member1", new TaskId("id1", 0), true);

        assertFalse(processState.hasCapacity());
    }

    @Test
    void shouldCompareBasedOnLoadAndCapacity() {
        ProcessState processState1 = new ProcessState("process1");
        ProcessState processState2 = new ProcessState("process2");
        processState1.addMember("member1");
        processState2.addMember("member2");
        processState1.addTask("member1", new TaskId("id1", 0), true);

        assertTrue(processState1.compareTo(processState2) > 0);
    }

    @Test
    void shouldReturnTrueIfTaskIsAssigned() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        TaskId taskId = new TaskId("id1", 0);
        processState.addTask("member1", taskId, true);

        assertTrue(processState.hasTask(taskId));
    }

    @Test
    void shouldReturnFalseIfTaskIsNotAssigned() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        TaskId taskId = new TaskId("id1", 0);

        assertFalse(processState.hasTask(taskId));
    }

    @Test
    void shouldReturnAllAssignedTasks() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        TaskId activeTaskId = new TaskId("id1", 0);
        TaskId standbyTaskId = new TaskId("id1", 1);
        processState.addTask("member1", activeTaskId, true);
        processState.addTask("member1", standbyTaskId, false);

        Set<TaskId> assignedTasks = processState.assignedTasks();

        assertTrue(assignedTasks.contains(activeTaskId));
        assertTrue(assignedTasks.contains(standbyTaskId));
    }
}
