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

import java.util.Map;
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

        processState.addTask("member1", taskId, true, true);

        assertEquals(Map.of("member1", Set.of(taskId)), processState.assignedActiveTasksByMember());
        assertEquals(Map.of("member1", 1), processState.memberToTaskCounts());
    }

    @Test
    void shouldAddStandbyTaskToMember() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        TaskId taskId = new TaskId("id1", 0);

        processState.addTask("member1", taskId, false, true);

        assertEquals(Map.of("member1", Set.of(taskId)), processState.assignedStandbyTasksByMember());
        assertEquals(Map.of("member1", 1), processState.memberToTaskCounts());
    }

    @Test
    void shouldComputeLoadFromTaskCountAndCapacity() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        processState.addMember("member2");

        processState.addTask("member1", new TaskId("id1", 0), true, true);
        assertEquals(0.5, processState.load());

        processState.addTask("member2", new TaskId("id1", 1), true, true);
        assertEquals(1.0, processState.load());
    }

    @Test
    void shouldReturnTrueIfTaskIsAssigned() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        TaskId activeTaskId = new TaskId("id1", 0);
        TaskId standbyTaskId = new TaskId("id1", 1);
        processState.addTask("member1", activeTaskId, true, true);
        processState.addTask("member1", standbyTaskId, false, true);

        assertTrue(processState.hasTask(activeTaskId));
        assertTrue(processState.hasTask(standbyTaskId));
    }

    @Test
    void shouldReturnFalseIfTaskIsNotAssigned() {
        ProcessState processState = new ProcessState("process1");
        processState.addMember("member1");
        TaskId taskId = new TaskId("id1", 0);

        assertFalse(processState.hasTask(taskId));
    }
}
