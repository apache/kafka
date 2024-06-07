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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkStreamsAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTaskAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentTest {

    @Test
    public void testTasksCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new Assignment(null, Collections.emptyMap(), Collections.emptyMap()));
        assertThrows(NullPointerException.class, () -> new Assignment(Collections.emptyMap(), null, Collections.emptyMap()));
        assertThrows(NullPointerException.class, () -> new Assignment(Collections.emptyMap(), Collections.emptyMap(), null));
    }

    @Test
    public void testAttributes() {
        Map<String, Set<Integer>> activeTasks = mkStreamsAssignment(
            mkTaskAssignment("subtopology1", 1, 2, 3)
        );
        Map<String, Set<Integer>> standbyTasks = mkStreamsAssignment(
            mkTaskAssignment("subtopology2", 9, 8, 7)
        );
        Map<String, Set<Integer>> warmupTasks = mkStreamsAssignment(
            mkTaskAssignment("subtopology3", 4, 5, 6)
        );
        Assignment assignment = new Assignment(activeTasks, standbyTasks, warmupTasks);

        assertEquals(activeTasks, assignment.activeTasks());
        assertEquals(standbyTasks, assignment.standbyTasks());
        assertEquals(warmupTasks, assignment.warmupTasks());
    }

    @Test
    public void testFromTargetAssignmentRecord() {
        String subtopology1 = "subtopology1";
        String subtopology2 = "subtopology2";
        List<StreamsGroupTargetAssignmentMemberValue.TaskId> activeTasks = new ArrayList<>();
        activeTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskId()
            .setSubtopology(subtopology1)
            .setPartitions(Arrays.asList(1, 2, 3)));
        activeTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskId()
            .setSubtopology(subtopology2)
            .setPartitions(Arrays.asList(4, 5, 6)));
        List<StreamsGroupTargetAssignmentMemberValue.TaskId> standbyTasks = new ArrayList<>();
        standbyTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskId()
            .setSubtopology(subtopology1)
            .setPartitions(Arrays.asList(7, 8, 9)));
        standbyTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskId()
            .setSubtopology(subtopology2)
            .setPartitions(Arrays.asList(1, 2, 3)));
        List<StreamsGroupTargetAssignmentMemberValue.TaskId> warmupTasks = new ArrayList<>();
        warmupTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskId()
            .setSubtopology(subtopology1)
            .setPartitions(Arrays.asList(4, 5, 6)));
        warmupTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskId()
            .setSubtopology(subtopology2)
            .setPartitions(Arrays.asList(7, 8, 9)));

        StreamsGroupTargetAssignmentMemberValue record = new StreamsGroupTargetAssignmentMemberValue()
            .setActiveTasks(activeTasks)
            .setStandbyTasks(standbyTasks)
            .setWarmupTasks(warmupTasks);

        Assignment assignment = Assignment.fromRecord(record);

        assertEquals(
            mkStreamsAssignment(
                mkTaskAssignment(subtopology1, 1, 2, 3),
                mkTaskAssignment(subtopology2, 4, 5, 6)
            ),
            assignment.activeTasks()
        );
        assertEquals(
            mkStreamsAssignment(
                mkTaskAssignment(subtopology1, 7, 8, 9),
                mkTaskAssignment(subtopology2, 1, 2, 3)
            ),
            assignment.standbyTasks()
        );
        assertEquals(
            mkStreamsAssignment(
                mkTaskAssignment(subtopology1, 4, 5, 6),
                mkTaskAssignment(subtopology2, 7, 8, 9)
            ),
            assignment.warmupTasks()
        );
    }

    @Test
    public void testEquals() {
        Map<String, Set<Integer>> activeTasks = mkStreamsAssignment(
            mkTaskAssignment("subtopology1", 1, 2, 3)
        );
        Map<String, Set<Integer>> standbyTasks = mkStreamsAssignment(
            mkTaskAssignment("subtopology2", 9, 8, 7)
        );
        Map<String, Set<Integer>> warmupTasks = mkStreamsAssignment(
            mkTaskAssignment("subtopology3", 4, 5, 6)
        );

        assertEquals(
            new Assignment(activeTasks, standbyTasks, warmupTasks),
            new Assignment(activeTasks, standbyTasks, warmupTasks)
        );
    }
}
