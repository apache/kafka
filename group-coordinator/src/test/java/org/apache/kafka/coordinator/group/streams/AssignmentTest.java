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

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentTest {

    static final String SUBTOPOLOGY_1 = "subtopology1";
    static final String SUBTOPOLOGY_2 = "subtopology2";
    static final String SUBTOPOLOGY_3 = "subtopology3";

    @Test
    public void testTasksCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new Assignment(null, Collections.emptyMap(), Collections.emptyMap()));
        assertThrows(NullPointerException.class, () -> new Assignment(Collections.emptyMap(), null, Collections.emptyMap()));
        assertThrows(NullPointerException.class, () -> new Assignment(Collections.emptyMap(), Collections.emptyMap(), null));
    }

    @Test
    public void testReturnUnmodifiableTaskAssignments() {
        Map<String, Set<Integer>> activeTasks = mkTasksPerSubtopology(
            mkTasks(SUBTOPOLOGY_1, 1, 2, 3)
        );
        Map<String, Set<Integer>> standbyTasks = mkTasksPerSubtopology(
            mkTasks(SUBTOPOLOGY_2, 9, 8, 7)
        );
        Map<String, Set<Integer>> warmupTasks = mkTasksPerSubtopology(
            mkTasks(SUBTOPOLOGY_3, 4, 5, 6)
        );
        Assignment assignment = new Assignment(activeTasks, standbyTasks, warmupTasks);

        assertEquals(activeTasks, assignment.activeTasks());
        assertThrows(UnsupportedOperationException.class, () -> assignment.activeTasks().put("not allowed", Collections.emptySet()));
        assertEquals(standbyTasks, assignment.standbyTasks());
        assertThrows(UnsupportedOperationException.class, () -> assignment.standbyTasks().put("not allowed", Collections.emptySet()));
        assertEquals(warmupTasks, assignment.warmupTasks());
        assertThrows(UnsupportedOperationException.class, () -> assignment.warmupTasks().put("not allowed", Collections.emptySet()));
    }

    @Test
    public void testFromTargetAssignmentRecord() {
        List<StreamsGroupTargetAssignmentMemberValue.TaskIds> activeTasks = new ArrayList<>();
        activeTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setPartitions(Arrays.asList(1, 2, 3)));
        activeTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_2)
            .setPartitions(Arrays.asList(4, 5, 6)));
        List<StreamsGroupTargetAssignmentMemberValue.TaskIds> standbyTasks = new ArrayList<>();
        standbyTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setPartitions(Arrays.asList(7, 8, 9)));
        standbyTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_2)
            .setPartitions(Arrays.asList(1, 2, 3)));
        List<StreamsGroupTargetAssignmentMemberValue.TaskIds> warmupTasks = new ArrayList<>();
        warmupTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setPartitions(Arrays.asList(4, 5, 6)));
        warmupTasks.add(new StreamsGroupTargetAssignmentMemberValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_2)
            .setPartitions(Arrays.asList(7, 8, 9)));

        StreamsGroupTargetAssignmentMemberValue record = new StreamsGroupTargetAssignmentMemberValue()
            .setActiveTasks(activeTasks)
            .setStandbyTasks(standbyTasks)
            .setWarmupTasks(warmupTasks);

        Assignment assignment = Assignment.fromRecord(record);

        assertEquals(
            mkTasksPerSubtopology(
                mkTasks(SUBTOPOLOGY_1, 1, 2, 3),
                mkTasks(SUBTOPOLOGY_2, 4, 5, 6)
            ),
            assignment.activeTasks()
        );
        assertEquals(
            mkTasksPerSubtopology(
                mkTasks(SUBTOPOLOGY_1, 7, 8, 9),
                mkTasks(SUBTOPOLOGY_2, 1, 2, 3)
            ),
            assignment.standbyTasks()
        );
        assertEquals(
            mkTasksPerSubtopology(
                mkTasks(SUBTOPOLOGY_1, 4, 5, 6),
                mkTasks(SUBTOPOLOGY_2, 7, 8, 9)
            ),
            assignment.warmupTasks()
        );
    }
}
