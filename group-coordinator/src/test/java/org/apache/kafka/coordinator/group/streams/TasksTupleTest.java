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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TasksTupleTest {

    private static final String SUBTOPOLOGY_1 = "subtopology1";
    private static final String SUBTOPOLOGY_2 = "subtopology2";
    private static final String SUBTOPOLOGY_3 = "subtopology3";

    @Test
    public void testTasksCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new TasksTuple(null, Collections.emptyMap(), Collections.emptyMap()));
        assertThrows(NullPointerException.class, () -> new TasksTuple(Collections.emptyMap(), null, Collections.emptyMap()));
        assertThrows(NullPointerException.class, () -> new TasksTuple(Collections.emptyMap(), Collections.emptyMap(), null));
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
        TasksTuple tuple = new TasksTuple(activeTasks, standbyTasks, warmupTasks);

        assertEquals(activeTasks, tuple.activeTasks());
        assertThrows(UnsupportedOperationException.class, () -> tuple.activeTasks().put("not allowed", Collections.emptySet()));
        assertEquals(standbyTasks, tuple.standbyTasks());
        assertThrows(UnsupportedOperationException.class, () -> tuple.standbyTasks().put("not allowed", Collections.emptySet()));
        assertEquals(warmupTasks, tuple.warmupTasks());
        assertThrows(UnsupportedOperationException.class, () -> tuple.warmupTasks().put("not allowed", Collections.emptySet()));
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

        TasksTuple tuple = TasksTuple.fromTargetAssignmentRecord(record);

        assertEquals(
            mkTasksPerSubtopology(
                mkTasks(SUBTOPOLOGY_1, 1, 2, 3),
                mkTasks(SUBTOPOLOGY_2, 4, 5, 6)
            ),
            tuple.activeTasks()
        );
        assertEquals(
            mkTasksPerSubtopology(
                mkTasks(SUBTOPOLOGY_1, 7, 8, 9),
                mkTasks(SUBTOPOLOGY_2, 1, 2, 3)
            ),
            tuple.standbyTasks()
        );
        assertEquals(
            mkTasksPerSubtopology(
                mkTasks(SUBTOPOLOGY_1, 4, 5, 6),
                mkTasks(SUBTOPOLOGY_2, 7, 8, 9)
            ),
            tuple.warmupTasks()
        );
    }

    @Test
    public void testMerge() {
        TasksTuple tuple1 = new TasksTuple(
            Map.of(SUBTOPOLOGY_1, Set.of(1, 2, 3)),
            Map.of(SUBTOPOLOGY_2, Set.of(4, 5, 6)),
            Map.of(SUBTOPOLOGY_3, Set.of(7, 8, 9))
        );

        TasksTuple tuple2 = new TasksTuple(
            Map.of(SUBTOPOLOGY_1, Set.of(10, 11)),
            Map.of(SUBTOPOLOGY_2, Set.of(12, 13)),
            Map.of(SUBTOPOLOGY_3, Set.of(14, 15))
        );

        TasksTuple mergedTuple = tuple1.merge(tuple2);

        assertEquals(Map.of(SUBTOPOLOGY_1, Set.of(1, 2, 3, 10, 11)), mergedTuple.activeTasks());
        assertEquals(Map.of(SUBTOPOLOGY_2, Set.of(4, 5, 6, 12, 13)), mergedTuple.standbyTasks());
        assertEquals(Map.of(SUBTOPOLOGY_3, Set.of(7, 8, 9, 14, 15)), mergedTuple.warmupTasks());
    }

    @Test
    public void testContainsAny() {
        TasksTuple tuple1 = new TasksTuple(
            Map.of(SUBTOPOLOGY_1, Set.of(1, 2, 3)),
            Map.of(SUBTOPOLOGY_2, Set.of(4, 5, 6)),
            Map.of(SUBTOPOLOGY_3, Set.of(7, 8, 9))
        );

        TasksTuple tuple2 = new TasksTuple(
            Map.of(SUBTOPOLOGY_1, Set.of(3, 10, 11)),
            Map.of(SUBTOPOLOGY_2, Set.of(12, 13)),
            Map.of(SUBTOPOLOGY_3, Set.of(14, 15))
        );

        assertTrue(tuple1.containsAny(tuple2));

        TasksTuple tuple3 = new TasksTuple(
            Map.of(SUBTOPOLOGY_1, Set.of(10, 11)),
            Map.of(SUBTOPOLOGY_2, Set.of(12, 13)),
            Map.of(SUBTOPOLOGY_3, Set.of(14, 15))
        );

        assertFalse(tuple1.containsAny(tuple3));
    }

    @Test
    public void testIsEmpty() {
        TasksTuple emptyTuple = new TasksTuple(Map.of(), Map.of(), Map.of());
        assertTrue(emptyTuple.isEmpty());

        TasksTuple nonEmptyTuple = new TasksTuple(Map.of(SUBTOPOLOGY_1, Set.of(1)), Map.of(), Map.of());
        assertFalse(nonEmptyTuple.isEmpty());
    }
}
