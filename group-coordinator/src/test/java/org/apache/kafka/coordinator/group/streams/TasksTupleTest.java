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
        assertThrows(NullPointerException.class, () -> new TasksTuple(null, Map.of(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TasksTuple(Map.of(), null, Map.of()));
        assertThrows(NullPointerException.class, () -> new TasksTuple(Map.of(), Map.of(), null));
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
        assertThrows(UnsupportedOperationException.class, () -> tuple.activeTasks().put("not allowed", Set.of()));
        assertEquals(standbyTasks, tuple.standbyTasks());
        assertThrows(UnsupportedOperationException.class, () -> tuple.standbyTasks().put("not allowed", Set.of()));
        assertEquals(warmupTasks, tuple.warmupTasks());
        assertThrows(UnsupportedOperationException.class, () -> tuple.warmupTasks().put("not allowed", Set.of()));
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
    public void testContainsAny() {
        TasksTuple tuple1 = new TasksTuple(
            Map.of(SUBTOPOLOGY_1, Set.of(1, 2, 3)),
            Map.of(SUBTOPOLOGY_2, Set.of(4, 5, 6)),
            Map.of(SUBTOPOLOGY_3, Set.of(7, 8, 9))
        );

        // Test with overlapping active tasks
        TasksTupleWithEpochs tuple2 = new TasksTupleWithEpochs(
            Map.of(SUBTOPOLOGY_1, Map.of(3, 5, 10, 5, 11, 5)),
            Map.of(SUBTOPOLOGY_2, Set.of(12, 13)),
            Map.of(SUBTOPOLOGY_3, Set.of(14, 15))
        );

        assertTrue(tuple1.containsAny(tuple2));

        // Test with no overlapping tasks
        TasksTupleWithEpochs tuple3 = new TasksTupleWithEpochs(
            Map.of(SUBTOPOLOGY_1, Map.of(10, 5, 11, 5)),
            Map.of(SUBTOPOLOGY_2, Set.of(12, 13)),
            Map.of(SUBTOPOLOGY_3, Set.of(14, 15))
        );

        assertFalse(tuple1.containsAny(tuple3));

        // Test with overlapping standby tasks
        TasksTupleWithEpochs tuple4 = new TasksTupleWithEpochs(
            Map.of(SUBTOPOLOGY_1, Map.of(10, 5, 11, 5)),
            Map.of(SUBTOPOLOGY_2, Set.of(4, 12, 13)),
            Map.of(SUBTOPOLOGY_3, Set.of(14, 15))
        );

        assertTrue(tuple1.containsAny(tuple4));

        // Test with overlapping warmup tasks
        TasksTupleWithEpochs tuple5 = new TasksTupleWithEpochs(
            Map.of(SUBTOPOLOGY_1, Map.of(10, 5, 11, 5)),
            Map.of(SUBTOPOLOGY_2, Set.of(12, 13)),
            Map.of(SUBTOPOLOGY_3, Set.of(7, 14, 15))
        );

        assertTrue(tuple1.containsAny(tuple5));
    }

    @Test
    public void testIsEmpty() {
        TasksTuple emptyTuple = new TasksTuple(Map.of(), Map.of(), Map.of());
        assertTrue(emptyTuple.isEmpty());

        TasksTuple nonEmptyTuple = new TasksTuple(Map.of(SUBTOPOLOGY_1, Set.of(1)), Map.of(), Map.of());
        assertFalse(nonEmptyTuple.isEmpty());
    }
}
