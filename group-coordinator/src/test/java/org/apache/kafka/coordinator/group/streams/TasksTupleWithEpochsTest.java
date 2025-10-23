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

import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;

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

public class TasksTupleWithEpochsTest {

    private static final String SUBTOPOLOGY_1 = "1";
    private static final String SUBTOPOLOGY_2 = "2";
    private static final String SUBTOPOLOGY_3 = "3";

    @Test
    public void testTasksCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new TasksTupleWithEpochs(null, Map.of(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TasksTupleWithEpochs(Map.of(), null, Map.of()));
        assertThrows(NullPointerException.class, () -> new TasksTupleWithEpochs(Map.of(), Map.of(), null));
    }

    @Test
    public void testReturnUnmodifiableTaskAssignments() {
        Map<String, Map<Integer, Integer>> activeTasks = Map.of(
            SUBTOPOLOGY_1, Map.of(1, 10, 2, 11, 3, 12)
        );
        Map<String, Set<Integer>> standbyTasks = mkTasksPerSubtopology(
            mkTasks(SUBTOPOLOGY_2, 9, 8, 7)
        );
        Map<String, Set<Integer>> warmupTasks = mkTasksPerSubtopology(
            mkTasks(SUBTOPOLOGY_3, 4, 5, 6)
        );
        TasksTupleWithEpochs tuple = new TasksTupleWithEpochs(activeTasks, standbyTasks, warmupTasks);

        assertEquals(activeTasks, tuple.activeTasksWithEpochs());
        assertThrows(UnsupportedOperationException.class, () -> tuple.activeTasksWithEpochs().put("not allowed", Map.of()));
        assertEquals(standbyTasks, tuple.standbyTasks());
        assertThrows(UnsupportedOperationException.class, () -> tuple.standbyTasks().put("not allowed", Set.of()));
        assertEquals(warmupTasks, tuple.warmupTasks());
        assertThrows(UnsupportedOperationException.class, () -> tuple.warmupTasks().put("not allowed", Set.of()));
    }


    @Test
    public void testFromCurrentAssignmentRecord() {
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> activeTasks = new ArrayList<>();
        activeTasks.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setPartitions(Arrays.asList(1, 2, 3))
            .setAssignmentEpochs(Arrays.asList(10, 11, 12)));
        activeTasks.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_2)
            .setPartitions(Arrays.asList(4, 5, 6))
            .setAssignmentEpochs(Arrays.asList(20, 21, 22)));

        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> standbyTasks = new ArrayList<>();
        standbyTasks.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setPartitions(Arrays.asList(7, 8, 9)));
        standbyTasks.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_2)
            .setPartitions(Arrays.asList(1, 2, 3)));

        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> warmupTasks = new ArrayList<>();
        warmupTasks.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setPartitions(Arrays.asList(4, 5, 6)));
        warmupTasks.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_2)
            .setPartitions(Arrays.asList(7, 8, 9)));

        TasksTupleWithEpochs tuple = TasksTupleWithEpochs.fromCurrentAssignmentRecord(
            activeTasks, standbyTasks, warmupTasks, 100
        );

        assertEquals(
            Map.of(
                SUBTOPOLOGY_1, Map.of(1, 10, 2, 11, 3, 12),
                SUBTOPOLOGY_2, Map.of(4, 20, 5, 21, 6, 22)
            ),
            tuple.activeTasksWithEpochs()
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
    public void testFromCurrentAssignmentRecordWithoutEpochs() {
        // Test legacy format where epochs are not present
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> activeTasks = new ArrayList<>();
        activeTasks.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setPartitions(Arrays.asList(1, 2, 3)));

        int memberEpoch = 100;
        TasksTupleWithEpochs tuple = TasksTupleWithEpochs.fromCurrentAssignmentRecord(
            activeTasks, List.of(), List.of(), memberEpoch
        );

        // Should use member epoch as default
        assertEquals(
            Map.of(SUBTOPOLOGY_1, Map.of(1, memberEpoch, 2, memberEpoch, 3, memberEpoch)),
            tuple.activeTasksWithEpochs()
        );
    }

    @Test
    public void testFromCurrentAssignmentRecordWithMismatchedEpochs() {
        // Test error case where number of epochs doesn't match number of partitions
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> activeTasks = new ArrayList<>();
        activeTasks.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setPartitions(Arrays.asList(1, 2, 3))
            .setAssignmentEpochs(Arrays.asList(10, 11))); // Only 2 epochs for 3 partitions

        assertThrows(IllegalStateException.class, () ->
            TasksTupleWithEpochs.fromCurrentAssignmentRecord(activeTasks, List.of(), List.of(), 100)
        );
    }

    @Test
    public void testIsEmpty() {
        TasksTupleWithEpochs emptyTuple = new TasksTupleWithEpochs(Map.of(), Map.of(), Map.of());
        assertTrue(emptyTuple.isEmpty());
        assertEquals(TasksTupleWithEpochs.EMPTY, emptyTuple);

        TasksTupleWithEpochs nonEmptyTuple = new TasksTupleWithEpochs(
            Map.of(SUBTOPOLOGY_1, Map.of(1, 10)),
            Map.of(),
            Map.of()
        );
        assertFalse(nonEmptyTuple.isEmpty());
    }

    @Test
    public void testMerge() {
        TasksTupleWithEpochs tuple1 = new TasksTupleWithEpochs(
            Map.of(SUBTOPOLOGY_1, Map.of(1, 10, 2, 11)),
            Map.of(SUBTOPOLOGY_2, Set.of(4, 5)),
            Map.of(SUBTOPOLOGY_3, Set.of(7, 8))
        );

        TasksTupleWithEpochs tuple2 = new TasksTupleWithEpochs(
            Map.of(
                SUBTOPOLOGY_1, Map.of(3, 13), // Different partition in same subtopology
                SUBTOPOLOGY_2, Map.of(6, 26)  // Different subtopology
            ),
            Map.of(SUBTOPOLOGY_2, Set.of(9, 10)),
            Map.of(SUBTOPOLOGY_3, Set.of(11, 12))
        );

        TasksTupleWithEpochs merged = tuple1.merge(tuple2);

        assertEquals(
            Map.of(
                SUBTOPOLOGY_1, Map.of(1, 10, 2, 11, 3, 13),
                SUBTOPOLOGY_2, Map.of(6, 26)
            ),
            merged.activeTasksWithEpochs()
        );
        assertEquals(
            mkTasksPerSubtopology(
                mkTasks(SUBTOPOLOGY_2, 4, 5, 9, 10)
            ),
            merged.standbyTasks()
        );
        assertEquals(
            mkTasksPerSubtopology(
                mkTasks(SUBTOPOLOGY_3, 7, 8, 11, 12)
            ),
            merged.warmupTasks()
        );
    }

    @Test
    public void testMergeWithOverlappingActiveTasks() {
        // When merging overlapping active tasks, epochs from the second tuple take precedence
        TasksTupleWithEpochs tuple1 = new TasksTupleWithEpochs(
            Map.of(SUBTOPOLOGY_1, Map.of(1, 10, 2, 11)),
            Map.of(),
            Map.of()
        );

        TasksTupleWithEpochs tuple2 = new TasksTupleWithEpochs(
            Map.of(SUBTOPOLOGY_1, Map.of(1, 99, 3, 13)), // partition 1 overlaps with different epoch
            Map.of(),
            Map.of()
        );

        TasksTupleWithEpochs merged = tuple1.merge(tuple2);

        // Epoch for partition 1 should be from tuple2 (99, not 10) since the second tuple takes precedence
        assertEquals(99, merged.activeTasksWithEpochs().get(SUBTOPOLOGY_1).get(1));
        assertEquals(11, merged.activeTasksWithEpochs().get(SUBTOPOLOGY_1).get(2));
        assertEquals(13, merged.activeTasksWithEpochs().get(SUBTOPOLOGY_1).get(3));
    }

    @Test
    public void testToString() {
        TasksTupleWithEpochs tuple = new TasksTupleWithEpochs(
            Map.of(
                SUBTOPOLOGY_1, Map.of(1, 10, 2, 11),
                SUBTOPOLOGY_2, Map.of(3, 20)
            ),
            Map.of(SUBTOPOLOGY_2, Set.of(4, 5)),
            Map.of(SUBTOPOLOGY_3, Set.of(6))
        );

        String result = tuple.toString();
        
        // Verify the exact toString format
        assertEquals(
            "(active=[1-1@10, 1-2@11, 2-3@20], " +
            "standby=[2-4, 2-5], " +
            "warmup=[3-6])",
            result
        );
    }
}
