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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable tuple containing active, standby and warm-up tasks with assignment epochs.
 * <p>
 * Active tasks include epoch information to support fencing of zombie commits.
 * Standby and warmup tasks do not have epochs as they don't commit offsets.
 *
 * @param activeTasksWithEpochs Active tasks with their assignment epochs.
 *                              The outer map key is the subtopology ID, the inner map key is the partition ID,
 *                              and the inner map value is the assignment epoch.
 * @param standbyTasks          Standby tasks.
 *                              The key of the map is the subtopology ID, and the value is the set of partition IDs.
 * @param warmupTasks           Warm-up tasks.
 *                              The key of the map is the subtopology ID, and the value is the set of partition IDs.
 */
public record TasksTupleWithEpochs(Map<String, Map<Integer, Integer>> activeTasksWithEpochs,
                                   Map<String, Set<Integer>> standbyTasks,
                                   Map<String, Set<Integer>> warmupTasks) {

    public TasksTupleWithEpochs {
        activeTasksWithEpochs = Collections.unmodifiableMap(Objects.requireNonNull(activeTasksWithEpochs));
        standbyTasks = Collections.unmodifiableMap(Objects.requireNonNull(standbyTasks));
        warmupTasks = Collections.unmodifiableMap(Objects.requireNonNull(warmupTasks));
    }

    /**
     * An empty task tuple.
     */
    public static final TasksTupleWithEpochs EMPTY = new TasksTupleWithEpochs(
        Map.of(),
        Map.of(),
        Map.of()
    );

    /**
     * @return true if all collections in the tuple are empty.
     */
    public boolean isEmpty() {
        return activeTasksWithEpochs.isEmpty() && standbyTasks.isEmpty() && warmupTasks.isEmpty();
    }

    /**
     * Merges this task tuple with another task tuple.
     * For overlapping active tasks, epochs from the other tuple take precedence.
     *
     * @param other The other task tuple.
     * @return A new task tuple, containing all active tasks, standby tasks and warm-up tasks from both tuples.
     */
    public TasksTupleWithEpochs merge(TasksTupleWithEpochs other) {
        Map<String, Map<Integer, Integer>> mergedActive = new HashMap<>();

        // Add all tasks from this tuple
        this.activeTasksWithEpochs.forEach((subtopologyId, partitionsWithEpochs) -> {
            mergedActive.put(subtopologyId, new HashMap<>(partitionsWithEpochs));
        });

        // Add tasks from other tuple, overwriting epochs for overlapping partitions
        other.activeTasksWithEpochs.forEach((subtopologyId, partitionsWithEpochs) -> {
            mergedActive.computeIfAbsent(subtopologyId, k -> new HashMap<>())
                .putAll(partitionsWithEpochs);
        });

        Map<String, Set<Integer>> mergedStandby = mergeTasks(this.standbyTasks, other.standbyTasks);
        Map<String, Set<Integer>> mergedWarmup = mergeTasks(this.warmupTasks, other.warmupTasks);
        return new TasksTupleWithEpochs(mergedActive, mergedStandby, mergedWarmup);
    }

    /**
     * Creates a TasksTupleWithEpochs from a current assignment record.
     *
     * @param activeTasks                    The active tasks from the record.
     * @param standbyTasks                   The standby tasks from the record.
     * @param warmupTasks                    The warmup tasks from the record.
     * @param memberEpoch                    The member epoch to use as default for tasks without explicit epochs.
     * @return The TasksTupleWithEpochs
     */
    public static TasksTupleWithEpochs fromCurrentAssignmentRecord(
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> activeTasks,
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> standbyTasks,
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> warmupTasks,
        int memberEpoch
    ) {
        return new TasksTupleWithEpochs(
            parseActiveTasksWithEpochs(activeTasks, memberEpoch),
            parseSimpleTasks(standbyTasks),
            parseSimpleTasks(warmupTasks)
        );
    }

    private static Map<String, Set<Integer>> mergeTasks(final Map<String, Set<Integer>> tasks1, final Map<String, Set<Integer>> tasks2) {
        HashMap<String, Set<Integer>> result = new HashMap<>();
        tasks1.forEach((subtopologyId, tasks) ->
            result.put(subtopologyId, new HashSet<>(tasks)));
        tasks2.forEach((subtopologyId, tasks) -> result
            .computeIfAbsent(subtopologyId, __ -> new HashSet<>())
            .addAll(tasks));
        return result;
    }

    private static Map<String, Map<Integer, Integer>> parseActiveTasksWithEpochs(
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> taskIdsList,
        int memberEpoch
    ) {
        Map<String, Map<Integer, Integer>> result = new HashMap<>();

        for (StreamsGroupCurrentMemberAssignmentValue.TaskIds taskIds : taskIdsList) {
            String subtopologyId = taskIds.subtopologyId();
            List<Integer> partitions = taskIds.partitions();
            List<Integer> epochs = taskIds.assignmentEpochs();

            Map<Integer, Integer> partitionsWithEpochs = new HashMap<>();

            if (epochs != null && !epochs.isEmpty()) {
                if (epochs.size() != partitions.size()) {
                    throw new IllegalStateException(
                        "Assignment epochs must be provided for all partitions. " +
                        "Subtopology " + subtopologyId + " has " + partitions.size() +
                        " partitions but " + epochs.size() + " epochs"
                    );
                }

                for (int i = 0; i < partitions.size(); i++) {
                    partitionsWithEpochs.put(partitions.get(i), epochs.get(i));
                }
            } else {
                // Legacy record without epochs: use member epoch as default
                for (Integer partition : partitions) {
                    partitionsWithEpochs.put(partition, memberEpoch);
                }
            }

            result.put(subtopologyId, partitionsWithEpochs);
        }

        return result;
    }

    private static Map<String, Set<Integer>> parseSimpleTasks(
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> taskIdsList
    ) {
        Map<String, Set<Integer>> result = new HashMap<>();

        for (StreamsGroupCurrentMemberAssignmentValue.TaskIds taskIds : taskIdsList) {
            result.put(taskIds.subtopologyId(), new HashSet<>(taskIds.partitions()));
        }

        return result;
    }

    @Override
    public String toString() {
        return "(active=" + taskAssignmentToString(activeTasksWithEpochs) +
            ", standby=" + TasksTuple.taskAssignmentToString(standbyTasks) +
            ", warmup=" + TasksTuple.taskAssignmentToString(warmupTasks) +
            ')';
    }

    private static String taskAssignmentToString(Map<String, Map<Integer, Integer>> assignment) {
        StringBuilder builder = new StringBuilder("[");
        
        // Sort subtopology IDs
        String[] subtopologyIds = assignment.keySet().toArray(new String[0]);
        java.util.Arrays.sort(subtopologyIds);
        
        boolean first = true;
        for (String subtopologyId : subtopologyIds) {
            Map<Integer, Integer> partitions = assignment.get(subtopologyId);
            
            // Sort partition IDs
            Integer[] partitionIds = partitions.keySet().toArray(new Integer[0]);
            java.util.Arrays.sort(partitionIds);
            
            for (Integer partitionId : partitionIds) {
                if (!first) {
                    builder.append(", ");
                }
                builder.append(subtopologyId);
                builder.append("-");
                builder.append(partitionId);
                builder.append("@");
                builder.append(partitions.get(partitionId));
                first = false;
            }
        }
        builder.append("]");
        return builder.toString();
    }
}
