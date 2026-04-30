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

import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberValue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An immutable tuple containing active, standby and warm-up tasks.
 *
 * @param activeTasks           Active tasks.
 *                              The key of the map is the subtopology ID, and the value is the set of partition IDs.
 * @param standbyTasks          Standby tasks.
 *                              The key of the map is the subtopology ID, and the value is the set of partition IDs.
 * @param warmupTasks           Warm-up tasks.
 *                              The key of the map is the subtopology ID, and the value is the set of partition IDs.
 */
public record TasksTuple(Map<String, Set<Integer>> activeTasks,
                         Map<String, Set<Integer>> standbyTasks,
                         Map<String, Set<Integer>> warmupTasks) {

    public TasksTuple {
        activeTasks = Collections.unmodifiableMap(Objects.requireNonNull(activeTasks));
        standbyTasks = Collections.unmodifiableMap(Objects.requireNonNull(standbyTasks));
        warmupTasks = Collections.unmodifiableMap(Objects.requireNonNull(warmupTasks));
    }

    /**
     * An empty task tuple.
     */
    public static final TasksTuple EMPTY = new TasksTuple(
        Map.of(),
        Map.of(),
        Map.of()
    );

    /**
     * @return true if all collections in the tuple are empty.
     */
    public boolean isEmpty() {
        return activeTasks.isEmpty() && standbyTasks.isEmpty() && warmupTasks.isEmpty();
    }

    /**
     * Checks if this task tuple contains any of the tasks in another task tuple with epochs.
     *
     * @param other Another task tuple with epochs.
     * @return true if there is at least one active, standby or warm-up task that is present in both tuples.
     */
    public boolean containsAny(TasksTupleWithEpochs other) {
        return activeTasks.entrySet().stream().anyMatch(
            entry -> other.activeTasksWithEpochs().containsKey(entry.getKey()) && !Collections.disjoint(entry.getValue(), other.activeTasksWithEpochs().get(entry.getKey()).keySet())
        ) || standbyTasks.entrySet().stream().anyMatch(
            entry -> other.standbyTasks().containsKey(entry.getKey()) && !Collections.disjoint(entry.getValue(), other.standbyTasks().get(entry.getKey()))
        ) || warmupTasks.entrySet().stream().anyMatch(
            entry -> other.warmupTasks().containsKey(entry.getKey()) && !Collections.disjoint(entry.getValue(), other.warmupTasks().get(entry.getKey()))
        );
    }

    /**
     * Creates a {{@link TasksTuple}} from a
     * {{@link org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberValue}}.
     *
     * @param record The record.
     * @return A {{@link TasksTuple}}.
     */
    public static TasksTuple fromTargetAssignmentRecord(StreamsGroupTargetAssignmentMemberValue record) {
        return new TasksTuple(
            record.activeTasks().stream()
                .collect(Collectors.toMap(
                        StreamsGroupTargetAssignmentMemberValue.TaskIds::subtopologyId,
                        taskId -> new HashSet<>(taskId.partitions())
                    )
                ),
            record.standbyTasks().stream()
                .collect(Collectors.toMap(
                        StreamsGroupTargetAssignmentMemberValue.TaskIds::subtopologyId,
                        taskId -> new HashSet<>(taskId.partitions())
                    )
                ),
            record.warmupTasks().stream()
                .collect(Collectors.toMap(
                        StreamsGroupTargetAssignmentMemberValue.TaskIds::subtopologyId,
                        taskId -> new HashSet<>(taskId.partitions())
                    )
                )
        );
    }

    public String toString() {
        return "(active=" + taskAssignmentToString(activeTasks) +
            ", standby=" + taskAssignmentToString(standbyTasks) +
            ", warmup=" + taskAssignmentToString(warmupTasks) +
            ')';
    }

    public static TasksTuple fromHeartbeatRequest(final List<StreamsGroupHeartbeatRequestData.TaskIds> ownedActiveTasks,
                                                  final List<StreamsGroupHeartbeatRequestData.TaskIds> ownedStandbyTasks,
                                                  final List<StreamsGroupHeartbeatRequestData.TaskIds> ownedWarmupTasks) {
        return new TasksTuple(
            ownedActiveTasks.stream()
                .collect(Collectors.toMap(
                    StreamsGroupHeartbeatRequestData.TaskIds::subtopologyId,
                        taskId -> new HashSet<>(taskId.partitions())
                    )
                ),
            ownedStandbyTasks.stream()
                .collect(Collectors.toMap(
                    StreamsGroupHeartbeatRequestData.TaskIds::subtopologyId,
                        taskId -> new HashSet<>(taskId.partitions())
                    )
                ),
            ownedWarmupTasks.stream()
                .collect(Collectors.toMap(
                    StreamsGroupHeartbeatRequestData.TaskIds::subtopologyId,
                        taskId -> new HashSet<>(taskId.partitions())
                    )
                )
        );
    }

    /**
     * @return The provided assignment as a String.
     *
     * Example:
     * [subtopologyID1-0, subtopologyID1-1, subtopologyID2-0, subtopologyID2-1]
     * 
     * Package-private to allow TasksTupleWithEpochs to use it.
     */
    static String taskAssignmentToString(
        Map<String, Set<Integer>> assignment
    ) {
        StringBuilder builder = new StringBuilder("[");
        
        // Sort subtopology IDs for deterministic output
        String[] subtopologyIds = assignment.keySet().toArray(new String[0]);
        java.util.Arrays.sort(subtopologyIds);
        
        boolean first = true;
        for (String subtopologyId : subtopologyIds) {
            Set<Integer> partitions = assignment.get(subtopologyId);
            
            // Sort partition IDs for deterministic output
            Integer[] partitionIds = partitions.toArray(new Integer[0]);
            java.util.Arrays.sort(partitionIds);
            
            for (Integer partitionId : partitionIds) {
                if (!first) {
                    builder.append(", ");
                }
                builder.append(subtopologyId);
                builder.append("-");
                builder.append(partitionId);
                first = false;
            }
        }
        builder.append("]");
        return builder.toString();
    }
}
