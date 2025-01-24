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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An immutable tuple containing active, standby and warm-up tasks.
 *
 * @param activeTasks           Active tasks.
 *                              The key of the map is the subtopology ID and the value is the set of partition IDs.
 * @param standbyTasks          Standby tasks.
 *                              The key of the map is the subtopology ID and the value is the set of partition IDs.
 * @param warmupTasks           Warm-up tasks.
 *                              The key of the map is the subtopology ID and the value is the set of partition IDs.
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
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    /**
     * @return true if all collections in the tuple are empty.
     */
    public boolean isEmpty() {
        return activeTasks.isEmpty() && standbyTasks.isEmpty() && warmupTasks.isEmpty();
    }

    /**
     * Merges this task tuple with another task tuple.
     *
     * @param other The other task tuple.
     * @return A new task tuple, containing all active tasks, standby tasks and warm-up tasks from both tuples.
     */
    public TasksTuple merge(TasksTuple other) {
        Map<String, Set<Integer>> mergedActiveTasks = merge(activeTasks, other.activeTasks);
        Map<String, Set<Integer>> mergedStandbyTasks = merge(standbyTasks, other.standbyTasks);
        Map<String, Set<Integer>> mergedWarmupTasks = merge(warmupTasks, other.warmupTasks);
        return new TasksTuple(mergedActiveTasks, mergedStandbyTasks, mergedWarmupTasks);
    }

    private static Map<String, Set<Integer>> merge(final Map<String, Set<Integer>> tasks1, final Map<String, Set<Integer>> tasks2) {
        HashMap<String, Set<Integer>> result = new HashMap<>();
        tasks1.forEach((subtopologyId, tasks) ->
            result.put(subtopologyId, new HashSet<>(tasks)));
        tasks2.forEach((subtopologyId, tasks) -> result
            .computeIfAbsent(subtopologyId, __ -> new HashSet<>())
            .addAll(tasks));
        return result;
    }

    /**
     * Checks if this task tuple contains any of the tasks in another task tuple.
     *
     * @param other The other task tuple.
     * @return true if there is at least one active, standby or warm-up task that is present in both tuples.
     */
    public boolean containsAny(TasksTuple other) {
        return activeTasks.entrySet().stream().anyMatch(
            entry -> other.activeTasks.containsKey(entry.getKey()) && !Collections.disjoint(entry.getValue(), other.activeTasks.get(entry.getKey()))
        ) || standbyTasks.entrySet().stream().anyMatch(
            entry -> other.standbyTasks.containsKey(entry.getKey()) && !Collections.disjoint(entry.getValue(), other.standbyTasks.get(entry.getKey()))
        ) || warmupTasks.entrySet().stream().anyMatch(
            entry -> other.warmupTasks.containsKey(entry.getKey()) && !Collections.disjoint(entry.getValue(), other.warmupTasks.get(entry.getKey()))
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
}
