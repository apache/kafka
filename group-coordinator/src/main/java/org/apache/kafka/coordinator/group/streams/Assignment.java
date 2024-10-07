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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An immutable assignment for a member.
 */
public class Assignment {

    public static final Assignment EMPTY = new Assignment(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    private final Map<String, Set<Integer>> activeTasks;
    private final Map<String, Set<Integer>> standbyTasks;
    private final Map<String, Set<Integer>> warmupTasks;

    public Assignment(final Map<String, Set<Integer>> activeTasks,
                      final Map<String, Set<Integer>> standbyTasks,
                      final Map<String, Set<Integer>> warmupTasks) {
        this.activeTasks = Collections.unmodifiableMap(Objects.requireNonNull(activeTasks));
        this.standbyTasks = Collections.unmodifiableMap(Objects.requireNonNull(standbyTasks));
        this.warmupTasks = Collections.unmodifiableMap(Objects.requireNonNull(warmupTasks));
    }

    /**
     * @return The assigned active tasks.
     */
    public Map<String, Set<Integer>> activeTasks() {
        return activeTasks;
    }

    /**
     * @return The assigned standby tasks.
     */
    public Map<String, Set<Integer>> standbyTasks() {
        return standbyTasks;
    }

    /**
     * @return The assigned warm-up tasks.
     */
    public Map<String, Set<Integer>> warmupTasks() {
        return warmupTasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Assignment that = (Assignment) o;
        return Objects.equals(activeTasks, that.activeTasks)
            && Objects.equals(standbyTasks, that.standbyTasks)
            && Objects.equals(warmupTasks, that.warmupTasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(activeTasks, standbyTasks, warmupTasks);
    }

    @Override
    public String toString() {
        return "Assignment(active tasks=" + activeTasks +
            ", standby tasks=" + standbyTasks +
            ", warm-up tasks=" + warmupTasks + ')';
    }

    /**
     * Creates a {{@link org.apache.kafka.coordinator.group.streams.Assignment}} from a
     * {{@link org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberValue}}.
     *
     * @param record The record.
     * @return A {{@link org.apache.kafka.coordinator.group.streams.Assignment}}.
     */
    public static Assignment fromRecord(
        StreamsGroupTargetAssignmentMemberValue record
    ) {
        return new Assignment(
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
