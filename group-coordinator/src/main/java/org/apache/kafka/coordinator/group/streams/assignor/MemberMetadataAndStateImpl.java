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

import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignmentMetadata;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignmentState;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of both the {@link MemberAssignmentMetadata} and the {@link MemberAssignmentState}
 * interfaces for a streams group member.
 *
 * @param instanceId    The instance ID if provided.
 * @param rackId        The rack ID if provided.
 * @param processId     The process ID.
 * @param clientTags    The client tags for a rack-aware assignment.
 * @param activeTasks   Current active tasks.
 * @param standbyTasks  Current standby tasks.
 * @param warmupTasks   Current warm-up tasks.
 * @param taskOffsets   The last received cumulative task offsets of assigned tasks or dormant tasks.
 * @param taskEndOffsets The last received cumulative task end offsets of assigned tasks or dormant tasks.
 */
public record MemberMetadataAndStateImpl(
    Optional<String> instanceId,
    Optional<String> rackId,
    String processId,
    Map<String, String> clientTags,
    Map<String, Set<Integer>> activeTasks,
    Map<String, Set<Integer>> standbyTasks,
    Map<String, Set<Integer>> warmupTasks,
    Map<String, Map<Integer, Long>> taskOffsets,
    Map<String, Map<Integer, Long>> taskEndOffsets
) implements MemberAssignmentMetadata, MemberAssignmentState {

    public MemberMetadataAndStateImpl {
        Objects.requireNonNull(instanceId);
        Objects.requireNonNull(rackId);
        Objects.requireNonNull(processId);
        clientTags = Collections.unmodifiableMap(Objects.requireNonNull(clientTags));
        // These collections belong to the coordinator (the member's current assignment and the offsets reported in
        // the last heartbeat) and are only unmodifiable at the outer level there, so wrap the nested ones as well.
        activeTasks = unmodifiableTasks(activeTasks);
        standbyTasks = unmodifiableTasks(standbyTasks);
        warmupTasks = unmodifiableTasks(warmupTasks);
        taskOffsets = unmodifiableOffsets(taskOffsets);
        taskEndOffsets = unmodifiableOffsets(taskEndOffsets);
    }

    private static Map<String, Set<Integer>> unmodifiableTasks(Map<String, Set<Integer>> tasks) {
        return Objects.requireNonNull(tasks).entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> Collections.unmodifiableSet(entry.getValue())));
    }

    private static Map<String, Map<Integer, Long>> unmodifiableOffsets(Map<String, Map<Integer, Long>> offsets) {
        return Objects.requireNonNull(offsets).entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> Collections.unmodifiableMap(entry.getValue())));
    }

}
