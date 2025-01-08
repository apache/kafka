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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The assignment specification for a Streams group member.
 *
 * @param instanceId   The instance ID if provided.
 * @param rackId       The rack ID if provided.
 * @param activeTasks  Reconciled active tasks
 * @param standbyTasks Reconciled standby tasks
 * @param warmupTasks  Reconciled warm-up tasks
 * @param processId    The process ID.
 * @param clientTags   The client tags for a rack-aware assignment.
 * @param taskOffsets  The last received cumulative task offsets of assigned tasks or dormant tasks.
 */
public record AssignmentMemberSpec(Optional<String> instanceId,
                                   Optional<String> rackId,
                                   Map<String, Set<Integer>> activeTasks,
                                   Map<String, Set<Integer>> standbyTasks,
                                   Map<String, Set<Integer>> warmupTasks,
                                   String processId,
                                   Map<String, String> clientTags,
                                   Map<TaskId, Long> taskOffsets,
                                   Map<TaskId, Long> taskEndOffsets
) {

    public AssignmentMemberSpec {
        Objects.requireNonNull(instanceId);
        Objects.requireNonNull(rackId);
        activeTasks = Collections.unmodifiableMap(Objects.requireNonNull(activeTasks));
        standbyTasks = Collections.unmodifiableMap(Objects.requireNonNull(standbyTasks));
        warmupTasks = Collections.unmodifiableMap(Objects.requireNonNull(warmupTasks));
        Objects.requireNonNull(processId);
        clientTags = Collections.unmodifiableMap(Objects.requireNonNull(clientTags));
        taskOffsets = Collections.unmodifiableMap(Objects.requireNonNull(taskOffsets));
        taskEndOffsets = Collections.unmodifiableMap(Objects.requireNonNull(taskEndOffsets));
    }

}
