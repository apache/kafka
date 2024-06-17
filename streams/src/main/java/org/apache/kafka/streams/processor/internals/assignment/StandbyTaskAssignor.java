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
package org.apache.kafka.streams.processor.internals.assignment;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;

interface StandbyTaskAssignor extends TaskAssignor {
    default boolean isAllowedTaskMovement(final ClientState source, final ClientState destination) {
        return true;
    }

    /**
     * If a specific task can be moved from source to destination
     * @param source Source client
     * @param destination Destination client
     * @param sourceTask Task to move
     * @param clientStateMap All client metadata
     * @return True if task can be moved, false otherwise
     */
    default boolean isAllowedTaskMovement(final ClientState source,
                                          final ClientState destination,
                                          final TaskId sourceTask,
                                          final Map<UUID, ClientState> clientStateMap) {
        return true;
    }

    default boolean assign(final Map<UUID, ClientState> clients,
                           final Set<TaskId> allTaskIds,
                           final Set<TaskId> statefulTaskIds,
                           final RackAwareTaskAssignor rackAwareTaskAssignor,
                           final AssignmentConfigs configs) {
        return assign(clients, allTaskIds, statefulTaskIds, configs);
    }

    boolean assign(final Map<UUID, ClientState> clients,
                   final Set<TaskId> allTaskIds,
                   final Set<TaskId> statefulTaskIds,
                   final AssignorConfiguration.AssignmentConfigs configs);
}