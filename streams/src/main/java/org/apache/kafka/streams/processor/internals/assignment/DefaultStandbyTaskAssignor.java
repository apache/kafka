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

import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.computeTasksToRemainingStandbys;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.createLeastLoadedPrioritySetConstrainedByAssignedTask;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.pollClientAndMaybeAssignAndUpdateRemainingStandbyTasks;

/**
 * Default standby task assignor that distributes standby tasks to the least loaded clients.
 *
 * @see ClientTagAwareStandbyTaskAssignor
 */
class DefaultStandbyTaskAssignor implements StandbyTaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(DefaultStandbyTaskAssignor.class);

    @Override
    public boolean assign(final Map<UUID, ClientState> clients,
                          final Set<TaskId> allTaskIds,
                          final Set<TaskId> statefulTaskIds,
                          final AssignorConfiguration.AssignmentConfigs configs) {
        final int numStandbyReplicas = configs.numStandbyReplicas;
        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(numStandbyReplicas,
                                                                                              statefulTaskIds);

        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = createLeastLoadedPrioritySetConstrainedByAssignedTask(clients);

        standbyTaskClientsByTaskLoad.offerAll(clients.keySet());

        for (final TaskId task : statefulTaskIds) {
            pollClientAndMaybeAssignAndUpdateRemainingStandbyTasks(numStandbyReplicas,
                                                                   clients,
                                                                   tasksToRemainingStandbys,
                                                                   standbyTaskClientsByTaskLoad,
                                                                   task,
                                                                   log);
        }

        // returning false, because standby task assignment will never require a follow-up probing rebalance.
        return false;
    }
}
