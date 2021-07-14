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
import java.util.SortedMap;
import java.util.UUID;

import static java.util.stream.Collectors.toMap;

class DefaultStandbyTaskAssignor extends StandbyTaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(DefaultStandbyTaskAssignor.class);

    public DefaultStandbyTaskAssignor(final AssignorConfiguration.AssignmentConfigs configs) {
        super(configs);
    }

    @Override
    public void assignStandbyTasks(final Map<TaskId, UUID> statefulTasksWithClients,
                                   final SortedMap<UUID, ClientState> clientStates) {
        final int numStandbyReplicas = configs.numStandbyReplicas;
        final Set<TaskId> statefulTasks = statefulTasksWithClients.keySet();
        final Map<TaskId, Integer> tasksToRemainingStandbys =
            statefulTasks.stream().collect(toMap(task -> task, t -> numStandbyReplicas));

        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = new ConstrainedPrioritySet(
            (client, task) -> !clientStates.get(client).hasAssignedTask(task),
            client -> clientStates.get(client).assignedTaskLoad()
        );

        standbyTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        for (final TaskId task : statefulTasks) {
            int numRemainingStandbys = tasksToRemainingStandbys.get(task);
            while (numRemainingStandbys > 0) {
                final UUID client = standbyTaskClientsByTaskLoad.poll(task);
                if (client == null) {
                    break;
                }
                clientStates.get(client).assignStandby(task);
                numRemainingStandbys--;
                standbyTaskClientsByTaskLoad.offer(client);
            }

            if (numRemainingStandbys > 0) {
                log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                         "There is not enough available capacity. You should " +
                         "increase the number of application instances " +
                         "to maintain the requested number of standby replicas.",
                         numRemainingStandbys, numStandbyReplicas, task);
            }
        }
    }
}
