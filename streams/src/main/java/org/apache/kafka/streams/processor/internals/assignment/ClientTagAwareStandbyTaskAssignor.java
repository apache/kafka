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
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static java.util.stream.Collectors.toMap;

class ClientTagAwareStandbyTaskAssignor extends StandbyTaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(ClientTagAwareStandbyTaskAssignor.class);

    ClientTagAwareStandbyTaskAssignor(final AssignmentConfigs configs) {
        super(configs);
    }

    @Override
    public void assignStandbyTasks(final Map<TaskId, UUID> statefulTasksWithClients,
                                   final TreeMap<UUID, ClientState> clientStates) {
        final Set<TaskId> statefulTasks = statefulTasksWithClients.keySet();
        final int numStandbyReplicas = configs.numStandbyReplicas;
        final Set<String> rackAwareAssignmentTags = new HashSet<>(configs.rackAwareAssignmentTags);

        final Map<TaskId, Integer> tasksToRemainingStandbys = statefulTasks.stream()
                                                                           .collect(
                                                                               toMap(
                                                                                   task -> task,
                                                                                   t -> numStandbyReplicas
                                                                               )
                                                                           );

        final Map<String, String> tagValueTagKeyMapping = new HashMap<>();
        final Map<String, Set<String>> tagKeyToTagValuesMapping = new HashMap<>();

        final Map<String, Set<UUID>> clientsPerTagValue = new HashMap<>();

        for (final Entry<UUID, ClientState> clientStateEntry : clientStates.entrySet()) {
            final UUID clientId = clientStateEntry.getKey();
            final ClientState clientState = clientStateEntry.getValue();

            clientState.clientTags().forEach((tagKey, tagValue) -> {
                tagValueTagKeyMapping.put(tagValue, tagKey);

                tagKeyToTagValuesMapping.compute(tagKey, (key, currentValue) -> {
                    final Set<String> tagValuesForKey = Optional.ofNullable(currentValue).orElse(new HashSet<>());
                    tagValuesForKey.add(tagValue);
                    return tagValuesForKey;
                });

                clientsPerTagValue.compute(tagValue, (key, currentValue) -> {
                    final Set<UUID> clientIdsForTagValue = Optional.ofNullable(currentValue).orElse(new HashSet<>());
                    clientIdsForTagValue.add(clientId);
                    return clientIdsForTagValue;
                });
            });
        }

        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = new ConstrainedPrioritySet(
            (client, t) -> !clientStates.get(client).hasAssignedTask(t),
            client -> clientStates.get(client).assignedTaskLoad()
        );

        standbyTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        for (final TaskId task : statefulTasks) {
            final List<List<String>> clientTagValues = new ArrayList<>();

            int numRemainingStandbys = tasksToRemainingStandbys.get(task);

            final ClientState activeTaskClient = clientStates.get(statefulTasksWithClients.get(task));

            clientTagValues.add(new ArrayList<>(activeTaskClient.clientTags().values()));

            while (numRemainingStandbys > 0) {
                final Set<UUID> toBeFilteredClients = findClientsViolatingRackAwareness(rackAwareAssignmentTags,
                                                                                        clientStates,
                                                                                        tagKeyToTagValuesMapping,
                                                                                        tagValueTagKeyMapping,
                                                                                        clientsPerTagValue,
                                                                                        clientTagValues);

                final UUID polledClient = standbyTaskClientsByTaskLoad.poll(task, uuid -> !toBeFilteredClients.contains(uuid));

                if (polledClient == null) {
                    break;
                }

                final ClientState standbyTaskClient = clientStates.get(polledClient);

                standbyTaskClient.assignStandby(task);

                clientTagValues.add(new ArrayList<>(standbyTaskClient.clientTags().values()));

                numRemainingStandbys--;
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

    @Override
    public boolean isValidTaskMovement(final TaskMovementAttempt taskMovementAttempt) {
        final Map<String, String> sourceClientTags = taskMovementAttempt.sourceClient().clientTags();
        final Map<String, String> destinationClientTags = taskMovementAttempt.destinationClient().clientTags();

        for (final Entry<String, String> sourceClientTagEntry : sourceClientTags.entrySet()) {
            if (!sourceClientTagEntry.getValue().equals(destinationClientTags.get(sourceClientTagEntry.getKey()))) {
                return false;
            }
        }

        log.info("Movement of task [{}] is valid", taskMovementAttempt.taskId());

        return true;
    }

    private static Set<UUID> findClientsViolatingRackAwareness(final Set<String> rackAwareAssignmentTags,
                                                               final TreeMap<UUID, ClientState> clientStates,
                                                               final Map<String, Set<String>> allTags,
                                                               final Map<String, String> tagValueTagKeyMapping,
                                                               final Map<String, Set<UUID>> clientsPerTagValue,
                                                               final List<List<String>> clientTagValues) {
        final Set<UUID> allClients = clientStates.keySet();
        final Set<UUID> filteredClients = new HashSet<>();

        for (final List<String> tagValues : clientTagValues) {
            for (final String tagValue : tagValues) {
                final String tagKey = tagValueTagKeyMapping.get(tagValue);

                if (!rackAwareAssignmentTags.contains(tagKey)) {
                    continue;
                }

                final Set<String> allTagValues = allTags.get(tagKey);
                final Set<UUID> clientsToBeFiltered = clientsPerTagValue.get(tagValue);

                if (allTagValues.size() <= clientTagValues.size()) {
                    continue;
                }

                if (willRemainingClientsHaveEnoughCapacity(allClients, filteredClients, clientsToBeFiltered, clientStates)) {
                    filteredClients.addAll(clientsToBeFiltered);
                }
            }
        }

        return filteredClients;
    }

    private static boolean willRemainingClientsHaveEnoughCapacity(final Set<UUID> allClients,
                                                                  final Set<UUID> filteredClients,
                                                                  final Set<UUID> clientsToBeFiltered,
                                                                  final TreeMap<UUID, ClientState> clientStates) {
        final Collection<UUID> validClients = allClientsNotIn(
            allClientsNotIn(
                allClients,
                filteredClients
            ),
            clientsToBeFiltered
        );

        return validClients.stream().anyMatch(it -> !clientStates.get(it).reachedCapacity());
    }

    private static Set<UUID> allClientsNotIn(final Set<UUID> allClients, final Set<UUID> toBeFilteredClients) {
        final Set<UUID> clients = new HashSet<>(allClients);
        clients.removeAll(toBeFilteredClients);
        return clients;
    }
}
