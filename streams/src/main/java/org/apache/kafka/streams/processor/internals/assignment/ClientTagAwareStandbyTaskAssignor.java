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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * Distributes standby tasks over different tag dimensions.
 * Only tags specified via {@link AssignmentConfigs#rackAwareAssignmentTags} are taken into account.
 * Standby task distribution is on a best-effort basis. For example, if there are not enough clients available
 * on different tag dimensions compared to an active and corresponding standby task,
 * in that case, the algorithm will fall back to distributing tasks on least-loaded clients.
 */
class ClientTagAwareStandbyTaskAssignor extends StandbyTaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(ClientTagAwareStandbyTaskAssignor.class);

    ClientTagAwareStandbyTaskAssignor(final AssignmentConfigs configs) {
        super(configs);
    }

    @Override
    public void assignStandbyTasks(final Map<TaskId, UUID> statefulTasksWithClients,
                                   final TreeMap<UUID, ClientState> clientStates) {
        final int numStandbyReplicas = configs.numStandbyReplicas;
        final Set<String> rackAwareAssignmentTags = new HashSet<>(configs.rackAwareAssignmentTags);

        final StandbyTaskDistributor standbyTaskDistributor = new StandbyTaskDistributor(
            numStandbyReplicas,
            clientStates,
            rackAwareAssignmentTags,
            statefulTasksWithClients
        );

        statefulTasksWithClients.forEach(standbyTaskDistributor::assignStandbyTasksForActiveTask);
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

        return true;
    }

    private static final class StandbyTaskDistributor {
        private final int numStandbyReplicas;
        private final TreeMap<UUID, ClientState> clientStates;
        private final Set<String> rackAwareAssignmentTags;
        private final Map<String, Set<String>> tagKeyToTagValuesMapping;
        private final Map<String, Set<UUID>> clientsPerTagValue;
        private final ConstrainedPrioritySet standbyTaskClientsByTaskLoad;

        private final Set<UUID> usedClients = new HashSet<>();
        private final Map<TaskId, Integer> tasksToRemainingStandbys;

        StandbyTaskDistributor(final int numStandbyReplicas,
                               final TreeMap<UUID, ClientState> clientStates,
                               final Set<String> rackAwareAssignmentTags,
                               final Map<TaskId, UUID> statefulTasksWithClients) {
            tagKeyToTagValuesMapping = new HashMap<>();
            clientsPerTagValue = new HashMap<>();
            standbyTaskClientsByTaskLoad = new ConstrainedPrioritySet(
                (client, t) -> !clientStates.get(client).hasAssignedTask(t),
                client -> clientStates.get(client).assignedTaskLoad()
            );
            tasksToRemainingStandbys = statefulTasksWithClients.keySet()
                                                               .stream()
                                                               .collect(
                                                                   toMap(
                                                                       Function.identity(),
                                                                       t -> numStandbyReplicas
                                                                   )
                                                               );

            this.clientStates = clientStates;
            this.numStandbyReplicas = numStandbyReplicas;
            this.rackAwareAssignmentTags = rackAwareAssignmentTags;
            fillData(clientStates);
        }

        void assignStandbyTasksForActiveTask(final TaskId activeTaskId, final UUID activeTaskClient) {
            standbyTaskClientsByTaskLoad.offerAll(clientStates.keySet());

            int numRemainingStandbys = tasksToRemainingStandbys.get(activeTaskId);

            recordActiveTaskClient(activeTaskClient);

            while (numRemainingStandbys > 0) {
                final Set<UUID> clientsOnAlreadyUsedTagDimensions = findClientsOnUsedTagDimensions();

                final UUID polledClient = standbyTaskClientsByTaskLoad.poll(
                    activeTaskId, uuid -> !clientsOnAlreadyUsedTagDimensions.contains(uuid)
                );

                if (polledClient == null) {
                    break;
                }

                final ClientState standbyTaskClient = clientStates.get(polledClient);

                standbyTaskClient.assignStandby(activeTaskId);

                usedClients.add(polledClient);

                standbyTaskClientsByTaskLoad.offerAll(clientsOnAlreadyUsedTagDimensions);
                numRemainingStandbys--;
            }

            if (numRemainingStandbys > 0) {
                log.warn("Unable to assign {} of {} standby tasks for task [{}] with client tags [{}]. " +
                         "There is not enough available capacity. You should " +
                         "increase the number of application instances " +
                         "on different client tag dimensions " +
                         "to maintain the requested number of standby replicas. " +
                         "Rack awareness is configured with [{}] tags.",
                         numRemainingStandbys, numStandbyReplicas, activeTaskId,
                         clientStates.get(activeTaskClient).clientTags(), rackAwareAssignmentTags);
            }
        }

        private void recordActiveTaskClient(final UUID activeTaskClientId) {
            usedClients.clear();
            usedClients.add(activeTaskClientId);
        }

        private void fillData(final TreeMap<UUID, ClientState> clientStates) {
            for (final Entry<UUID, ClientState> clientStateEntry : clientStates.entrySet()) {
                final UUID clientId = clientStateEntry.getKey();
                final ClientState clientState = clientStateEntry.getValue();

                clientState.clientTags().forEach((tagKey, tagValue) -> {
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
        }

        private Set<UUID> findClientsOnUsedTagDimensions() {
            final Set<UUID> filteredClients = new HashSet<>();

            for (final UUID usedClientId : usedClients) {
                final Map<String, String> usedClientTags = clientStates.get(usedClientId).clientTags();

                for (final Entry<String, String> usedClientTagEntry : usedClientTags.entrySet()) {
                    final String tagKey = usedClientTagEntry.getKey();

                    if (!rackAwareAssignmentTags.contains(tagKey)) {
                        continue;
                    }

                    final Set<String> allTagValues = tagKeyToTagValuesMapping.get(tagKey);
                    final String tagValue = usedClientTagEntry.getValue();

                    // If we have used more clients than all the tag's unique values,
                    // we can't filter out clients located on that tag.
                    if (allTagValues.size() <= usedClients.size()) {
                        continue;
                    }

                    final Set<UUID> clientsOnUsedTagValue = clientsPerTagValue.get(tagValue);
                    filteredClients.addAll(clientsOnUsedTagValue);
                }
            }

            return filteredClients;
        }
    }
}