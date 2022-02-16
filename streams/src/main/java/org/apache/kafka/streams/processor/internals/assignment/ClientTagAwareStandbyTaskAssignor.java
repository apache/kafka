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
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.computeTasksToRemainingStandbys;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.pollClientAndMaybeAssignRemainingStandbyTasks;

/**
 * Distributes standby tasks over different tag dimensions. Standby task distribution is on a best-effort basis.
 * If rack aware standby task assignment is not possible, implementation fall backs to distributing standby tasks on least-loaded clients.
 *
 * @see DefaultStandbyTaskAssignor
 */
class ClientTagAwareStandbyTaskAssignor implements StandbyTaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(ClientTagAwareStandbyTaskAssignor.class);

    /**
     * The algorithm distributes standby tasks for the {@param statefulTaskIds} over different tag dimensions.
     * For each stateful task, the number of standby tasks will be assigned based on configured {@link AssignmentConfigs#numStandbyReplicas}.
     * Rack aware standby tasks distribution only takes into account tags specified via {@link AssignmentConfigs#rackAwareAssignmentTags}.
     * Ideally, all standby tasks for any given stateful task will be located on different tag dimensions to have the best possible distribution.
     * However, if the ideal distribution is impossible, the algorithm will fall back to the least-loaded clients without considering rack awareness constraints into consideration.
     * The least-loaded clients are determined based on the total number of tasks (active and standby tasks) assigned to the client.
     */
    @Override
    public boolean assign(final Map<UUID, ClientState> clients,
                          final Set<TaskId> allTaskIds,
                          final Set<TaskId> statefulTaskIds,
                          final AssignorConfiguration.AssignmentConfigs configs) {
        final int numStandbyReplicas = configs.numStandbyReplicas;
        final Set<String> rackAwareAssignmentTags = new HashSet<>(configs.rackAwareAssignmentTags);

        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(
            numStandbyReplicas,
            allTaskIds
        );

        final Map<String, Set<String>> tagKeyToValues = new HashMap<>();
        final Map<TagEntry, Set<UUID>> tagEntryToClients = new HashMap<>();

        fillClientsTagStatistics(clients, tagEntryToClients, tagKeyToValues);

        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = new ConstrainedPrioritySet(
            (client, t) -> !clients.get(client).hasAssignedTask(t),
            client -> clients.get(client).assignedTaskLoad()
        );

        final Map<TaskId, UUID> pendingStandbyTasksToClientId = new HashMap<>();

        for (final TaskId statefulTaskId : statefulTaskIds) {
            for (final Map.Entry<UUID, ClientState> entry : clients.entrySet()) {
                final UUID clientId = entry.getKey();
                final ClientState clientState = entry.getValue();

                if (clientState.activeTasks().contains(statefulTaskId)) {
                    final int numberOfRemainingStandbys = assignStandbyTasksToClientsWithDifferentTags(
                        standbyTaskClientsByTaskLoad,
                        statefulTaskId,
                        clientId,
                        rackAwareAssignmentTags,
                        clients,
                        tasksToRemainingStandbys,
                        tagKeyToValues,
                        tagEntryToClients
                    );

                    if (numberOfRemainingStandbys > 0) {
                        pendingStandbyTasksToClientId.put(statefulTaskId, clientId);
                    } else {
                        tasksToRemainingStandbys.remove(statefulTaskId);
                    }
                }
            }
        }

        if (!tasksToRemainingStandbys.isEmpty()) {
            log.debug("Rack aware standby task assignment was not able to assign all standby tasks. " +
                      "tasksToRemainingStandbys=[{}], pendingStandbyTasksToClientId=[{}]. " +
                      "Will distribute the remaining standby tasks to least loaded clients.",
                      tasksToRemainingStandbys, pendingStandbyTasksToClientId);

            assignPendingStandbyTasksToLeastLoadedClients(clients,
                                                          numStandbyReplicas,
                                                          rackAwareAssignmentTags,
                                                          standbyTaskClientsByTaskLoad,
                                                          tasksToRemainingStandbys,
                                                          pendingStandbyTasksToClientId);
        }

        // returning false, because standby task assignment will never require a follow-up probing rebalance.
        return false;
    }

    private static void assignPendingStandbyTasksToLeastLoadedClients(final Map<UUID, ClientState> clients,
                                                                      final int numStandbyReplicas,
                                                                      final Set<String> rackAwareAssignmentTags,
                                                                      final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                                      final Map<TaskId, Integer> pendingStandbyTaskToNumberRemainingStandbys,
                                                                      final Map<TaskId, UUID> pendingStandbyTaskToClientId) {
        // We need to re offer all the clients to find the least loaded ones
        standbyTaskClientsByTaskLoad.offerAll(clients.keySet());

        for (final Entry<TaskId, Integer> pendingStandbyTaskAssignmentEntry : pendingStandbyTaskToNumberRemainingStandbys.entrySet()) {
            final TaskId activeTaskId = pendingStandbyTaskAssignmentEntry.getKey();
            final UUID clientId = pendingStandbyTaskToClientId.get(activeTaskId);

            final int numberOfRemainingStandbys = pollClientAndMaybeAssignRemainingStandbyTasks(
                clients,
                pendingStandbyTaskToNumberRemainingStandbys,
                standbyTaskClientsByTaskLoad,
                activeTaskId
            );

            if (numberOfRemainingStandbys > 0) {
                log.warn("Unable to assign {} of {} standby tasks for task [{}] with client tags [{}]. " +
                         "There is not enough available capacity. You should " +
                         "increase the number of application instances " +
                         "on different client tag dimensions " +
                         "to maintain the requested number of standby replicas. " +
                         "Rack awareness is configured with [{}] tags.",
                         numberOfRemainingStandbys, numStandbyReplicas, activeTaskId,
                         clients.get(clientId).clientTags(), rackAwareAssignmentTags);
            }
        }
    }

    @Override
    public boolean isAllowedTaskMovement(final ClientState source, final ClientState destination) {
        final Map<String, String> sourceClientTags = source.clientTags();
        final Map<String, String> destinationClientTags = destination.clientTags();

        for (final Entry<String, String> sourceClientTagEntry : sourceClientTags.entrySet()) {
            if (!sourceClientTagEntry.getValue().equals(destinationClientTags.get(sourceClientTagEntry.getKey()))) {
                return false;
            }
        }

        return true;
    }

    private static void fillClientsTagStatistics(final Map<UUID, ClientState> clientStates,
                                                 final Map<TagEntry, Set<UUID>> tagEntryToClients,
                                                 final Map<String, Set<String>> tagKeyToValues) {
        for (final Entry<UUID, ClientState> clientStateEntry : clientStates.entrySet()) {
            final UUID clientId = clientStateEntry.getKey();
            final ClientState clientState = clientStateEntry.getValue();

            clientState.clientTags().forEach((tagKey, tagValue) -> {
                tagKeyToValues.computeIfAbsent(tagKey, ignored -> new HashSet<>()).add(tagValue);
                tagEntryToClients.computeIfAbsent(new TagEntry(tagKey, tagValue), ignored -> new HashSet<>()).add(clientId);
            });
        }
    }

    private static int assignStandbyTasksToClientsWithDifferentTags(final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                                    final TaskId activeTaskId,
                                                                    final UUID activeTaskClient,
                                                                    final Set<String> rackAwareAssignmentTags,
                                                                    final Map<UUID, ClientState> clientStates,
                                                                    final Map<TaskId, Integer> tasksToRemainingStandbys,
                                                                    final Map<String, Set<String>> tagKeyToValues,
                                                                    final Map<TagEntry, Set<UUID>> tagEntryToClients) {
        standbyTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        // We set numberOfUsedClients as 1 because client where active task is located has to be considered as used.
        int numberOfUsedClients = 1;
        int numRemainingStandbys = tasksToRemainingStandbys.get(activeTaskId);

        final Set<UUID> clientsOnAlreadyUsedTagDimensions = new HashSet<>(
            findClientsOnUsedClientTagDimensions(
                activeTaskClient,
                numberOfUsedClients,
                rackAwareAssignmentTags,
                clientStates,
                tagEntryToClients,
                tagKeyToValues
            )
        );

        while (numRemainingStandbys > 0) {
            final UUID clientOnUnusedTagDimensions = standbyTaskClientsByTaskLoad.poll(
                activeTaskId, uuid -> !clientsOnAlreadyUsedTagDimensions.contains(uuid)
            );

            if (clientOnUnusedTagDimensions == null) {
                break;
            }

            clientStates.get(clientOnUnusedTagDimensions).assignStandby(activeTaskId);

            numberOfUsedClients++;

            clientsOnAlreadyUsedTagDimensions.addAll(
                findClientsOnUsedClientTagDimensions(
                    clientOnUnusedTagDimensions,
                    numberOfUsedClients,
                    rackAwareAssignmentTags,
                    clientStates,
                    tagEntryToClients,
                    tagKeyToValues
                )
            );

            numRemainingStandbys--;
        }

        return numRemainingStandbys;
    }

    private static Set<UUID> findClientsOnUsedClientTagDimensions(final UUID usedClient,
                                                                  final int numberOfUsedClients,
                                                                  final Set<String> rackAwareAssignmentTags,
                                                                  final Map<UUID, ClientState> clientStates,
                                                                  final Map<TagEntry, Set<UUID>> tagEntryToClients,
                                                                  final Map<String, Set<String>> tagKeyToValues) {
        final Set<UUID> filteredClients = new HashSet<>();

        final Map<String, String> usedClientTags = clientStates.get(usedClient).clientTags();

        for (final Entry<String, String> usedClientTagEntry : usedClientTags.entrySet()) {
            final String tagKey = usedClientTagEntry.getKey();

            if (!rackAwareAssignmentTags.contains(tagKey)) {
                log.warn("Client tag with key [{}] will be ignored when computing rack aware standby " +
                         "task assignment because it is not part of the configured rack awareness [{}].",
                         tagKey, rackAwareAssignmentTags);
                continue;
            }

            final Set<String> allTagValues = tagKeyToValues.get(tagKey);
            final String tagValue = usedClientTagEntry.getValue();

            // If we have used more clients than all the tag's unique values,
            // we can't filter out clients located on that tag.
            if (allTagValues.size() <= numberOfUsedClients) {
                continue;
            }

            final Set<UUID> clientsOnUsedTagValue = tagEntryToClients.get(new TagEntry(tagKey, tagValue));
            filteredClients.addAll(clientsOnUsedTagValue);
        }

        return filteredClients;
    }

    private static final class TagEntry {
        private final String tagKey;
        private final String tagValue;

        TagEntry(final String tagKey, final String tagValue) {
            this.tagKey = tagKey;
            this.tagValue = tagValue;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final TagEntry that = (TagEntry) o;
            return Objects.equals(tagKey, that.tagKey) && Objects.equals(tagValue, that.tagValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tagKey, tagValue);
        }
    }
}