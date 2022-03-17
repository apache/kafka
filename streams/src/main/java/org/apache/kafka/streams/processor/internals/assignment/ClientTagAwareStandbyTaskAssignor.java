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
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.createLeastLoadedPrioritySetConstrainedByAssignedTask;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.pollClientAndMaybeAssignAndUpdateRemainingStandbyTasks;

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
     * However, if the ideal (or partially ideal) distribution is impossible, the algorithm will fall back to the least-loaded clients without taking rack awareness constraints into consideration.
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
            statefulTaskIds
        );

        final Map<String, Set<String>> tagKeyToValues = new HashMap<>();
        final Map<TagEntry, Set<UUID>> tagEntryToClients = new HashMap<>();

        fillClientsTagStatistics(clients, tagEntryToClients, tagKeyToValues);

        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = createLeastLoadedPrioritySetConstrainedByAssignedTask(clients);

        final Map<TaskId, UUID> pendingStandbyTasksToClientId = new HashMap<>();

        for (final TaskId statefulTaskId : statefulTaskIds) {
            for (final Map.Entry<UUID, ClientState> entry : clients.entrySet()) {
                final UUID clientId = entry.getKey();
                final ClientState clientState = entry.getValue();

                if (clientState.activeTasks().contains(statefulTaskId)) {
                    assignStandbyTasksToClientsWithDifferentTags(
                        numStandbyReplicas,
                        standbyTaskClientsByTaskLoad,
                        statefulTaskId,
                        clientId,
                        rackAwareAssignmentTags,
                        clients,
                        tasksToRemainingStandbys,
                        tagKeyToValues,
                        tagEntryToClients,
                        pendingStandbyTasksToClientId
                    );
                }
            }
        }

        if (!tasksToRemainingStandbys.isEmpty()) {
            assignPendingStandbyTasksToLeastLoadedClients(clients,
                                                          numStandbyReplicas,
                                                          standbyTaskClientsByTaskLoad,
                                                          tasksToRemainingStandbys);
        }

        // returning false, because standby task assignment will never require a follow-up probing rebalance.
        return false;
    }

    private static void assignPendingStandbyTasksToLeastLoadedClients(final Map<UUID, ClientState> clients,
                                                                      final int numStandbyReplicas,
                                                                      final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                                      final Map<TaskId, Integer> pendingStandbyTaskToNumberRemainingStandbys) {
        // We need to re offer all the clients to find the least loaded ones
        standbyTaskClientsByTaskLoad.offerAll(clients.keySet());

        for (final Entry<TaskId, Integer> pendingStandbyTaskAssignmentEntry : pendingStandbyTaskToNumberRemainingStandbys.entrySet()) {
            final TaskId activeTaskId = pendingStandbyTaskAssignmentEntry.getKey();

            pollClientAndMaybeAssignAndUpdateRemainingStandbyTasks(
                numStandbyReplicas,
                clients,
                pendingStandbyTaskToNumberRemainingStandbys,
                standbyTaskClientsByTaskLoad,
                activeTaskId,
                log
            );
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

    // Visible for testing
    static void fillClientsTagStatistics(final Map<UUID, ClientState> clientStates,
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

    // Visible for testing
    static void assignStandbyTasksToClientsWithDifferentTags(final int numberOfStandbyClients,
                                                             final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                             final TaskId activeTaskId,
                                                             final UUID activeTaskClient,
                                                             final Set<String> rackAwareAssignmentTags,
                                                             final Map<UUID, ClientState> clientStates,
                                                             final Map<TaskId, Integer> tasksToRemainingStandbys,
                                                             final Map<String, Set<String>> tagKeyToValues,
                                                             final Map<TagEntry, Set<UUID>> tagEntryToClients,
                                                             final Map<TaskId, UUID> pendingStandbyTasksToClientId) {
        standbyTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        // We set countOfUsedClients as 1 because client where active task is located has to be considered as used.
        int countOfUsedClients = 1;
        int numRemainingStandbys = tasksToRemainingStandbys.get(activeTaskId);

        final Map<TagEntry, Set<UUID>> tagEntryToUsedClients = new HashMap<>();

        UUID lastUsedClient = activeTaskClient;
        do {
            updateClientsOnAlreadyUsedTagEntries(
                lastUsedClient,
                countOfUsedClients,
                rackAwareAssignmentTags,
                clientStates,
                tagEntryToClients,
                tagKeyToValues,
                tagEntryToUsedClients
            );

            final UUID clientOnUnusedTagDimensions = standbyTaskClientsByTaskLoad.poll(
                activeTaskId, uuid -> !isClientUsedOnAnyOfTheTagEntries(uuid, tagEntryToUsedClients)
            );

            if (clientOnUnusedTagDimensions == null) {
                break;
            }

            final ClientState clientStateOnUsedTagDimensions = clientStates.get(clientOnUnusedTagDimensions);
            countOfUsedClients++;
            numRemainingStandbys--;

            log.debug("Assigning {} out of {} standby tasks for an active task [{}] with client tags {}. " +
                      "Standby task client tags are {}.",
                      numberOfStandbyClients - numRemainingStandbys, numberOfStandbyClients, activeTaskId,
                      clientStates.get(activeTaskClient).clientTags(), clientStateOnUsedTagDimensions.clientTags());

            clientStateOnUsedTagDimensions.assignStandby(activeTaskId);
            lastUsedClient = clientOnUnusedTagDimensions;
        } while (numRemainingStandbys > 0);

        if (numRemainingStandbys > 0) {
            pendingStandbyTasksToClientId.put(activeTaskId, activeTaskClient);
            tasksToRemainingStandbys.put(activeTaskId, numRemainingStandbys);
            log.warn("Rack aware standby task assignment was not able to assign {} of {} standby tasks for the " +
                     "active task [{}] with the rack aware assignment tags {}. " +
                     "This may happen when there aren't enough application instances on different tag " +
                     "dimensions compared to an active and corresponding standby task. " +
                     "Consider launching application instances on different tag dimensions than [{}]. " +
                     "Standby task assignment will fall back to assigning standby tasks to the least loaded clients.",
                     numRemainingStandbys, numberOfStandbyClients,
                     activeTaskId, rackAwareAssignmentTags,
                     clientStates.get(activeTaskClient).clientTags());

        } else {
            tasksToRemainingStandbys.remove(activeTaskId);
        }
    }

    private static boolean isClientUsedOnAnyOfTheTagEntries(final UUID client,
                                                            final Map<TagEntry, Set<UUID>> tagEntryToUsedClients) {
        return tagEntryToUsedClients.values().stream().anyMatch(usedClients -> usedClients.contains(client));
    }

    private static void updateClientsOnAlreadyUsedTagEntries(final UUID usedClient,
                                                             final int countOfUsedClients,
                                                             final Set<String> rackAwareAssignmentTags,
                                                             final Map<UUID, ClientState> clientStates,
                                                             final Map<TagEntry, Set<UUID>> tagEntryToClients,
                                                             final Map<String, Set<String>> tagKeyToValues,
                                                             final Map<TagEntry, Set<UUID>> tagEntryToUsedClients) {
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

            // Consider the following client setup where we need to distribute 2 standby tasks for each stateful task.
            //
            // # Kafka Streams Client 1
            // client.tag.zone: eu-central-1a
            // client.tag.cluster: k8s-cluster1
            // rack.aware.assignment.tags: zone,cluster
            //
            // # Kafka Streams Client 2
            // client.tag.zone: eu-central-1b
            // client.tag.cluster: k8s-cluster1
            // rack.aware.assignment.tags: zone,cluster
            //
            // # Kafka Streams Client 3
            // client.tag.zone: eu-central-1c
            // client.tag.cluster: k8s-cluster1
            // rack.aware.assignment.tags: zone,cluster
            //
            // # Kafka Streams Client 4
            // client.tag.zone: eu-central-1a
            // client.tag.cluster: k8s-cluster2
            // rack.aware.assignment.tags: zone,cluster
            //
            // # Kafka Streams Client 5
            // client.tag.zone: eu-central-1b
            // client.tag.cluster: k8s-cluster2
            // rack.aware.assignment.tags: zone,cluster
            //
            // # Kafka Streams Client 6
            // client.tag.zone: eu-central-1c
            // client.tag.cluster: k8s-cluster2
            // rack.aware.assignment.tags: zone,cluster
            //
            // Since we have only two unique `cluster` tag values,
            // we can only achieve "ideal" distribution on the 1st standby task assignment.
            // Ideal distribution for the 1st standby task can be achieved because we can assign standby task
            // to the client located on different cluster and zone compared to an active task.
            // We can't consider the `cluster` tag for the 2nd standby task assignment because the 1st standby
            // task would already be assigned on different cluster compared to the active one, which means
            // we have already used all the available cluster tag values. Taking the `cluster` tag into consideration
            // for the 2nd standby task assignment would effectively mean excluding all the clients.
            // Instead, for the 2nd standby task, we can only achieve partial rack awareness based on the `zone` tag.
            // As we don't consider the `cluster` tag for the 2nd standby task assignment, partial rack awareness
            // can be satisfied by placing the 2nd standby client on a different `zone` tag compared to active and corresponding standby tasks.
            // The `zone` on either `cluster` tags are valid candidates for the partial rack awareness, as our goal is to distribute clients on the different `zone` tags.

            // This statement checks if we have used more clients than the number of unique values for the given tag,
            // and if so, removes those tag entries from the tagEntryToUsedClients map.
            if (allTagValues.size() <= countOfUsedClients) {
                allTagValues.forEach(tagValue -> tagEntryToUsedClients.remove(new TagEntry(tagKey, tagValue)));
            } else {
                final String tagValue = usedClientTagEntry.getValue();
                final TagEntry tagEntry = new TagEntry(tagKey, tagValue);
                final Set<UUID> clientsOnUsedTagValue = tagEntryToClients.get(tagEntry);
                tagEntryToUsedClients.put(tagEntry, clientsOnUsedTagValue);
            }
        }
    }

    // Visible for testing
    static final class TagEntry {
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
