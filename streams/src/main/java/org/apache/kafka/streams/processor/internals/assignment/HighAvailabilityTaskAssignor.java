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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.processor.internals.assignment.AssignmentUtils.taskIsCaughtUpOnClientOrNoCaughtUpClientsExist;
import static org.apache.kafka.streams.processor.internals.assignment.RankedClient.buildClientRankingsByTask;
import static org.apache.kafka.streams.processor.internals.assignment.RankedClient.tasksToCaughtUpClients;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.assignTaskMovements;

public class HighAvailabilityTaskAssignor implements TaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(HighAvailabilityTaskAssignor.class);

    private Map<UUID, ClientState> clientStates;
    private Map<UUID, Integer> clientsToNumberOfThreads;
    private SortedSet<UUID> sortedClients;

    private Set<TaskId> allTasks;
    private SortedSet<TaskId> statefulTasks;
    private SortedSet<TaskId> statelessTasks;

    private AssignmentConfigs configs;

    private SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates;
    private Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients;

    @Override
    public boolean assign(final Map<UUID, ClientState> clientStates,
                          final Set<TaskId> allTasks,
                          final Set<TaskId> statefulTasks,
                          final AssignmentConfigs configs) {
        this.configs = configs;
        this.clientStates = clientStates;
        this.allTasks = allTasks;
        this.statefulTasks = new TreeSet<>(statefulTasks);

        statelessTasks = new TreeSet<>(allTasks);
        statelessTasks.removeAll(statefulTasks);

        sortedClients = new TreeSet<>();
        clientsToNumberOfThreads = new HashMap<>();
        clientStates.forEach((client, state) -> {
            sortedClients.add(client);
            clientsToNumberOfThreads.put(client, state.capacity());
        });

        statefulTasksToRankedCandidates =
            buildClientRankingsByTask(statefulTasks, clientStates, configs.acceptableRecoveryLag);
        tasksToCaughtUpClients = tasksToCaughtUpClients(statefulTasksToRankedCandidates);


        if (shouldUsePreviousAssignment()) {
            assignPreviousTasksToClientStates();
            return false;
        }

        final Map<TaskId, Integer> tasksToRemainingStandbys =
            statefulTasks.stream().collect(Collectors.toMap(task -> task, t -> configs.numStandbyReplicas));

        final boolean probingRebalanceNeeded = assignStatefulActiveTasks(tasksToRemainingStandbys);

        assignStandbyReplicaTasks(tasksToRemainingStandbys);

        assignStatelessActiveTasks();

        log.info("Decided on assignment: " +
                     clientStates +
                     " with " +
                     (probingRebalanceNeeded ? "" : "no") +
                     " followup probing rebalance.");
        return probingRebalanceNeeded;
    }

    private boolean assignStatefulActiveTasks(final Map<TaskId, Integer> tasksToRemainingStandbys) {
        final Map<UUID, List<TaskId>> statefulActiveTaskAssignment = new DefaultBalancedAssignor().assign(
            sortedClients,
            statefulTasks,
            clientsToNumberOfThreads,
            configs.balanceFactor
        );

        return assignTaskMovements(
            statefulActiveTaskAssignment,
            tasksToCaughtUpClients,
            clientStates,
            tasksToRemainingStandbys,
            configs.maxWarmupReplicas
        );
    }

    private void assignStandbyReplicaTasks(final Map<TaskId, Integer> tasksToRemainingStandbys) {
        final ValidClientsByTaskLoadQueue standbyTaskClientsByTaskLoad = new ValidClientsByTaskLoadQueue(
            clientStates,
            (client, task) -> !clientStates.get(client).assignedTasks().contains(task)
        );
        standbyTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        for (final TaskId task : statefulTasksToRankedCandidates.keySet()) {
            final int numRemainingStandbys = tasksToRemainingStandbys.get(task);
            final List<UUID> clients = standbyTaskClientsByTaskLoad.poll(task, numRemainingStandbys);
            for (final UUID client : clients) {
                clientStates.get(client).assignStandby(task);
            }
            standbyTaskClientsByTaskLoad.offerAll(clients);

            final int numStandbysAssigned = clients.size();
            if (numStandbysAssigned < numRemainingStandbys) {
                log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                             "There is not enough available capacity. You should " +
                             "increase the number of threads and/or application instances " +
                             "to maintain the requested number of standby replicas.",
                         numRemainingStandbys - numStandbysAssigned, configs.numStandbyReplicas, task);
            }
        }
    }

    private void assignStatelessActiveTasks() {
        final ValidClientsByTaskLoadQueue statelessActiveTaskClientsByTaskLoad = new ValidClientsByTaskLoadQueue(
            clientStates,
            (client, task) -> true
        );
        statelessActiveTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        for (final TaskId task : statelessTasks) {
            final UUID client = statelessActiveTaskClientsByTaskLoad.poll(task);
            final ClientState state = clientStates.get(client);
            state.assignActive(task);
            statelessActiveTaskClientsByTaskLoad.offer(client);
        }
    }

    /**
     * @return true iff all active tasks with caught-up client are assigned to one of them, and all tasks are assigned
     */
    boolean previousAssignmentIsValid() {
        final Set<TaskId> unassignedActiveTasks = new HashSet<>(allTasks);
        final Map<TaskId, Integer> unassignedStandbyTasks =
            configs.numStandbyReplicas == 0 ?
                Collections.emptyMap() :
                new HashMap<>(statefulTasksToRankedCandidates.keySet().stream()
                                  .collect(Collectors.toMap(task -> task, task -> configs.numStandbyReplicas)));

        for (final Map.Entry<UUID, ClientState> clientEntry : clientStates.entrySet()) {
            final UUID client = clientEntry.getKey();
            final ClientState state = clientEntry.getValue();
            final Set<TaskId> prevActiveTasks = state.prevActiveTasks();

            // Verify that this client was caught-up on all stateful active tasks
            for (final TaskId activeTask : prevActiveTasks) {
                if (!taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(activeTask, client, tasksToCaughtUpClients)) {
                    return false;
                }
            }
            if (!unassignedActiveTasks.containsAll(prevActiveTasks)) {
                return false;
            }
            unassignedActiveTasks.removeAll(prevActiveTasks);

            for (final TaskId task : state.prevStandbyTasks()) {
                final Integer remainingStandbys = unassignedStandbyTasks.get(task);
                if (remainingStandbys != null) {
                    if (remainingStandbys == 1) {
                        unassignedStandbyTasks.remove(task);
                    } else {
                        unassignedStandbyTasks.put(task, remainingStandbys - 1);
                    }
                } else {
                    return false;
                }
            }

        }
        return unassignedActiveTasks.isEmpty() && unassignedStandbyTasks.isEmpty();
    }

    /**
     * Compute the balance factor as the difference in stateful active task count per thread between the most and
     * least loaded clients
     */
    static int computeBalanceFactor(final Collection<ClientState> clientStates,
                                    final Set<TaskId> statefulTasks) {
        int minActiveStatefulTasksPerThreadCount = Integer.MAX_VALUE;
        int maxActiveStatefulTasksPerThreadCount = 0;

        for (final ClientState state : clientStates) {
            final Set<TaskId> activeTasks = new HashSet<>(state.prevActiveTasks());
            activeTasks.retainAll(statefulTasks);
            final int taskPerThreadCount = activeTasks.size() / state.capacity();
            if (taskPerThreadCount < minActiveStatefulTasksPerThreadCount) {
                minActiveStatefulTasksPerThreadCount = taskPerThreadCount;
            }
            if (taskPerThreadCount > maxActiveStatefulTasksPerThreadCount) {
                maxActiveStatefulTasksPerThreadCount = taskPerThreadCount;
            }
        }

        return maxActiveStatefulTasksPerThreadCount - minActiveStatefulTasksPerThreadCount;
    }

    /**
     * Determines whether to use the new proposed assignment or just return the group's previous assignment. The
     * previous assignment will be chosen and returned iff all of the following are true:
     *   1) it satisfies the state constraint, ie all tasks with caught up clients are assigned to one of those clients
     *   2) it satisfies the balance factor
     *   3) there are no unassigned tasks (eg due to a client that dropped out of the group)
     *   4) there are no warmup tasks
     */
    private boolean shouldUsePreviousAssignment() {
        if (previousAssignmentIsValid()) {
            final int previousAssignmentBalanceFactor =
                computeBalanceFactor(clientStates.values(), statefulTasks);
            return previousAssignmentBalanceFactor <= configs.balanceFactor;
        } else {
            return false;
        }
    }

    private void assignPreviousTasksToClientStates() {
        for (final ClientState clientState : clientStates.values()) {
            clientState.assignActiveTasks(clientState.prevActiveTasks());
            clientState.assignStandbyTasks(clientState.prevStandbyTasks());
        }
    }

}
