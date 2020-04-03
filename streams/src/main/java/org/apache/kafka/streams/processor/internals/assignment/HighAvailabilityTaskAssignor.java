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

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.processor.internals.assignment.RankedClient.buildClientRankingsByTask;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.getMovements;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class HighAvailabilityTaskAssignor implements TaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(HighAvailabilityTaskAssignor.class);

    private final Map<UUID, ClientState> clientStates;
    private final Map<UUID, Integer> clientsToNumberOfThreads;
    private final SortedSet<UUID> sortedClients;

    private final Set<TaskId> allTasks;
    private final SortedSet<TaskId> statefulTasks;
    private final SortedSet<TaskId> statelessTasks;

    private final AssignmentConfigs configs;

    private final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedCandidates;

    public HighAvailabilityTaskAssignor(final Map<UUID, ClientState> clientStates,
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
    }

    @Override
    public boolean assign() {
        if (shouldUsePreviousAssignment()) {
            assignPreviousTasksToClientStates();
            return false;
        }

        final Map<UUID, List<TaskId>> warmupTaskAssignment = initializeEmptyTaskAssignmentMap(sortedClients);
        final Map<UUID, List<TaskId>> standbyTaskAssignment = initializeEmptyTaskAssignmentMap(sortedClients);
        final Map<UUID, List<TaskId>> statelessActiveTaskAssignment = initializeEmptyTaskAssignmentMap(sortedClients);

        // ---------------- Stateful Active Tasks ---------------- //

        final Map<UUID, List<TaskId>> statefulActiveTaskAssignment =
            new DefaultStateConstrainedBalancedAssignor().assign(
                statefulTasksToRankedCandidates,
                configs.balanceFactor,
                sortedClients,
                clientsToNumberOfThreads
            );

        // ---------------- Warmup Replica Tasks ---------------- //

        final Map<UUID, List<TaskId>> balancedStatefulActiveTaskAssignment =
            new DefaultBalancedAssignor().assign(
                sortedClients,
                statefulTasks,
                clientsToNumberOfThreads,
                configs.balanceFactor);

        final List<TaskMovement> movements = getMovements(
            statefulActiveTaskAssignment,
            balancedStatefulActiveTaskAssignment,
            configs.maxWarmupReplicas);

        for (final TaskMovement movement : movements) {
            warmupTaskAssignment.get(movement.destination).add(movement.task);
        }

        // ---------------- Standby Replica Tasks ---------------- //

        final List<Map<UUID, List<TaskId>>> allTaskAssignments = asList(
            statefulActiveTaskAssignment,
            warmupTaskAssignment,
            standbyTaskAssignment,
            statelessActiveTaskAssignment
        );

        final ValidClientsByTaskLoadQueue<UUID> clientsByStandbyTaskLoad =
            new ValidClientsByTaskLoadQueue<>(
                configs.numStandbyReplicas,
                getClientPriorityQueueByTaskLoad(allTaskAssignments),
                allTaskAssignments
            );

        for (final TaskId task : statefulTasksToRankedCandidates.keySet()) {
            final List<UUID> clients = clientsByStandbyTaskLoad.poll(task);
            for (final UUID client : clients) {
                standbyTaskAssignment.get(client).add(task);
            }
            clientsByStandbyTaskLoad.offer(clients);
            final int numStandbysAssigned = clients.size();
            if (numStandbysAssigned < configs.numStandbyReplicas) {
                log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                             "There is not enough available capacity. You should " +
                             "increase the number of threads and/or application instances " +
                             "to maintain the requested number of standby replicas.",
                    configs.numStandbyReplicas - numStandbysAssigned, configs.numStandbyReplicas, task);
            }
        }

        // ---------------- Stateless Active Tasks ---------------- //

        final PriorityQueue<UUID> statelessActiveTaskClientsQueue = getClientPriorityQueueByTaskLoad(allTaskAssignments);

        for (final TaskId task : statelessTasks) {
            final UUID client = statelessActiveTaskClientsQueue.poll();
            statelessActiveTaskAssignment.get(client).add(task);
            statelessActiveTaskClientsQueue.offer(client);
        }

        // ---------------- Assign Tasks To Clients ---------------- //

        assignActiveTasksToClients(statefulActiveTaskAssignment);
        assignStandbyTasksToClients(warmupTaskAssignment);
        assignStandbyTasksToClients(standbyTaskAssignment);
        assignActiveTasksToClients(statelessActiveTaskAssignment);

        return !movements.isEmpty();
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
                if (!taskIsCaughtUpOnClient(activeTask, client)) {
                    return false;
                }
            }
            unassignedActiveTasks.removeAll(prevActiveTasks);

            if (!unassignedStandbyTasks.isEmpty()) {
                for (final TaskId task : state.prevStandbyTasks()) {
                    final Integer remainingStandbys = unassignedStandbyTasks.get(task);
                    if (remainingStandbys != null) {
                        if (remainingStandbys == 1) {
                            unassignedStandbyTasks.remove(task);
                        } else {
                            unassignedStandbyTasks.put(task, remainingStandbys - 1);
                        }
                    }
                }
            }
        }
        return unassignedActiveTasks.isEmpty() && unassignedStandbyTasks.isEmpty();
    }

    /**
     * @return true if this client is caught-up for this task, or the task has no caught-up clients
     */
    boolean taskIsCaughtUpOnClient(final TaskId task, final UUID client) {
        boolean hasNoCaughtUpClients = true;
        final SortedSet<RankedClient> rankedClients = statefulTasksToRankedCandidates.get(task);
        if (rankedClients == null) {
            return true;
        }
        for (final RankedClient rankedClient : rankedClients) {
            if (rankedClient.rank() <= 0L) {
                if (rankedClient.clientId().equals(client)) {
                    return true;
                } else {
                    hasNoCaughtUpClients = false;
                }
            }

            // If we haven't found our client yet, it must not be caught-up
            if (rankedClient.rank() > 0L) {
                break;
            }
        }
        return hasNoCaughtUpClients;
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

    private static Map<UUID, List<TaskId>> initializeEmptyTaskAssignmentMap(final Set<UUID> clients) {
        return clients.stream().collect(Collectors.toMap(id -> id, id -> new ArrayList<>()));
    }

    private void assignActiveTasksToClients(final Map<UUID, List<TaskId>> activeTasks) {
        for (final Map.Entry<UUID, ClientState> clientEntry : clientStates.entrySet()) {
            final UUID clientId = clientEntry.getKey();
            final ClientState state = clientEntry.getValue();
            state.assignActiveTasks(activeTasks.get(clientId));
        }
    }

    private void assignStandbyTasksToClients(final Map<UUID, List<TaskId>> standbyTasks) {
        for (final Map.Entry<UUID, ClientState> clientEntry : clientStates.entrySet()) {
            final UUID clientId = clientEntry.getKey();
            final ClientState state = clientEntry.getValue();
            state.assignStandbyTasks(standbyTasks.get(clientId));
        }
    }

    private void assignPreviousTasksToClientStates() {
        for (final ClientState clientState : clientStates.values()) {
            clientState.assignActiveTasks(clientState.prevActiveTasks());
            clientState.assignStandbyTasks(clientState.prevStandbyTasks());
        }
    }

    private PriorityQueue<UUID> getClientPriorityQueueByTaskLoad(final List<Map<UUID, List<TaskId>>> taskLoadsByClient) {
        final PriorityQueue<UUID> queue = new PriorityQueue<>(
            (client, other) -> {
                final int clientTasksPerThread = tasksPerThread(client, taskLoadsByClient);
                final int otherTasksPerThread = tasksPerThread(other, taskLoadsByClient);
                if (clientTasksPerThread != otherTasksPerThread) {
                    return clientTasksPerThread - otherTasksPerThread;
                } else {
                    return client.compareTo(other);
                }
            });

        queue.addAll(sortedClients);
        return queue;
    }

    private int tasksPerThread(final UUID client, final List<Map<UUID, List<TaskId>>> taskLoadsByClient) {
        double numTasks = 0;
        for (final Map<UUID, List<TaskId>> assignment : taskLoadsByClient) {
            numTasks += assignment.get(client).size();
        }
        return (int) Math.ceil(numTasks / clientsToNumberOfThreads.get(client));
    }
    
    /**
     * Wraps a priority queue of clients and returns the next valid candidate(s) based on the current task assignment
     */
    static class ValidClientsByTaskLoadQueue<UUID> {
        private final int numClientsPerTask;
        private final PriorityQueue<UUID> clientsByTaskLoad;
        private final List<Map<UUID, List<TaskId>>> allStatefulTaskAssignments;

        ValidClientsByTaskLoadQueue(final int numClientsPerTask,
                                      final PriorityQueue<UUID> clientsByTaskLoad,
                                      final List<Map<UUID, List<TaskId>>> allStatefulTaskAssignments) {
            this.numClientsPerTask = numClientsPerTask;
            this.clientsByTaskLoad = clientsByTaskLoad;
            this.allStatefulTaskAssignments = allStatefulTaskAssignments;
        }

        /**
         * @return the next N <= {@code numClientsPerTask} clients in the underlying priority queue that are valid
         * candidates for the given task (ie do not already have any version of this task assigned)
         */
        List<UUID> poll(final TaskId task) {
            final List<UUID> nextLeastLoadedValidClients = new LinkedList<>();
            final Set<UUID> invalidPolledClients = new HashSet<>();
            while (nextLeastLoadedValidClients.size() < numClientsPerTask) {
                UUID candidateClient;
                while (true) {
                    candidateClient = clientsByTaskLoad.poll();
                    if (candidateClient == null) {
                        returnPolledClientsToQueue(invalidPolledClients);
                        return nextLeastLoadedValidClients;
                    }

                    if (canBeAssignedToClient(task, candidateClient)) {
                        nextLeastLoadedValidClients.add(candidateClient);
                        break;
                    } else {
                        invalidPolledClients.add(candidateClient);
                    }
                }
            }
            returnPolledClientsToQueue(invalidPolledClients);
            return nextLeastLoadedValidClients;
        }

        void offer(final Collection<UUID> clients) {
            returnPolledClientsToQueue(clients);
        }

        private boolean canBeAssignedToClient(final TaskId task, final UUID client) {
            for (final Map<UUID, List<TaskId>> taskAssignment : allStatefulTaskAssignments) {
                if (taskAssignment.get(client).contains(task)) {
                    return false;
                }
            }
            return true;
        }

        private void returnPolledClientsToQueue(final Collection<UUID> polledClients) {
            for (final UUID client : polledClients) {
                clientsByTaskLoad.offer(client);
            }
        }
    }
}
