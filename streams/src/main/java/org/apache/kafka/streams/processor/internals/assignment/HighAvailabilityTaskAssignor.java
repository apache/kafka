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
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class HighAvailabilityTaskAssignor<ID extends Comparable<ID>> implements TaskAssignor<ID> {
    private static final Logger log = LoggerFactory.getLogger(HighAvailabilityTaskAssignor.class);

    private final Map<ID, ClientState> clientStates;
    private final Map<ID, Integer> clientsToNumberOfThreads;
    private final SortedSet<ID> sortedClients;

    private final Set<TaskId> allTasks;
    private final SortedSet<TaskId> statefulTasks;
    private final SortedSet<TaskId> statelessTasks;

    private final AssignmentConfigs configs;

    private final SortedMap<TaskId, SortedSet<RankedClient<ID>>> statefulTasksToRankedCandidates;

    public HighAvailabilityTaskAssignor(final Map<ID, ClientState> clientStates,
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

        final Map<ID, List<TaskId>> warmupTaskAssignment = initializeEmptyTaskAssignmentMap();
        final Map<ID, List<TaskId>> standbyTaskAssignment = initializeEmptyTaskAssignmentMap();
        final Map<ID, List<TaskId>> statelessActiveTaskAssignment = initializeEmptyTaskAssignmentMap();

        // ---------------- Stateful Active Tasks ---------------- //

        final Map<ID, List<TaskId>> statefulActiveTaskAssignment =
            new DefaultStateConstrainedBalancedAssignor<ID>().assign(
                statefulTasksToRankedCandidates,
                configs.balanceFactor,
                sortedClients,
                clientsToNumberOfThreads
            );

        // ---------------- Warmup Replica Tasks ---------------- //

        final Map<ID, List<TaskId>> balancedStatefulActiveTaskAssignment =
            new DefaultBalancedAssignor<ID>().assign(
                sortedClients,
                statefulTasks,
                clientsToNumberOfThreads,
                configs.balanceFactor);

        final List<Movement<ID>> movements =
            getMovements(statefulActiveTaskAssignment, balancedStatefulActiveTaskAssignment,
                configs.maxWarmupReplicas);
        for (final Movement<ID> movement : movements) {
            warmupTaskAssignment.get(movement.destination).add(movement.task);
        }

        // ---------------- Standby Replica Tasks ---------------- //

        final List<Map<ID, List<TaskId>>> allTaskAssignments = asList(
            statefulActiveTaskAssignment,
            warmupTaskAssignment,
            standbyTaskAssignment,
            statelessActiveTaskAssignment
        );

        final ValidClientsByTaskLoadQueue<ID> clientsByStandbyTaskLoad =
            new ValidClientsByTaskLoadQueue<>(
                configs.numStandbyReplicas,
                getClientPriorityQueueByTaskLoad(allTaskAssignments),
                allTaskAssignments
            );

        for (final TaskId task : statefulTasksToRankedCandidates.keySet()) {
            final List<ID> clients = clientsByStandbyTaskLoad.poll(task);
            for (final ID client : clients) {
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

        final PriorityQueue<ID> statelessActiveTaskClientsQueue = getClientPriorityQueueByTaskLoad(allTaskAssignments);

        for (final TaskId task : statelessTasks) {
            final ID client = statelessActiveTaskClientsQueue.poll();
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
     * Returns a list of the movements of tasks from statefulActiveTaskAssignment to balancedStatefulActiveTaskAssignment
     * @param statefulActiveTaskAssignment the initial assignment, with source clients
     * @param balancedStatefulActiveTaskAssignment the final assignment, with destination clients
     */
    static <ID> List<Movement<ID>> getMovements(final Map<ID, List<TaskId>> statefulActiveTaskAssignment,
                                                final Map<ID, List<TaskId>> balancedStatefulActiveTaskAssignment,
                                                final int maxWarmupReplicas) {
        if (statefulActiveTaskAssignment.size() != balancedStatefulActiveTaskAssignment.size()) {
            throw new IllegalStateException("Tried to compute movements but assignments differ in size.");
        }

        final Map<TaskId, ID> taskToDestinationClient = new HashMap<>();
        for (final Map.Entry<ID, List<TaskId>> clientEntry : balancedStatefulActiveTaskAssignment.entrySet()) {
            final ID destination = clientEntry.getKey();
            for (final TaskId task : clientEntry.getValue()) {
                taskToDestinationClient.put(task, destination);
            }
        }

        final List<Movement<ID>> movements = new LinkedList<>();
        for (final Map.Entry<ID, List<TaskId>> sourceClientEntry : statefulActiveTaskAssignment.entrySet()) {
            final ID source = sourceClientEntry.getKey();

            for (final TaskId task : sourceClientEntry.getValue()) {
                final ID destination = taskToDestinationClient.get(task);
                if (destination == null) {
                    log.error("Task {} is assigned to client {} in initial assignment but has no owner in the final " +
                                  "balanced assignment.", task, source);
                    throw new IllegalStateException("Found task in initial assignment that was not assigned in the final.");
                } else if (!source.equals(destination)) {
                    movements.add(new Movement<>(task, source, destination));
                    if (movements.size() == maxWarmupReplicas) {
                        return movements;
                    }
                }
            }
        }
        return movements;
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

        for (final Map.Entry<ID, ClientState> clientEntry : clientStates.entrySet()) {
            final ID client = clientEntry.getKey();
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
    boolean taskIsCaughtUpOnClient(final TaskId task, final ID client) {
        boolean hasNoCaughtUpClients = true;
        final SortedSet<RankedClient<ID>> rankedClients = statefulTasksToRankedCandidates.get(task);
        if (rankedClients == null) {
            return true;
        }
        for (final RankedClient<ID> rankedClient : rankedClients) {
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
     * Rankings are computed as follows, with lower being more caught up:
     *      Rank -1: active running task
     *      Rank 0: standby or restoring task whose overall lag is within the acceptableRecoveryLag bounds
     *      Rank 1: tasks whose lag is unknown, eg because it was not encoded in an older version subscription.
     *                 Since it may have been caught-up, we rank it higher than clients whom we know are not caught-up
     *                 to give it priority without classifying it as caught-up and risking violating high availability
     *      Rank 1+: all other tasks are ranked according to their actual total lag
     * @return Sorted set of all client candidates for each stateful task, ranked by their overall lag. Tasks are
     */
    static <ID extends Comparable<ID>> SortedMap<TaskId, SortedSet<RankedClient<ID>>> buildClientRankingsByTask(final Set<TaskId> statefulTasks,
                                                                                                                final Map<ID, ClientState> clientStates,
                                                                                                                final long acceptableRecoveryLag) {
        final SortedMap<TaskId, SortedSet<RankedClient<ID>>> statefulTasksToRankedCandidates = new TreeMap<>();

        for (final TaskId task : statefulTasks) {
            final SortedSet<RankedClient<ID>> rankedClientCandidates = new TreeSet<>();
            statefulTasksToRankedCandidates.put(task, rankedClientCandidates);

            for (final Map.Entry<ID, ClientState> clientEntry : clientStates.entrySet()) {
                final ID clientId = clientEntry.getKey();
                final long taskLag = clientEntry.getValue().lagFor(task);
                final long clientRank;
                if (taskLag == Task.LATEST_OFFSET) {
                    clientRank = Task.LATEST_OFFSET;
                } else if (taskLag == UNKNOWN_OFFSET_SUM) {
                    clientRank = 1L;
                } else if (taskLag <= acceptableRecoveryLag) {
                    clientRank = 0L;
                } else {
                    clientRank = taskLag;
                }
                rankedClientCandidates.add(new RankedClient<>(clientId, clientRank));
            }
        }
        log.trace("Computed statefulTasksToRankedCandidates map as {}", statefulTasksToRankedCandidates);

        return statefulTasksToRankedCandidates;
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

    private Map<ID, List<TaskId>> initializeEmptyTaskAssignmentMap() {
        return sortedClients.stream().collect(Collectors.toMap(id -> id, id -> new ArrayList<>()));
    }

    private void assignActiveTasksToClients(final Map<ID, List<TaskId>> activeTasks) {
        for (final Map.Entry<ID, ClientState> clientEntry : clientStates.entrySet()) {
            final ID clientId = clientEntry.getKey();
            final ClientState state = clientEntry.getValue();
            state.assignActiveTasks(activeTasks.get(clientId));
        }
    }

    private void assignStandbyTasksToClients(final Map<ID, List<TaskId>> standbyTasks) {
        for (final Map.Entry<ID, ClientState> clientEntry : clientStates.entrySet()) {
            final ID clientId = clientEntry.getKey();
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

    private PriorityQueue<ID> getClientPriorityQueueByTaskLoad(final List<Map<ID, List<TaskId>>> taskLoadsByClient) {
        final PriorityQueue<ID> queue = new PriorityQueue<>(
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

    private int tasksPerThread(final ID client, final List<Map<ID, List<TaskId>>> taskLoadsByClient) {
        double numTasks = 0;
        for (final Map<ID, List<TaskId>> assignment : taskLoadsByClient) {
            numTasks += assignment.get(client).size();
        }
        return (int) Math.ceil(numTasks / clientsToNumberOfThreads.get(client));
    }

    static class RankedClient<ID extends Comparable<? super ID>> implements Comparable<RankedClient<ID>> {
        private final ID clientId;
        private final long rank;

        RankedClient(final ID clientId, final long rank) {
            this.clientId = clientId;
            this.rank = rank;
        }

        ID clientId() {
            return clientId;
        }

        long rank() {
            return rank;
        }

        @Override
        public int compareTo(final RankedClient<ID> clientIdAndLag) {
            if (rank < clientIdAndLag.rank) {
                return -1;
            } else if (rank > clientIdAndLag.rank) {
                return 1;
            } else {
                return clientId.compareTo(clientIdAndLag.clientId);
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final RankedClient<?> that = (RankedClient<?>) o;
            return rank == that.rank && Objects.equals(clientId, that.clientId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientId, rank);
        }
    }

    static class Movement<ID> {
        final TaskId task;
        final ID source;
        final ID destination;

        Movement(final TaskId task, final ID source, final ID destination) {
            this.task = task;
            this.source = source;
            this.destination = destination;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Movement<?> movement = (Movement<?>) o;
            return Objects.equals(task, movement.task) &&
                       Objects.equals(source, movement.source) &&
                       Objects.equals(destination, movement.destination);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task, source, destination);
        }
    }

    /**
     * Wraps a priority queue of clients and returns the next valid candidate(s) based on the current task assignment
     */
    static class ValidClientsByTaskLoadQueue<ID> {
        private final int numClientsPerTask;
        private final PriorityQueue<ID> clientsByTaskLoad;
        private final List<Map<ID, List<TaskId>>> allStatefulTaskAssignments;

        ValidClientsByTaskLoadQueue(final int numClientsPerTask,
                                      final PriorityQueue<ID> clientsByTaskLoad,
                                      final List<Map<ID, List<TaskId>>> allStatefulTaskAssignments) {
            this.numClientsPerTask = numClientsPerTask;
            this.clientsByTaskLoad = clientsByTaskLoad;
            this.allStatefulTaskAssignments = allStatefulTaskAssignments;
        }

        /**
         * @return the next N <= {@code numClientsPerTask} clients in the underlying priority queue that are valid
         * candidates for the given task (ie do not already have any version of this task assigned)
         */
        List<ID> poll(final TaskId task) {
            final List<ID> nextLeastLoadedValidClients = new LinkedList<>();
            final Set<ID> invalidPolledClients = new HashSet<>();
            while (nextLeastLoadedValidClients.size() < numClientsPerTask) {
                ID candidateClient;
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

        void offer(final Collection<ID> clients) {
            returnPolledClientsToQueue(clients);
        }

        private boolean canBeAssignedToClient(final TaskId task, final ID client) {
            for (final Map<ID, List<TaskId>> taskAssignment : allStatefulTaskAssignments) {
                if (taskAssignment.get(client).contains(task)) {
                    return false;
                }
            }
            return true;
        }

        private void returnPolledClientsToQueue(final Collection<ID> polledClients) {
            for (final ID client : polledClients) {
                clientsByTaskLoad.offer(client);
            }
        }
    }
}
