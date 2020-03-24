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

import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.RankedClient;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class HighAvailabilityTaskAssignor<ID extends Comparable<ID>> implements TaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(HighAvailabilityTaskAssignor.class);

    private final Map<ID, ClientState> clientStates;
    private final AssignmentConfigs configs;
    private final Set<TaskId> allTasks;
    private final Set<TaskId> statelessTasks;
    private final SortedMap<TaskId, SortedSet<RankedClient<ID>>> statefulTasksToRankedCandidates;

    public HighAvailabilityTaskAssignor(final Map<ID, ClientState> clientStates,
                                        final Set<TaskId> allTasks,
                                        final Set<TaskId> statefulTasks,
                                        final AssignmentConfigs configs) {
        this.configs = configs;
        this.clientStates = clientStates;
        this.allTasks = allTasks;

        statelessTasks = new HashSet<>(allTasks);
        statelessTasks.removeAll(statefulTasks);

        statefulTasksToRankedCandidates =
            buildClientRankingsByTask(statefulTasks, clientStates, configs.acceptableRecoveryLag);
    }

    @Override
    public boolean assign() {
        final Map<ID, List<TaskId>> warmupTaskAssignment = initializeEmptyTaskAssignmentMap();
        final Map<ID, List<TaskId>> standbyTaskAssignment = initializeEmptyTaskAssignmentMap();
        final Map<ID, List<TaskId>> statelessActiveTaskAssignment = initializeEmptyTaskAssignmentMap();

        final Map<ID, Integer> clientsToNumberOfThreads = new HashMap<>();
        clientStates.forEach((client, state) -> clientsToNumberOfThreads.put(client, state.capacity()));
        final SortedSet<ID> sortedClients = new TreeSet<>(clientsToNumberOfThreads.keySet());
        final SortedSet<TaskId> sortedTasks = new TreeSet<>(statefulTasksToRankedCandidates.keySet());

        final Map<ID, List<TaskId>> statefulActiveTaskAssignment =
            new DefaultStateConstrainedBalancedAssignor<ID>().assign(
                statefulTasksToRankedCandidates,
                configs.balanceFactor,
                sortedClients,
                clientsToNumberOfThreads);
        final Map<ID, List<TaskId>> balancedStatefulActiveTaskAssignment =
            new DefaultBalancedAssignor<ID>().assign(
                sortedClients,
                sortedTasks,
                clientsToNumberOfThreads,
                configs.balanceFactor);

        int totalWarmupReplicasAssigned = 0;
        for (final Movement<ID> movement : getMovements(statefulActiveTaskAssignment, balancedStatefulActiveTaskAssignment)) {
            if (totalWarmupReplicasAssigned >= configs.maxWarmupReplicas) {
                break;
            }
            warmupTaskAssignment.get(movement.destination).add(movement.task);
            ++totalWarmupReplicasAssigned;
        }

        final PriorityQueue<ID> clientsByStandbyTaskLoad =
            new PriorityQueue<>(Comparator.comparingInt(client -> standbyTaskAssignment.get(client).size()));

        //TODO-soph account for individual client capacities! maybe check out StickyTaskAssignor
        for (final TaskId task : statefulTasksToRankedCandidates.keySet()) {
            final Set<ID> standbyHostClients = getNumStandbyLeastLoadedCandidates(
                task,
                configs.numStandbyReplicas,
                clientsByStandbyTaskLoad,
                warmupTaskAssignment,
                standbyTaskAssignment,
                statelessActiveTaskAssignment
            );
            for (final ID client : standbyHostClients) {
                standbyTaskAssignment.get(client).add(task);
            }
        }

        final PriorityQueue<ID> clientsByActiveTaskLoad =
            new PriorityQueue<>(Comparator.comparingInt(client ->
                statefulActiveTaskAssignment.get(client).size() + statelessActiveTaskAssignment.get(client).size())
            );

        for (final TaskId task : statelessTasks) {
            final ID client = clientsByActiveTaskLoad.poll();
            statelessActiveTaskAssignment.get(client).add(task);
            clientsByActiveTaskLoad.offer(client);
        }

        final boolean followupRebalanceRequired;
        if (shouldUsePreviousAssignment()) {
            assignPreviousTasksToClientStates();
            followupRebalanceRequired = false;
        } else {
            assignTasksToClientStates(
                statefulActiveTaskAssignment,
                statelessActiveTaskAssignment,
                standbyTaskAssignment,
                warmupTaskAssignment
            );
            final int assignmentBalanceFactor = computeActualAssignmentBalanceFactor();
            followupRebalanceRequired = assignmentBalanceFactor <= configs.balanceFactor;
        }
        return followupRebalanceRequired;
    }

    /**
     * Returns a list of the movements of tasks from statefulActiveTaskAssignment to balancedStatefulActiveTaskAssignment
     * @param statefulActiveTaskAssignment the initial assignment, with source clients
     * @param balancedStatefulActiveTaskAssignment the final assignment, with destination clients
     */
    static <ID> List<Movement<ID>> getMovements(final Map<ID, List<TaskId>> statefulActiveTaskAssignment,
                                                final Map<ID, List<TaskId>> balancedStatefulActiveTaskAssignment) {
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

        final List<Movement<ID>> movements = new ArrayList<>();
        for (final Map.Entry<ID, List<TaskId>> sourceClientEntry : statefulActiveTaskAssignment.entrySet()) {
            final ID source = sourceClientEntry.getKey();

            for (final TaskId task : sourceClientEntry.getValue()) {
                final ID destination = taskToDestinationClient.get(task);
                if (destination == null) {
                    log.error("Task {} is assigned to client {} in initial assignment but has no owner in the final " +
                                  "balanced assignment.", task, source);
                    throw new IllegalStateException("Found task in initial assignment that was not assigned in the final.");
                }
                movements.add(new Movement<>(task, source, destination));
            }
        }
        return movements;
    }

    /**
     * @param taskId the standby task id
     * @param numStandbyReplicas the number of clients to return (may be fewer if there is insufficient capacity)
     * @param clientsByStandbyTaskLoad all clients, ordered by standby task load
     * @return the clients that should be assigned this standby task
     */
    static <ID> Set<ID> getNumStandbyLeastLoadedCandidates(final TaskId taskId,
                                                           final int numStandbyReplicas,
                                                           final PriorityQueue<ID> clientsByStandbyTaskLoad,
                                                           final Map<ID, List<TaskId>> warmupTaskAssignment,
                                                           final Map<ID, List<TaskId>> standbyTaskAssignment,
                                                           final Map<ID, List<TaskId>> statelessActiveTaskAssignment) {
        final Set<ID> standbyHostClients = new HashSet<>();
        final Set<ID> polledClients = new HashSet<>();
        while (standbyHostClients.size() < numStandbyReplicas) {
            ID candidateClient;
            while (true) {
                candidateClient = clientsByStandbyTaskLoad.poll();
                if (candidateClient == null) {
                    log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                                 "There is not enough available capacity. You should " +
                                 "increase the number of threads and/or application instances " +
                                 "to maintain the requested number of standby replicas.",
                        numStandbyReplicas - standbyHostClients.size(),
                        numStandbyReplicas, taskId);
                    return standbyHostClients;
                } else if (canBeAssignedToClient(
                    taskId,
                    candidateClient,
                    warmupTaskAssignment,
                    standbyTaskAssignment,
                    statelessActiveTaskAssignment)) {
                    break;
                }
                polledClients.add(candidateClient);
            }
        }
        clientsByStandbyTaskLoad.addAll(polledClients);
        return standbyHostClients;
    }

    /**
     * @return true iff this client is allowed to host this task
     *         false iff it has already been assigned an active, standby, or warmup version of this task
     */
    private static <ID> boolean canBeAssignedToClient(final TaskId task,
                                                      final ID client,
                                                      final Map<ID, List<TaskId>> warmupTaskAssignment,
                                                      final Map<ID, List<TaskId>> standbyTaskAssignment,
                                                      final Map<ID, List<TaskId>> statelessActiveTaskAssignment) {
        return !warmupTaskAssignment.get(client).contains(task)
                   && !standbyTaskAssignment.get(client).contains(task)
                   && !statelessActiveTaskAssignment.get(client).contains(task);
    }

    /**
     * @return true iff all active tasks with caught-up client are assigned to one of them, and all tasks are assigned
     */
    private boolean previousAssignmentIsValid() {
        final Set<TaskId> unassignedActiveTasks = new HashSet<>(allTasks);
        final Map<TaskId, Integer> unassignedStandbyTasks =
            configs.numStandbyReplicas == 0 ?
                Collections.emptyMap() :
                new HashMap<>(allTasks.stream().collect(Collectors.toMap(task -> task, task -> configs.numStandbyReplicas)));

        for (final Map.Entry<ID, ClientState> clientEntry : clientStates.entrySet()) {
            final ID client = clientEntry.getKey();
            final ClientState state = clientEntry.getValue();

            // Verify that this client was caught-up on all active tasks
            for (final TaskId activeTask : state.prevActiveTasks()) {
                if (!taskIsCaughtUpOnClientOrHasNoCaughtUpClients(activeTask, client)) {
                    return false;
                }
            }
            unassignedActiveTasks.removeAll(state.prevActiveTasks());

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

    private boolean taskIsCaughtUpOnClientOrHasNoCaughtUpClients(final TaskId task, final ID client) {
        boolean hasNoCaughtUpClients = true;
        for (final RankedClient rankedClient : statefulTasksToRankedCandidates.get(task)) {
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

    private void assignTasksToClientStates(final Map<ID, List<TaskId>> statefulActiveTasks,
                                           final Map<ID, List<TaskId>> statelessActiveTasks,
                                           final Map<ID, List<TaskId>> standbyTasks,
                                           final Map<ID, List<TaskId>> warmupTasks) {
        for (final Map.Entry<ID, ClientState> clientEntry : clientStates.entrySet()) {
            final ID clientId = clientEntry.getKey();
            final ClientState state = clientEntry.getValue();
            state.assignActiveTasks(statefulActiveTasks.get(clientId));
            state.assignActiveTasks(statelessActiveTasks.get(clientId));
            state.assignStandbyTasks(standbyTasks.get(clientId));
            state.assignStandbyTasks(warmupTasks.get(clientId));
        }
    }

    private void assignPreviousTasksToClientStates() {
        for (final ClientState clientState : clientStates.values()) {
            clientState.reusePreviousAssignment();
        }
    }

    /**
     * Rankings are computed as follows, with lower being more caught up:
     *      Rank -1: active running task
     *      Rank 0: standby or restoring task whose overall lag is within the acceptableRecoveryLag bounds
     *      Rank 1: tasks whose lag is unknown, eg because it was not encoded in an older version subscription
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
     * Determines whether to use the new proposed assignment or just return the group's previous assignment. The
     * previous assignment will be chosen iff all of the following are true:
     *   1) it satisfies the state constraint, ie all tasks with caught up clients are assigned to one of those clients
     *   2) it satisfies the balance factor
     *   3) there are no unassigned tasks (eg due to a client that dropped out of the group)
     */
    private boolean shouldUsePreviousAssignment() {
        if (previousAssignmentIsValid()) {
            return computePreviousAssignmentBalanceFactor() <= configs.balanceFactor;
        } else {
            return false;
        }
    }

    private int computePreviousAssignmentBalanceFactor() {
        return computeBalanceFactor(clientStates.values(), statelessTasks, ClientState::prevActiveTasks);
    }

    private int computeActualAssignmentBalanceFactor() {
        return computeBalanceFactor(clientStates.values(), statelessTasks, ClientState::activeTasks);
    }

    static int computeBalanceFactor(final Collection<ClientState> clientStates,
                                    final Set<TaskId> statelessTasks,
                                    final Function<ClientState, Set<TaskId>> activeTaskSet) {
        int minActiveStatefulTaskCount = Integer.MAX_VALUE;
        int maxActiveStatefulTaskCount = 0;

        for (final ClientState state : clientStates) {
            final Set<TaskId> prevActiveTasks = new HashSet<>(activeTaskSet.apply(state));
            prevActiveTasks.removeAll(statelessTasks);
            final int taskCount = prevActiveTasks.size();
            if (taskCount < minActiveStatefulTaskCount) {
                minActiveStatefulTaskCount = taskCount;
            } else if (taskCount > maxActiveStatefulTaskCount) {
                maxActiveStatefulTaskCount = taskCount;
            }
        }

        return maxActiveStatefulTaskCount - minActiveStatefulTaskCount;
    }

    private Map<ID, List<TaskId>> initializeEmptyTaskAssignmentMap() {
        return clientStates.keySet().stream().collect(Collectors.toMap(id -> id, id -> new ArrayList<>()));
    }

    public static class Movement<ID> {
        final TaskId task;
        final ID source;
        final ID destination;

        Movement(final TaskId task, final ID source, final ID destination) {
            this.task = task;
            this.source = source;
            this.destination = destination;
        }
    }
}