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

public class HighAvailabilityTaskAssignor<ID extends Comparable<ID>>  implements TaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(HighAvailabilityTaskAssignor.class);

    private final Map<ID, ClientState> clientStates;
    private final AssignmentConfigs configs;
    private final Set<TaskId> statefulTasks;
    private final Set<TaskId> statelessTasks;
    private final SortedMap<TaskId, SortedSet<RankedClient<ID>>> statefulTasksToRankedCandidates;

    public HighAvailabilityTaskAssignor(final Map<ID, ClientState> clientStates,
                                        final Set<TaskId> allTasks,
                                        final Set<TaskId> statefulTasks,
                                        final AssignmentConfigs configs) {
        this.configs = configs;
        this.clientStates = clientStates;
        this.statefulTasks = statefulTasks;

        statelessTasks = new HashSet<>(allTasks);
        statelessTasks.removeAll(statefulTasks);

        statefulTasksToRankedCandidates =
            buildClientRankingsByTask(clientStates, statefulTasks, configs.acceptableRecoveryLag);
    }

    @Override
    public boolean assign() {
        final Map<ID, Integer> clientsToNumberOfThreads = getClientsToNumberOfThreads(clientStates);

        final Map<ID, List<TaskId>> actualStatefulActiveTaskAssignment =
            new DefaultStateConstrainedBalancedAssignor<ID>().assign(
                statefulTasksToRankedCandidates,
                configs.balanceFactor,
                clientsToNumberOfThreads);
        final Map<ID, List<TaskId>> balancedStatefulActiveTaskAssignment =
            new DefaultDataParallelBalancedAssignor<ID>().assign(
                statefulTasksToRankedCandidates,
                configs.balanceFactor,
                clientsToNumberOfThreads);

        final Map<ID, List<TaskId>> standbyTaskAssignment = getEmptyTaskAssignmentMap();
        final Map<TaskId, Integer> standbyTaskCounts =
            statefulTasks.stream().collect(Collectors.toMap(task -> task, task -> 0));

        final List<Movement<ID>> proposedMovements =
            getMovements(actualStatefulActiveTaskAssignment, balancedStatefulActiveTaskAssignment);

        for (final Movement<ID> movement : proposedMovements) {
            assignStandby(movement.task, movement.destination,  standbyTaskAssignment, standbyTaskCounts);
        }

        for (final Map.Entry<TaskId, SortedSet<RankedClient<ID>>> taskEntry : statefulTasksToRankedCandidates.entrySet()) {
            final TaskId task = taskEntry.getKey();
            // TODO-soph need to account for max.warmup.replicas here as well?
            while (standbyTaskCounts.get(task) < configs.numStandbyReplicas) {
                final ID client = getLeastLoadedCandidate(task);
                assignStandby(task, client, standbyTaskAssignment, standbyTaskCounts);
            }
        }

        final Map<ID, List<TaskId>> statelessActiveTaskAssignment = getEmptyTaskAssignmentMap();
        final PriorityQueue<ID> clientsByActiveTaskLoad =
            new PriorityQueue<>(Comparator.comparingInt(client ->
                actualStatefulActiveTaskAssignment.get(client).size() + statelessActiveTaskAssignment.get(client).size())
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
                actualStatefulActiveTaskAssignment,
                statelessActiveTaskAssignment,
                standbyTaskAssignment
            );
            final int assignmentBalanceFactor = computeActualAssignmentBalanceFactor();
            followupRebalanceRequired = assignmentBalanceFactor <= configs.balanceFactor;
        }
        return followupRebalanceRequired;
    }

    static <ID> List<Movement<ID>> getMovements(final Map<ID, List<TaskId>> actualStatefulActiveTaskAssignment,
                                                final Map<ID, List<TaskId>> balancedStatefulActiveTaskAssignment) {
        // should account for max.warmup.replicas
        //TODO-soph
        return Collections.emptyList();
    }

    private ID getLeastLoadedCandidate(final TaskId task) {
        //TODO-soph
        return null;
    }

    //TODO-soph don't need this parametrized if not using for standbys getLeastLoadedCandidate?
    private Function<ID, Integer> getTaskCount(final Map<ID, List<TaskId>>... taskAssignments) {
        return (client) -> {
            int taskCount = 0;
            for (final Map<ID, List<TaskId>> taskAssignment : taskAssignments) {
                taskCount += taskAssignment.get(client).size();
            }
            return taskCount;
        };
    }

    /**
     * @return true iff the previous assignment is a viable candidate, ie all active tasks with caught-up clients
     *             are assigned to one of those clients.
     */
    private boolean completePreviousAssignment() {
        //TODO-soph complete previous assignment by assigning ALL tasks (ie tasks remaining due to missing member)
        //TODO-soph look into refactoring and reusing code in StickyTaskAssignor
        return true;
    }

    private void assignTasksToClientStates(final Map<ID, List<TaskId>> statefulActiveTasks,
                                           final Map<ID, List<TaskId>> statelessActiveTasks,
                                           final Map<ID, List<TaskId>> standbyTasks) {
        for (final Map.Entry<ID, ClientState> clientEntry : clientStates.entrySet()) {
            final ID clientId = clientEntry.getKey();
            final ClientState state = clientEntry.getValue();
            state.assignActiveTasks(statefulActiveTasks.get(clientId));
            state.assignActiveTasks(statelessActiveTasks.get(clientId));
            state.assignStandbyTasks(standbyTasks.get(clientId));
        }
    }

    private void assignPreviousTasksToClientStates() {
        for (final ClientState clientState : clientStates.values()) {
            clientState.reusePreviousAssignment();
        }
    }

    private static <ID> void assignStandby(final TaskId task,
                                           final ID client,
                                           final Map<ID, List<TaskId>> standbyTaskAssignment,
                                           final Map<TaskId, Integer> standbyTaskCounts) {
        standbyTaskAssignment.get(client).add(task);
        standbyTaskCounts.compute(task, (id, count) -> count + 1);
    }

    /**
     * Rankings are computed as follows, with lower being more caught up:
     *      Rank -1: active running task
     *      Rank 0: standby or restoring task whose overall lag is within the acceptableRecoveryLag bounds
     *      Rank 1: tasks whose lag is unknown, eg because it was not encoded in an older version subscription
     *      Rank 1+: all other tasks are ranked according to their actual total lag
     * @return Sorted set of all client candidates for each stateful task, ranked by their overall lag
     */
    static <ID extends Comparable<ID>> SortedMap<TaskId, SortedSet<RankedClient<ID>>> buildClientRankingsByTask(final Map<ID, ClientState> clientStates,
                                                                                                                final Set<TaskId> statefulTasks,
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
     * previous assignment will be chosen iff both of the following are true:
     *   1) it satisfies the state constraint, ie all tasks with caught up clients are assigned to one of those clients
     *   2) it satisfies the balance factor
     */
    private boolean shouldUsePreviousAssignment() {
        final boolean previousAssignmentIsValid = completePreviousAssignment();
        if (previousAssignmentIsValid) {
            return computePreviousAssignmentBalanceFactor() <= configs.balanceFactor;
        } else {
            return false;
        }
    }

    private static <ID> Map<ID, Integer> getClientsToNumberOfThreads(final Map<ID, ClientState> clientStates) {
        return clientStates.entrySet().stream().collect(Collectors.toMap(
            Entry::getKey,
            client -> client.getValue().capacity())
        );
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

    private Map<ID, List<TaskId>> getEmptyTaskAssignmentMap() {
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