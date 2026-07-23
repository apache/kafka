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

package org.apache.kafka.streams.processor.assignment.assignors;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils.RackAwareOptimizationParams;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.apache.kafka.streams.processor.assignment.TaskTopicPartition;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

@InterfaceAudience.Public
public class StickyTaskAssignor implements TaskAssignor {
    private static final Logger LOG = LoggerFactory.getLogger(StickyTaskAssignor.class);

    public static final int DEFAULT_STICKY_TRAFFIC_COST = 1;
    public static final int DEFAULT_STICKY_NON_OVERLAP_COST = 10;

    private final boolean mustPreserveActiveTaskAssignment;

    public StickyTaskAssignor() {
        this(false);
    }

    public StickyTaskAssignor(final boolean mustPreserveActiveTaskAssignment) {
        this.mustPreserveActiveTaskAssignment = mustPreserveActiveTaskAssignment;
    }

    @Override
    public TaskAssignment assign(final ApplicationState applicationState) {
        final Map<ProcessId, KafkaStreamsState> clients = applicationState.kafkaStreamsStates(false);
        final Map<TaskId, ProcessId> previousActiveAssignment = mapPreviousActiveTasks(clients);
        final Map<TaskId, Set<ProcessId>> previousStandbyAssignment = mapPreviousStandbyTasks(clients);
        final AssignmentState assignmentState = new AssignmentState(applicationState, clients,
            previousActiveAssignment, previousStandbyAssignment);

        assignActive(applicationState, clients.values(), assignmentState, this.mustPreserveActiveTaskAssignment);
        optimizeActive(applicationState, assignmentState);
        // data is stale after optimizeActive and not used in the standby assignment
        assignmentState.currentActiveClientWeight.clear();
        // after optimizeActive, newAssignments might have updated, but not my map
        assignStandby(applicationState, assignmentState);
        optimizeStandby(applicationState, assignmentState);

        final Map<ProcessId, KafkaStreamsAssignment> finalAssignments = assignmentState.newAssignments;
        if (mustPreserveActiveTaskAssignment && !finalAssignments.isEmpty()) {
            // We set the followup deadline for only one of the clients.
            final ProcessId clientId = finalAssignments.entrySet().iterator().next().getKey();
            final KafkaStreamsAssignment previousAssignment = finalAssignments.get(clientId);
            finalAssignments.put(clientId, previousAssignment.withFollowupRebalance(Instant.ofEpochMilli(0)));
        }

        return new TaskAssignment(finalAssignments.values());
    }

    private void optimizeActive(final ApplicationState applicationState,
                                final AssignmentState assignmentState) {
        if (mustPreserveActiveTaskAssignment) {
            return;
        }

        final Map<ProcessId, KafkaStreamsAssignment> currentAssignments = assignmentState.newAssignments;

        final RackAwareOptimizationParams statefulTaskParams = RackAwareOptimizationParams.of(applicationState)
            .withTrafficCostOverride(
                applicationState.assignmentConfigs().rackAwareTrafficCost().orElse(DEFAULT_STICKY_TRAFFIC_COST)
            )
            .withNonOverlapCostOverride(
                applicationState.assignmentConfigs().rackAwareNonOverlapCost().orElse(DEFAULT_STICKY_NON_OVERLAP_COST)
            )
            .forStatefulTasks();
        TaskAssignmentUtils.optimizeRackAwareActiveTasks(statefulTaskParams, currentAssignments);

        TaskAssignmentUtils.optimizeRackAwareActiveTasks(
            RackAwareOptimizationParams.of(applicationState)
                .forStatelessTasks()
                .withTrafficCostOverride(RackAwareTaskAssignor.STATELESS_TRAFFIC_COST)
                .withNonOverlapCostOverride(RackAwareTaskAssignor.STATELESS_NON_OVERLAP_COST),
            currentAssignments
        );
        assignmentState.processOptimizedAssignments(currentAssignments);
    }

    private void optimizeStandby(final ApplicationState applicationState, final AssignmentState assignmentState) {
        if (applicationState.assignmentConfigs().numStandbyReplicas() <= 0) {
            return;
        }

        if (mustPreserveActiveTaskAssignment) {
            return;
        }

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assignmentState.newAssignments;

        final RackAwareOptimizationParams optimizationParams = RackAwareOptimizationParams.of(applicationState)
            .withTrafficCostOverride(
                applicationState.assignmentConfigs().rackAwareTrafficCost().orElse(DEFAULT_STICKY_TRAFFIC_COST)
            )
            .withNonOverlapCostOverride(
                applicationState.assignmentConfigs().rackAwareNonOverlapCost().orElse(DEFAULT_STICKY_NON_OVERLAP_COST)
            );
        TaskAssignmentUtils.optimizeRackAwareStandbyTasks(optimizationParams, assignments);
        assignmentState.processOptimizedAssignments(assignments);
    }

    private static void assignActive(final ApplicationState applicationState,
                                     final Collection<KafkaStreamsState> clients,
                                     final AssignmentState assignmentState,
                                     final boolean mustPreserveActiveTaskAssignment) {
        final Set<TaskId> allTaskIds = applicationState.allTasks().keySet();
        final Set<TaskId> unassigned = new HashSet<>(allTaskIds);

        // first try and re-assign existing active tasks to clients that previously had
        // the same active task
        for (final TaskId taskId : assignmentState.previousActiveAssignment.keySet()) {
            if (allTaskIds.contains(taskId)) {
                final ProcessId previousClientForTask = assignmentState.previousActiveAssignment.get(taskId);
                if (mustPreserveActiveTaskAssignment || assignmentState.hasRoomForActiveTask(previousClientForTask, taskId)) {
                    assignmentState.finalizeAssignment(taskId, previousClientForTask, AssignedTask.Type.ACTIVE);
                    assignmentState.updateClientWeightMap(previousClientForTask, taskId);
                    unassigned.remove(taskId);

                }
            }
        }

        // try and assign any remaining unassigned tasks to clients that previously
        // have seen the task.
        for (final Iterator<TaskId> iterator = unassigned.iterator(); iterator.hasNext(); ) {
            final TaskId taskId = iterator.next();
            final Set<ProcessId> previousClientsForStandbyTask = assignmentState.previousStandbyAssignment.getOrDefault(taskId, new HashSet<>());
            for (final ProcessId client: previousClientsForStandbyTask) {
                if (assignmentState.hasRoomForActiveTask(client, taskId)) {
                    assignmentState.finalizeAssignment(taskId, client, AssignedTask.Type.ACTIVE);
                    assignmentState.updateClientWeightMap(client, taskId);
                    iterator.remove();
                    break;
                }
            }
        }

//        would prefer to assign remaining tasks from highest to lowest partition count
//        final List<TaskId> sortedTasks = assignmentState.taskInputPartitionCount.entrySet()
//                .stream().filter(entry -> unassigned.contains(entry.getKey()))
//                .sorted(Map.Entry.<TaskId, Integer>comparingByValue().reversed())
//                .map(Map.Entry::getKey)
//                .collect(Collectors.toList());
        final List<TaskId> sortedTasks = new ArrayList<>(unassigned);
        Collections.sort(sortedTasks);

        final Set<ProcessId> candidateClients = clients.stream()
                .map(KafkaStreamsState::processId)
                .collect(Collectors.toSet());
        for (final TaskId taskId : sortedTasks) {
            final ProcessId bestClient = assignmentState.findBestClientForTask(taskId, candidateClients, assignmentState::clientLoadPartitions, assignmentState::shouldBalanceLoadPartitions);
            assignmentState.finalizeAssignment(taskId, bestClient, AssignedTask.Type.ACTIVE);
            assignmentState.updateClientWeightMap(bestClient, taskId);
        }
    }

    private static void assignStandby(final ApplicationState applicationState,
                                      final AssignmentState assignmentState) {
        final Set<TaskInfo> statefulTasks = applicationState.allTasks().values().stream()
            .filter(taskInfo -> taskInfo.topicPartitions().stream().anyMatch(TaskTopicPartition::isChangelog))
            .collect(Collectors.toSet());
        final int numStandbyReplicas = applicationState.assignmentConfigs().numStandbyReplicas();
        for (final TaskInfo task : statefulTasks) {
            for (int i = 0; i < numStandbyReplicas; i++) {
                final Set<ProcessId> candidateClients = assignmentState.findClientsWithoutAssignedTask(task.id());
                if (candidateClients.isEmpty()) {
                    LOG.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                            "There is not enough available capacity. You should " +
                            "increase the number of threads and/or application instances " +
                            "to maintain the requested number of standby replicas.",
                        numStandbyReplicas - i,
                        numStandbyReplicas, task.id());
                    break;
                }
                // use task count, not partitions for standby assignments
                final ProcessId bestClient = assignmentState.findBestClientForTask(task.id(), candidateClients, assignmentState::clientLoad, (t, u) -> assignmentState.shouldBalanceLoadTasks(t));
                assignmentState.finalizeAssignment(task.id(), bestClient, AssignedTask.Type.STANDBY);
            }
        }
    }

    private static Map<TaskId, ProcessId> mapPreviousActiveTasks(final Map<ProcessId, KafkaStreamsState> clients) {
        final Map<TaskId, ProcessId> previousActiveTasks = new HashMap<>();
        for (final KafkaStreamsState client : clients.values()) {
            for (final TaskId taskId : client.previousActiveTasks()) {
                previousActiveTasks.put(taskId, client.processId());
            }
        }
        return previousActiveTasks;
    }

    private static Map<TaskId, Set<ProcessId>> mapPreviousStandbyTasks(final Map<ProcessId, KafkaStreamsState> clients) {
        final Map<TaskId, Set<ProcessId>> previousStandbyTasks = new HashMap<>();
        for (final KafkaStreamsState client : clients.values()) {
            for (final TaskId taskId : client.previousStandbyTasks()) {
                previousStandbyTasks.computeIfAbsent(taskId, k -> new HashSet<>());
                previousStandbyTasks.get(taskId).add(client.processId());
            }
        }
        return previousStandbyTasks;
    }

    private static class AssignmentState {
        private final Map<ProcessId, KafkaStreamsState> clients;
        private final Map<TaskId, ProcessId> previousActiveAssignment;
        private final Map<TaskId, Set<ProcessId>> previousStandbyAssignment;
        private final Map<TaskId, Integer> taskInputPartitionCount;
        private final Map<ProcessId, Integer> currentActiveClientWeight;
        private final int fairPartitionsPerClientThread;
        private final int averageTaskWeight;

        private final TaskPairs taskPairs;

        private Map<TaskId, Set<ProcessId>> newTaskLocations;
        private Map<ProcessId, KafkaStreamsAssignment> newAssignments;

        private AssignmentState(final ApplicationState applicationState,
                                final Map<ProcessId, KafkaStreamsState> clients,
                                final Map<TaskId, ProcessId> previousActiveAssignment,
                                final Map<TaskId, Set<ProcessId>> previousStandbyAssignment) {
            this.clients = clients;
            this.previousActiveAssignment = unmodifiableMap(previousActiveAssignment);
            this.previousStandbyAssignment = unmodifiableMap(previousStandbyAssignment);
            this.currentActiveClientWeight = new HashMap<>();
            this.taskInputPartitionCount = calculateInputPartitionsPerTask(applicationState.allTasks());

            // task weight is partition count
            final int totalPartitionCount = this.taskInputPartitionCount.values().stream().mapToInt(Integer::intValue).sum();
            final int totalNumberOfThreads = clients.values().stream().mapToInt(KafkaStreamsState::numProcessingThreads).sum();
            this.fairPartitionsPerClientThread = totalPartitionCount / totalNumberOfThreads;

            final int taskCount = applicationState.allTasks().size();
            final int safeTaskCount = taskCount == 0 ? 1 : taskCount;

            this.averageTaskWeight = Math.max(totalPartitionCount / safeTaskCount, 1);

            final int maxPairs = taskCount * (taskCount - 1) / 2;
            this.taskPairs = new TaskPairs(maxPairs);

            this.newTaskLocations = previousActiveAssignment.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), taskId -> new HashSet<>()));
            this.newAssignments = clients.values().stream().collect(Collectors.toMap(
                KafkaStreamsState::processId,
                state -> KafkaStreamsAssignment.of(state.processId(), new HashSet<>())
            ));
        }

        private void finalizeAssignment(final TaskId taskId, final ProcessId client, final AssignedTask.Type type) {
            final Set<TaskId> newAssignmentsForClient = newAssignments.get(client).tasks().keySet();
            taskPairs.addPairs(taskId, newAssignmentsForClient);

            newAssignments.get(client).assignTask(new AssignedTask(taskId, type));
            newTaskLocations.computeIfAbsent(taskId, k -> new HashSet<>()).add(client);
        }

        private void processOptimizedAssignments(final Map<ProcessId, KafkaStreamsAssignment> optimizedAssignments) {
            final Map<TaskId, Set<ProcessId>> newTaskLocations = new HashMap<>();

            for (final Map.Entry<ProcessId, KafkaStreamsAssignment> entry : optimizedAssignments.entrySet()) {
                final ProcessId processId = entry.getKey();
                final Set<AssignedTask> assignedTasks = new HashSet<>(optimizedAssignments.get(processId).tasks().values());

                for (final AssignedTask task : assignedTasks) {
                    newTaskLocations.computeIfAbsent(task.id(), k -> new HashSet<>()).add(processId);
                }
            }

            this.newTaskLocations = newTaskLocations;
            this.newAssignments = optimizedAssignments;
        }

        private boolean hasRoomForActiveTask(final ProcessId processId, final TaskId taskId) {

            final int capacity = clients.get(processId).numProcessingThreads();
            final int currentClientPartitionWeight = this.currentActiveClientWeight.getOrDefault(processId, 0);
            final int addedTaskWeight = taskInputPartitionCount.getOrDefault(taskId, 1);

            // compare absolute weight of current client with best case distribution plus buffer
            return currentClientPartitionWeight + addedTaskWeight < fairPartitionsPerClientThread * capacity + averageTaskWeight;
        }

        private ProcessId findBestClientForTask(final TaskId taskId, final Set<ProcessId> clientsWithin, final ToDoubleFunction<ProcessId> calculateLoad, final BiPredicate<ProcessId, TaskId> shouldBalance) {
            if (clientsWithin.size() == 1) {
                return clientsWithin.iterator().next();
            }

            final ProcessId previousClient = findLeastLoadedClientWithPreviousActiveOrStandbyTask(
                taskId, clientsWithin, calculateLoad);
            if (previousClient == null) {
                return findLeastLoadedClient(taskId, clientsWithin, calculateLoad);
            }

            if (shouldBalance.test(previousClient, taskId)) {
                final ProcessId standby = findLeastLoadedClientWithPreviousStandbyTask(taskId, clientsWithin, calculateLoad);
                if (standby == null || shouldBalance.test(standby, taskId)) {
                    return findLeastLoadedClient(taskId, clientsWithin, calculateLoad);
                }
                return standby;
            }
            return previousClient;
        }

        private Set<ProcessId> findClientsWithoutAssignedTask(final TaskId taskId) {
            final Set<ProcessId> unavailableClients = newTaskLocations.get(taskId);
            return clients.values().stream()
                .map(KafkaStreamsState::processId)
                .filter(o -> !unavailableClients.contains(o))
                .collect(Collectors.toSet());
        }

        private double clientLoad(final ProcessId processId) {
            final int capacity = clients.get(processId).numProcessingThreads();
            final double totalTaskCount = newAssignments.get(processId).tasks().size();
            return totalTaskCount / capacity;
        }

        private double clientLoadPartitions(final ProcessId processId) {
            final int capacity = clients.get(processId).numProcessingThreads();
            final double totalPartitionCount = currentActiveClientWeight.getOrDefault(processId, 0);
            return totalPartitionCount / capacity;
        }

        private Map<TaskId, Integer> calculateInputPartitionsPerTask(final Map<TaskId, TaskInfo> map) {
            final Map<TaskId, Integer> taskPartitionCount = new HashMap<>();
            for (final Map.Entry<TaskId, TaskInfo> entry : map.entrySet()) {
                int inputPartitionCount = 0;
                for (final TaskTopicPartition partition : entry.getValue().topicPartitions()) {

                    if (partition.isChangelog())
                        continue;
                    inputPartitionCount++;
                }
                taskPartitionCount.put(entry.getKey(), Math.max(1, inputPartitionCount));
            }
            return taskPartitionCount;
        }

        private void updateClientWeightMap(final ProcessId client, final TaskId taskId) {
            currentActiveClientWeight.merge(client, taskInputPartitionCount.getOrDefault(taskId, 1), Integer::sum);
        }

        private ProcessId findLeastLoadedClient(final TaskId taskId, final Set<ProcessId> clientIds, final ToDoubleFunction<ProcessId> calculateLoad) {
            ProcessId leastLoaded = null;
            double minLoad = Double.MAX_VALUE;

            ProcessId overallMinLoadClient = null;
            double minOverallLoad = Double.MAX_VALUE;

            for (final ProcessId processId : clientIds) {
                final double thisClientLoad = calculateLoad.applyAsDouble(processId);
                if (thisClientLoad == 0) {
                    return processId;
                }

                if (leastLoaded == null || thisClientLoad < minLoad) {
                    final Set<TaskId> assignedTasks = newAssignments.get(processId).tasks().values()
                        .stream().map(AssignedTask::id).collect(Collectors.toSet());
                    if (taskPairs.hasNewPair(taskId, assignedTasks)) {
                        leastLoaded = processId;
                        minLoad = thisClientLoad;
                    }
                }

                if (thisClientLoad < minOverallLoad) {
                    minOverallLoad = thisClientLoad;
                    overallMinLoadClient = processId;
                }
            }

            if (leastLoaded != null) {
                return leastLoaded;
            }

            return overallMinLoadClient;
        }

        private ProcessId findLeastLoadedClientWithPreviousActiveOrStandbyTask(final TaskId taskId,
                                                                               final Set<ProcessId> clientsWithin,
                                                                               final ToDoubleFunction<ProcessId> calculateLoad) {
            final ProcessId previous = previousActiveAssignment.get(taskId);
            if (previous != null && clientsWithin.contains(previous)) {
                return previous;
            }
            return findLeastLoadedClientWithPreviousStandbyTask(taskId, clientsWithin, calculateLoad);
        }

        private ProcessId findLeastLoadedClientWithPreviousStandbyTask(final TaskId taskId,
                                                                       final Set<ProcessId> clientsWithin,
                                                                       final ToDoubleFunction<ProcessId> calculateLoad) {
            final Set<ProcessId> ids = previousStandbyAssignment.getOrDefault(taskId, new HashSet<>());
            final HashSet<ProcessId> constrainTo = new HashSet<>(ids);
            constrainTo.retainAll(clientsWithin);
            return findLeastLoadedClient(taskId, constrainTo, calculateLoad);
        }

        private boolean shouldBalanceLoadPartitions(final ProcessId client, final TaskId taskId) {
            final double thisClientLoadPartition = clientLoadPartitions(client);
            final int clientCapacity = clients.get(client).numProcessingThreads();
            final int newTaskWeight = this.taskInputPartitionCount.get(taskId);
            // using absolute weights, so I can add the new task weight and buffer correctly
            if (thisClientLoadPartition * clientCapacity + newTaskWeight < fairPartitionsPerClientThread * clientCapacity + averageTaskWeight) {
                return false;
            }
            for (final ProcessId otherClient : clients.keySet()) {
                if (clientLoadPartitions(otherClient) < thisClientLoadPartition) {
                    return true;
                }
            }
            return false;
        }

        private boolean shouldBalanceLoadTasks(final ProcessId client) {
            final double thisClientLoad = clientLoad(client);
            if (thisClientLoad < 1) {
                return false;
            }

            for (final ProcessId otherClient : clients.keySet()) {
                if (clientLoad(otherClient) < thisClientLoad) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class TaskPairs {
        private final Set<TaskPair> pairs;
        private final int maxPairs;

        public TaskPairs(final int maxPairs) {
            this.maxPairs = maxPairs;
            this.pairs = new HashSet<>(maxPairs);
        }

        public boolean hasNewPair(final TaskId task1,
                                  final Set<TaskId> taskIds) {
            if (pairs.size() == maxPairs) {
                return false;
            }
            for (final TaskId taskId : taskIds) {
                if (!pairs.contains(pair(task1, taskId))) {
                    return true;
                }
            }
            return false;
        }

        public void addPairs(final TaskId taskId, final Set<TaskId> assigned) {
            for (final TaskId id : assigned) {
                pairs.add(pair(id, taskId));
            }
        }

        public TaskPair pair(final TaskId task1, final TaskId task2) {
            if (task1.compareTo(task2) < 0) {
                return new TaskPair(task1, task2);
            }
            return new TaskPair(task2, task1);
        }
    }

    private static class TaskPair {
        private final TaskId task1;
        private final TaskId task2;

        TaskPair(final TaskId task1, final TaskId task2) {
            this.task1 = task1;
            this.task2 = task2;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TaskPair pair = (TaskPair) o;
            return Objects.equals(task1, pair.task1) &&
                   Objects.equals(task2, pair.task2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task1, task2);
        }
    }
}
