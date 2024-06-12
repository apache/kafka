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

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import static org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask.Type.ACTIVE;
import static org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask.Type.STANDBY;
import static org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.DEFAULT_HIGH_AVAILABILITY_NON_OVERLAP_COST;
import static org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.DEFAULT_HIGH_AVAILABILITY_TRAFFIC_COST;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils.RackAwareOptimizationParams;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.assignment.ConstrainedPrioritySet;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HighAvailabilityTaskAssignor implements TaskAssignor {
    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityTaskAssignor.class);

    @Override
    public TaskAssignment assign(final ApplicationState applicationState) {
        final Map<ProcessId, KafkaStreamsState> clientStates =
            applicationState.kafkaStreamsStates(true);
        final Map<ProcessId, KafkaStreamsAssignment> assignments = clientStates.keySet().stream()
            .map(processId -> KafkaStreamsAssignment.of(processId, new HashSet<>()))
            .collect(Collectors.toMap(
                KafkaStreamsAssignment::processId,
                Function.identity()
            ));
        assignActiveStatefulTasks(applicationState, clientStates, assignments);
        assignStandbyReplicaTasks(applicationState, clientStates, assignments);

        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag =
            tasksToClientByLag(applicationState, clientStates);
        final AtomicInteger remainingWarmupReplicas =
            new AtomicInteger(applicationState.assignmentConfigs().maxWarmupReplicas());
        final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients =
            tasksToCaughtUpClients(applicationState, clientStates);

        // We temporarily need to know which standby tasks were intended as warmups
        // for active tasks, so that we don't move them (again) when we plan standby
        // task movements. We can then immediately treat warmups exactly the same as
        // hot-standby replicas, so we just track it right here as metadata, rather
        // than add "warmup" assignments to ClientState, for example.
        final Map<ProcessId, Set<TaskId>> warmupAssignments = new TreeMap<>();
        final int neededActiveTaskMovements = assignActiveTaskMovements(
            tasksToCaughtUpClients,
            tasksToClientByLag,
            clientStates,
            assignments,
            warmupAssignments,
            remainingWarmupReplicas
        );

        final int neededStandbyTaskMovements = assignStandbyTaskMovements(
            tasksToCaughtUpClients,
            tasksToClientByLag,
            clientStates,
            assignments,
            remainingWarmupReplicas,
            warmupAssignments
        );

        assignStatelessActiveTasks(applicationState, clientStates, assignments);
        final boolean probingRebalanceNeeded = neededActiveTaskMovements + neededStandbyTaskMovements > 0;
        if (probingRebalanceNeeded) {
            final ProcessId clientId = assignments.entrySet().iterator().next().getKey();
            final KafkaStreamsAssignment previousAssignment = assignments.get(clientId);
            assignments.put(clientId, previousAssignment.withFollowupRebalance(Instant.ofEpochMilli(0)));
        }

        LOG.info("Decided on assignment: " + assignments);
        return new TaskAssignment(assignments.values());
    }

    private static void assignActiveStatefulTasks(final ApplicationState applicationState,
                                                  final Map<ProcessId, KafkaStreamsState> clientStates,
                                                  final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        final Set<TaskId> statefulTasks = applicationState.allTasks().values().stream()
            .filter(TaskInfo::isStateful)
            .map(TaskInfo::id)
            .collect(Collectors.toSet());

        Iterator<ProcessId> clientStateIterator = clientStates.keySet().iterator();
        for (final TaskId task : statefulTasks) {
            if (!clientStateIterator.hasNext()) {
                clientStateIterator = assignments.keySet().iterator();
            }
            final ProcessId designatedClient = clientStateIterator.next();
            assignments.get(designatedClient).assignTask(
                new AssignedTask(task, ACTIVE)
            );
        }

        balanceTasksOverThreads(
            applicationState,
            clientStates,
            assignments,
            ACTIVE
        );

        final RackAwareOptimizationParams statefulTaskParams =
            RackAwareOptimizationParams.of(applicationState)
                .withTrafficCostOverride(
                    applicationState.assignmentConfigs()
                        .rackAwareTrafficCost()
                        .orElse(DEFAULT_HIGH_AVAILABILITY_TRAFFIC_COST)
                )
                .withNonOverlapCostOverride(
                    applicationState.assignmentConfigs()
                        .rackAwareNonOverlapCost()
                        .orElse(DEFAULT_HIGH_AVAILABILITY_NON_OVERLAP_COST)
                )
                .forStatefulTasks();
        TaskAssignmentUtils.optimizeRackAwareActiveTasks(statefulTaskParams, assignments);
    }

    private static void assignStandbyReplicaTasks(
        final ApplicationState applicationState,
        final Map<ProcessId, KafkaStreamsState> clientStates,
        final Map<ProcessId, KafkaStreamsAssignment> assignments
    ) {
        if (applicationState.assignmentConfigs().numStandbyReplicas() <= 0) {
            return;
        }

        TaskAssignmentUtils.defaultStandbyTaskAssignment(applicationState, assignments);
        balanceTasksOverThreads(
            applicationState,
            clientStates,
            assignments,
            STANDBY
        );

        final RackAwareOptimizationParams optimizationParams =
            RackAwareOptimizationParams.of(applicationState)
                .withTrafficCostOverride(
                    applicationState.assignmentConfigs()
                        .rackAwareTrafficCost()
                        .orElse(DEFAULT_HIGH_AVAILABILITY_TRAFFIC_COST)
                )
                .withNonOverlapCostOverride(
                    applicationState.assignmentConfigs()
                        .rackAwareNonOverlapCost()
                        .orElse(DEFAULT_HIGH_AVAILABILITY_NON_OVERLAP_COST)
                );
        TaskAssignmentUtils.optimizeRackAwareStandbyTasks(optimizationParams, assignments);
    }

    private static void assignStatelessActiveTasks(
        final ApplicationState applicationState,
        final Map<ProcessId, KafkaStreamsState> clientStates,
        final Map<ProcessId, KafkaStreamsAssignment> assignments
    ) {
        final Set<TaskId> statelessTasks = applicationState.allTasks().values().stream()
            .filter(task -> !task.isStateful())
            .map(TaskInfo::id)
            .collect(Collectors.toSet());

        final ConstrainedPrioritySet statelessActiveTaskClientsByTaskLoad =
            new ConstrainedPrioritySet(
                (client, task) -> true,
                client -> {
                    final long numActiveTasks = assignments.get(client).tasks().values().stream()
                        .filter(task -> task.type() == ACTIVE)
                        .count();
                    return (double) numActiveTasks / (double) clientStates.get(client)
                        .numProcessingThreads();
                }
            );
        statelessActiveTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        for (final TaskId task : statelessTasks) {
            final ProcessId client = statelessActiveTaskClientsByTaskLoad.poll(task);
            assignments.get(client).assignTask(new AssignedTask(task, ACTIVE));
            statelessActiveTaskClientsByTaskLoad.offer(client);
        }

        TaskAssignmentUtils.optimizeRackAwareActiveTasks(
            RackAwareOptimizationParams.of(applicationState)
                .forStatelessTasks()
                .withTrafficCostOverride(RackAwareTaskAssignor.STATELESS_TRAFFIC_COST)
                .withNonOverlapCostOverride(RackAwareTaskAssignor.STATELESS_NON_OVERLAP_COST),
            assignments
        );
    }

    private static void balanceTasksOverThreads(final ApplicationState applicationState,
                                                final Map<ProcessId, KafkaStreamsState> clientStates,
                                                final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                final AssignedTask.Type taskType) {
        boolean keepBalancing = true;
        while (keepBalancing) {
            keepBalancing = false;
            for (final KafkaStreamsAssignment sourceAssignment : assignments.values()) {
                for (final KafkaStreamsAssignment destinationAssignment : assignments.values()) {
                    if (sourceAssignment.processId().equals(destinationAssignment.processId())) {
                        continue;
                    }
                    final boolean canClientsSwapTasks = canClientsSwapTask(
                        applicationState,
                        clientStates.get(sourceAssignment.processId()),
                        clientStates.get(destinationAssignment.processId())
                    );
                    if (canClientsSwapTasks) {
                        keepBalancing |= tryTaskSwaps(
                            clientStates.get(sourceAssignment.processId()),
                            clientStates.get(destinationAssignment.processId()),
                            sourceAssignment,
                            destinationAssignment,
                            taskType
                        );
                    }
                }
            }
        }
    }

    private static boolean tryTaskSwaps(final KafkaStreamsState sourceState,
                                        final KafkaStreamsState destinationState,
                                        final KafkaStreamsAssignment sourceAssignment,
                                        final KafkaStreamsAssignment destinationAssignment,
                                        final AssignedTask.Type taskType) {
        boolean keepBalancing = false;
        final Set<TaskId> sourceTasks = sourceAssignment.tasks().values().stream()
            .filter(task -> task.type() == taskType)
            .map(AssignedTask::id)
            .collect(Collectors.toSet());
        for (final TaskId taskToMove : sourceTasks) {
            final boolean shouldMoveATask = shouldMoveATask(
                sourceState,
                destinationState,
                sourceAssignment,
                destinationAssignment
            );
            if (!shouldMoveATask) {
                break;
            }
            final boolean destinationHasTask =
                destinationAssignment.tasks().containsKey(taskToMove);
            if (!destinationHasTask) {
                sourceAssignment.removeTask(new AssignedTask(taskToMove, taskType));
                destinationAssignment.assignTask(new AssignedTask(taskToMove, taskType));
                keepBalancing = true;
            }
        }
        return keepBalancing;
    }

    private static boolean canClientsSwapTask(
        final ApplicationState applicationState,
        final KafkaStreamsState source,
        final KafkaStreamsState destination
    ) {
        if (applicationState.assignmentConfigs().rackAwareAssignmentTags().isEmpty()) {
            return true;
        }

        final Map<String, String> sourceClientTags = source.clientTags();
        final Map<String, String> destinationClientTags = destination.clientTags();
        for (final Map.Entry<String, String> sourceClientTagEntry : sourceClientTags.entrySet()) {
            if (!sourceClientTagEntry.getValue()
                .equals(destinationClientTags.get(sourceClientTagEntry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static boolean shouldMoveATask(final KafkaStreamsState sourceState,
                                           final KafkaStreamsState destinationState,
                                           final KafkaStreamsAssignment sourceAssignment,
                                           final KafkaStreamsAssignment destinationAssignment) {
        final double sourceLoad =
            (double) sourceAssignment.tasks().size() / (double) sourceState.numProcessingThreads();
        final double destinationLoad = (double) destinationAssignment.tasks().size()
                                       / (double) destinationState.numProcessingThreads();
        final double skew = sourceLoad - destinationLoad;

        if (skew <= 0) {
            return false;
        }

        final double proposedAssignedTasksPerStreamThreadAtDestination =
            (destinationAssignment.tasks().size() + 1.0) / destinationState.numProcessingThreads();
        final double proposedAssignedTasksPerStreamThreadAtSource =
            (sourceAssignment.tasks().size() - 1.0) / sourceState.numProcessingThreads();
        final double proposedSkew = proposedAssignedTasksPerStreamThreadAtSource
                                    - proposedAssignedTasksPerStreamThreadAtDestination;

        if (proposedSkew < 0) {
            // then the move would only create an imbalance in the other direction.
            return false;
        }
        // we should only move a task if doing so would actually improve the skew.
        return proposedSkew < skew;
    }

    private static Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag(final ApplicationState applicationState,
                                                                        final Map<ProcessId, KafkaStreamsState> clientStates) {
        final Set<TaskId> statefulTasks = applicationState.allTasks().values().stream()
            .filter(TaskInfo::isStateful)
            .map(TaskInfo::id)
            .collect(Collectors.toSet());

        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = new HashMap<>();
        for (final TaskId task : statefulTasks) {
            final SortedSet<ProcessId> clientLag =
                new TreeSet<>(Comparator.<ProcessId>comparingLong(a ->
                    clientStates.get(a).lagFor(task)).thenComparing(a -> a));
            clientLag.addAll(clientStates.keySet());
            tasksToClientByLag.put(task, clientLag);
        }
        return tasksToClientByLag;
    }

    private static Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients(
        final ApplicationState applicationState,
        final Map<ProcessId, KafkaStreamsState> clientStates
    ) {
        final Set<TaskId> statefulTasks = applicationState.allTasks().values().stream()
            .filter(TaskInfo::isStateful)
            .map(TaskInfo::id)
            .collect(Collectors.toSet());

        final long acceptableRecoveryLag =
            applicationState.assignmentConfigs().acceptableRecoveryLag();

        final Map<TaskId, SortedSet<ProcessId>> taskToCaughtUpClients = new HashMap<>();
        for (final TaskId task : statefulTasks) {
            final TreeSet<ProcessId> caughtUpClients = new TreeSet<>();
            for (final KafkaStreamsState client : clientStates.values()) {
                final long taskLag = client.lagFor(task);
                if (activeRunning(taskLag) || unbounded(acceptableRecoveryLag) || acceptable(
                    acceptableRecoveryLag,
                    taskLag
                )) {
                    caughtUpClients.add(client.processId());
                }
            }
            taskToCaughtUpClients.put(task, caughtUpClients);
        }

        return taskToCaughtUpClients;
    }

    private static boolean unbounded(final long acceptableRecoveryLag) {
        return acceptableRecoveryLag == Long.MAX_VALUE;
    }

    private static boolean acceptable(final long acceptableRecoveryLag, final long taskLag) {
        return taskLag >= 0 && taskLag <= acceptableRecoveryLag;
    }

    private static boolean activeRunning(final long taskLag) {
        return taskLag == Task.LATEST_OFFSET;
    }

    private static final class TaskMovement {
        private final TaskId task;
        private final ProcessId destination;
        private final SortedSet<ProcessId> caughtUpClients;

        private TaskMovement(final TaskId task,
                             final ProcessId destination,
                             final SortedSet<ProcessId> caughtUpClients) {
            this.task = task;
            this.destination = destination;
            this.caughtUpClients = caughtUpClients;
        }

        private TaskId task() {
            return task;
        }

        private int numCaughtUpClients() {
            return caughtUpClients.size();
        }
    }

    private static boolean taskIsNotCaughtUpOnClientAndOtherMoreCaughtUpClientsExist(final TaskId task,
                                                                                     final ProcessId client,
                                                                                     final Map<ProcessId, KafkaStreamsState> clientStates,
                                                                                     final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients,
                                                                                     final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag) {
        final SortedSet<ProcessId> taskClients = requireNonNull(tasksToClientByLag.get(task), "uninitialized set");
        if (taskIsCaughtUpOnClient(task, client, tasksToCaughtUpClients)) {
            return false;
        }
        final long mostCaughtUpLag = clientStates.get(taskClients.first()).lagFor(task);
        final long clientLag = clientStates.get(client).lagFor(task);
        return mostCaughtUpLag < clientLag;
    }

    private static boolean taskIsCaughtUpOnClient(final TaskId task,
                                                  final ProcessId client,
                                                  final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients) {
        final Set<ProcessId> caughtUpClients = requireNonNull(tasksToCaughtUpClients.get(task), "uninitialized set");
        return caughtUpClients.contains(client);
    }

    static int assignActiveTaskMovements(final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients,
                                         final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag,
                                         final Map<ProcessId, KafkaStreamsState> clientStates,
                                         final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                         final Map<ProcessId, Set<TaskId>> warmups,
                                         final AtomicInteger remainingWarmupReplicas) {
        final BiFunction<ProcessId, TaskId, Boolean> caughtUpPredicate =
            (client, task) -> taskIsCaughtUpOnClient(task, client, tasksToCaughtUpClients);

        final ConstrainedPrioritySet caughtUpClientsByTaskLoad = new ConstrainedPrioritySet(
            caughtUpPredicate,
            client -> {
                final long numActiveTasks = assignments.get(client).tasks().size();
                return (double) numActiveTasks / (double) clientStates.get(client).numProcessingThreads();
            }
        );

        final Queue<TaskMovement>
            taskMovements = new PriorityQueue<>(
            Comparator.comparing(TaskMovement::numCaughtUpClients).thenComparing(
                TaskMovement::task)
        );

        for (final Map.Entry<ProcessId, KafkaStreamsState> clientStateEntry : clientStates.entrySet()) {
            final ProcessId client = clientStateEntry.getKey();
            final KafkaStreamsState state = clientStateEntry.getValue();
            final Set<TaskId> activeTasks = assignments.get(client).tasks().values().stream()
                .filter(task -> task.type() == ACTIVE)
                .map(AssignedTask::id)
                .collect(Collectors.toSet());
            for (final TaskId task : activeTasks) {
                // if the desired client is not caught up, and there is another client that _is_ more caught up, then
                // we schedule a movement, so we can move the active task to a more caught-up client. We'll try to
                // assign a warm-up to the desired client so that we can move it later on.
                if (taskIsNotCaughtUpOnClientAndOtherMoreCaughtUpClientsExist(task, client, clientStates, tasksToCaughtUpClients, tasksToClientByLag)) {
                    taskMovements.add(new TaskMovement(
                        task,
                        client,
                        tasksToCaughtUpClients.get(task)
                    ));
                }
            }
            caughtUpClientsByTaskLoad.offer(client);
        }

        final int movementsNeeded = taskMovements.size();

        while (!taskMovements.isEmpty()) {
            final TaskMovement movement = taskMovements.poll();
            // Attempt to find a caught up standby, otherwise find any caught up client, failing that use the most
            // caught up client.
            final boolean moved = tryToSwapStandbyAndActiveOnCaughtUpClient(assignments, caughtUpClientsByTaskLoad, movement) ||
                                  tryToMoveActiveToCaughtUpClientAndTryToWarmUp(assignments, warmups, remainingWarmupReplicas, caughtUpClientsByTaskLoad, movement) ||
                                  tryToMoveActiveToMostCaughtUpClient(tasksToClientByLag, assignments, warmups, remainingWarmupReplicas, caughtUpClientsByTaskLoad, movement);

            if (!moved) {
                throw new IllegalStateException("Tried to move task to more caught-up client as scheduled before but none exist");
            }
        }

        return movementsNeeded;
    }

    static int assignStandbyTaskMovements(final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients,
                                          final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag,
                                          final Map<ProcessId, KafkaStreamsState> clientStates,
                                          final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                          final AtomicInteger remainingWarmupReplicas,
                                          final Map<ProcessId, Set<TaskId>> warmups) {
        final BiFunction<ProcessId, TaskId, Boolean> caughtUpPredicate =
            (client, task) -> taskIsCaughtUpOnClient(task, client, tasksToCaughtUpClients);

        final ConstrainedPrioritySet caughtUpClientsByTaskLoad = new ConstrainedPrioritySet(
            caughtUpPredicate,
            client -> {
                final long numActiveTasks = assignments.get(client).tasks().size();
                return (double) numActiveTasks / (double) clientStates.get(client).numProcessingThreads();
            }
        );

        final Queue<TaskMovement> taskMovements = new PriorityQueue<>(
            Comparator.comparing(TaskMovement::numCaughtUpClients).thenComparing(
                TaskMovement::task)
        );

        for (final Map.Entry<ProcessId, KafkaStreamsState> clientStateEntry : clientStates.entrySet()) {
            final ProcessId destination = clientStateEntry.getKey();
            final Set<TaskId> standbyTasks = assignments.get(destination).tasks().values().stream()
                .filter(task -> task.type() == STANDBY)
                .map(AssignedTask::id)
                .collect(Collectors.toSet());
            for (final TaskId task : standbyTasks) {
                if (warmups.getOrDefault(destination, Collections.emptySet()).contains(task)) {
                    // this is a warmup, so we won't move it.
                } else if (taskIsNotCaughtUpOnClientAndOtherMoreCaughtUpClientsExist(task, destination, clientStates, tasksToCaughtUpClients, tasksToClientByLag)) {
                    // if the desired client is not caught up, and there is another client that _is_ more caught up, then
                    // we schedule a movement, so we can move the active task to the caught-up client. We'll try to
                    // assign a warm-up to the desired client so that we can move it later on.
                    taskMovements.add(new TaskMovement(
                        task,
                        destination,
                        tasksToCaughtUpClients.get(task)
                    ));
                }
            }
            caughtUpClientsByTaskLoad.offer(destination);
        }

        int movementsNeeded = 0;

        while (!taskMovements.isEmpty()) {
            final TaskMovement movement = taskMovements.poll();
            final Function<ProcessId, Boolean> eligibleClientPredicate = clientId -> {
                return !assignments.get(clientId).tasks().containsKey(movement.task);
            };
            ProcessId sourceClient = caughtUpClientsByTaskLoad.poll(
                movement.task,
                eligibleClientPredicate
            );

            if (sourceClient == null) {
                sourceClient = mostCaughtUpEligibleClient(tasksToClientByLag, eligibleClientPredicate, movement.task, movement.destination);
            }

            if (sourceClient == null) {
                // then there's no caught-up client that doesn't already have a copy of this task, so there's
                // nowhere to move it.
            } else {
                moveStandbyAndTryToWarmUp(
                    remainingWarmupReplicas,
                    movement.task,
                    assignments.get(sourceClient),
                    assignments.get(movement.destination)
                );
                caughtUpClientsByTaskLoad.offerAll(asList(sourceClient, movement.destination));
                movementsNeeded++;
            }
        }

        return movementsNeeded;
    }

    private static boolean tryToSwapStandbyAndActiveOnCaughtUpClient(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                                     final ConstrainedPrioritySet caughtUpClientsByTaskLoad,
                                                                     final TaskMovement movement) {
        final ProcessId caughtUpStandbySourceClient = caughtUpClientsByTaskLoad.poll(
            movement.task,
            clientId -> {
                return assignments.get(clientId).tasks().containsKey(movement.task)
                    && assignments.get(clientId).tasks().get(movement.task).type() == STANDBY;
            }
        );
        if (caughtUpStandbySourceClient != null) {
            swapStandbyAndActive(
                movement.task,
                assignments.get(caughtUpStandbySourceClient),
                assignments.get(movement.destination)
            );
            caughtUpClientsByTaskLoad.offerAll(asList(caughtUpStandbySourceClient, movement.destination));
            return true;
        }
        return false;
    }

    private static boolean tryToMoveActiveToCaughtUpClientAndTryToWarmUp(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                                         final Map<ProcessId, Set<TaskId>> warmups,
                                                                         final AtomicInteger remainingWarmupReplicas,
                                                                         final ConstrainedPrioritySet caughtUpClientsByTaskLoad,
                                                                         final TaskMovement movement) {
        final ProcessId caughtUpSourceClient = caughtUpClientsByTaskLoad.poll(movement.task);
        if (caughtUpSourceClient != null) {
            moveActiveAndTryToWarmUp(
                remainingWarmupReplicas,
                movement.task,
                assignments.get(caughtUpSourceClient),
                assignments.get(movement.destination),
                warmups.computeIfAbsent(movement.destination, x -> new TreeSet<>())
            );
            caughtUpClientsByTaskLoad.offerAll(asList(caughtUpSourceClient, movement.destination));
            return true;
        }
        return false;
    }

    private static boolean tryToMoveActiveToMostCaughtUpClient(final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag,
                                                               final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                               final Map<ProcessId, Set<TaskId>> warmups,
                                                               final AtomicInteger remainingWarmupReplicas,
                                                               final ConstrainedPrioritySet caughtUpClientsByTaskLoad,
                                                               final TaskMovement movement) {
        final ProcessId mostCaughtUpSourceClient = mostCaughtUpEligibleClient(tasksToClientByLag, movement.task, movement.destination);
        if (mostCaughtUpSourceClient == null) {
            return false;
        }

        final boolean hasStandbyTask = assignments.get(mostCaughtUpSourceClient).tasks().containsKey(movement.task)
               && assignments.get(mostCaughtUpSourceClient).tasks().get(movement.task).type() == STANDBY;
        if (hasStandbyTask) {
            swapStandbyAndActive(
                movement.task,
                assignments.get(mostCaughtUpSourceClient),
                assignments.get(movement.destination)
            );
        } else {
            moveActiveAndTryToWarmUp(
                remainingWarmupReplicas,
                movement.task,
                assignments.get(mostCaughtUpSourceClient),
                assignments.get(movement.destination),
                warmups.computeIfAbsent(movement.destination, x -> new TreeSet<>())
            );
        }
        caughtUpClientsByTaskLoad.offerAll(asList(mostCaughtUpSourceClient, movement.destination));
        return true;
    }

    private static void moveActiveAndTryToWarmUp(final AtomicInteger remainingWarmupReplicas,
                                                 final TaskId task,
                                                 final KafkaStreamsAssignment sourceAssignment,
                                                 final KafkaStreamsAssignment destinationAssignment,
                                                 final Set<TaskId> warmups) {
        sourceAssignment.assignTask(new AssignedTask(task, ACTIVE));

        if (remainingWarmupReplicas.getAndDecrement() > 0) {
            destinationAssignment.removeTask(new AssignedTask(task, ACTIVE));
            destinationAssignment.assignTask(new AssignedTask(task, STANDBY));
            warmups.add(task);
        } else {
            // we have no more standbys or warmups to hand out, so we have to try and move it
            // to the destination in a follow-on rebalance
            destinationAssignment.removeTask(new AssignedTask(task, ACTIVE));
        }
    }

    private static void moveStandbyAndTryToWarmUp(final AtomicInteger remainingWarmupReplicas,
                                                  final TaskId task,
                                                  final KafkaStreamsAssignment sourceAssignment,
                                                  final KafkaStreamsAssignment destinationAssignment) {
        sourceAssignment.assignTask(new AssignedTask(task, STANDBY));

        if (remainingWarmupReplicas.getAndDecrement() > 0) {
            // Then we can leave it also assigned to the destination as a warmup
        } else {
            // we have no more warmups to hand out, so we have to try and move it
            // to the destination in a follow-on rebalance
            destinationAssignment.removeTask(new AssignedTask(task, STANDBY));
        }
    }

    private static void swapStandbyAndActive(final TaskId task,
                                             final KafkaStreamsAssignment sourceAssignment,
                                             final KafkaStreamsAssignment destinationAssignment) {
        sourceAssignment.removeTask(new AssignedTask(task, STANDBY));
        sourceAssignment.assignTask(new AssignedTask(task, ACTIVE));
        destinationAssignment.removeTask(new AssignedTask(task, ACTIVE));
        destinationAssignment.assignTask(new AssignedTask(task, STANDBY));
    }

    private static ProcessId mostCaughtUpEligibleClient(final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag,
                                                        final TaskId task,
                                                        final ProcessId destinationClient) {
        return mostCaughtUpEligibleClient(tasksToClientByLag, client -> true, task, destinationClient);
    }

    private static ProcessId mostCaughtUpEligibleClient(final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag,
                                                        final Function<ProcessId, Boolean> constraint,
                                                        final TaskId task,
                                                        final ProcessId destinationClient) {
        for (final ProcessId client : tasksToClientByLag.get(task)) {
            if (destinationClient.equals(client)) {
                break;
            } else if (constraint.apply(client)) {
                return client;
            }
        }
        return null;
    }
}