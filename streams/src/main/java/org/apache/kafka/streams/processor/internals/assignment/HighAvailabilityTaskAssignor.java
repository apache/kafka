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
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.diff;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.assignActiveTaskMovements;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.assignStandbyTaskMovements;

public class HighAvailabilityTaskAssignor implements TaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(HighAvailabilityTaskAssignor.class);

    @Override
    public boolean assign(final Map<UUID, ClientState> clients,
                          final Set<TaskId> allTaskIds,
                          final Set<TaskId> statefulTaskIds,
                          final AssignmentConfigs configs) {
        final SortedSet<TaskId> statefulTasks = new TreeSet<>(statefulTaskIds);
        final TreeMap<UUID, ClientState> clientStates = new TreeMap<>(clients);

        assignActiveStatefulTasks(clientStates, statefulTasks);

        assignStandbyReplicaTasks(
            clientStates,
            allTaskIds,
            statefulTasks,
            configs
        );

        final AtomicInteger remainingWarmupReplicas = new AtomicInteger(configs.maxWarmupReplicas);

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = tasksToCaughtUpClients(
            statefulTasks,
            clientStates,
            configs.acceptableRecoveryLag
        );

        final Map<TaskId, SortedSet<UUID>> tasksToClientByLag = tasksToClientByLag(statefulTasks, clientStates);

        // We temporarily need to know which standby tasks were intended as warmups
        // for active tasks, so that we don't move them (again) when we plan standby
        // task movements. We can then immediately treat warmups exactly the same as
        // hot-standby replicas, so we just track it right here as metadata, rather
        // than add "warmup" assignments to ClientState, for example.
        final Map<UUID, Set<TaskId>> warmups = new TreeMap<>();

        final int neededActiveTaskMovements = assignActiveTaskMovements(
            tasksToCaughtUpClients,
            tasksToClientByLag,
            clientStates,
            warmups,
            remainingWarmupReplicas
        );

        final int neededStandbyTaskMovements = assignStandbyTaskMovements(
            tasksToCaughtUpClients,
            tasksToClientByLag,
            clientStates,
            remainingWarmupReplicas,
            warmups
        );

        assignStatelessActiveTasks(clientStates, diff(TreeSet::new, allTaskIds, statefulTasks));

        final boolean probingRebalanceNeeded = neededActiveTaskMovements + neededStandbyTaskMovements > 0;

        log.info("Decided on assignment: " +
                 clientStates +
                 " with" +
                 (probingRebalanceNeeded ? "" : " no") +
                 " followup probing rebalance.");

        return probingRebalanceNeeded;
    }

    private static void assignActiveStatefulTasks(final SortedMap<UUID, ClientState> clientStates,
                                                  final SortedSet<TaskId> statefulTasks) {
        Iterator<ClientState> clientStateIterator = null;
        for (final TaskId task : statefulTasks) {
            if (clientStateIterator == null || !clientStateIterator.hasNext()) {
                clientStateIterator = clientStates.values().iterator();
            }
            clientStateIterator.next().assignActive(task);
        }

        balanceTasksOverThreads(
            clientStates,
            ClientState::activeTasks,
            ClientState::unassignActive,
            ClientState::assignActive,
            (source, destination) -> true
        );
    }

    private void assignStandbyReplicaTasks(final TreeMap<UUID, ClientState> clientStates,
                                           final Set<TaskId> allTaskIds,
                                           final Set<TaskId> statefulTasks,
                                           final AssignmentConfigs configs) {
        if (configs.numStandbyReplicas == 0) {
            return;
        }

        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(configs);

        standbyTaskAssignor.assign(clientStates, allTaskIds, statefulTasks, configs);

        balanceTasksOverThreads(
            clientStates,
            ClientState::standbyTasks,
            ClientState::unassignStandby,
            ClientState::assignStandby,
            standbyTaskAssignor::isAllowedTaskMovement
        );
    }

    private static void balanceTasksOverThreads(final SortedMap<UUID, ClientState> clientStates,
                                                final Function<ClientState, Set<TaskId>> currentAssignmentAccessor,
                                                final BiConsumer<ClientState, TaskId> taskUnassignor,
                                                final BiConsumer<ClientState, TaskId> taskAssignor,
                                                final BiPredicate<ClientState, ClientState> taskMovementAttemptPredicate) {
        boolean keepBalancing = true;
        while (keepBalancing) {
            keepBalancing = false;
            for (final Map.Entry<UUID, ClientState> sourceEntry : clientStates.entrySet()) {
                final UUID sourceClient = sourceEntry.getKey();
                final ClientState sourceClientState = sourceEntry.getValue();

                for (final Map.Entry<UUID, ClientState> destinationEntry : clientStates.entrySet()) {
                    final UUID destinationClient = destinationEntry.getKey();
                    final ClientState destinationClientState = destinationEntry.getValue();
                    if (sourceClient.equals(destinationClient)) {
                        continue;
                    }

                    final Set<TaskId> sourceTasks = new TreeSet<>(currentAssignmentAccessor.apply(sourceClientState));
                    final Iterator<TaskId> sourceIterator = sourceTasks.iterator();
                    while (shouldMoveATask(sourceClientState, destinationClientState) && sourceIterator.hasNext()) {
                        final TaskId taskToMove = sourceIterator.next();
                        final boolean canMove = !destinationClientState.hasAssignedTask(taskToMove)
                                                // When ClientTagAwareStandbyTaskAssignor is used, we need to make sure that
                                                // sourceClient tags matches destinationClient tags.
                                                && taskMovementAttemptPredicate.test(sourceClientState, destinationClientState);
                        if (canMove) {
                            taskUnassignor.accept(sourceClientState, taskToMove);
                            taskAssignor.accept(destinationClientState, taskToMove);
                            keepBalancing = true;
                        }
                    }
                }
            }
        }
    }

    private static boolean shouldMoveATask(final ClientState sourceClientState,
                                           final ClientState destinationClientState) {
        final double skew = sourceClientState.assignedTaskLoad() - destinationClientState.assignedTaskLoad();

        if (skew <= 0) {
            return false;
        }

        final double proposedAssignedTasksPerStreamThreadAtDestination =
            (destinationClientState.assignedTaskCount() + 1.0) / destinationClientState.capacity();
        final double proposedAssignedTasksPerStreamThreadAtSource =
            (sourceClientState.assignedTaskCount() - 1.0) / sourceClientState.capacity();
        final double proposedSkew = proposedAssignedTasksPerStreamThreadAtSource - proposedAssignedTasksPerStreamThreadAtDestination;

        if (proposedSkew < 0) {
            // then the move would only create an imbalance in the other direction.
            return false;
        }
        // we should only move a task if doing so would actually improve the skew.
        return proposedSkew < skew;
    }

    private static void assignStatelessActiveTasks(final TreeMap<UUID, ClientState> clientStates,
                                                   final Iterable<TaskId> statelessTasks) {
        final ConstrainedPrioritySet statelessActiveTaskClientsByTaskLoad = new ConstrainedPrioritySet(
            (client, task) -> true,
            client -> clientStates.get(client).activeTaskLoad()
        );
        statelessActiveTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        for (final TaskId task : statelessTasks) {
            final UUID client = statelessActiveTaskClientsByTaskLoad.poll(task);
            final ClientState state = clientStates.get(client);
            state.assignActive(task);
            statelessActiveTaskClientsByTaskLoad.offer(client);
        }
    }

    private static Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients(final Set<TaskId> statefulTasks,
                                                                       final Map<UUID, ClientState> clientStates,
                                                                       final long acceptableRecoveryLag) {
        final Map<TaskId, SortedSet<UUID>> taskToCaughtUpClients = new HashMap<>();

        for (final TaskId task : statefulTasks) {
            final TreeSet<UUID> caughtUpClients = new TreeSet<>();
            for (final Map.Entry<UUID, ClientState> clientEntry : clientStates.entrySet()) {
                final UUID client = clientEntry.getKey();
                final long taskLag = clientEntry.getValue().lagFor(task);
                if (activeRunning(taskLag) || unbounded(acceptableRecoveryLag) || acceptable(acceptableRecoveryLag, taskLag)) {
                    caughtUpClients.add(client);
                }
            }
            taskToCaughtUpClients.put(task, caughtUpClients);
        }

        return taskToCaughtUpClients;
    }

    private static Map<TaskId, SortedSet<UUID>> tasksToClientByLag(final Set<TaskId> statefulTasks,
                                                              final Map<UUID, ClientState> clientStates) {
        final Map<TaskId, SortedSet<UUID>> tasksToClientByLag = new HashMap<>();
        for (final TaskId task : statefulTasks) {
            final SortedSet<UUID> clientLag = new TreeSet<>(Comparator.<UUID>comparingLong(a ->
                    clientStates.get(a).lagFor(task)).thenComparing(a -> a));
            clientLag.addAll(clientStates.keySet());
            tasksToClientByLag.put(task, clientLag);
        }
        return tasksToClientByLag;
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
}
