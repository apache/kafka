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

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

final class TaskMovement {
    private final TaskId task;
    private final UUID destination;
    private final SortedSet<UUID> caughtUpClients;

    private TaskMovement(final TaskId task, final UUID destination, final SortedSet<UUID> caughtUpClients) {
        this.task = task;
        this.destination = destination;
        this.caughtUpClients = caughtUpClients;

        if (caughtUpClients == null || caughtUpClients.isEmpty()) {
            throw new IllegalStateException("Should not attempt to move a task if no caught up clients exist");
        }
    }

    private TaskId task() {
        return task;
    }

    private int numCaughtUpClients() {
        return caughtUpClients.size();
    }

    private static boolean taskIsNotCaughtUpOnClientAndOtherCaughtUpClientsExist(final TaskId task,
                                                                                 final UUID client,
                                                                                 final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients) {
        return !taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(task, client, tasksToCaughtUpClients);
    }

    private static boolean taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(final TaskId task,
                                                                          final UUID client,
                                                                          final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients) {
        final Set<UUID> caughtUpClients = requireNonNull(tasksToCaughtUpClients.get(task), "uninitialized set");
        return caughtUpClients.isEmpty() || caughtUpClients.contains(client);
    }

    static int assignActiveTaskMovements(final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                         final Map<UUID, ClientState> clientStates,
                                         final Map<UUID, Set<TaskId>> warmups,
                                         final AtomicInteger remainingWarmupReplicas) {
        final BiFunction<UUID, TaskId, Boolean> caughtUpPredicate =
            (client, task) -> taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(task, client, tasksToCaughtUpClients);

        final ConstrainedPrioritySet caughtUpClientsByTaskLoad = new ConstrainedPrioritySet(
            caughtUpPredicate,
            client -> clientStates.get(client).assignedTaskLoad()
        );

        final Queue<TaskMovement> taskMovements = new PriorityQueue<>(
            Comparator.comparing(TaskMovement::numCaughtUpClients).thenComparing(TaskMovement::task)
        );

        for (final Map.Entry<UUID, ClientState> clientStateEntry : clientStates.entrySet()) {
            final UUID client = clientStateEntry.getKey();
            final ClientState state = clientStateEntry.getValue();
            for (final TaskId task : state.activeTasks()) {
                // if the desired client is not caught up, and there is another client that _is_ caught up, then
                // we schedule a movement, so we can move the active task to the caught-up client. We'll try to
                // assign a warm-up to the desired client so that we can move it later on.
                if (taskIsNotCaughtUpOnClientAndOtherCaughtUpClientsExist(task, client, tasksToCaughtUpClients)) {
                    taskMovements.add(new TaskMovement(task, client, tasksToCaughtUpClients.get(task)));
                }
            }
            caughtUpClientsByTaskLoad.offer(client);
        }

        final int movementsNeeded = taskMovements.size();

        for (final TaskMovement movement : taskMovements) {
            final UUID standbySourceClient = caughtUpClientsByTaskLoad.poll(
                movement.task,
                c -> clientStates.get(c).hasStandbyTask(movement.task)
            );
            if (standbySourceClient == null) {
                // there's not a caught-up standby available to take over the task, so we'll schedule a warmup instead
                final UUID sourceClient = requireNonNull(
                    caughtUpClientsByTaskLoad.poll(movement.task),
                    "Tried to move task to caught-up client but none exist"
                );

                moveActiveAndTryToWarmUp(
                    remainingWarmupReplicas,
                    movement.task,
                    clientStates.get(sourceClient),
                    clientStates.get(movement.destination),
                    warmups.computeIfAbsent(movement.destination, x -> new TreeSet<>())
                );
                caughtUpClientsByTaskLoad.offerAll(asList(sourceClient, movement.destination));
            } else {
                // we found a candidate to trade standby/active state with our destination, so we don't need a warmup
                swapStandbyAndActive(
                    movement.task,
                    clientStates.get(standbySourceClient),
                    clientStates.get(movement.destination)
                );
                caughtUpClientsByTaskLoad.offerAll(asList(standbySourceClient, movement.destination));
            }
        }

        return movementsNeeded;
    }

    static int assignStandbyTaskMovements(final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                          final Map<UUID, ClientState> clientStates,
                                          final AtomicInteger remainingWarmupReplicas,
                                          final Map<UUID, Set<TaskId>> warmups) {
        final BiFunction<UUID, TaskId, Boolean> caughtUpPredicate =
            (client, task) -> taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(task, client, tasksToCaughtUpClients);

        final ConstrainedPrioritySet caughtUpClientsByTaskLoad = new ConstrainedPrioritySet(
            caughtUpPredicate,
            client -> clientStates.get(client).assignedTaskLoad()
        );

        final Queue<TaskMovement> taskMovements = new PriorityQueue<>(
            Comparator.comparing(TaskMovement::numCaughtUpClients).thenComparing(TaskMovement::task)
        );

        for (final Map.Entry<UUID, ClientState> clientStateEntry : clientStates.entrySet()) {
            final UUID destination = clientStateEntry.getKey();
            final ClientState state = clientStateEntry.getValue();
            for (final TaskId task : state.standbyTasks()) {
                if (warmups.getOrDefault(destination, Collections.emptySet()).contains(task)) {
                    // this is a warmup, so we won't move it.
                } else if (taskIsNotCaughtUpOnClientAndOtherCaughtUpClientsExist(task, destination, tasksToCaughtUpClients)) {
                    // if the desired client is not caught up, and there is another client that _is_ caught up, then
                    // we schedule a movement, so we can move the active task to the caught-up client. We'll try to
                    // assign a warm-up to the desired client so that we can move it later on.
                    taskMovements.add(new TaskMovement(task, destination, tasksToCaughtUpClients.get(task)));
                }
            }
            caughtUpClientsByTaskLoad.offer(destination);
        }

        int movementsNeeded = 0;

        for (final TaskMovement movement : taskMovements) {
            final UUID sourceClient = caughtUpClientsByTaskLoad.poll(
                movement.task,
                clientId -> !clientStates.get(clientId).hasAssignedTask(movement.task)
            );

            if (sourceClient == null) {
                // then there's no caught-up client that doesn't already have a copy of this task, so there's
                // nowhere to move it.
            } else {
                moveStandbyAndTryToWarmUp(
                    remainingWarmupReplicas,
                    movement.task,
                    clientStates.get(sourceClient),
                    clientStates.get(movement.destination)
                );
                caughtUpClientsByTaskLoad.offerAll(asList(sourceClient, movement.destination));
                movementsNeeded++;
            }
        }

        return movementsNeeded;
    }

    private static void moveActiveAndTryToWarmUp(final AtomicInteger remainingWarmupReplicas,
                                                 final TaskId task,
                                                 final ClientState sourceClientState,
                                                 final ClientState destinationClientState,
                                                 final Set<TaskId> warmups) {
        sourceClientState.assignActive(task);

        if (remainingWarmupReplicas.getAndDecrement() > 0) {
            destinationClientState.unassignActive(task);
            destinationClientState.assignStandby(task);
            warmups.add(task);
        } else {
            // we have no more standbys or warmups to hand out, so we have to try and move it
            // to the destination in a follow-on rebalance
            destinationClientState.unassignActive(task);
        }
    }

    private static void moveStandbyAndTryToWarmUp(final AtomicInteger remainingWarmupReplicas,
                                                  final TaskId task,
                                                  final ClientState sourceClientState,
                                                  final ClientState destinationClientState) {
        sourceClientState.assignStandby(task);

        if (remainingWarmupReplicas.getAndDecrement() > 0) {
            // Then we can leave it also assigned to the destination as a warmup
        } else {
            // we have no more warmups to hand out, so we have to try and move it
            // to the destination in a follow-on rebalance
            destinationClientState.unassignStandby(task);
        }
    }

    private static void swapStandbyAndActive(final TaskId task,
                                             final ClientState sourceClientState,
                                             final ClientState destinationClientState) {
        sourceClientState.unassignStandby(task);
        sourceClientState.assignActive(task);
        destinationClientState.unassignActive(task);
        destinationClientState.assignStandby(task);
    }

}
