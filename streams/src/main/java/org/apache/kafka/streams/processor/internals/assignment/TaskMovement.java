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
import java.util.function.Function;

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
    }

    private TaskId task() {
        return task;
    }

    private int numCaughtUpClients() {
        return caughtUpClients.size();
    }

    private static boolean taskIsNotCaughtUpOnClientAndOtherMoreCaughtUpClientsExist(final TaskId task,
                                                                                     final UUID client,
                                                                                     final Map<UUID, ClientState> clientStates,
                                                                                     final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                                                                     final Map<TaskId, SortedSet<UUID>> tasksToClientByLag) {
        final SortedSet<UUID> taskClients = requireNonNull(tasksToClientByLag.get(task), "uninitialized set");
        if (taskIsCaughtUpOnClient(task, client, tasksToCaughtUpClients)) {
            return false;
        }
        final long mostCaughtUpLag = clientStates.get(taskClients.first()).lagFor(task);
        final long clientLag = clientStates.get(client).lagFor(task);
        return mostCaughtUpLag < clientLag;
    }

    private static boolean taskIsCaughtUpOnClient(final TaskId task,
                                                  final UUID client,
                                                  final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients) {
        final Set<UUID> caughtUpClients = requireNonNull(tasksToCaughtUpClients.get(task), "uninitialized set");
        return caughtUpClients.contains(client);
    }

    static int assignActiveTaskMovements(final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                         final Map<TaskId, SortedSet<UUID>> tasksToClientByLag,
                                         final Map<UUID, ClientState> clientStates,
                                         final Map<UUID, Set<TaskId>> warmups,
                                         final AtomicInteger remainingWarmupReplicas) {
        final BiFunction<UUID, TaskId, Boolean> caughtUpPredicate =
            (client, task) -> taskIsCaughtUpOnClient(task, client, tasksToCaughtUpClients);

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
                // if the desired client is not caught up, and there is another client that _is_ more caught up, then
                // we schedule a movement, so we can move the active task to a more caught-up client. We'll try to
                // assign a warm-up to the desired client so that we can move it later on.
                if (taskIsNotCaughtUpOnClientAndOtherMoreCaughtUpClientsExist(task, client, clientStates, tasksToCaughtUpClients, tasksToClientByLag)) {
                    taskMovements.add(new TaskMovement(task, client, tasksToCaughtUpClients.get(task)));
                }
            }
            caughtUpClientsByTaskLoad.offer(client);
        }

        final int movementsNeeded = taskMovements.size();

        for (final TaskMovement movement : taskMovements) {
            // Attempt to find a caught up standby, otherwise find any caught up client, failing that use the most
            // caught up client.
            final boolean moved = tryToSwapStandbyAndActiveOnCaughtUpClient(clientStates, caughtUpClientsByTaskLoad, movement) ||
                    tryToMoveActiveToCaughtUpClientAndTryToWarmUp(clientStates, warmups, remainingWarmupReplicas, caughtUpClientsByTaskLoad, movement) ||
                    tryToMoveActiveToMostCaughtUpClient(tasksToClientByLag, clientStates, warmups, remainingWarmupReplicas, caughtUpClientsByTaskLoad, movement);

            if (!moved) {
                throw new IllegalStateException("Tried to move task to more caught-up client as scheduled before but none exist");
            }
        }

        return movementsNeeded;
    }

    static int assignStandbyTaskMovements(final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                          final Map<TaskId, SortedSet<UUID>> tasksToClientByLag,
                                          final Map<UUID, ClientState> clientStates,
                                          final AtomicInteger remainingWarmupReplicas,
                                          final Map<UUID, Set<TaskId>> warmups) {
        final BiFunction<UUID, TaskId, Boolean> caughtUpPredicate =
            (client, task) -> taskIsCaughtUpOnClient(task, client, tasksToCaughtUpClients);

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
                } else if (taskIsNotCaughtUpOnClientAndOtherMoreCaughtUpClientsExist(task, destination, clientStates, tasksToCaughtUpClients, tasksToClientByLag)) {
                    // if the desired client is not caught up, and there is another client that _is_ more caught up, then
                    // we schedule a movement, so we can move the active task to the caught-up client. We'll try to
                    // assign a warm-up to the desired client so that we can move it later on.
                    taskMovements.add(new TaskMovement(task, destination, tasksToCaughtUpClients.get(task)));
                }
            }
            caughtUpClientsByTaskLoad.offer(destination);
        }

        int movementsNeeded = 0;

        for (final TaskMovement movement : taskMovements) {
            final Function<UUID, Boolean> eligibleClientPredicate =
                    clientId -> !clientStates.get(clientId).hasAssignedTask(movement.task);
            UUID sourceClient = caughtUpClientsByTaskLoad.poll(
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
                    clientStates.get(sourceClient),
                    clientStates.get(movement.destination)
                );
                caughtUpClientsByTaskLoad.offerAll(asList(sourceClient, movement.destination));
                movementsNeeded++;
            }
        }

        return movementsNeeded;
    }

    private static boolean tryToSwapStandbyAndActiveOnCaughtUpClient(final Map<UUID, ClientState> clientStates,
                                                                     final ConstrainedPrioritySet caughtUpClientsByTaskLoad,
                                                                     final TaskMovement movement) {
        final UUID caughtUpStandbySourceClient = caughtUpClientsByTaskLoad.poll(
                movement.task,
                c -> clientStates.get(c).hasStandbyTask(movement.task)
        );
        if (caughtUpStandbySourceClient != null) {
            swapStandbyAndActive(
                    movement.task,
                    clientStates.get(caughtUpStandbySourceClient),
                    clientStates.get(movement.destination)
            );
            caughtUpClientsByTaskLoad.offerAll(asList(caughtUpStandbySourceClient, movement.destination));
            return true;
        }
        return false;
    }

    private static boolean tryToMoveActiveToCaughtUpClientAndTryToWarmUp(final Map<UUID, ClientState> clientStates,
                                                                         final Map<UUID, Set<TaskId>> warmups,
                                                                         final AtomicInteger remainingWarmupReplicas,
                                                                         final ConstrainedPrioritySet caughtUpClientsByTaskLoad,
                                                                         final TaskMovement movement) {
        final UUID caughtUpSourceClient = caughtUpClientsByTaskLoad.poll(movement.task);
        if (caughtUpSourceClient != null) {
            moveActiveAndTryToWarmUp(
                    remainingWarmupReplicas,
                    movement.task,
                    clientStates.get(caughtUpSourceClient),
                    clientStates.get(movement.destination),
                    warmups.computeIfAbsent(movement.destination, x -> new TreeSet<>())
            );
            caughtUpClientsByTaskLoad.offerAll(asList(caughtUpSourceClient, movement.destination));
            return true;
        }
        return false;
    }

    private static boolean tryToMoveActiveToMostCaughtUpClient(final Map<TaskId, SortedSet<UUID>> tasksToClientByLag,
                                                               final Map<UUID, ClientState> clientStates,
                                                               final Map<UUID, Set<TaskId>> warmups,
                                                               final AtomicInteger remainingWarmupReplicas,
                                                               final ConstrainedPrioritySet caughtUpClientsByTaskLoad,
                                                               final TaskMovement movement) {
        final UUID mostCaughtUpSourceClient = mostCaughtUpEligibleClient(tasksToClientByLag, movement.task, movement.destination);
        if (mostCaughtUpSourceClient != null) {
            if (clientStates.get(mostCaughtUpSourceClient).hasStandbyTask(movement.task)) {
                swapStandbyAndActive(
                        movement.task,
                        clientStates.get(mostCaughtUpSourceClient),
                        clientStates.get(movement.destination)
                );
            } else {
                moveActiveAndTryToWarmUp(
                        remainingWarmupReplicas,
                        movement.task,
                        clientStates.get(mostCaughtUpSourceClient),
                        clientStates.get(movement.destination),
                        warmups.computeIfAbsent(movement.destination, x -> new TreeSet<>())
                );
            }
            caughtUpClientsByTaskLoad.offerAll(asList(mostCaughtUpSourceClient, movement.destination));
            return true;
        }
        return false;
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

    private static UUID mostCaughtUpEligibleClient(final Map<TaskId, SortedSet<UUID>> tasksToClientByLag,
                                                   final TaskId task,
                                                   final UUID destinationClient) {
        return mostCaughtUpEligibleClient(tasksToClientByLag, client -> true, task, destinationClient);
    }

    private static UUID mostCaughtUpEligibleClient(final Map<TaskId, SortedSet<UUID>> tasksToClientByLag,
                                                   final Function<UUID, Boolean> constraint,
                                                   final TaskId task,
                                                   final UUID destinationClient) {
        for (final UUID client : tasksToClientByLag.get(task)) {
            if (destinationClient.equals(client)) {
                break;
            } else if (constraint.apply(client)) {
                return client;
            }
        }
        return null;
    }

}
