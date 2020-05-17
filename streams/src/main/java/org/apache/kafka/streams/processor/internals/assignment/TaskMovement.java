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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.streams.processor.TaskId;

class TaskMovement {
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

    /**
     * @return true if this client is caught-up for this task, or the task has no caught-up clients
     */
    private static boolean taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(final TaskId task,
                                                                          final UUID client,
                                                                          final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients) {
        final Set<UUID> caughtUpClients = tasksToCaughtUpClients.get(task);
        return caughtUpClients == null || caughtUpClients.contains(client);
    }

    /**
     * @return whether any warmup replicas were assigned
     */
    static boolean assignTaskMovements(final Map<UUID, List<TaskId>> statefulActiveTaskAssignment,
                                       final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                       final Map<UUID, ClientState> clientStates,
                                       final Map<TaskId, Integer> tasksToRemainingStandbys,
                                       final int maxWarmupReplicas) {
        boolean warmupReplicasAssigned = false;

        final ValidClientsByTaskLoadQueue clientsByTaskLoad = new ValidClientsByTaskLoadQueue(
            clientStates,
            (client, task) -> taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(task, client, tasksToCaughtUpClients)
        );

        final SortedSet<TaskMovement> taskMovements = new TreeSet<>(
            (movement, other) -> {
                final int numCaughtUpClients = movement.caughtUpClients.size();
                final int otherNumCaughtUpClients = other.caughtUpClients.size();
                if (numCaughtUpClients != otherNumCaughtUpClients) {
                    return Integer.compare(numCaughtUpClients, otherNumCaughtUpClients);
                } else {
                    return movement.task.compareTo(other.task);
                }
            }
        );

        for (final Map.Entry<UUID, List<TaskId>> assignmentEntry : statefulActiveTaskAssignment.entrySet()) {
            final UUID client = assignmentEntry.getKey();
            final ClientState state = clientStates.get(client);
            for (final TaskId task : assignmentEntry.getValue()) {
                if (taskIsCaughtUpOnClientOrNoCaughtUpClientsExist(task, client, tasksToCaughtUpClients)) {
                    state.assignActive(task);
                } else {
                    final TaskMovement taskMovement = new TaskMovement(task, client, tasksToCaughtUpClients.get(task));
                    taskMovements.add(taskMovement);
                }
            }
            clientsByTaskLoad.offer(client);
        }

        final AtomicInteger remainingWarmupReplicas = new AtomicInteger(maxWarmupReplicas);
        for (final TaskMovement movement : taskMovements) {
            final UUID sourceClient = clientsByTaskLoad.poll(movement.task);
            if (sourceClient == null) {
                throw new IllegalStateException("Tried to move task to caught-up client but none exist");
            }

            final ClientState sourceClientState = clientStates.get(sourceClient);
            sourceClientState.assignActive(movement.task);
            clientsByTaskLoad.offer(sourceClient);

            final ClientState destinationClientState = clientStates.get(movement.destination);
            if (shouldAssignWarmupReplica(movement.task, destinationClientState, remainingWarmupReplicas, tasksToRemainingStandbys)) {
                destinationClientState.assignStandby(movement.task);
                clientsByTaskLoad.offer(movement.destination);
                warmupReplicasAssigned = true;
            }
        }
        return warmupReplicasAssigned;
    }

    private static boolean shouldAssignWarmupReplica(final TaskId task,
                                                     final ClientState destinationClientState,
                                                     final AtomicInteger remainingWarmupReplicas,
                                                     final Map<TaskId, Integer> tasksToRemainingStandbys) {
        if (destinationClientState.previousAssignedTasks().contains(task) && tasksToRemainingStandbys.get(task) > 0) {
            tasksToRemainingStandbys.compute(task, (t, numStandbys) -> numStandbys - 1);
            return true;
        } else {
            return remainingWarmupReplicas.getAndDecrement() > 0;
        }
    }

}
