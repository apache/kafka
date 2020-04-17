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

import static org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.taskIsCaughtUpOnClient;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;

public class TaskMovement {
    private static final UUID UNKNOWN = null;

    final TaskId task;
    private UUID source;
    private final UUID destination;

    TaskMovement(final TaskId task, final UUID destination) {
        this.task = task;
        this.destination = destination;
        source = UNKNOWN;
    }

    void assignSource(final UUID source) {
        if (!Objects.equals(this.source, UNKNOWN)) {
            throw new IllegalStateException("Tried to assign source client but source was already assigned!");
        }
        this.source = source;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TaskMovement movement = (TaskMovement) o;
        return Objects.equals(task, movement.task) &&
                   Objects.equals(source, movement.source) &&
                   Objects.equals(destination, movement.destination);
    }

    @Override
    public int hashCode() {
        return Objects.hash(task, source, destination);
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

        final ValidClientsByTaskLoadQueue clientsByTaskLoad =
            new ValidClientsByTaskLoadQueue(
                clientStates,
                (client, task) -> taskIsCaughtUpOnClient(task, client, tasksToCaughtUpClients)
            );

        final SortedSet<TaskMovement> taskMovements = new TreeSet<>(
            (movement, other) -> {
                final int numCaughtUpClients = tasksToCaughtUpClients.get(movement.task).size();
                final int otherNumCaughtUpClients = tasksToCaughtUpClients.get(other.task).size();
                if (numCaughtUpClients != otherNumCaughtUpClients) {
                    return numCaughtUpClients - otherNumCaughtUpClients;
                } else {
                    return movement.task.compareTo(other.task);
                }
            }
        );

        for (final Map.Entry<UUID, List<TaskId>> assignmentEntry : statefulActiveTaskAssignment.entrySet()) {
            final UUID client = assignmentEntry.getKey();
            final ClientState state = clientStates.get(client);
            for (final TaskId task : assignmentEntry.getValue()) {
                if (taskIsCaughtUpOnClient(task, client, tasksToCaughtUpClients)) {
                    state.assignActive(task);
                } else {
                    final TaskMovement taskMovement = new TaskMovement(task,  client);
                    taskMovements.add(taskMovement);
                }
            }
            clientsByTaskLoad.offer(client);
        }

        int remainingWarmupReplicas = maxWarmupReplicas;
        for (final TaskMovement movement : taskMovements) {
            final UUID leastLoadedClient = clientsByTaskLoad.poll(movement.task);
            if (leastLoadedClient == null) {
                throw new IllegalStateException("Tried to move task to caught-up client but none exist");
            }
            movement.assignSource(leastLoadedClient);

            final ClientState sourceClientState = clientStates.get(movement.source);
            sourceClientState.assignActive(movement.task);

            final ClientState destinationClientState = clientStates.get(movement.destination);
            if (destinationClientState.prevStandbyTasks().contains(movement.task) && tasksToRemainingStandbys.get(movement.task) > 0) {
                decrementRemainingStandbys(movement.task, tasksToRemainingStandbys);
                destinationClientState.assignStandby(movement.task);
                warmupReplicasAssigned = true;
            } else if (remainingWarmupReplicas > 0) {
                --remainingWarmupReplicas;
                destinationClientState.assignStandby(movement.task);
                warmupReplicasAssigned = true;
            }

            clientsByTaskLoad.offer(leastLoadedClient);
        }
        return warmupReplicasAssigned;
    }

    private static void decrementRemainingStandbys(final TaskId task, final Map<TaskId, Integer> tasksToRemainingStandbys) {
        tasksToRemainingStandbys.compute(task, (t, numStandbys) -> numStandbys - 1);
    }

}
