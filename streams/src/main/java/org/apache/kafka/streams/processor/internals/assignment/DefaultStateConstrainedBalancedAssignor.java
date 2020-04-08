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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;

public class DefaultStateConstrainedBalancedAssignor implements StateConstrainedBalancedAssignor {

    /**
     * This assignment algorithm guarantees that all task for which caught-up clients exist are assigned to one of the
     * caught-up clients. Tasks for which no caught-up client exist are assigned best-effort to satisfy the balance
     * factor. There is no guarantee that the balance factor is satisfied.
     *
     * @param statefulTasksToRankedClients ranked clients map
     * @param balanceFactor balance factor (at least 1)
     * @param clients set of clients to assign tasks to
     * @param clientsToNumberOfStreamThreads map of clients to their number of stream threads
     * @return assignment
     */
    @Override
    public Map<UUID, List<TaskId>> assign(final SortedMap<TaskId, SortedSet<RankedClient>> statefulTasksToRankedClients,
                                          final int balanceFactor,
                                          final Set<UUID> clients,
                                          final Map<UUID, Integer> clientsToNumberOfStreamThreads,
                                          final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients) {
        checkClientsAndNumberOfStreamThreads(clientsToNumberOfStreamThreads, clients);
        final Map<UUID, List<TaskId>> assignment = initAssignment(clients);
        assignTasksWithCaughtUpClients(
            assignment,
            tasksToCaughtUpClients,
            statefulTasksToRankedClients
        );
        assignTasksWithoutCaughtUpClients(
            assignment,
            tasksToCaughtUpClients,
            statefulTasksToRankedClients
        );
        balance(
            assignment,
            balanceFactor,
            statefulTasksToRankedClients,
            tasksToCaughtUpClients,
            clientsToNumberOfStreamThreads
        );
        return assignment;
    }

    private void checkClientsAndNumberOfStreamThreads(final Map<UUID, Integer> clientsToNumberOfStreamThreads,
                                                      final Set<UUID> clients) {
        if (clients.isEmpty()) {
            throw new IllegalStateException("Set of clients must not be empty");
        }
        if (clientsToNumberOfStreamThreads.isEmpty()) {
            throw new IllegalStateException("Map from clients to their number of stream threads must not be empty");
        }
        final Set<UUID> copyOfClients = new HashSet<>(clients);
        copyOfClients.removeAll(clientsToNumberOfStreamThreads.keySet());
        if (!copyOfClients.isEmpty()) {
            throw new IllegalStateException(
                "Map from clients to their number of stream threads must contain an entry for each client involved in "
                    + "the assignment."
            );
        }
    }

    /**
     * Initialises the assignment with an empty list for each client.
     *
     * @param clients list of clients
     * @return initialised assignment with empty lists
     */
    private Map<UUID, List<TaskId>> initAssignment(final Set<UUID> clients) {
        final Map<UUID, List<TaskId>> assignment = new HashMap<>();
        clients.forEach(client -> assignment.put(client, new ArrayList<>()));
        return assignment;
    }

    /**
     * Maps a task to the client that host the task according to the previous assignment.
     *
     * @return map from task UUIDs to clients hosting the corresponding task
     */
    private Map<TaskId, UUID> previouslyRunningTasksToPreviousClients(final Map<TaskId, SortedSet<RankedClient>> statefulTasksToRankedClients) {
        final Map<TaskId, UUID> tasksToPreviousClients = new HashMap<>();
        for (final Map.Entry<TaskId, SortedSet<RankedClient>> taskToRankedClients : statefulTasksToRankedClients.entrySet()) {
            final RankedClient topRankedClient = taskToRankedClients.getValue().first();
            if (topRankedClient.rank() == Task.LATEST_OFFSET) {
                tasksToPreviousClients.put(taskToRankedClients.getKey(), topRankedClient.clientId());
            }
        }
        return tasksToPreviousClients;
    }

    /**
     * Assigns tasks for which one or more caught-up clients exist to one of the caught-up clients.
     * @param assignment assignment
     * @param tasksToCaughtUpClients map from task UUIDs to lists of caught-up clients
     */
    private void assignTasksWithCaughtUpClients(final Map<UUID, List<TaskId>> assignment,
                                                final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                                final Map<TaskId, SortedSet<RankedClient>> statefulTasksToRankedClients) {
        // If a task was previously assigned to a client that is caught-up and still exists, give it back to the client
        final Map<TaskId, UUID> previouslyRunningTasksToPreviousClients =
            previouslyRunningTasksToPreviousClients(statefulTasksToRankedClients);
        previouslyRunningTasksToPreviousClients.forEach((task, client) -> assignment.get(client).add(task));
        final List<TaskId> unassignedTasksWithCaughtUpClients = new ArrayList<>(tasksToCaughtUpClients.keySet());
        unassignedTasksWithCaughtUpClients.removeAll(previouslyRunningTasksToPreviousClients.keySet());

        // If a task's previous host client was not caught-up or no longer exists, assign it to the caught-up client
        // with the least tasks
        for (final TaskId taskId : unassignedTasksWithCaughtUpClients) {
            final SortedSet<UUID> caughtUpClients = tasksToCaughtUpClients.get(taskId);
            UUID clientWithLeastTasks = null;
            int minTaskPerStreamThread = Integer.MAX_VALUE;
            for (final UUID client : caughtUpClients) {
                final int assignedTasks = assignment.get(client).size();
                if (minTaskPerStreamThread > assignedTasks) {
                    clientWithLeastTasks = client;
                    minTaskPerStreamThread = assignedTasks;
                }
            }
            assignment.get(clientWithLeastTasks).add(taskId);
        }
    }

    /**
     * Assigns tasks for which no caught-up clients exist.
     * A task is assigned to one of the clients with the highest rank and the least tasks assigned.
     * @param assignment assignment
     * @param tasksToCaughtUpClients map from task UUIDs to lists of caught-up clients
     * @param statefulTasksToRankedClients ranked clients map
     */
    private void assignTasksWithoutCaughtUpClients(final Map<UUID, List<TaskId>> assignment,
                                                   final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                                   final Map<TaskId, SortedSet<RankedClient>> statefulTasksToRankedClients) {
        final SortedSet<TaskId> unassignedTasksWithoutCaughtUpClients = new TreeSet<>(statefulTasksToRankedClients.keySet());
        unassignedTasksWithoutCaughtUpClients.removeAll(tasksToCaughtUpClients.keySet());
        for (final TaskId taskId : unassignedTasksWithoutCaughtUpClients) {
            final SortedSet<RankedClient> rankedClients = statefulTasksToRankedClients.get(taskId);
            final long topRank = rankedClients.first().rank();
            int minTasksPerStreamThread = Integer.MAX_VALUE;
            UUID clientWithLeastTasks = rankedClients.first().clientId();
            for (final RankedClient rankedClient : rankedClients) {
                if (rankedClient.rank() == topRank) {
                    final UUID clientId = rankedClient.clientId();
                    final int assignedTasks = assignment.get(clientId).size();
                    if (minTasksPerStreamThread > assignedTasks) {
                        clientWithLeastTasks = clientId;
                        minTasksPerStreamThread = assignedTasks;
                    }
                } else {
                    break;
                }
            }
            assignment.get(clientWithLeastTasks).add(taskId);
        }
    }

    /**
     * Balance the assignment.
     * @param assignment assignment
     * @param balanceFactor balance factor
     * @param statefulTasksToRankedClients ranked clients map
     * @param tasksToCaughtUpClients map from task UUIDs to lists of caught-up clients
     * @param clientsToNumberOfStreamThreads map from clients to their number of stream threads
     */
    private void balance(final Map<UUID, List<TaskId>> assignment,
                         final int balanceFactor,
                         final Map<TaskId, SortedSet<RankedClient>> statefulTasksToRankedClients,
                         final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                         final Map<UUID, Integer> clientsToNumberOfStreamThreads) {
        final List<UUID> clients = new ArrayList<>(assignment.keySet());
        Collections.sort(clients);
        for (final UUID sourceClientId : clients) {
            final List<TaskId> sourceTasks = assignment.get(sourceClientId);
            maybeMoveSourceTasksWithoutCaughtUpClients(
                assignment,
                balanceFactor,
                statefulTasksToRankedClients,
                tasksToCaughtUpClients,
                clientsToNumberOfStreamThreads,
                sourceClientId,
                sourceTasks
            );
            maybeMoveSourceTasksWithCaughtUpClients(
                assignment,
                balanceFactor,
                tasksToCaughtUpClients,
                clientsToNumberOfStreamThreads,
                sourceClientId,
                sourceTasks
            );
        }
    }

    private void maybeMoveSourceTasksWithoutCaughtUpClients(final Map<UUID, List<TaskId>> assignment,
                                                            final int balanceFactor,
                                                            final Map<TaskId, SortedSet<RankedClient>> statefulTasksToRankedClients,
                                                            final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                                            final Map<UUID, Integer> clientsToNumberOfStreamThreads,
                                                            final UUID sourceClientId,
                                                            final List<TaskId> sourceTasks) {
        for (final TaskId task : assignedTasksWithoutCaughtUpClientsThatMightBeMoved(sourceTasks, tasksToCaughtUpClients)) {
            final int assignedTasksPerStreamThreadAtSource =
                sourceTasks.size() / clientsToNumberOfStreamThreads.get(sourceClientId);
            for (final RankedClient clientAndRank : statefulTasksToRankedClients.get(task)) {
                final UUID destinationClientId = clientAndRank.clientId();
                final List<TaskId> destination = assignment.get(destinationClientId);
                final int assignedTasksPerStreamThreadAtDestination =
                    destination.size() / clientsToNumberOfStreamThreads.get(destinationClientId);
                if (assignedTasksPerStreamThreadAtSource - assignedTasksPerStreamThreadAtDestination > balanceFactor) {
                    sourceTasks.remove(task);
                    destination.add(task);
                    break;
                }
            }
        }
    }

    private void maybeMoveSourceTasksWithCaughtUpClients(final Map<UUID, List<TaskId>> assignment,
                                                         final int balanceFactor,
                                                         final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                                         final Map<UUID, Integer> clientsToNumberOfStreamThreads,
                                                         final UUID sourceClientId,
                                                         final List<TaskId> sourceTasks) {
        for (final TaskId task : assignedTasksWithCaughtUpClientsThatMightBeMoved(sourceTasks, tasksToCaughtUpClients)) {
            final int assignedTasksPerStreamThreadAtSource =
                sourceTasks.size() / clientsToNumberOfStreamThreads.get(sourceClientId);
            for (final UUID destinationClientId : tasksToCaughtUpClients.get(task)) {
                final List<TaskId> destination = assignment.get(destinationClientId);
                final int assignedTasksPerStreamThreadAtDestination =
                    destination.size() / clientsToNumberOfStreamThreads.get(destinationClientId);
                if (assignedTasksPerStreamThreadAtSource - assignedTasksPerStreamThreadAtDestination > balanceFactor) {
                    sourceTasks.remove(task);
                    destination.add(task);
                    break;
                }
            }
        }
    }

    /**
     * Returns a sublist of tasks in the given list that does not have a caught-up client.
     *
     * @param tasks list of task UUIDs
     * @param tasksToCaughtUpClients map from task UUIDs to lists of caught-up clients
     * @return a list of task UUIDs that does not have a caught-up client
     */
    private List<TaskId> assignedTasksWithoutCaughtUpClientsThatMightBeMoved(final List<TaskId> tasks,
                                                                             final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients) {
        return assignedTasksThatMightBeMoved(tasks, tasksToCaughtUpClients, false);
    }

    /**
     * Returns a sublist of tasks in the given list that have a caught-up client.
     *
     * @param tasks list of task UUIDs
     * @param tasksToCaughtUpClients map from task UUIDs to lists of caught-up clients
     * @return a list of task UUIDs that have a caught-up client
     */
    private List<TaskId> assignedTasksWithCaughtUpClientsThatMightBeMoved(final List<TaskId> tasks,
                                                                          final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients) {
        return assignedTasksThatMightBeMoved(tasks, tasksToCaughtUpClients, true);
    }

    private List<TaskId> assignedTasksThatMightBeMoved(final List<TaskId> tasks,
                                                       final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients,
                                                       final boolean isCaughtUp) {
        final List<TaskId> tasksWithCaughtUpClients = new ArrayList<>();
        for (int i = tasks.size() - 1; i >= 0; --i) {
            final TaskId task = tasks.get(i);
            if (isCaughtUp == tasksToCaughtUpClients.containsKey(task)) {
                tasksWithCaughtUpClients.add(task);
            }
        }
        return Collections.unmodifiableList(tasksWithCaughtUpClients);
    }
}
