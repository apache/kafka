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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

public class DefaultStateConstrainedBalancedAssignor<ID extends Comparable<? super ID>> implements StateConstrainedBalancedAssignor<ID> {

    /**
     * This assignment algorithm guarantees that all task for which caught-up clients exist are assigned to one of the
     * caught-up clients. Tasks for which no caught-up client exist are assigned best-effort to satisfy the balance
     * factor. There is not guarantee that the balance factor is satisfied.
     *
     * @param statefulTasksToRankedClients ranked clients map
     * @param balanceFactor balance factor (at least 1)
     * @return assignment
     */
    @Override
    public Map<ID, List<TaskId>> assign(final SortedMap<TaskId, SortedSet<ClientIdAndRank<ID>>> statefulTasksToRankedClients,
                                        final int balanceFactor,
                                        final Set<ID> clients) {
        if (clients.isEmpty()) {
            throw new IllegalStateException("Set of clients must not be empty");
        }
        final Map<ID, List<TaskId>> assignment = initAssignment(clients);
        final Map<TaskId, List<ID>> tasksToCaughtUpClients = tasksToCaughtUpClients(statefulTasksToRankedClients);
        assignTasksWithCaughtUpClients(assignment, tasksToCaughtUpClients, statefulTasksToRankedClients);
        assignTasksWithoutCaughtUpClients(assignment, tasksToCaughtUpClients, statefulTasksToRankedClients);
        balance(assignment, balanceFactor, statefulTasksToRankedClients, tasksToCaughtUpClients);
        return assignment;
    }

    /**
     * Initialises the assignment with an empty list for each client.
     *
     * @param clients list of clients
     * @return initialised assignment with empty lists
     */
    private Map<ID, List<TaskId>> initAssignment(final Set<ID> clients) {
        final Map<ID, List<TaskId>> assignment = new HashMap<>();
        clients.forEach(client -> assignment.put(client, new ArrayList<>()));
        return assignment;
    }

    /**
     * Maps tasks to clients with caught-up states for the task.
     *
     * @param statefulTasksToRankedClients ranked clients map
     * @return map from tasks with caught-up clients to the list of client candidates
     */
    private Map<TaskId, List<ID>> tasksToCaughtUpClients(final SortedMap<TaskId, SortedSet<ClientIdAndRank<ID>>> statefulTasksToRankedClients) {
        final Map<TaskId, List<ID>> taskToCaughtUpClients = new HashMap<>();
        for (final SortedMap.Entry<TaskId, SortedSet<ClientIdAndRank<ID>>> taskToRankedClient : statefulTasksToRankedClients.entrySet()) {
            final SortedSet<ClientIdAndRank<ID>> rankedClients = taskToRankedClient.getValue();
            for (final ClientIdAndRank<ID> clientIdAndRank : rankedClients) {
                if (clientIdAndRank.rank() == Task.LATEST_OFFSET || clientIdAndRank.rank() == 0) {
                    final TaskId taskId = taskToRankedClient.getKey();
                    taskToCaughtUpClients.computeIfAbsent(taskId, ignored -> new ArrayList<>()).add(clientIdAndRank.clientId());
                } else {
                    break;
                }
            }
        }
        return taskToCaughtUpClients;
    }

    /**
     * Maps a task to the client that host the task according to the previous assignment.
     *
     * @return map from task IDs to clients hosting the corresponding task
     */
    private Map<TaskId, ID> previouslyRunningTasksToPreviousClients(final Map<TaskId, SortedSet<ClientIdAndRank<ID>>> statefulTasksToRankedClients) {
        final Map<TaskId, ID> tasksToPreviousClients = new HashMap<>();
        for (final Map.Entry<TaskId, SortedSet<ClientIdAndRank<ID>>> taskToRankedClients : statefulTasksToRankedClients.entrySet()) {
            final ClientIdAndRank<ID> topRankedClient = taskToRankedClients.getValue().first();
            if (topRankedClient.rank() == Task.LATEST_OFFSET) {
                tasksToPreviousClients.put(taskToRankedClients.getKey(), topRankedClient.clientId());
            }
        }
        return tasksToPreviousClients;
    }

    /**
     * Assigns tasks for which one or more caught-up clients exist to one of the caught-up clients.
     *
     * @param assignment assignment
     * @param tasksToCaughtUpClients map from task IDs to lists of caught-up clients
     */
    private void assignTasksWithCaughtUpClients(final Map<ID, List<TaskId>> assignment,
                                                final Map<TaskId, List<ID>> tasksToCaughtUpClients,
                                                final Map<TaskId, SortedSet<ClientIdAndRank<ID>>> statefulTasksToRankedClients) {
        // If a task was previously assigned to a client that is caught-up and still exists, give it back to the client
        final Map<TaskId, ID> previouslyRunningTasksToPreviousClients =
            previouslyRunningTasksToPreviousClients(statefulTasksToRankedClients);
        previouslyRunningTasksToPreviousClients.forEach((task, client) -> assignment.get(client).add(task));
        final List<TaskId> unassignedTasksWithCaughtUpClients = new ArrayList<>(tasksToCaughtUpClients.keySet());
        unassignedTasksWithCaughtUpClients.removeAll(previouslyRunningTasksToPreviousClients.keySet());

        // If a task's previous host client was not caught-up or no longer exists, assign it to the caught-up client
        // with the least tasks
        for (final TaskId taskId : unassignedTasksWithCaughtUpClients) {
            final List<ID> caughtUpClients = tasksToCaughtUpClients.get(taskId);
            ID clientWithLeastTasks = null;
            int minTaskCount = Integer.MAX_VALUE;
            for (final ID client : caughtUpClients) {
                final int assignedTasksCount = assignment.get(client).size();
                if (minTaskCount > assignedTasksCount) {
                    clientWithLeastTasks = client;
                    minTaskCount = assignedTasksCount;
                }
            }
            assignment.get(clientWithLeastTasks).add(taskId);
        }
    }

    /**
     * Assigns tasks for which no caught-up clients exist.
     * A task is assigned to one of the clients with the highest rank and the least tasks assigned.
     *
     * @param assignment assignment
     * @param tasksToCaughtUpClients map from task IDs to lists of caught-up clients
     * @param statefulTasksToRankedClients ranked clients map
     */
    private void assignTasksWithoutCaughtUpClients(final Map<ID, List<TaskId>> assignment,
                                                   final Map<TaskId, List<ID>> tasksToCaughtUpClients,
                                                   final Map<TaskId, SortedSet<ClientIdAndRank<ID>>> statefulTasksToRankedClients) {
        final SortedSet<TaskId> unassignedTasksWithoutCaughtUpClients = new TreeSet<>(statefulTasksToRankedClients.keySet());
        unassignedTasksWithoutCaughtUpClients.removeAll(tasksToCaughtUpClients.keySet());
        for (final TaskId taskId : unassignedTasksWithoutCaughtUpClients) {
            final SortedSet<ClientIdAndRank<ID>> rankedClients = statefulTasksToRankedClients.get(taskId);
            final long topRank = rankedClients.first().rank();
            int minTasksCount = Integer.MAX_VALUE;
            ID clientWithLeastTasks = rankedClients.first().clientId();
            for (final ClientIdAndRank<ID> clientIdAndRank : rankedClients) {
                if (clientIdAndRank.rank() == topRank) {
                    final ID clientId = clientIdAndRank.clientId();
                    final int assignedTasksCount = assignment.get(clientId).size();
                    if (minTasksCount > assignedTasksCount) {
                        clientWithLeastTasks = clientId;
                        minTasksCount = assignedTasksCount;
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
     *
     * @param assignment assignment
     * @param balanceFactor balance factor
     * @param statefulTasksToRankedClients ranked clients map
     * @param tasksToCaughtUpClients map from task IDs to lists of caught-up clients
     */
    private void balance(final Map<ID, List<TaskId>> assignment,
                         final int balanceFactor,
                         final Map<TaskId, SortedSet<ClientIdAndRank<ID>>> statefulTasksToRankedClients,
                         final Map<TaskId, List<ID>> tasksToCaughtUpClients) {
        final List<ID> clients = new ArrayList<>(assignment.keySet());
        Collections.sort(clients);
        for (final ID client : clients) {
            final List<TaskId> source = assignment.get(client);
            for (final TaskId task : assignedTasksWithoutCaughtUpClientsThatMightBeMoved(source, tasksToCaughtUpClients)) {
                for (final ClientIdAndRank<ID> clientAndRank : statefulTasksToRankedClients.get(task)) {
                    final List<TaskId> destination = assignment.get(clientAndRank.clientId());
                    if (source.size() - destination.size() > balanceFactor) {
                        source.remove(task);
                        destination.add(task);
                        break;
                    }
                }
            }
            for (final TaskId task : assignedTasksWithCaughtUpClientsThatMightBeMoved(source, tasksToCaughtUpClients)) {
                for (final ID caughtUpClient : tasksToCaughtUpClients.get(task)) {
                    final List<TaskId> destination = assignment.get(caughtUpClient);
                    if (source.size() - destination.size() > balanceFactor) {
                        source.remove(task);
                        destination.add(task);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Returns a sublist of tasks in the given list that does not have a caught-up client.
     *
     * @param tasks list of task IDs
     * @param tasksToCaughtUpClients map from task IDs to lists of caught-up clients
     * @return a list of task IDs that does not have a caught-up client
     */
    private List<TaskId> assignedTasksWithoutCaughtUpClientsThatMightBeMoved(final List<TaskId> tasks,
                                                                             final Map<TaskId, List<ID>> tasksToCaughtUpClients) {
        return assignedTasksThatMightBeMoved(tasks, tasksToCaughtUpClients, false);
    }

    /**
     * Returns a sublist of tasks in the given list that have a caught-up client.
     *
     * @param tasks list of task IDs
     * @param tasksToCaughtUpClients map from task IDs to lists of caught-up clients
     * @return a list of task IDs that have a caught-up client
     */
    private List<TaskId> assignedTasksWithCaughtUpClientsThatMightBeMoved(final List<TaskId> tasks,
                                                                          final Map<TaskId, List<ID>> tasksToCaughtUpClients) {
        return assignedTasksThatMightBeMoved(tasks, tasksToCaughtUpClients, true);
    }

    private List<TaskId> assignedTasksThatMightBeMoved(final List<TaskId> tasks,
                                                       final Map<TaskId, List<ID>> tasksToCaughtUpClients,
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
