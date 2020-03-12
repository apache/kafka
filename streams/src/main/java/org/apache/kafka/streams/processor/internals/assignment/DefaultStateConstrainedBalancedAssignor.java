package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

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
    public Map<ID, List<TaskId>> assign(final Map<TaskId, SortedSet<ClientIdAndLag<ID>>> statefulTasksToRankedClients,
                                        final int balanceFactor) {
        final Map<ID, List<TaskId>> assignment = initAssignment(statefulTasksToRankedClients);
        final Map<TaskId, List<ID>> tasksToCaughtUpClients = tasksToCaughtUpClients(statefulTasksToRankedClients);
        assignTasksWithCaughtUpClients(assignment, tasksToCaughtUpClients, statefulTasksToRankedClients);
        assignTasksWithoutCaughtUpClients(assignment, tasksToCaughtUpClients, statefulTasksToRankedClients);
        balance(assignment, balanceFactor, statefulTasksToRankedClients, tasksToCaughtUpClients);
        return assignment;
    }

    /**
     * Initialises the assignment with the clients in the ranked clients map and assigns to each client an empty list.
     *
     * @param statefulTasksToRankedClients ranked clients map
     * @return initialised assignment with empty lists
     */
    private Map<ID, List<TaskId>> initAssignment(final Map<TaskId, SortedSet<ClientIdAndLag<ID>>> statefulTasksToRankedClients) {
        final Map<ID, List<TaskId>> assignment = new HashMap<>();
        final SortedSet<ClientIdAndLag<ID>> clientIdAndLags = statefulTasksToRankedClients.values().stream()
            .findFirst().orElseThrow(() -> new IllegalStateException("The list of clients and lags must not be empty"));
        for (final ClientIdAndLag<ID> clientIdAndLag : clientIdAndLags) {
            assignment.put(clientIdAndLag.clientId(), new ArrayList<>());
        }
        if (assignment.isEmpty()) {
            throw new IllegalStateException("Clients within an assignment must not be empty");
        }
        return assignment;
    }

    /**
     * Maps tasks to clients with caught-up states for the task.
     *
     * @param statefulTasksToRankedClients ranked clients map
     * @return map from tasks with caught-up clients to the list of client candidates
     */
    private Map<TaskId, List<ID>> tasksToCaughtUpClients(final Map<TaskId, SortedSet<ClientIdAndLag<ID>>> statefulTasksToRankedClients) {
        final Map<TaskId, List<ID>> taskToCaughtUpClients = new HashMap<>();
        for (final Map.Entry<TaskId, SortedSet<ClientIdAndLag<ID>>> taskToRankedClient : statefulTasksToRankedClients.entrySet()) {
            final SortedSet<ClientIdAndLag<ID>> rankedClients = taskToRankedClient.getValue();
            final List<ID> caughtUpClients = new ArrayList<>();
            final Iterator<ClientIdAndLag<ID>> iterator = rankedClients.iterator();
            ClientIdAndLag<ID> clientIdAndLag = iterator.next();
            while (clientIdAndLag != null && (clientIdAndLag.lag() == Task.LATEST_OFFSET || clientIdAndLag.lag() == 0)) {
                final TaskId taskId = taskToRankedClient.getKey();
                taskToCaughtUpClients.computeIfAbsent(taskId, ignored -> caughtUpClients).add(clientIdAndLag.clientId());
                clientIdAndLag = iterator.next();
            }
        }
        return taskToCaughtUpClients;
    }

    /**
     * Maps a task to the client that host the task according to the previous assignment.
     *
     * @return map from task IDs to clients hosting the corresponding task
     */
    private Map<TaskId, ID> tasksToPreviousClients(final Map<TaskId, SortedSet<ClientIdAndLag<ID>>> statefulTasksToRankedClients) {
        final Map<TaskId, ID> tasksToPreviousClients = new HashMap<>();
        for (final Map.Entry<TaskId, SortedSet<ClientIdAndLag<ID>>> taskToRankedClients : statefulTasksToRankedClients.entrySet()) {
            final ClientIdAndLag<ID> topRankedClient = taskToRankedClients.getValue().first();
            if (topRankedClient.lag() == Task.LATEST_OFFSET) {
                tasksToPreviousClients.put(taskToRankedClients.getKey(), topRankedClient.clientId());
            }
        }
        return tasksToPreviousClients;
    }

    /**
     * Assigns task for which one or more caught-up clients exist to one of the caught-up clients.
     *
     * @param assignment assignment
     * @param tasksToCaughtUpClients map from task IDs to lists of caught-up clients
     */
    private void assignTasksWithCaughtUpClients(final Map<ID, List<TaskId>> assignment,
                                                final Map<TaskId, List<ID>> tasksToCaughtUpClients,
                                                final Map<TaskId, SortedSet<ClientIdAndLag<ID>>> statefulTasksToRankedClients) {
        // If a task was previously assigned to a client that is caught-up and still exists, give it back to the client
        final Map<TaskId, ID> tasksToPreviousClients = tasksToPreviousClients(statefulTasksToRankedClients);
        final List<TaskId> unassignedTasksWithCaughtUpClients = new ArrayList<>();
        for (final Map.Entry<TaskId, List<ID>> taskToCaughtUpClients : tasksToCaughtUpClients.entrySet()) {
            final TaskId taskId = taskToCaughtUpClients.getKey();
            final List<ID> caughtUpClients = taskToCaughtUpClients.getValue();
            final ID previousHostingClients = tasksToPreviousClients.get(taskId);
            if (previousHostingClients != null && caughtUpClients.contains(previousHostingClients)) {
                assignment.get(previousHostingClients).add(taskId);
            } else {
                unassignedTasksWithCaughtUpClients.add(taskId);
            }
        }
        // If a task's previous host client was not caught-up or no longer exists, assign it to the caught-up client with the least tasks
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
     * A task is assigned to one of the clients with the least lag and the least tasks assigned.
     *
     * @param assignment assignment
     * @param tasksToCaughtUpClients map from task IDs to lists of caught-up clients
     * @param statefulTasksToRankedClients ranked clients map
     */
    private void assignTasksWithoutCaughtUpClients(final Map<ID, List<TaskId>> assignment,
                                                   final Map<TaskId, List<ID>> tasksToCaughtUpClients,
                                                   final Map<TaskId, SortedSet<ClientIdAndLag<ID>>> statefulTasksToRankedClients) {
        final Set<TaskId> unassignedTasksWithoutCaughtUpClients = new HashSet<>(statefulTasksToRankedClients.keySet());
        unassignedTasksWithoutCaughtUpClients.removeAll(tasksToCaughtUpClients.keySet());
        for (final TaskId taskId : unassignedTasksWithoutCaughtUpClients) {
            SortedSet<ClientIdAndLag<ID>> rankedClients = statefulTasksToRankedClients.get(taskId);
            int minTasksCount = Integer.MAX_VALUE;
            final Iterator<ClientIdAndLag<ID>> iterator = rankedClients.iterator();
            ClientIdAndLag<ID> clientIdAndLag = iterator.next();
            long minLag = clientIdAndLag.lag();
            ID clientWithLeastTasks = clientIdAndLag.clientId();
            clientIdAndLag = iterator.next();
            while (clientIdAndLag != null && clientIdAndLag.lag() == minLag) {
                final ID clientId = clientIdAndLag.clientId();
                final int assignedTasksCount = assignment.get(clientId).size();
                if (minTasksCount > assignedTasksCount) {
                    clientWithLeastTasks = clientId;
                    minTasksCount = assignedTasksCount;
                }
                clientIdAndLag = iterator.next();
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
                         final Map<TaskId, SortedSet<ClientIdAndLag<ID>>> statefulTasksToRankedClients,
                         final Map<TaskId, List<ID>> tasksToCaughtUpClients) {
        final List<ID> clients = new ArrayList<>(assignment.keySet());
        Collections.sort(clients);
        for (final ID client : clients) {
            final List<TaskId> source = assignment.get(client);
            TaskId taskToMove = nextTaskWithoutCaughtUpClient(source, tasksToCaughtUpClients);
            while (taskToMove != null) {
                for (final ClientIdAndLag<ID> clientAndLag : statefulTasksToRankedClients.get(taskToMove)) {
                    final List<TaskId> destination = assignment.get(clientAndLag.clientId());
                    if (source.size() - destination.size() > balanceFactor) {
                        source.remove(taskToMove);
                        destination.add(taskToMove);
                        break;
                    }
                }
                taskToMove = nextTaskWithoutCaughtUpClient(source, tasksToCaughtUpClients);
            }
        }
    }

    /**
     * Returns a task in the given list that does not have a caught-up client.
     *
     * @param tasks list of tasks
     * @param tasksToCaughtUpClients map from task IDs to lists of caught-up clients
     * @return an ID of a task that does not have a caught-up client,
     *         {@code null} if all tasks in the list have a caught-up client
     */
    private TaskId nextTaskWithoutCaughtUpClient(final List<TaskId> tasks,
                                                 final Map<TaskId, List<ID>> tasksToCaughtUpClients) {
        final TaskId task = tasks.get(tasks.size() - 1);
        if (!tasksToCaughtUpClients.containsKey(task)) {
            return task;
        }
        return null;
    }
}
