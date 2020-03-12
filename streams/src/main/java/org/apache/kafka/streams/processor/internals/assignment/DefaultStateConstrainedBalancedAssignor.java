package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultStateConstrainedBalancedAssignor implements StateConstrainedBalancedAssignor {

    private final Map<String, List<TaskId>> previousAssignment;

    public DefaultStateConstrainedBalancedAssignor(final Map<String, List<TaskId>> previousAssignment) {
        this.previousAssignment = previousAssignment;
    }

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
    public Map<String, List<TaskId>> assign(final Map<TaskId, List<ClientIdAndLag>> statefulTasksToRankedClients,
                                            final int balanceFactor) {
        final Map<String, List<TaskId>> assignment = initAssignment(statefulTasksToRankedClients);
        final Map<TaskId, List<String>> tasksToCaughtUpClients = tasksToCaughtUpClients(statefulTasksToRankedClients);
        final List<TaskId> unassignedTasksWithoutCaughtUpClients = new ArrayList<>();
        assignTasksWithCaughtUpClients(
            assignment,
            unassignedTasksWithoutCaughtUpClients,
            tasksToCaughtUpClients
        );
        assignTasksWithoutCaughtUpClients(
            assignment,
            unassignedTasksWithoutCaughtUpClients,
            statefulTasksToRankedClients
        );
        balance(assignment, balanceFactor, statefulTasksToRankedClients, tasksToCaughtUpClients);
        return assignment;
    }

    /**
     * Initialises the assignment with the clients in the ranked clients map and assigns to each client an empty list.
     *
     * @param statefulTasksToRankedClients ranked clients map
     * @return initialised assignment with empty lists
     */
    private Map<String, List<TaskId>> initAssignment(final Map<TaskId, List<ClientIdAndLag>> statefulTasksToRankedClients) {
        final Map<String, List<TaskId>> assignment = new HashMap<>();
        final List<ClientIdAndLag> clientIdAndLags = statefulTasksToRankedClients.values().stream()
            .findFirst().orElseThrow(() -> new IllegalStateException("The list of clients and lags must not be empty"));
        for (final ClientIdAndLag clientIdAndLag : clientIdAndLags) {
            assignment.put(clientIdAndLag.clientId(), new ArrayList<>());
        }
        return assignment;
    }

    /**
     * Maps tasks to clients with caught-up states for the task.
     *
     * @param statefulTasksToRankedClients ranked clients map
     * @return map from tasks with caught-up clients to the list of client candidates
     */
    private Map<TaskId, List<String>> tasksToCaughtUpClients(final Map<TaskId, List<ClientIdAndLag>> statefulTasksToRankedClients) {
        final Map<TaskId, List<String>> taskToCaughtUpClients = new HashMap<>();
        for (final Map.Entry<TaskId, List<ClientIdAndLag>> taskToRankedClient : statefulTasksToRankedClients.entrySet()) {
            final List<ClientIdAndLag> clientRanking = taskToRankedClient.getValue();
            final List<String> caughtUpClients = new ArrayList<>();
            int i = 0;
            while (clientRanking.get(i).lag() == 0) {
                final TaskId taskId = taskToRankedClient.getKey();
                taskToCaughtUpClients
                    .computeIfAbsent(taskId, ignored -> caughtUpClients).add(clientRanking.get(i).clientId());
                ++i;
            }
            Collections.sort(caughtUpClients);
        }
        return taskToCaughtUpClients;
    }

    /**
     * Maps a task to the client that host the task according to the previous assignment.
     *
     * @return map from task IDs to clients hosting the corresponding task
     */
    private Map<TaskId, String> tasksToHostClients() {
        final Map<TaskId, String> tasksToHostClients = new HashMap<>();
        for (final Map.Entry<String, List<TaskId>> clientToTasks : previousAssignment.entrySet()) {
            for (final TaskId taskId : clientToTasks.getValue()) {
                tasksToHostClients.put(taskId, clientToTasks.getKey());
            }
        }
        return tasksToHostClients;
    }

    /**
     * Assigns task for which one or more caught-up clients exist to one of the caught-up clients.
     *
     * @param assignment assignment
     * @param unassignedTasksWithoutCaughtUpClients list of task that could not be assigned since no caught-up client exists for them
     * @param tasksToCaughtUpClients map from task IDs to lists of caught-up clients
     */
    private void assignTasksWithCaughtUpClients(final Map<String, List<TaskId>> assignment,
                                                final List<TaskId> unassignedTasksWithoutCaughtUpClients,
                                                final Map<TaskId, List<String>> tasksToCaughtUpClients) {
        // If a task has already been assigned to a caught-up client that still exists, assign it back to the client
        final Map<TaskId, String> tasksToHostClients = tasksToHostClients();
        final List<TaskId> unassignedTasksWithCaughtUpClients = new ArrayList<>();
        for (final Map.Entry<TaskId, List<String>> taskToCaughtUpClients : tasksToCaughtUpClients.entrySet()) {
            final TaskId taskId = taskToCaughtUpClients.getKey();
            final List<String> caughtUpClients = taskToCaughtUpClients.getValue();
            if (caughtUpClients != null && !caughtUpClients.isEmpty()) {
                final String clientHostingTask = tasksToHostClients.get(taskId);
                if (clientHostingTask != null && caughtUpClients.contains(clientHostingTask)) {
                    assignment.get(clientHostingTask).add(taskId);
                } else {
                    unassignedTasksWithCaughtUpClients.add(taskId);
                }
            } else {
                unassignedTasksWithoutCaughtUpClients.add(taskId);
            }
        }
        // If a task has not been assigned to a caught-up client, assign it to the caught-up client with the least tasks
        for (final TaskId taskId : unassignedTasksWithCaughtUpClients) {
            final List<String> caughtUpClients = tasksToCaughtUpClients.get(taskId);
            String clientWithLeastTasks = null;
            int taskCount = Integer.MAX_VALUE;
            for (final String client : caughtUpClients) {
                final int assignedTasksCount = assignment.get(client).size();
                if (taskCount > assignedTasksCount) {
                    clientWithLeastTasks = client;
                    taskCount = assignedTasksCount;
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
     * @param unassignedTasksWithoutCaughtUpClients list of task that could not be assigned since no caught-up client exists for them
     * @param statefulTasksToRankedClients ranked clients map
     */
    private void assignTasksWithoutCaughtUpClients(final Map<String, List<TaskId>> assignment,
                                                   final List<TaskId> unassignedTasksWithoutCaughtUpClients,
                                                   final Map<TaskId, List<ClientIdAndLag>> statefulTasksToRankedClients) {
        for (final TaskId taskId : unassignedTasksWithoutCaughtUpClients) {
            List<ClientIdAndLag> clientIdAndLags = statefulTasksToRankedClients.get(taskId);
            int previousLag = clientIdAndLags.get(0).lag();
            String clientWithLeastTasks = null;
            int taskCount = Integer.MAX_VALUE;
            int i = 0;
            while (clientIdAndLags.get(i).lag() == previousLag) {
                final String clientId = clientIdAndLags.get(i).clientId();
                final int assignedTasksCount = assignment.get(clientId).size();
                if (taskCount > assignedTasksCount) {
                    clientWithLeastTasks = clientId;
                    taskCount = assignedTasksCount;
                }
                ++i;
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
    private void balance(final Map<String, List<TaskId>> assignment,
                         final int balanceFactor,
                         final Map<TaskId, List<ClientIdAndLag>> statefulTasksToRankedClients,
                         final Map<TaskId, List<String>> tasksToCaughtUpClients) {
        final List<String> clients = new ArrayList<>(assignment.keySet());
        if (clients.isEmpty()) {
            throw new IllegalStateException("Clients within an assignment must not be empty");
        }
        Collections.sort(clients);
        for (final String client1 : clients) {
            for (final String client2 : clients) {
                final List<TaskId> taskIds1 = assignment.get(client1);
                final List<TaskId> taskIds2 =  assignment.get(client2);
                final List<TaskId> source = taskIds1.size() > taskIds2.size() ? taskIds1 : taskIds2;
                final List<TaskId> destination = source == taskIds1 ? taskIds2 : taskIds1;
                while (
                    source.size() - destination.size() > balanceFactor &&
                    !tasksToCaughtUpClients.containsKey(source.get(source.size() - 1))
                ) {
                    final TaskId taskToMove = source.get(source.size() - 1);
                    List<ClientIdAndLag> rankedClients = statefulTasksToRankedClients.get(taskToMove);
                    for (final ClientIdAndLag clientAndLag : rankedClients) {
                        final String clientId = clientAndLag.clientId();
                        final List<TaskId> assignedTasks = assignment.get(clientId);
                        if (assignment.get(clientId).size() < source.size() - 1) {
                            source.remove(taskToMove);
                            assignedTasks.add(taskToMove);
                            break;
                        }
                    }
                }
            }
        }
    }
}
