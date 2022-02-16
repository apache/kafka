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
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class StickyTaskAssignor implements TaskAssignor {

    private static final Logger log = LoggerFactory.getLogger(StickyTaskAssignor.class);
    private Map<UUID, ClientState> clients;
    private Set<TaskId> allTaskIds;
    private Set<TaskId> standbyTaskIds;
    private final Map<TaskId, UUID> previousActiveTaskAssignment = new HashMap<>();
    private final Map<TaskId, Set<UUID>> previousStandbyTaskAssignment = new HashMap<>();
    private TaskPairs taskPairs;

    private final boolean mustPreserveActiveTaskAssignment;

    public StickyTaskAssignor() {
        this(false);
    }

    StickyTaskAssignor(final boolean mustPreserveActiveTaskAssignment) {
        this.mustPreserveActiveTaskAssignment = mustPreserveActiveTaskAssignment;
    }

    @Override
    public boolean assign(final Map<UUID, ClientState> clients,
                          final Set<TaskId> allTaskIds,
                          final Set<TaskId> statefulTaskIds,
                          final AssignmentConfigs configs) {
        this.clients = clients;
        this.allTaskIds = allTaskIds;
        this.standbyTaskIds = statefulTaskIds;

        final int maxPairs = allTaskIds.size() * (allTaskIds.size() - 1) / 2;
        taskPairs = new TaskPairs(maxPairs);
        mapPreviousTaskAssignment(clients);

        assignActive();
        assignStandby(configs.numStandbyReplicas);
        return false;
    }

    private void assignStandby(final int numStandbyReplicas) {
        for (final TaskId taskId : standbyTaskIds) {
            for (int i = 0; i < numStandbyReplicas; i++) {
                final Set<UUID> ids = findClientsWithoutAssignedTask(taskId);
                if (ids.isEmpty()) {
                    log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                                     "There is not enough available capacity. You should " +
                                     "increase the number of threads and/or application instances " +
                                     "to maintain the requested number of standby replicas.",
                             numStandbyReplicas - i,
                             numStandbyReplicas, taskId);
                    break;
                }
                allocateTaskWithClientCandidates(taskId, ids, false);
            }
        }
    }

    private void assignActive() {
        final int totalCapacity = sumCapacity(clients.values());
        final int tasksPerThread = allTaskIds.size() / totalCapacity;
        final Set<TaskId> assigned = new HashSet<>();

        // first try and re-assign existing active tasks to clients that previously had
        // the same active task
        for (final Map.Entry<TaskId, UUID> entry : previousActiveTaskAssignment.entrySet()) {
            final TaskId taskId = entry.getKey();
            if (allTaskIds.contains(taskId)) {
                final ClientState client = clients.get(entry.getValue());
                if (mustPreserveActiveTaskAssignment || client.hasUnfulfilledQuota(tasksPerThread)) {
                    assignTaskToClient(assigned, taskId, client);
                }
            }
        }

        final Set<TaskId> unassigned = new HashSet<>(allTaskIds);
        unassigned.removeAll(assigned);

        // try and assign any remaining unassigned tasks to clients that previously
        // have seen the task.
        for (final Iterator<TaskId> iterator = unassigned.iterator(); iterator.hasNext(); ) {
            final TaskId taskId = iterator.next();
            final Set<UUID> clientIds = previousStandbyTaskAssignment.get(taskId);
            if (clientIds != null) {
                for (final UUID clientId : clientIds) {
                    final ClientState client = clients.get(clientId);
                    if (client.hasUnfulfilledQuota(tasksPerThread)) {
                        assignTaskToClient(assigned, taskId, client);
                        iterator.remove();
                        break;
                    }
                }
            }
        }

        // assign any remaining unassigned tasks
        final List<TaskId> sortedTasks = new ArrayList<>(unassigned);
        Collections.sort(sortedTasks);
        for (final TaskId taskId : sortedTasks) {
            allocateTaskWithClientCandidates(taskId, clients.keySet(), true);
        }
    }

    private void allocateTaskWithClientCandidates(final TaskId taskId, final Set<UUID> clientsWithin, final boolean active) {
        final ClientState client = findClient(taskId, clientsWithin);
        taskPairs.addPairs(taskId, client.assignedTasks());
        if (active) {
            client.assignActive(taskId);
        } else {
            client.assignStandby(taskId);
        }
    }

    private void assignTaskToClient(final Set<TaskId> assigned, final TaskId taskId, final ClientState client) {
        taskPairs.addPairs(taskId, client.assignedTasks());
        client.assignActive(taskId);
        assigned.add(taskId);
    }

    private Set<UUID> findClientsWithoutAssignedTask(final TaskId taskId) {
        final Set<UUID> clientIds = new HashSet<>();
        for (final Map.Entry<UUID, ClientState> client : clients.entrySet()) {
            if (!client.getValue().hasAssignedTask(taskId)) {
                clientIds.add(client.getKey());
            }
        }
        return clientIds;
    }


    private ClientState findClient(final TaskId taskId, final Set<UUID> clientsWithin) {

        // optimize the case where there is only 1 id to search within.
        if (clientsWithin.size() == 1) {
            return clients.get(clientsWithin.iterator().next());
        }

        final ClientState previous = findClientsWithPreviousAssignedTask(taskId, clientsWithin);
        if (previous == null) {
            return leastLoaded(taskId, clientsWithin);
        }

        if (shouldBalanceLoad(previous)) {
            final ClientState standby = findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
            if (standby == null || shouldBalanceLoad(standby)) {
                return leastLoaded(taskId, clientsWithin);
            }
            return standby;
        }

        return previous;
    }

    private boolean shouldBalanceLoad(final ClientState client) {
        return client.reachedCapacity() && hasClientsWithMoreAvailableCapacity(client);
    }

    private boolean hasClientsWithMoreAvailableCapacity(final ClientState client) {
        for (final ClientState clientState : clients.values()) {
            if (clientState.hasMoreAvailableCapacityThan(client)) {
                return true;
            }
        }
        return false;
    }

    private ClientState findClientsWithPreviousAssignedTask(final TaskId taskId, final Set<UUID> clientsWithin) {
        final UUID previous = previousActiveTaskAssignment.get(taskId);
        if (previous != null && clientsWithin.contains(previous)) {
            return clients.get(previous);
        }
        return findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
    }

    private ClientState findLeastLoadedClientWithPreviousStandByTask(final TaskId taskId, final Set<UUID> clientsWithin) {
        final Set<UUID> ids = previousStandbyTaskAssignment.get(taskId);
        if (ids == null) {
            return null;
        }
        final HashSet<UUID> constrainTo = new HashSet<>(ids);
        constrainTo.retainAll(clientsWithin);
        return leastLoaded(taskId, constrainTo);
    }

    private ClientState leastLoaded(final TaskId taskId, final Set<UUID> clientIds) {
        final ClientState leastLoaded = findLeastLoaded(taskId, clientIds, true);
        if (leastLoaded == null) {
            return findLeastLoaded(taskId, clientIds, false);
        }
        return leastLoaded;
    }

    private ClientState findLeastLoaded(final TaskId taskId,
                                        final Set<UUID> clientIds,
                                        final boolean checkTaskPairs) {
        ClientState leastLoaded = null;
        for (final UUID id : clientIds) {
            final ClientState client = clients.get(id);
            if (client.assignedTaskCount() == 0) {
                return client;
            }

            if (leastLoaded == null || client.hasMoreAvailableCapacityThan(leastLoaded)) {
                if (!checkTaskPairs) {
                    leastLoaded = client;
                } else if (taskPairs.hasNewPair(taskId, client.assignedTasks())) {
                    leastLoaded = client;
                }
            }

        }
        return leastLoaded;

    }

    private void mapPreviousTaskAssignment(final Map<UUID, ClientState> clients) {
        for (final Map.Entry<UUID, ClientState> clientState : clients.entrySet()) {
            for (final TaskId activeTask : clientState.getValue().prevActiveTasks()) {
                previousActiveTaskAssignment.put(activeTask, clientState.getKey());
            }

            for (final TaskId prevAssignedTask : clientState.getValue().prevStandbyTasks()) {
                previousStandbyTaskAssignment.computeIfAbsent(prevAssignedTask, t -> new HashSet<>());
                previousStandbyTaskAssignment.get(prevAssignedTask).add(clientState.getKey());
            }
        }

    }

    private int sumCapacity(final Collection<ClientState> values) {
        int capacity = 0;
        for (final ClientState client : values) {
            capacity += client.capacity();
        }
        return capacity;
    }

    private static class TaskPairs {
        private final Set<Pair> pairs;
        private final int maxPairs;

        TaskPairs(final int maxPairs) {
            this.maxPairs = maxPairs;
            this.pairs = new HashSet<>(maxPairs);
        }

        boolean hasNewPair(final TaskId task1,
                           final Set<TaskId> taskIds) {
            if (pairs.size() == maxPairs) {
                return false;
            }
            for (final TaskId taskId : taskIds) {
                if (!pairs.contains(pair(task1, taskId))) {
                    return true;
                }
            }
            return false;
        }

        void addPairs(final TaskId taskId, final Set<TaskId> assigned) {
            for (final TaskId id : assigned) {
                pairs.add(pair(id, taskId));
            }
        }

        Pair pair(final TaskId task1, final TaskId task2) {
            if (task1.compareTo(task2) < 0) {
                return new Pair(task1, task2);
            }
            return new Pair(task2, task1);
        }

        private static class Pair {
            private final TaskId task1;
            private final TaskId task2;

            Pair(final TaskId task1, final TaskId task2) {
                this.task1 = task1;
                this.task2 = task2;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final Pair pair = (Pair) o;
                return Objects.equals(task1, pair.task1) &&
                        Objects.equals(task2, pair.task2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(task1, task2);
            }
        }


    }

}
