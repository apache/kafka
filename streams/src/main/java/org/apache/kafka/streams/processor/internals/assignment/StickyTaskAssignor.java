/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class StickyTaskAssignor<ID> implements TaskAssignor<ID, TaskId> {

    private static final Logger log = LoggerFactory.getLogger(StickyTaskAssignor.class);
    private final Map<ID, ClientState<TaskId>> clients;
    private final Set<TaskId> taskIds;
    private final Map<TaskId, ID> previousActiveTaskAssignment = new HashMap<>();
    private final Map<TaskId, Set<ID>> previousStandbyTaskAssignment = new HashMap<>();
    private final TaskPairs taskPairs;
    private final int availableCapacity;
    private final boolean hasNewTasks;

    public StickyTaskAssignor(final Map<ID, ClientState<TaskId>> clients, final Set<TaskId> taskIds) {
        this.clients = clients;
        this.taskIds = taskIds;
        this.availableCapacity = sumCapacity(clients.values());
        taskPairs = new TaskPairs(taskIds.size() * (taskIds.size() - 1) / 2);
        mapPreviousTaskAssignment(clients);
        this.hasNewTasks = !previousActiveTaskAssignment.keySet().containsAll(taskIds);
    }

    @Override
    public void assign(final int numStandbyReplicas) {
        assignActive();
        assignStandby(numStandbyReplicas);
    }

    private void assignStandby(final int numStandbyReplicas) {
        for (final TaskId taskId : taskIds) {
            for (int i = 0; i < numStandbyReplicas; i++) {
                final Set<ID> ids = findClientsWithoutAssignedTask(taskId);
                if (ids.isEmpty()) {
                    log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                                     "There is not enough available capacity. You should " +
                                     "increase the number of threads and/or application instances " +
                                     "to maintain the requested number of standby replicas.",
                             numStandbyReplicas - i,
                             numStandbyReplicas, taskId);
                    break;
                }
                assign(taskId, ids, false);
            }
        }
    }

    private void assignActive() {
        final Set<TaskId> previouslyAssignedTaskIds = new HashSet<>(previousActiveTaskAssignment.keySet());
        previouslyAssignedTaskIds.addAll(previousStandbyTaskAssignment.keySet());
        previouslyAssignedTaskIds.retainAll(taskIds);

        // assign previously assigned tasks first
        for (final TaskId taskId : previouslyAssignedTaskIds) {
            assign(taskId, clients.keySet(), true);
        }

        final Set<TaskId> newTasks  = new HashSet<>(taskIds);
        newTasks.removeAll(previouslyAssignedTaskIds);

        for (final TaskId taskId : newTasks) {
            assign(taskId, clients.keySet(), true);
        }
    }

    private void assign(final TaskId taskId, final Set<ID> clientsWithin, final boolean active) {
        final ClientState<TaskId> client = findClient(taskId, clientsWithin);
        taskPairs.addPairs(taskId, client.assignedTasks());
        client.assign(taskId, active);
    }

    private Set<ID> findClientsWithoutAssignedTask(final TaskId taskId) {
        final Set<ID> clientIds = new HashSet<>();
        for (final Map.Entry<ID, ClientState<TaskId>> client : clients.entrySet()) {
            if (!client.getValue().hasAssignedTask(taskId)) {
                clientIds.add(client.getKey());
            }
        }
        return clientIds;
    }


    private ClientState<TaskId> findClient(final TaskId taskId,
                                           final Set<ID> clientsWithin) {
        // optimize the case where there is only 1 id to search within.
        if (clientsWithin.size() == 1) {
            return clients.get(clientsWithin.iterator().next());
        }

        final ClientState<TaskId> previous = findClientsWithPreviousAssignedTask(taskId, clientsWithin);
        if (previous == null) {
            return leastLoaded(taskId, clientsWithin);
        }

        if (shouldBalanceLoad(previous)) {
            final ClientState<TaskId> standby = findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
            if (standby == null
                    || shouldBalanceLoad(standby)) {
                return leastLoaded(taskId, clientsWithin);
            }
            return standby;
        }

        return previous;
    }

    private boolean shouldBalanceLoad(final ClientState<TaskId> client) {
        return !hasNewTasks
                && client.reachedCapacity()
                && hasClientsWithMoreAvailableCapacity(client);
    }

    private boolean hasClientsWithMoreAvailableCapacity(final ClientState<TaskId> client) {
        for (ClientState<TaskId> clientState : clients.values()) {
            if (clientState.hasMoreAvailableCapacityThan(client)) {
                return true;
            }
        }
        return false;
    }

    private ClientState<TaskId> findClientsWithPreviousAssignedTask(final TaskId taskId,
                                                                    final Set<ID> clientsWithin) {
        final ID previous = previousActiveTaskAssignment.get(taskId);
        if (previous != null && clientsWithin.contains(previous)) {
            return clients.get(previous);
        }
        return findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
    }

    private ClientState<TaskId> findLeastLoadedClientWithPreviousStandByTask(final TaskId taskId, final Set<ID> clientsWithin) {
        final Set<ID> ids = previousStandbyTaskAssignment.get(taskId);
        if (ids == null) {
            return null;
        }
        final HashSet<ID> constrainTo = new HashSet<>(ids);
        constrainTo.retainAll(clientsWithin);
        return leastLoaded(taskId, constrainTo);
    }

    private ClientState<TaskId> leastLoaded(final TaskId taskId, final Set<ID> clientIds) {
        final ClientState<TaskId> leastLoaded = findLeastLoaded(taskId, clientIds, true);
        if (leastLoaded == null) {
            return findLeastLoaded(taskId, clientIds, false);
        }
        return leastLoaded;
    }

    private ClientState<TaskId> findLeastLoaded(final TaskId taskId,
                                                final Set<ID> clientIds,
                                                boolean checkTaskPairs) {
        ClientState<TaskId> leastLoaded = null;
        for (final ID id : clientIds) {
            final ClientState<TaskId> client = clients.get(id);
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

    private void mapPreviousTaskAssignment(final Map<ID, ClientState<TaskId>> clients) {
        for (final Map.Entry<ID, ClientState<TaskId>> clientState : clients.entrySet()) {
            for (final TaskId activeTask : clientState.getValue().previousActiveTasks()) {
                previousActiveTaskAssignment.put(activeTask, clientState.getKey());
            }

            for (final TaskId prevAssignedTask : clientState.getValue().previousStandbyTasks()) {
                if (!previousStandbyTaskAssignment.containsKey(prevAssignedTask)) {
                    previousStandbyTaskAssignment.put(prevAssignedTask, new HashSet<ID>());
                }
                previousStandbyTaskAssignment.get(prevAssignedTask).add(clientState.getKey());
            }
        }
    }

    private int sumCapacity(final Collection<ClientState<TaskId>> values) {
        int capacity = 0;
        for (ClientState<TaskId> client : values) {
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

        boolean hasNewPair(final TaskId task1, final Set<TaskId> taskIds) {
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

        class Pair {
            private final TaskId task1;
            private final TaskId task2;

            Pair(final TaskId task1, final TaskId task2) {
                this.task1 = task1;
                this.task2 = task2;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
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
