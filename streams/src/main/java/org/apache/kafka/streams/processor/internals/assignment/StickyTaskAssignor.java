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
import java.util.Set;

public class StickyTaskAssignor<ID> implements TaskAssignor<ID, TaskId> {

    private static final Logger log = LoggerFactory.getLogger(StickyTaskAssignor.class);
    private final Map<ID, ClientState<TaskId>> clients;
    private final Set<TaskId> taskIds;
    private final Map<TaskId, ID> previousActiveTaskAssignment = new HashMap<>();
    private final Map<TaskId, Set<ID>> previousStandbyTaskAssignment = new HashMap<>();
    private final int availableCapacity;

    public StickyTaskAssignor(final Map<ID, ClientState<TaskId>> clients, final Set<TaskId> taskIds) {
        this.clients = clients;
        this.taskIds = taskIds;
        this.availableCapacity = sumCapacity(clients.values());
        mapPreviousTaskAssignment(clients);
    }

    @Override
    public void assign(final int numStandbyReplicas) {
        assignActive();
        assignStandby(numStandbyReplicas);
    }

    // Visible for testing
    void assignStandby(final int numStandbyReplicas) {
        for (int i = 0; i < numStandbyReplicas; i++) {
            for (final TaskId taskId : taskIds) {
                final Set<ID> ids = findClientsWithoutAssignedTask(taskId);
                if (ids.isEmpty()) {
                    log.warn("Unable to assign replica for task [{}]", taskId);
                    continue;
                }
                final ClientState<TaskId> client = findClient(taskId, ids);
                client.assign(taskId, false);
            }
        }
    }

    private void assignActive() {
        for (final TaskId taskId : taskIds) {
            final ClientState<TaskId> client = findClient(taskId, clients.keySet());
            client.assign(taskId, true);
        }
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


    private ClientState<TaskId> findClient(final TaskId taskId, final Set<ID> clientsWithin) {
        // optimize the case where there is only 1 id to search within.
        if (clientsWithin.size() == 1) {
            return clients.get(clientsWithin.iterator().next());
        }

        final ClientState<TaskId> previous = findClientsWithPreviousAssignedTask(taskId, clientsWithin);
        if (previous == null) {
            return leastLoaded(clientsWithin);
        }

        if (overCapacity(previous)) {
            final ClientState<TaskId> standby = findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
            if (standby == null || standby.reachedCapacity()) {
                return leastLoaded(clientsWithin);
            }
            return standby;
        }

        return previous;
    }

    private boolean overCapacity(final ClientState<TaskId> previous) {
        return previous.reachedCapacity() && taskIds.size() <= availableCapacity;
    }

    private ClientState<TaskId> findClientsWithPreviousAssignedTask(final TaskId taskId, final Set<ID> clientsWithin) {
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
        return leastLoaded(constrainTo);
    }

    private ClientState<TaskId> leastLoaded(final Set<ID> clientIds) {
        ClientState<TaskId> leastLoaded = null;
        for (final ID id : clientIds) {
            final ClientState<TaskId> client = clients.get(id);
            if (client.assignedTaskCount() == 0) {
                return client;
            }
            if (leastLoaded == null || client.hasMoreAvailableCapacityThan(leastLoaded)) {
                leastLoaded = client;
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
}
