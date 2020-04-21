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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import org.apache.kafka.streams.processor.TaskId;

/**
 * Wraps a priority queue of clients and returns the next valid candidate(s) based on the current task assignment
 */
class ValidClientsByTaskLoadQueue {

    private final PriorityQueue<UUID> clientsByTaskLoad;
    private final BiFunction<UUID, TaskId, Boolean> validClientCriteria;
    private final Set<UUID> uniqueClients = new HashSet<>();

    ValidClientsByTaskLoadQueue(final Map<UUID, ClientState> clientStates,
                                final BiFunction<UUID, TaskId, Boolean> validClientCriteria) {
        this.validClientCriteria = validClientCriteria;

        clientsByTaskLoad = new PriorityQueue<>(
            (client, other) -> {
                final double clientTaskLoad = clientStates.get(client).taskLoad();
                final double otherTaskLoad = clientStates.get(other).taskLoad();
                if (clientTaskLoad < otherTaskLoad) {
                    return -1;
                } else if (clientTaskLoad > otherTaskLoad) {
                    return 1;
                } else {
                    return client.compareTo(other);
                }
            });
    }

    /**
     * @return the next least loaded client that satisfies the given criteria, or null if none do
     */
    UUID poll(final TaskId task) {
        final List<UUID> validClient = poll(task, 1);
        return validClient.isEmpty() ? null : validClient.get(0);
    }

    /**
     * @return the next N <= {@code numClientsPerTask} clients in the underlying priority queue that are valid candidates for the given task
     */
    List<UUID> poll(final TaskId task, final int numClients) {
        final List<UUID> nextLeastLoadedValidClients = new LinkedList<>();
        final Set<UUID> invalidPolledClients = new HashSet<>();
        while (nextLeastLoadedValidClients.size() < numClients) {
            UUID candidateClient;
            while (true) {
                candidateClient = pollNextClient();
                if (candidateClient == null) {
                    offerAll(invalidPolledClients);
                    return nextLeastLoadedValidClients;
                }

                if (validClientCriteria.apply(candidateClient, task)) {
                    nextLeastLoadedValidClients.add(candidateClient);
                    break;
                } else {
                    invalidPolledClients.add(candidateClient);
                }
            }
        }
        offerAll(invalidPolledClients);
        return nextLeastLoadedValidClients;
    }

    void offerAll(final Collection<UUID> clients) {
        for (final UUID client : clients) {
            offer(client);
        }
    }

    void offer(final UUID client) {
        if (uniqueClients.contains(client)) {
            clientsByTaskLoad.remove(client);
        } else {
            uniqueClients.add(client);
        }
        clientsByTaskLoad.offer(client);
    }

    private UUID pollNextClient() {
        final UUID client = clientsByTaskLoad.poll();
        uniqueClients.remove(client);
        return client;
    }
}
