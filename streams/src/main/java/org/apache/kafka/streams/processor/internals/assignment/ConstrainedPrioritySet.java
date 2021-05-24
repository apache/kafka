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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Wraps a priority queue of clients and returns the next valid candidate(s) based on the current task assignment
 */
class ConstrainedPrioritySet {

    private final PriorityQueue<UUID> clientsByTaskLoad;
    private final BiFunction<UUID, TaskId, Boolean> constraint;
    private final Set<UUID> uniqueClients = new HashSet<>();

    ConstrainedPrioritySet(final BiFunction<UUID, TaskId, Boolean> constraint,
                           final Function<UUID, Double> weight) {
        this.constraint = constraint;
        clientsByTaskLoad = new PriorityQueue<>(Comparator.comparing(weight).thenComparing(clientId -> clientId));
    }

    /**
     * @return the next least loaded client that satisfies the given criteria, or null if none do
     */
    UUID poll(final TaskId task, final Function<UUID, Boolean> extraConstraint) {
        final Set<UUID> invalidPolledClients = new HashSet<>();
        while (!clientsByTaskLoad.isEmpty()) {
            final UUID candidateClient = pollNextClient();
            if (constraint.apply(candidateClient, task) && extraConstraint.apply(candidateClient)) {
                // then we found the lightest, valid client
                offerAll(invalidPolledClients);
                return candidateClient;
            } else {
                // remember this client and try again later
                invalidPolledClients.add(candidateClient);
            }
        }
        // we tried all the clients, and none met the constraint (or there are no clients)
        offerAll(invalidPolledClients);
        return null;
    }

    /**
     * @return the next least loaded client that satisfies the given criteria, or null if none do
     */
    UUID poll(final TaskId task) {
        return poll(task, client -> true);
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
        final UUID client = clientsByTaskLoad.remove();
        uniqueClients.remove(client);
        return client;
    }
}
