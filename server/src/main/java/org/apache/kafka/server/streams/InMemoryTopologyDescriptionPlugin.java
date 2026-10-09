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
package org.apache.kafka.server.streams;

import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reference {@link StreamsGroupTopologyDescriptionPlugin} that stores topology descriptions
 * in an in-memory map. Intended for testing and as a starting point for real implementations;
 * not suitable for production because its state is lost on broker restart and is not shared
 * across brokers.
 *
 * <p>Stores at most one description per group, identified by its topology epoch. A
 * {@code setTopology} call overwrites any previously stored description for that group;
 * {@code getTopology} returns the stored description only when the requested topology epoch
 * matches the stored epoch, and {@code null} otherwise.
 */
public class InMemoryTopologyDescriptionPlugin implements StreamsGroupTopologyDescriptionPlugin {

    private record Entry(int topologyEpoch, StreamsGroupTopologyDescription description) { }

    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        // No-op.
    }

    @Override
    public CompletableFuture<Void> setTopology(String groupId, int topologyEpoch, StreamsGroupTopologyDescription description) {
        store.put(groupId, new Entry(topologyEpoch, description));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteTopology(String groupId) {
        store.remove(groupId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<StreamsGroupTopologyDescription> getTopology(String groupId, int topologyEpoch) {
        Entry entry = store.get(groupId);
        if (entry == null || entry.topologyEpoch() != topologyEpoch) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(entry.description());
    }

    @Override
    public void close() {
        store.clear();
    }
}
