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
package kafka.server;

import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionTransientFailureException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only {@link StreamsGroupTopologyDescriptionPlugin} whose {@code setTopology} and
 * {@code deleteTopology} failures are toggled at runtime, so broker integration tests can
 * exercise permanent/transient plugin-failure surfacing without a real backend. Calls that
 * are not configured to fail are delegated to an in-memory store, like
 * {@link InMemoryTopologyDescriptionPlugin}.
 *
 * <p>The broker, not the test, creates the plugin instance, so the failure toggles are
 * static. Callers must {@link #reset()} before relying on this plugin, since the JVM-wide
 * state otherwise leaks across test methods sharing this class.
 */
public class FailingTopologyDescriptionPlugin implements StreamsGroupTopologyDescriptionPlugin {

    public enum SetTopologyFailureMode {
        NONE, PERMANENT, TRANSIENT
    }

    private static final AtomicReference<SetTopologyFailureMode> SET_TOPOLOGY_FAILURE_MODE =
        new AtomicReference<>(SetTopologyFailureMode.NONE);
    private static final AtomicReference<RuntimeException> DELETE_TOPOLOGY_FAILURE =
        new AtomicReference<>(null);

    private record Entry(int topologyEpoch, StreamsGroupTopologyDescription description) { }

    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();

    public static void failNextSetTopology(SetTopologyFailureMode mode) {
        SET_TOPOLOGY_FAILURE_MODE.set(mode);
    }

    public static void failDeleteTopologyWith(RuntimeException exception) {
        DELETE_TOPOLOGY_FAILURE.set(exception);
    }

    public static void reset() {
        SET_TOPOLOGY_FAILURE_MODE.set(SetTopologyFailureMode.NONE);
        DELETE_TOPOLOGY_FAILURE.set(null);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No-op.
    }

    @Override
    public CompletableFuture<Void> setTopology(String groupId, int topologyEpoch, StreamsGroupTopologyDescription description) {
        switch (SET_TOPOLOGY_FAILURE_MODE.get()) {
            case PERMANENT:
                return CompletableFuture.failedFuture(
                    new StreamsTopologyDescriptionPermanentFailureException("topology rejected by test plugin"));
            case TRANSIENT:
                return CompletableFuture.failedFuture(
                    new StreamsTopologyDescriptionTransientFailureException("backend offline"));
            default:
                store.put(groupId, new Entry(topologyEpoch, description));
                return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<Void> deleteTopology(String groupId) {
        RuntimeException failure = DELETE_TOPOLOGY_FAILURE.get();
        if (failure != null) {
            return CompletableFuture.failedFuture(failure);
        }
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
