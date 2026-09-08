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
package org.apache.kafka.streams.integration.utils;

import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;
import org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link InMemoryTopologyDescriptionPlugin} for integration tests that records every
 * {@code setTopology} / {@code deleteTopology} invocation per group and can be told to
 * reject pushes for selected groups with a permanent failure.
 *
 * <p>The brokers of an embedded cluster run in the test JVM, so the recorded state is kept
 * in static fields the test can inspect directly. Call {@link #reset()} between tests.
 */
public class TrackingTopologyDescriptionPlugin extends InMemoryTopologyDescriptionPlugin {

    private static final Map<String, AtomicInteger> SET_TOPOLOGY_CALLS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> DELETE_TOPOLOGY_CALLS = new ConcurrentHashMap<>();
    private static final Set<String> FAIL_PERMANENTLY_GROUPS = ConcurrentHashMap.newKeySet();

    public static void reset() {
        SET_TOPOLOGY_CALLS.clear();
        DELETE_TOPOLOGY_CALLS.clear();
        FAIL_PERMANENTLY_GROUPS.clear();
    }

    /**
     * Make every subsequent {@code setTopology} for this group fail with a
     * {@link StreamsTopologyDescriptionPermanentFailureException}.
     */
    public static void failPermanently(final String groupId) {
        FAIL_PERMANENTLY_GROUPS.add(groupId);
    }

    public static int setTopologyCalls(final String groupId) {
        final AtomicInteger calls = SET_TOPOLOGY_CALLS.get(groupId);
        return calls == null ? 0 : calls.get();
    }

    public static int deleteTopologyCalls(final String groupId) {
        final AtomicInteger calls = DELETE_TOPOLOGY_CALLS.get(groupId);
        return calls == null ? 0 : calls.get();
    }

    @Override
    public CompletableFuture<Void> setTopology(final String groupId, final int topologyEpoch, final StreamsGroupTopologyDescription description) {
        SET_TOPOLOGY_CALLS.computeIfAbsent(groupId, id -> new AtomicInteger()).incrementAndGet();
        if (FAIL_PERMANENTLY_GROUPS.contains(groupId)) {
            return CompletableFuture.failedFuture(
                new StreamsTopologyDescriptionPermanentFailureException("Topology description rejected by test plugin."));
        }
        return super.setTopology(groupId, topologyEpoch, description);
    }

    @Override
    public CompletableFuture<Void> deleteTopology(final String groupId) {
        DELETE_TOPOLOGY_CALLS.computeIfAbsent(groupId, id -> new AtomicInteger()).incrementAndGet();
        return super.deleteTopology(groupId);
    }
}
