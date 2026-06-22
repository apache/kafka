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
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A simple in-memory implementation of {@link StreamsGroupTopologyDescriptionPlugin}
 * that stores topology descriptions keyed by groupId, retaining only the most recent
 * topology epoch per group.
 *
 * <p>This implementation maintains a static registry of all instances, accessible via
 * {@link #instances()}, to support integration tests that need to verify plugin state.
 */
public class InMemoryTopologyDescriptionPlugin implements StreamsGroupTopologyDescriptionPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryTopologyDescriptionPlugin.class);
    /** Config key tests can set on the broker to tag a cluster; used by {@link #instanceForClusterId}. */
    public static final String TEST_CLUSTER_ID_CONFIG = "streams.topology.plugin.test.cluster.id";

    private static final List<InMemoryTopologyDescriptionPlugin> INSTANCES = new CopyOnWriteArrayList<>();
    private static final ConcurrentHashMap<String, InMemoryTopologyDescriptionPlugin> INSTANCES_BY_CLUSTER_ID = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, InMemoryTopologyDescriptionPlugin> INSTANCES_BY_PORT = new ConcurrentHashMap<>();
    private static final Pattern PORT_PATTERN = Pattern.compile(":(\\d+)(?:,|$)");

    private final ConcurrentHashMap<String, TopologyEntry> topologies = new ConcurrentHashMap<>();
    private final AtomicInteger setTopologyAttemptCount = new AtomicInteger(0);
    private final AtomicInteger setTopologyCount = new AtomicInteger(0);
    private final AtomicInteger getTopologyCount = new AtomicInteger(0);
    private final AtomicInteger deleteTopologyCount = new AtomicInteger(0);
    private volatile boolean failOnSet = false;
    private volatile boolean failOnSetPermanent = false;
    private volatile boolean failOnGet = false;

    private static class TopologyEntry {
        final int topologyEpoch;
        final StreamsGroupTopologyDescription description;

        TopologyEntry(int topologyEpoch, StreamsGroupTopologyDescription description) {
            this.topologyEpoch = topologyEpoch;
            this.description = description;
        }
    }

    /**
     * Returns all plugin instances created during this JVM's lifetime.
     * Useful for integration tests that need to verify plugin state.
     */
    public static List<InMemoryTopologyDescriptionPlugin> instances() {
        return INSTANCES;
    }

    /**
     * Clears the static instance registry. Should be called in test setup/teardown.
     */
    public static void clearInstances() {
        INSTANCES.clear();
        INSTANCES_BY_PORT.clear();
        INSTANCES_BY_CLUSTER_ID.clear();
    }

    /**
     * Returns the plugin instance tagged with the given cluster ID (set via
     * {@link #TEST_CLUSTER_ID_CONFIG} in the broker config). Useful when multiple clusters
     * run in the same JVM concurrently and tests need to distinguish their plugin instances.
     */
    public static InMemoryTopologyDescriptionPlugin instanceForClusterId(String clusterId) {
        return INSTANCES_BY_CLUSTER_ID.get(clusterId);
    }

    /**
     * Returns the plugin instance that was configured for the broker listening on the port
     * extracted from the given bootstrap servers string (e.g. {@code "localhost:9092"}).
     */
    public static InMemoryTopologyDescriptionPlugin instanceForBootstrap(String bootstrapServers) {
        Matcher m = PORT_PATTERN.matcher(bootstrapServers + ",");
        if (m.find()) {
            return INSTANCES_BY_PORT.get(Integer.parseInt(m.group(1)));
        }
        return null;
    }

    /**
     * Returns the stored topology description for the given group, if present.
     * Test helper — bypasses the async plugin contract.
     */
    public Optional<StreamsGroupTopologyDescription> storedTopology(String groupId) {
        TopologyEntry entry = topologies.get(groupId);
        return entry != null ? Optional.of(entry.description) : Optional.empty();
    }

    /**
     * Sets whether {@link #setTopology} should fail with an exception.
     * Useful for integration tests that need to verify plugin error handling.
     */
    public void setFailOnSet(boolean fail) {
        this.failOnSet = fail;
    }

    /**
     * Sets whether {@link #setTopology} should fail specifically with
     * {@link StreamsTopologyDescriptionPermanentFailureException}.
     * The broker treats this as a permanent failure and persists
     * {@code FailedDescriptionTopologyEpoch}; integration tests use it to verify the
     * hot-loop ratchet.
     */
    public void setFailOnSetPermanent(boolean fail) {
        this.failOnSetPermanent = fail;
    }

    /**
     * Sets whether {@link #getTopology} should fail with an exception.
     * Useful for integration tests that need to verify read-path error handling.
     */
    public void setFailOnGet(boolean fail) {
        this.failOnGet = fail;
    }

    /**
     * Returns the number of times {@link #setTopology} was called successfully (without failOnSet).
     */
    public int getSetTopologyCount() {
        return setTopologyCount.get();
    }

    /**
     * Returns the total number of times {@link #setTopology} was invoked, including attempts that
     * failed because of {@link #setFailOnSet} or {@link #setFailOnSetPermanent}. Integration tests
     * use this to verify that the broker stops re-soliciting after a permanent failure.
     */
    public int getSetTopologyAttemptCount() {
        return setTopologyAttemptCount.get();
    }

    /**
     * Returns the number of times {@link #getTopology} was called (regardless of outcome).
     */
    public int getGetTopologyCount() {
        return getTopologyCount.get();
    }

    /**
     * Returns the number of times {@link #deleteTopology} was called.
     */
    public int getDeleteTopologyCount() {
        return deleteTopologyCount.get();
    }

    /**
     * Returns the stored topology epoch for the given group, or -1 if not present.
     */
    public int storedDescriptionTopologyEpoch(String groupId) {
        TopologyEntry entry = topologies.get(groupId);
        return entry != null ? entry.topologyEpoch : -1;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        INSTANCES.add(this);
        Object clusterId = configs.get(TEST_CLUSTER_ID_CONFIG);
        if (clusterId != null) {
            INSTANCES_BY_CLUSTER_ID.put(clusterId.toString(), this);
        }
        Object listeners = configs.get("listeners");
        if (listeners != null) {
            Matcher m = PORT_PATTERN.matcher(listeners.toString() + ",");
            if (m.find()) {
                INSTANCES_BY_PORT.put(Integer.parseInt(m.group(1)), this);
            }
        }
        LOG.info("InMemoryTopologyDescriptionPlugin configured");
    }

    @Override
    public CompletableFuture<Void> setTopology(String groupId, int topologyEpoch,
                                                StreamsGroupTopologyDescription description) {
        setTopologyAttemptCount.incrementAndGet();
        if (failOnSetPermanent) {
            return CompletableFuture.failedFuture(
                new StreamsTopologyDescriptionPermanentFailureException("Simulated permanent failure"));
        }
        if (failOnSet) {
            return CompletableFuture.failedFuture(new RuntimeException("Simulated plugin error"));
        }
        setTopologyCount.incrementAndGet();
        topologies.put(groupId, new TopologyEntry(topologyEpoch, description));
        LOG.info("Stored topology description for group {} (epoch={})", groupId, topologyEpoch);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteTopology(String groupId) {
        deleteTopologyCount.incrementAndGet();
        topologies.remove(groupId);
        LOG.info("Deleted topology description for group {}", groupId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<StreamsGroupTopologyDescription> getTopology(String groupId, int topologyEpoch) {
        getTopologyCount.incrementAndGet();
        if (failOnGet) {
            return CompletableFuture.failedFuture(new RuntimeException("Simulated plugin get error"));
        }
        TopologyEntry entry = topologies.get(groupId);
        if (entry == null || entry.topologyEpoch != topologyEpoch) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(entry.description);
    }

    @Override
    public void close() {
        LOG.info("Closing InMemoryTopologyDescriptionPlugin");
        topologies.clear();
    }
}
