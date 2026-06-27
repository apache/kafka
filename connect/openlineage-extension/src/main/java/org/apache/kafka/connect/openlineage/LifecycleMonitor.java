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

package org.apache.kafka.connect.openlineage;

import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.health.ConnectorHealth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background thread that polls {@link ConnectClusterState} at a configurable
 * interval and emits OpenLineage lifecycle events when connector state
 * transitions are detected.
 *
 * <p>State transitions that produce events:
 * <ul>
 *   <li><b>New RUNNING connector</b> &rarr; {@code START}</li>
 *   <li><b>RUNNING &rarr; PAUSED</b> &rarr; {@code COMPLETE}</li>
 *   <li><b>RUNNING &rarr; deleted</b> &rarr; {@code COMPLETE}</li>
 *   <li><b>RUNNING &rarr; FAILED</b> &rarr; {@code FAIL}</li>
 *   <li><b>PAUSED &rarr; RUNNING (resumed)</b> &rarr; {@code START} (new
 *       runId)</li>
 * </ul>
 */
public final class LifecycleMonitor {

    private static final Logger log = LoggerFactory.getLogger(LifecycleMonitor.class);

    private final ConnectClusterState clusterState;
    private final OpenLineageConfig config;
    private final VisitorRegistry visitorRegistry;
    private final OpenLineageTransport transport;

    /** Current state for each connector we are tracking. */
    private final Map<String, String> previousStates = new HashMap<>();

    /** Current runId for each connector. */
    private final Map<String, UUID> runIds = new HashMap<>();

    /** Cached lineage for each connector (survives config unavailability on delete). */
    private final Map<String, ConnectorLineage> cachedLineage = new HashMap<>();

    /** Timestamp of last RUNNING event emission per connector. */
    private final Map<String, Long> lastRunningEmitTime = new HashMap<>();

    private ScheduledExecutorService executor;

    public LifecycleMonitor(ConnectClusterState clusterState, OpenLineageConfig config) {
        this.clusterState = clusterState;
        this.config = config;
        this.visitorRegistry = new VisitorRegistry();
        this.transport = OpenLineageTransport.create(config);
    }

    /**
     * Starts the background polling thread.
     */
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "openlineage-lifecycle-monitor");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(
            this::poll,
            0,
            config.pollIntervalMs(),
            TimeUnit.MILLISECONDS
        );
        log.info("LifecycleMonitor started with poll interval {} ms",
            config.pollIntervalMs());
    }

    /**
     * Stops the background polling thread and releases resources.
     */
    public void stop() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        try {
            transport.close();
        } catch (Exception e) {
            log.warn("Error closing OpenLineage transport", e);
        }
    }

    /**
     * Perform a single poll of the cluster state and emit events for any
     * state transitions.  Package-private for testing.
     */
    void poll() {
        try {
            Collection<String> currentConnectors = clusterState.connectors();
            Set<String> currentNames = new HashSet<>(currentConnectors);

            // Detect deleted connectors
            Set<String> removed = new HashSet<>(previousStates.keySet());
            removed.removeAll(currentNames);
            for (String name : removed) {
                String prevState = previousStates.get(name);
                if ("RUNNING".equals(prevState)) {
                    emitEvent(name, OpenLineageEventBuilder.EventType.COMPLETE, null);
                }
                previousStates.remove(name);
                runIds.remove(name);
                cachedLineage.remove(name);
                lastRunningEmitTime.remove(name);
                log.debug("Connector '{}' removed, previous state was '{}'", name, prevState);
            }

            // Check each current connector
            long now = System.currentTimeMillis();
            for (String name : currentConnectors) {
                try {
                    ConnectorHealth health = clusterState.connectorHealth(name);
                    String currentState = health.connectorState().state();
                    String prevState = previousStates.get(name);

                    if (prevState == null) {
                        // Newly observed connector
                        if ("RUNNING".equals(currentState)) {
                            runIds.put(name, UUID.randomUUID());
                            emitEvent(name, OpenLineageEventBuilder.EventType.START, null);
                            lastRunningEmitTime.put(name, now);
                        }
                    } else if (!prevState.equals(currentState)) {
                        // State transition detected
                        handleTransition(name, prevState, currentState, health);
                        if ("RUNNING".equals(currentState)) {
                            lastRunningEmitTime.put(name, now);
                        }
                    } else if ("RUNNING".equals(currentState)) {
                        // Steady state — emit periodic RUNNING heartbeat
                        long lastEmit = lastRunningEmitTime.getOrDefault(name, 0L);
                        if (now - lastEmit >= config.runningIntervalMs()) {
                            emitEvent(name, OpenLineageEventBuilder.EventType.RUNNING, null);
                            lastRunningEmitTime.put(name, now);
                        }
                    }

                    previousStates.put(name, currentState);
                } catch (Exception e) {
                    log.warn("Error checking connector '{}': {}", name, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Error during lifecycle poll", e);
        }
    }

    private void handleTransition(String name, String prevState,
                                  String currentState, ConnectorHealth health) {
        switch (currentState) {
            case "RUNNING":
                if ("PAUSED".equals(prevState) || "UNASSIGNED".equals(prevState)) {
                    // Resumed: new run
                    runIds.put(name, UUID.randomUUID());
                    emitEvent(name, OpenLineageEventBuilder.EventType.START, null);
                } else if ("FAILED".equals(prevState)) {
                    // Recovered from failure: new run
                    runIds.put(name, UUID.randomUUID());
                    emitEvent(name, OpenLineageEventBuilder.EventType.START, null);
                }
                break;
            case "PAUSED":
                if ("RUNNING".equals(prevState)) {
                    emitEvent(name, OpenLineageEventBuilder.EventType.COMPLETE, null);
                }
                break;
            case "FAILED":
                if ("RUNNING".equals(prevState)) {
                    String errorMsg = health.connectorState().traceMessage();
                    emitEvent(name, OpenLineageEventBuilder.EventType.FAIL, errorMsg);
                }
                break;
            default:
                log.debug("Connector '{}' transitioned from '{}' to '{}'",
                    name, prevState, currentState);
                break;
        }
    }

    /**
     * The per-connector config from {@link ConnectClusterState} does not include
     * the worker-level {@code bootstrap.servers}, so Kafka dataset namespaces
     * would otherwise fall back to {@code kafka://localhost:9092}.  Inject the
     * worker's bootstrap servers (unless the connector specifies its own) so
     * topics are named {@code kafka://<broker>:<port>} per the OpenLineage spec.
     */
    private Map<String, String> withWorkerBootstrap(Map<String, String> connConfig) {
        String bootstrap = config.bootstrapServers();
        if (bootstrap == null || bootstrap.isEmpty() || connConfig.containsKey("bootstrap.servers")) {
            return connConfig;
        }
        Map<String, String> enriched = new HashMap<>(connConfig);
        enriched.put("bootstrap.servers", bootstrap);
        return enriched;
    }

    private void emitEvent(String connectorName,
                           OpenLineageEventBuilder.EventType eventType,
                           String errorMessage) {
        UUID runId = runIds.get(connectorName);
        if (runId == null) {
            runId = UUID.randomUUID();
            runIds.put(connectorName, runId);
        }

        // Use cached lineage if available (survives deletion).
        // Refresh from live config on START and RUNNING events to pick up
        // config changes (e.g., topics added to a sink connector).
        ConnectorLineage lineage = cachedLineage.get(connectorName);
        boolean shouldRefresh = lineage == null
            || eventType == OpenLineageEventBuilder.EventType.START
            || eventType == OpenLineageEventBuilder.EventType.RUNNING;
        if (shouldRefresh) {
            Map<String, String> connConfig;
            try {
                connConfig = clusterState.connectorConfig(connectorName);
            } catch (Exception e) {
                log.debug("Could not retrieve config for connector '{}': {}",
                    connectorName, e.getMessage());
                connConfig = Map.of();
            }
            ConnectorLineage freshLineage =
                visitorRegistry.extractLineage(withWorkerBootstrap(connConfig));
            if (!freshLineage.inputs().isEmpty() || !freshLineage.outputs().isEmpty()) {
                lineage = freshLineage;
                cachedLineage.put(connectorName, lineage);
            } else if (lineage == null) {
                lineage = freshLineage;
            }
        }

        String eventJson = OpenLineageEventBuilder.buildRunEvent(
            eventType,
            runId,
            config.namespace(),
            connectorName,
            lineage.jobType(),
            lineage.inputs(),
            lineage.outputs(),
            errorMessage
        );

        log.debug("Emitting {} event for connector '{}'", eventType, connectorName);
        transport.emit(eventJson);
    }
}
