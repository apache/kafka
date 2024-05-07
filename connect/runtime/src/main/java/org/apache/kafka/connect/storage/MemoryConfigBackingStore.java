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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.SessionKey;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of ConfigBackingStore that stores Kafka Connect connector configurations in-memory (i.e. configs
 * aren't persisted and will be wiped if the worker is restarted).
 */
public class MemoryConfigBackingStore implements ConfigBackingStore {

    private final Map<String, ConnectorState> connectors = new HashMap<>();
    private UpdateListener updateListener;
    private WorkerConfigTransformer configTransformer;

    public MemoryConfigBackingStore() {
    }

    public MemoryConfigBackingStore(WorkerConfigTransformer configTransformer) {
        this.configTransformer = configTransformer;
    }

    @Override
    public synchronized void start() {
    }

    @Override
    public synchronized void stop() {
    }

    @Override
    public synchronized ClusterConfigState snapshot() {
        Map<String, Integer> connectorTaskCounts = new HashMap<>();
        Map<String, Map<String, String>> connectorConfigs = new HashMap<>();
        Map<String, TargetState> connectorTargetStates = new HashMap<>();
        Map<ConnectorTaskId, Map<String, String>> taskConfigs = new HashMap<>();

        for (Map.Entry<String, ConnectorState> connectorStateEntry : connectors.entrySet()) {
            String connector = connectorStateEntry.getKey();
            ConnectorState connectorState = connectorStateEntry.getValue();
            connectorTaskCounts.put(connector, connectorState.taskConfigs.size());
            connectorConfigs.put(connector, connectorState.connConfig);
            connectorTargetStates.put(connector, connectorState.targetState);
            taskConfigs.putAll(connectorState.taskConfigs);
        }

        return new ClusterConfigState(
                ClusterConfigState.NO_OFFSET,
                null,
                connectorTaskCounts,
                connectorConfigs,
                connectorTargetStates,
                taskConfigs,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet(),
                configTransformer
        );
    }

    @Override
    public synchronized boolean contains(String connector) {
        return connectors.containsKey(connector);
    }

    @Override
    public synchronized void putConnectorConfig(String connector, Map<String, String> properties, TargetState targetState) {
        ConnectorState state = connectors.get(connector);
        if (state == null)
            connectors.put(connector, new ConnectorState(properties, targetState));
        else {
            state.connConfig = properties;
            if (targetState != null) {
                state.targetState = targetState;
            }
        }

        if (updateListener != null)
            updateListener.onConnectorConfigUpdate(connector);
    }

    @Override
    public synchronized void removeConnectorConfig(String connector) {
        ConnectorState state = connectors.remove(connector);

        if (updateListener != null && state != null)
            updateListener.onConnectorConfigRemove(connector);
    }

    @Override
    public synchronized void removeTaskConfigs(String connector) {
        ConnectorState state = connectors.get(connector);
        if (state == null)
            throw new IllegalArgumentException("Cannot remove tasks for non-existing connector");

        HashSet<ConnectorTaskId> taskIds = new HashSet<>(state.taskConfigs.keySet());
        state.taskConfigs.clear();

        if (updateListener != null)
            updateListener.onTaskConfigUpdate(taskIds);
    }

    @Override
    public synchronized void putTaskConfigs(String connector, List<Map<String, String>> configs) {
        ConnectorState state = connectors.get(connector);
        if (state == null)
            throw new IllegalArgumentException("Cannot put tasks for non-existing connector");

        Map<ConnectorTaskId, Map<String, String>> taskConfigsMap = taskConfigListAsMap(connector, configs);
        state.taskConfigs = taskConfigsMap;

        if (updateListener != null)
            updateListener.onTaskConfigUpdate(taskConfigsMap.keySet());
    }

    @Override
    public void refresh(long timeout, TimeUnit unit) {
    }

    @Override
    public synchronized void putTargetState(String connector, TargetState state) {
        ConnectorState connectorState = connectors.get(connector);
        if (connectorState == null)
            throw new IllegalArgumentException("No connector `" + connector + "` configured");

        TargetState prevState = connectorState.targetState;
        connectorState.targetState = state;

        if (updateListener != null && !state.equals(prevState))
            updateListener.onConnectorTargetStateChange(connector);
    }

    @Override
    public void putSessionKey(SessionKey sessionKey) {
        // no-op
    }

    @Override
    public void putRestartRequest(RestartRequest restartRequest) {
        // no-op
    }

    @Override
    public void putTaskCountRecord(String connector, int taskCount) {
        // no-op
    }

    @Override
    public void putLoggerLevel(String namespace, String level) {
        // no-op
    }

    @Override
    public synchronized void setUpdateListener(UpdateListener listener) {
        this.updateListener = listener;
    }

    private static class ConnectorState {
        private TargetState targetState;
        private Map<String, String> connConfig;
        private Map<ConnectorTaskId, Map<String, String>> taskConfigs;

        /**
         * @param connConfig the connector's configuration
         * @param targetState the connector's initial {@link TargetState}; may be {@code null} in which case the default initial target state
         * {@link TargetState#STARTED} will be used
         */
        public ConnectorState(Map<String, String> connConfig, TargetState targetState) {
            this.targetState = targetState == null ? TargetState.STARTED : targetState;
            this.connConfig = connConfig;
            this.taskConfigs = new HashMap<>();
        }
    }

    private static Map<ConnectorTaskId, Map<String, String>> taskConfigListAsMap(String connector, List<Map<String, String>> configs) {
        int index = 0;
        Map<ConnectorTaskId, Map<String, String>> result = new TreeMap<>();
        for (Map<String, String> taskConfigMap: configs) {
            result.put(new ConnectorTaskId(connector, index++), taskConfigMap);
        }
        return result;
    }
}
