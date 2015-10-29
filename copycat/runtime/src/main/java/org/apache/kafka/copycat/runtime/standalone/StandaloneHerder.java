/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime.standalone;

import org.apache.kafka.copycat.errors.AlreadyExistsException;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.errors.NotFoundException;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.Herder;
import org.apache.kafka.copycat.runtime.HerderConnectorContext;
import org.apache.kafka.copycat.runtime.TaskConfig;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.copycat.runtime.rest.entities.TaskInfo;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


/**
 * Single process, in-memory "herder". Useful for a standalone copycat process.
 */
public class StandaloneHerder implements Herder {
    private static final Logger log = LoggerFactory.getLogger(StandaloneHerder.class);

    private final Worker worker;
    private HashMap<String, ConnectorState> connectors = new HashMap<>();

    public StandaloneHerder(Worker worker) {
        this.worker = worker;
    }

    public synchronized void start() {
        log.info("Herder starting");
        log.info("Herder started");
    }

    public synchronized void stop() {
        log.info("Herder stopping");

        // There's no coordination/hand-off to do here since this is all standalone. Instead, we
        // should just clean up the stuff we normally would, i.e. cleanly checkpoint and shutdown all
        // the tasks.
        for (String connName : new HashSet<>(connectors.keySet())) {
            removeConnectorTasks(connName);
            try {
                worker.stopConnector(connName);
            } catch (CopycatException e) {
                log.error("Error shutting down connector {}: ", connName, e);
            }
        }
        connectors.clear();

        log.info("Herder stopped");
    }

    @Override
    public synchronized void addConnector(Map<String, String> connectorProps,
                                          Callback<String> callback) {
        try {
            String connName = startConnector(connectorProps, false);
            updateConnectorTasks(connName);
            if (callback != null)
                callback.onCompletion(null, connName);
        } catch (CopycatException e) {
            if (callback != null)
                callback.onCompletion(e, null);
        }
    }

    @Override
    public synchronized void deleteConnector(String connName, Callback<Void> callback) {
        if (!connectors.containsKey(connName)) {
            callback.onCompletion(new NotFoundException("Connector " + connName + " not found", null), null);
            return;
        }

        try {
            removeConnectorTasks(connName);
            worker.stopConnector(connName);
            connectors.remove(connName);
            if (callback != null)
                callback.onCompletion(null, null);
        } catch (CopycatException e) {
            if (callback != null)
                callback.onCompletion(e, null);
        }
    }

    @Override
    public synchronized void connectors(Callback<Collection<String>> callback) {
        callback.onCompletion(null, new ArrayList<>(connectors.keySet()));
    }

    @Override
    public synchronized void connectorInfo(String connName, Callback<ConnectorInfo> callback) {
        ConnectorState state = connectors.get(connName);
        if (state == null) {
            callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
            return;
        }

        List<ConnectorTaskId> taskIds = new ArrayList<>();
        for (int i = 0; i < state.taskConfigs.size(); i++)
            taskIds.add(new ConnectorTaskId(connName, i));
        callback.onCompletion(null, new ConnectorInfo(connName, state.configOriginals, taskIds));
    }

    @Override
    public void connectorConfig(String connName, final Callback<Map<String, String>> callback) {
        // Subset of connectorInfo, so piggy back on that implementation
        connectorInfo(connName, new Callback<ConnectorInfo>() {
            @Override
            public void onCompletion(Throwable error, ConnectorInfo result) {
                if (error != null) {
                    callback.onCompletion(error, null);
                    return;
                }
                callback.onCompletion(null, result.config());
            }
        });
    }

    @Override
    public synchronized void putConnectorConfig(String connName, final Map<String, String> config, final Callback<Void> callback) {
        if (!connectors.containsKey(connName)) {
            callback.onCompletion(new NotFoundException("Connector " + connName + " not found", null), null);
            return;
        }

        try {
            worker.stopConnector(connName);
            startConnector(config, true);
            updateConnectorTasks(connName);
            if (callback != null)
                callback.onCompletion(null, null);
        } catch (CopycatException e) {
            if (callback != null)
                callback.onCompletion(e, null);
        }

    }

    @Override
    public synchronized void requestTaskReconfiguration(String connName) {
        if (!worker.connectorNames().contains(connName)) {
            log.error("Task that requested reconfiguration does not exist: {}", connName);
            return;
        }
        updateConnectorTasks(connName);
    }

    @Override
    public synchronized void taskConfigs(String connName, Callback<List<TaskInfo>> callback) {
        ConnectorState state = connectors.get(connName);
        if (state == null) {
            callback.onCompletion(new NotFoundException("Connector " + connName + " not found", null), null);
            return;
        }

        List<TaskInfo> result = new ArrayList<>();
        for (int i = 0; i < state.taskConfigs.size(); i++) {
            TaskInfo info = new TaskInfo(new org.apache.kafka.copycat.runtime.rest.entities.ConnectorTaskId(connName, i),
                    state.taskConfigs.get(i));
            result.add(info);
        }
        callback.onCompletion(null, result);
    }

    @Override
    public void putTaskConfigs(String connName, List<Map<String, String>> configs, Callback<Void> callback) {
        throw new UnsupportedOperationException("Copycat in standalone mode does not support externally setting task configurations.");
    }

    /**
     * Start a connector in the worker and record its state.
     * @param connectorProps new connector configuration
     * @param replace if true, allow an existing connector configuration to be replaced (but this does not handle stopping
     *                the previous instance); if false, throws AlreadyExistsException if the connector already has state
     * @return the connector name
     */
    private String startConnector(Map<String, String> connectorProps, boolean replace) {
        ConnectorConfig connConfig = new ConnectorConfig(connectorProps);
        String connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
        ConnectorState state = connectors.get(connName);
        if (!replace && state != null)
            throw new AlreadyExistsException("Connector " + connName + " already exists");
        worker.addConnector(connConfig, new HerderConnectorContext(this, connName));
        if (state == null) {
            connectors.put(connName, new ConnectorState(connectorProps, connConfig));
        } else {
            state.configOriginals = connectorProps;
            state.config = connConfig;
        }
        return connName;
    }


    private List<Map<String, String>> recomputeTaskConfigs(String connName) {
        ConnectorState state = connectors.get(connName);
        return worker.connectorTaskConfigs(connName,
                state.config.getInt(ConnectorConfig.TASKS_MAX_CONFIG),
                state.config.getList(ConnectorConfig.TOPICS_CONFIG));
    }

    private void createConnectorTasks(String connName) {
        ConnectorState state = connectors.get(connName);
        int index = 0;
        for (Map<String, String> taskConfigMap : state.taskConfigs) {
            ConnectorTaskId taskId = new ConnectorTaskId(connName, index);
            TaskConfig config = new TaskConfig(taskConfigMap);
            try {
                worker.addTask(taskId, config);
            } catch (Throwable e) {
                log.error("Failed to add task {}: ", taskId, e);
                // Swallow this so we can continue updating the rest of the tasks
                // FIXME what's the proper response? Kill all the tasks? Consider this the same as a task
                // that died after starting successfully.
            }
            index++;
        }
    }

    private void removeConnectorTasks(String connName) {
        ConnectorState state = connectors.get(connName);
        for (int i = 0; i < state.taskConfigs.size(); i++) {
            ConnectorTaskId taskId = new ConnectorTaskId(connName, i);
            try {
                worker.stopTask(taskId);
            } catch (CopycatException e) {
                log.error("Failed to stop task {}: ", taskId, e);
                // Swallow this so we can continue stopping the rest of the tasks
                // FIXME: Forcibly kill the task?
            }
        }
        state.taskConfigs = new ArrayList<>();
    }

    private void updateConnectorTasks(String connName) {
        List<Map<String, String>> newTaskConfigs = recomputeTaskConfigs(connName);
        ConnectorState state = connectors.get(connName);
        if (!newTaskConfigs.equals(state.taskConfigs)) {
            removeConnectorTasks(connName);
            state.taskConfigs = newTaskConfigs;
            createConnectorTasks(connName);
        }
    }


    private static class ConnectorState {
        public Map<String, String> configOriginals;
        public ConnectorConfig config;
        List<Map<String, String>> taskConfigs;

        public ConnectorState(Map<String, String> configOriginals, ConnectorConfig config) {
            this.configOriginals = configOriginals;
            this.config = config;
            this.taskConfigs = new ArrayList<>();
        }
    }
}
