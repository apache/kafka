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

package org.apache.kafka.connect.runtime.standalone;

import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.HerderConnectorContext;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.storage.MemoryStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Single process, in-memory "herder". Useful for a standalone Kafka Connect process.
 */
public class StandaloneHerder extends AbstractHerder {
    private static final Logger log = LoggerFactory.getLogger(StandaloneHerder.class);

    private HashMap<String, ConnectorState> connectors = new HashMap<>();

    public StandaloneHerder(Worker worker) {
        this(worker.workerId(), worker, new MemoryStatusBackingStore());
    }

    // visible for testing
    StandaloneHerder(String workerId,
                     Worker worker,
                     StatusBackingStore statusBackingStore) {
        super(worker, statusBackingStore, workerId);
    }

    public synchronized void start() {
        log.info("Herder starting");
        startServices();
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
            } catch (ConnectException e) {
                log.error("Error shutting down connector {}: ", connName, e);
            }
        }
        connectors.clear();

        log.info("Herder stopped");
    }

    @Override
    public int generation() {
        return 0;
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
        callback.onCompletion(null, createConnectorInfo(state));
    }

    private ConnectorInfo createConnectorInfo(ConnectorState state) {
        if (state == null)
            return null;

        List<ConnectorTaskId> taskIds = new ArrayList<>();
        for (int i = 0; i < state.taskConfigs.size(); i++)
            taskIds.add(new ConnectorTaskId(state.name, i));
        return new ConnectorInfo(state.name, state.configOriginals, taskIds);
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
    public synchronized void putConnectorConfig(String connName, final Map<String, String> config,
                                                boolean allowReplace,
                                                final Callback<Created<ConnectorInfo>> callback) {
        try {
            boolean created = false;
            if (connectors.containsKey(connName)) {
                if (!allowReplace) {
                    callback.onCompletion(new AlreadyExistsException("Connector " + connName + " already exists"), null);
                    return;
                }
                if (config == null) // Deletion, kill tasks as well
                    removeConnectorTasks(connName);
                worker.stopConnector(connName);
                if (config == null) {
                    connectors.remove(connName);
                    onDeletion(connName);
                }
            } else {
                if (config == null) {
                    // Deletion, must already exist
                    callback.onCompletion(new NotFoundException("Connector " + connName + " not found", null), null);
                    return;
                }
                created = true;
            }
            if (config != null) {
                startConnector(config);
                updateConnectorTasks(connName);
            }
            if (config != null)
                callback.onCompletion(null, new Created<>(created, createConnectorInfo(connectors.get(connName))));
            else
                callback.onCompletion(null, new Created<ConnectorInfo>(false, null));
        } catch (ConnectException e) {
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
            TaskInfo info = new TaskInfo(new ConnectorTaskId(connName, i), state.taskConfigs.get(i));
            result.add(info);
        }
        callback.onCompletion(null, result);
    }

    @Override
    public void putTaskConfigs(String connName, List<Map<String, String>> configs, Callback<Void> callback) {
        throw new UnsupportedOperationException("Kafka Connect in standalone mode does not support externally setting task configurations.");
    }

    @Override
    public synchronized void restartTask(ConnectorTaskId taskId, Callback<Void> cb) {
        if (!connectors.containsKey(taskId.connector()))
            cb.onCompletion(new NotFoundException("Connector " + taskId.connector() + " not found", null), null);

        ConnectorState state = connectors.get(taskId.connector());
        if (state.taskConfigs.contains(taskId))
            cb.onCompletion(new NotFoundException("Task " + taskId + " not found", null), null);

        try {
            worker.stopAndAwaitTask(taskId);

            Map<String, String> taskConfig = state.taskConfigs.get(taskId.task());
            worker.startTask(taskId, new TaskConfig(taskConfig), this);

            cb.onCompletion(null, null);
        } catch (Exception e) {
            log.error("Failed to restart task {}", taskId, e);
            cb.onCompletion(e, null);
        }
    }

    @Override
    public synchronized void restartConnector(String connName, Callback<Void> cb) {
        if (!connectors.containsKey(connName))
            cb.onCompletion(new NotFoundException("Connector " + connName + " not found", null), null);

        ConnectorState state = connectors.get(connName);
        try {
            worker.stopConnector(connName);
            worker.startConnector(state.config, new HerderConnectorContext(this, connName), this);
            cb.onCompletion(null, null);
        } catch (Exception e) {
            log.error("Failed to restart connector {}", connName, e);
            cb.onCompletion(e, null);
        }
    }

    /**
     * Start a connector in the worker and record its state.
     * @param connectorProps new connector configuration
     * @return the connector name
     */
    private String startConnector(Map<String, String> connectorProps) {
        ConnectorConfig connConfig = new ConnectorConfig(connectorProps);
        String connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
        ConnectorState state = connectors.get(connName);
        worker.startConnector(connConfig, new HerderConnectorContext(this, connName), this);
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
            startTask(taskId, taskConfigMap);
            index++;
        }
    }

    private void startTask(ConnectorTaskId taskId, Map<String, String> taskConfigMap) {
        TaskConfig config = new TaskConfig(taskConfigMap);
        try {
            worker.startTask(taskId, config, this);
        } catch (Throwable e) {
            log.error("Failed to add task {}: ", taskId, e);
            // Swallow this so we can continue updating the rest of the tasks
            // FIXME what's the proper response? Kill all the tasks? Consider this the same as a task
            // that died after starting successfully.
        }
    }

    private Set<ConnectorTaskId> tasksFor(ConnectorState state) {
        Set<ConnectorTaskId> tasks = new HashSet<>();
        for (int i = 0; i < state.taskConfigs.size(); i++)
            tasks.add(new ConnectorTaskId(state.name, i));
        return tasks;
    }

    private void removeConnectorTasks(String connName) {
        ConnectorState state = connectors.get(connName);
        Set<ConnectorTaskId> tasks = tasksFor(state);
        if (!tasks.isEmpty()) {
            worker.stopTasks(tasks);
            worker.awaitStopTasks(tasks);
            state.taskConfigs = new ArrayList<>();
        }
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
        public String name;
        public Map<String, String> configOriginals;
        public ConnectorConfig config;
        List<Map<String, String>> taskConfigs;

        public ConnectorState(Map<String, String> configOriginals, ConnectorConfig config) {
            this.name = config.getString(ConnectorConfig.NAME_CONFIG);
            this.configOriginals = configOriginals;
            this.config = config;
            this.taskConfigs = new ArrayList<>();
        }
    }
}
