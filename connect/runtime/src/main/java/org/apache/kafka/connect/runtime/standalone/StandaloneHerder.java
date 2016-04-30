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
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.MemoryConfigBackingStore;
import org.apache.kafka.connect.storage.MemoryStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Single process, in-memory "herder". Useful for a standalone Kafka Connect process.
 */
public class StandaloneHerder extends AbstractHerder {
    private static final Logger log = LoggerFactory.getLogger(StandaloneHerder.class);

    private ClusterConfigState configState;

    public StandaloneHerder(Worker worker) {
        this(worker, worker.workerId(), new MemoryStatusBackingStore(), new MemoryConfigBackingStore());
    }

    // visible for testing
    StandaloneHerder(Worker worker,
                     String workerId,
                     StatusBackingStore statusBackingStore,
                     MemoryConfigBackingStore configBackingStore) {
        super(worker, workerId, statusBackingStore, configBackingStore);
        this.configState = ClusterConfigState.EMPTY;
        configBackingStore.setUpdateListener(new ConfigUpdateListener());
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
        for (String connName : configState.connectors()) {
            removeConnectorTasks(connName);
            try {
                worker.stopConnector(connName);
            } catch (ConnectException e) {
                log.error("Error shutting down connector {}: ", connName, e);
            }
        }
        stopServices();
        log.info("Herder stopped");
    }

    @Override
    public int generation() {
        return 0;
    }

    @Override
    public synchronized void connectors(Callback<Collection<String>> callback) {
        callback.onCompletion(null, configState.connectors());
    }

    @Override
    public synchronized void connectorInfo(String connName, Callback<ConnectorInfo> callback) {
        ConnectorInfo connectorInfo = createConnectorInfo(connName);
        if (connectorInfo == null) {
            callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
            return;
        }
        callback.onCompletion(null, connectorInfo);
    }

    private ConnectorInfo createConnectorInfo(String connector) {
        if (!configState.contains(connector))
            return null;
        Map<String, String> config = configState.connectorConfig(connector);
        return new ConnectorInfo(connector, config, configState.tasks(connector));
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
    public synchronized void putConnectorConfig(String connName,
                                                final Map<String, String> config,
                                                boolean allowReplace,
                                                final Callback<Created<ConnectorInfo>> callback) {
        try {
            boolean created = false;
            if (configState.contains(connName)) {
                if (!allowReplace) {
                    callback.onCompletion(new AlreadyExistsException("Connector " + connName + " already exists"), null);
                    return;
                }
                if (config == null) // Deletion, kill tasks as well
                    removeConnectorTasks(connName);
                worker.stopConnector(connName);
                if (config == null) {
                    configBackingStore.removeConnectorConfig(connName);
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
                callback.onCompletion(null, new Created<>(created, createConnectorInfo(connName)));
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
        if (!configState.contains(connName)) {
            callback.onCompletion(new NotFoundException("Connector " + connName + " not found", null), null);
            return;
        }

        List<TaskInfo> result = new ArrayList<>();
        for (ConnectorTaskId taskId : configState.tasks(connName))
            result.add(new TaskInfo(taskId, configState.taskConfig(taskId)));
        callback.onCompletion(null, result);
    }

    @Override
    public void putTaskConfigs(String connName, List<Map<String, String>> configs, Callback<Void> callback) {
        throw new UnsupportedOperationException("Kafka Connect in standalone mode does not support externally setting task configurations.");
    }

    @Override
    public synchronized void restartTask(ConnectorTaskId taskId, Callback<Void> cb) {
        if (!configState.contains(taskId.connector()))
            cb.onCompletion(new NotFoundException("Connector " + taskId.connector() + " not found", null), null);

        Map<String, String> taskConfig = configState.taskConfig(taskId);
        if (taskConfig == null)
            cb.onCompletion(new NotFoundException("Task " + taskId + " not found", null), null);

        TargetState targetState = configState.targetState(taskId.connector());
        try {
            worker.stopAndAwaitTask(taskId);
            worker.startTask(taskId, new TaskConfig(taskConfig), this, targetState);
            cb.onCompletion(null, null);
        } catch (Exception e) {
            log.error("Failed to restart task {}", taskId, e);
            cb.onCompletion(e, null);
        }
    }

    @Override
    public synchronized void restartConnector(String connName, Callback<Void> cb) {
        if (!configState.contains(connName))
            cb.onCompletion(new NotFoundException("Connector " + connName + " not found", null), null);

        Map<String, String> config = configState.connectorConfig(connName);
        try {
            worker.stopConnector(connName);
            startConnector(config);
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
        configBackingStore.putConnectorConfig(connName, connectorProps);
        TargetState targetState = configState.targetState(connName);
        worker.startConnector(connConfig, new HerderConnectorContext(this, connName), this, targetState);
        return connName;
    }

    private List<Map<String, String>> recomputeTaskConfigs(String connName) {
        Map<String, String> config = configState.connectorConfig(connName);
        ConnectorConfig connConfig = new ConnectorConfig(config);

        return worker.connectorTaskConfigs(connName,
                connConfig.getInt(ConnectorConfig.TASKS_MAX_CONFIG),
                connConfig.getList(ConnectorConfig.TOPICS_CONFIG));
    }

    private void createConnectorTasks(String connName, TargetState initialState) {
        for (ConnectorTaskId taskId : configState.tasks(connName)) {
            Map<String, String> taskConfigMap = configState.taskConfig(taskId);
            TaskConfig config = new TaskConfig(taskConfigMap);
            try {
                worker.startTask(taskId, config, this, initialState);
            } catch (Throwable e) {
                log.error("Failed to add task {}: ", taskId, e);
                // Swallow this so we can continue updating the rest of the tasks
                // FIXME what's the proper response? Kill all the tasks? Consider this the same as a task
                // that died after starting successfully.
            }
        }
    }

    private void removeConnectorTasks(String connName) {
        Collection<ConnectorTaskId> tasks = configState.tasks(connName);
        if (!tasks.isEmpty()) {
            worker.stopTasks(tasks);
            worker.awaitStopTasks(tasks);
            configBackingStore.removeTaskConfigs(connName);
        }
    }

    private void updateConnectorTasks(String connName) {
        if (!worker.isRunning(connName)) {
            log.info("Skipping reconfiguration of connector {} since it is not running", connName);
            return;
        }

        List<Map<String, String>> newTaskConfigs = recomputeTaskConfigs(connName);
        List<Map<String, String>> oldTaskConfigs = configState.allTaskConfigs(connName);

        if (!newTaskConfigs.equals(oldTaskConfigs)) {
            removeConnectorTasks(connName);
            configBackingStore.putTaskConfigs(connName, newTaskConfigs);
            createConnectorTasks(connName, configState.targetState(connName));
        }
    }

    // This update listener assumes synchronous updates the ConfigBackingStore, which only works
    // with the MemoryConfigBackingStore. This allows us to write a change (e.g. through
    // ConfigBackingStore.putConnectorConfig()) and then immediately read it back from an updated
    // snapshot.
    // TODO: To get any real benefit from the backing store abstraction, we should move some of
    // the handling into the callbacks in this listener.
    private class ConfigUpdateListener implements ConfigBackingStore.UpdateListener {

        @Override
        public void onConnectorConfigRemove(String connector) {
            synchronized (StandaloneHerder.this) {
                configState = configBackingStore.snapshot();
            }
        }

        @Override
        public void onConnectorConfigUpdate(String connector) {
            // TODO: move connector configuration update handling here to be consistent with
            //       the semantics of the config backing store

            synchronized (StandaloneHerder.this) {
                configState = configBackingStore.snapshot();
            }
        }

        @Override
        public void onTaskConfigUpdate(Collection<ConnectorTaskId> tasks) {
            synchronized (StandaloneHerder.this) {
                configState = configBackingStore.snapshot();
            }
        }

        @Override
        public void onConnectorTargetStateChange(String connector) {
            synchronized (StandaloneHerder.this) {
                configState = configBackingStore.snapshot();
                TargetState targetState = configState.targetState(connector);
                worker.setTargetState(connector, targetState);
                if (targetState == TargetState.STARTED)
                    updateConnectorTasks(connector);
            }
        }
    }

}
