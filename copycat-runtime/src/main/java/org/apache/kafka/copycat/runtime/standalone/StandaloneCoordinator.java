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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.connector.Connector;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.errors.CopycatRuntimeException;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.Coordinator;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.sink.SinkConnector;
import org.apache.kafka.copycat.sink.SinkTask;
import org.apache.kafka.copycat.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Single process, in-memory "coordinator". Useful for a standalone copycat process.
 */
public class StandaloneCoordinator implements Coordinator {
    private static final Logger log = LoggerFactory.getLogger(StandaloneCoordinator.class);

    public static final String STORAGE_CONFIG = "coordinator.standalone.storage";

    private Worker worker;
    private Properties configs;
    private ConfigStorage configStorage;
    private HashMap<String, ConnectorState> connectors = new HashMap<>();

    public StandaloneCoordinator(Worker worker, Properties props) {
        this.worker = worker;
        this.configs = props;
    }

    public synchronized void start() {
        log.info("Coordinator starting");

        String storage = configs.getProperty(STORAGE_CONFIG);
        if (storage != null && !storage.isEmpty()) {
            try {
                configStorage = Utils.newInstance(storage, ConfigStorage.class);
            } catch (ClassNotFoundException e) {
                throw new CopycatRuntimeException("Couldn't configure storage", e);
            }
            configStorage.configure(configs);
        } else {
            configStorage = null;
        }

        restoreConnectors();

        log.info("Coordinator started");
    }

    public synchronized void stop() {
        log.info("Coordinator stopping");

        // There's no coordination/hand-off to do here since this is all standalone. Instead, we
        // should just clean up the stuff we normally would, i.e. cleanly checkpoint and shutdown all
        // the tasks.
        for (Map.Entry<String, ConnectorState> entry : connectors.entrySet()) {
            ConnectorState state = entry.getValue();
            stopConnector(state);
        }
        connectors.clear();

        if (configStorage != null) {
            configStorage.close();
            configStorage = null;
        }

        log.info("Coordinator stopped");
    }

    @Override
    public synchronized void addConnector(Properties connectorProps,
                                          Callback<String> callback) {
        try {
            ConnectorState connState = createConnector(connectorProps);
            if (callback != null)
                callback.onCompletion(null, connState.name);
            // This should always be a new job, create jobs from scratch
            createConnectorTasks(connState);
        } catch (CopycatRuntimeException e) {
            if (callback != null)
                callback.onCompletion(e, null);
        }
    }

    @Override
    public synchronized void deleteConnector(String name, Callback<Void> callback) {
        try {
            destroyConnector(name);
            if (callback != null)
                callback.onCompletion(null, null);
        } catch (CopycatRuntimeException e) {
            if (callback != null)
                callback.onCompletion(e, null);
        }
    }

    // Creates the and configures the connector. Does not setup any tasks
    private ConnectorState createConnector(Properties connectorProps) {
        ConnectorConfig connConfig = new ConnectorConfig(connectorProps);
        String connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
        String className = connConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
        log.info("Creating connector {} of type {}", connName, className);
        int maxTasks = connConfig.getInt(ConnectorConfig.TASKS_MAX_CONFIG);
        List<String> topics = connConfig.getList(ConnectorConfig.TOPICS_CONFIG); // Sinks only
        Properties configs = connConfig.getUnusedProperties();

        if (connectors.containsKey(connName)) {
            log.error("Ignoring request to create connector due to conflicting connector name");
            throw new CopycatRuntimeException("Connector with name " + connName + " already exists");
        }

        final Connector connector;
        try {
            connector = instantiateConnector(className);
        } catch (Throwable t) {
            // Catches normal exceptions due to instantiation errors as well as any runtime errors that
            // may be caused by user code
            throw new CopycatRuntimeException("Failed to create connector instance", t);
        }
        connector.initialize(new StandaloneConnectorContext(this, connName));
        try {
            connector.start(configs);
        } catch (CopycatException e) {
            throw new CopycatRuntimeException("Connector threw an exception while starting", e);
        }
        ConnectorState state = new ConnectorState(connName, connector, maxTasks, topics);
        connectors.put(connName, state);
        if (configStorage != null)
            configStorage.putConnectorConfig(connName, connectorProps);

        log.info("Finished creating connector {}", connName);

        return state;
    }

    private static Connector instantiateConnector(String className) {
        try {
            return Utils.newInstance(className, Connector.class);
        } catch (ClassNotFoundException e) {
            throw new CopycatRuntimeException("Couldn't instantiate connector class", e);
        }
    }

    private void destroyConnector(String connName) {
        log.info("Destroying connector {}", connName);
        ConnectorState state = connectors.get(connName);
        if (state == null) {
            log.error("Failed to destroy connector {} because it does not exist", connName);
            throw new CopycatRuntimeException("Connector does not exist");
        }

        stopConnector(state);
        connectors.remove(state.name);
        if (configStorage != null)
            configStorage.putConnectorConfig(state.name, null);

        log.info("Finished destroying connector {}", connName);
    }

    // Stops a connectors tasks, then the connector
    private void stopConnector(ConnectorState state) {
        removeConnectorTasks(state);
        try {
            state.connector.stop();
        } catch (CopycatException e) {
            log.error("Error shutting down connector {}: ", state.connector, e);
        }
    }

    private void createConnectorTasks(ConnectorState state) {
        String taskClassName = state.connector.getTaskClass().getName();

        log.info("Creating tasks for connector {} of type {}", state.name, taskClassName);

        List<Properties> taskConfigs = state.connector.getTaskConfigs(state.maxTasks);

        // Generate the final configs, including framework provided settings
        Map<ConnectorTaskId, Properties> taskProps = new HashMap<>();
        for (int i = 0; i < taskConfigs.size(); i++) {
            ConnectorTaskId taskId = new ConnectorTaskId(state.name, i);
            Properties config = taskConfigs.get(i);
            // TODO: This probably shouldn't be in the Coordinator. It's nice to have Copycat ensure the list of topics
            // is automatically provided to tasks since it is required by the framework, but this
            String subscriptionTopics = Utils.join(state.inputTopics, ",");
            if (state.connector instanceof SinkConnector) {
                // Make sure we don't modify the original since the connector may reuse it internally
                Properties configForSink = new Properties();
                configForSink.putAll(config);
                configForSink.setProperty(SinkTask.TOPICS_CONFIG, subscriptionTopics);
                config = configForSink;
            }
            taskProps.put(taskId, config);
        }

        // And initiate the tasks
        for (int i = 0; i < taskConfigs.size(); i++) {
            ConnectorTaskId taskId = new ConnectorTaskId(state.name, i);
            Properties config = taskProps.get(taskId);
            try {
                worker.addTask(taskId, taskClassName, config);
                // We only need to store the task IDs so we can clean up.
                state.tasks.add(taskId);
            } catch (Throwable e) {
                log.error("Failed to add task {}: ", taskId, e);
                // Swallow this so we can continue updating the rest of the tasks
                // FIXME what's the proper response? Kill all the tasks? Consider this the same as a task
                // that died after starting successfully.
            }
        }
    }

    private void removeConnectorTasks(ConnectorState state) {
        Iterator<ConnectorTaskId> taskIter = state.tasks.iterator();
        while (taskIter.hasNext()) {
            ConnectorTaskId taskId = taskIter.next();
            try {
                worker.stopTask(taskId);
                taskIter.remove();
            } catch (CopycatException e) {
                log.error("Failed to stop task {}: ", taskId, e);
                // Swallow this so we can continue stopping the rest of the tasks
                // FIXME: Forcibly kill the task?
            }
        }
    }

    private void updateConnectorTasks(ConnectorState state) {
        removeConnectorTasks(state);
        createConnectorTasks(state);
    }

    private void restoreConnectors() {
        if (configStorage == null)
            return;

        Collection<String> connNames = configStorage.getConnectors();
        for (String connName : connNames) {
            log.info("Restoring connector {}", connName);
            Properties connProps = configStorage.getConnectorConfig(connName);
            ConnectorState connState = createConnector(connProps);
            // Because this coordinator is standalone, connectors are only restored when this process
            // starts and we know there can't be any existing tasks. So in this special case we're able
            // to just create the tasks rather than having to check for existing tasks and sort out
            // whether they need to be reconfigured.
            createConnectorTasks(connState);
        }
    }

    /**
     * Requests reconfiguration of the task. This should only be triggered by
     * {@link StandaloneConnectorContext}.
     *
     * @param connName name of the connector that should be reconfigured
     */
    public synchronized void requestTaskReconfiguration(String connName) {
        ConnectorState state = connectors.get(connName);
        if (state == null) {
            log.error("Task that requested reconfiguration does not exist: {}", connName);
            return;
        }
        updateConnectorTasks(state);
    }


    private static class ConnectorState {
        public String name;
        public Connector connector;
        public int maxTasks;
        public List<String> inputTopics;
        Set<ConnectorTaskId> tasks;

        public ConnectorState(String name, Connector connector, int maxTasks,
                              List<String> inputTopics) {
            this.name = name;
            this.connector = connector;
            this.maxTasks = maxTasks;
            this.inputTopics = inputTopics;
            this.tasks = new HashSet<>();
        }
    }
}
