/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime.distributed;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.connector.Connector;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.Herder;
import org.apache.kafka.copycat.runtime.HerderConnectorContext;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.sink.SinkConnector;
import org.apache.kafka.copycat.sink.SinkTask;
import org.apache.kafka.copycat.storage.KafkaConfigStorage;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Distributed "herder" that coordinates with other workers to spread work across multiple processes.
 */
public class DistributedHerder implements Herder {
    private static final Logger log = LoggerFactory.getLogger(DistributedHerder.class);

    private Worker worker;
    private KafkaConfigStorage configStorage;
    private ClusterConfigState configState;
    private HashMap<String, ConnectorState> connectors = new HashMap<>();

    public DistributedHerder(Worker worker) {
        this.worker = worker;
        this.configStorage = new KafkaConfigStorage(worker.getInternalValueConverter(),
                new ConnectorConfigCallback(), new TaskConfigCallback());
    }

    // Public for testing (mock KafkaConfigStorage)
    public DistributedHerder(Worker worker, KafkaConfigStorage configStorage) {
        this.worker = worker;
        this.configStorage = configStorage;
    }

    public synchronized void configure(Map<String, ?> configs) {
        configStorage.configure(configs);
    }

    public synchronized void start() {
        log.info("Herder starting");

        configStorage.start();

        log.info("Restoring connectors from stored configs");
        restoreConnectors();

        log.info("Herder started");
    }

    public synchronized void stop() {
        log.info("Herder stopping");

        // There's no coordination/hand-off to do here since this is all standalone. Instead, we
        // should just clean up the stuff we normally would, i.e. cleanly checkpoint and shutdown all
        // the tasks.
        for (Map.Entry<String, ConnectorState> entry : connectors.entrySet()) {
            ConnectorState state = entry.getValue();
            stopConnector(state);
        }
        connectors.clear();

        if (configStorage != null) {
            configStorage.stop();
            configStorage = null;
        }

        log.info("Herder stopped");
    }

    @Override
    public synchronized void addConnector(Map<String, String> connectorProps,
                                          Callback<String> callback) {
        try {
            // Ensure the config is written to storage first
            ConnectorConfig connConfig = new ConnectorConfig(connectorProps);
            String connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
            configStorage.putConnectorConfig(connName, connectorProps);

            ConnectorState connState = createConnector(connConfig);
            if (callback != null)
                callback.onCompletion(null, connState.name);
            // This should always be a new job, create jobs from scratch
            createConnectorTasks(connState);
        } catch (CopycatException e) {
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
        } catch (CopycatException e) {
            if (callback != null)
                callback.onCompletion(e, null);
        }
    }

    @Override
    public synchronized void requestTaskReconfiguration(String connName) {
        ConnectorState state = connectors.get(connName);
        if (state == null) {
            log.error("Task that requested reconfiguration does not exist: {}", connName);
            return;
        }
        updateConnectorTasks(state);
    }

    // Creates and configures the connector. Does not setup any tasks
    private ConnectorState createConnector(ConnectorConfig connConfig) {
        String connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
        String className = connConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
        log.info("Creating connector {} of type {}", connName, className);
        int maxTasks = connConfig.getInt(ConnectorConfig.TASKS_MAX_CONFIG);
        List<String> topics = connConfig.getList(ConnectorConfig.TOPICS_CONFIG); // Sinks only
        Properties configs = connConfig.unusedProperties();

        if (connectors.containsKey(connName)) {
            log.error("Ignoring request to create connector due to conflicting connector name");
            throw new CopycatException("Connector with name " + connName + " already exists");
        }

        final Connector connector;
        try {
            connector = instantiateConnector(className);
        } catch (Throwable t) {
            // Catches normal exceptions due to instantiation errors as well as any runtime errors that
            // may be caused by user code
            throw new CopycatException("Failed to create connector instance", t);
        }
        connector.initialize(new HerderConnectorContext(this, connName));
        try {
            connector.start(configs);
        } catch (CopycatException e) {
            throw new CopycatException("Connector threw an exception while starting", e);
        }
        ConnectorState state = new ConnectorState(connName, connector, maxTasks, topics);
        connectors.put(connName, state);

        log.info("Finished creating connector {}", connName);

        return state;
    }

    private static Connector instantiateConnector(String className) {
        try {
            return Utils.newInstance(className, Connector.class);
        } catch (ClassNotFoundException e) {
            throw new CopycatException("Couldn't instantiate connector class", e);
        }
    }

    private void destroyConnector(String connName) {
        log.info("Destroying connector {}", connName);
        ConnectorState state = connectors.get(connName);
        if (state == null) {
            log.error("Failed to destroy connector {} because it does not exist", connName);
            throw new CopycatException("Connector does not exist");
        }

        stopConnector(state);
        configStorage.putConnectorConfig(state.name, null);
        connectors.remove(state.name);

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
        String taskClassName = state.connector.taskClass().getName();

        log.info("Creating tasks for connector {} of type {}", state.name, taskClassName);

        List<Properties> taskConfigs = state.connector.taskConfigs(state.maxTasks);

        // Generate the final configs, including framework provided settings
        Map<ConnectorTaskId, Properties> taskProps = new HashMap<>();
        for (int i = 0; i < taskConfigs.size(); i++) {
            ConnectorTaskId taskId = new ConnectorTaskId(state.name, i);
            Properties config = taskConfigs.get(i);
            // TODO: This probably shouldn't be in the Herder. It's nice to have Copycat ensure the list of topics
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
        configState = configStorage.snapshot();
        Collection<String> connNames = configState.connectors();
        for (String connName : connNames) {
            log.info("Restoring connector {}", connName);
            Map<String, String> connProps = configState.connectorConfig(connName);
            ConnectorConfig connConfig = new ConnectorConfig(connProps);
            ConnectorState connState = createConnector(connConfig);
            // Because this coordinator is standalone, connectors are only restored when this process
            // starts and we know there can't be any existing tasks. So in this special case we're able
            // to just create the tasks rather than having to check for existing tasks and sort out
            // whether they need to be reconfigured.
            createConnectorTasks(connState);
        }
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

    private class ConnectorConfigCallback implements Callback<String> {
        @Override
        public void onCompletion(Throwable error, String result) {
            configState = configStorage.snapshot();
            // FIXME
        }
    }

    private class TaskConfigCallback implements Callback<List<ConnectorTaskId>> {
        @Override
        public void onCompletion(Throwable error, List<ConnectorTaskId> result) {
            configState = configStorage.snapshot();
            // FIXME
        }
    }


}
