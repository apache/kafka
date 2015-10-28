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

import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.Herder;
import org.apache.kafka.copycat.runtime.HerderConnectorContext;
import org.apache.kafka.copycat.runtime.TaskConfig;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/**
 * Single process, in-memory "herder". Useful for a standalone copycat process.
 */
public class StandaloneHerder implements Herder {
    private static final Logger log = LoggerFactory.getLogger(StandaloneHerder.class);

    private Worker worker;
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
        for (String connName : new HashSet<>(connectors.keySet()))
            stopConnector(connName);

        log.info("Herder stopped");
    }

    @Override
    public synchronized void addConnector(Map<String, String> connectorProps,
                                          Callback<String> callback) {
        try {
            ConnectorConfig connConfig = new ConnectorConfig(connectorProps);
            String connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
            worker.addConnector(connConfig, new HerderConnectorContext(this, connName));
            connectors.put(connName, new ConnectorState(connConfig));
            if (callback != null)
                callback.onCompletion(null, connName);
            // This should always be a new job, create jobs from scratch
            createConnectorTasks(connName);
        } catch (CopycatException e) {
            if (callback != null)
                callback.onCompletion(e, null);
        }
    }

    @Override
    public synchronized void deleteConnector(String connName, Callback<Void> callback) {
        try {
            stopConnector(connName);
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

    // Stops a connectors tasks, then the connector
    private void stopConnector(String connName) {
        removeConnectorTasks(connName);
        try {
            worker.stopConnector(connName);
            connectors.remove(connName);
        } catch (CopycatException e) {
            log.error("Error shutting down connector {}: ", connName, e);
        }
    }

    private void createConnectorTasks(String connName) {
        ConnectorState state = connectors.get(connName);
        Map<ConnectorTaskId, Map<String, String>> taskConfigs = worker.reconfigureConnectorTasks(connName,
                state.config.getInt(ConnectorConfig.TASKS_MAX_CONFIG),
                state.config.getList(ConnectorConfig.TOPICS_CONFIG));

        for (Map.Entry<ConnectorTaskId, Map<String, String>> taskEntry : taskConfigs.entrySet()) {
            ConnectorTaskId taskId = taskEntry.getKey();
            TaskConfig config = new TaskConfig(taskEntry.getValue());
            try {
                worker.addTask(taskId, config);
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

    private void removeConnectorTasks(String connName) {
        ConnectorState state = connectors.get(connName);
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

    private void updateConnectorTasks(String connName) {
        removeConnectorTasks(connName);
        createConnectorTasks(connName);
    }


    private static class ConnectorState {
        public ConnectorConfig config;
        Set<ConnectorTaskId> tasks;

        public ConnectorState(ConnectorConfig config) {
            this.config = config;
            this.tasks = new HashSet<>();
        }
    }
}
