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
package org.apache.kafka.connect.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A handle to a connector executing in a Connect cluster.
 */
public class ConnectorHandle {

    private static final Logger log = LoggerFactory.getLogger(ConnectorHandle.class);
    private final String connectorName;

    private Map<String, TaskHandle> taskHandles = new ConcurrentHashMap<>();

    public ConnectorHandle(String connectorName) {
        this.connectorName = connectorName;
    }

    /**
     * Get or create a task handle for a given task id. The task need not be created when this method is called. If the
     * handle is called before the task is created, the task will bind to the handle once it starts (or restarts).
     *
     * @param taskId the task id
     * @return a non-null {@link TaskHandle}
     */
    public TaskHandle taskHandle(String taskId) {
        return taskHandles.computeIfAbsent(taskId, k -> new TaskHandle(taskId));
    }

    /**
     * Delete the task handle for this task id.
     *
     * @param taskId the task id.
     */
    public void deleteTask(String taskId) {
        log.info("Removing handle for {} task in connector {}", taskId, connectorName);
        taskHandles.remove(taskId);
    }

}
