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
package org.apache.kafka.connect.runtime;

import java.util.Objects;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;

/**
 * A request to restart a connector and/or task instances.
 */
public class RestartRequest {

    private final String connectorName;
    private final boolean onlyFailed;
    private final boolean includeTasks;

    /**
     * Create a new request to restart a connector and optionally its tasks.
     *
     * @param connectorName the name of the connector; may not be null
     * @param onlyFailed    true if only failed instances should be restarted
     * @param includeTasks  true if tasks should be restarted, or false if only the connector should be restarted
     */
    public RestartRequest(String connectorName, boolean onlyFailed, boolean includeTasks) {
        this.connectorName = Objects.requireNonNull(connectorName, "Connector name may not be null");
        this.onlyFailed = onlyFailed;
        this.includeTasks = includeTasks;
    }

    /**
     * Get the name of the connector.
     *
     * @return the connector name; never null
     */
    public String connectorName() {
        return connectorName;
    }

    /**
     * Determine whether only failed instances be restarted.
     *
     * @return true if only failed instances should be restarted, or false if all applicable instances should be restarted
     */
    public boolean onlyFailed() {
        return onlyFailed;
    }

    /**
     * Determine whether {@link Task} instances should also be restarted in addition to the {@link Connector} instance.
     *
     * @return true if the connector and task instances should be restarted, or false if just the connector should be restarted
     */
    public boolean includeTasks() {
        return includeTasks;
    }

    /**
     * Determine whether the connector with the given status is to be restarted.
     *
     * @param status the connector status; may not be null
     * @return true if the connector is to be restarted, or false otherwise
     */
    public boolean shouldRestartConnector(ConnectorStatus status) {
        return !onlyFailed || status.state() == AbstractStatus.State.FAILED;
    }

    /**
     * Determine whether only the {@link Connector} instance is to be restarted even if not failed.
     *
     * @return true if only the {@link Connector} instance is to be restarted even if not failed, or false otherwise
     */
    public boolean forciblyRestartConnectorOnly() {
        return !onlyFailed() && !includeTasks();
    }

    /**
     * Determine whether the task instance with the given status is to be restarted.
     *
     * @param status the task status; may not be null
     * @return true if the task is to be restarted, or false otherwise
     */
    public boolean shouldRestartTask(TaskStatus status) {
        return includeTasks && (!onlyFailed || status.state() == AbstractStatus.State.FAILED);
    }

    @Override
    public String toString() {
        return "restart request for {" + "connectorName='" + connectorName + "', onlyFailed=" + onlyFailed + ", includeTasks=" + includeTasks + '}';
    }
}
