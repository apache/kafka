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
 * <p>The natural order is based first upon the connector name and then requested restart behaviors. 
 * If two requests have the same connector name, then the requests are ordered based on the 
 * probable number of tasks/connector this request is going to restart.
 */
public class RestartRequest implements Comparable<RestartRequest> {

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
    public boolean forceRestartConnectorOnly() {
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
    public int compareTo(RestartRequest o) {
        int result = connectorName.compareTo(o.connectorName);
        return result == 0 ? impactRank() - o.impactRank() : result;
    }
    //calculates an internal rank for the restart request based on the probable number of tasks/connector this request is going to restart
    private int impactRank() {
        if (onlyFailed && !includeTasks) { //restarts only failed connector so least impactful
            return 0;
        } else if (onlyFailed && includeTasks) { //restarts only failed connector and tasks
            return 1;
        } else if (!onlyFailed && !includeTasks) { //restart connector in any state but no tasks
            return 2;
        }
        //onlyFailed==false&&includeTasks  restarts both connector and tasks in any state so highest impact
        return 3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RestartRequest that = (RestartRequest) o;
        return onlyFailed == that.onlyFailed && includeTasks == that.includeTasks && Objects.equals(connectorName, that.connectorName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorName, onlyFailed, includeTasks);
    }

    @Override
    public String toString() {
        return "restart request for {" + "connectorName='" + connectorName + "', onlyFailed=" + onlyFailed + ", includeTasks=" + includeTasks + '}';
    }
}
