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

import org.apache.kafka.connect.connector.Connector;

import java.util.Objects;

/**
 * A request to restart a connector and/or task instances.
 * <p>
 * The natural order is based first upon the connector name and then requested restart behaviors.
 * If two requests have the same connector name, then the requests are ordered based on the
 * probable number of tasks/connector this request is going to restart.
 * @param connectorName the name of the connector; may not be null
 * @param onlyFailed    true if only failed instances should be restarted
 * @param includeTasks  true if tasks should be restarted, or false if only the connector should be restarted
 */
public record RestartRequest(String connectorName,
                             boolean onlyFailed,
                             boolean includeTasks) implements Comparable<RestartRequest> {

    public RestartRequest {
        Objects.requireNonNull(connectorName, "Connector name may not be null");
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
    public String toString() {
        return "restart request for {" + "connectorName='" + connectorName + "', onlyFailed=" + onlyFailed + ", includeTasks=" + includeTasks + '}';
    }
}
