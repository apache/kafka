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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.ConnectorTaskId;

/**
 * An immutable restart plan per connector.
 */
public class RestartPlan {

    private final RestartRequest request;
    private final ConnectorStateInfo stateInfo;
    private final Collection<ConnectorTaskId> idsToRestart;

    /**
     * Create a new plan to restart a connector and optionally its tasks.
     *
     * @param request          the restart request; may not be null
     * @param restartStateInfo the current state info for the connector; may not be null
     */
    public RestartPlan(RestartRequest request, ConnectorStateInfo restartStateInfo) {
        this.request = Objects.requireNonNull(request, "RestartRequest name may not be null");
        this.stateInfo = Objects.requireNonNull(restartStateInfo, "ConnectorStateInfo name may not be null");
        // Collect the task IDs to stop and restart (may be none)
        this.idsToRestart = Collections.unmodifiableList(
                stateInfo.tasks()
                        .stream()
                        .filter(this::isRestarting)
                        .map(taskState -> new ConnectorTaskId(request.connectorName(), taskState.id()))
                        .collect(Collectors.toList())
        );
    }

    /**
     * Get the connector name.
     *
     * @return the name of the connector; never null
     */
    public String connectorName() {
        return request.connectorName();
    }

    /**
     * Get the original {@link RestartRequest}.
     *
     * @return the restart request; never null
     */
    public RestartRequest restartRequest() {
        return request;
    }

    /**
     * Get the {@link ConnectorStateInfo} that reflects the current state of the connector <em>except</em> with the {@code status}
     * set to {@link AbstractStatus.State#RESTARTING} for the {@link Connector} instance and any {@link Task} instances that
     * are to be restarted, based upon the {@link #restartRequest() restart request}.
     *
     * @return the connector state info that reflects the restart plan; never null
     */
    public ConnectorStateInfo restartConnectorStateInfo() {
        return stateInfo;
    }

    /**
     * Get the immutable collection of {@link ConnectorTaskId} for all tasks to be restarted
     * based upon the {@link #restartRequest() restart request}.
     *
     * @return the IDs of the tasks to be restarted; never null but possibly empty
     */
    public Collection<ConnectorTaskId> taskIdsToRestart() {
        return idsToRestart;
    }

    /**
     * Determine whether the {@link Connector} instance is to be restarted
     * based upon the {@link #restartRequest() restart request}.
     *
     * @return true if the {@link Connector} instance is to be restarted, or false otherwise
     */
    public boolean shouldRestartConnector() {
        return isRestarting(stateInfo.connector());
    }

    /**
     * Determine whether at least one {@link Task} instance is to be restarted
     * based upon the {@link #restartRequest() restart request}.
     *
     * @return true if any {@link Task} instances are to be restarted, or false if none are to be restarted
     */
    public boolean shouldRestartTasks() {
        return !taskIdsToRestart().isEmpty();
    }

    /**
     * Get the number of connector tasks that are to be restarted
     * based upon the {@link #restartRequest() restart request}.
     *
     * @return the number of {@link Task} instance is to be restarted
     */
    public int restartTaskCount() {
        return taskIdsToRestart().size();
    }

    /**
     * Get the total number of tasks in the connector.
     *
     * @return the total number of tasks
     */
    public int totalTaskCount() {
        return stateInfo.tasks().size();
    }

    private boolean isRestarting(ConnectorStateInfo.AbstractState state) {
        return isRestarting(state.state());
    }

    private boolean isRestarting(String state) {
        return AbstractStatus.State.RESTARTING.toString().equalsIgnoreCase(state);
    }

    @Override
    public String toString() {
        return shouldRestartConnector()
                ? String.format("plan to restart connector and %d of %d tasks for %s", restartTaskCount(), totalTaskCount(), request)
                : String.format("plan to restart %d of %d tasks for %s", restartTaskCount(), totalTaskCount(), request);
    }
}
