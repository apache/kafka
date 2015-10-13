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

import org.apache.kafka.copycat.util.ConnectorTaskId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An immutable snapshot of the configuration state of connectors and tasks in a Copycat cluster.
 */
public class ClusterConfigState {
    private final long offset;
    private final Map<String, Integer> connectorTaskCounts;
    private final Map<String, Map<String, String>> connectorConfigs;
    private final Map<ConnectorTaskId, Map<String, String>> taskConfigs;
    private final Set<String> inconsistentConnectors;

    public ClusterConfigState(long offset,
                              Map<String, Integer> connectorTaskCounts,
                              Map<String, Map<String, String>> connectorConfigs,
                              Map<ConnectorTaskId, Map<String, String>> taskConfigs,
                              Set<String> inconsistentConnectors) {
        this.offset = offset;
        this.connectorTaskCounts = connectorTaskCounts;
        this.connectorConfigs = connectorConfigs;
        this.taskConfigs = taskConfigs;
        this.inconsistentConnectors = inconsistentConnectors;
    }

    /**
     * Get the last offset read to generate this config state. This offset is not guaranteed to be perfectly consistent
     * with the recorded state because some partial updates to task configs may have been read.
     * @return the latest config offset
     */
    public long offset() {
        return offset;
    }

    /**
     * Get a list of the connectors in this configuration
     */
    public Collection<String> connectors() {
        return connectorTaskCounts.keySet();
    }

    /**
     * Get the configuration for a connector.
     * @param connector name of the connector
     * @return a map containing configuration parameters
     */
    public Map<String, String> connectorConfig(String connector) {
        return connectorConfigs.get(connector);
    }

    /**
     * Get the configuration for a task.
     * @param task id of the task
     * @return a map containing configuration parameters
     */
    public Map<String, String> taskConfig(ConnectorTaskId task) {
        return taskConfigs.get(task);
    }

    /**
     * Get the current set of task IDs for the specified connector.
     * @param connectorName the name of the connector to look up task configs for
     * @return the current set of connector task IDs
     */
    public Collection<ConnectorTaskId> tasks(String connectorName) {
        if (inconsistentConnectors.contains(connectorName))
            return Collections.EMPTY_LIST;

        Integer numTasks = connectorTaskCounts.get(connectorName);
        if (numTasks == null)
            throw new IllegalArgumentException("Connector does not exist in current configuration.");

        List<ConnectorTaskId> taskIds = new ArrayList<>();
        for (int taskIndex = 0; taskIndex < numTasks; taskIndex++) {
            ConnectorTaskId taskId = new ConnectorTaskId(connectorName, taskIndex);
            taskIds.add(taskId);
        }
        return taskIds;
    }

    /**
     * Get the set of connectors which have inconsistent data in this snapshot. These inconsistencies can occur due to
     * partially completed writes combined with log compaction.
     *
     * Connectors in this set will appear in the output of {@link #connectors()} since their connector configuration is
     * available, but not in the output of {@link #taskConfig(ConnectorTaskId)} since the task configs are incomplete.
     *
     * When a worker detects a connector in this state, it should request that the connector regenerate its task
     * configurations.
     *
     * @return the set of inconsistent connectors
     */
    public Set<String> inconsistentConnectors() {
        return inconsistentConnectors;
    }

}
