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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.SessionKey;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface ConfigBackingStore {

    void start();

    void stop();

    /**
     * Get a snapshot of the current configuration state including all connector and task
     * configurations.
     * @return the cluster config state
     */
    ClusterConfigState snapshot();

    /**
     * Check if the store has configuration for a connector.
     * @param connector name of the connector
     * @return true if the backing store contains configuration for the connector
     */
    boolean contains(String connector);

    /**
     * Update the configuration for a connector.
     * @param connector name of the connector
     * @param properties the connector configuration
     */
    void putConnectorConfig(String connector, Map<String, String> properties);

    /**
     * Remove configuration for a connector
     * @param connector name of the connector
     */
    void removeConnectorConfig(String connector);

    /**
     * Update the task configurations for a connector.
     * @param connector name of the connector
     * @param configs the new task configs for the connector
     */
    void putTaskConfigs(String connector, List<Map<String, String>> configs);

    /**
     * Remove the task configs associated with a connector.
     * @param connector name of the connector
     */
    void removeTaskConfigs(String connector);

    /**
     * Refresh the backing store. This forces the store to ensure that it has the latest
     * configs that have been written.
     * @param timeout max time to wait for the refresh to complete
     * @param unit unit of timeout
     * @throws TimeoutException if the timeout expires before the refresh has completed
     */
    void refresh(long timeout, TimeUnit unit) throws TimeoutException;

    /**
     * Transition a connector to a new target state (e.g. paused).
     * @param connector name of the connector
     * @param state the state to transition to
     */
    void putTargetState(String connector, TargetState state);

    void putSessionKey(SessionKey sessionKey);

    /**
     * Request a restart of a connector and optionally its tasks.
     * @param restartRequest the restart request details
     */
    void putRestartRequest(RestartRequest restartRequest);

    /**
     * Set an update listener to get notifications when there are config/target state
     * changes.
     * @param listener non-null listener
     */
    void setUpdateListener(UpdateListener listener);

    interface UpdateListener {
        /**
         * Invoked when a connector configuration has been removed
         * @param connector name of the connector
         */
        void onConnectorConfigRemove(String connector);

        /**
         * Invoked when a connector configuration has been updated.
         * @param connector name of the connector
         */
        void onConnectorConfigUpdate(String connector);

        /**
         * Invoked when task configs are updated.
         * @param tasks all the tasks whose configs have been updated
         */
        void onTaskConfigUpdate(Collection<ConnectorTaskId> tasks);

        /**
         * Invoked when the user has set a new target state (e.g. paused)
         * @param connector name of the connector
         */
        void onConnectorTargetStateChange(String connector);

        /**
         * Invoked when the leader has distributed a new session key
         * @param sessionKey the {@link SessionKey session key}
         */
        void onSessionKeyUpdate(SessionKey sessionKey);

        /**
         * Invoked when a connector and possibly its tasks have been requested to be restarted.
         * @param restartRequest the {@link RestartRequest restart request}
         */
        void onRestartRequest(RestartRequest restartRequest);
    }

}
