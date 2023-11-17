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
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An interface to store and retrieve (via {@link #snapshot()}) configuration information that is created during
 * runtime (i.e. not static configuration like the {@link org.apache.kafka.connect.runtime.WorkerConfig worker config}).
 * This configuration information includes connector configs, task configs, connector target states etc.
 */
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
     * @param targetState the desired target state for the connector; may be {@code null} if no target state change is desired. Note that the default
     *                    target state is {@link TargetState#STARTED} if no target state exists previously
     */
    void putConnectorConfig(String connector, Map<String, String> properties, TargetState targetState);

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

    /**
     * Store a new {@link SessionKey} that can be used to validate internal (i.e., non-user-triggered) inter-worker communication.
     * @param sessionKey the session key to store
     */
    void putSessionKey(SessionKey sessionKey);

    /**
     * Request a restart of a connector and optionally its tasks.
     * @param restartRequest the restart request details
     */
    void putRestartRequest(RestartRequest restartRequest);

    /**
     * Record the number of tasks for the connector after a successful round of zombie fencing.
     * @param connector name of the connector
     * @param taskCount number of tasks used by the connector
     */
    void putTaskCountRecord(String connector, int taskCount);

    /**
     * Prepare to write to the backing config store. May be required by some implementations (such as those that only permit a single
     * writer at a time across a cluster of workers) before performing mutating operations like writing configurations, target states, etc.
     * The default implementation is a no-op; it is the responsibility of the implementing class to override this and document any expectations for
     * when it must be invoked.
     */
    default void claimWritePrivileges() {
    }

    /**
     * Emit a new level for the specified logging namespace (and all of its children). This level should
     * be applied by all workers currently in the cluster, but not to workers that join after it is stored.
     * @param namespace the namespace to adjust; may not be null
     * @param level the new level for the namespace; may not be null
     */
    void putLoggerLevel(String namespace, String level);

    /**
     * Set an update listener to get notifications when there are new records written to the backing store.
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

        /**
         * Invoked when a dynamic log level adjustment has been read
         * @param namespace the namespace to adjust; never null
         * @param level the level to set the namespace to; never null
         */
        void onLoggingLevelUpdate(String namespace, String level);
    }

}
