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

package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * <p>
 * The herder interface tracks and manages workers and connectors. It is the main interface for external components
 * to make changes to the state of the cluster. For example, in distributed mode, an implementation of this class
 * knows how to accept a connector configuration, may need to route it to the current leader worker for the cluster so
 * the config can be written to persistent storage, and then ensures the new connector is correctly instantiated on one
 * of the workers.
 * </p>
 * <p>
 * This class must implement all the actions that can be taken on the cluster (add/remove connectors, pause/resume tasks,
 * get state of connectors and tasks, etc). The non-Java interfaces to the cluster (REST API and CLI) are very simple
 * wrappers of the functionality provided by this interface.
 * </p>
 * <p>
 * In standalone mode, this implementation of this class will be trivial because no coordination is needed. In that case,
 * the implementation will mainly be delegating tasks directly to other components. For example, when creating a new
 * connector in standalone mode, there is no need to persist the config and the connector and its tasks must run in the
 * same process, so the standalone herder implementation can immediately instantiate and start the connector and its
 * tasks.
 * </p>
 */
public interface Herder {

    void start();

    void stop();

    /**
     * Get a list of connectors currently running in this cluster. This is a full list of connectors in the cluster gathered
     * from the current configuration. However, note
     *
     * @returns A list of connector names
     * @throws org.apache.kafka.connect.runtime.distributed.RequestTargetException if this node can not resolve the request
     *         (e.g., because it has not joined the cluster or does not have configs in sync with the group) and it is
     *         not the leader or the task owner (e.g., task restart must be handled by the worker which owns the task)
     * @throws org.apache.kafka.connect.errors.ConnectException if this node is the leader, but still cannot resolve the
     *         request (e.g., it is not in sync with other worker's config state)
     */
    void connectors(Callback<Collection<String>> callback);

    /**
     * Get the definition and status of a connector.
     */
    void connectorInfo(String connName, Callback<ConnectorInfo> callback);

    /**
     * Get the configuration for a connector.
     * @param connName name of the connector
     * @param callback callback to invoke with the configuration
     */
    void connectorConfig(String connName, Callback<Map<String, String>> callback);

    /**
     * Set the configuration for a connector. This supports creation, update, and deletion.
     * @param connName name of the connector
     * @param config the connectors configuration, or null if deleting the connector
     * @param allowReplace if true, allow overwriting previous configs; if false, throw AlreadyExistsException if a connector
     *                     with the same name already exists
     * @param callback callback to invoke when the configuration has been written
     */
    void putConnectorConfig(String connName, Map<String, String> config, boolean allowReplace, Callback<Created<ConnectorInfo>> callback);

    /**
     * Requests reconfiguration of the task. This should only be triggered by
     * {@link HerderConnectorContext}.
     *
     * @param connName name of the connector that should be reconfigured
     */
    void requestTaskReconfiguration(String connName);

    /**
     * Get the configurations for the current set of tasks of a connector.
     * @param connName connector to update
     * @param callback callback to invoke upon completion
     */
    void taskConfigs(String connName, Callback<List<TaskInfo>> callback);

    /**
     * Set the configurations for the tasks of a connector. This should always include all tasks in the connector; if
     * there are existing configurations and fewer are provided, this will reduce the number of tasks, and if more are
     * provided it will increase the number of tasks.
     * @param connName connector to update
     * @param configs list of configurations
     * @param callback callback to invoke upon completion
     */
    void putTaskConfigs(String connName, List<Map<String, String>> configs, Callback<Void> callback);

    /**
     * Lookup the current status of a connector.
     * @param connName name of the connector
     */
    ConnectorStateInfo connectorStatus(String connName);

    /**
     * Lookup the status of the a task.
     * @param id id of the task
     */
    ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId id);

    /**
     * Validate the provided connector config values against the configuration definition.
     * @param connType the connector class
     * @param connectorConfig the provided connector config values
     */
    ConfigInfos validateConfigs(String connType, Map<String, String> connectorConfig);

    /**
     * Restart the task with the given id.
     * @param id id of the task
     * @param cb callback to invoke upon completion
     */
    void restartTask(ConnectorTaskId id, Callback<Void> cb);

    /**
     * Restart the connector.
     * @param connName name of the connector
     * @param cb callback to invoke upon completion
     */
    void restartConnector(String connName, Callback<Void> cb);

    /**
     * Pause the connector. This call will asynchronously suspend processing by the connector and all
     * of its tasks.
     * @param connector name of the connector
     */
    void pauseConnector(String connector);

    /**
     * Resume the connector. This call will asynchronously start the connector and its tasks (if
     * not started already).
     * @param connector name of the connector
     */
    void resumeConnector(String connector);


    class Created<T> {
        private final boolean created;
        private final T result;

        public Created(boolean created, T result) {
            this.created = created;
            this.result = result;
        }

        public boolean created() {
            return created;
        }

        public T result() {
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Created<?> created1 = (Created<?>) o;
            return Objects.equals(created, created1.created) &&
                    Objects.equals(result, created1.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(created, result);
        }
    }
}