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

import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;
import org.apache.kafka.connect.runtime.rest.entities.Message;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * <p>
 * The herder interface tracks and manages workers and connectors. It is the main interface for external components
 * to make changes to the state of the cluster. For example, in distributed mode, an implementation of this interface
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
 * In standalone mode, the implementation of this interface will be trivial because no coordination is needed. In that case,
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
     * @return whether the worker is ready; i.e., it has completed all initialization and startup
     * steps such as creating internal topics, joining a cluster, etc.
     */
    boolean isReady();

    /**
     * Check for worker health; i.e., its ability to service external requests from the user such
     * as creating, reconfiguring, and deleting connectors
     * @param callback callback to invoke once worker health is assured
     */
    void healthCheck(Callback<Void> callback);

    /**
     * Get a list of connectors currently running in this cluster. This is a full list of connectors in the cluster gathered
     * from the current configuration.
     *
     * @param callback callback to invoke with the full list of connector names
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
     * Set the configuration for a connector. This supports creation and updating.
     * @param connName name of the connector
     * @param config the connector's configuration
     * @param allowReplace if true, allow overwriting previous configs; if false, throw {@link AlreadyExistsException}
     *                     if a connector with the same name already exists
     * @param callback callback to invoke when the configuration has been written
     */
    void putConnectorConfig(String connName, Map<String, String> config, boolean allowReplace, Callback<Created<ConnectorInfo>> callback);

    /**
     * Set the configuration for a connector, along with a target state optionally. This supports creation and updating.
     * @param connName name of the connector
     * @param config the connector's configuration
     * @param targetState the desired target state for the connector; may be {@code null} if no target state change is desired. Note that the default
     *                    target state is {@link TargetState#STARTED} if no target state exists previously
     * @param allowReplace if true, allow overwriting previous configs; if false, throw {@link AlreadyExistsException}
     *                     if a connector with the same name already exists
     * @param callback callback to invoke when the configuration has been written
     */
    void putConnectorConfig(String connName, Map<String, String> config, TargetState targetState, boolean allowReplace,
                            Callback<Created<ConnectorInfo>> callback);

    /**
     * Patch the configuration for a connector.
     * @param connName name of the connector
     * @param configPatch the connector's configuration patch.
     * @param callback callback to invoke when the configuration has been written
     */
    void patchConnectorConfig(String connName, Map<String, String> configPatch, Callback<Created<ConnectorInfo>> callback);

    /**
     * Delete a connector and its configuration.
     * @param connName name of the connector
     * @param callback callback to invoke when the configuration has been written
     */
    void deleteConnectorConfig(String connName, Callback<Created<ConnectorInfo>> callback);

    /**
     * Requests reconfiguration of the tasks of a connector. This should only be triggered by
     * {@link HerderConnectorContext}.
     *
     * @param connName name of the connector that should be reconfigured
     */
    void requestTaskReconfiguration(String connName);

    /**
     * Get the configurations for the current set of tasks of a connector.
     * @param connName name of the connector
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
     * @param requestSignature the signature of the request made for this task (re-)configuration;
     *                         may be null if no signature was provided
     */
    void putTaskConfigs(String connName, List<Map<String, String>> configs, Callback<Void> callback, InternalRequestSignature requestSignature);

    /**
     * Fence out any older task generations for a source connector, and then write a record to the config topic
     * indicating that it is safe to bring up a new generation of tasks. If that record is already present, do nothing
     * and invoke the callback successfully.
     * @param connName the name of the connector to fence out, which must refer to a source connector; if the
     *                 connector does not exist or is not a source connector, the callback will be invoked with an error
     * @param callback callback to invoke upon completion
     * @param requestSignature the signature of the request made for this connector;
     *                         may be null if no signature was provided
     */
    void fenceZombieSourceTasks(String connName, Callback<Void> callback, InternalRequestSignature requestSignature);

    /**
     * Get a list of connectors currently running in this cluster.
     * @return A list of connector names
     */
    Collection<String> connectors();

    /**
     * Get the definition and status of a connector.
     * @param connName name of the connector
     */
    ConnectorInfo connectorInfo(String connName);

    /**
     * Lookup the current status of a connector.
     * @param connName name of the connector
     */
    ConnectorStateInfo connectorStatus(String connName);

    /**
     * Lookup the set of topics currently used by a connector.
     *
     * @param connName name of the connector
     * @return the set of active topics
     */
    ActiveTopicsInfo connectorActiveTopics(String connName);

    /**
     * Request to asynchronously reset the active topics for the named connector.
     *
     * @param connName name of the connector
     */
    void resetConnectorActiveTopics(String connName);

    /**
     * Return a reference to the status backing store used by this herder.
     *
     * @return the status backing store used by this herder
     */
    StatusBackingStore statusBackingStore();

    /**
     * Lookup the status of a task.
     * @param id id of the task
     */
    ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId id);

    /**
     * Validate the provided connector config values against the configuration definition.
     * @param connectorConfig the provided connector config values
     * @param callback the callback to invoke after validation has completed (successfully or not)
     */
    void validateConnectorConfig(Map<String, String> connectorConfig, Callback<ConfigInfos> callback);

    /**
     * Validate the provided connector config values against the configuration definition.
     * @param connectorConfig the provided connector config values
     * @param callback the callback to invoke after validation has completed (successfully or not)
     * @param doLog if true log all the connector configurations at INFO level; if false, no connector configurations are logged.
     *              Note that logging of configuration is not necessary in every endpoint that uses this method.
     */
    default void validateConnectorConfig(Map<String, String> connectorConfig, Callback<ConfigInfos> callback, boolean doLog) {
        validateConnectorConfig(connectorConfig, callback);
    }

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
     * Restart the connector.
     * @param delayMs delay before restart
     * @param connName name of the connector
     * @param cb callback to invoke upon completion
     * @return The id of the request
     */
    HerderRequest restartConnector(long delayMs, String connName, Callback<Void> cb);

    /**
     * Restart the connector and optionally its tasks.
     * @param request the details of the restart request
     * @param cb      callback to invoke upon completion with the connector state info
     */
    void restartConnectorAndTasks(RestartRequest request, Callback<ConnectorStateInfo> cb);

    /**
     * Stop the connector. This call will asynchronously suspend processing by the connector and
     * shut down all of its tasks.
     * @param connector name of the connector
     * @param cb callback to invoke upon completion
     */
    void stopConnector(String connector, Callback<Void> cb);

    /**
     * Pause the connector. This call will asynchronously suspend processing by the connector and all
     * of its tasks.
     * <p>
     * Note that, unlike {@link #stopConnector(String, Callback)}, tasks for this connector will not
     * be shut down and none of their resources will be de-allocated. Instead, they will be left in an
     * "idling" state where no data is polled from them (if source tasks) or given to them (if sink tasks),
     * but all internal state kept by the tasks and their resources is left intact and ready to begin
     * processing records again as soon as the connector is {@link #resumeConnector(String) resumed}.
     * @param connector name of the connector
     */
    void pauseConnector(String connector);

    /**
     * Resume the connector. This call will asynchronously start the connector and its tasks (if
     * not started already).
     * @param connector name of the connector
     */
    void resumeConnector(String connector);

    /**
     * Returns a handle to the plugin factory used by this herder and its worker.
     *
     * @return a reference to the plugin factory.
     */
    Plugins plugins();

    /**
     * Get the cluster ID of the Kafka cluster backing this Connect cluster.
     * @return the cluster ID of the Kafka cluster backing this connect cluster
     */
    String kafkaClusterId();


    /**
     * Returns the configuration of a plugin
     * @param pluginName the name of the plugin
     * @return the list of ConfigKeyInfo of the plugin
     */
    List<ConfigKeyInfo> connectorPluginConfig(String pluginName);

    /**
     * Get the current offsets for a connector.
     * @param connName the name of the connector whose offsets are to be retrieved
     * @param cb callback to invoke upon completion
     */
    void connectorOffsets(String connName, Callback<ConnectorOffsets> cb);

    /**
     * Alter a connector's offsets.
     * @param connName the name of the connector whose offsets are to be altered
     * @param offsets a mapping from partitions to offsets that need to be written
     * @param cb callback to invoke upon completion
     */
    void alterConnectorOffsets(String connName, Map<Map<String, ?>, Map<String, ?>> offsets, Callback<Message> cb);

    /**
     * Reset a connector's offsets.
     * @param connName the name of the connector whose offsets are to be reset
     * @param cb callback to invoke upon completion
     */
    void resetConnectorOffsets(String connName, Callback<Message> cb);

    /**
     * Get the level for a logger.
     * @param logger the name of the logger to retrieve the level for; may not be null
     * @return the level for the logger, or null if no logger with the given name exists
     */
    LoggerLevel loggerLevel(String logger);

    /**
     * Get the levels for all known loggers.
     * @return a map of logger name to {@link LoggerLevel}; may be empty, but never null
     */
    Map<String, LoggerLevel> allLoggerLevels();

    /**
     * Set the level for a logging namespace (i.e., a specific logger and all of its children) on this
     * worker. Changes should only last over the lifetime of the worker, and should be wiped if/when
     * the worker is restarted.
     * @param namespace the logging namespace to alter; may not be null
     * @param level the new level to set for the namespace; may not be null
     * @return all loggers that were affected by this action; may be empty (including if the specified
     * level is not a valid logging level), but never null
     */
    List<String> setWorkerLoggerLevel(String namespace, String level);

    /**
     * Set the level for a logging namespace (i.e., a specific logger and all of its children) for all workers
     * in the cluster. Changes should only last over the lifetime of workers, and should be wiped if/when
     * workers are restarted.
     * @param namespace the logging namespace to alter; may not be null
     * @param level the new level to set for the namespace; may not be null
     */
    void setClusterLoggerLevel(String namespace, String level);

    enum ConfigReloadAction {
        NONE,
        RESTART
    }

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
