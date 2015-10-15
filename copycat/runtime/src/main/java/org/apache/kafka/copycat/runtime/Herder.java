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

package org.apache.kafka.copycat.runtime;

import org.apache.kafka.copycat.util.Callback;

import java.util.Map;

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
     * Submit a connector job to the cluster. This works from any node by forwarding the request to
     * the leader herder if necessary.
     *
     * @param connectorProps user-specified properties for this job
     * @param callback       callback to invoke when the request completes
     */
    void addConnector(Map<String, String> connectorProps, Callback<String> callback);

    /**
     * Delete a connector job by name.
     *
     * @param name     name of the connector job to shutdown and delete
     * @param callback callback to invoke when the request completes
     */
    void deleteConnector(String name, Callback<Void> callback);

    /**
     * Requests reconfiguration of the task. This should only be triggered by
     * {@link HerderConnectorContext}.
     *
     * @param connName name of the connector that should be reconfigured
     */
    void requestTaskReconfiguration(String connName);

}