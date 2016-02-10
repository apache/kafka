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
package org.apache.kafka.connect.storage;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Map;
import java.util.Set;

public interface StatusBackingStore extends Configurable {

    /**
     * Start dependent services (if needed)
     */
    void start();

    /**
     * Stop dependent services (if needed)
     */
    void stop();

    /**
     * Set the state of the connector to the given value.
     * @param connector the connector name
     * @param status the status of the connector
     */
    void put(String connector, ConnectorStatus status);

    /**
     * Safely set the state of the connector to the given value. What is
     * considered "safe" depends on the implementation, but basically it
     * means that the store can provide higher assurance that another worker
     * hasn't concurrently written any conflicting data.
     * @param connector the connector name
     * @param status the status of the connector
     */
    void putSafe(String connector, ConnectorStatus status);

    /**
     * Set the state of the connector to the given value.
     * @param id the id of the task
     * @param status the status of the task
     */
    void put(ConnectorTaskId id, TaskStatus status);

    /**
     * Safely set the state of the task to the given value. What is
     * considered "safe" depends on the implementation, but basically it
     * means that the store can provide higher assurance that another worker
     * hasn't concurrently written any conflicting data.
     * @param id the id of the task
     * @param status the status of the task
     */
    void putSafe(ConnectorTaskId id, TaskStatus status);

    /**
     * Get the current state of the task.
     * @param id the id of the task
     * @return the state or null if there is none
     */
    TaskStatus get(ConnectorTaskId id);

    /**
     * Get the current state of the connector.
     * @param connector the connector name
     * @return the state or null if there is none
     */
    ConnectorStatus get(String connector);

    /**
     * Get the states of all tasks for the given connector.
     * @param connector the connector name
     * @return a map from task ids to their respective status
     */
    Map<Integer, TaskStatus> getAll(String connector);

    /**
     * Get all cached connectors.
     * @return the set of connector names
     */
    Set<String> connectors();

    /**
     * Flush any pending writes
     */
    void flush();
}
