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

import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.TopicStatus;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Collection;
import java.util.Set;

/**
 * An interface to store and retrieve connector / task statuses.
 */
public interface StatusBackingStore {

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
     * @param status the status of the connector
     */
    void put(ConnectorStatus status);

    /**
     * Safely set the state of the connector to the given value. What is
     * considered "safe" depends on the implementation, but basically it
     * means that the store can provide higher assurance that another worker
     * hasn't concurrently written any conflicting data.
     * @param status the status of the connector
     */
    void putSafe(ConnectorStatus status);

    /**
     * Set the state of the connector to the given value.
     * @param status the status of the task
     */
    void put(TaskStatus status);

    /**
     * Safely set the state of the task to the given value. What is
     * considered "safe" depends on the implementation, but basically it
     * means that the store can provide higher assurance that another worker
     * hasn't concurrently written any conflicting data.
     * @param status the status of the task
     */
    void putSafe(TaskStatus status);

    /**
     * Set the state of a connector's topic to the given value.
     * @param status the status of the topic used by a connector
     */
    void put(TopicStatus status);

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
    Collection<TaskStatus> getAll(String connector);

    /**
     * Get the status of a connector's topic if the connector is actively using this topic
     * @param connector the connector name; never null
     * @param topic the topic name; never null
     * @return the state or null if there is none
     */
    TopicStatus getTopic(String connector, String topic);

    /**
     * Get the states of all topics that a connector is using.
     * @param connector the connector name; never null
     * @return a collection of topic states or an empty collection if there is none
     */
    Collection<TopicStatus> getAllTopics(String connector);

    /**
     * Delete this topic from the connector's set of active topics
     * @param connector the connector name; never null
     * @param topic the topic name; never null
     */
    void deleteTopic(String connector, String topic);

    /**
     * Get all cached connectors.
     * @return the set of connector names
     */
    Set<String> connectors();

    /**
     * Flush any pending writes
     */
    void flush();

    /**
     * Configure class with the given key-value pairs
     * @param config config for StatusBackingStore
     */
    void configure(WorkerConfig config);
}
