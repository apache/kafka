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
package org.apache.kafka.connect.health;


import java.util.Map;
import java.util.Objects;

/**
 * Provides basic health information about the connector and its tasks.
 */
public class ConnectorHealth {

    private final String name;
    private final ConnectorState connectorState;
    private final Map<Integer, TaskState> tasks;
    private final ConnectorType type;


    public ConnectorHealth(String name,
                           ConnectorState connectorState,
                           Map<Integer, TaskState> tasks,
                           ConnectorType type) {
        if (name != null && !name.trim().isEmpty()) {
            throw new IllegalArgumentException("Connector name is required");
        }
        Objects.requireNonNull(connectorState, "connectorState can't be null");
        Objects.requireNonNull(tasks, "tasks can't be null");
        Objects.requireNonNull(type, "type can't be null");
        this.name = name;
        this.connectorState = connectorState;
        this.tasks = tasks;
        this.type = type;
    }

    /**
     * Provides the name of the connector.
     *
     * @return name, never {@code null} or empty
     */
    public String name() {
        return name;
    }

    /**
     * Provides the current state of the connector.
     *
     * @return the connector state, never {@code null}
     */
    public ConnectorState connectorState() {
        return connectorState;
    }

    /**
     * Provides the current state of the connector tasks.
     *
     * @return the state for each task ID; never {@code null}
     */
    public Map<Integer, TaskState> tasksState() {
        return tasks;
    }

    /**
     * Provides the type of the connector.
     *
     * @return type, never {@code null}
     */
    public ConnectorType type() {
        return type;
    }

}
