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

public class ConnectorHealth {

    private final String name;
    private final ConnectorState connector;
    private final Map<Integer, TaskState> tasks;
    private final ConnectorType type;


    public ConnectorHealth(String name,
                           ConnectorState connector,
                           Map<Integer, TaskState> tasks,
                           ConnectorType type) {
        this.name = name;
        this.connector = connector;
        this.tasks = tasks;
        this.type = type;
    }

    public String name() {
        return name;
    }

    public ConnectorState connectorState() {
        return connector;
    }

    public Map<Integer, TaskState> tasksState() {
        return tasks;
    }

    public ConnectorType type() {
        return type;
    }

}
