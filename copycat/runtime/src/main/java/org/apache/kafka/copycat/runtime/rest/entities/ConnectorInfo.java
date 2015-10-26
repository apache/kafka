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

package org.apache.kafka.copycat.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ConnectorInfo {

    private final String name;
    private final Map<String, String> config;
    private final List<ConnectorTaskId> tasks;

    public ConnectorInfo(String name, Map<String, String> config, List<ConnectorTaskId> tasks) {
        this.name = name;
        this.config = config;
        this.tasks = tasks;
    }

    public ConnectorInfo(String name, Map<String, String> config, Collection<org.apache.kafka.copycat.util.ConnectorTaskId> tasks) {
        this(name, config, jsonTasks(tasks));
    }

    @JsonProperty
    public String name() {
        return name;
    }

    @JsonProperty
    public Map<String, String> config() {
        return config;
    }

    @JsonProperty
    public List<ConnectorTaskId> tasks() {
        return tasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorInfo that = (ConnectorInfo) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(config, that.config) &&
                Objects.equals(tasks, that.tasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, config, tasks);
    }


    private static List<ConnectorTaskId> jsonTasks(Collection<org.apache.kafka.copycat.util.ConnectorTaskId> tasks) {
        List<ConnectorTaskId> jsonTasks = new ArrayList<>();
        for (org.apache.kafka.copycat.util.ConnectorTaskId task : tasks)
            jsonTasks.add(new ConnectorTaskId(task));
        return jsonTasks;
    }
}
