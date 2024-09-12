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

package org.apache.kafka.trogdor.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * The response to /coordinator/tasks
 */
public class TasksResponse extends Message {
    private final Map<String, TaskState> tasks;

    @JsonCreator
    public TasksResponse(@JsonProperty("tasks") TreeMap<String, TaskState> tasks) {
        this.tasks = Collections.unmodifiableMap((tasks == null) ? new TreeMap<>() : tasks);
    }

    @JsonProperty
    public Map<String, TaskState> tasks() {
        return tasks;
    }
}
