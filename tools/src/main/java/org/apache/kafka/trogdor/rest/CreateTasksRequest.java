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
import org.apache.kafka.trogdor.task.TaskSpec;

import java.util.Collections;
import java.util.Map;

/**
 * A request to the Trogdor coordinator to create mulitple tasks.
 */
public class CreateTasksRequest extends Message {
    private final Map<String, TaskSpec> tasks;

    @JsonCreator
    public CreateTasksRequest(@JsonProperty(value = "tasks", required = true) Map<String, TaskSpec>  tasks) {
        this.tasks = tasks == null ? Collections.emptyMap() : Collections.unmodifiableMap(tasks);
    }

    @JsonProperty
    public Map<String, TaskSpec> tasks() {
        return tasks;
    }
}
