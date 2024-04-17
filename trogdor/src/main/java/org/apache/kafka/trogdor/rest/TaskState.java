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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.kafka.trogdor.task.TaskSpec;

/**
 * The state which a task is in on the Coordinator.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "state")
@JsonSubTypes({
        @JsonSubTypes.Type(value = TaskPending.class, name = TaskStateType.Constants.PENDING_VALUE),
        @JsonSubTypes.Type(value = TaskRunning.class, name = TaskStateType.Constants.RUNNING_VALUE),
        @JsonSubTypes.Type(value = TaskStopping.class, name = TaskStateType.Constants.STOPPING_VALUE),
        @JsonSubTypes.Type(value = TaskDone.class, name = TaskStateType.Constants.DONE_VALUE)
    })
public abstract class TaskState extends Message {
    private final TaskSpec spec;

    private final JsonNode status;

    public TaskState(TaskSpec spec, JsonNode status) {
        this.spec = spec;
        this.status = status == null ? NullNode.instance : status;
    }

    @JsonProperty
    public TaskSpec spec() {
        return spec;
    }

    @JsonProperty
    public JsonNode status() {
        return status;
    }

    public abstract TaskStateType stateType();
}
