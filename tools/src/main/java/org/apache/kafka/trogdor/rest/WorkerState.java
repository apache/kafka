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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.trogdor.task.TaskSpec;

/**
 * The state which a worker is in on the Agent.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "state")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = WorkerReceiving.class, name = "RECEIVING"),
    @JsonSubTypes.Type(value = WorkerStarting.class, name = "STARTING"),
    @JsonSubTypes.Type(value = WorkerRunning.class, name = "RUNNING"),
    @JsonSubTypes.Type(value = WorkerStopping.class, name = "STOPPING"),
    @JsonSubTypes.Type(value = WorkerDone.class, name = "DONE")
    })
public abstract class WorkerState extends Message {
    private final String taskId;
    private final TaskSpec spec;

    public WorkerState(String taskId, TaskSpec spec) {
        this.taskId = taskId;
        this.spec = spec;
    }

    @JsonProperty
    public String taskId() {
        return taskId;
    }

    @JsonProperty
    public TaskSpec spec() {
        return spec;
    }

    public boolean stopping() {
        return false;
    }

    public boolean done() {
        return false;
    }

    public long startedMs() {
        throw new KafkaException("invalid state");
    }

    public abstract JsonNode status();

    public boolean running() {
        return false;
    }
}
