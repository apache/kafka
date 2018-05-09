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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.kafka.trogdor.task.TaskSpec;

/**
 * The state a worker is in once it's done.
 */
public class WorkerDone extends WorkerState {
    /**
     * The time on the agent when the task was started.
     */
    private final long startedMs;

    /**
     * The time on the agent when the task was completed.
     */
    private final long doneMs;

    /**
     * The task status.  The format will depend on the type of task that is
     * being run.
     */
    private final JsonNode status;

    /**
     * Empty if the task completed without error; the error message otherwise.
     */
    private final String error;

    @JsonCreator
    public WorkerDone(@JsonProperty("taskId") String taskId,
            @JsonProperty("spec") TaskSpec spec,
            @JsonProperty("startedMs") long startedMs,
            @JsonProperty("doneMs") long doneMs,
            @JsonProperty("status") JsonNode status,
            @JsonProperty("error") String error) {
        super(taskId, spec);
        this.startedMs = startedMs;
        this.doneMs = doneMs;
        this.status = status == null ? NullNode.instance : status;
        this.error = error == null ? "" : error;
    }

    @JsonProperty
    @Override
    public long startedMs() {
        return startedMs;
    }

    @JsonProperty
    public long doneMs() {
        return doneMs;
    }

    @JsonProperty
    @Override
    public JsonNode status() {
        return status;
    }

    @JsonProperty
    public String error() {
        return error;
    }

    @Override
    public boolean done() {
        return true;
    }
}
