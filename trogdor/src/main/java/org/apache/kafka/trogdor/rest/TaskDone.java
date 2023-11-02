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
import org.apache.kafka.trogdor.task.TaskSpec;

/**
 * The state a task is in once it's done.
 */
public class TaskDone extends TaskState {
    /**
     * The time on the coordinator when the task was started.
     */
    private final long startedMs;

    /**
     * The time on the coordinator when the task was completed.
     */
    private final long doneMs;

    /**
     * Empty if the task completed without error; the error message otherwise.
     */
    private final String error;

    /**
     * True if the task was manually cancelled, rather than terminating itself.
     */
    private final boolean cancelled;

    @JsonCreator
    public TaskDone(@JsonProperty("spec") TaskSpec spec,
            @JsonProperty("startedMs") long startedMs,
            @JsonProperty("doneMs") long doneMs,
            @JsonProperty("error") String error,
            @JsonProperty("cancelled") boolean cancelled,
            @JsonProperty("status") JsonNode status) {
        super(spec, status);
        this.startedMs = startedMs;
        this.doneMs = doneMs;
        this.error = error;
        this.cancelled = cancelled;
    }

    @JsonProperty
    public long startedMs() {
        return startedMs;
    }

    @JsonProperty
    public long doneMs() {
        return doneMs;
    }

    @JsonProperty
    public String error() {
        return error;
    }

    @JsonProperty
    public boolean cancelled() {
        return cancelled;
    }

    @Override
    public TaskStateType stateType() {
        return TaskStateType.DONE;
    }
}
