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

package org.apache.kafka.trogdor.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.kafka.trogdor.common.JsonUtil;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;


/**
 * The specification for a task. This should be immutable and suitable for serializing and sending over the wire.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
              include = JsonTypeInfo.As.PROPERTY,
              property = "class")
public abstract class TaskSpec {
    /**
     * The maximum task duration.
     *
     * We cap the task duration at this value to avoid worrying about 64-bit overflow or floating
     * point rounding.  (Objects serialized as JSON canonically contain only floating point numbers,
     * because JavaScript did not support integers.)
     */
    public final static long MAX_TASK_DURATION_MS = 1000000000000000L;

    /**
     * When the time should start in milliseconds.
     */
    private final long startMs;

    /**
     * How long the task should run in milliseconds.
     */
    private final long durationMs;

    protected TaskSpec(@JsonProperty("startMs") long startMs,
            @JsonProperty("durationMs") long durationMs) {
        this.startMs = startMs;
        this.durationMs = Math.max(0, Math.min(durationMs, MAX_TASK_DURATION_MS));
    }

    /**
     * Get the target start time of this task in ms.
     */
    @JsonProperty
    public final long startMs() {
        return startMs;
    }

    /**
     * Get the deadline time of this task in ms
     */
    public final long endMs() {
        return startMs + durationMs;
    }

    /**
     * Get the duration of this task in ms.
     */
    @JsonProperty
    public final long durationMs() {
        return durationMs;
    }

    /**
     * Hydrate this task on the coordinator.
     *
     * @param id        The task id.
     */
    public abstract TaskController newController(String id);

    /**
     * Hydrate this task on the agent.
     *
     * @param id        The worker id.
     */
    public abstract TaskWorker newTaskWorker(String id);

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return toString().equals(o.toString());
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(toString());
    }

    @Override
    public String toString() {
        return JsonUtil.toJsonString(this);
    }

    protected Map<String, String> configOrEmptyMap(Map<String, String> config) {
        return (config == null) ? Collections.<String, String>emptyMap() : config;
    }
}
