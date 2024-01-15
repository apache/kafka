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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * The request to /coordinator/tasks
 */
public class TasksRequest extends Message {
    /**
     * The task IDs to list.
     * An empty set of task IDs indicates that we should list all task IDs.
     */
    private final Set<String> taskIds;

    /**
     * If this is non-zero, only tasks with a startMs at or after this time will be listed.
     */
    private final long firstStartMs;

    /**
     * If this is non-zero, only tasks with a startMs at or before this time will be listed.
     */
    private final long lastStartMs;

    /**
     * If this is non-zero, only tasks with an endMs at or after this time will be listed.
     */
    private final long firstEndMs;

    /**
     * If this is non-zero, only tasks with an endMs at or before this time will be listed.
     */
    private final long lastEndMs;

    /**
     * The desired state of the tasks.
     * An empty string will match all states.
     */
    private final Optional<TaskStateType> state;

    @JsonCreator
    public TasksRequest(@JsonProperty("taskIds") Collection<String> taskIds,
            @JsonProperty("firstStartMs") long firstStartMs,
            @JsonProperty("lastStartMs") long lastStartMs,
            @JsonProperty("firstEndMs") long firstEndMs,
            @JsonProperty("lastEndMs") long lastEndMs,
            @JsonProperty("state") Optional<TaskStateType> state) {
        this.taskIds = Collections.unmodifiableSet((taskIds == null) ?
            new HashSet<String>() : new HashSet<>(taskIds));
        this.firstStartMs = Math.max(0, firstStartMs);
        this.lastStartMs = Math.max(0, lastStartMs);
        this.firstEndMs = Math.max(0, firstEndMs);
        this.lastEndMs = Math.max(0, lastEndMs);
        this.state = state == null ? Optional.empty() : state;
    }

    @JsonProperty
    public Collection<String> taskIds() {
        return taskIds;
    }

    @JsonProperty
    public long firstStartMs() {
        return firstStartMs;
    }

    @JsonProperty
    public long lastStartMs() {
        return lastStartMs;
    }

    @JsonProperty
    public long firstEndMs() {
        return firstEndMs;
    }

    @JsonProperty
    public long lastEndMs() {
        return lastEndMs;
    }

    @JsonProperty
    public Optional<TaskStateType> state() {
        return state;
    }

    /**
     * Determine if this TaskRequest should return a particular task.
     *
     * @param taskId    The task ID.
     * @param startMs   The task start time, or -1 if the task hasn't started.
     * @param endMs     The task end time, or -1 if the task hasn't ended.
     * @return          True if information about the task should be returned.
     */
    public boolean matches(String taskId, long startMs, long endMs, TaskStateType state) {
        if ((!taskIds.isEmpty()) && (!taskIds.contains(taskId))) {
            return false;
        }
        if ((firstStartMs > 0) && (startMs < firstStartMs)) {
            return false;
        }
        if ((lastStartMs > 0) && ((startMs < 0) || (startMs > lastStartMs))) {
            return false;
        }
        if ((firstEndMs > 0) && (endMs < firstEndMs)) {
            return false;
        }
        if ((lastEndMs > 0) && ((endMs < 0) || (endMs > lastEndMs))) {
            return false;
        }

        if (this.state.isPresent() && !this.state.get().equals(state)) {
            return false;
        }

        return true;
    }
}
