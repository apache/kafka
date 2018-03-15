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

/**
 * The state for a worker which is being stopped on the agent.
 */
public class WorkerStopping extends WorkerState {
    /**
     * The time on the agent when the task was received.
     */
    private final long startedMs;

    /**
     * The task status.  The format will depend on the type of task that is
     * being run.
     */
    private final String status;

    @JsonCreator
    public WorkerStopping(@JsonProperty("spec") TaskSpec spec,
            @JsonProperty("startedMs") long startedMs,
            @JsonProperty("status") String status) {
        super(spec);
        this.startedMs = startedMs;
        this.status = status == null ? "" : status;
    }

    @JsonProperty
    @Override
    public long startedMs() {
        return startedMs;
    }

    @JsonProperty
    @Override
    public String status() {
        return status;
    }

    @Override
    public boolean stopping() {
        return true;
    }

    @Override
    public boolean running() {
        return true;
    }
}
