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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SampleTaskSpec extends TaskSpec {
    private final Map<String, Long> nodeToExitMs;
    private final String error;

    @JsonCreator
    public SampleTaskSpec(@JsonProperty("startMs") long startMs,
                        @JsonProperty("durationMs") long durationMs,
                        @JsonProperty("nodeToExitMs") Map<String, Long> nodeToExitMs,
                        @JsonProperty("error") String error) {
        super(startMs, durationMs);
        this.nodeToExitMs = nodeToExitMs == null ? new HashMap<String, Long>() :
            Collections.unmodifiableMap(nodeToExitMs);
        this.error = error == null ? "" : error;
    }

    @JsonProperty
    public Map<String, Long> nodeToExitMs() {
        return nodeToExitMs;
    }

    @JsonProperty
    public String error() {
        return error;
    }

    @Override
    public TaskController newController(String id) {
        return new SampleTaskController();
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new SampleTaskWorker(this);
    }
};
