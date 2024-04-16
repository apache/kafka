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

package org.apache.kafka.trogdor.fault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The specification for a fault that creates a network partition.
 */
public class ProcessStopFaultSpec extends TaskSpec {
    private final Set<String> nodeNames;
    private final String javaProcessName;

    @JsonCreator
    public ProcessStopFaultSpec(@JsonProperty("startMs") long startMs,
                        @JsonProperty("durationMs") long durationMs,
                        @JsonProperty("nodeNames") List<String> nodeNames,
                        @JsonProperty("javaProcessName") String javaProcessName) {
        super(startMs, durationMs);
        this.nodeNames = nodeNames == null ? new HashSet<>() : new HashSet<>(nodeNames);
        this.javaProcessName = javaProcessName == null ? "" : javaProcessName;
    }

    @JsonProperty
    public Set<String> nodeNames() {
        return nodeNames;
    }

    @JsonProperty
    public String javaProcessName() {
        return javaProcessName;
    }

    @Override
    public TaskController newController(String id) {
        return new ProcessStopFaultController(nodeNames);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ProcessStopFaultWorker(id, javaProcessName);
    }
}
