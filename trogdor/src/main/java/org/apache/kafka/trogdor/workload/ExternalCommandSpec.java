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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * ExternalCommandSpec describes a task that executes Trogdor tasks with the command.
 *
 * An example uses the python runner to execute the ProduceBenchSpec task.
 *
 * #{@code
 *   {
 *      "class": "org.apache.kafka.trogdor.workload.ExternalCommandSpec",
 *      "command": ["python", "/path/to/trogdor/python/runner"],
 *      "durationMs": 10000000,
 *      "producerNode": "node0",
 *      "workload": {
 *        "class": "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
 *        "bootstrapServers": "localhost:9092",
 *        "targetMessagesPerSec": 10,
 *        "maxMessages": 100,
 *        "activeTopics": {
 *          "foo[1-3]": {
 *            "numPartitions": 3,
 *            "replicationFactor": 1
 *          }
 *        },
 *        "inactiveTopics": {
 *          "foo[4-5]": {
 *            "numPartitions": 3,
 *            "replicationFactor": 1
 *          }
 *        }
 *     }
 *   }
 */
public class ExternalCommandSpec extends TaskSpec {
    private final String commandNode;
    private final List<String> command;
    private final JsonNode workload;
    private final Optional<Integer> shutdownGracePeriodMs;

    @JsonCreator
    public ExternalCommandSpec(
            @JsonProperty("startMs") long startMs,
            @JsonProperty("durationMs") long durationMs,
            @JsonProperty("commandNode") String commandNode,
            @JsonProperty("command") List<String> command,
            @JsonProperty("workload") JsonNode workload,
            @JsonProperty("shutdownGracePeriodMs") Optional<Integer> shutdownGracePeriodMs) {
        super(startMs, durationMs);
        this.commandNode = (commandNode == null) ? "" : commandNode;
        this.command = (command == null) ? Collections.unmodifiableList(new ArrayList<String>()) : command;
        this.workload = (workload == null) ? NullNode.instance : workload;
        this.shutdownGracePeriodMs = shutdownGracePeriodMs;
    }

    @JsonProperty
    public String commandNode() {
        return commandNode;
    }

    @JsonProperty
    public List<String> command() {
        return command;
    }

    @JsonProperty
    public JsonNode workload() {
        return workload;
    }

    @JsonProperty
    public Optional<Integer> shutdownGracePeriodMs() {
        return shutdownGracePeriodMs;
    }

    @Override
    public TaskController newController(String id) {
        return topology -> Collections.singleton(commandNode);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ExternalCommandWorker(id, this);
    }
}
