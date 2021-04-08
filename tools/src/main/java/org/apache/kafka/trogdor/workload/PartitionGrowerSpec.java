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
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.Collections;
import java.util.Map;

/**
 * The specification for a benchmark that incrementally grows a topic's partition count.
 *
 * An example JSON representation which will ensure we start at 9 partitions and grow the topic by 9 partitions every
 * 10 minutes for an hour, starting immediately.
 *
 * #{@code
 *   {
 *    "class": "org.apache.kafka.trogdor.workload.TopicGrowerSpec",
 *    "durationMs": 3600000,
 *    "clientNode": "node0",
 *    "bootstrapServers": "localhost:9092",
 *    "growPartitions": 9,
 *    "growthIntervalMs": 600000,
 *    "topicName": "test-topic1"
 *   }
 *  }
 */

public class PartitionGrowerSpec extends TaskSpec {
    private final String clientNode;
    private final String bootstrapServers;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final int growPartitions;
    private final long growthIntervalMs;
    private final String topicName;

    @JsonCreator
    public PartitionGrowerSpec(
            @JsonProperty("startMs") long startMs,
            @JsonProperty("durationMs") long durationMs,
            @JsonProperty("clientNode") String clientNode,
            @JsonProperty("bootstrapServers") String bootstrapServers,
            @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
            @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
            @JsonProperty("growPartitions") int growPartitions,
            @JsonProperty("growthIntervalMs") long growthIntervalMs,
            @JsonProperty("topicName") String topicName) {
        super(startMs, durationMs);
        this.clientNode = clientNode == null ? "" : clientNode;
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.growPartitions = growPartitions;
        this.growthIntervalMs = growthIntervalMs;
        this.topicName = topicName;
    }

    @JsonProperty
    public String clientNode() {
        return clientNode;
    }

    @JsonProperty
    public String bootstrapServers() {
        return bootstrapServers;
    }

    @JsonProperty
    public Map<String, String> adminClientConf() {
        return adminClientConf;
    }

    @JsonProperty
    public Map<String, String> commonClientConf() {
        return commonClientConf;
    }

    @JsonProperty
    public int growPartitions() {
        return growPartitions;
    }

    @JsonProperty
    public long growthIntervalMs() {
        return growthIntervalMs;
    }

    @JsonProperty
    public String topicName() {
        return topicName;
    }

    public TaskController newController(String id) {
        return topology -> Collections.singleton(clientNode);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new PartitionGrowerWorker(id, this);
    }
}
