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
import org.apache.kafka.trogdor.common.Topology;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * The specification for a benchmark that produces messages to a set of topics.
 */
public class ProduceBenchSpec extends TaskSpec {
    private final String producerNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final int maxMessages;
    private final int messageSize;
    private final Map<String, String> producerConf;
    private final int totalTopics;
    private final int activeTopics;

    @JsonCreator
    public ProduceBenchSpec(@JsonProperty("startMs") long startMs,
                         @JsonProperty("durationMs") long durationMs,
                         @JsonProperty("producerNode") String producerNode,
                         @JsonProperty("bootstrapServers") String bootstrapServers,
                         @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
                         @JsonProperty("maxMessages") int maxMessages,
                         @JsonProperty("messageSize") int messageSize,
                         @JsonProperty("producerConf") Map<String, String> producerConf,
                         @JsonProperty("totalTopics") int totalTopics,
                         @JsonProperty("activeTopics") int activeTopics) {
        super(startMs, durationMs);
        this.producerNode = producerNode;
        this.bootstrapServers = bootstrapServers;
        this.targetMessagesPerSec = targetMessagesPerSec;
        this.maxMessages = maxMessages;
        this.messageSize = (messageSize == 0) ? ProducerPayload.DEFAULT_MESSAGE_SIZE : messageSize;
        this.producerConf = producerConf;
        this.totalTopics = totalTopics;
        this.activeTopics = activeTopics;
    }

    @JsonProperty
    public String producerNode() {
        return producerNode;
    }

    @JsonProperty
    public String bootstrapServers() {
        return bootstrapServers;
    }

    @JsonProperty
    public int targetMessagesPerSec() {
        return targetMessagesPerSec;
    }

    @JsonProperty
    public int maxMessages() {
        return maxMessages;
    }

    @JsonProperty
    public int messageSize() {
        return messageSize;
    }

    @JsonProperty
    public Map<String, String> producerConf() {
        return producerConf;
    }

    @JsonProperty
    public int totalTopics() {
        return totalTopics;
    }

    @JsonProperty
    public int activeTopics() {
        return activeTopics;
    }

    @Override
    public TaskController newController(String id) {
        return new TaskController() {
            @Override
            public Set<String> targetNodes(Topology topology) {
                return Collections.singleton(producerNode);
            }
        };
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ProduceBenchWorker(id, this);
    }
}
