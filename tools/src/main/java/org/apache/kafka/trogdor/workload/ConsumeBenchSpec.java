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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * The specification for a benchmark that consumer messages from a set of topic partitions.
 *
 * If the consumerGroup field is defined, the consumer will subscribe to the
 * topics using the #{@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Collection)} method.
 * If it is not passed, #{@link org.apache.kafka.clients.consumer.KafkaConsumer#assign(Collection)} will be used,
 * which means the Consumer will not use the consumer's group management functionality.
 *
 * An example JSON representation
 * #{@code
 *    {
 *        "class": "org.apache.kafka.trogdor.workload.ConsumeBenchSpec",
 *        "durationMs": 10000000,
 *        "consumerNode": "node0",
 *        "bootstrapServers": "localhost:9092",
 *        "maxMessages": 100,
 *        "consumerGroup": "cg",
 *        "useGroupPartitionAssignment": true,
 *        "activeTopics": {
 *            "foo[1-3]": {
 *                "numPartitions": 3,
 *                "replicationFactor": 1
 *            }
 *        }
 *    }
 * }
 */
public class ConsumeBenchSpec extends TaskSpec {

    public final static String EMPTY_CONSUMER_GROUP = "";

    private final String consumerNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final int maxMessages;
    private final boolean useGroupPartitionAssignment;
    private final Map<String, String> consumerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final TopicsSpec activeTopics;
    private String consumerGroup;

    @JsonCreator
    public ConsumeBenchSpec(@JsonProperty("startMs") long startMs,
                            @JsonProperty("durationMs") long durationMs,
                            @JsonProperty("consumerNode") String consumerNode,
                            @JsonProperty("bootstrapServers") String bootstrapServers,
                            @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
                            @JsonProperty("maxMessages") int maxMessages,
                            @JsonProperty("useGroupPartitionAssignment") Boolean useGroupPartitionAssignment,
                            @JsonProperty("consumerGroup") String consumerGroup,
                            @JsonProperty("consumerConf") Map<String, String> consumerConf,
                            @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
                            @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
                            @JsonProperty("activeTopics") TopicsSpec activeTopics) {
        super(startMs, durationMs);
        this.consumerNode = (consumerNode == null) ? "" : consumerNode;
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.targetMessagesPerSec = targetMessagesPerSec;
        this.maxMessages = maxMessages;
        this.useGroupPartitionAssignment = useGroupPartitionAssignment == null ? true : useGroupPartitionAssignment;
        this.consumerConf = configOrEmptyMap(consumerConf);
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.activeTopics = activeTopics == null ? TopicsSpec.EMPTY : activeTopics.immutableCopy();
        this.consumerGroup = consumerGroup == null ? EMPTY_CONSUMER_GROUP : consumerGroup;
    }

    @JsonProperty
    public String consumerNode() {
        return consumerNode;
    }

    @JsonProperty
    public String consumerGroup() {
        return consumerGroup;
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
    public Map<String, String> consumerConf() {
        return consumerConf;
    }

    @JsonProperty
    public Map<String, String> commonClientConf() {
        return commonClientConf;
    }

    @JsonProperty
    public Map<String, String> adminClientConf() {
        return adminClientConf;
    }

    @JsonProperty
    public TopicsSpec activeTopics() {
        return activeTopics;
    }

    /**
     * Denotes whether to use a dynamic partition assignment from the consumer group or a manual assignment
     */
    @JsonProperty
    public boolean useGroupPartitionAssignment() {
        return useGroupPartitionAssignment;
    }

    @Override
    public TaskController newController(String id) {
        return topology -> Collections.singleton(consumerNode);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ConsumeBenchWorker(id, this);
    }
}
