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

    private static final String DEFAULT_TOPIC_PREFIX = "produceBenchTopic";
    private static final int DEFAULT_NUM_PARTITIONS = 1;
    private static final short DEFAULT_REPLICATION_FACTOR = 3;

    private final String producerNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final int maxMessages;
    private final PayloadGenerator keyGenerator;
    private final PayloadGenerator valueGenerator;
    private final Map<String, String> producerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final int totalTopics;
    private final int activeTopics;
    private final String topicPrefix;
    private final int numPartitions;
    private final short replicationFactor;

    @JsonCreator
    public ProduceBenchSpec(@JsonProperty("startMs") long startMs,
                         @JsonProperty("durationMs") long durationMs,
                         @JsonProperty("producerNode") String producerNode,
                         @JsonProperty("bootstrapServers") String bootstrapServers,
                         @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
                         @JsonProperty("maxMessages") int maxMessages,
                         @JsonProperty("keyGenerator") PayloadGenerator keyGenerator,
                         @JsonProperty("valueGenerator") PayloadGenerator valueGenerator,
                         @JsonProperty("producerConf") Map<String, String> producerConf,
                         @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
                         @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
                         @JsonProperty("totalTopics") int totalTopics,
                         @JsonProperty("activeTopics") int activeTopics,
                         @JsonProperty("topicPrefix") String topicPrefix,
                         @JsonProperty("partitionsPerTopic") int partitionsPerTopic,
                         @JsonProperty("replicationFactor") short replicationFactor) {
        super(startMs, durationMs);
        this.producerNode = (producerNode == null) ? "" : producerNode;
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.targetMessagesPerSec = targetMessagesPerSec;
        this.maxMessages = maxMessages;
        this.keyGenerator = keyGenerator == null ?
            new SequentialPayloadGenerator(4, 0) : keyGenerator;
        this.valueGenerator = valueGenerator == null ?
            new ConstantPayloadGenerator(512, new byte[0]) : valueGenerator;
        this.producerConf = configOrEmptyMap(producerConf);
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.totalTopics = totalTopics;
        this.activeTopics = activeTopics;
        this.topicPrefix = (topicPrefix == null) ? DEFAULT_TOPIC_PREFIX : topicPrefix;
        this.numPartitions = (partitionsPerTopic == 0)
                             ? DEFAULT_NUM_PARTITIONS : partitionsPerTopic;
        this.replicationFactor = (replicationFactor == 0)
                                 ? DEFAULT_REPLICATION_FACTOR : replicationFactor;
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
    public PayloadGenerator keyGenerator() {
        return keyGenerator;
    }

    @JsonProperty
    public PayloadGenerator valueGenerator() {
        return valueGenerator;
    }

    @JsonProperty
    public Map<String, String> producerConf() {
        return producerConf;
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
    public int totalTopics() {
        return totalTopics;
    }

    @JsonProperty
    public int activeTopics() {
        return activeTopics;
    }

    @JsonProperty
    public String topicPrefix() {
        return topicPrefix;
    }

    @JsonProperty
    public int numPartitions() {
        return numPartitions;
    }

    @JsonProperty
    public short replicationFactor() {
        return replicationFactor;
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
