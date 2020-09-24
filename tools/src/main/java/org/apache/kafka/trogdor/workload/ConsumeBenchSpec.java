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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.trogdor.common.StringExpander;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Optional;

/**
 * The specification for a benchmark that consumer messages from a set of topic/partitions.
 *
 * If a consumer group is not given to the specification, a random one will be generated and
 *  used to track offsets/subscribe to topics.
 *
 * This specification uses a specific way to represent a topic partition via its "activeTopics" field.
 * The notation for that is topic_name:partition_number (e.g "foo:1" represents partition-1 of topic "foo")
 * Note that a topic name cannot have more than one colon.
 *
 * The "activeTopics" field also supports ranges that get expanded. See #{@link StringExpander}.
 *
 * There now exists a clever and succinct way to represent multiple partitions of multiple topics.
 * Example:
 * Given "activeTopics": ["foo[1-3]:[1-3]"], "foo[1-3]:[1-3]" will get
 * expanded to [foo1:1, foo1:2, foo1:3, foo2:1, ..., foo3:3].
 * This represents all partitions 1-3 for the three topics foo1, foo2 and foo3.
 *
 * If there is at least one topic:partition pair, the consumer will be manually assigned partitions via
 * #{@link org.apache.kafka.clients.consumer.KafkaConsumer#assign(Collection)}.
 * Note that in this case the consumer will fetch and assign all partitions for a topic if no partition is given for it (e.g ["foo:1", "bar"])
 *
 * If there are no topic:partition pairs given, the consumer will subscribe to the topics via
 * #{@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Collection)}.
 * It will be assigned partitions dynamically from the consumer group.
 *
 * This specification supports the spawning of multiple consumers in the single Trogdor worker agent.
 * The "threadsPerWorker" field denotes how many consumers should be spawned for this spec.
 * It is worth noting that the "targetMessagesPerSec", "maxMessages" and "activeTopics" fields apply for every consumer individually.
 *
 * If a consumer group is not specified, every consumer is assigned a different, random group. When specified, all consumers use the same group.
 * Since no two consumers in the same group can be assigned the same partition,
 * explicitly specifying partitions in "activeTopics" when there are multiple "threadsPerWorker"
 * and a particular "consumerGroup" will result in an #{@link ConfigException}, aborting the task.
 *
 * The "recordProcessor" field allows the specification of tasks to run on records that are consumed.  This is run
 * immediately after the messages are polled.  See the `RecordProcessor` interface for more information.
 *
 * An example JSON representation which will result in a consumer that is part of the consumer group "cg" and
 * subscribed to topics foo1, foo2, foo3 and bar.
 * #{@code
 *    {
 *        "class": "org.apache.kafka.trogdor.workload.ConsumeBenchSpec",
 *        "durationMs": 10000000,
 *        "consumerNode": "node0",
 *        "bootstrapServers": "localhost:9092",
 *        "maxMessages": 100,
 *        "consumerGroup": "cg",
 *        "activeTopics": ["foo[1-3]", "bar"]
 *    }
 * }
 */
public class ConsumeBenchSpec extends TaskSpec {

    private static final String VALID_EXPANDED_TOPIC_NAME_PATTERN = "^[^:]+(:[\\d]+|[^:]*)$";
    private final String consumerNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final long maxMessages;
    private final Map<String, String> consumerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final List<String> activeTopics;
    private final String consumerGroup;
    private final int threadsPerWorker;
    private final Optional<RecordProcessor> recordProcessor;

    @JsonCreator
    public ConsumeBenchSpec(@JsonProperty("startMs") long startMs,
                            @JsonProperty("durationMs") long durationMs,
                            @JsonProperty("consumerNode") String consumerNode,
                            @JsonProperty("bootstrapServers") String bootstrapServers,
                            @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
                            @JsonProperty("maxMessages") long maxMessages,
                            @JsonProperty("consumerGroup") String consumerGroup,
                            @JsonProperty("consumerConf") Map<String, String> consumerConf,
                            @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
                            @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
                            @JsonProperty("threadsPerWorker") Integer threadsPerWorker,
                            @JsonProperty("recordProcessor") Optional<RecordProcessor> recordProcessor,
                            @JsonProperty("activeTopics") List<String> activeTopics) {
        super(startMs, durationMs);
        this.consumerNode = (consumerNode == null) ? "" : consumerNode;
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.targetMessagesPerSec = targetMessagesPerSec;
        this.maxMessages = maxMessages;
        this.consumerConf = configOrEmptyMap(consumerConf);
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.activeTopics = activeTopics == null ? new ArrayList<>() : activeTopics;
        this.consumerGroup = consumerGroup == null ? "" : consumerGroup;
        this.threadsPerWorker = threadsPerWorker == null ? 1 : threadsPerWorker;
        this.recordProcessor = recordProcessor;
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
    public long maxMessages() {
        return maxMessages;
    }

    @JsonProperty
    public int threadsPerWorker() {
        return threadsPerWorker;
    }

    @JsonProperty
    public Optional<RecordProcessor> recordProcessor() {
        return this.recordProcessor;
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
    public List<String> activeTopics() {
        return activeTopics;
    }

    @Override
    public TaskController newController(String id) {
        return topology -> Collections.singleton(consumerNode);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ConsumeBenchWorker(id, this);
    }

    /**
     * Materializes a list of topic names (optionally with ranges) into a map of the topics and their partitions
     *
     * Example:
     * ['foo[1-3]', 'foobar:2', 'bar[1-2]:[1-2]'] => {'foo1': [], 'foo2': [], 'foo3': [], 'foobar': [2],
     *                                                'bar1': [1, 2], 'bar2': [1, 2] }
     */
    Map<String, List<TopicPartition>> materializeTopics() {
        Map<String, List<TopicPartition>> partitionsByTopics = new HashMap<>();

        for (String rawTopicName : this.activeTopics) {
            Set<String> expandedNames = expandTopicName(rawTopicName);
            if (!expandedNames.iterator().next().matches(VALID_EXPANDED_TOPIC_NAME_PATTERN))
                throw new IllegalArgumentException(String.format("Expanded topic name %s is invalid", rawTopicName));

            for (String topicName : expandedNames) {
                TopicPartition partition = null;
                if (topicName.contains(":")) {
                    String[] topicAndPartition = topicName.split(":");
                    topicName = topicAndPartition[0];
                    partition = new TopicPartition(topicName, Integer.parseInt(topicAndPartition[1]));
                }
                if (!partitionsByTopics.containsKey(topicName)) {
                    partitionsByTopics.put(topicName, new ArrayList<>());
                }
                if (partition != null) {
                    partitionsByTopics.get(topicName).add(partition);
                }
            }
        }

        return partitionsByTopics;
    }

    /**
     * Expands a topic name until there are no more ranges in it
     */
    private Set<String> expandTopicName(String topicName) {
        Set<String> expandedNames = StringExpander.expand(topicName);
        if (expandedNames.size() == 1) {
            return expandedNames;
        }

        Set<String> newNames = new HashSet<>();
        for (String name : expandedNames) {
            newNames.addAll(expandTopicName(name));
        }
        return newNames;
    }
}
