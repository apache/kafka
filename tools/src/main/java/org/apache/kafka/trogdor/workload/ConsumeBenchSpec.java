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
import org.apache.kafka.trogdor.common.StringExpander;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * The specification for a benchmark that consumer messages from a set of topic/partitions.
 *
 * If a consumer group is not given to the specification, a random one will be generated and
 *  used to track offsets/subscribe to topics.
 *
 * This specification uses a specific way to represent a number of topic/partitions via its "activeTopics" field.
 * There are three value notations accepted in the "activeTopics" field:
 *   1. foo[1-3] - this will be expanded to 3 topics (foo1, foo2, foo3)
 *   2. foo[1-3][1-2] - this will expand to 3 topics (foo1, foo2, foo3), each with two partitions (1, 2)
 *   3. foo - this will denote one topic (foo)
 *
 * If there is at least one value of notation #2, the consumer will be manually assign partitions to track via
 * #{@link org.apache.kafka.clients.consumer.KafkaConsumer#assign(Collection)}.
 * Note that in this case the consumer will fetch all partitions for a topic if it is not represented using notation #2
 *
 * If there are no values of notation #2, the consumer will subscribe to the given topics via
 * #{@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Collection)}.
 * It will be assigned partitions dynamically from the consumer group.
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

    public final static String EMPTY_CONSUMER_GROUP = "";

    private final String consumerNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final int maxMessages;
    private final Map<String, String> consumerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final List<String> activeTopics;
    private final Map<String, List<TopicPartition>> materializedTopics;
    private final boolean useGroupPartitionAssignment;
    private String consumerGroup;

    @JsonCreator
    public ConsumeBenchSpec(@JsonProperty("startMs") long startMs,
                            @JsonProperty("durationMs") long durationMs,
                            @JsonProperty("consumerNode") String consumerNode,
                            @JsonProperty("bootstrapServers") String bootstrapServers,
                            @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
                            @JsonProperty("maxMessages") int maxMessages,
                            @JsonProperty("consumerGroup") String consumerGroup,
                            @JsonProperty("consumerConf") Map<String, String> consumerConf,
                            @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
                            @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
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
        this.materializedTopics = new HashMap<>();
        this.consumerGroup = consumerGroup == null ? EMPTY_CONSUMER_GROUP : consumerGroup;
        // use consumer group assignment unless we're given specific partitions
        this.useGroupPartitionAssignment = materializeTopics(this.activeTopics);
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
    public List<String> activeTopics() {
        return activeTopics;
    }

    public Map<String, List<TopicPartition>> materializedTopics() {
        return materializedTopics;
    }

    /**
     * Denotes whether to use a dynamic partition assignment from the consumer group or a manual assignment
     */
    boolean useGroupPartitionAssignment() {
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

    /**
     * Materializes a list of topic names with ranges into partitions
     * ['foo[1-3]', 'foobar', 'bar[1-2][1-2]'] => {'foo1': [], 'foo2': [], 'foo3': [], 'foobar': [],
     *                                             'bar1': [1, 2], 'bar2': [1, 2] }
     *
     * @param topics - a list of topic names, potentially with ranges in them that would be expanded
     * @return boolean - whether to use a dynamic partition assignment from the consumer group or not
     */
    private boolean materializeTopics(List<String> topics) {
        boolean useGroupPartitionAssignment = true;
        for (String rawTopicName : topics) {
            if (!StringExpander.canExpand(rawTopicName)) {
                materializedTopics.put(rawTopicName, new ArrayList<>());
                continue;
            }

            Map<String, List<Integer>> expandedResult = StringExpander.expandIntoMap(rawTopicName);
            if (expandedResult.values().isEmpty()) {
                for (String parsedTopicName : expandedResult.keySet())
                    materializedTopics.put(parsedTopicName, new ArrayList<>());
            } else {
                // double range given in topic name (e.g foo[1-10][1-3])
                for (Map.Entry<String, List<Integer>> expandedEntry : expandedResult.entrySet()) {
                    String topicName = expandedEntry.getKey();
                    List<TopicPartition> partitions = expandedEntry.getValue().stream()
                        .map(i -> new TopicPartition(topicName, i)).collect(Collectors.toList());

                    materializedTopics.put(topicName, partitions);
                }
                useGroupPartitionAssignment = false; // use manual assignment
            }
        }

        return useGroupPartitionAssignment;
    }
}
