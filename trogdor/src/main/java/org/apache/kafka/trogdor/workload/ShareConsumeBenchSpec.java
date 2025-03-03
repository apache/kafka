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

import org.apache.kafka.trogdor.common.StringExpander;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The specification for a benchmark that consumes messages from a set of topic/partitions.
 *
 * If a share group is not given to the specification, the default group name "share" will be used.
 *
 * This specification uses a specific way to represent a topic partition via its "activeTopics" field.
 * The notation for that is topic_name:partition_number (e.g "foo:1" represents partition-1 of topic "foo")
 * Note that a topic name cannot have more than one colon.
 *
 * The "activeTopics" field also supports ranges that get expanded. See #{@link StringExpander}.
 *
 * There now exists a clever and succinct way to represent multiple topics.
 * Example:
 * Given "activeTopics": ["foo[1-3]"], "foo[1-3]" will get
 * expanded to [foo1, foo2, foo3].
 *
 * The consumer will subscribe to the topics via
 * #{@link org.apache.kafka.clients.consumer.KafkaShareConsumer#subscribe(Collection)}.
 * It will be assigned partitions dynamically from the share group by the broker.
 *
 * This specification supports the spawning of multiple share consumers in the single Trogdor worker agent.
 * The "threadsPerWorker" field denotes how many consumers should be spawned for this spec.
 * It is worth noting that the "targetMessagesPerSec", "maxMessages" and "activeTopics" fields apply for every share consumer individually.
 *
 * The "recordProcessor" field allows the specification of tasks to run on records that are consumed.  This is run
 * immediately after the messages are polled.  See the `RecordProcessor` interface for more information.
 *
 * An example JSON representation which will result in a share consumer that is part of the share group "sg" and
 * subscribed to topics foo1, foo2, foo3 and bar.
 * #{@code
 *    {
 *        "class": "org.apache.kafka.trogdor.workload.ShareConsumeBenchSpec",
 *        "durationMs": 10000000,
 *        "consumerNode": "node0",
 *        "bootstrapServers": "localhost:9092",
 *        "maxMessages": 100,
 *        "shareGroup": "sg",
 *        "activeTopics": ["foo[1-3]", "bar"]
 *    }
 * }
 */
public final class ShareConsumeBenchSpec extends TaskSpec {

    private static final String VALID_EXPANDED_TOPIC_NAME_PATTERN = "^[^:]+$";
    private static final String DEFAULT_SHARE_GROUP_NAME = "share";
    private final String consumerNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final long maxMessages;
    private final Map<String, String> consumerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final List<String> activeTopics;
    private final String shareGroup;
    private final int threadsPerWorker;
    private final Optional<RecordProcessor> recordProcessor;

    @JsonCreator
    public ShareConsumeBenchSpec(@JsonProperty("startMs") long startMs,
                            @JsonProperty("durationMs") long durationMs,
                            @JsonProperty("consumerNode") String consumerNode,
                            @JsonProperty("bootstrapServers") String bootstrapServers,
                            @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
                            @JsonProperty("maxMessages") long maxMessages,
                            @JsonProperty("consumerGroup") String shareGroup,
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
        this.shareGroup = shareGroup == null ? DEFAULT_SHARE_GROUP_NAME : shareGroup;
        this.threadsPerWorker = threadsPerWorker == null ? 1 : threadsPerWorker;
        this.recordProcessor = recordProcessor;
    }

    @JsonProperty
    public String consumerNode() {
        return consumerNode;
    }

    @JsonProperty
    public String shareGroup() {
        return shareGroup;
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
        return new ShareConsumeBenchWorker(id, this);
    }

    /**
     * Materializes a list of topic names (optionally with ranges) into a map of the topics and their partitions
     *
     * Example:
     * ['foo[1-3]', 'bar[1-2]'] => {'foo1', 'foo2', 'foo3', 'bar1', 'bar2' }
     */
    Set<String> expandTopicNames() {
        Set<String> expandedTopics = new HashSet<>();

        for (String rawTopicName : this.activeTopics) {
            Set<String> expandedNames = expandTopicName(rawTopicName);
            if (!expandedNames.iterator().next().matches(VALID_EXPANDED_TOPIC_NAME_PATTERN))
                throw new IllegalArgumentException(String.format("Expanded topic name %s is invalid", expandedNames));

            expandedTopics.addAll(expandedNames);
        }
        return expandedTopics;
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
