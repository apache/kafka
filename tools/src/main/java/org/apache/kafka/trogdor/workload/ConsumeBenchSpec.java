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
public class ConsumeBenchSpec extends TaskSpec {

    private final String consumerNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final int maxMessages;
    private final Map<String, String> consumerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final TopicsSpec activeTopics;

    @JsonCreator
    public ConsumeBenchSpec(@JsonProperty("startMs") long startMs,
                            @JsonProperty("durationMs") long durationMs,
                            @JsonProperty("consumerNode") String consumerNode,
                            @JsonProperty("bootstrapServers") String bootstrapServers,
                            @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
                            @JsonProperty("maxMessages") int maxMessages,
                            @JsonProperty("consumerConf") Map<String, String> consumerConf,
                            @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
                            @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
                            @JsonProperty("activeTopics") TopicsSpec activeTopics) {
        super(startMs, durationMs);
        this.consumerNode = (consumerNode == null) ? "" : consumerNode;
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.targetMessagesPerSec = targetMessagesPerSec;
        this.maxMessages = maxMessages;
        this.consumerConf = configOrEmptyMap(consumerConf);
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.activeTopics = activeTopics == null ? TopicsSpec.EMPTY : activeTopics.immutableCopy();
    }

    @JsonProperty
    public String consumerNode() {
        return consumerNode;
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

    @Override
    public TaskController newController(String id) {
        return new TaskController() {
            @Override
            public Set<String> targetNodes(Topology topology) {
                return Collections.singleton(consumerNode);
            }
        };
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ConsumeBenchWorker(id, this);
    }
}
