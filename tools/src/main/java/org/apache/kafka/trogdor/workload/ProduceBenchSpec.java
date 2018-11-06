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
 *
 * If the "messagesPerTransaction" field is present, the producer will group and produce messages as separate transactions using the producer transactional API.
 *
 * An example JSON representation which will result in a producer that creates three topics (foo1, foo2, foo3)
 * with three partitions each and produces to them:
 * #{@code
 *   {
 *      "class": "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
 *      "durationMs": 10000000,
 *      "producerNode": "node0",
 *      "bootstrapServers": "localhost:9092",
 *      "targetMessagesPerSec": 10,
 *      "maxMessages": 100,
 *      "activeTopics": {
 *        "foo[1-3]": {
 *          "numPartitions": 3,
 *          "replicationFactor": 1
 *        }
 *      },
 *      "inactiveTopics": {
 *        "foo[4-5]": {
 *          "numPartitions": 3,
 *          "replicationFactor": 1
 *        }
 *      }
 *   }
 * }
 */
public class ProduceBenchSpec extends TaskSpec {
    private final String producerNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final int messagesPerTransaction;
    private final int maxMessages;
    private final boolean useTransactions;
    private final PayloadGenerator keyGenerator;
    private final PayloadGenerator valueGenerator;
    private final Map<String, String> producerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final TopicsSpec activeTopics;
    private final TopicsSpec inactiveTopics;

    @JsonCreator
    public ProduceBenchSpec(@JsonProperty("startMs") long startMs,
                         @JsonProperty("durationMs") long durationMs,
                         @JsonProperty("producerNode") String producerNode,
                         @JsonProperty("bootstrapServers") String bootstrapServers,
                         @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
                         @JsonProperty("messagesPerTransaction") int messagesPerTransaction,
                         @JsonProperty("maxMessages") int maxMessages,
                         @JsonProperty("keyGenerator") PayloadGenerator keyGenerator,
                         @JsonProperty("valueGenerator") PayloadGenerator valueGenerator,
                         @JsonProperty("producerConf") Map<String, String> producerConf,
                         @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
                         @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
                         @JsonProperty("activeTopics") TopicsSpec activeTopics,
                         @JsonProperty("inactiveTopics") TopicsSpec inactiveTopics) {
        super(startMs, durationMs);
        this.producerNode = (producerNode == null) ? "" : producerNode;
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.targetMessagesPerSec = targetMessagesPerSec;
        this.useTransactions = messagesPerTransaction != 0;
        this.messagesPerTransaction = messagesPerTransaction;
        this.maxMessages = maxMessages;
        this.keyGenerator = keyGenerator == null ?
            new SequentialPayloadGenerator(4, 0) : keyGenerator;
        this.valueGenerator = valueGenerator == null ?
            new ConstantPayloadGenerator(512, new byte[0]) : valueGenerator;
        this.producerConf = configOrEmptyMap(producerConf);
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.activeTopics = (activeTopics == null) ?
            TopicsSpec.EMPTY : activeTopics.immutableCopy();
        this.inactiveTopics = (inactiveTopics == null) ?
            TopicsSpec.EMPTY : inactiveTopics.immutableCopy();
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
    public int messagesPerTransaction() {
        return messagesPerTransaction;
    }

    public boolean useTransactions() {
        return useTransactions;
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
    public TopicsSpec activeTopics() {
        return activeTopics;
    }

    @JsonProperty
    public TopicsSpec inactiveTopics() {
        return inactiveTopics;
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
