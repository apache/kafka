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

import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * The specification for a benchmark that produces messages to a set of topics.
 *
 * To configure a transactional producer, a #{@link TransactionGenerator} must be passed in.
 * Said generator works in lockstep with the producer by instructing it what action to take next in regards to a transaction.
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
public final class ProduceBenchSpec extends TaskSpec {
    private final String producerNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final long maxMessages;
    private final PayloadGenerator keyGenerator;
    private final PayloadGenerator valueGenerator;
    private final Optional<TransactionGenerator> transactionGenerator;
    private final Map<String, String> producerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final TopicsSpec activeTopics;
    private final TopicsSpec inactiveTopics;
    private final boolean useConfiguredPartitioner;
    private final boolean skipFlush;

    @JsonCreator
    public ProduceBenchSpec(@JsonProperty("startMs") long startMs,
                         @JsonProperty("durationMs") long durationMs,
                         @JsonProperty("producerNode") String producerNode,
                         @JsonProperty("bootstrapServers") String bootstrapServers,
                         @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
                         @JsonProperty("maxMessages") long maxMessages,
                         @JsonProperty("keyGenerator") PayloadGenerator keyGenerator,
                         @JsonProperty("valueGenerator") PayloadGenerator valueGenerator,
                         @JsonProperty("transactionGenerator") Optional<TransactionGenerator> txGenerator,
                         @JsonProperty("producerConf") Map<String, String> producerConf,
                         @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
                         @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
                         @JsonProperty("activeTopics") TopicsSpec activeTopics,
                         @JsonProperty("inactiveTopics") TopicsSpec inactiveTopics,
                         @JsonProperty("useConfiguredPartitioner") boolean useConfiguredPartitioner, 
                         @JsonProperty("skipFlush") boolean skipFlush) {
        super(startMs, durationMs);
        this.producerNode = (producerNode == null) ? "" : producerNode;
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.targetMessagesPerSec = targetMessagesPerSec;
        this.maxMessages = maxMessages;
        this.keyGenerator = keyGenerator == null ?
            new SequentialPayloadGenerator(4, 0) : keyGenerator;
        this.valueGenerator = valueGenerator == null ?
            new ConstantPayloadGenerator(512, new byte[0]) : valueGenerator;
        this.transactionGenerator = txGenerator == null ? Optional.empty() : txGenerator;
        this.producerConf = configOrEmptyMap(producerConf);
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.activeTopics = (activeTopics == null) ?
            TopicsSpec.EMPTY : activeTopics.immutableCopy();
        this.inactiveTopics = (inactiveTopics == null) ?
            TopicsSpec.EMPTY : inactiveTopics.immutableCopy();
        this.useConfiguredPartitioner = useConfiguredPartitioner;
        this.skipFlush = skipFlush;
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
    public long maxMessages() {
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
    public Optional<TransactionGenerator> transactionGenerator() {
        return transactionGenerator;
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

    @JsonProperty
    public boolean useConfiguredPartitioner() {
        return useConfiguredPartitioner;
    }

    @JsonProperty
    public boolean skipFlush() {
        return skipFlush;
    }

    @Override
    public TaskController newController(String id) {
        return topology -> Collections.singleton(producerNode);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ProduceBenchWorker(id, this);
    }
}
