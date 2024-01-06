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
import java.util.Optional;

/**
 * This is the spec to pass in to be able to run the `ConfigurableProducerWorker` workload.  This allows for customized
 * and even variable configurations in terms of messages per second, message size, batch size, key size, and even the
 * ability to target a specific partition out of a topic.
 *
 * This has several notable differences from the ProduceBench classes, namely the ability to dynamically control
 * flushing and throughput through configurable classes, but also the ability to run against specific partitions within
 * a topic directly.  This workload can only run against one topic at a time, unlike the ProduceBench workload.
 *
 * The parameters that differ from ProduceBenchSpec:
 *
 *     `flushGenerator` -      Used to instruct the KafkaProducer when to issue flushes.  This allows us to simulate
 *                             variable batching since batch flushing is not currently exposed within the KafkaProducer
 *                             class.  See the `FlushGenerator` interface for more information.
 *
 *     `throughputGenerator` - Used to throttle the ConfigurableProducerWorker based on a calculated number of messages
 *                             within a window.  See the `ThroughputGenerator` interface for more information.
 *
 *     `activeTopic`         - This class only supports execution against a single topic at a time.  If more than one
 *                             topic is specified, the ConfigurableProducerWorker will throw an error.
 *
 *     `activePartition`     - Specify a specific partition number within the activeTopic to run load against, or
 *                             specify `-1` to allow use of all partitions.
 *
 * Here is an example spec:
 *
 * {
 *     "startMs": 1606949497662,
 *     "durationMs": 3600000,
 *     "producerNode": "trogdor-agent-0",
 *     "bootstrapServers": "some.example.kafka.server:9091",
 *     "flushGenerator": {
 *         "type": "gaussian",
 *         "messagesPerFlushAverage": 16,
 *         "messagesPerFlushDeviation": 4
 *     },
 *     "throughputGenerator": {
 *         "type": "gaussian",
 *         "messagesPerSecondAverage": 500,
 *         "messagesPerSecondDeviation": 50,
 *         "windowsUntilRateChange": 100,
 *         "windowSizeMs": 100
 *     },
 *     "keyGenerator": {
 *         "type": "constant",
 *         "size": 8
 *     },
 *     "valueGenerator": {
 *         "type": "gaussianTimestampRandom",
 *         "messageSizeAverage": 512,
 *         "messageSizeDeviation": 100,
 *         "messagesUntilSizeChange": 100
 *     },
 *     "producerConf": {
 *         "acks": "all"
 *     },
 *     "commonClientConf": {},
 *     "adminClientConf": {},
 *     "activeTopic": {
 *         "topic0": {
 *             "numPartitions": 100,
 *             "replicationFactor": 3,
 *             "configs": {
 *                 "retention.ms": "1800000"
 *             }
 *         }
 *     },
 *     "activePartition": 5
 * }
 *
 * This example spec performed the following:
 *
 *   * Ran on `trogdor-agent-0` for 1 hour starting at 2020-12-02 22:51:37.662 GMT
 *   * Produced with acks=all to Partition 5 of `topic0` on kafka server `some.example.kafka.server:9091`.
 *   * The average batch had 16 messages, with a standard deviation of 4 messages.
 *   * The messages had 8-byte constant keys with an average size of 512 bytes and a standard deviation of 100 bytes.
 *   * The messages had millisecond timestamps embedded in the first several bytes of the value.
 *   * The average throughput was 500 messages/second, with a window of 100ms and a deviation of 50 messages/second.
 */

public class ConfigurableProducerSpec extends TaskSpec {
    private final String producerNode;
    private final String bootstrapServers;
    private final Optional<FlushGenerator> flushGenerator;
    private final ThroughputGenerator throughputGenerator;
    private final PayloadGenerator keyGenerator;
    private final PayloadGenerator valueGenerator;
    private final Map<String, String> producerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final TopicsSpec activeTopic;
    private final int activePartition;

    @SuppressWarnings("this-escape")
    @JsonCreator
    public ConfigurableProducerSpec(@JsonProperty("startMs") long startMs,
                                    @JsonProperty("durationMs") long durationMs,
                                    @JsonProperty("producerNode") String producerNode,
                                    @JsonProperty("bootstrapServers") String bootstrapServers,
                                    @JsonProperty("flushGenerator") Optional<FlushGenerator> flushGenerator,
                                    @JsonProperty("throughputGenerator") ThroughputGenerator throughputGenerator,
                                    @JsonProperty("keyGenerator") PayloadGenerator keyGenerator,
                                    @JsonProperty("valueGenerator") PayloadGenerator valueGenerator,
                                    @JsonProperty("producerConf") Map<String, String> producerConf,
                                    @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
                                    @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
                                    @JsonProperty("activeTopic") TopicsSpec activeTopic,
                                    @JsonProperty("activePartition") int activePartition) {
        super(startMs, durationMs);
        this.producerNode = (producerNode == null) ? "" : producerNode;
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.flushGenerator = flushGenerator;
        this.keyGenerator = keyGenerator;
        this.valueGenerator = valueGenerator;
        this.throughputGenerator = throughputGenerator;
        this.producerConf = configOrEmptyMap(producerConf);
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.activeTopic = activeTopic.immutableCopy();
        this.activePartition = activePartition;
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
    public Optional<FlushGenerator> flushGenerator() {
        return flushGenerator;
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
    public ThroughputGenerator throughputGenerator() {
        return throughputGenerator;
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
    public TopicsSpec activeTopic() {
        return activeTopic;
    }

    @JsonProperty
    public int activePartition() {
        return activePartition;
    }

    @Override
    public TaskController newController(String id) {
        return topology -> Collections.singleton(producerNode);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ConfigurableProducerWorker(id, this);
    }
}
