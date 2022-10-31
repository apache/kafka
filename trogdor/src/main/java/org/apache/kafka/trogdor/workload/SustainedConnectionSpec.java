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

/**
 * The specification for a benchmark that creates sustained connections.
 *
 * An example JSON representation which will result in a test that creates 27 connections (9 of each), refreshes them
 * every 10 seconds using 2 threads, running against topic `topic1`, for a duration of 1 hour, and with various other
 * options set:
 *
 * #{@code
 *   {
 *    "class": "org.apache.kafka.trogdor.workload.SustainedConnectionSpec",
 *    "durationMs": 3600000,
 *    "clientNode": "node0",
 *    "bootstrapServers": "localhost:9092",
 *    "commonClientConf": {
 *      "compression.type": "lz4",
 *      "auto.offset.reset": "earliest",
 *      "linger.ms": "100"
 *    },
 *    "keyGenerator": {
 *      "type": "sequential",
 *      "size": 4,
 *      "startOffset": 0
 *    },
 *    "valueGenerator": {
 *      "type": "uniformRandom",
 *      "size": 512,
 *      "seed": 0,
 *      "padding": 0
 *    },
 *    "producerConnectionCount": 9,
 *    "consumerConnectionCount": 9,
 *    "metadataConnectionCount": 9,
 *    "topicName": "test-topic1-1",
 *    "numThreads": 2,
 *    "refreshRateMs": 10000
 *   }
 *  }
 */
public class SustainedConnectionSpec extends TaskSpec {
    private final String clientNode;
    private final String bootstrapServers;
    private final Map<String, String> producerConf;
    private final Map<String, String> consumerConf;
    private final Map<String, String> adminClientConf;
    private final Map<String, String> commonClientConf;
    private final PayloadGenerator keyGenerator;
    private final PayloadGenerator valueGenerator;
    private final int producerConnectionCount;
    private final int consumerConnectionCount;
    private final int metadataConnectionCount;
    private final String topicName;
    private final int numThreads;
    private final int refreshRateMs;

    @JsonCreator
    public SustainedConnectionSpec(
            @JsonProperty("startMs") long startMs,
            @JsonProperty("durationMs") long durationMs,
            @JsonProperty("clientNode") String clientNode,
            @JsonProperty("bootstrapServers") String bootstrapServers,
            @JsonProperty("producerConf") Map<String, String> producerConf,
            @JsonProperty("consumerConf") Map<String, String> consumerConf,
            @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
            @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
            @JsonProperty("keyGenerator") PayloadGenerator keyGenerator,
            @JsonProperty("valueGenerator") PayloadGenerator valueGenerator,
            @JsonProperty("producerConnectionCount") int producerConnectionCount,
            @JsonProperty("consumerConnectionCount") int consumerConnectionCount,
            @JsonProperty("metadataConnectionCount") int metadataConnectionCount,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("numThreads") int numThreads,
            @JsonProperty("refreshRateMs") int refreshRateMs) {
        super(startMs, durationMs);
        this.clientNode = clientNode == null ? "" : clientNode;
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.producerConf = configOrEmptyMap(producerConf);
        this.consumerConf = configOrEmptyMap(consumerConf);
        this.adminClientConf = configOrEmptyMap(adminClientConf);
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.keyGenerator = keyGenerator;
        this.valueGenerator = valueGenerator;
        this.producerConnectionCount = producerConnectionCount;
        this.consumerConnectionCount = consumerConnectionCount;
        this.metadataConnectionCount = metadataConnectionCount;
        this.topicName = topicName;
        this.numThreads = numThreads < 1 ? 1 : numThreads;
        this.refreshRateMs = refreshRateMs < 1 ? 1 : refreshRateMs;
    }

    @JsonProperty
    public String clientNode() {
        return clientNode;
    }

    @JsonProperty
    public String bootstrapServers() {
        return bootstrapServers;
    }

    @JsonProperty
    public Map<String, String> producerConf() {
        return producerConf;
    }

    @JsonProperty
    public Map<String, String> consumerConf() {
        return consumerConf;
    }

    @JsonProperty
    public Map<String, String> adminClientConf() {
        return adminClientConf;
    }

    @JsonProperty
    public Map<String, String> commonClientConf() {
        return commonClientConf;
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
    public int producerConnectionCount() {
        return producerConnectionCount;
    }

    @JsonProperty
    public int consumerConnectionCount() {
        return consumerConnectionCount;
    }

    @JsonProperty
    public int metadataConnectionCount() {
        return metadataConnectionCount;
    }

    @JsonProperty
    public String topicName() {
        return topicName;
    }

    @JsonProperty
    public int numThreads() {
        return numThreads;
    }

    @JsonProperty
    public int refreshRateMs() {
        return refreshRateMs;
    }

    public TaskController newController(String id) {
        return topology -> Collections.singleton(clientNode);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new SustainedConnectionWorker(id, this);
    }
}
