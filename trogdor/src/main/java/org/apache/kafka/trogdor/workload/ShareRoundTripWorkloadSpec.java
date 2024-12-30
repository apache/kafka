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

import org.apache.kafka.trogdor.task.TaskWorker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class ShareRoundTripWorkloadSpec extends RoundTripWorkloadSpec {

    @JsonCreator
    public ShareRoundTripWorkloadSpec(@JsonProperty("startMs") long startMs,
              @JsonProperty("durationMs") long durationMs,
              @JsonProperty("clientNode") String clientNode,
              @JsonProperty("bootstrapServers") String bootstrapServers,
              @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
              @JsonProperty("adminClientConf") Map<String, String> adminClientConf,
              @JsonProperty("consumerConf") Map<String, String> consumerConf,
              @JsonProperty("producerConf") Map<String, String> producerConf,
              @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
              @JsonProperty("valueGenerator") PayloadGenerator valueGenerator,
              @JsonProperty("activeTopics") TopicsSpec activeTopics,
              @JsonProperty("maxMessages") long maxMessages) {
        super(startMs, durationMs, clientNode, bootstrapServers, commonClientConf, adminClientConf, consumerConf, producerConf, targetMessagesPerSec, valueGenerator, activeTopics, maxMessages);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ShareRoundTripWorker(id, this);
    }
}
