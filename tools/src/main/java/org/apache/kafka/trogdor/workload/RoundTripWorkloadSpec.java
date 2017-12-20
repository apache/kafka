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
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

/**
 * The specification for a workload that sends messages to a broker and then
 * reads them back.
 */
public class RoundTripWorkloadSpec extends TaskSpec {
    private final String clientNode;
    private final String bootstrapServers;
    private final int targetMessagesPerSec;
    private final NavigableMap<Integer, List<Integer>> partitionAssignments;
    private final int maxMessages;

    @JsonCreator
    public RoundTripWorkloadSpec(@JsonProperty("startMs") long startMs,
             @JsonProperty("durationMs") long durationMs,
             @JsonProperty("clientNode") String clientNode,
             @JsonProperty("bootstrapServers") String bootstrapServers,
             @JsonProperty("targetMessagesPerSec") int targetMessagesPerSec,
             @JsonProperty("partitionAssignments") NavigableMap<Integer, List<Integer>> partitionAssignments,
             @JsonProperty("maxMessages") int maxMessages) {
        super(startMs, durationMs);
        this.clientNode = clientNode;
        this.bootstrapServers = bootstrapServers;
        this.targetMessagesPerSec = targetMessagesPerSec;
        this.partitionAssignments = partitionAssignments;
        this.maxMessages = maxMessages;
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
    public int targetMessagesPerSec() {
        return targetMessagesPerSec;
    }

    @JsonProperty
    public NavigableMap<Integer, List<Integer>> partitionAssignments() {
        return partitionAssignments;
    }

    @JsonProperty
    public int maxMessages() {
        return maxMessages;
    }

    @Override
    public TaskController newController(String id) {
        return new TaskController() {
            @Override
            public Set<String> targetNodes(Topology topology) {
                return Collections.singleton(clientNode);
            }
        };
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new RoundTripWorker(id, this);
    }
}
