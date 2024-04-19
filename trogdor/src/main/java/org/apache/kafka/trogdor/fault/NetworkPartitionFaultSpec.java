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

package org.apache.kafka.trogdor.fault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The specification for a fault that creates a network partition.
 */
public class NetworkPartitionFaultSpec extends TaskSpec {
    private final List<List<String>> partitions;

    @JsonCreator
    public NetworkPartitionFaultSpec(@JsonProperty("startMs") long startMs,
                         @JsonProperty("durationMs") long durationMs,
                         @JsonProperty("partitions") List<List<String>> partitions) {
        super(startMs, durationMs);
        this.partitions = partitions == null ? new ArrayList<>() : partitions;
    }

    @JsonProperty
    public List<List<String>> partitions() {
        return partitions;
    }

    @Override
    public TaskController newController(String id) {
        return new NetworkPartitionFaultController(partitionSets());
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new NetworkPartitionFaultWorker(id, partitionSets());
    }

    private List<Set<String>> partitionSets() {
        List<Set<String>> partitionSets = new ArrayList<>();
        HashSet<String> prevNodes = new HashSet<>();
        for (List<String> partition : this.partitions()) {
            for (String nodeName : partition) {
                if (prevNodes.contains(nodeName)) {
                    throw new RuntimeException("Node " + nodeName +
                        " appears in more than one partition.");
                }
                prevNodes.add(nodeName);
                partitionSets.add(new HashSet<>(partition));
            }
        }
        return partitionSets;
    }
}
