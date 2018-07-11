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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.trogdor.rest.Message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Describes some partitions.
 */
public class PartitionsSpec extends Message {
    private final static short DEFAULT_REPLICATION_FACTOR = 3;
    private final static short DEFAULT_NUM_PARTITIONS = 1;

    private final int numPartitions;
    private final short replicationFactor;
    private final Map<Integer, List<Integer>> partitionAssignments;

    @JsonCreator
    public PartitionsSpec(@JsonProperty("numPartitions") int numPartitions,
            @JsonProperty("replicationFactor") short replicationFactor,
            @JsonProperty("partitionAssignments") Map<Integer, List<Integer>> partitionAssignments) {
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        HashMap<Integer, List<Integer>> partMap = new HashMap<>();
        if (partitionAssignments != null) {
            for (Entry<Integer, List<Integer>> entry : partitionAssignments.entrySet()) {
                int partition = entry.getKey() == null ? 0 : entry.getKey();
                ArrayList<Integer> assignments = new ArrayList<>();
                if (entry.getValue() != null) {
                    for (Integer brokerId : entry.getValue()) {
                        assignments.add(brokerId == null ? Integer.valueOf(0) : brokerId);
                    }
                }
                partMap.put(partition, Collections.unmodifiableList(assignments));
            }
        }
        this.partitionAssignments = Collections.unmodifiableMap(partMap);
    }

    @JsonProperty
    public int numPartitions() {
        return numPartitions;
    }

    public List<Integer> partitionNumbers() {
        if (partitionAssignments.isEmpty()) {
            ArrayList<Integer> partitionNumbers = new ArrayList<>();
            int effectiveNumPartitions = numPartitions <= 0 ? DEFAULT_NUM_PARTITIONS : numPartitions;
            for (int i = 0; i < effectiveNumPartitions; i++) {
                partitionNumbers.add(i);
            }
            return partitionNumbers;
        } else {
            return new ArrayList<>(partitionAssignments.keySet());
        }
    }

    @JsonProperty
    public short replicationFactor() {
        return replicationFactor;
    }

    @JsonProperty
    public Map<Integer, List<Integer>> partitionAssignments() {
        return partitionAssignments;
    }

    public NewTopic newTopic(String topicName) {
        if (partitionAssignments.isEmpty()) {
            int effectiveNumPartitions = numPartitions <= 0 ?
                DEFAULT_NUM_PARTITIONS : numPartitions;
            short effectiveReplicationFactor = replicationFactor <= 0 ?
                DEFAULT_REPLICATION_FACTOR : replicationFactor;
            return new NewTopic(topicName, effectiveNumPartitions, effectiveReplicationFactor);
        } else {
            return new NewTopic(topicName, partitionAssignments);
        }
    }
}
