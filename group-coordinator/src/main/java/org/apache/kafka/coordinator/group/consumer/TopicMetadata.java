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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable topic metadata.
 */
public class TopicMetadata {
    /**
     * The topic id.
     */
    private final Uuid id;

    /**
     * The topic name.
     */
    private final String name;

    /**
     * The number of partitions.
     */
    private final int numPartitions;

    /**
     * Map of every partition Id to a set of its rack Ids, if they exist.
     * If rack information is unavailable for all partitions, this is an empty map.
     */
    private final Map<Integer, Set<String>> partitionRacks;

    public TopicMetadata(
        Uuid id,
        String name,
        int numPartitions,
        Map<Integer, Set<String>> partitionRacks
    ) {
        this.id = Objects.requireNonNull(id);
        if (Uuid.ZERO_UUID.equals(id)) {
            throw new IllegalArgumentException("Topic id cannot be ZERO_UUID.");
        }
        this.name = Objects.requireNonNull(name);
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be empty.");
        }
        this.numPartitions = numPartitions;
        if (numPartitions < 0) {
            throw new IllegalArgumentException("Number of partitions cannot be negative.");
        }
        this.partitionRacks = Objects.requireNonNull(partitionRacks);
    }

    /**
     * @return The topic id.
     */
    public Uuid id() {
        return this.id;
    }

    /**
     * @return The topic name.
     */
    public String name() {
        return this.name;
    }

    /**
     * @return The number of partitions.
     */
    public int numPartitions() {
        return this.numPartitions;
    }

    /**
     * @return Every partition mapped to the set of corresponding available rack Ids of its replicas.
     *         An empty map is returned if rack information is unavailable for all partitions.
     */
    public Map<Integer, Set<String>> partitionRacks() {
        return this.partitionRacks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicMetadata that = (TopicMetadata) o;

        if (!id.equals(that.id)) return false;
        if (!name.equals(that.name)) return false;
        if (numPartitions != that.numPartitions) return false;
        return partitionRacks.equals(that.partitionRacks);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + numPartitions;
        result = 31 * result + partitionRacks.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TopicMetadata(" +
            "id=" + id +
            ", name=" + name +
            ", numPartitions=" + numPartitions +
            ", partitionRacks=" + partitionRacks +
            ')';
    }

    public static TopicMetadata fromRecord(
        ConsumerGroupPartitionMetadataValue.TopicMetadata record
    ) {
        // Converting the data type from a list stored in the record to a map for the topic metadata.
        Map<Integer, Set<String>> partitionRacks = new HashMap<>();
        for (ConsumerGroupPartitionMetadataValue.PartitionMetadata partitionMetadata : record.partitionMetadata()) {
            partitionRacks.put(
                partitionMetadata.partition(),
                Collections.unmodifiableSet(new HashSet<>(partitionMetadata.racks()))
            );
        }

        return new TopicMetadata(
            record.topicId(),
            record.topicName(),
            record.numPartitions(),
            partitionRacks);
    }
}
