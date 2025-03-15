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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.generated.StreamsGroupPartitionMetadataValue;

import java.util.Objects;

/**
 * Immutable topic metadata, representing the current state of a topic in the broker.
 *
 * @param id             The topic ID.
 * @param name           The topic name.
 * @param numPartitions  The number of partitions.
 */
public record TopicMetadata(Uuid id, String name, int numPartitions) {

    public TopicMetadata(Uuid id,
                         String name,
                         int numPartitions) {
        this.id = Objects.requireNonNull(id);
        if (Uuid.ZERO_UUID.equals(id)) {
            throw new IllegalArgumentException("Topic id cannot be ZERO_UUID.");
        }
        this.name = Objects.requireNonNull(name);
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be empty.");
        }
        this.numPartitions = numPartitions;
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be positive.");
        }
    }

    public static TopicMetadata fromRecord(StreamsGroupPartitionMetadataValue.TopicMetadata record) {
        return new TopicMetadata(
            record.topicId(),
            record.topicName(),
            record.numPartitions());
    }
}
