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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;

import java.util.Objects;

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

    public TopicMetadata(
        Uuid id,
        String name,
        int numPartitions
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicMetadata that = (TopicMetadata) o;

        if (!id.equals(that.id)) return false;
        if (!name.equals(that.name)) return false;
        return numPartitions == that.numPartitions;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + numPartitions;
        return result;
    }

    @Override
    public String toString() {
        return "TopicMetadata(" +
            "id=" + id +
            ", name=" + name +
            ", numPartitions=" + numPartitions +
            ')';
    }

    public static TopicMetadata fromRecord(
        ConsumerGroupPartitionMetadataValue.TopicMetadata record
    ) {
        return new TopicMetadata(
            record.topicId(),
            record.topicName(),
            record.numPartitions()
        );
    }

    public static TopicMetadata fromRecord(
        ShareGroupPartitionMetadataValue.TopicMetadata record
    ) {
        return new TopicMetadata(
            record.topicId(),
            record.topicName(),
            record.numPartitions()
        );
    }
}
