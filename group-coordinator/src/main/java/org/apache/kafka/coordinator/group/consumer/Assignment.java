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
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An immutable assignment for a member.
 */
public class Assignment {
    public static final Assignment EMPTY = new Assignment(
        (byte) 0,
        Collections.emptyMap(),
        VersionedMetadata.EMPTY
    );

    /**
     * The error assigned to the member.
     */
    private final byte error;

    /**
     * The partitions assigned to the member.
     */
    private final Map<Uuid, Set<Integer>> partitions;

    /**
     * The metadata assigned to the member.
     */
    private final VersionedMetadata metadata;

    public Assignment(
        Map<Uuid, Set<Integer>> partitions
    ) {
        this(
            (byte) 0,
            partitions,
            VersionedMetadata.EMPTY
        );
    }

    public Assignment(
        byte error,
        Map<Uuid, Set<Integer>> partitions,
        VersionedMetadata metadata
    ) {
        this.error = error;
        this.partitions = Collections.unmodifiableMap(Objects.requireNonNull(partitions));
        this.metadata = Objects.requireNonNull(metadata);
    }

    /**
     * @return The error.
     */
    public byte error() {
        return error;
    }

    /**
     * @return The assigned partitions.
     */
    public Map<Uuid, Set<Integer>> partitions() {
        return partitions;
    }

    /**
     * @return The metadata.
     */
    public VersionedMetadata metadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Assignment that = (Assignment) o;

        if (error != that.error) return false;
        if (!partitions.equals(that.partitions)) return false;
        return metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        int result = error;
        result = 31 * result + partitions.hashCode();
        result = 31 * result + metadata.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Assignment(" +
            "error=" + error +
            ", partitions=" + partitions +
            ", metadata=" + metadata +
            ')';
    }

    /**
     * Creates a {{@link Assignment}} from a {{@link ConsumerGroupTargetAssignmentMemberValue}}.
     *
     * @param record The record.
     * @return A {{@link Assignment}}.
     */
    public static Assignment fromRecord(
        ConsumerGroupTargetAssignmentMemberValue record
    ) {
        return new Assignment(
            record.error(),
            record.topicPartitions().stream().collect(Collectors.toMap(
                ConsumerGroupTargetAssignmentMemberValue.TopicPartition::topicId,
                topicPartitions -> new HashSet<>(topicPartitions.partitions()))),
            new VersionedMetadata(
                record.metadataVersion(),
                ByteBuffer.wrap(record.metadataBytes()))
        );
    }
}
