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
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An immutable assignment for a member.
 */
public class Assignment implements MemberAssignment {
    public static final Assignment EMPTY = new Assignment(Map.of());

    /**
     * The partitions assigned to the member.
     */
    private final Map<Uuid, Set<Integer>> partitions;

    public Assignment(
        Map<Uuid, Set<Integer>> partitions
    ) {
        // Assignments are used as input to assignors, which expect to receive immutable
        // assignment maps, otherwise they will be modified in place.
        this.partitions = Collections.unmodifiableMap(Objects.requireNonNull(partitions));
    }

    /**
     * @return The assigned partitions.
     */
    @Override
    public Map<Uuid, Set<Integer>> partitions() {
        return partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Assignment that = (Assignment) o;
        return partitions.equals(that.partitions);
    }

    @Override
    public int hashCode() {
        return partitions.hashCode();
    }

    @Override
    public String toString() {
        return "Assignment(partitions=" + partitions + ')';
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
            record.topicPartitions().stream().collect(Collectors.toMap(
                ConsumerGroupTargetAssignmentMemberValue.TopicPartition::topicId,
                topicPartitions -> new HashSet<>(topicPartitions.partitions())))
        );
    }

    /**
     * Creates a {{@link Assignment}} from a {{@link ShareGroupTargetAssignmentMemberValue}}.
     *
     * @param record The record.
     * @return A {{@link Assignment}}.
     */
    public static Assignment fromRecord(
        ShareGroupTargetAssignmentMemberValue record
    ) {
        return new Assignment(
            record.topicPartitions().stream().collect(Collectors.toMap(
                ShareGroupTargetAssignmentMemberValue.TopicPartition::topicId,
                topicPartitions -> new HashSet<>(topicPartitions.partitions())))
        );
    }
}
