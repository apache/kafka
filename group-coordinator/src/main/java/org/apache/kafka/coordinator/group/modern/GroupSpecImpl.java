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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The assignment specification for a modern group.
 */
public class GroupSpecImpl implements GroupSpec {
    /**
     * The metadata image, used to size the assigned partitions of each topic.
     */
    private final CoordinatorMetadataImage metadataImage;

    /**
     * Member subscription metadata keyed by member Id.
     */
    private final Map<String, MemberSubscriptionAndAssignmentImpl> members;

    /**
     * The subscription type of the group.
     */
    private final SubscriptionType subscriptionType;

    /**
     * The partitions which are currently assigned to a member,
     * keyed by topic id.
     */
    private Map<Uuid, BitSet> assignedTopicPartitions;

    /**
     * In case of share groups, this map will be queried to decide
     * which partition is assignable. For non-share groups,
     * this optional should be empty.
     */
    private final Optional<Map<Uuid, Set<Integer>>> topicPartitionAllowedMap;

    public GroupSpecImpl(
        CoordinatorMetadataImage metadataImage,
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscriptionType subscriptionType
    ) {
        this(metadataImage, members, subscriptionType, Optional.empty());
    }

    public GroupSpecImpl(
        CoordinatorMetadataImage metadataImage,
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscriptionType subscriptionType,
        Optional<Map<Uuid, Set<Integer>>> topicPartitionAllowedMap
    ) {
        this.members = Objects.requireNonNull(members);
        this.subscriptionType = Objects.requireNonNull(subscriptionType);
        this.topicPartitionAllowedMap = Objects.requireNonNull(topicPartitionAllowedMap);
        this.metadataImage = Objects.requireNonNull(metadataImage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> memberIds() {
        return members.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SubscriptionType subscriptionType() {
        return subscriptionType;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Partitions which do not exist in the metadata image are reported as unassigned.
     */
    @Override
    public boolean isPartitionAssigned(Uuid topicId, int partitionId) {
        if (assignedTopicPartitions == null) {
            assignedTopicPartitions = computeAssignedTopicPartitions();
        }
        BitSet assignedPartitions = assignedTopicPartitions.get(topicId);
        return assignedPartitions != null && assignedPartitions.get(partitionId);
    }

    private Map<Uuid, BitSet> computeAssignedTopicPartitions() {
        Map<Uuid, BitSet> assignedTopicPartitions = new HashMap<>();
        for (MemberSubscriptionAndAssignmentImpl member : members.values()) {
            for (Map.Entry<Uuid, Set<Integer>> entry : member.partitions().entrySet()) {
                BitSet assignedPartitions = assignedTopicPartitions.computeIfAbsent(
                    entry.getKey(),
                    topicId -> new BitSet(
                        metadataImage.topicMetadata(topicId)
                            .map(CoordinatorMetadataImage.TopicMetadata::partitionCount)
                            .orElse(0)
                    )
                );
                for (int partition : entry.getValue()) {
                    if (partition >= assignedPartitions.size()) {
                        // Drop partitions beyond BitSet.size(), to avoid over-allocating memory in
                        // the BitSet. Note that BitSet.size() rounds up to the nearest multiple of
                        // 64, so non-existent partitions can still fall through.
                        continue;
                    }

                    assignedPartitions.set(partition);
                }
            }
        }

        // The filter for non-existent partitions checks against BitSet.size(), which is rounded up
        // to the nearest multiple of 64, so drop the remaining partitions that do not exist.
        for (Map.Entry<Uuid, BitSet> entry : assignedTopicPartitions.entrySet()) {
            int partitionCount = metadataImage.topicMetadata(entry.getKey())
                .map(CoordinatorMetadataImage.TopicMetadata::partitionCount)
                .orElse(0);
            BitSet assignedPartitions = entry.getValue();
            assignedPartitions.clear(partitionCount, assignedPartitions.size());
        }

        return assignedTopicPartitions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPartitionAssignable(Uuid topicId, int partitionId) {
        return topicPartitionAllowedMap.map(allowedMap -> allowedMap.containsKey(topicId) && allowedMap.get(topicId).contains(partitionId)).orElse(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MemberSubscription memberSubscription(String memberId) {
        MemberSubscription memberSubscription = members.get(memberId);
        if (memberSubscription == null) {
            throw new IllegalArgumentException("Member Id " + memberId + " not found.");
        }
        return memberSubscription;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MemberAssignment memberAssignment(String memberId) {
        MemberSubscriptionAndAssignmentImpl member = members.get(memberId);
        if (member == null) {
            return new MemberAssignmentImpl(Map.of());
        }
        return member;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GroupSpecImpl groupSpec)) return false;
        return Objects.equals(members, groupSpec.members) &&
            subscriptionType == groupSpec.subscriptionType &&
            Objects.equals(topicPartitionAllowedMap, groupSpec.topicPartitionAllowedMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(members, subscriptionType, topicPartitionAllowedMap);
    }

    @Override
    public String toString() {
        return "GroupSpecImpl(" +
            "members=" + members +
            ", subscriptionType=" + subscriptionType +
            ", topicPartitionAllowedMap=" + topicPartitionAllowedMap +
            ')';
    }
}
