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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;

import java.util.Map;
import java.util.Objects;

/**
 * The assignment specification for a consumer group.
 */
public class GroupSpecImpl implements GroupSpec {
    /**
     * The member metadata keyed by member Id.
     */
    private final Map<String, AssignmentMemberSpec> members;

    /**
     * The subscription type followed by the group.
     */
    private final SubscriptionType subscriptionType;

    /**
     * Reverse lookup map representing topic partitions with
     * their current member assignments.
     */
    private final Map<Uuid, Map<Integer, String>> invertedTargetAssignment;

    public GroupSpecImpl(
        Map<String, AssignmentMemberSpec> members,
        SubscriptionType subscriptionType,
        Map<Uuid, Map<Integer, String>> invertedTargetAssignment
    ) {
        Objects.requireNonNull(members);
        Objects.requireNonNull(subscriptionType);
        Objects.requireNonNull(invertedTargetAssignment);
        this.members = members;
        this.subscriptionType = subscriptionType;
        this.invertedTargetAssignment = invertedTargetAssignment;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, AssignmentMemberSpec> members() {
        return members;
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
     */
    @Override
    public boolean isPartitionAssigned(Uuid topicId, int partitionId) {
        Map<Integer, String> partitionMap = invertedTargetAssignment.get(topicId);
        if (partitionMap == null) {
            return false;
        }
        return partitionMap.containsKey(partitionId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupSpecImpl that = (GroupSpecImpl) o;
        return subscriptionType == that.subscriptionType &&
            members.equals(that.members) &&
            invertedTargetAssignment.equals(that.invertedTargetAssignment);
    }

    @Override
    public int hashCode() {
        int result = members.hashCode();
        result = 31 * result + subscriptionType.hashCode();
        result = 31 * result + invertedTargetAssignment.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "GroupSpecImpl(members=" + members +
            ", subscriptionType=" + subscriptionType +
            ", invertedTargetAssignment=" + invertedTargetAssignment +
            ')';
    }
}
