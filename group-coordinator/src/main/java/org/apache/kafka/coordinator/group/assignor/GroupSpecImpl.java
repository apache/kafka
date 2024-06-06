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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The assignment specification for a consumer group.
 */
public class GroupSpecImpl implements GroupSpec {
    /**
     * Member subscription metadata keyed by member Id.
     */
    private final Map<String, MemberSubscriptionSpecImpl> memberSubscriptions;

    /**
     * The subscription type of the group.
     */
    private final SubscriptionType subscriptionType;

    /**
     * Reverse lookup map representing topic partitions with
     * their current member assignments.
     */
    private final Map<Uuid, Map<Integer, String>> invertedMemberAssignment;

    public GroupSpecImpl(
        Map<String, MemberSubscriptionSpecImpl> memberSubscriptions,
        SubscriptionType subscriptionType,
        Map<Uuid, Map<Integer, String>> invertedMemberAssignment
    ) {
        this.memberSubscriptions = Objects.requireNonNull(memberSubscriptions);
        this.subscriptionType = Objects.requireNonNull(subscriptionType);
        this.invertedMemberAssignment = Objects.requireNonNull(invertedMemberAssignment);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> memberIds() {
        return memberSubscriptions.keySet();
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
        Map<Integer, String> partitionMap = invertedMemberAssignment.get(topicId);
        if (partitionMap == null) {
            return false;
        }
        return partitionMap.containsKey(partitionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MemberSubscriptionSpec memberSubscription(String memberId) {
        MemberSubscriptionSpec memberSubscription = memberSubscriptions.get(memberId);
        if (memberSubscription == null) {
            throw new IllegalArgumentException("Member Id " + memberId + " not found.");
        }
        return memberSubscription;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Uuid, Set<Integer>> memberAssignment(String memberId) {
        MemberSubscriptionSpecImpl memberSubscription = memberSubscriptions.get(memberId);
        if (memberSubscription == null) {
            return Collections.emptyMap();
        }
        return memberSubscription.memberAssignment();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupSpecImpl that = (GroupSpecImpl) o;
        return subscriptionType == that.subscriptionType &&
            memberSubscriptions.equals(that.memberSubscriptions) &&
            invertedMemberAssignment.equals(that.invertedMemberAssignment);
    }

    @Override
    public int hashCode() {
        int result = memberSubscriptions.hashCode();
        result = 31 * result + subscriptionType.hashCode();
        result = 31 * result + invertedMemberAssignment.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "GroupSpecImpl(memberSubscriptions=" + memberSubscriptions +
            ", subscriptionType=" + subscriptionType +
            ", invertedMemberAssignment=" + invertedMemberAssignment +
            ')';
    }
}
