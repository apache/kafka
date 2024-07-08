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
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Implementation of the {@link MemberSubscription} and the {@link MemberAssignment} interfaces.
 */
public class MemberSubscriptionAndAssignmentImpl implements MemberSubscription, MemberAssignment {
    private final Optional<String> rackId;
    private final Optional<String> instanceId;
    private final Set<Uuid> subscribedTopicIds;
    private final Assignment memberAssignment;

    /**
     * Constructs a new {@code MemberSubscriptionAndAssignmentImpl}.
     *
     * @param rackId                The rack Id.
     * @param subscribedTopicIds    The set of subscribed topic Ids.
     * @param memberAssignment      The current member assignment.
     */
    public MemberSubscriptionAndAssignmentImpl(
        Optional<String> rackId,
        Optional<String> instanceId,
        Set<Uuid> subscribedTopicIds,
        Assignment memberAssignment
    ) {
        this.rackId = Objects.requireNonNull(rackId);
        this.instanceId = Objects.requireNonNull(instanceId);
        this.subscribedTopicIds = Objects.requireNonNull(subscribedTopicIds);
        this.memberAssignment = Objects.requireNonNull(memberAssignment);
    }

    @Override
    public Optional<String> rackId() {
        return rackId;
    }

    @Override
    public Optional<String> instanceId() {
        return instanceId;
    }

    @Override
    public Set<Uuid> subscribedTopicIds() {
        return subscribedTopicIds;
    }

    @Override
    public Map<Uuid, Set<Integer>> partitions() {
        return memberAssignment.partitions();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberSubscriptionAndAssignmentImpl that = (MemberSubscriptionAndAssignmentImpl) o;
        return rackId.equals(that.rackId) &&
            instanceId.equals(that.instanceId) &&
            subscribedTopicIds.equals(that.subscribedTopicIds) &&
            memberAssignment.equals(that.memberAssignment);
    }

    @Override
    public int hashCode() {
        int result = rackId.hashCode();
        result = 31 * result + instanceId.hashCode();
        result = 31 * result + subscribedTopicIds.hashCode();
        result = 31 * result + memberAssignment.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MemberSubscriptionAndAssignmentImpl(rackId=" + rackId.orElse("N/A") +
            ", instanceId=" + instanceId +
            ", subscribedTopicIds=" + subscribedTopicIds +
            ", memberAssignment=" + memberAssignment +
            ')';
    }
}
