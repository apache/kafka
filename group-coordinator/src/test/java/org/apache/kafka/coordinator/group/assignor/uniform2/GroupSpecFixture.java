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
package org.apache.kafka.coordinator.group.assignor.uniform2;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds the {@link GroupSpec} for tests, preserving the order of members as they are added. The
 * subscription type is homogeneous when every member has the same subscription, as the coordinator
 * declares it. A partition is held by one member at most, as the coordinator guarantees. The spec
 * can be built only once.
 */
public final class GroupSpecFixture {
    private final Map<String, MemberSubscriptionAndAssignmentImpl> members = new LinkedHashMap<>();
    private final Map<Uuid, Map<Integer, String>> invertedTargetAssignment = new HashMap<>();
    private Set<Uuid> firstSubscription;
    private boolean sameSubscriptions = true;
    private boolean built;

    /**
     * @param spec       The group spec.
     * @param assignment An assignment of the group.
     * @return A fixture with the members of the spec, in its order, each holding its partitions of
     *         the assignment. More members can be added to it.
     */
    public static GroupSpecFixture after(GroupSpec spec, GroupAssignment assignment) {
        var fixture = new GroupSpecFixture();
        for (String memberId : spec.memberIds()) {
            fixture.withMember(
                memberId,
                spec.memberSubscription(memberId).subscribedTopicIds(),
                assignment.members().get(memberId).partitions()
            );
        }
        return fixture;
    }

    /**
     * Adds a member holding no partitions.
     *
     * @param memberId The member id.
     * @param topics   The topics it subscribes to.
     * @return This fixture.
     */
    public GroupSpecFixture withMember(String memberId, Set<Uuid> topics) {
        return withMember(memberId, topics, Map.of());
    }

    /**
     * Adds a member.
     *
     * @param memberId   The member id.
     * @param topics     The topics it subscribes to.
     * @param partitions The partitions it holds, per topic.
     * @return This fixture.
     * @throws IllegalArgumentException If the member is already added, or a partition it holds is
     *                                  held by another member.
     */
    public GroupSpecFixture withMember(String memberId, Set<Uuid> topics, Map<Uuid, Set<Integer>> partitions) {
        checkNotBuilt();
        if (members.containsKey(memberId)) {
            throw new IllegalArgumentException("Member " + memberId + " is already added");
        }
        members.put(memberId, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            topics,
            new Assignment(partitions)
        ));

        if (firstSubscription == null) {
            firstSubscription = topics;
        } else {
            sameSubscriptions &= firstSubscription.equals(topics);
        }
        partitions.forEach((topicId, topicPartitions) -> {
            var owners = invertedTargetAssignment.computeIfAbsent(topicId, k -> new HashMap<>());
            for (int partition : topicPartitions) {
                var holder = owners.put(partition, memberId);
                if (holder != null) {
                    throw new IllegalArgumentException("Partition " + partition + " of " + topicId + " is already held by " + holder);
                }
            }
        });
        return this;
    }

    /**
     * @return The group spec.
     */
    public GroupSpec build() {
        checkNotBuilt();
        built = true;
        var subscriptionType = sameSubscriptions ? SubscriptionType.HOMOGENEOUS : SubscriptionType.HETEROGENEOUS;
        // The metadata image is only used by isPartitionAssigned, which the uniform2 assignor never calls.
        return new GroupSpecImpl(CoordinatorMetadataImage.EMPTY, members, subscriptionType);
    }

    private void checkNotBuilt() {
        if (built) {
            throw new IllegalStateException("The group spec is already built");
        }
    }
}
