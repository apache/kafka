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
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A range assignor assigns contiguous partition ranges to members of a consumer group such that:
 * <ol>
 *   <li>Each subscribed member receives at least one partition from that topic.</li>
 *   <li>Each member receives the same partition number from every subscribed topic when co-partitioning is possible.</li>
 * </ol>
 *
 * Co-partitioning is possible when the below conditions are satisfied:
 * <ol>
 *   <li>All the members are subscribed to the same set of topics.</li>
 *   <li>All the topics have the same number of partitions.</li>
 * </ol>
 *
 * Co-partitioning is useful in performing joins on data streams.
 *
 * <p>For example, suppose there are two members M0 and M1, two topics T1 and T2, and each topic has 3 partitions.
 *
 * <p>The co-partitioned assignment will be:
 * <ul>
 * <li<code>    M0: [T1P0, T1P1, T2P0, T2P1]    </code></li>
 * <li><code>   M1: [T1P2, T2P2]                </code></li>
 * </ul>
 *
 * Since the introduction of static membership, we could leverage <code>member.instance.id</code> to make the
 * assignment behavior more sticky.
 * For the above example, after one rolling bounce, the group coordinator will attempt to assign new member Ids towards
 * members, for example if <code>M0</code> -&gt; <code>M3</code> <code>M1</code> -&gt; <code>M2</code>.
 *
 * <p>The assignment could be completely shuffled to:
 * <ul>
 * <li><code>   M3 (was M0): [T1P2, T2P2]               (before it was [T1P0, T1P1, T2P0, T2P1])  </code>
 * <li><code>   M2 (was M1): [T1P0, T1P1, T2P0, T2P1]   (before it was [T1P2, T2P2])  </code>
 * </ul>
 *
 * The assignment change was caused by the change of <code>member.id</code> relative order, and
 * can be avoided by setting the instance.id.
 * Members will have individual instance Ids <code>I0</code>, <code>I1</code>. As long as
 * 1. Number of members remain the same.
 * 2. Topic metadata doesn't change.
 * 3. Subscription pattern doesn't change for any member.
 *
 * <p>The assignment will always be:
 * <ul>
 * <li><code>   I0: [T1P0, T1P1, T2P0, T2P1]    </code>
 * <li><code>   I1: [T1P2, T2P2]                </code>
 * </ul>
 * <p>
 */
public class RangeAssignor implements ConsumerGroupPartitionAssignor {
    public static final String NAME = "range";

    @Override
    public String name() {
        return NAME;
    }

    /**
     * Metadata for a topic including partition and subscription details.
     */
    private static class TopicMetadata {
        private final Uuid topicId;
        private final int numPartitions;
        private int numMembers;
        private int minQuota = -1;
        private int extraPartitions = -1;
        private int nextRange = 0;

        /**
         * Constructs a new TopicMetadata instance.
         *
         * @param topicId           The topic Id.
         * @param numPartitions     The number of partitions.
         * @param numMembers        The number of subscribed members.
         */
        private TopicMetadata(Uuid topicId, int numPartitions, int numMembers) {
            this.topicId = topicId;
            this.numPartitions = numPartitions;
            this.numMembers = numMembers;
        }

        /**
         * Computes the minimum partition quota per member and the extra partitions, if not already computed.
         */
        private void maybeComputeQuota() {
            if (minQuota != -1) return;

            // The minimum number of partitions each member should receive for a balanced assignment.
            minQuota = numPartitions / numMembers;

            // Extra partitions to be distributed one to each member.
            extraPartitions = numPartitions % numMembers;
        }

        @Override
        public String toString() {
            return "TopicMetadata(topicId=" + topicId +
                ", numPartitions=" + numPartitions +
                ", numMembers=" + numMembers +
                ", minQuota=" + minQuota +
                ", extraPartitions=" + extraPartitions +
                ", nextRange=" + nextRange +
                ')';
        }
    }

    /**
     * Assigns partitions to members of a homogeneous group. All members are subscribed to the same set of topics.
     * Assignment will be co-partitioned when all the topics have an equal number of partitions.
     */
    private GroupAssignment assignHomogeneousGroup(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        List<String> memberIds = sortMemberIds(groupSpec);
        int numMembers = groupSpec.memberIds().size();

        MemberSubscription subs = groupSpec.memberSubscription(memberIds.get(0));
        List<TopicMetadata> topics = new ArrayList<>(subs.subscribedTopicIds().size());

        for (Uuid topicId : subs.subscribedTopicIds()) {
            int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            if (numPartitions == -1) {
                throw new PartitionAssignorException("Member is subscribed to a non-existent topic");
            }
            TopicMetadata m = new TopicMetadata(
                topicId,
                numPartitions,
                numMembers
            );
            topics.add(m);
        }

        Map<String, MemberAssignment> assignments = new HashMap<>((int) ((groupSpec.memberIds().size() / 0.75f) + 1));
        int memberAssignmentInitialCapacity = (int) ((topics.size() / 0.75f) + 1);

        for (String memberId : memberIds) {
            Map<Uuid, Set<Integer>> assignment = new HashMap<>(memberAssignmentInitialCapacity);
            for (TopicMetadata topicMetadata : topics) {
                topicMetadata.maybeComputeQuota();
                addPartitionsToAssignment(topicMetadata, assignment);
            }
            assignments.put(memberId, new MemberAssignmentImpl(assignment));
        }

        return new GroupAssignment(assignments);
    }

    /**
     * Assigns partitions to members of a heterogeneous group. Not all members are subscribed to the same topics.
     */
    private GroupAssignment assignHeterogeneousGroup(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        List<String> memberIds = sortMemberIds(groupSpec);

        Map<Uuid, TopicMetadata> topics = new HashMap<>();

        for (String memberId : memberIds) {
            MemberSubscription subs = groupSpec.memberSubscription(memberId);
            for (Uuid topicId : subs.subscribedTopicIds()) {
                TopicMetadata topicMetadata = topics.computeIfAbsent(topicId, __ -> {
                    int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
                    if (numPartitions == -1) {
                        throw new PartitionAssignorException("Member is subscribed to a non-existent topic");
                    }

                    return new TopicMetadata(
                        topicId,
                        numPartitions,
                        0
                    );
                });
                topicMetadata.numMembers++;
            }
        }

        Map<String, MemberAssignment> assignments = new HashMap<>((int) ((groupSpec.memberIds().size() / 0.75f) + 1));

        for (String memberId : memberIds) {
            MemberSubscription subs = groupSpec.memberSubscription(memberId);
            Map<Uuid, Set<Integer>> assignment = new HashMap<>((int) ((subs.subscribedTopicIds().size() / 0.75f) + 1));
            for (Uuid topicId : subs.subscribedTopicIds()) {
                TopicMetadata metadata = topics.get(topicId);
                metadata.maybeComputeQuota();
                addPartitionsToAssignment(metadata, assignment);
            }
            assignments.put(memberId, new MemberAssignmentImpl(assignment));
        }

        return new GroupAssignment(assignments);
    }

    /**
     * Sorts members based on their instance Ids if available or by member Ids if not.
     *
     * Static members are placed first and non-static members follow.
     *
     * Prioritizing static members helps them retain their partitions, enhancing stickiness
     * and stability. Non-static members, which do not have guaranteed rejoining Ids, are placed
     * later, allowing for more dynamic and flexible partition assignments.
     *
     * @param groupSpec     The group specification containing the member information.
     * @return A sorted list of member Ids.
     */
    private List<String> sortMemberIds(
        GroupSpec groupSpec
    ) {
        List<String> sortedMemberIds = new ArrayList<>(groupSpec.memberIds());

        sortedMemberIds.sort((memberId1, memberId2) -> {
            Optional<String> instanceId1 = groupSpec.memberSubscription(memberId1).instanceId();
            Optional<String> instanceId2 = groupSpec.memberSubscription(memberId2).instanceId();

            if (instanceId1.isPresent() && instanceId2.isPresent()) {
                return instanceId1.get().compareTo(instanceId2.get());
            } else if (instanceId1.isPresent()) {
                return -1;
            } else if (instanceId2.isPresent()) {
                return 1;
            } else {
                return memberId1.compareTo(memberId2);
            }
        });
        return sortedMemberIds;
    }

    /**
     * Assigns a range of partitions to the specified topic based on the provided metadata.
     *
     * @param topicMetadata         Metadata containing the topic details, including the number of partitions,
     *                              the next range to assign, minQuota, and extra partitions.
     * @param memberAssignment      Map from topic Id to the set of assigned partition Ids.
     */
    private void addPartitionsToAssignment(
        TopicMetadata topicMetadata,
        Map<Uuid, Set<Integer>> memberAssignment
    ) {
        int start = topicMetadata.nextRange;
        int quota = topicMetadata.minQuota;

        // Adjust quota to account for extra partitions if available.
        if (topicMetadata.extraPartitions > 0) {
            quota++;
            topicMetadata.extraPartitions--;
        }

        // Calculate the end using the quota.
        int end = Math.min(start + quota, topicMetadata.numPartitions);

        topicMetadata.nextRange = end;

        if (start < end) {
            memberAssignment.put(topicMetadata.topicId, new RangeSet(start, end));
        }
    }

    /**
     * Assigns partitions to members based on their topic subscriptions and the properties of a range assignor:
     *
     * @param groupSpec                     The group specification containing the member information.
     * @param subscribedTopicDescriber      The describer for subscribed topics to get the number of partitions.
     * @return The group's assignment with the partition assignments for each member.
     * @throws PartitionAssignorException if any member is subscribed to a non-existent topic.
     */
    @Override
    public GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        if (groupSpec.memberIds().isEmpty()) {
            return new GroupAssignment(Collections.emptyMap());
        } else if (groupSpec.subscriptionType() == SubscriptionType.HOMOGENEOUS) {
            return assignHomogeneousGroup(groupSpec, subscribedTopicDescriber);
        } else {
            return assignHeterogeneousGroup(groupSpec, subscribedTopicDescriber);
        }
    }
}
