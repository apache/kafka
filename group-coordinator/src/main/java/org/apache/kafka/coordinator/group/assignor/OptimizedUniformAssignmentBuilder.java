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
import org.apache.kafka.server.common.TopicIdPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The optimized uniform assignment builder is used to generate the target assignment for a consumer group with
 * all its members subscribed to the same set of topics.
 * It is optimized since the assignment can be done in fewer, less complicated steps compared to when
 * the subscriptions are different across the members.
 *
 * Assignments are done according to the following principles:
 *
 * <li> Balance:          Ensure partitions are distributed equally among all members.
 *                        The difference in assignments sizes between any two members
 *                        should not exceed one partition. </li>
 * <li> Stickiness:       Minimize partition movements among members by retaining
 *                        as much of the existing assignment as possible. </li>
 *
 * The assignment builder prioritizes the properties in the following order:
 *      Balance > Stickiness.
 */
public class OptimizedUniformAssignmentBuilder extends AbstractUniformAssignmentBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(OptimizedUniformAssignmentBuilder.class);

    /**
     * The assignment specification which includes member metadata.
     */
    private final AssignmentSpec assignmentSpec;

    /**
     * The topic and partition metadata describer.
     */
    private final SubscribedTopicDescriber subscribedTopicDescriber;

    /**
     * The set of topic Ids that the consumer group is subscribed to.
     */
    private final Set<Uuid> subscribedTopicIds;

    /**
     * The number of members to receive an extra partition beyond the minimum quota.
     * Minimum Quota = Total Partitions / Total Members
     * Example: If there are 11 partitions to be distributed among 3 members,
     *          each member gets 3 (11 / 3) [minQuota] partitions and 2 (11 % 3) members get an extra partition.
     */
    private int remainingMembersToGetAnExtraPartition;

    /**
     * Members mapped to the remaining number of partitions needed to meet the minimum quota.
     * Minimum quota = total partitions / total members.
     */
    private Map<String, Integer> potentiallyUnfilledMembers;

    /**
     * The partitions that still need to be assigned.
     * Initially this contains all the subscribed topics' partitions.
     */
    private Set<TopicIdPartition> unassignedPartitions;

    /**
     * The target assignment.
     */
    private final Map<String, MemberAssignment> targetAssignment;

    OptimizedUniformAssignmentBuilder(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.assignmentSpec = assignmentSpec;
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscribedTopicIds = new HashSet<>(assignmentSpec.members().values().iterator().next().subscribedTopicIds());
        this.potentiallyUnfilledMembers = new HashMap<>();
        this.targetAssignment = new HashMap<>();
    }

    /**
     * Here's the step-by-step breakdown of the assignment process:
     *
     * <li> Compute the quotas of partitions for each member based on the total partitions and member count.</li>
     * <li> Initialize unassigned partitions to all the topic partitions and
     *      remove partitions from the list as and when they are assigned.</li>
     * <li> For existing assignments, retain partitions based on the determined quota.</li>
     * <li> Identify members that haven't fulfilled their partition quota or are eligible to receive extra partitions.</li>
     * <li> Proceed with a round-robin assignment according to quotas.
     *      For each unassigned partition, locate the first compatible member from the potentially unfilled list.</li>
     */
    @Override
    protected GroupAssignment buildAssignment() throws PartitionAssignorException {
        int totalPartitionsCount = 0;

        if (subscribedTopicIds.isEmpty()) {
            LOG.debug("The subscription list is empty, returning an empty assignment");
            return new GroupAssignment(Collections.emptyMap());
        }

        for (Uuid topicId : subscribedTopicIds) {
            int partitionCount = subscribedTopicDescriber.numPartitions(topicId);
            if (partitionCount == -1) {
                throw new PartitionAssignorException(
                    "Members are subscribed to topic " + topicId + " which doesn't exist in the topic metadata."
                );
            } else {
                totalPartitionsCount += partitionCount;
            }
        }

        // The minimum required quota that each member needs to meet for a balanced assignment.
        // This is the same for all members.
        final int numberOfMembers = assignmentSpec.members().size();
        final int minQuota = totalPartitionsCount / numberOfMembers;
        remainingMembersToGetAnExtraPartition = totalPartitionsCount % numberOfMembers;

        unassignedPartitions = topicIdPartitions(subscribedTopicIds, subscribedTopicDescriber);
        potentiallyUnfilledMembers = assignStickyPartitions(minQuota);
        assignRemainingPartitions();

        if (!unassignedPartitions.isEmpty()) {
            throw new PartitionAssignorException("Partitions were left unassigned");
        }

        return new GroupAssignment(targetAssignment);
    }

    /**
     * Retains a set of partitions from the existing assignment and includes them in the target assignment.
     * Only relevant partitions that exist in the current topic metadata and subscriptions are considered.
     *
     * <p> For each member:
     * <ol>
     *     <li> Find the valid current assignment considering topic subscriptions and metadata</li>
     *     <li> If the current assignment exists, retain partitions up to the minimum quota.</li>
     *     <li> If the current assignment size is greater than the minimum quota and
     *          there are members that could get an extra partition, assign the next partition as well.</li>
     *     <li> Finally, if the member's current assignment size is less than the minimum quota,
     *          add them to the potentially unfilled members map and track the number of remaining
     *          partitions required to meet the quota.</li>
     * </ol>
     * </p>
     *
     * @return  Members mapped to the remaining number of partitions needed to meet the minimum quota,
     *          including members that are eligible to receive an extra partition.
     */
    private Map<String, Integer> assignStickyPartitions(int minQuota) {
        Map<String, Integer> potentiallyUnfilledMembers = new HashMap<>();

        for (Map.Entry<String, AssignmentMemberSpec> entry : assignmentSpec.members().entrySet()) {
            String memberId = entry.getKey();
            AssignmentMemberSpec assignmentMemberSpec = entry.getValue();
            Map<Uuid, Set<Integer>> oldAssignment = assignmentMemberSpec.assignedPartitions();
            Map<Uuid, Set<Integer>> newAssignment = null;

            AtomicInteger quota = new AtomicInteger(minQuota);
            if (remainingMembersToGetAnExtraPartition > 0) {
                quota.incrementAndGet();
                remainingMembersToGetAnExtraPartition--;
            }

            for (Map.Entry<Uuid, Set<Integer>> topicPartitions : oldAssignment.entrySet()) {
                Uuid topicId = topicPartitions.getKey();
                Set<Integer> partitions = topicPartitions.getValue();

                if (subscribedTopicIds.contains(topicId)) {
                    for (Integer partition : partitions) {
                        if (quota.get() > 0) {
                            quota.decrementAndGet();
                            unassignedPartitions.remove(new TopicIdPartition(topicId, partition));
                        } else {
                            if (newAssignment == null) newAssignment = deepCopy(oldAssignment);
                            Set<Integer> parts = newAssignment.get(topicId);
                            parts.remove(partition);
                            if (parts.isEmpty()) newAssignment.remove(topicId);
                        }
                    }
                } else {
                    if (newAssignment == null) newAssignment = deepCopy(oldAssignment);
                    newAssignment.remove(topicId);
                }
            }

            if (quota.get() > 0) {
                potentiallyUnfilledMembers.put(memberId, quota.get());
            }

            if (newAssignment == null) {
                targetAssignment.put(memberId, new MemberAssignment(oldAssignment));
            } else {
                targetAssignment.put(memberId, new MemberAssignment(newAssignment));
            }
        }

        return potentiallyUnfilledMembers;
    }

    private void assignRemainingPartitions() {
        for (Map.Entry<String, Integer> unfilledMemberEntry : potentiallyUnfilledMembers.entrySet()) {
            String memberId = unfilledMemberEntry.getKey();

            int remaining = unfilledMemberEntry.getValue();
            if (remainingMembersToGetAnExtraPartition > 0) {
                remaining++;
                remainingMembersToGetAnExtraPartition--;
            }

            Map<Uuid, Set<Integer>> newAssignment = targetAssignment.get(memberId).targetPartitions();
            if (!(newAssignment instanceof HashMap)) {
                newAssignment = deepCopy(newAssignment);
                targetAssignment.put(memberId, new MemberAssignment(newAssignment));
            }

            Iterator<TopicIdPartition> it = unassignedPartitions.iterator();
            for (int i = 0; i < remaining && it.hasNext(); i++) {
                TopicIdPartition unassignedTopicIdPartition = it.next();
                it.remove();
                newAssignment
                    .computeIfAbsent(unassignedTopicIdPartition.topicId(), __ -> new HashSet<>())
                    .add(unassignedTopicIdPartition.partitionId());
            }
        }
    }

    private static Map<Uuid, Set<Integer>> deepCopy(Map<Uuid, Set<Integer>> map) {
        Map<Uuid, Set<Integer>> copy = new HashMap<>(map.size());
        for (Map.Entry<Uuid, Set<Integer>> entry : map.entrySet()) {
            copy.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return copy;
    }
}
