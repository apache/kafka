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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private static final Class<?> UNMODIFIALBE_MAP_CLASS = Collections.unmodifiableMap(new HashMap<>()).getClass();
    private static final Class<?> EMPTY_MAP_CLASS = Collections.emptyMap().getClass();

    private static boolean isImmutableMap(Map<?, ?> map) {
        return UNMODIFIALBE_MAP_CLASS.isInstance(map) || EMPTY_MAP_CLASS.isInstance(map);
    }

    /**
     * The assignment specification which includes member metadata.
     */
    private final GroupSpec groupSpec;

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
    private final List<TopicIdPartition> unassignedPartitions;

    /**
     * The target assignment.
     */
    private final Map<String, MemberAssignment> targetAssignment;

    OptimizedUniformAssignmentBuilder(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.groupSpec = groupSpec;
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscribedTopicIds = new HashSet<>(groupSpec.members().values().iterator().next().subscribedTopicIds());
        this.potentiallyUnfilledMembers = new HashMap<>();
        this.unassignedPartitions = new ArrayList<>();
        this.targetAssignment = new HashMap<>();
    }

    /**
     * Here's the step-by-step breakdown of the assignment process:
     *
     * <li> Compute the quotas of partitions for each member based on the total partitions and member count.</li>
     * <li> Initialize unassigned partitions with all the topic partitions that aren't present in the
     *      current target assignment.</li>
     * <li> For existing assignments, retain partitions based on the determined quota. Add extras to unassigned partitions.</li>
     * <li> Identify members that haven't fulfilled their partition quota or are eligible to receive extra partitions.</li>
     * <li> Proceed with a round-robin assignment according to quotas.
     *      For each unassigned partition, locate the first compatible member from the potentially unfilled list.</li>
     */
    @Override
    protected GroupAssignment buildAssignment() throws PartitionAssignorException {
        if (subscribedTopicIds.isEmpty()) {
            LOG.debug("The subscription list is empty, returning an empty assignment");
            return new GroupAssignment(Collections.emptyMap());
        }

        // Check if the subscribed topicId is still valid.
        // Update unassigned partitions based on the current target assignment
        // and topic metadata.
        int totalPartitionsCount = 0;
        for (Uuid topicId : subscribedTopicIds) {
            int partitionCount = subscribedTopicDescriber.numPartitions(topicId);
            if (partitionCount == -1) {
                throw new PartitionAssignorException(
                    "Members are subscribed to topic " + topicId + " which doesn't exist in the topic metadata."
                );
            } else {
                for (int i = 0; i < partitionCount; i++) {
                    if (!groupSpec.isPartitionAssigned(topicId, i)) {
                        unassignedPartitions.add(new TopicIdPartition(topicId, i));
                    }
                }
                totalPartitionsCount += partitionCount;
            }
        }

        // The minimum required quota that each member needs to meet for a balanced assignment.
        // This is the same for all members.
        final int numberOfMembers = groupSpec.members().size();
        final int minQuota = totalPartitionsCount / numberOfMembers;
        remainingMembersToGetAnExtraPartition = totalPartitionsCount % numberOfMembers;

        maybeRevokePartitions(minQuota);
        assignRemainingPartitions();

        return new GroupAssignment(targetAssignment);
    }

    private void maybeRevokePartitions(int minQuota) {
        for (Map.Entry<String, AssignmentMemberSpec> entry : groupSpec.members().entrySet()) {
            String memberId = entry.getKey();
            AssignmentMemberSpec assignmentMemberSpec = entry.getValue();
            Map<Uuid, Set<Integer>> oldAssignment = assignmentMemberSpec.assignedPartitions();
            Map<Uuid, Set<Integer>> newAssignment = null;

            if (!isImmutableMap(oldAssignment)) {
                throw new IllegalStateException("The assignor expect an immutable map.");
            }

            int quota = minQuota;
            if (remainingMembersToGetAnExtraPartition > 0) {
                quota++;
                remainingMembersToGetAnExtraPartition--;
            }

            for (Map.Entry<Uuid, Set<Integer>> topicPartitions : oldAssignment.entrySet()) {
                Uuid topicId = topicPartitions.getKey();
                Set<Integer> partitions = topicPartitions.getValue();

                if (subscribedTopicIds.contains(topicId)) {
                    for (Integer partition : partitions) {
                        if (quota > 0) {
                            quota--;
                        } else {
                            if (newAssignment == null) newAssignment = deepCopy(oldAssignment);
                            Set<Integer> parts = newAssignment.get(topicId);
                            parts.remove(partition);
                            if (parts.isEmpty()) newAssignment.remove(topicId);
                            unassignedPartitions.add(new TopicIdPartition(topicId, partition));
                        }
                    }
                } else {
                    if (newAssignment == null) newAssignment = deepCopy(oldAssignment);
                    newAssignment.remove(topicId);
                }
            }

            if (quota > 0) {
                potentiallyUnfilledMembers.put(memberId, quota);
            }

            if (newAssignment == null) {
                targetAssignment.put(memberId, new MemberAssignment(oldAssignment));
            } else {
                targetAssignment.put(memberId, new MemberAssignment(newAssignment));
            }
        }
    }

    private void assignRemainingPartitions() {
        Iterator<TopicIdPartition> it = unassignedPartitions.iterator();

        for (Map.Entry<String, Integer> unfilledMemberEntry : potentiallyUnfilledMembers.entrySet()) {
            String memberId = unfilledMemberEntry.getKey();
            int remaining = unfilledMemberEntry.getValue();

            Map<Uuid, Set<Integer>> newAssignment = targetAssignment.get(memberId).targetPartitions();
            if (isImmutableMap(newAssignment)) {
                newAssignment = deepCopy(newAssignment);
                targetAssignment.put(memberId, new MemberAssignment(newAssignment));
            }

            for (int i = 0; i < remaining && it.hasNext(); i++) {
                TopicIdPartition unassignedTopicIdPartition = it.next();
                newAssignment
                    .computeIfAbsent(unassignedTopicIdPartition.topicId(), __ -> new HashSet<>())
                    .add(unassignedTopicIdPartition.partitionId());
            }
        }

        if (it.hasNext()) {
            throw new PartitionAssignorException("Partitions were left unassigned");
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
