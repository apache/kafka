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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Math.min;

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
 * <li> Rack Matching:    When feasible, aim to assign partitions to members
 *                        located on the same rack thus avoiding cross-zone traffic. </li>
 * <li> Stickiness:       Minimize partition movements among members by retaining
 *                        as much of the existing assignment as possible. </li>
 *
 * The assignment builder prioritizes the properties in the following order:
 *      Balance > Rack Matching > Stickiness.
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
     * Rack information and helper methods.
     */
    private final RackInfo rackInfo;

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

    /**
     * Tracks the existing owner of each partition.
     * Only populated when the rack awareness strategy is used.
     */
    private final Map<TopicIdPartition, String> currentPartitionOwners;

    OptimizedUniformAssignmentBuilder(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.assignmentSpec = assignmentSpec;
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscribedTopicIds = new HashSet<>(assignmentSpec.members().values().iterator().next().subscribedTopicIds());
        this.rackInfo = new RackInfo(assignmentSpec, subscribedTopicDescriber, subscribedTopicIds);
        this.potentiallyUnfilledMembers = new HashMap<>();
        this.targetAssignment = new HashMap<>();
        // Without rack-aware strategy, tracking current owners of unassigned partitions is unnecessary
        // as all sticky partitions are retained until a member meets its quota.
        this.currentPartitionOwners = rackInfo.useRackStrategy ? new HashMap<>() : Collections.emptyMap();
    }

    /**
     * Here's the step-by-step breakdown of the assignment process:
     *
     * <li> Compute the quotas of partitions for each member based on the total partitions and member count.</li>
     * <li> Initialize unassigned partitions to all the topic partitions and
     *      remove partitions from the list as and when they are assigned.</li>
     * <li> For existing assignments, retain partitions based on the determined quota and member's rack compatibility.</li>
     * <li> If a partition's rack mismatches with its owner, track it for future use.</li>
     * <li> Identify members that haven't fulfilled their partition quota or are eligible to receive extra partitions.</li>
     * <li> Proceed with a round-robin assignment adhering to rack awareness.
     *      For each unassigned partition, locate the first compatible member from the potentially unfilled list.</li>
     * <li> If no rack-compatible member is found, revert to the tracked current owner.
     *      If that member can't accommodate the partition due to quota limits, resort to a generic round-robin assignment.</li>
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

        assignmentSpec.members().keySet().forEach(memberId ->
            targetAssignment.put(memberId, new MemberAssignment(new HashMap<>())
        ));

        unassignedPartitions = topicIdPartitions(subscribedTopicIds, subscribedTopicDescriber);
        potentiallyUnfilledMembers = assignStickyPartitions(minQuota);

        if (rackInfo.useRackStrategy) rackAwarePartitionAssignment();
        unassignedPartitionsRoundRobinAssignment();

        if (!unassignedPartitions.isEmpty()) {
            throw new PartitionAssignorException("Partitions were left unassigned");
        }

        return new GroupAssignment(targetAssignment);
    }

    /**
     * Retains a set of partitions from the existing assignment and includes them in the target assignment.
     * Only relevant partitions that exist in the current topic metadata and subscriptions are considered.
     * In addition, if rack awareness is enabled, it is ensured that a partition's rack matches the member's rack.
     *
     * <p> For each member:
     * <ol>
     *     <li> Find the valid current assignment considering topic subscriptions, metadata and rack information.</li>
     *     <li> When rack aware strategy is used, only partitions with their rack matching their current
     *          owner's rack are returned in the valid assignment.</li>
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

        assignmentSpec.members().forEach((memberId, assignmentMemberSpec) -> {
            List<TopicIdPartition> validCurrentMemberAssignment = validCurrentMemberAssignment(
                memberId,
                assignmentMemberSpec.assignedPartitions()
            );

            int currentAssignmentSize = validCurrentMemberAssignment.size();
            // Number of partitions required to meet the minimum quota.
            int remaining = minQuota - currentAssignmentSize;

            if (currentAssignmentSize > 0) {
                int retainedPartitionsCount = min(currentAssignmentSize, minQuota);
                IntStream.range(0, retainedPartitionsCount).forEach(i -> {
                    TopicIdPartition topicIdPartition = validCurrentMemberAssignment.get(i);
                    addPartitionToAssignment(
                        targetAssignment,
                        memberId,
                        topicIdPartition.topicId(),
                        topicIdPartition.partitionId()
                    );
                    unassignedPartitions.remove(topicIdPartition);
                });

                // The extra partition is located at the last index from the previous step.
                if (remaining < 0 && remainingMembersToGetAnExtraPartition > 0) {
                    TopicIdPartition topicIdPartition = validCurrentMemberAssignment.get(retainedPartitionsCount);
                    addPartitionToAssignment(
                        targetAssignment,
                        memberId,
                        topicIdPartition.topicId(),
                        topicIdPartition.partitionId()
                    );
                    unassignedPartitions.remove(topicIdPartition);
                    remainingMembersToGetAnExtraPartition--;
                }
            }

            if (remaining >= 0) {
                potentiallyUnfilledMembers.put(memberId, remaining);
            }
        });

        return potentiallyUnfilledMembers;
    }

    /**
     * Filters the current assignment of partitions for a given member based on certain criteria.
     *
     * Any partition that still belongs to the member's subscribed topics list is considered valid.
     * If rack aware strategy can be used: Only partitions with matching rack are valid and non-matching partitions are
     * tracked with their current owner for future use.
     *
     * @param memberId                      The Id of the member whose assignment is being validated.
     * @param currentMemberAssignment       The map of topics to partitions currently assigned to the member.
     *
     * @return List of valid partitions after applying the filters.
     */
    private List<TopicIdPartition> validCurrentMemberAssignment(
        String memberId,
        Map<Uuid, Set<Integer>> currentMemberAssignment
    ) {
        List<TopicIdPartition> validCurrentAssignmentList = new ArrayList<>();
        currentMemberAssignment.forEach((topicId, partitions) -> {
            if (subscribedTopicIds.contains(topicId)) {
                partitions.forEach(partition -> {
                    TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, partition);
                    if (rackInfo.useRackStrategy && rackInfo.racksMismatch(memberId, topicIdPartition)) {
                        currentPartitionOwners.put(topicIdPartition, memberId);
                    } else {
                        validCurrentAssignmentList.add(topicIdPartition);
                    }
                });
            } else {
                LOG.debug("The topic " + topicId + " is no longer present in the subscribed topics list");
            }
        });

        return validCurrentAssignmentList;
    }

    /**
     * Allocates the unassigned partitions to unfilled members present in the same rack.
     * Partitions with the least number of potential members in the same rack are allotted first.
     * Members in the same rack with the least number of partitions in the target assignment
     * are assigned partitions first.
     */
    private void rackAwarePartitionAssignment() {
        // Sort partitions in ascending order by number of potential members with matching racks.
        // Partitions with no potential members in the same rack aren't included in this list.
        List<TopicIdPartition> sortedPartitions = rackInfo.sortPartitionsByRackMembers(unassignedPartitions);

        sortedPartitions.forEach(partition -> {
            List<String> sortedMembersWithMatchingRack = rackInfo.getSortedMembersWithMatchingRack(partition, targetAssignment);

            for (String memberId : sortedMembersWithMatchingRack) {
                if (potentiallyUnfilledMembers.containsKey(memberId) && maybeAssignPartitionToMember(memberId, partition)) {
                    unassignedPartitions.remove(partition);
                    break;
                }
            }
        });
    }

    /**
     * Allocates the unassigned partitions to unfilled members in a round-robin fashion.
     *
     * If the rack-aware strategy is enabled, partitions are attempted to be assigned back to their current owners first.
     * This is because pure stickiness without rack matching is not considered initially.
     *
     * If a partition couldn't be assigned to its current owner due to the quotas OR
     * if the rack-aware strategy is not enabled, the partitions are allocated to the unfilled members.
     */
    private void unassignedPartitionsRoundRobinAssignment() {
        Queue<String> roundRobinMembers = new LinkedList<>(potentiallyUnfilledMembers.keySet());

        // Partitions are sorted to ensure an even topic wise distribution across members.
        // This not only balances the load but also makes partition-to-member mapping more predictable.
        List<TopicIdPartition> sortedPartitionsList = unassignedPartitions.stream()
            .sorted(Comparator.comparing(TopicIdPartition::topicId).thenComparing(TopicIdPartition::partitionId))
            .collect(Collectors.toList());

        for (TopicIdPartition topicIdPartition : sortedPartitionsList) {
            boolean assigned = false;

            if (rackInfo.useRackStrategy && currentPartitionOwners.containsKey(topicIdPartition)) {
                String currentOwner = currentPartitionOwners.get(topicIdPartition);
                if (potentiallyUnfilledMembers.containsKey(currentOwner)) {
                    assigned = maybeAssignPartitionToMember(currentOwner, topicIdPartition);
                    if (!potentiallyUnfilledMembers.containsKey(currentOwner)) {
                        roundRobinMembers.remove(currentOwner);
                    }
                }
            }

            for (int i = 0; i < roundRobinMembers.size() && !assigned; i++) {
                String memberId = roundRobinMembers.poll();
                if (potentiallyUnfilledMembers.containsKey(memberId)) {
                    assigned = maybeAssignPartitionToMember(memberId, topicIdPartition);
                }
                // Only re-add the member to the end of the queue if it's still available for assignment.
                if (potentiallyUnfilledMembers.containsKey(memberId)) {
                    roundRobinMembers.add(memberId);
                }
            }

            if (assigned) {
                unassignedPartitions.remove(topicIdPartition);
            }
        }
    }

    /**
     * Assigns the specified partition to the given member and updates the potentially unfilled members map.
     * Only assign extra partitions once the member has met its minimum quota = total partitions / total members.
     *
     * <ol>
     *     <li> If the minimum quota hasn't been met aka remaining > 0 directly assign the partition.
     *          After assigning the partition, if the min quota has been met aka remaining = 0, remove the member
     *          if there's no members left to receive an extra partition. Otherwise, keep it in the
     *          potentially unfilled map. </li>
     *     <li> If the minimum quota has been met and if there is potential to receive an extra partition, assign it.
     *          Remove the member from the potentially unfilled map since it has already received the extra partition
     *          and met the min quota. </li>
     *     <li> Else, don't assign the partition. </li>
     * </ol>
     *
     * @param memberId              The Id of the member to which the partition will be assigned.
     * @param topicIdPartition      The topicIdPartition to be assigned.
     * @return true if the assignment was successful, false otherwise.
     */
    private boolean maybeAssignPartitionToMember(String memberId, TopicIdPartition topicIdPartition) {
        int remaining = potentiallyUnfilledMembers.get(memberId);
        boolean shouldAssign = false;

        // If the member hasn't met the minimum quota, set the flag for assignment.
        // If member has met minimum quota and there's an extra partition available, set the flag for assignment.
        if (remaining > 0) {
            potentiallyUnfilledMembers.put(memberId, --remaining);
            shouldAssign = true;

            // If the member meets the minimum quota due to this assignment,
            // check if any extra partitions are available.
            // Removing the member from the list reduces an iteration for when remaining = 0 but there's no extras left.
            if (remaining == 0 && remainingMembersToGetAnExtraPartition == 0) {
                potentiallyUnfilledMembers.remove(memberId);
            }
        } else if (remaining == 0 && remainingMembersToGetAnExtraPartition > 0) {
            remainingMembersToGetAnExtraPartition--;
            // Each member can only receive one extra partition, once they meet the minimum quota and receive an extra
            // partition they can be removed from the potentially unfilled members map.
            potentiallyUnfilledMembers.remove(memberId);
            shouldAssign = true;
        }

        // Assign the partition if flag is set.
        if (shouldAssign) {
            addPartitionToAssignment(
                targetAssignment,
                memberId,
                topicIdPartition.topicId(),
                topicIdPartition.partitionId()
            );
            return true;
        }

        // No assignment possible because the member met the minimum quota but
        // number of members to receive an extra partition is zero.
        return false;
    }
}
