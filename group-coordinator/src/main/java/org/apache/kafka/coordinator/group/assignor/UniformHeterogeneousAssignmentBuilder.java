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
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The heterogeneous uniform assignment builder is used to generate the target assignment for a consumer group with
 * at least one of its members subscribed to a different set of topics.
 * <p/>
 * Assignments are done according to the following principles:
 * <ol>
 *   <li>Balance:          Ensure partitions are distributed equally among all members.
 *                         The difference in assignments sizes between any two members
 *                         should not exceed one partition.</li>
 *   <li>Stickiness:       Minimize partition movements among members by retaining
 *                         as much of the existing assignment as possible.</li>
 * </ol>
 *
 * This assignment builder prioritizes the above properties in the following order:
 *      Balance > Stickiness.
 */
public class UniformHeterogeneousAssignmentBuilder {

    /**
     * The maximum number of iterations to perform in the final iterative balancing phase.
     * <p/>
     * When all subscribers to a topic have the same subscriptions, the final balancing phase
     * converges in a single iteration and takes another iteration to detect that no further
     * balancing is possible. This describes most subscription patterns.
     * <p/>
     * When subscribers have overlapping, non-identical subscriptions, the final balancing phase can
     * take a lot longer to converge. We assume that each iteration roughly halves the imbalance and
     * choose a limit that allows for a reasonable balance to be achieved without taking too long.
     */
    private static final int MAX_ITERATION_COUNT = 16;

    /**
     * The group metadata specification.
     */
    private final GroupSpec groupSpec;

    /**
     * The topic and partition metadata describer.
     */
    private final SubscribedTopicDescriber subscribedTopicDescriber;

    /**
     * The list of all the topic Ids that the consumer group is subscribed to.
     */
    private final Set<Uuid> subscribedTopicIds;

    /**
     * The list of members in the consumer group.
     */
    private final List<String> memberIds;

    /**
     * Maps member ids to their indices in the memberIds list.
     */
    private final Map<String, Integer> memberIndices;

    /**
     * List of subscribed members for each topic, in ascending order.
     * <p/>
     * Members are stored as integer indices into the memberIds array.
     */
    private final Map<Uuid, List<Integer>> topicSubscribers;

    /**
     * The new assignment that will be returned.
     */
    private final Map<String, MemberAssignment> targetAssignment;

    /**
     * The number of partitions each member is assigned in the new assignment.
     * <p/>
     * The indices are the same as the memberIds array. memberTargetAssignmentSizes[i] contains the
     * number of partitions assigned to memberIds[i].
     * <p/>
     * We use a plain int[] array here for performance reasons. Looking up the number of partitions
     * assigned to a given member is a very common operation. By using a plain int[] array, we can
     * avoid:
     * <ul>
     *   <li>the overhead of boxing and unboxing integers.</li>
     *   <li>the cost of HashMap lookups.</li>
     *   <li>the cost of checking member ids for equality when there are HashMap collisions.</li>
     * </ul>
     */
    private final int[] memberTargetAssignmentSizes;

    /**
     * Orders topics by partitions per subscriber, descending.
     * <p/>
     * Ties are broken by subscriber count, ascending.
     * <p/>
     * Remaining ties are broken by topic id, ascending.
     */
    private final Comparator<Uuid> topicComparator;

    /**
     * Orders members by their number of assigned partitions, ascending.
     * <p/>
     * Ties are broken by member index, ascending.
     */
    private final Comparator<Integer> memberComparator;

    /**
     * Tracks the owner of each partition in the target assignment.
     * <p/>
     * Owners are represented as indices into the memberIds array.
     * -1 indicates an unassigned partition.
     */
    private final Map<Uuid, int[]> targetAssignmentPartitionOwners;

    public UniformHeterogeneousAssignmentBuilder(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.groupSpec = groupSpec;
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscribedTopicIds = new HashSet<>();

        // Number the members 0 to M - 1.
        this.memberIds = new ArrayList<>(groupSpec.memberIds());
        this.memberIndices = new HashMap<>();
        for (int memberIndex = 0; memberIndex < this.memberIds.size(); memberIndex++) {
            memberIndices.put(memberIds.get(memberIndex), memberIndex);
        }

        this.topicSubscribers = new HashMap<>();

        this.targetAssignment = new HashMap<>();
        this.memberTargetAssignmentSizes = new int[this.memberIds.size()];

        // Build set of all subscribed topics and sets of subscribers per topic.
        for (int memberIndex = 0; memberIndex < this.memberIds.size(); memberIndex++) {
            String memberId = this.memberIds.get(memberIndex);
            for (Uuid topicId : groupSpec.memberSubscription(memberId).subscribedTopicIds()) {
                // Check if the subscribed topic exists.
                int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
                if (numPartitions == -1) {
                    throw new PartitionAssignorException(
                        "Members are subscribed to topic " + topicId + " which doesn't exist in the topic metadata."
                    );
                }
                subscribedTopicIds.add(topicId);
                topicSubscribers.computeIfAbsent(topicId, k -> new ArrayList<>()).add(memberIndex);
            }
        }

        this.topicComparator = (topic1Id, topic2Id) -> {
            int topic1PartitionCount = subscribedTopicDescriber.numPartitions(topic1Id);
            int topic2PartitionCount = subscribedTopicDescriber.numPartitions(topic2Id);
            int topic1SubscriberCount = topicSubscribers.get(topic1Id).size();
            int topic2SubscriberCount = topicSubscribers.get(topic2Id).size();

            // Order by partitions per subscriber, descending.
            int order = Double.compare(
                (double) topic2PartitionCount / topic2SubscriberCount,
                (double) topic1PartitionCount / topic1SubscriberCount
            );

            // Then order by subscriber count, ascending.
            if (order == 0) {
                order = Integer.compare(topic1SubscriberCount, topic2SubscriberCount);
            }

            // Then order by topic id, ascending.
            if (order == 0) {
                order = topic1Id.compareTo(topic2Id);
            }

            return order;
        };

        this.memberComparator = (memberIndex1, memberIndex2) -> {
            // Order by number of assigned partitions, ascending.
            int order = Integer.compare(
                memberTargetAssignmentSizes[memberIndex1],
                memberTargetAssignmentSizes[memberIndex2]
            );

            // Then order by member index, ascending.
            if (order == 0) {
                order = memberIndex1.compareTo(memberIndex2);
            }

            return order;
        };

        // Initialize partition owners for the target assignments.
        this.targetAssignmentPartitionOwners = new HashMap<>((int) ((this.subscribedTopicIds.size() / 0.75f) + 1));
        for (Uuid topicId : this.subscribedTopicIds) {
            int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            int[] partitionOwners = new int[numPartitions];
            Arrays.fill(partitionOwners, -1);
            this.targetAssignmentPartitionOwners.put(topicId, partitionOwners);
        }
    }

    /**
     * Here's the step-by-step breakdown of the assignment process:
     * <ol>
     *   <li>Revoke partitions from the existing assignment that are no longer part of each member's
     *     subscriptions.</li>
     *   <li>Allocate all the remaining unassigned partitions to the members in a balanced manner.</li>
     *   <li>Iterate through the assignment until it is balanced.</li>
     * </ol>
     */
    public GroupAssignment build() {
        if (subscribedTopicIds.isEmpty()) {
            return new GroupAssignment(Collections.emptyMap());
        }

        maybeRevokePartitions();

        Map<Uuid, List<Integer>> unassignedPartitions = computeUnassignedPartitions();
        assignRemainingPartitions(unassignedPartitions);

        balance();

        return new GroupAssignment(targetAssignment);
    }

    /**
     * Sorts topics based on the following criteria:
     * <ol>
     *   <li>Topics are sorted in descending order of totalPartitions / number of subscribers
     *       (partitions per subscriber).</li>
     *   <li>Ties are broken by sorting in ascending order of number of subscribers.</li>
     *   <li>Any remaining ties are broken by sorting in ascending order of topic id.</li>
     * </ol>
     *
     * The last criteria is for predictability of assignments.
     *
     * @param topicIds          The topic ids that need to be sorted.
     * @return A list of sorted topic ids.
     */
    private List<Uuid> sortTopicIds(Collection<Uuid> topicIds) {
        List<Uuid> sortedTopicIds = new ArrayList<>(topicIds);
        sortedTopicIds.sort(topicComparator);
        return sortedTopicIds;
    }

    /**
     * Revoke the partitions that are not part of each member's subscriptions.
     *
     * This method ensures that the original assignment is not copied if it is not altered.
     */
    private void maybeRevokePartitions() {
        for (String memberId : groupSpec.memberIds()) {
            int memberIndex = memberIndices.get(memberId);

            Map<Uuid, Set<Integer>> oldAssignment = groupSpec.memberAssignment(memberId).partitions();
            Map<Uuid, Set<Integer>> newAssignment = null;

            // The assignor expects to receive the assignment as an immutable map. It leverages
            // this knowledge in order to avoid having to copy all assignments.
            if (!AssignorHelpers.isImmutableMap(oldAssignment)) {
                throw new IllegalStateException("The assignor expect an immutable map.");
            }

            for (Map.Entry<Uuid, Set<Integer>> topicPartitions : oldAssignment.entrySet()) {
                Uuid topicId = topicPartitions.getKey();
                Set<Integer> partitions = topicPartitions.getValue();

                if (groupSpec.memberSubscription(memberId).subscribedTopicIds().contains(topicId)) {
                    memberTargetAssignmentSizes[memberIndex] += partitions.size();

                    for (int partition : partitions) {
                        targetAssignmentPartitionOwners.get(topicId)[partition] = memberIndex;
                    }
                } else {
                    if (newAssignment == null) {
                        // If the new assignment is null, we create a deep copy of the
                        // original assignment so that we can alter it.
                        newAssignment = AssignorHelpers.deepCopyAssignment(oldAssignment);
                    }
                    // Remove the entire topic.
                    newAssignment.remove(topicId);
                }
            }

            if (newAssignment == null) {
                newAssignment = oldAssignment;
            }
            targetAssignment.put(memberId, new MemberAssignmentImpl(newAssignment));
        }
    }

    /**
     * A class that holds the state for the balancing process.
     * <p/>
     * Keeps track of two ranges of members: the members with the least number of assigned
     * partitions and the members with the most number of assigned partitions. Within each range,
     * all members start with the same number of assigned partitions.
     */
    private final class MemberAssignmentBalancer {
        private final int[] memberTargetAssignmentSizes;

        /**
         * The members to balance, sorted by number of assigned partitions.
         * <p/>
         * Can be visualized like so:
         * <pre>
         *           ^
         *           |          #
         * partition |        ###
         * count     |        ###
         *           | ##########
         *           +----------->
         *             members
         * </pre>
         */
        private final List<Integer> sortedMembers;

        /**
         * [0, leastLoadedRangeEnd) represents the range of members with the smallest partition
         * count.
         */
        private int leastLoadedRangeEnd = 0; // exclusive

        /**
         * The partition count of each member in the least loaded range before we start assigning
         * partitions to them.
         */
        private int leastLoadedRangePartitionCount = -1;

        /**
         * The index of the next element in sortedMembers to which to assign the next partition.
         */
        private int nextLeastLoadedMember = 0;

        /**
         * [mostLoadedRangeStart, mostLoadedRangeEnd) is the range of members with the largest
         * partition count.
         */
        private int mostLoadedRangeStart = 0; // inclusive

        /**
         * [mostLoadedRangeStart, mostLoadedRangeEnd) is the range of members with the largest
         * partition count.
         * <p/>
         * mostLoadedRangeEnd is not simply the end of the member list, since we need to be able to
         * exclude heavily loaded members which do not have any partitions to give up.
         */
        private int mostLoadedRangeEnd = 0; // inclusive

        /**
         * The partition count of each member in the most loaded range before we start unassigning
         * partitions from them.
         */
        private int mostLoadedRangePartitionCount = 1;

        /**
         * The index of the next element in sortedMembers from which to unassign the next partition.
         */
        private int nextMostLoadedMember = -1;

        // The ranges can be visualized like so:
        //
        //                         most
        //                         loaded
        //           ^ least       range
        //           | loaded               #
        //           | range      [##<-   ]##
        // partition |          ##[#######]##
        // count     | [...->  ]##[#######]##
        //           | [#######]##[#######]##
        //           +----------------------->
        //             members
        //                                 ^^
        //                                 These members have no partitions to give up.
        //
        // The least loaded range expands rightwards and the most loaded range expands leftwards.
        // Note that we are not done once the ranges touch, but rather when the partition counts in
        // the ranges differ by one or less.

        public MemberAssignmentBalancer() {
            this.memberTargetAssignmentSizes = UniformHeterogeneousAssignmentBuilder.this.memberTargetAssignmentSizes;
            this.sortedMembers = new ArrayList<>(memberTargetAssignmentSizes.length);
        }

        /**
         * Resets the state of the balancer for a new set of members.
         *
         * @param members The members to balance.
         * @return The difference in partition counts between the most and least loaded members.
         */
        public int initialize(List<Integer> members) {
            if (members.isEmpty()) {
                // nextLeastLoadedMember needs at least one member.
                throw new IllegalArgumentException("Cannot balance an empty subscriber list.");
            }

            // Build a sorted list of subscribers, where members with the smallest partition count
            // appear first.
            sortedMembers.clear();
            sortedMembers.addAll(members);
            sortedMembers.sort(memberComparator);

            leastLoadedRangeEnd = 0; // exclusive
            leastLoadedRangePartitionCount = memberTargetAssignmentSizes[sortedMembers.get(0)] - 1;
            nextLeastLoadedMember = 0;

            mostLoadedRangeStart = sortedMembers.size(); // inclusive
            mostLoadedRangeEnd = sortedMembers.size(); // exclusive
            mostLoadedRangePartitionCount = memberTargetAssignmentSizes[sortedMembers.get(sortedMembers.size() - 1)] + 1;
            nextMostLoadedMember = sortedMembers.size() - 1;

            return memberTargetAssignmentSizes[sortedMembers.get(sortedMembers.size() - 1)] -
                   memberTargetAssignmentSizes[sortedMembers.get(0)];
        }

        /**
         * Gets the least loaded member to which to assign a partition.
         * <p/>
         * Advances the state of the balancer by assuming that an additional partition has been
         * assigned to the returned member.
         * <p/>
         * Assumes that there is at least one member.
         *
         * @return The least loaded member to which to assign a partition
         */
        public int nextLeastLoadedMember() {
            // To generate a balanced assignment, we assign partitions to members in the
            // [0, leastLoadedRangeEnd) range first. Once each member in the range has received a
            // partition, the partition count of the range rises by one and we can try to expand it.
            //                                     Range full,             Range full,
            //                                     bump level by one       bump level by one,
            //                                     can't expand range.     expand range.
            //           ^                       ^                       ^
            //           |            #          |            #          | [..->     ]#
            // partition |          ###    ->    | [..->   ]###    ->    | [.......##]#
            // count     | [..->   ]###          | [.......]###          | [.......##]#
            //           | [#######]###          | [#######]###          | [#########]#
            //           +------------->         +------------->         +------------>
            //             members                members                members

            if (nextLeastLoadedMember >= leastLoadedRangeEnd) {
                // We've hit the end of the range. Bump its level and try to expand it.
                leastLoadedRangePartitionCount++;

                // Expand the range.
                while (leastLoadedRangeEnd < sortedMembers.size() &&
                    memberTargetAssignmentSizes[sortedMembers.get(leastLoadedRangeEnd)] == leastLoadedRangePartitionCount) {
                    leastLoadedRangeEnd++;
                }

                // Reset the pointer to the start of the range of members with identical
                // partition counts.
                nextLeastLoadedMember = 0;
            }

            int leastLoadedMemberIndex = sortedMembers.get(nextLeastLoadedMember);
            nextLeastLoadedMember++;

            return leastLoadedMemberIndex;
        }

        /**
         * Gets the most loaded member from which to reassign a partition.
         * <p/>
         * Advances the state of the balancer by assuming that a partition has been unassigned from
         * the returned member.
         *
         * @return The most loaded member from which to reassign a partition, or -1 if no such
         *         member exists.
         */
        public int nextMostLoadedMember() {
            if (nextMostLoadedMember < mostLoadedRangeStart) {
                if (mostLoadedRangeEnd <= mostLoadedRangeStart &&
                    mostLoadedRangeStart > 0) {
                    // The range is empty due to calls to excludeMostLoadedMember(). We risk not
                    // expanding the range below and returning a member outside the range. Ensure
                    // that we always expand the range below by resetting the partition count.
                    mostLoadedRangePartitionCount = memberTargetAssignmentSizes[sortedMembers.get(mostLoadedRangeStart - 1)];
                } else {
                    mostLoadedRangePartitionCount--;
                }

                // Expand the range.
                while (mostLoadedRangeStart > 0 &&
                    memberTargetAssignmentSizes[sortedMembers.get(mostLoadedRangeStart - 1)] == mostLoadedRangePartitionCount) {
                    mostLoadedRangeStart--;
                }

                // Reset the pointer to the end of the range of members with identical partition
                // counts.
                nextMostLoadedMember = mostLoadedRangeEnd - 1;
            }

            if (nextMostLoadedMember < 0) {
                // The range is empty due to excludeMostLoadedMember() calls and there are no more
                // members that can give up partitions.
                return -1;
            }

            int mostLoadedMemberIndex = sortedMembers.get(nextMostLoadedMember);
            nextMostLoadedMember--;

            return mostLoadedMemberIndex;
        }

        /**
         * Excludes the last member returned from nextMostLoadedMember from the most loaded range.
         *
         * Must not be called if nextMostLoadedMember has not been called yet or returned -1.
         */
        public void excludeMostLoadedMember() {
            // Kick the member out of the most loaded range by swapping with the member at the end
            // of the range and contracting the end of the range.
            int mostLoadedMemberIndex = sortedMembers.get(nextMostLoadedMember + 1);
            sortedMembers.set(nextMostLoadedMember + 1, sortedMembers.get(mostLoadedRangeEnd - 1));
            sortedMembers.set(mostLoadedRangeEnd - 1, mostLoadedMemberIndex);
            mostLoadedRangeEnd--;
        }

        /**
         * Checks whether the members are balanced based on the partition counts of the least and
         * most loaded ranges.
         */
        public boolean isBalanced() {
            return mostLoadedRangePartitionCount - leastLoadedRangePartitionCount <= 1;
        }
    }

    /**
     * Allocates the remaining unassigned partitions to members in a balanced manner.
     * <ol>
     *   <li>Topics are sorted to maximize the probability of a balanced assignment.</li>
     *   <li>Unassigned partitions within each topic are distributed to the least loaded
     *       subscribers, repeatedly.</li>
     * </ol>
     *
     * @param partitions        The partitions to be assigned.
     */
    private void assignRemainingPartitions(Map<Uuid, List<Integer>> partitions) {
        List<Uuid> sortedTopicIds = sortTopicIds(partitions.keySet());

        MemberAssignmentBalancer memberAssignmentBalancer = new MemberAssignmentBalancer();

        for (Uuid topicId : sortedTopicIds) {
            memberAssignmentBalancer.initialize(topicSubscribers.get(topicId));

            for (int partition : partitions.get(topicId)) {
                int leastLoadedMemberIndex = memberAssignmentBalancer.nextLeastLoadedMember();
                assignPartition(topicId, partition, leastLoadedMemberIndex);
            }
        }
    }

    /**
     * If a topic has two or more potential members it is subject to reassignment.
     *
     * @return true if the topic can participate in reassignment, false otherwise.
     */
    private boolean canTopicParticipateInReassignment(Uuid topicId) {
        return topicSubscribers.get(topicId).size() >= 2;
    }

    /**
     * Balance the current assignment after the initial round of assignments have completed.
     */
    private void balance() {
        Set<Uuid> topicIds = new HashSet<>(subscribedTopicIds);

        // Narrow down the reassignment scope to only those topics that can actually be reassigned.
        for (Uuid topicId : subscribedTopicIds) {
            if (!canTopicParticipateInReassignment(topicId)) {
                topicIds.remove(topicId);
            }
        }

        // If all the partitions are fixed i.e. unassigned partitions is empty there is no point of re-balancing.
        if (!topicIds.isEmpty()) balanceTopics(topicIds);
    }

    /**
     * Performs reassignments of partitions to balance the load across members.
     * This method iteratively reassigns partitions until no further moves can improve the balance.
     * <p/>
     * The method loops over the topics repeatedly until an entire loop around the topics has
     * completed without any reassignments, or we hit an iteration limit.
     * <p/>
     * This method produces perfectly balanced assignments when all subscribers of a topic have the
     * same subscriptions. However, when subscribers have overlapping, non-identical subscriptions,
     * the method produces almost-balanced assignments, assuming the iteration limit is not hit.
     * eg. if there are three members and two topics and members 1 and 2 are subscribed to the first
     *     topic and members 2 and 3 are subscribed to the second topic, we can end up with an
     *     assignment like:
     * <ul>
     *   <li>Member 1: 9 partitions</li>
     *   <li>Member 2: 10 partitions</li>
     *   <li>Member 3: 11 partitions</li>
     * </ul>
     *
     * In this assignment, the subscribers of the first topic have a difference in partitions of 1,
     * so the topic is considered balanced. The same applies to the second topic. However, balance
     * can be improved by moving a partition from the second topic from member 3 to member 2 and a
     * partition from the first topic from member 2 to member 1.
     *
     * @param topicIds          The topics to consider for reassignment. These topics must have at
     *                          least two subscribers.
     */
    private void balanceTopics(Collection<Uuid> topicIds) {
        List<Uuid> sortedTopicIds = sortTopicIds(topicIds);
        // The index of the last topic in sortedTopicIds that was rebalanced.
        // Used to decide when to exit early.
        int lastRebalanceTopicIndex = -1;

        MemberAssignmentBalancer memberAssignmentBalancer = new MemberAssignmentBalancer();

        // An array of partitions, with partitions owned by the same member grouped together.
        List<Integer> partitions = new ArrayList<>();

        // The ranges in the partitions list assigned to members.
        // Maintaining these ranges allows for constant time, deterministic picking of partitions
        // owned by a given member.
        Map<Integer, Integer> startPartitionIndices = new HashMap<>();
        Map<Integer, Integer> endPartitionIndices = new HashMap<>(); // exclusive

        // Repeat reassignment until no partition can be moved to improve the balance or we hit an
        // iteration limit.
        for (int i = 0; i < MAX_ITERATION_COUNT; i++) {
            for (int topicIndex = 0; topicIndex < sortedTopicIds.size(); topicIndex++) {
                if (topicIndex == lastRebalanceTopicIndex) {
                    // The last rebalanced topic was this one, which means we've gone through all
                    // the topics and didn't perform any additional rebalancing. Don't bother trying
                    // to rebalance this topic again and exit.
                    return;
                }

                Uuid topicId = sortedTopicIds.get(topicIndex);

                int reassignedPartitionCount = balanceTopic(
                    topicId,
                    memberAssignmentBalancer,
                    partitions,
                    startPartitionIndices,
                    endPartitionIndices
                );

                if (reassignedPartitionCount > 0 ||
                    lastRebalanceTopicIndex == -1) {
                    lastRebalanceTopicIndex = topicIndex;
                }
            }
        }
    }

    /**
     * Performs reassignments of a topic's partitions to balance the load across its subscribers.
     *
     * @param topicId                  The topic to consider for reassignment. The topic must have
     *                                 at least two subscribers.
     * @param memberAssignmentBalancer A MemberAssignmentBalancer to hold the state of the balancing
     *                                 algorithm.
     *                                 subscribers in ascending order of total assigned partitions.
     * @param partitions               A list for the balancing algorithm to store the topic's
     *                                 partitions, with partitions owned by the same member grouped
     *                                 together.
     * @param startPartitionIndices    A map for the balancing algorithm to store the ranges in the
     *                                 partitions list assigned to members. These ranges allows for
     *                                 constant time, deterministic picking of partitions owned by a
     *                                 given member.
     * @param endPartitionIndices      A map for the balancing algorithm to store the ranges in the
     *                                 partitions list assigned to members. These ranges allows for
     *                                 constant time, deterministic picking of partitions owned by a
     *                                 given member.
     * @return the number of partitions reassigned.
     */
    private int balanceTopic(
        Uuid topicId,
        MemberAssignmentBalancer memberAssignmentBalancer,
        List<Integer> partitions,
        Map<Integer, Integer> startPartitionIndices,
        Map<Integer, Integer> endPartitionIndices // exclusive
    ) {
        int reassignedPartitionCount = 0;

        int numPartitions = subscribedTopicDescriber.numPartitions(topicId);

        int[] partitionOwners = targetAssignmentPartitionOwners.get(topicId);

        // Next, try to reassign partitions from the most loaded subscribers to the least loaded
        // subscribers. We use a similar approach to assignRemainingPartitions, except instead
        // we are reassigning partitions from the most loaded subscribers to the least loaded
        // subscribers. Once the difference in partition counts between the ranges is one or less,
        // we are done, since reassigning partitions would not improve balance.
        int imbalance = memberAssignmentBalancer.initialize(topicSubscribers.get(topicId));
        if (imbalance <= 1) {
            // The topic is already balanced.
            return reassignedPartitionCount;
        }

        // Initialize the array of partitions, with partitions owned by the same member grouped
        // together.
        partitions.clear();
        for (int partition = 0; partition < numPartitions; partition++) {
            partitions.add(partition);
        }
        partitions.sort(
            Comparator
                .comparingInt((Integer partition) -> partitionOwners[partition])
                .thenComparingInt(partition -> partition)
        );

        // Initialize the ranges in the partitions list owned by members.
        startPartitionIndices.clear();
        endPartitionIndices.clear();

        for (int i = 0; i < numPartitions; i++) {
            int partition = partitions.get(i);
            int ownerIndex = partitionOwners[partition];
            startPartitionIndices.putIfAbsent(ownerIndex, i);
            endPartitionIndices.put(ownerIndex, i + 1); // endPartitionIndices is exclusive
        }

        // Redistribute partitions from the most loaded subscriber to the least loaded subscriber
        // until the topic's subscribers are balanced.
        while (!memberAssignmentBalancer.isBalanced()) {
            // NB: The condition above does not do much. This loop will terminate due to the checks
            //     inside instead.

            // First, choose a member from the most loaded range to reassign a partition from.

            // Loop until we find a member that has partitions to give up.
            int mostLoadedMemberIndex = -1;
            while (true) {
                mostLoadedMemberIndex = memberAssignmentBalancer.nextMostLoadedMember();

                if (mostLoadedMemberIndex == -1) {
                    // There are no more members with partitions to give up.
                    // We need to break out of two loops here.
                    break;
                }

                if (!endPartitionIndices.containsKey(mostLoadedMemberIndex) ||
                    endPartitionIndices.get(mostLoadedMemberIndex) - startPartitionIndices.get(mostLoadedMemberIndex) <= 0) {
                    memberAssignmentBalancer.excludeMostLoadedMember();
                    continue;
                }

                break;
            }

            if (mostLoadedMemberIndex == -1) {
                // There are no more members with partitions to give up.
                break;
            }

            // We've picked a member. Now pick its last partition to reassign.
            int partition = partitions.get(endPartitionIndices.get(mostLoadedMemberIndex) - 1);
            endPartitionIndices.put(mostLoadedMemberIndex, endPartitionIndices.get(mostLoadedMemberIndex) - 1);

            // Find the least loaded subscriber.
            int leastLoadedMemberIndex = memberAssignmentBalancer.nextLeastLoadedMember();

            // If the most and least loaded subscribers came from member ranges with partition
            // counts that differ by one or less, then the members are balanced and we are done.
            if (memberAssignmentBalancer.isBalanced()) {
                // The topic is balanced.
                break;
            }

            // Reassign the partition.
            assignPartition(topicId, partition, leastLoadedMemberIndex);
            reassignedPartitionCount++;
        }

        return reassignedPartitionCount;
    }

    /**
     * Computes the set of unassigned partitions, based on targetAssignmentPartitionOwners.
     */
    private Map<Uuid, List<Integer>> computeUnassignedPartitions() {
        Map<Uuid, List<Integer>> unassignedPartitions = new HashMap<>();

        for (Uuid topicId : subscribedTopicIds) {
            List<Integer> topicUnassignedPartitions = new ArrayList<>();
            int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            for (int partition = 0; partition < numPartitions; partition++) {
                if (targetAssignmentPartitionOwners.get(topicId)[partition] == -1) {
                    topicUnassignedPartitions.add(partition);
                }
            }

            if (!topicUnassignedPartitions.isEmpty()) {
                unassignedPartitions.put(topicId, topicUnassignedPartitions);
            }
        }

        return unassignedPartitions;
    }

    /**
     * Assigns or reassigns the given partition to a member.
     *
     * @param topicId           The topic containing the partition to be assigned or reassigned.
     * @param partition         The partition to be assigned or reassigned.
     * @param memberIndex       The index of the member to which the partition should be assigned.
     */
    private void assignPartition(Uuid topicId, int partition, int memberIndex) {
        int oldMemberIndex = targetAssignmentPartitionOwners.get(topicId)[partition];

        if (oldMemberIndex != -1) {
            removePartitionFromTargetAssignment(topicId, partition, oldMemberIndex);
        }

        addPartitionToTargetAssignment(topicId, partition, memberIndex);
    }

    /**
     * Assigns a partition to a member and updates the current assignment size.
     *
     * @param topicId               The topic containing the partition to be assigned.
     * @param partition             The partition to be assigned.
     * @param memberIndex           Member that the partition needs to be added to.
     */
    private void addPartitionToTargetAssignment(
        Uuid topicId,
        int partition,
        int memberIndex
    ) {
        String memberId = memberIds.get(memberIndex);
        Map<Uuid, Set<Integer>> assignment = targetAssignment.get(memberId).partitions();
        if (AssignorHelpers.isImmutableMap(assignment)) {
            assignment = AssignorHelpers.deepCopyAssignment(assignment);
            targetAssignment.put(memberId, new MemberAssignmentImpl(assignment));
        }
        assignment
            .computeIfAbsent(topicId, __ -> {
                int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
                int numSubscribers = topicSubscribers.get(topicId).size();
                int estimatedPartitionsPerSubscriber = (numPartitions + numSubscribers - 1) / numSubscribers;
                return new HashSet<>((int) ((estimatedPartitionsPerSubscriber / 0.75f) + 1));
            })
            .add(partition);

        targetAssignmentPartitionOwners.get(topicId)[partition] = memberIndex;

        memberTargetAssignmentSizes[memberIndex]++;
    }

    /**
     * Revokes the partition from a member and updates the current target assignment size.
     *
     * @param topicId               The topic containing the partition to be revoked.
     * @param partition             The partition to be revoked.
     * @param memberIndex           Member that the partition needs to be revoked from.
     */
    private void removePartitionFromTargetAssignment(
        Uuid topicId,
        int partition,
        int memberIndex
    ) {
        String memberId = memberIds.get(memberIndex);
        Map<Uuid, Set<Integer>> assignment = targetAssignment.get(memberId).partitions();
        if (AssignorHelpers.isImmutableMap(assignment)) {
            assignment = AssignorHelpers.deepCopyAssignment(assignment);
            targetAssignment.put(memberId, new MemberAssignmentImpl(assignment));
        }
        Set<Integer> partitionsSet = assignment.get(topicId);
        // Remove the partition from the assignment, if there are no more partitions from a particular topic,
        // remove the topic from the assignment as well.
        if (partitionsSet != null) {
            partitionsSet.remove(partition);
            if (partitionsSet.isEmpty()) {
                assignment.remove(topicId);
            }
        }

        targetAssignmentPartitionOwners.get(topicId)[partition] = -1;

        memberTargetAssignmentSizes[memberIndex]--;
    }
}
