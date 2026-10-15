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
package org.apache.kafka.coordinator.group.assignor.uniform2.steps;

import org.apache.kafka.coordinator.group.assignor.uniform2.IndexedGroup;
import org.apache.kafka.coordinator.group.assignor.uniform2.Quotas;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntPair;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.LongHeap;

import java.util.Arrays;

/**
 * The second step deciding the {@link Quotas}: assigns the extra partitions that no owner kept,
 * one at a time, to subscribers not having one of the topic yet, among the ones with the smallest
 * assignments.
 *
 * <p>The topics with the fewest subscribers go first, so that they can still choose the smallest
 * of their few subscribers. When the subscriptions of the cohorts are subsets of each other, this
 * leads to more balanced assignment sizes. The extra partition goes to the cohort holding the
 * smallest assignment, and among its members those with the smallest assignment are served round
 * robin in member order. The member with the smallest assignment may already have an extra
 * partition of the topic, and another member of the cohort then gets it. The balance step moves it
 * later if it has to. With an empty current assignment, a homogeneous group is served round robin.
 *
 * <p>On exit, every extra partition of every topic is assigned, each to a distinct subscriber.
 */
public final class AssignRemainingExtraPartitions {
    /**
     * The state of a cohort while the extra partitions are assigned.
     */
    private static final class CohortState {
        /**
         * The members of the cohort.
         */
        final int[] members;

        /**
         * The position in the members of the next member to serve, round robin.
         */
        int cursor;

        /**
         * The lowest number of extra partitions among the members.
         */
        int lowestExtraPartitions;

        /**
         * How many members have the lowest number of extra partitions.
         */
        int membersWithLowestExtraPartitions;

        /**
         * How many members have an extra partition of the topic being assigned.
         */
        int membersWithExtraPartitionOfTopic;

        CohortState(int[] members) {
            this.members = members;
        }
    }

    /**
     * The members and topics of the group.
     */
    private final IndexedGroup group;

    /**
     * The quotas, which the step updates.
     */
    private final Quotas quotas;

    /**
     * The cohorts that can still receive an extra partition of the topic being assigned. They are
     * ordered by the smallest assignment size among their members, whether that member has an
     * extra partition of the topic or not, then by cohort order, see {@link #sizeAndCohort(int)}.
     */
    private LongHeap receivingCohorts;

    /**
     * Per cohort, its state.
     */
    private CohortState[] cohortStates;

    /**
     * Per member, whether it has an extra partition of the topic being assigned.
     */
    private boolean[] hasExtraPartitionOfTopic;

    /**
     * @param group  The members and topics of the group.
     * @param quotas The quotas, as the first step left them.
     */
    public AssignRemainingExtraPartitions(IndexedGroup group, Quotas quotas) {
        this.group = group;
        this.quotas = quotas;
    }

    /**
     * @return The key of the cohort in {@link #receivingCohorts}: the pair of the smallest
     *         assignment size among its members and the cohort, so that the cohorts are ordered by
     *         that assignment size, then cohort order. That assignment size does not change while
     *         the cohort is in the heap, since only the members of a polled cohort get an extra
     *         partition.
     */
    private long sizeAndCohort(int cohort) {
        return IntPair.pack(quotas.minAssignmentSize(cohort) + cohortStates[cohort].lowestExtraPartitions, cohort);
    }

    /**
     * Assigns the extra partitions that no owner kept. When the owners kept them all, as in a
     * stable group, returns at once without allocating anything.
     */
    public void run() {
        if (!hasRemainingExtraPartitions()) {
            return;
        }
        cohortStates = new CohortState[group.cohortCount()];
        for (int cohort = 0; cohort < cohortStates.length; cohort++) {
            cohortStates[cohort] = new CohortState(group.membersOf(cohort));
            computeLowestExtraPartitions(cohortStates[cohort]);
        }
        hasExtraPartitionOfTopic = new boolean[group.memberCount()];
        receivingCohorts = new LongHeap(16);
        for (int topic : topicsWithRemainingExtraPartitions()) {
            assignRemainingExtraPartitions(topic);
        }
    }

    /**
     * @return {@code true} if a topic has extra partitions that no owner kept.
     */
    private boolean hasRemainingExtraPartitions() {
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (quotas.membersWithExtraPartition(topic).size() < quotas.extraPartitions(topic)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Assigns the extra partitions of the topic that no owner kept, one at a time. Each goes to the
     * next member of the cohort holding the smallest assignment, among the cohorts still having a
     * member without one.
     */
    private void assignRemainingExtraPartitions(int topic) {
        // The members with an extra partition of the topic. The list grows as they are assigned.
        var members = quotas.membersWithExtraPartition(topic);
        for (int i = 0; i < members.size(); i++) {
            hasExtraPartitionOfTopic[members.get(i)] = true;
            cohortStates[group.cohortOf(members.get(i))].membersWithExtraPartitionOfTopic++;
        }
        // The cohorts of the topic with a member still without an extra partition of it, the only
        // ones that can receive one.
        receivingCohorts.clear();
        for (int cohort : group.cohortsOf(topic)) {
            var state = cohortStates[cohort];
            if (state.membersWithExtraPartitionOfTopic < state.members.length) {
                receivingCohorts.add(sizeAndCohort(cohort));
            }
        }
        // Computed before the loop, which adds to the members with an extra partition of the
        // topic. There is at least one, since only the topics with some are assigned.
        int remainingExtraPartitions = quotas.extraPartitions(topic) - members.size();
        for (int i = 0; i < remainingExtraPartitions; i++) {
            int cohort = IntPair.low(receivingCohorts.poll());
            var state = cohortStates[cohort];
            int member = nextMember(state);
            int extraPartitionCount = quotas.extraPartitionCount(member);
            quotas.giveExtraPartition(member, topic);
            hasExtraPartitionOfTopic[member] = true;
            // The last member of the cohort with the lowest number of extra partitions moves up.
            if (extraPartitionCount == state.lowestExtraPartitions && --state.membersWithLowestExtraPartitions == 0) {
                computeLowestExtraPartitions(state);
            }
            if (++state.membersWithExtraPartitionOfTopic < state.members.length) {
                receivingCohorts.add(sizeAndCohort(cohort));
            }
        }
        // Resets the state of the topic for the next one, including the members just assigned.
        for (int i = 0; i < members.size(); i++) {
            hasExtraPartitionOfTopic[members.get(i)] = false;
            cohortStates[group.cohortOf(members.get(i))].membersWithExtraPartitionOfTopic = 0;
        }
    }

    /**
     * @return The topics with extra partitions that no owner kept, by increasing number of
     *         subscribers, then topic order.
     */
    private int[] topicsWithRemainingExtraPartitions() {
        // Pairs of the number of subscribers and the topic, which sort by subscribers, then topic.
        var subscribersAndTopics = new long[group.topicCount()];
        int count = 0;
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (quotas.extraPartitions(topic) > quotas.membersWithExtraPartition(topic).size()) {
                subscribersAndTopics[count++] = IntPair.pack(group.subscriberCount(topic), topic);
            }
        }
        Arrays.sort(subscribersAndTopics, 0, count);
        var topics = new int[count];
        for (int i = 0; i < count; i++) {
            topics[i] = IntPair.low(subscribersAndTopics[i]);
        }
        return topics;
    }

    /**
     * Computes the lowest number of extra partitions of the members of the cohort, and how many
     * of them have it.
     */
    private void computeLowestExtraPartitions(CohortState state) {
        int lowest = Integer.MAX_VALUE;
        int count = 0;
        for (int member : state.members) {
            int extraPartitionCount = quotas.extraPartitionCount(member);
            if (extraPartitionCount < lowest) {
                lowest = extraPartitionCount;
                count = 1;
            } else if (extraPartitionCount == lowest) {
                count++;
            }
        }
        state.lowestExtraPartitions = lowest;
        state.membersWithLowestExtraPartitions = count;
    }

    /**
     * @return The next member of the cohort, in round robin order from the cursor, having the
     *         lowest number of extra partitions among its members and no extra partition of the
     *         topic. If every member having the lowest number of extra partitions already has an
     *         extra partition of the topic, the first member without one having the fewest extra
     *         partitions.
     */
    private int nextMember(CohortState state) {
        var members = state.members;
        for (int step = 0; step < members.length; step++) {
            int index = (state.cursor + step) % members.length;
            int member = members[index];
            if (quotas.extraPartitionCount(member) == state.lowestExtraPartitions && !hasExtraPartitionOfTopic[member]) {
                state.cursor = (index + 1) % members.length;
                return member;
            }
        }
        // Every member with the lowest number of extra partitions already has one of the topic.
        int best = -1;
        for (int member : members) {
            if (!hasExtraPartitionOfTopic[member] &&
                (best < 0 || quotas.extraPartitionCount(member) < quotas.extraPartitionCount(best))) {
                best = member;
            }
        }
        return best;
    }
}
