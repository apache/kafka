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

import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntPair;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.LongHeap;

import java.util.Arrays;

/**
 * The second step deciding the {@link Quotas}: assigns the extra partitions that no owner kept,
 * one at a time, to subscribers not having one of the topic yet, among the ones with the smallest
 * assignments.
 *
 * <p>The topics with the fewest subscribers go first, so that they get the smallest of their few
 * candidates: when the subscriptions are nested, this alone balances the assignment sizes. The
 * extra partition goes to the cohort holding the smallest assignment, and among its members those
 * with the smallest assignment are served round robin in member order. The member with the
 * smallest assignment may already have an extra partition of the topic, and another member of the
 * cohort then gets it: the balance step moves it if it has to. Without owners, a homogeneous group
 * is served round robin.
 *
 * <p>On exit every topic has exactly its number of extra partitions, on distinct subscribers.
 */
final class AssignRemainingExtraPartitions {
    /**
     * The state of a cohort while the extra partitions are handed out.
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
         * The lowest number of extra partitions of the members.
         */
        int lowest;

        /**
         * How many members have the lowest number of extra partitions.
         */
        int atLowest;

        /**
         * How many members have an extra partition of the topic being handed out.
         */
        int holders;

        CohortState(int[] members) {
            this.members = members;
        }
    }

    /**
     * The members and topics of the group.
     */
    private final IndexedGroup group;

    /**
     * The quotas, which the step completes.
     */
    private final Quotas quotas;

    /**
     * The cohorts of the topic being handed out which can still take an extra partition of it, by
     * increasing smallest assignment size among their members, whether that member has an extra
     * partition of the topic or not, then in cohort order, see {@link #sizeAndCohort(int)}.
     */
    private LongHeap candidates;

    /**
     * Per cohort, its state.
     */
    private CohortState[] cohortStates;

    /**
     * Per member, whether it has an extra partition of the topic being handed out.
     */
    private boolean[] hasExtra;

    /**
     * @param group  The members and topics of the group.
     * @param quotas The quotas, as the first step left them, which the step completes.
     */
    AssignRemainingExtraPartitions(IndexedGroup group, Quotas quotas) {
        this.group = group;
        this.quotas = quotas;
    }

    /**
     * @return The key of the cohort in the heap of candidates: the pair of the smallest assignment
     *         size among its members and the cohort, so that the heap orders the cohorts by that
     *         assignment size, then cohort order. That assignment size does not change while the
     *         cohort is a candidate, since only the members of a polled cohort get an extra
     *         partition.
     */
    private long sizeAndCohort(int cohort) {
        return IntPair.pack(quotas.minAssignmentSize(cohort) + cohortStates[cohort].lowest, cohort);
    }

    /**
     * Hands out the extra partitions that no owner kept. When the owners kept them all, as in a
     * stable group, returns at once without allocating anything.
     */
    void run() {
        if (!hasExtraPartitionsToHandOut()) {
            return;
        }
        cohortStates = new CohortState[group.cohortCount()];
        for (int cohort = 0; cohort < cohortStates.length; cohort++) {
            cohortStates[cohort] = new CohortState(group.membersOf(cohort));
            findLowest(cohortStates[cohort]);
        }
        hasExtra = new boolean[group.memberCount()];
        candidates = new LongHeap(16);
        for (int topic : topicsToHandOut()) {
            handOutTopic(topic);
        }
    }

    /**
     * @return {@code true} if a topic has extra partitions that no owner kept.
     */
    private boolean hasExtraPartitionsToHandOut() {
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (quotas.membersWithExtraPartition(topic).size() < quotas.extraPartitions(topic)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Hands out the extra partitions of the topic that no owner kept, one at a time, each to the
     * next member of the cohort holding the smallest assignment among those still having a member
     * without one.
     */
    private void handOutTopic(int topic) {
        // The members keeping an extra partition of the topic, which grows as the topic is handed
        // out.
        var members = quotas.membersWithExtraPartition(topic);
        for (int i = 0; i < members.size(); i++) {
            hasExtra[members.get(i)] = true;
            cohortStates[group.cohortOf(members.get(i))].holders++;
        }
        // The cohorts of the topic with a member still without an extra partition of it, the only
        // ones that can take one.
        candidates.clear();
        for (int cohort : group.cohortsOf(topic)) {
            var state = cohortStates[cohort];
            if (state.holders < state.members.length) {
                candidates.add(sizeAndCohort(cohort));
            }
        }
        // The extra partitions that no owner kept, counted once since the members grow while they
        // are handed out. There is at least one: topicsToHandOut only lists the topics with some.
        for (int toHandOut = quotas.extraPartitions(topic) - members.size(); toHandOut > 0; toHandOut--) {
            int cohort = IntPair.low(candidates.poll());
            var state = cohortStates[cohort];
            int member = nextMember(state);
            int count = quotas.extraPartitionCount(member);
            quotas.giveExtraPartition(member, topic);
            hasExtra[member] = true;
            // The last member of the cohort at the lowest number of extra partitions moves up.
            if (count == state.lowest && --state.atLowest == 0) {
                findLowest(state);
            }
            if (++state.holders < state.members.length) {
                candidates.add(sizeAndCohort(cohort));
            }
        }
        // Resets the scratch state for the next topic, including the members just served.
        for (int i = 0; i < members.size(); i++) {
            hasExtra[members.get(i)] = false;
            cohortStates[group.cohortOf(members.get(i))].holders = 0;
        }
    }

    /**
     * @return The topics with extra partitions nobody kept, by increasing number of subscribers,
     *         then topic order.
     */
    private int[] topicsToHandOut() {
        // Pairs of the number of subscribers and the topic, which sort by subscribers, then topic.
        var keys = new long[group.topicCount()];
        int count = 0;
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (quotas.extraPartitions(topic) > quotas.membersWithExtraPartition(topic).size()) {
                keys[count++] = IntPair.pack(group.subscriberCount(topic), topic);
            }
        }
        Arrays.sort(keys, 0, count);
        var topics = new int[count];
        for (int i = 0; i < count; i++) {
            topics[i] = IntPair.low(keys[i]);
        }
        return topics;
    }

    /**
     * Finds the lowest number of extra partitions of the members of the cohort, and how many of
     * them have it.
     */
    private void findLowest(CohortState state) {
        int min = Integer.MAX_VALUE;
        int count = 0;
        for (int member : state.members) {
            int extras = quotas.extraPartitionCount(member);
            if (extras < min) {
                min = extras;
                count = 1;
            } else if (extras == min) {
                count++;
            }
        }
        state.lowest = min;
        state.atLowest = count;
    }

    /**
     * @return The next member of the cohort, in round robin order from the cursor, having the
     *         lowest number of extra partitions among its members and no extra partition of the
     *         topic. If every member having the lowest number has one, the first member without
     *         one having the fewest extra partitions.
     */
    private int nextMember(CohortState state) {
        var members = state.members;
        for (int step = 0; step < members.length; step++) {
            int index = (state.cursor + step) % members.length;
            int member = members[index];
            if (quotas.extraPartitionCount(member) == state.lowest && !hasExtra[member]) {
                state.cursor = (index + 1) % members.length;
                return member;
            }
        }
        // Every member at the lowest number has an extra partition of the topic.
        int best = -1;
        for (int member : members) {
            if (!hasExtra[member] && (best < 0 || quotas.extraPartitionCount(member) < quotas.extraPartitionCount(best))) {
                best = member;
            }
        }
        return best;
    }
}
