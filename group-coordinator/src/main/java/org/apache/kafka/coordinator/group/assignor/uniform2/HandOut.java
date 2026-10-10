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

import org.apache.kafka.coordinator.group.assignor.uniform2.util.LongHeap;

import java.util.Arrays;

/**
 * The hand out step of the {@link Shares}: gives the extra partitions that no owner kept, one
 * at a time, to subscribers not having one of the topic yet, among the ones with the smallest
 * assignments.
 *
 * <p>The topics with the fewest subscribers go first, so that they get the smallest of their few
 * candidates: when the subscriptions are nested, this alone balances the sizes. The extra partition
 * goes to the cohort holding the smallest assignment, and among its members those with the smallest
 * assignment are served round robin in member order. The member with the smallest assignment may
 * already have an extra partition of the topic, and another member of the cohort then gets it: the
 * balance step moves it if it has to. Without owners, a homogeneous group is served round robin.
 *
 * <p>On exit every topic has exactly its number of extra partitions, on distinct subscribers.
 */
final class HandOut {
    /**
     * The members and topics of the group.
     */
    private final GroupModel group;

    /**
     * The shares, which the step completes.
     */
    private final Shares shares;

    /**
     * The cohorts of the topic being handed out which can still take an extra partition of it,
     * by increasing size of the smallest assignment among their members, whether that member has
     * an extra partition of the topic or not, then in cohort order, see
     * {@link #sizeAndCohort(int)}.
     */
    private LongHeap candidates;

    /**
     * Per cohort, the position in its members of the next member to serve, round robin.
     */
    private int[] cursors;

    /**
     * Per cohort, the lowest number of extra partitions of its members.
     */
    private int[] lowest;

    /**
     * Per cohort, how many of its members have the lowest number of extra partitions.
     */
    private int[] atLowest;

    /**
     * Per cohort, how many of its members have an extra partition of the topic being handed out.
     */
    private int[] holders;

    /**
     * Per member, whether it has an extra partition of the topic being handed out.
     */
    private boolean[] hasExtra;

    /**
     * @param group  The members and topics of the group.
     * @param shares The shares, as the keep step left them, which the step completes.
     */
    HandOut(GroupModel group, Shares shares) {
        this.group = group;
        this.shares = shares;
    }

    /**
     * @return The key of the cohort in the heap of candidates: the size of the smallest assignment
     *         among its members in the high 32 bits of a long and the cohort in the low 32 bits, so
     *         that the heap orders the cohorts by that size, then cohort order. The size does not
     *         change while the cohort is a candidate, since only the members of a polled cohort get
     *         an extra partition.
     */
    private long sizeAndCohort(int cohort) {
        return ((long) smallestSize(cohort) << 32) | cohort;
    }

    /**
     * @return The size of the smallest assignment among the members of the cohort.
     */
    private int smallestSize(int cohort) {
        return shares.cohortBaseSize(cohort) + lowest[cohort];
    }

    /**
     * Hands out the extra partitions that no owner kept. When the owners kept them all, as in a
     * stable group, returns at once without allocating anything.
     */
    void run() {
        if (!hasExtraPartitionsToHandOut()) {
            return;
        }
        int cohortCount = group.cohortCount();
        cursors = new int[cohortCount];
        lowest = new int[cohortCount];
        atLowest = new int[cohortCount];
        holders = new int[cohortCount];
        for (int cohort = 0; cohort < cohortCount; cohort++) {
            findLowest(cohort);
        }
        hasExtra = new boolean[group.memberCount()];
        candidates = new LongHeap(16);
        for (int topic : topicsToHandOut()) {
            handOutTopic(topic);
        }
    }

    /**
     * @return True if a topic has extra partitions that no owner kept.
     */
    private boolean hasExtraPartitionsToHandOut() {
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (shares.membersWithExtra(topic).size() < shares.extraPartitions(topic)) {
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
        var members = shares.membersWithExtra(topic);
        for (int i = 0; i < members.size(); i++) {
            hasExtra[members.get(i)] = true;
            holders[group.cohortOf(members.get(i))]++;
        }
        // The cohorts of the topic with a member still without an extra partition of it, the only
        // ones that can take one.
        candidates.clear();
        for (int cohort : group.cohortsOf(topic)) {
            if (holders[cohort] < group.membersOf(cohort).length) {
                candidates.add(sizeAndCohort(cohort));
            }
        }
        // The extra partitions that no owner kept, counted once since the members grow while they
        // are handed out. There is at least one: topicsToHandOut only lists the topics with some.
        for (int toHandOut = shares.extraPartitions(topic) - members.size(); toHandOut > 0; toHandOut--) {
            int cohort = (int) candidates.poll();
            int member = nextMember(cohort);
            int count = shares.extraCount(member);
            shares.giveExtraPartition(member, topic);
            hasExtra[member] = true;
            // The last member of the cohort at the lowest number of extra partitions moves up.
            if (count == lowest[cohort] && --atLowest[cohort] == 0) {
                findLowest(cohort);
            }
            if (++holders[cohort] < group.membersOf(cohort).length) {
                candidates.add(sizeAndCohort(cohort));
            }
        }
        // Resets the scratch state for the next topic, including the members just served.
        for (int i = 0; i < members.size(); i++) {
            hasExtra[members.get(i)] = false;
            holders[group.cohortOf(members.get(i))] = 0;
        }
    }

    /**
     * @return The topics with extra partitions nobody kept, by increasing number of subscribers,
     *         then topic order.
     */
    private int[] topicsToHandOut() {
        // Sorts the topics as longs, the number of subscribers in the high 32 bits and the topic in
        // the low 32 bits.
        var keys = new long[group.topicCount()];
        int count = 0;
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (shares.extraPartitions(topic) > shares.membersWithExtra(topic).size()) {
                keys[count++] = ((long) group.subscriberCount(topic) << 32) | topic;
            }
        }
        Arrays.sort(keys, 0, count);
        var topics = new int[count];
        for (int i = 0; i < count; i++) {
            topics[i] = (int) keys[i];
        }
        return topics;
    }

    /**
     * Finds the lowest number of extra partitions of the members of the cohort, and how many of
     * them have it.
     */
    private void findLowest(int cohort) {
        int min = Integer.MAX_VALUE;
        int count = 0;
        for (int member : group.membersOf(cohort)) {
            int extras = shares.extraCount(member);
            if (extras < min) {
                min = extras;
                count = 1;
            } else if (extras == min) {
                count++;
            }
        }
        lowest[cohort] = min;
        atLowest[cohort] = count;
    }

    /**
     * @return The next member of the cohort, in round robin order from the cursor, having the
     *         lowest number of extra partitions among its members and no extra partition of the
     *         topic. If every member having the lowest number has one, the first member without
     *         one having the fewest extra partitions.
     */
    private int nextMember(int cohort) {
        var members = group.membersOf(cohort);
        for (int step = 0; step < members.length; step++) {
            int index = (cursors[cohort] + step) % members.length;
            int member = members[index];
            if (shares.extraCount(member) == lowest[cohort] && !hasExtra[member]) {
                cursors[cohort] = (index + 1) % members.length;
                return member;
            }
        }
        // Every member at the lowest number has an extra partition of the topic.
        int best = -1;
        for (int member : members) {
            if (!hasExtra[member] && (best < 0 || shares.extraCount(member) < shares.extraCount(best))) {
                best = member;
            }
        }
        return best;
    }
}
