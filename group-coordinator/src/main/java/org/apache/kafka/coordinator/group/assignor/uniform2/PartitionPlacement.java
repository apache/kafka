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

import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntList;

/**
 * Places the partitions of a topic on its subscribers, given their shares, moving as few
 * partitions as possible.
 *
 * <p>Every owner keeps its partitions up to its share, the lowest partition ids first. The
 * other partitions, those without owner and those beyond the share of their owner, go to the
 * subscribers below their share, in member order, the lowest partition ids first. A partition
 * thus moves only when its owner's share is below what it owns.
 */
final class PartitionPlacement {
    /**
     * The members and topics of the group.
     */
    private final GroupModel group;

    /**
     * The current assignment, which tells the owners of every partition.
     */
    private final CurrentAssignment current;

    /**
     * The shares of the members.
     */
    private final Shares shares;

    /**
     * Per member, how many partitions of the topic being placed it got so far, reset after every
     * topic.
     */
    private final int[] placed;

    /**
     * Per member, whether it gets an extra partition of the topic being placed, reset after every
     * topic.
     */
    private final boolean[] hasExtra;

    /**
     * The partitions of the topic being placed free to go to the members below their share:
     * those without owner and those beyond the share of their owner.
     */
    private final IntList free = new IntList(16);

    /**
     * The subscribers of the topic being placed below their share, in member order.
     */
    private final IntList receivers = new IntList(16);

    /**
     * The members that got partitions of the topic being placed, to reset {@link #placed}.
     */
    private final IntList placedMembers = new IntList(16);

    /**
     * @param group   The members and topics of the group.
     * @param current The current assignment.
     * @param shares  The shares of the members.
     */
    PartitionPlacement(GroupModel group, CurrentAssignment current, Shares shares) {
        this.group = group;
        this.current = current;
        this.shares = shares;
        this.placed = new int[group.memberCount()];
        this.hasExtra = new boolean[group.memberCount()];
    }

    /**
     * Places the partitions of the topic.
     *
     * @param topic      The topic.
     * @param assignment Receives, for every partition of the topic, the member getting it. Its
     *                   length is at least the partition count of the topic.
     * @return True if a partition may move. False guarantees that every partition stays with
     *         its owner, and the caller need not read the assignment.
     */
    boolean place(int topic, int[] assignment) {
        markExtras(topic, true);
        int base = shares.basePartitions(topic);
        var owners = current.owners(topic);
        free.clear();
        for (int partition = 0; partition < group.partitionCount(topic); partition++) {
            int owner = owners == null ? -1 : owners[partition];
            if (owner >= 0 && placed[owner] < share(owner, base)) {
                assignment[partition] = owner;
                addPlaced(owner);
            } else {
                free.add(partition);
            }
        }
        // The shares add up to the partition count: when every partition stays with its owner,
        // every member has its share and nothing moves.
        boolean moves = !free.isEmpty();
        if (moves) {
            collectReceivers(topic, base);
            int next = 0;
            for (int i = 0; i < receivers.size(); i++) {
                int member = receivers.get(i);
                for (int missing = share(member, base) - placed[member]; missing > 0; missing--) {
                    assignment[free.get(next++)] = member;
                }
            }
        }
        reset(topic);
        return moves;
    }

    /**
     * Sets or clears the marks of the members getting an extra partition of the topic.
     */
    private void markExtras(int topic, boolean mark) {
        var members = shares.membersWithExtra(topic);
        for (int i = 0; i < members.size(); i++) {
            hasExtra[members.get(i)] = mark;
        }
    }

    /**
     * @return The share of the member for the topic being placed.
     */
    private int share(int member, int base) {
        return hasExtra[member] ? base + 1 : base;
    }

    /**
     * Counts one more partition placed on the member.
     */
    private void addPlaced(int member) {
        if (placed[member]++ == 0) {
            placedMembers.add(member);
        }
    }

    /**
     * Collects in {@link #receivers} the subscribers of the topic below their share, in member
     * order. Without base partitions, only the members with an extra partition have a share.
     */
    private void collectReceivers(int topic, int base) {
        receivers.clear();
        if (base == 0) {
            var members = shares.membersWithExtra(topic);
            for (int i = 0; i < members.size(); i++) {
                if (placed[members.get(i)] == 0) {
                    receivers.add(members.get(i));
                }
            }
        } else {
            for (int cohort : group.cohortsOf(topic)) {
                for (int member : group.membersOf(cohort)) {
                    if (placed[member] < share(member, base)) {
                        receivers.add(member);
                    }
                }
            }
        }
        // A single cohort lists its members in member order already; the members with an extra
        // partition and the members of several cohorts are not.
        if (base == 0 || group.cohortsOf(topic).length > 1) {
            receivers.sort();
        }
    }

    /**
     * Resets the numbers of partitions placed on the members, after a topic or before placing it
     * again.
     */
    private void clearPlaced() {
        for (int i = 0; i < placedMembers.size(); i++) {
            placed[placedMembers.get(i)] = 0;
        }
        placedMembers.clear();
    }

    /**
     * Resets the scratch state of the members of the topic.
     */
    private void reset(int topic) {
        markExtras(topic, false);
        clearPlaced();
    }
}
