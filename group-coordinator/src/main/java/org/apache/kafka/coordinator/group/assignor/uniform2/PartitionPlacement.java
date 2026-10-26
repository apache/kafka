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
 * Places the partitions of a topic on its subscribers, given their quotas, moving as few
 * partitions as possible. It only decides which member gets every partition; it does not build
 * the assignments of the members.
 *
 * <p>Every owner keeps its partitions with the lowest ids, up to its quota. The other partitions,
 * those without an owner and those beyond the quota of their owner, go to the subscribers below
 * their quota, each taking the lowest ids left until it reaches its quota. The subscribers take
 * them cohort by cohort, in member order within a cohort. With a minimum quota of 0, only the
 * members with an extra partition can be below their quota, and they take them in the order of
 * {@link Quotas#membersWithExtraPartition}. A partition thus moves only when its owner owns more
 * partitions than its quota. Owners keep their lowest ids so that partitions that were
 * consecutive stay consecutive, which lets their sets be {@code RangeSet}s.
 */
final class PartitionPlacement {
    /**
     * The members and topics of the group.
     */
    private final IndexedGroup group;

    /**
     * The current assignment, which tells the owners of every partition.
     */
    private final CurrentAssignment currentAssignment;

    /**
     * The quotas of the members.
     */
    private final Quotas quotas;

    /**
     * Per member, the number of partitions of the topic being placed that it keeps. Reset after
     * every topic.
     */
    private final int[] kept;

    /**
     * Per member, whether it gets an extra partition of the topic being placed. Reset after every
     * topic.
     */
    private final boolean[] hasExtraPartitionOfTopic;

    /**
     * The partitions of the topic being placed that do not stay with their owner, in increasing
     * id order. A partition stays with its owner when it has one and the owner keeps fewer
     * partitions than its quota so far.
     */
    private final IntList movingPartitions = new IntList(16);

    /**
     * The owners keeping partitions of the topic being placed, whose entries of {@link #kept} are
     * reset after the topic.
     */
    private final IntList keepingOwners = new IntList(16);

    /**
     * @param group             The members and topics of the group.
     * @param currentAssignment The current assignment.
     * @param quotas            The quotas, as the steps left them. Every extra partition of every
     *                          topic must be given, so that the quotas of a topic add up to its
     *                          partition count.
     */
    PartitionPlacement(IndexedGroup group, CurrentAssignment currentAssignment, Quotas quotas) {
        this.group = group;
        this.currentAssignment = currentAssignment;
        this.quotas = quotas;
        this.kept = new int[group.memberCount()];
        this.hasExtraPartitionOfTopic = new boolean[group.memberCount()];
    }

    /**
     * Places the partitions of the topic, see the class documentation.
     *
     * @param topic      The topic.
     * @param assignment Receives, for every partition of the topic, the member getting it. Its
     *                   length must be at least the partition count of the topic.
     * @return {@code true} if a partition goes to a member that does not own it. Otherwise every
     *         partition stays with its owner, and the caller need not read the assignment.
     */
    boolean place(int topic, int[] assignment) {
        markMembersWithExtraPartition(topic, true);
        int minQuota = quotas.minQuota(topic);
        var owners = currentAssignment.owners(topic);
        movingPartitions.clear();
        for (int partition = 0; partition < group.partitionCount(topic); partition++) {
            int owner = owners == null ? -1 : owners[partition];
            if (owner >= 0 && kept[owner] < quota(owner, minQuota)) {
                assignment[partition] = owner;
                keep(owner);
            } else {
                movingPartitions.add(partition);
            }
        }
        // No member is below its quota when no partition moves, since the owners keep every
        // partition and the quotas add up to the partition count.
        boolean moves = !movingPartitions.isEmpty();
        if (moves) {
            handOutMovingPartitions(topic, minQuota, assignment);
        }
        reset(topic);
        return moves;
    }

    /**
     * Sets or clears the marks of the members getting an extra partition of the topic.
     */
    private void markMembersWithExtraPartition(int topic, boolean mark) {
        var members = quotas.membersWithExtraPartition(topic);
        for (int i = 0; i < members.size(); i++) {
            hasExtraPartitionOfTopic[members.get(i)] = mark;
        }
    }

    /**
     * @return The quota of the member for the topic being placed.
     */
    private int quota(int member, int minQuota) {
        return hasExtraPartitionOfTopic[member] ? minQuota + 1 : minQuota;
    }

    /**
     * Counts one more partition of the topic that the owner keeps.
     */
    private void keep(int owner) {
        if (kept[owner]++ == 0) {
            keepingOwners.add(owner);
        }
    }

    /**
     * Gives the moving partitions of the topic to its subscribers below their quota, in the order
     * of the class documentation.
     */
    private void handOutMovingPartitions(int topic, int minQuota, int[] assignment) {
        int moved = 0;
        // With a minimum quota of 0, only the members with an extra partition can be below their
        // quota, so the subscribers of the topic need not be walked.
        if (minQuota == 0) {
            var members = quotas.membersWithExtraPartition(topic);
            for (int i = 0; i < members.size(); i++) {
                moved = giveMissingPartitions(members.get(i), minQuota, moved, assignment);
            }
        } else {
            for (int cohort : group.cohortsOf(topic)) {
                for (int member : group.membersOf(cohort)) {
                    moved = giveMissingPartitions(member, minQuota, moved, assignment);
                }
            }
        }
    }

    /**
     * Gives the member the next moving partitions until it reaches its quota.
     *
     * @param moved The number of moving partitions given so far.
     * @return The number of moving partitions given so far, including those given to the member.
     */
    private int giveMissingPartitions(int member, int minQuota, int moved, int[] assignment) {
        for (int count = kept[member]; count < quota(member, minQuota); count++) {
            assignment[movingPartitions.get(moved++)] = member;
        }
        return moved;
    }

    /**
     * Resets {@link #hasExtraPartitionOfTopic} and {@link #kept} after placing the topic.
     */
    private void reset(int topic) {
        markMembersWithExtraPartition(topic, false);
        for (int i = 0; i < keepingOwners.size(); i++) {
            kept[keepingOwners.get(i)] = 0;
        }
        keepingOwners.clear();
    }
}
