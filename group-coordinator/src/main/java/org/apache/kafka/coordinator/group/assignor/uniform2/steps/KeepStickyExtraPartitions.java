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

import org.apache.kafka.coordinator.group.assignor.uniform2.CurrentAssignment;
import org.apache.kafka.coordinator.group.assignor.uniform2.IndexedGroup;
import org.apache.kafka.coordinator.group.assignor.uniform2.Quotas;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntList;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntPair;

import java.util.Arrays;

/**
 * The first step deciding the {@link Quotas}: every member owning more than the minimum quota of a
 * topic keeps one of its extra partitions, so that it only gives up what it owns beyond its quota.
 * When more owners qualify than the topic has extra partitions, the ones with the smallest
 * assignment sizes keep them. The assignment sizes include the extra partitions kept for the topics
 * so far. Ties are broken by member order.
 */
public final class KeepStickyExtraPartitions {
    /**
     * The members and topics of the group.
     */
    private final IndexedGroup group;

    /**
     * The current assignment, which tells the owners of every topic.
     */
    private final CurrentAssignment currentAssignment;

    /**
     * The quotas, which the step updates.
     */
    private final Quotas quotas;

    /**
     * @param group             The members and topics of the group.
     * @param currentAssignment The current assignment.
     * @param quotas            The quotas, at the minimum quota.
     */
    public KeepStickyExtraPartitions(IndexedGroup group, CurrentAssignment currentAssignment, Quotas quotas) {
        this.group = group;
        this.currentAssignment = currentAssignment;
        this.quotas = quotas;
    }

    /**
     * Lets the owners of more than the minimum quota of every topic keep an extra partition.
     */
    public void run() {
        // Per member, the number of partitions of the topic that it owns.
        var ownedPartitionCounts = new int[group.memberCount()];
        // The owners of the topic.
        var owners = new IntList(16);
        // The owners above the minimum quota of the topic, which may keep an extra partition.
        var ownersAboveMinQuota = new IntList(16);
        for (int topic = 0; topic < group.topicCount(); topic++) {
            int extraPartitions = quotas.extraPartitions(topic);
            if (extraPartitions == 0) {
                continue;
            }
            currentAssignment.countOwned(topic, ownedPartitionCounts, owners);
            ownersAboveMinQuota.clear();
            for (int i = 0; i < owners.size(); i++) {
                int owner = owners.get(i);
                if (ownedPartitionCounts[owner] > quotas.minQuota(topic)) {
                    ownersAboveMinQuota.add(owner);
                }
                // Reset for the next topic, as countOwned requires.
                ownedPartitionCounts[owner] = 0;
            }
            // More owners qualify than the topic has extra partitions, as after a join. They are
            // at most the subscribers of the topic, so the sort stays small.
            if (ownersAboveMinQuota.size() > extraPartitions) {
                sortBySize(ownersAboveMinQuota);
            }
            for (int i = 0; i < Math.min(ownersAboveMinQuota.size(), extraPartitions); i++) {
                quotas.giveExtraPartition(ownersAboveMinQuota.get(i), topic);
            }
        }
    }

    /**
     * Sorts the members by increasing assignment size, then member order.
     */
    private void sortBySize(IntList members) {
        // Pairs of the assignment size and the member, which sort by assignment size, then member.
        var assignmentSizesAndMembers = new long[members.size()];
        for (int i = 0; i < assignmentSizesAndMembers.length; i++) {
            assignmentSizesAndMembers[i] = IntPair.pack(quotas.assignmentSize(members.get(i)), members.get(i));
        }
        Arrays.sort(assignmentSizesAndMembers);
        for (int i = 0; i < assignmentSizesAndMembers.length; i++) {
            members.set(i, IntPair.low(assignmentSizesAndMembers[i]));
        }
    }
}
