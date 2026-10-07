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
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntPair;

import java.util.Arrays;

/**
 * The first step deciding the {@link Quotas}: every member owning more than the minimum quota of a
 * topic keeps one of its extra partitions, so that it only gives up what it owns beyond its quota.
 * When more owners qualify than the topic has extra partitions, the ones with the smallest
 * assignments keep them, the assignment sizes counting the extra partitions kept for the previous
 * topics, then the members in member order.
 */
final class KeepStickyExtraPartitions {
    /**
     * The members and topics of the group.
     */
    private final IndexedGroup group;

    /**
     * The current assignment, which tells the owners of every topic.
     */
    private final CurrentAssignment current;

    /**
     * The quotas, which the step completes.
     */
    private final Quotas quotas;

    /**
     * @param group   The members and topics of the group.
     * @param current The current assignment.
     * @param quotas  The quotas, at the minimum quota.
     */
    KeepStickyExtraPartitions(IndexedGroup group, CurrentAssignment current, Quotas quotas) {
        this.group = group;
        this.current = current;
        this.quotas = quotas;
    }

    /**
     * Lets the owners of more than the minimum quota of every topic keep an extra partition.
     */
    void run() {
        var counts = new int[group.memberCount()];
        var owners = new IntList(16);
        var candidates = new IntList(16);
        for (int topic = 0; topic < group.topicCount(); topic++) {
            int extraPartitions = quotas.extraPartitions(topic);
            if (extraPartitions == 0) {
                continue;
            }
            current.countOwned(topic, counts, owners);
            candidates.clear();
            for (int i = 0; i < owners.size(); i++) {
                int owner = owners.get(i);
                if (counts[owner] > quotas.minQuota(topic)) {
                    candidates.add(owner);
                }
                // Reset for the next topic, as countOwned requires.
                counts[owner] = 0;
            }
            // More owners qualify than the topic has extra partitions, as after a join. They are
            // at most the subscribers of the topic, so the sort stays small.
            if (candidates.size() > extraPartitions) {
                sortBySize(candidates);
            }
            for (int i = 0; i < Math.min(candidates.size(), extraPartitions); i++) {
                quotas.giveExtraPartition(candidates.get(i), topic);
            }
        }
    }

    /**
     * Sorts the members by increasing assignment size, then member order.
     */
    private void sortBySize(IntList members) {
        // Pairs of the assignment size and the member, which sort by assignment size, then member.
        var keys = new long[members.size()];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = IntPair.pack(quotas.assignmentSize(members.get(i)), members.get(i));
        }
        Arrays.sort(keys);
        for (int i = 0; i < keys.length; i++) {
            members.set(i, IntPair.low(keys[i]));
        }
    }
}
