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

import java.util.Arrays;

/**
 * The keep step of the {@link Shares}: every member owning more than the base partitions of a
 * topic keeps one of its extra partitions, so that it only gives up what it owns beyond the base
 * partitions and one. When more owners qualify than the topic has extra partitions, the ones with
 * the smallest assignments keep them, the sizes counting the extra partitions kept for the
 * previous topics, then the members in member order.
 */
final class Keep {
    /**
     * The members and topics of the group.
     */
    private final GroupModel group;

    /**
     * The current assignment, which tells the owners of every topic.
     */
    private final CurrentAssignment current;

    /**
     * The shares, which the step completes.
     */
    private final Shares shares;

    /**
     * @param group   The members and topics of the group.
     * @param current The current assignment.
     * @param shares  The shares, holding the base partitions only.
     */
    Keep(GroupModel group, CurrentAssignment current, Shares shares) {
        this.group = group;
        this.current = current;
        this.shares = shares;
    }

    /**
     * Lets the owners of more than the base partitions of every topic keep an extra partition.
     */
    void run() {
        var counts = new int[group.memberCount()];
        var owners = new IntList(16);
        var candidates = new IntList(16);
        for (int topic = 0; topic < group.topicCount(); topic++) {
            int extraPartitions = shares.extraPartitions(topic);
            if (extraPartitions == 0) {
                continue;
            }
            current.countOwned(topic, counts, owners);
            candidates.clear();
            for (int i = 0; i < owners.size(); i++) {
                int owner = owners.get(i);
                if (counts[owner] > shares.basePartitions(topic)) {
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
                shares.giveExtraPartition(candidates.get(i), topic);
            }
        }
    }

    /**
     * Sorts the members by increasing assignment size, then member order.
     */
    private void sortBySize(IntList members) {
        // Sorts the members as longs, the size in the high 32 bits and the member in the low 32
        // bits, so that the sizes order them first and the members break the ties.
        var keys = new long[members.size()];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = ((long) shares.size(members.get(i)) << 32) | members.get(i);
        }
        Arrays.sort(keys);
        for (int i = 0; i < keys.length; i++) {
            members.set(i, (int) keys[i]);
        }
    }
}
