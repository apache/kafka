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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The current assignment of a group: the owner of every partition.
 *
 * <p>A member owns a partition when it holds it, subscribes to its topic, and the partition
 * exists. Everything else the members hold is stale: the partitions of topics they do not
 * subscribe to or which no longer exist, the partitions beyond the partition count of their topic,
 * and the empty sets of partitions. Stale partitions are dropped silently, and the members holding
 * some get a new map. As the coordinator guarantees, a partition is held by at most one member.
 */
final class CurrentAssignment {
    /**
     * Per member, the partitions it holds, as given by the spec.
     */
    private final List<Map<Uuid, Set<Integer>>> assignments;

    /**
     * Per member, whether it holds stale partitions, and thus needs a new map.
     */
    private final boolean[] holdsStalePartitions;

    /**
     * Per topic, the owner of every partition, or -1; null while no partition of the topic has an
     * owner.
     */
    private final int[][] owners;

    /**
     * The sets of partitions holding partitions that do not exist, beyond the partition count of
     * their topic or negative, each as the key of its member and topic, see {@link #key}. They are
     * rare, hence a set rather than a flag per member and topic.
     */
    private final Set<Long> setsBeyondPartitionCount = new HashSet<>();

    /**
     * Reads the current assignment of every member.
     *
     * @param groupSpec The group spec.
     * @param group     The members and topics of the group.
     */
    CurrentAssignment(GroupSpec groupSpec, GroupModel group) {
        int memberCount = group.memberCount();
        int topicCount = group.topicCount();
        assignments = new ArrayList<>(memberCount);
        for (int member = 0; member < memberCount; member++) {
            assignments.add(groupSpec.memberAssignment(group.memberId(member)).partitions());
        }
        holdsStalePartitions = new boolean[memberCount];
        owners = new int[topicCount][];

        // The members are read cohort by cohort, so that marking the topics of the cohort tells
        // in constant time whether a member subscribes to a topic.
        var subscribed = new int[topicCount];
        Arrays.fill(subscribed, -1);
        for (int cohort = 0; cohort < group.cohortCount(); cohort++) {
            for (int topic : group.topicsOf(cohort)) {
                subscribed[topic] = cohort;
            }
            for (int member : group.membersOf(cohort)) {
                recordAssignment(group, member, cohort, subscribed);
            }
        }
    }

    /**
     * Records the owners of the partitions that the member holds, and whether it holds stale
     * partitions.
     */
    private void recordAssignment(GroupModel group, int member, int cohort, int[] subscribed) {
        // forEach walks the table of a HashMap directly, measurably cheaper than its iterator on
        // large groups.
        assignments.get(member).forEach((topicId, partitions) -> {
            int topic = group.topicIndex(topicId);
            if (topic < 0 || subscribed[topic] != cohort) {
                holdsStalePartitions[member] = true;
                return;
            }
            int owned = recordOwner(group, topic, member, partitions);
            if (owned < partitions.size()) {
                setsBeyondPartitionCount.add(key(member, topic));
                holdsStalePartitions[member] = true;
            } else if (partitions.isEmpty()) {
                holdsStalePartitions[member] = true;
            }
        });
    }

    /**
     * Records the member as the owner of its partitions of the topic that exist.
     *
     * @return The number of partitions it owns.
     */
    private int recordOwner(GroupModel group, int topic, int member, Set<Integer> partitions) {
        int partitionCount = group.partitionCount(topic);
        int owned = 0;
        for (int partition : partitions) {
            if (partition >= 0 && partition < partitionCount) {
                var topicOwners = owners[topic];
                if (topicOwners == null) {
                    topicOwners = new int[partitionCount];
                    Arrays.fill(topicOwners, -1);
                    owners[topic] = topicOwners;
                }
                topicOwners[partition] = member;
                owned++;
            }
        }
        return owned;
    }

    /**
     * @return The key of the member and the topic: the member in the high 32 bits of a long and
     *         the topic in the low 32 bits, so that every member and topic have their own key.
     */
    private static long key(int member, int topic) {
        return ((long) member << 32) | topic;
    }

    /**
     * @return The current assignment of the member, as given by the spec.
     */
    Map<Uuid, Set<Integer>> assignment(int member) {
        return assignments.get(member);
    }

    /**
     * @return True if the member holds stale partitions.
     */
    boolean holdsStalePartitions(int member) {
        return holdsStalePartitions[member];
    }

    /**
     * @return True if the set of partitions of the topic that the member holds has partitions
     *         which do not exist, beyond the partition count or negative.
     */
    boolean holdsPartitionsBeyondCount(int member, int topic) {
        return !setsBeyondPartitionCount.isEmpty() && setsBeyondPartitionCount.contains(key(member, topic));
    }

    /**
     * @return Per partition of the topic, its owner, or -1 if it has none; or null if no
     *         partition of the topic has an owner. The array must not be changed.
     */
    int[] owners(int topic) {
        return owners[topic];
    }

    /**
     * Counts the partitions of the topic that each of its owners owns.
     *
     * @param topic  The topic.
     * @param counts Receives, at the index of every owner, its number of partitions. The entries
     *               of the owners must be zero, and the caller resets them.
     * @param distinctOwners Receives the owners, in the order of their lowest partition. It is
     *                       cleared first.
     */
    void countOwned(int topic, int[] counts, IntList distinctOwners) {
        distinctOwners.clear();
        var topicOwners = owners[topic];
        if (topicOwners == null) {
            return;
        }
        for (int owner : topicOwners) {
            if (owner >= 0 && counts[owner]++ == 0) {
                distinctOwners.add(owner);
            }
        }
    }
}
