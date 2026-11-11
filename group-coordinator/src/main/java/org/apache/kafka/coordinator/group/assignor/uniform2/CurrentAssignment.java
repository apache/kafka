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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The current assignment of a group: the owner of every partition.
 *
 * <p>A member owns a partition when it holds it and subscribes to its topic. The partitions of
 * topics that a member does not subscribe to, or which no longer exist, are stale: they have no
 * owner, {@link #holdsStalePartitions} tells which members hold some, and {@link #assignment} still
 * contains them. As the coordinator guarantees, a partition is held by at most one member, a topic
 * never has fewer partitions than its members hold, and no set of partitions is empty.
 */
public final class CurrentAssignment {
    /**
     * Per member, the partitions it holds, as given by the spec.
     */
    private final List<Map<Uuid, Set<Integer>>> assignments;

    /**
     * Per member, whether it needs a new map because it holds stale partitions.
     */
    private final boolean[] holdsStalePartitions;

    /**
     * Per topic, the owner of every partition, or -1; null while no partition of the topic has an
     * owner, so that no array is allocated for a topic that nobody owns, as on the first assignment
     * of a group.
     */
    private final int[][] owners;

    /**
     * Reads the current assignment of every member.
     *
     * @param groupSpec The group spec.
     * @param group     The members and topics of the group.
     */
    public CurrentAssignment(GroupSpec groupSpec, IndexedGroup group) {
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
    private void recordAssignment(IndexedGroup group, int member, int cohort, int[] subscribed) {
        // forEach walks the table of a HashMap directly, measurably cheaper than its iterator on
        // large groups.
        assignments.get(member).forEach((topicId, partitions) -> {
            int topic = group.topicIndex(topicId);
            if (topic < 0 || subscribed[topic] != cohort) {
                holdsStalePartitions[member] = true;
            } else {
                recordOwner(group, topic, member, partitions);
            }
        });
    }

    /**
     * Records the member as the owner of its partitions of the topic.
     */
    private void recordOwner(IndexedGroup group, int topic, int member, Set<Integer> partitions) {
        var topicOwners = owners[topic];
        if (topicOwners == null) {
            topicOwners = new int[group.partitionCount(topic)];
            Arrays.fill(topicOwners, -1);
            owners[topic] = topicOwners;
        }
        for (int partition : partitions) {
            topicOwners[partition] = member;
        }
    }

    /**
     * @return The current assignment of the member, as given by the spec.
     */
    public Map<Uuid, Set<Integer>> assignment(int member) {
        return assignments.get(member);
    }

    /**
     * @return {@code true} if the member holds stale partitions.
     */
    public boolean holdsStalePartitions(int member) {
        return holdsStalePartitions[member];
    }

    /**
     * @return Per partition of the topic, its owner, or -1 if it has none; or null if no
     *         partition of the topic has an owner. The array must not be changed.
     */
    public int[] owners(int topic) {
        return owners[topic];
    }

    /**
     * Counts the partitions of the topic that each of its owners owns.
     *
     * @param topic          The topic.
     * @param counts         Receives, at the index of every owner, its number of partitions. Must
     *                       be sized to the number of members, with zeroed entries.
     * @param distinctOwners Receives the owners, in the order of their lowest partition. Existing
     *                       entries are cleared by this method.
     */
    public void countOwned(int topic, int[] counts, IntList distinctOwners) {
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
