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
import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.RangeSet;
import org.apache.kafka.coordinator.group.assignor.uniform2.steps.AssignRemainingExtraPartitions;
import org.apache.kafka.coordinator.group.assignor.uniform2.steps.BalanceExtraPartitions;
import org.apache.kafka.coordinator.group.assignor.uniform2.steps.KeepStickyExtraPartitions;
import org.apache.kafka.coordinator.group.assignor.uniform2.util.IntList;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Computes the assignment of a group. It reads the group and its current assignment, decides the
 * quotas in three steps, {@link KeepStickyExtraPartitions}, {@link AssignRemainingExtraPartitions}
 * and {@link BalanceExtraPartitions}, places the partitions of every topic, and builds the result,
 * reusing the maps and sets of the current assignment wherever they do not change.
 *
 * <p>A member whose partitions do not change gets the same map it had, so that the coordinator
 * recognizes it as unchanged. A member whose partitions change gets a new map, which shares the
 * partition sets of its topics that do not change. A new set is a {@link RangeSet} when its
 * partitions are consecutive, which they often are, and a {@link HashSet} otherwise.
 */
public final class AssignmentBuilder {
    /**
     * The members and topics of the group.
     */
    private final IndexedGroup group;

    /**
     * The current assignment of the group.
     */
    private final CurrentAssignment currentAssignment;

    /**
     * The quotas of the members, which {@link #build} decides.
     */
    private Quotas quotas;

    /**
     * Per member, its new map once one of its topics changed, or null.
     */
    private final List<Map<Uuid, Set<Integer>>> newAssignments;

    /**
     * Per member, the number of partitions of the topic being recorded that it gets. Reset after
     * every topic.
     */
    private final int[] placed;

    /**
     * Per member, the number of partitions it owns among those of the topic being recorded that it
     * gets. Reset after every topic.
     */
    private final int[] kept;

    /**
     * Per member, the lowest partition of the topic being recorded that it gets.
     */
    private final int[] lowest;

    /**
     * Per member, the highest partition of the topic being recorded that it gets.
     */
    private final int[] highest;

    /**
     * Per member, whether it gets exactly the partitions of the topic being recorded that it holds.
     * Reset after every topic.
     */
    private final boolean[] unchanged;

    /**
     * Per member, the hash set collecting its partitions of the topic being recorded when they
     * are not consecutive, or null.
     */
    private final List<Set<Integer>> newPartitions;

    /**
     * The members getting partitions of the topic being recorded, whose entries in the fields above
     * are reset after the topic.
     */
    private final IntList touched = new IntList(16);

    /**
     * Per member, whether it is an owner of the topic being recorded that was already looked at.
     * All false between topics.
     */
    private final boolean[] recorded;

    /**
     * The owners of the topic being recorded already looked at, to reset {@link #recorded}.
     */
    private final IntList recordedOwners = new IntList(16);

    /**
     * Reads the members, their subscriptions and their current assignment.
     *
     * @param groupSpec                The group spec.
     * @param subscribedTopicDescriber The describer of the subscribed topics.
     */
    public AssignmentBuilder(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        group = new IndexedGroup.Builder(groupSpec, subscribedTopicDescriber).build();
        currentAssignment = new CurrentAssignment(groupSpec, group);
        int memberCount = group.memberCount();
        newAssignments = new ArrayList<>(memberCount);
        newPartitions = new ArrayList<>(memberCount);
        for (int member = 0; member < memberCount; member++) {
            newAssignments.add(null);
            newPartitions.add(null);
        }
        placed = new int[memberCount];
        kept = new int[memberCount];
        lowest = new int[memberCount];
        highest = new int[memberCount];
        unchanged = new boolean[memberCount];
        recorded = new boolean[memberCount];
    }

    /**
     * Decides the quotas, places the partitions of every topic, records the changes, and builds
     * the assignment of every member, reusing the unchanged maps.
     *
     * @return The assignment.
     */
    public GroupAssignment build() {
        quotas = new Quotas(group);
        new KeepStickyExtraPartitions(group, currentAssignment, quotas).run();
        new AssignRemainingExtraPartitions(group, quotas).run();
        new BalanceExtraPartitions(group, currentAssignment, quotas).run();

        var placement = new PartitionPlacement(group, currentAssignment, quotas);
        int maxPartitionCount = 0;
        for (int topic = 0; topic < group.topicCount(); topic++) {
            maxPartitionCount = Math.max(maxPartitionCount, group.partitionCount(topic));
        }
        var assignment = new int[maxPartitionCount];
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (placement.place(topic, assignment)) {
                record(topic, assignment);
            }
        }

        Map<String, MemberAssignment> members = Utils.newHashMap(group.memberCount());
        for (int member = 0; member < group.memberCount(); member++) {
            var partitions = newAssignments.get(member);
            if (partitions == null) {
                partitions = currentAssignment.holdsStalePartitions(member)
                    ? ownedPartitions(member)
                    : currentAssignment.assignment(member);
            }
            members.put(group.memberId(member), new MemberAssignmentImpl(partitions));
        }
        return new GroupAssignment(members);
    }

    /**
     * Records the partitions of the topic that every member gets. A member getting exactly the
     * partitions it holds keeps its set, and the others get a new set or lose the topic.
     */
    private void record(int topic, int[] assignment) {
        int partitionCount = group.partitionCount(topic);
        var owners = currentAssignment.owners(topic);
        touched.clear();
        for (int partition = 0; partition < partitionCount; partition++) {
            int member = assignment[partition];
            if (placed[member]++ == 0) {
                touched.add(member);
                lowest[member] = partition;
            }
            highest[member] = partition;
            if (owners != null && owners[partition] == member) {
                kept[member]++;
            }
        }
        recordOwners(topic);
        collectScatteredPartitions(topic, assignment);

        var topicId = group.topicId(topic);
        for (int i = 0; i < touched.size(); i++) {
            int member = touched.get(i);
            if (!unchanged[member]) {
                var partitions = newPartitions.get(member);
                if (partitions == null) {
                    partitions = new RangeSet(lowest[member], highest[member] + 1);
                }
                newAssignment(member).put(topicId, partitions);
                newPartitions.set(member, null);
            }
            placed[member] = 0;
            kept[member] = 0;
            unchanged[member] = false;
        }
    }

    /**
     * Marks the owners of the topic getting exactly the partitions they hold as unchanged, and
     * removes the topic from the owners getting none of its partitions.
     */
    private void recordOwners(int topic) {
        var owners = currentAssignment.owners(topic);
        if (owners == null) {
            return;
        }
        var topicId = group.topicId(topic);
        for (int owner : owners) {
            if (owner < 0 || recorded[owner]) {
                continue;
            }
            recorded[owner] = true;
            recordedOwners.add(owner);
            if (placed[owner] == 0) {
                newAssignment(owner).remove(topicId);
            } else if (placed[owner] == kept[owner] && currentAssignment.assignment(owner).get(topicId).size() == kept[owner]) {
                unchanged[owner] = true;
            }
        }
        for (int i = 0; i < recordedOwners.size(); i++) {
            recorded[recordedOwners.get(i)] = false;
        }
        recordedOwners.clear();
    }

    /**
     * Collects in hash sets the partitions of the members whose new partitions of the topic are
     * not consecutive.
     */
    private void collectScatteredPartitions(int topic, int[] assignment) {
        for (int partition = 0; partition < group.partitionCount(topic); partition++) {
            int member = assignment[partition];
            if (!unchanged[member] && highest[member] - lowest[member] + 1 != placed[member]) {
                var partitions = newPartitions.get(member);
                if (partitions == null) {
                    partitions = Utils.newHashSet(placed[member]);
                    newPartitions.set(member, partitions);
                }
                partitions.add(partition);
            }
        }
    }

    /**
     * @return The new map of the member, created on first use with the entries it owns.
     */
    private Map<Uuid, Set<Integer>> newAssignment(int member) {
        var partitions = newAssignments.get(member);
        if (partitions == null) {
            partitions = ownedPartitions(member);
            newAssignments.set(member, partitions);
        }
        return partitions;
    }

    /**
     * @return A new map with the partitions the member owns, see {@link CurrentAssignment}. It
     *         holds the sets of its current assignment, without the stale entries.
     */
    private Map<Uuid, Set<Integer>> ownedPartitions(int member) {
        var assignment = currentAssignment.assignment(member);
        // Sized for the entries it owns and for those it may get, so that it does not grow.
        int capacity = Math.max(assignment.size(), quotas.maxTopicsWithPartitions(member));
        Map<Uuid, Set<Integer>> partitions = Utils.newHashMap(capacity);
        assignment.forEach((topicId, topicPartitions) -> {
            int topic = group.topicIndex(topicId);
            if (topic >= 0 && group.subscribes(member, topic)) {
                partitions.put(topicId, topicPartitions);
            }
        });
        return partitions;
    }
}
