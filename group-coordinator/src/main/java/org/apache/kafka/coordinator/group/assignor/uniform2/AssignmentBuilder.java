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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Computes the assignment of a group. It reads the group and its current assignment, then runs
 * {@link KeepStickyExtraPartitions}, {@link AssignRemainingExtraPartitions} and
 * {@link BalanceExtraPartitions}, which decide the quotas. {@link PartitionPlacement} then decides
 * which member gets every partition of every topic, and this class builds the assignments of the
 * members from its decisions.
 *
 * <p>A member whose partitions do not change gets back the map it had, so that the coordinator
 * finds it equal to its current assignment without comparing the sets. A member whose partitions
 * change gets a new map, which shares the sets of its topics that do not change. A new set is a
 * {@link RangeSet} when its partitions are consecutive, and a {@link HashSet} otherwise.
 */
public final class AssignmentBuilder {
    /**
     * The group spec.
     */
    private final GroupSpec groupSpec;

    /**
     * The describer of the subscribed topics.
     */
    private final SubscribedTopicDescriber subscribedTopicDescriber;

    /**
     * The members and topics of the group.
     */
    private IndexedGroup group;

    /**
     * The current assignment of the group.
     */
    private CurrentAssignment currentAssignment;

    /**
     * The quotas of the members, which the steps compute.
     */
    private Quotas quotas;

    /**
     * Per member, its new map once one of its topics changed, or null.
     */
    private List<Map<Uuid, Set<Integer>>> newAssignments;

    /**
     * Per member, the number of partitions of the topic being placed that it gets. All 0 between
     * topics.
     */
    private int[] placed;

    /**
     * Per member, the lowest partition of the topic being placed that it gets. Only defined for
     * the members getting partitions of the topic.
     */
    private int[] lowest;

    /**
     * Per member, the highest partition of the topic being placed that it gets. Only defined for
     * the members getting partitions of the topic.
     */
    private int[] highest;

    /**
     * Per member, whether its partitions of the topic being placed change, because it gets a
     * partition it does not own or loses one it owns. All false between topics.
     */
    private boolean[] changed;

    /**
     * The members whose partitions of the topic being placed change, to write their new sets and
     * reset {@link #changed}.
     */
    private IntList changedMembers;

    /**
     * Per partition of the topic being placed, the next partition that its member gets. It links
     * the partitions of every member from its lowest to its highest. The entry of the highest
     * partition of a member is not defined.
     */
    private int[] nextPartition;

    /**
     * The members getting partitions of the topic being placed, to reset {@link #placed} after
     * the topic.
     */
    private IntList membersGettingPartitions;

    /**
     * @param groupSpec                The group spec.
     * @param subscribedTopicDescriber The describer of the subscribed topics.
     */
    public AssignmentBuilder(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.groupSpec = groupSpec;
        this.subscribedTopicDescriber = subscribedTopicDescriber;
    }

    /**
     * Reads the group and its current assignment, computes the quotas, places the partitions of
     * every topic, updates the assignments of the members whose partitions change, and builds the
     * assignment of every member, reusing the unchanged maps.
     *
     * @return The assignment.
     */
    public GroupAssignment build() {
        // Reads the group and its current assignment, and computes the quotas.
        group = new IndexedGroup.Builder(groupSpec, subscribedTopicDescriber).build();
        currentAssignment = new CurrentAssignment(groupSpec, group);
        quotas = computeQuotas();

        // Creates the new maps of the members, all null until one of their topics changes, and the
        // state of updateMemberAssignments.
        int memberCount = group.memberCount();
        newAssignments = new ArrayList<>(Collections.nCopies(memberCount, null));
        placed = new int[memberCount];
        lowest = new int[memberCount];
        highest = new int[memberCount];
        changed = new boolean[memberCount];
        changedMembers = new IntList(16);
        nextPartition = new int[group.maxPartitionCount()];
        membersGettingPartitions = new IntList(16);

        // Places the partitions of every topic, and updates the assignments of the members whose
        // partitions of the topic change. The assignment of the topic gives the member getting
        // every partition. It is sized for the largest topic and reused for every topic.
        var placement = new PartitionPlacement(group, currentAssignment, quotas);
        var assignment = new int[group.maxPartitionCount()];
        for (int topic = 0; topic < group.topicCount(); topic++) {
            if (placement.place(topic, assignment)) {
                updateMemberAssignments(topic, assignment);
            }
        }

        // Builds the assignment of every member: its new map if one of its topics changed, and
        // otherwise the map it holds, without its stale partitions if it has any.
        Map<String, MemberAssignment> members = Utils.newHashMap(memberCount);
        for (int member = 0; member < memberCount; member++) {
            var partitions = newAssignments.get(member);
            // A member without a new map gets exactly the partitions it owns of every topic, so it
            // keeps its map, unless the map has stale partitions, which a copy without them drops.
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
     * @return The quotas, computed by running the steps in order.
     */
    private Quotas computeQuotas() {
        var quotas = new Quotas(group);
        new KeepStickyExtraPartitions(group, currentAssignment, quotas).run();
        new AssignRemainingExtraPartitions(group, quotas).run();
        new BalanceExtraPartitions(group, currentAssignment, quotas).run();
        return quotas;
    }

    /**
     * Updates the new assignments of the members whose partitions of the topic change. Such a
     * member gets a new set, or loses the topic if it gets none of the topic's partitions. Every
     * other member gets exactly the partitions it owns, so it keeps its set.
     */
    private void updateMemberAssignments(int topic, int[] assignment) {
        int partitionCount = group.partitionCount(topic);
        var owners = currentAssignment.owners(topic);
        membersGettingPartitions.clear();

        // Counts the partitions of every member and links them, from its lowest to its highest. A
        // partition going to a member which does not own it changes both that member and its
        // owner, if it has one.
        for (int partition = 0; partition < partitionCount; partition++) {
            int member = assignment[partition];
            if (placed[member]++ == 0) {
                membersGettingPartitions.add(member);
                lowest[member] = partition;
            } else {
                nextPartition[highest[member]] = partition;
            }
            highest[member] = partition;
            int owner = owners == null ? -1 : owners[partition];
            if (owner != member) {
                markChanged(member);
                if (owner >= 0) {
                    markChanged(owner);
                }
            }
        }

        // Gives every changed member its new set, or removes the topic from it if it gets none of
        // the topic's partitions.
        var topicId = group.topicId(topic);
        for (int i = 0; i < changedMembers.size(); i++) {
            int member = changedMembers.get(i);
            if (placed[member] == 0) {
                newAssignment(member).remove(topicId);
            } else {
                newAssignment(member).put(topicId, newSet(member));
            }
            changed[member] = false;
        }
        changedMembers.clear();

        // Resets the counts of the members getting partitions for the next topic, at most one per
        // partition of the topic rather than every member of the group.
        for (int i = 0; i < membersGettingPartitions.size(); i++) {
            placed[membersGettingPartitions.get(i)] = 0;
        }
    }

    /**
     * Marks the partitions of the topic of the member as changed. The member is added to
     * {@link #changedMembers} the first time.
     */
    private void markChanged(int member) {
        if (!changed[member]) {
            changed[member] = true;
            changedMembers.add(member);
        }
    }

    /**
     * @return The new set of the partitions of the topic being placed that the member gets: a
     *         {@link RangeSet} when they are consecutive, and otherwise a hash set filled by
     *         following {@link #nextPartition} from the lowest partition of the member to its
     *         highest.
     */
    private Set<Integer> newSet(int member) {
        if (highest[member] - lowest[member] + 1 == placed[member]) {
            return new RangeSet(lowest[member], highest[member] + 1);
        }
        Set<Integer> partitions = Utils.newHashSet(placed[member]);
        int partition = lowest[member];
        partitions.add(partition);
        while (partition != highest[member]) {
            partition = nextPartition[partition];
            partitions.add(partition);
        }
        return partitions;
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
        Map<Uuid, Set<Integer>> partitions = Utils.newHashMap(assignment.size());
        assignment.forEach((topicId, topicPartitions) -> {
            int topic = group.topicIndex(topicId);
            if (topic >= 0 && group.subscribes(member, topic)) {
                partitions.put(topicId, topicPartitions);
            }
        });
        return partitions;
    }
}
