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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.server.common.TopicIdPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

/**
 * A simple partition assignor for share groups that assigns partitions of the subscribed topics
 * to different members based on the rules defined in KIP-932. It is not rack-aware.
 * <p>
 * Assignments are done according to the following principles:
 * <ol>
 *   <li>Balance:          Ensure partitions are distributed equally among all members.
 *                         The difference in assignments sizes between any two members
 *                         should not exceed one partition.</li>
 *   <li>Stickiness:       Minimize partition movements among members by retaining
 *                         as much of the existing assignment as possible.</li>
 * </ol>
 * <p>
 * Balance is prioritized above stickiness.
 */
public class SimpleAssignor implements ShareGroupPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(SimpleAssignor.class);
    private static final String SIMPLE_ASSIGNOR_NAME = "simple";

    /**
     * Unique name for this assignor.
     */
    @Override
    public String name() {
        return SIMPLE_ASSIGNOR_NAME;
    }

    /**
     * Assigns partitions to group members based on the given assignment specification and topic metadata.
     *
     * @param groupSpec                The assignment spec which includes member metadata.
     * @param subscribedTopicDescriber The topic and partition metadata describer.
     * @return The new assignment for the group.
     */
    @Override
    public GroupAssignment assign(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) throws PartitionAssignorException {
        if (groupSpec.memberIds().isEmpty())
            return new GroupAssignment(Map.of());

        if (groupSpec.subscriptionType().equals(HOMOGENEOUS)) {
            log.debug("Detected that all members are subscribed to the same set of topics, invoking the homogeneous assignment algorithm");
            return new SimpleHomogeneousAssignmentBuilder(groupSpec, subscribedTopicDescriber).build();
        } else {
            log.debug("Detected that the members are subscribed to different sets of topics, invoking the heterogeneous assignment algorithm");
            return assignHeterogeneous(groupSpec, subscribedTopicDescriber);
        }
    }

    private GroupAssignment assignHeterogeneous(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        Map<String, List<TopicIdPartition>> memberToPartitionsSubscription = new HashMap<>();
        for (String memberId : groupSpec.memberIds()) {
            MemberSubscription spec = groupSpec.memberSubscription(memberId);
            if (spec.subscribedTopicIds().isEmpty())
                continue;

            // Subscribed topic partitions for the share group member.
            List<TopicIdPartition> targetPartitions = AssignorHelpers.computeTargetPartitions(groupSpec, spec.subscribedTopicIds(), subscribedTopicDescriber);
            memberToPartitionsSubscription.put(memberId, targetPartitions);
        }

        // The current assignment from topic partition to members.
        Map<TopicIdPartition, List<String>> currentAssignment = currentAssignment(groupSpec);

        return newAssignmentHeterogeneous(groupSpec, memberToPartitionsSubscription, currentAssignment);
    }

    /**
     * Get the current assignment by topic partitions.
     *
     * @param groupSpec The group metadata specifications.
     * @return the current assignment for subscribed topic partitions to memberIds.
     */
    static Map<TopicIdPartition, List<String>> currentAssignment(GroupSpec groupSpec) {
        Map<TopicIdPartition, List<String>> assignment = new HashMap<>();

        for (String member : groupSpec.memberIds()) {
            Map<Uuid, Set<Integer>> assignedTopicPartitions = groupSpec.memberAssignment(member).partitions();
            assignedTopicPartitions.forEach((topicId, partitions) -> partitions.forEach(
                partition -> assignment.computeIfAbsent(new TopicIdPartition(topicId, partition), k -> new ArrayList<>()).add(member)));
        }

        return assignment;
    }

    /**
     * This function computes the new assignment for a heterogeneous group.
     *
     * @param groupSpec                      The group metadata specifications.
     * @param memberToPartitionsSubscription The member to subscribed topic partitions map.
     * @param currentAssignment              The current assignment for subscribed topic partitions to memberIds.
     * @return the new partition assignment for the members of the group.
     */
    private GroupAssignment newAssignmentHeterogeneous(
        GroupSpec groupSpec,
        Map<String, List<TopicIdPartition>> memberToPartitionsSubscription,
        Map<TopicIdPartition, List<String>> currentAssignment
    ) {
        int numGroupMembers = groupSpec.memberIds().size();

        // Exhaustive set of all subscribed topic partitions.
        Set<TopicIdPartition> targetPartitions = new LinkedHashSet<>();
        memberToPartitionsSubscription.values().forEach(targetPartitions::addAll);

        // Create a map for topic to members subscription.
        Map<Uuid, Set<String>> topicToMemberSubscription = new HashMap<>();
        memberToPartitionsSubscription.forEach((member, partitions) ->
            partitions.forEach(partition -> topicToMemberSubscription.computeIfAbsent(partition.topicId(), k -> new LinkedHashSet<>()).add(member)));

        Map<TopicIdPartition, List<String>> newAssignment = new HashMap<>();

        // Step 1: Hash member IDs to partitions.
        memberToPartitionsSubscription.forEach((member, partitions) ->
            memberHashAssignment(List.of(member), partitions, newAssignment));

        // Step 2: Round-robin assignment for unassigned partitions which do not have members already assigned in the current assignment.
        Set<TopicIdPartition> assignedPartitions = new LinkedHashSet<>(newAssignment.keySet());
        Map<Uuid, List<TopicIdPartition>> unassignedPartitions = new HashMap<>();
        targetPartitions.forEach(targetPartition -> {
            if (!assignedPartitions.contains(targetPartition) && !currentAssignment.containsKey(targetPartition))
                unassignedPartitions.computeIfAbsent(targetPartition.topicId(), k -> new ArrayList<>()).add(targetPartition);
        });

        unassignedPartitions.keySet().forEach(unassignedTopic ->
            roundRobinAssignment(topicToMemberSubscription.get(unassignedTopic), unassignedPartitions.get(unassignedTopic), newAssignment));

        // Step 3: We combine current assignment and new assignment.
        Map<String, Set<TopicIdPartition>> finalAssignment = AssignorHelpers.newHashMap(numGroupMembers);

        newAssignment.forEach((targetPartition, members) -> members.forEach(member ->
            finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition)));

        // When combining current assignment, we need to only consider the member topic subscription in current assignment
        // which is being subscribed in the new assignment as well.
        currentAssignment.forEach((topicIdPartition, members) -> members.forEach(member -> {
            if (topicToMemberSubscription.getOrDefault(topicIdPartition.topicId(), Set.of()).contains(member)
                && !newAssignment.containsKey(topicIdPartition))
                finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(topicIdPartition);
        }));

        return groupAssignment(finalAssignment, groupSpec.memberIds());
    }

    /**
     * This function updates assignment by hashing the member IDs of the members and maps the partitions assigned to the
     * members based on the hash, one partition per member. This gives approximately even balance.
     *
     * @param memberIds          The member ids to which the topic partitions need to be assigned.
     * @param partitionsToAssign The subscribed topic partitions which needs assignment.
     * @param assignment         The existing assignment by topic partition. We need to pass it as a parameter because this
     *                           method can be called multiple times for heterogeneous assignment.
     */
    // Visible for testing
    void memberHashAssignment(
        Collection<String> memberIds,
        List<TopicIdPartition> partitionsToAssign,
        Map<TopicIdPartition, List<String>> assignment
    ) {
        if (!partitionsToAssign.isEmpty()) {
            for (String memberId : memberIds) {
                int topicPartitionIndex = Math.abs(memberId.hashCode() % partitionsToAssign.size());
                TopicIdPartition topicPartition = partitionsToAssign.get(topicPartitionIndex);
                assignment.computeIfAbsent(topicPartition, k -> new ArrayList<>()).add(memberId);
            }
        }
    }

    /**
     * This functions assigns topic partitions to members by a round-robin approach and updates the existing assignment.
     *
     * @param memberIds          The member ids to which the topic partitions need to be assigned, should be non-empty.
     * @param partitionsToAssign The subscribed topic partitions which needs assignment.
     * @param assignment         The existing assignment by topic partition. We need to pass it as a parameter because this
     *                           method can be called multiple times for heterogeneous assignment.
     */
    // Visible for testing
    void roundRobinAssignment(
        Collection<String> memberIds,
        List<TopicIdPartition> partitionsToAssign,
        Map<TopicIdPartition, List<String>> assignment
    ) {
        // We iterate through the target partitions and assign a memberId to them. In case we run out of members (members < targetPartitions),
        // we again start from the starting index of memberIds.
        Iterator<String> memberIdIterator = memberIds.iterator();
        for (TopicIdPartition topicPartition : partitionsToAssign) {
            if (!memberIdIterator.hasNext()) {
                memberIdIterator = memberIds.iterator();
            }
            String memberId = memberIdIterator.next();
            assignment.computeIfAbsent(topicPartition, k -> new ArrayList<>()).add(memberId);
        }
    }

    private GroupAssignment groupAssignment(
        Map<String, Set<TopicIdPartition>> assignmentByMember,
        Collection<String> allGroupMembers
    ) {
        Map<String, MemberAssignment> members = new HashMap<>();
        for (Map.Entry<String, Set<TopicIdPartition>> entry : assignmentByMember.entrySet()) {
            Map<Uuid, Set<Integer>> targetPartitions = new HashMap<>();
            entry.getValue().forEach(targetPartition ->
                targetPartitions.computeIfAbsent(targetPartition.topicId(), k -> new HashSet<>()).add(targetPartition.partitionId()));
            members.put(entry.getKey(), new MemberAssignmentImpl(targetPartitions));
        }
        allGroupMembers.forEach(member -> {
            if (!members.containsKey(member))
                members.put(member, new MemberAssignmentImpl(new HashMap<>()));
        });

        return new GroupAssignment(members);
    }
}
