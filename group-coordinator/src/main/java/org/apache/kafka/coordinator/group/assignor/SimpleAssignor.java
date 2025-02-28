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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

/**
 * A simple partition assignor that assigns partitions of the subscribed topics based on the rules defined in KIP-932 to different members.
 */
public class SimpleAssignor implements ShareGroupPartitionAssignor {

    private static final String SIMPLE_ASSIGNOR_NAME = "simple";

    @Override
    public String name() {
        return SIMPLE_ASSIGNOR_NAME;
    }

    @Override
    public GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        if (groupSpec.memberIds().isEmpty())
            return new GroupAssignment(Map.of());

        if (groupSpec.subscriptionType().equals(HOMOGENEOUS)) {
            return assignHomogenous(groupSpec, subscribedTopicDescriber);
        } else {
            return assignHeterogeneous(groupSpec, subscribedTopicDescriber);
        }
    }

    private GroupAssignment assignHomogenous(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        Set<Uuid> subscribedTopicIds = groupSpec.memberSubscription(groupSpec.memberIds().iterator().next())
            .subscribedTopicIds();
        if (subscribedTopicIds.isEmpty())
            return new GroupAssignment(Map.of());

        // Subscribed topic partitions for the share group.
        List<TopicIdPartition> targetPartitions = computeTargetPartitions(
            subscribedTopicIds, subscribedTopicDescriber);

        // The current assignment from topic partition to members.
        Map<TopicIdPartition, List<String>> currentAssignment = currentAssignment(groupSpec);
        return newAssignmentHomogeneous(groupSpec, subscribedTopicIds, targetPartitions, currentAssignment);
    }

    private GroupAssignment assignHeterogeneous(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        Map<String, List<TopicIdPartition>> memberToPartitionsSubscription = new HashMap<>();
        for (String memberId : groupSpec.memberIds()) {
            MemberSubscription spec = groupSpec.memberSubscription(memberId);
            if (spec.subscribedTopicIds().isEmpty())
                continue;

            // Subscribed topic partitions for the share group member.
            List<TopicIdPartition> targetPartitions = computeTargetPartitions(
                spec.subscribedTopicIds(), subscribedTopicDescriber);
            memberToPartitionsSubscription.put(memberId, targetPartitions);
        }

        // The current assignment from topic partition to members.
        Map<TopicIdPartition, List<String>> currentAssignment = currentAssignment(groupSpec);
        return newAssignmentHeterogeneous(groupSpec, memberToPartitionsSubscription, currentAssignment);
    }

    /**
     * Get the current assignment by topic partitions.
     * @param groupSpec - The group metadata specifications.
     * @return the current assignment for subscribed topic partitions to memberIds.
     */
    private Map<TopicIdPartition, List<String>> currentAssignment(GroupSpec groupSpec) {
        Map<TopicIdPartition, List<String>> assignment = new HashMap<>();

        for (String member : groupSpec.memberIds()) {
            Map<Uuid, Set<Integer>> assignedTopicPartitions = groupSpec.memberAssignment(member).partitions();
            assignedTopicPartitions.forEach((topicId, partitions) -> partitions.forEach(
                partition -> assignment.computeIfAbsent(new TopicIdPartition(topicId, partition), k -> new ArrayList<>()).add(member)));
        }
        return assignment;
    }

    /**
     * This function computes the new assignment for a homogeneous group.
     * @param groupSpec - The group metadata specifications.
     * @param subscribedTopicIds - The set of all the subscribed topic ids for the group.
     * @param targetPartitions - The list of all topic partitions that need assignment.
     * @param currentAssignment - The current assignment for subscribed topic partitions to memberIds.
     * @return the new partition assignment for the members of the group.
     */
    private GroupAssignment newAssignmentHomogeneous(
        GroupSpec groupSpec,
        Set<Uuid> subscribedTopicIds,
        List<TopicIdPartition> targetPartitions,
        Map<TopicIdPartition, List<String>> currentAssignment
    ) {
        Map<TopicIdPartition, List<String>> newAssignment = new HashMap<>();

        // Step 1: Hash member IDs to topic partitions.
        memberHashAssignment(targetPartitions, groupSpec.memberIds(), newAssignment);

        // Step 2: Round-robin assignment for unassigned partitions which do not have members already assigned in the current assignment.
        List<TopicIdPartition> unassignedPartitions = targetPartitions.stream()
            .filter(targetPartition -> !newAssignment.containsKey(targetPartition))
            .filter(targetPartition -> !currentAssignment.containsKey(targetPartition))
            .toList();

        roundRobinAssignment(groupSpec.memberIds(), unassignedPartitions, newAssignment);

        // Step 3: We combine current assignment and new assignment.
        Map<String, Set<TopicIdPartition>> finalAssignment = new HashMap<>();

        // As per the KIP, we should revoke the assignments from current assignment for partitions that were assigned by step 1
        // in the new assignment and have members in current assignment by step 2. But we haven't implemented it to avoid the
        // complexity in both the implementation and the run time complexity. This step was mentioned in the KIP to reduce
        // the burden of certain members of the share groups. This can be achieved with the help of limiting the max
        // no. of partitions assignment for every member(KAFKA-18788). Hence, the potential problem of burdening
        // the share consumers will be addressed in a future PR.

        newAssignment.forEach((targetPartition, members) -> members.forEach(member ->
            finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition)));
        // When combining current assignment, we need to only consider the topics in current assignment that are also being
        // subscribed in the new assignment as well.
        currentAssignment.forEach((targetPartition, members) -> {
            if (subscribedTopicIds.contains(targetPartition.topicId()))
                members.forEach(member -> {
                    if (groupSpec.memberIds().contains(member) && !newAssignment.containsKey(targetPartition))
                        finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition);
                });
        });

        return groupAssignment(finalAssignment, groupSpec.memberIds());
    }

    /**
     * This function computes the new assignment for a heterogeneous group.
     * @param groupSpec - The group metadata specifications.
     * @param memberToPartitionsSubscription - The member to subscribed topic partitions map.
     * @param currentAssignment - The current assignment for subscribed topic partitions to memberIds.
     * @return the new partition assignment for the members of the group.
     */
    private GroupAssignment newAssignmentHeterogeneous(
        GroupSpec groupSpec,
        Map<String, List<TopicIdPartition>> memberToPartitionsSubscription,
        Map<TopicIdPartition, List<String>> currentAssignment
    ) {

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
            memberHashAssignment(partitions, List.of(member), newAssignment));

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
        Map<String, Set<TopicIdPartition>> finalAssignment = new HashMap<>();
        // As per the KIP, we should revoke the assignments from current assignment for partitions that were assigned by step 1
        // in the new assignment and have members in current assignment by step 2. But we haven't implemented it to avoid the
        // complexity in both the implementation and the run time complexity. This step was mentioned in the KIP to reduce
        // the burden of certain members of the share groups. This can be achieved with the help of limiting the max
        // no. of partitions assignment for every member(KAFKA-18788). Hence, the potential problem of burdening
        // the share consumers will be addressed in a future PR.

        newAssignment.forEach((targetPartition, members) -> members.forEach(member ->
            finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition)));
        // When combining current assignment, we need to only consider the member topic subscription in current assignment
        // which is being subscribed in the new assignment as well.
        currentAssignment.forEach((topicIdPartition, members) -> members.forEach(member -> {
            if (topicToMemberSubscription.getOrDefault(topicIdPartition.topicId(), Collections.emptySet()).contains(member)
                && !newAssignment.containsKey(topicIdPartition))
                finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(topicIdPartition);
        }));

        return groupAssignment(finalAssignment, groupSpec.memberIds());
    }

    private GroupAssignment groupAssignment(
        Map<String, Set<TopicIdPartition>> assignmentByMember,
        Collection<String> allGroupMembers
    ) {
        Map<String, MemberAssignment> members = new HashMap<>();
        for (Map.Entry<String, Set<TopicIdPartition>> entry : assignmentByMember.entrySet()) {
            Map<Uuid, Set<Integer>> targetPartitions = new HashMap<>();
            entry.getValue().forEach(targetPartition -> targetPartitions.computeIfAbsent(targetPartition.topicId(), k -> new HashSet<>()).add(targetPartition.partitionId()));
            members.put(entry.getKey(), new MemberAssignmentImpl(targetPartitions));
        }
        allGroupMembers.forEach(member -> {
            if (!members.containsKey(member))
                members.put(member, new MemberAssignmentImpl(new HashMap<>()));
        });

        return new GroupAssignment(members);
    }

    /**
     * This function updates assignment by hashing the member IDs of the members and maps the partitions assigned to the
     * members based on the hash. This gives approximately even balance.
     * @param unassignedPartitions - the subscribed topic partitions which needs assignment.
     * @param memberIds - the member ids to which the topic partitions need to be assigned.
     * @param assignment - the existing assignment by topic partition. We need to pass it as a parameter because this
     *                   function would be called multiple times for heterogeneous assignment.
     */
    // Visible for testing
    void memberHashAssignment(
        List<TopicIdPartition> unassignedPartitions,
        Collection<String> memberIds,
        Map<TopicIdPartition, List<String>> assignment
    ) {
        if (!unassignedPartitions.isEmpty())
            for (String memberId : memberIds) {
                int topicPartitionIndex = Math.abs(memberId.hashCode() % unassignedPartitions.size());
                TopicIdPartition topicPartition = unassignedPartitions.get(topicPartitionIndex);
                assignment.computeIfAbsent(topicPartition, k -> new ArrayList<>()).add(memberId);
            }
    }

    /**
     * This functions assigns topic partitions to members by round-robin approach and updates the existing assignment.
     * @param memberIds - the member ids to which the topic partitions need to be assigned, should be non-empty.
     * @param unassignedPartitions - the subscribed topic partitions which needs assignment.
     * @param assignment - the existing assignment by topic partition.
     */
    // Visible for testing
    void roundRobinAssignment(
        Collection<String> memberIds,
        List<TopicIdPartition> unassignedPartitions,
        Map<TopicIdPartition, List<String>> assignment
    ) {
        // We iterate through the target partitions and assign a memberId to them. In case we run out of members (members < targetPartitions),
        // we again start from the starting index of memberIds.
        Iterator<String> memberIdIterator = memberIds.iterator();
        for (TopicIdPartition targetPartition : unassignedPartitions) {
            if (!memberIdIterator.hasNext()) {
                memberIdIterator = memberIds.iterator();
            }
            String memberId = memberIdIterator.next();
            assignment.computeIfAbsent(targetPartition, k -> new ArrayList<>()).add(memberId);
        }
    }

    private List<TopicIdPartition> computeTargetPartitions(
        Set<Uuid> subscribedTopicIds,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        List<TopicIdPartition> targetPartitions = new ArrayList<>();
        subscribedTopicIds.forEach(topicId -> {
            int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            if (numPartitions == -1) {
                throw new PartitionAssignorException(
                    "Members are subscribed to topic " + topicId
                        + " which doesn't exist in the topic metadata."
                );
            }

            for (int i = 0; i < numPartitions; i++) {
                targetPartitions.add(new TopicIdPartition(topicId, i));
            }
        });
        return targetPartitions;
    }
}
