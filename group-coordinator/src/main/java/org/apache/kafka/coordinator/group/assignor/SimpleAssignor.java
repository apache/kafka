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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

/**
 * A simple partition assignor that assigns each member all partitions of the subscribed topics.
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
            return new GroupAssignment(Collections.emptyMap());

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
        Set<Uuid> subscribeTopicIds = groupSpec.memberSubscription(groupSpec.memberIds().iterator().next())
            .subscribedTopicIds();
        if (subscribeTopicIds.isEmpty())
            return new GroupAssignment(Collections.emptyMap());

        // Subscribed topic partitions for the share group.
        List<TargetPartition> targetPartitions = computeTargetPartitions(
            subscribeTopicIds, subscribedTopicDescriber);

        // The current assignment from topic partition to members.
        Map<TargetPartition, List<String>> currentAssignment = currentAssignment(groupSpec);
        return newAssignmentHomogeneous(groupSpec, targetPartitions, currentAssignment);
    }

    private GroupAssignment assignHeterogeneous(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        Map<String, List<TargetPartition>> memberToPartitionsSubscription = new HashMap<>();
        for (String memberId : groupSpec.memberIds()) {
            MemberSubscription spec = groupSpec.memberSubscription(memberId);
            if (spec.subscribedTopicIds().isEmpty())
                continue;

            // Subscribed topic partitions for the share group member.
            List<TargetPartition> targetPartitions = computeTargetPartitions(
                spec.subscribedTopicIds(), subscribedTopicDescriber);
            memberToPartitionsSubscription.put(memberId, targetPartitions);
        }

        // The current assignment from topic partition to members.
        Map<TargetPartition, List<String>> currentAssignment = currentAssignment(groupSpec);
        return newAssignmentHeterogeneous(memberToPartitionsSubscription, currentAssignment);
    }

    // Get the current assignment for subscribed topic partitions to share group members.
    private Map<TargetPartition, List<String>> currentAssignment(GroupSpec groupSpec) {
        Map<TargetPartition, List<String>> assignment = new HashMap<>();
        Collection<String> members = groupSpec.memberIds();

        for (String member : members) {
            Map<Uuid, Set<Integer>> assignedTopicPartitions = groupSpec.memberAssignment(member).partitions();
            assignedTopicPartitions.forEach((topicId, partitions) -> partitions.forEach(
                partition -> assignment.computeIfAbsent(new TargetPartition(topicId, partition), k -> new ArrayList<>()).add(member)));
        }
        return assignment;
    }

    private GroupAssignment newAssignmentHomogeneous(
        GroupSpec groupSpec,
        List<TargetPartition> targetPartitions,
        Map<TargetPartition, List<String>> currentAssignment) {

        Map<TargetPartition, List<String>> newAssignment = new HashMap<>();
        // Step 1: Hash member IDs to partitions.
        memberHashAssignment(targetPartitions, groupSpec.memberIds(), newAssignment);

        // Step 2: Round-robin assignment for unassigned partitions which do not have members already assigned in the current assignment.
        Set<TargetPartition> assignedPartitions = new HashSet<>(newAssignment.keySet());
        List<TargetPartition> unassignedPartitions = targetPartitions.stream()
            .filter(targetPartition -> !assignedPartitions.contains(targetPartition))
            .filter(targetPartition -> !currentAssignment.containsKey(targetPartition))
            .collect(Collectors.toList());

        roundRobinAssignment(groupSpec.memberIds(), unassignedPartitions, newAssignment);

        // Step 3: Combine current and new assignment and only consider the hash based assignment for the current assignment.
        Map<TargetPartition, List<String>> currentAssignmentByHash = computePartitionsAssignmentByHash(currentAssignment);
        Map<String, Set<TargetPartition>> finalAssignment = new HashMap<>();

        currentAssignmentByHash.forEach((targetPartition, members) -> members.forEach(member ->
            finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition)));
        newAssignment.forEach((targetPartition, members) -> members.forEach(member ->
            finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition)));

        return groupAssignment(finalAssignment);
    }

    private GroupAssignment newAssignmentHeterogeneous(
        Map<String, List<TargetPartition>> memberToPartitionsSubscription,
        Map<TargetPartition, List<String>> currentAssignment) {

        // exhaustive set of all subscribed topic partitions
        Set<TargetPartition> targetPartitions = new HashSet<>();
        memberToPartitionsSubscription.values().forEach(targetPartitions::addAll);

        // Create an inverted map for partition to members subscription.
        Map<TargetPartition, List<String>> partitionToMemberSubscription = new HashMap<>();
        memberToPartitionsSubscription.forEach((member, partitions) ->
            partitions.forEach(partition -> partitionToMemberSubscription.computeIfAbsent(partition, k -> new ArrayList<>()).add(member)));

        Map<TargetPartition, List<String>> newAssignment = new HashMap<>();
        // Step 1: Hash member IDs to partitions.
        memberToPartitionsSubscription.forEach((member, partitions) ->
            memberHashAssignment(partitions, Collections.singletonList(member), newAssignment));

        // Step 2: Round-robin assignment for unassigned partitions which do not have members already assigned in the current assignment.
        Set<TargetPartition> assignedPartitions = new HashSet<>(newAssignment.keySet());
        List<TargetPartition> unassignedPartitions = targetPartitions.stream()
            .filter(targetPartition -> !assignedPartitions.contains(targetPartition))
            .filter(targetPartition -> !currentAssignment.containsKey(targetPartition))
            .toList();

        unassignedPartitions.forEach(unassignedPartition ->
            roundRobinAssignment(partitionToMemberSubscription.get(unassignedPartition), Collections.singletonList(unassignedPartition), newAssignment));

        // Step 3: Combine current and new assignment and only consider the hash based assignment for the current assignment.
        Map<TargetPartition, List<String>> currentAssignmentByHash = computePartitionsAssignmentByHash(currentAssignment);
        Map<String, Set<TargetPartition>> finalAssignment = new HashMap<>();

        // When combining current assignment, we need to consider the member subscription in current assignment and
        // the subscription being used for new assignment.
        currentAssignmentByHash.forEach((targetPartition, members) -> members.forEach(member -> {
            if (partitionToMemberSubscription.get(targetPartition).contains(member))
                finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition);
        }));
        newAssignment.forEach((targetPartition, members) -> members.forEach(member ->
            finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition)));

        return groupAssignment(finalAssignment);
    }

    private GroupAssignment groupAssignment(
        Map<String, Set<TargetPartition>> assignmentByMember) {
        Map<String, MemberAssignment> members = new HashMap<>();
        for (Map.Entry<String, Set<TargetPartition>> entry : assignmentByMember.entrySet()) {
            Map<Uuid, Set<Integer>> targetPartitions = new HashMap<>();
            entry.getValue().forEach(targetPartition -> targetPartitions.computeIfAbsent(targetPartition.topicId(), k -> new HashSet<>()).add(targetPartition.partition()));
            members.put(entry.getKey(), new MemberAssignmentImpl(targetPartitions));
        }
        return new GroupAssignment(members);
    }

    Map<TargetPartition, List<String>> computePartitionsAssignmentByHash(
        Map<TargetPartition, List<String>> currentAssignment) {
        List<TargetPartition> targetPartitions = currentAssignment.keySet().stream().toList();
        Set<String> members = new HashSet<>();
        currentAssignment.values().forEach(members::addAll);
        Map<TargetPartition, List<String>> hashAssignment = new HashMap<>();
        memberHashAssignment(targetPartitions, members, hashAssignment);
        return hashAssignment;
    }

    private void memberHashAssignment(
        List<TargetPartition> targetPartitions,
        Collection<String> memberIds,
        Map<TargetPartition, List<String>> assignment) {
        for (String memberId : memberIds) {
            int topicPartitionIndex = Math.abs(memberId.hashCode() % targetPartitions.size());
            TargetPartition topicPartition = targetPartitions.get(topicPartitionIndex);
            assignment.computeIfAbsent(topicPartition, k -> new ArrayList<>()).add(memberId);
        }
    }

    private void roundRobinAssignment(
        Collection<String> members,
        List<TargetPartition> partitions,
        Map<TargetPartition, List<String>> assignment) {
        Iterator<String> memberIterator = members.iterator();
        for (TargetPartition targetPartition : partitions) {
            if (!memberIterator.hasNext()) {
                memberIterator = members.iterator();
            }
            String member = memberIterator.next();
            assignment.computeIfAbsent(targetPartition, k -> new ArrayList<>()).add(member);
        }
    }

    private List<TargetPartition> computeTargetPartitions(
        Set<Uuid> subscribeTopicIds,
        SubscribedTopicDescriber subscribedTopicDescriber) {
        List<TargetPartition> targetPartitions = new ArrayList<>();
        subscribeTopicIds.forEach(topicId -> {
            int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            if (numPartitions == -1) {
                throw new PartitionAssignorException(
                    "Members are subscribed to topic " + topicId
                        + " which doesn't exist in the topic metadata."
                );
            }

            for (int i = 0; i < numPartitions; i++) {
                targetPartitions.add(new TargetPartition(topicId, i));
            }
        });
        return targetPartitions;
    }

    static class TargetPartition {
        Uuid topicId;
        int partition;

        TargetPartition(Uuid topicId, int partition) {
            this.topicId = topicId;
            this.partition = partition;
        }

        Uuid topicId() {
            return topicId;
        }

        int partition() {
            return partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TargetPartition that = (TargetPartition) o;
            return topicId.equals(that.topicId) && partition == that.partition;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicId, partition);
        }
    }
}
