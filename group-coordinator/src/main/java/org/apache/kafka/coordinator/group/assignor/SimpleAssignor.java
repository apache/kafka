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
import java.util.LinkedHashSet;
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
        return newAssignmentHomogeneous(groupSpec, subscribeTopicIds, targetPartitions, currentAssignment);
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
        return newAssignmentHeterogeneous(groupSpec, memberToPartitionsSubscription, currentAssignment);
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
        Set<Uuid> subscribeTopicIds,
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

        // Step 3: We combine current assignment and new assignment.
        // However, if any partitions are assigned members by step 1 in new assignment, and also have members in the current assignment assigned by 2,
        // the members assigned in the current assignment by step 2 are ignored.
        Map<TargetPartition, List<String>> currentAssignmentFiltered = filterCurrentAssignment(currentAssignment, assignedPartitions);
        Map<String, Set<TargetPartition>> finalAssignment = new HashMap<>();

        // When combining current assignment, we need to only consider the topics in current assignment that are also being
        // subscribed in the new assignment as well.
        currentAssignmentFiltered.forEach((targetPartition, members) -> {
            if (subscribeTopicIds.contains(targetPartition.topicId))
                members.forEach(member -> {
                    if (groupSpec.memberIds().contains(member))
                        finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition);
                });
        });
        newAssignment.forEach((targetPartition, members) -> members.forEach(member ->
            finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition)));

        return groupAssignment(finalAssignment, groupSpec.memberIds());
    }

    private GroupAssignment newAssignmentHeterogeneous(
        GroupSpec groupSpec,
        Map<String, List<TargetPartition>> memberToPartitionsSubscription,
        Map<TargetPartition, List<String>> currentAssignment) {

        // Exhaustive set of all subscribed topic partitions.
        Set<TargetPartition> targetPartitions = new LinkedHashSet<>();
        memberToPartitionsSubscription.values().forEach(targetPartitions::addAll);

        // Create a map for topic to members subscription.
        Map<Uuid, Set<String>> topicToMemberSubscription = new HashMap<>();
        memberToPartitionsSubscription.forEach((member, partitions) ->
            partitions.forEach(partition -> topicToMemberSubscription.computeIfAbsent(partition.topicId(), k -> new LinkedHashSet<>()).add(member)));

        Map<TargetPartition, List<String>> newAssignment = new HashMap<>();
        // Step 1: Hash member IDs to partitions.
        memberToPartitionsSubscription.forEach((member, partitions) ->
            memberHashAssignment(partitions, Collections.singletonList(member), newAssignment));

        // Step 2: Round-robin assignment for unassigned partitions which do not have members already assigned in the current assignment.
        Set<TargetPartition> assignedPartitions = new HashSet<>(newAssignment.keySet());
        Map<Uuid, List<TargetPartition>> unassignedPartitions = new HashMap<>();
        targetPartitions.forEach(targetPartition -> {
            if (!assignedPartitions.contains(targetPartition) && !currentAssignment.containsKey(targetPartition))
                unassignedPartitions.computeIfAbsent(targetPartition.topicId(), k -> new ArrayList<>()).add(targetPartition);
        });

        unassignedPartitions.keySet().forEach(unassignedTopic ->
            roundRobinAssignment(topicToMemberSubscription.get(unassignedTopic), unassignedPartitions.get(unassignedTopic), newAssignment));

        // Step 3: We combine current assignment and new assignment.
        // However, if any partitions are assigned members by step 1 in new assignment, and also have members in the current assignment assigned by 2,
        // the members assigned in the current assignment by step 2 are ignored.
        Map<TargetPartition, List<String>> currentAssignmentFiltered = filterCurrentAssignment(currentAssignment, assignedPartitions);
        Map<String, Set<TargetPartition>> finalAssignment = new HashMap<>();

        // When combining current assignment, we need to only consider the member topic subscription in current assignment
        // which is being subscribed in the new assignment as well.
        currentAssignmentFiltered.forEach((targetPartition, members) -> members.forEach(member -> {
            if (topicToMemberSubscription.getOrDefault(targetPartition.topicId(), Collections.emptySet()).contains(member))
                finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition);
        }));
        newAssignment.forEach((targetPartition, members) -> members.forEach(member ->
            finalAssignment.computeIfAbsent(member, k -> new HashSet<>()).add(targetPartition)));

        return groupAssignment(finalAssignment, groupSpec.memberIds());
    }

    private GroupAssignment groupAssignment(
        Map<String, Set<TargetPartition>> assignmentByMember,
        Collection<String> allGroupMembers) {
        Map<String, MemberAssignment> members = new HashMap<>();
        for (Map.Entry<String, Set<TargetPartition>> entry : assignmentByMember.entrySet()) {
            Map<Uuid, Set<Integer>> targetPartitions = new HashMap<>();
            entry.getValue().forEach(targetPartition -> targetPartitions.computeIfAbsent(targetPartition.topicId(), k -> new HashSet<>()).add(targetPartition.partition()));
            members.put(entry.getKey(), new MemberAssignmentImpl(targetPartitions));
        }
        allGroupMembers.forEach(member -> {
            if (!members.containsKey(member))
                members.put(member, new MemberAssignmentImpl(new HashMap<>()));
        });

        return new GroupAssignment(members);
    }

    private Map<TargetPartition, List<String>> filterCurrentAssignment(
        Map<TargetPartition, List<String>> currentAssignment,
        Set<TargetPartition> assignedPartitions) {
        // topic partitions which were a part of current assignment.
        List<TargetPartition> targetPartitions = currentAssignment.keySet().stream().toList();
        // members which were a part of current assignment.
        Set<String> members = new HashSet<>();
        currentAssignment.values().forEach(members::addAll);
        // Computing hash based assignment that would have occurred for the current assignment.
        Map<TargetPartition, List<String>> hashAssignment = new HashMap<>();
        memberHashAssignment(targetPartitions, members, hashAssignment);

        Map<TargetPartition, List<String>> filteredCurrentAssignment = new HashMap<>();
        currentAssignment.forEach((targetPartition, assignedMembers) -> {
            if (!assignedPartitions.contains(targetPartition)) {
                filteredCurrentAssignment.put(targetPartition, assignedMembers);
            } else {
                assignedMembers.forEach(assignedMember -> {
                    // only adding members which were added by step 1 for the current assignment for the assigned partitions.
                    if (hashAssignment.getOrDefault(targetPartition, Collections.emptyList()).contains(assignedMember))
                        filteredCurrentAssignment.computeIfAbsent(targetPartition, k -> new ArrayList<>()).add(assignedMember);
                });
            }
        });
        return filteredCurrentAssignment;
    }

    // Visible for testing.
    void memberHashAssignment(
        List<TargetPartition> targetPartitions,
        Collection<String> memberIds,
        Map<TargetPartition, List<String>> assignment) {
        if (!targetPartitions.isEmpty())
            for (String memberId : memberIds) {
                int topicPartitionIndex = Math.abs(memberId.hashCode() % targetPartitions.size());
                TargetPartition topicPartition = targetPartitions.get(topicPartitionIndex);
                assignment.computeIfAbsent(topicPartition, k -> new ArrayList<>()).add(memberId);
            }
    }

    // Visible for testing.
    void roundRobinAssignment(
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
