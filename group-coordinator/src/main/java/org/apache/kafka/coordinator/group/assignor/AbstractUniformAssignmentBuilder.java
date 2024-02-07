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
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The assignment builder is used to construct the target assignment based on the members' subscriptions.
 *
 * This class contains common utility methods and a class for obtaining and storing rack information.
 */
public abstract class AbstractUniformAssignmentBuilder {
    protected abstract GroupAssignment buildAssignment();

    /**
     * Determines if rack-aware assignment is appropriate based on the provided rack information.
     *
     * @param memberRacks               Racks where members are located.
     * @param allPartitionRacks         Racks where partitions are located.
     * @param racksPerPartition         Map of partitions to their associated racks.
     *
     * @return {@code true} if rack-aware assignment should be applied; {@code false} otherwise.
     */
    protected static boolean useRackAwareAssignment(
        Set<String> memberRacks,
        Set<String> allPartitionRacks,
        Map<TopicIdPartition, Set<String>> racksPerPartition
    ) {
        if (memberRacks.isEmpty() || Collections.disjoint(memberRacks, allPartitionRacks))
            return false;
        else {
            return !racksPerPartition.values().stream().allMatch(allPartitionRacks::equals);
        }
    }

    /**
     * Adds the topic's partition to the member's target assignment.
     */
    protected static void addPartitionToAssignment(
        Map<String, MemberAssignment> memberAssignments,
        String memberId,
        Uuid topicId,
        int partition
    ) {
        memberAssignments.get(memberId)
            .targetPartitions()
            .computeIfAbsent(topicId, __ -> new HashSet<>())
            .add(partition);
    }

    /**
     * Constructs a set of {@code TopicIdPartition} including all the given topic Ids based on their partition counts.
     *
     * @param topicIds                      Collection of topic Ids.
     * @param subscribedTopicDescriber      Describer to fetch partition counts for topics.
     *
     * @return Set of {@code TopicIdPartition} including all the provided topic Ids.
     */
    protected static Set<TopicIdPartition> topicIdPartitions(
        Collection<Uuid> topicIds,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        return topicIds.stream()
            .flatMap(topic -> IntStream
                .range(0, subscribedTopicDescriber.numPartitions(topic))
                .mapToObj(i -> new TopicIdPartition(topic, i))
            ).collect(Collectors.toSet());
    }

    /**
     * Processes partitions for the given topic Ids using the provided function.
     *
     * @param topicIds                   Collection of topic Ids.
     * @param subscribedTopicDescriber   Describer to fetch partition counts for topics.
     * @param func                       Function to apply on each {@code TopicIdPartition}.
     */
    protected static void processTopicIdPartitions(
        Collection<Uuid> topicIds,
        SubscribedTopicDescriber subscribedTopicDescriber,
        Consumer<TopicIdPartition> func
    ) {
        topicIds.stream()
            .flatMap(topic -> IntStream
                .range(0, subscribedTopicDescriber.numPartitions(topic))
                .mapToObj(i -> new TopicIdPartition(topic, i))
            ).forEach(func);
    }

    /**
     * Represents the rack information of members and partitions along with utility methods
     * to facilitate rack-aware assignment strategies for a given consumer group.
     */
    protected static class RackInfo {
        /**
         * Map of every member to its rack.
         */
        protected final Map<String, String> memberRacks;

        /**
         * Map of every partition to a list of its racks.
         */
        protected final Map<TopicIdPartition, Set<String>> partitionRacks;

        /**
         * List of members with the same rack as the partition.
         */
        protected final Map<TopicIdPartition, List<String>> membersWithSameRackAsPartition;

        /**
         * Indicates if a rack aware assignment can be done.
         * True if racks are defined for both members and partitions and there is an intersection between the sets.
         */
        protected final boolean useRackStrategy;

        /**
         * Constructs rack information based on the assignment specification and subscribed topics.
         *
         * @param assignmentSpec                The current assignment specification.
         * @param subscribedTopicDescriber      Topic and partition metadata of the subscribed topics.
         * @param topicIds                      List of topic Ids.
         */
        public RackInfo(
            AssignmentSpec assignmentSpec,
            SubscribedTopicDescriber subscribedTopicDescriber,
            Set<Uuid> topicIds
        ) {
            Map<String, List<String>> membersByRack = new HashMap<>();
            assignmentSpec.members().forEach((memberId, assignmentMemberSpec) ->
                assignmentMemberSpec.rackId().filter(r -> !r.isEmpty()).ifPresent(
                    rackId -> membersByRack.computeIfAbsent(rackId, __ -> new ArrayList<>()).add(memberId)
                )
            );

            Set<String> allPartitionRacks;
            Map<TopicIdPartition, Set<String>> racksPerPartition;

            if (membersByRack.isEmpty()) {
                allPartitionRacks = Collections.emptySet();
                racksPerPartition = Collections.emptyMap();
            } else {
                racksPerPartition = new HashMap<>();
                allPartitionRacks = new HashSet<>();
                processTopicIdPartitions(topicIds, subscribedTopicDescriber, tp -> {
                    Set<String> racks = subscribedTopicDescriber.racksForPartition(tp.topicId(), tp.partitionId());
                    racksPerPartition.put(tp, racks);
                    if (!racks.isEmpty()) allPartitionRacks.addAll(racks);
                });
            }

            if (useRackAwareAssignment(membersByRack.keySet(), allPartitionRacks, racksPerPartition)) {
                this.memberRacks = new HashMap<>(assignmentSpec.members().size());
                membersByRack.forEach((rack, rackMembers) -> rackMembers.forEach(c -> memberRacks.put(c, rack)));
                this.partitionRacks = racksPerPartition;
                useRackStrategy = true;
            } else {
                this.memberRacks = Collections.emptyMap();
                this.partitionRacks = Collections.emptyMap();
                useRackStrategy = false;
            }

            this.membersWithSameRackAsPartition = racksPerPartition.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().stream()
                        .flatMap(rack -> membersByRack.getOrDefault(rack, Collections.emptyList()).stream())
                        .distinct() // Ensure that there are no duplicate members
                        .collect(Collectors.toList())
                ));
        }

        /**
         * Determines if there's a mismatch between the member's rack and the partition's replica racks.
         *
         * <p> Racks are considered mismatched under the following conditions: (returns {@code true}):
         * <ul>
         *     <li> Member lacks an associated rack. </li>
         *     <li> Partition lacks associated replica racks. </li>
         *     <li> Member's rack isn't among the partition's replica racks. </li>
         * </ul>
         *
         * @param memberId      The member Id.
         * @param tp            The topic partition.
         * @return {@code true} for a mismatch; {@code false} if member and partition racks exist and align.
         */
        protected boolean racksMismatch(String memberId, TopicIdPartition tp) {
            String memberRack = memberRacks.get(memberId);
            Set<String> replicaRacks = partitionRacks.get(tp);
            return memberRack == null || (replicaRacks == null || !replicaRacks.contains(memberRack));
        }

        /**
         * Sort partitions in ascending order by number of members with matching racks.
         *
         * @param topicIdPartitions    The partitions to be sorted.
         * @return A sorted list of partitions with potential members in the same rack.
         */
        protected List<TopicIdPartition> sortPartitionsByRackMembers(Collection<TopicIdPartition> topicIdPartitions) {
            return topicIdPartitions.stream()
                .filter(tp -> membersWithSameRackAsPartition.containsKey(tp) && !membersWithSameRackAsPartition.get(tp).isEmpty())
                .sorted(Comparator.comparing(
                        (TopicIdPartition tp) -> membersWithSameRackAsPartition.getOrDefault(tp, Collections.emptyList()).size())
                    .thenComparing(TopicIdPartition::topicId)
                    .thenComparing(TopicIdPartition::partitionId))
                .collect(Collectors.toList());
        }

        /**
         * @return List of members with the same rack as any of the provided partition's replicas.
         *         Members are sorted in ascending order of number of partitions in the assignment.
         */
        protected List<String> getSortedMembersWithMatchingRack(
            TopicIdPartition topicIdPartition,
            Map<String, MemberAssignment> assignment
        ) {
            List<String> membersList = membersWithSameRackAsPartition.getOrDefault(
                topicIdPartition,
                Collections.emptyList()
            );

            // Sort the list based on the size of each member's assignment.
            membersList.sort((member1, member2) -> {
                int sum1 = assignment.get(member1).targetPartitions().values().stream().mapToInt(Set::size).sum();
                int sum2 = assignment.get(member2).targetPartitions().values().stream().mapToInt(Set::size).sum();

                return Integer.compare(sum1, sum2);
            });

            return membersList;
        }

        @Override
        public String toString() {
            return "RackInfo(" +
                "memberRacks=" + memberRacks +
                ", partitionRacks=" + partitionRacks +
                ")";
        }
    }
}
