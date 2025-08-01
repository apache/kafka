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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The RackAwareAssignor is a consumer group partition assignor that takes into account the rack
 * information of the members when assigning partitions to them.
 * It needs all brokers and members to have rack information available.
 */
public class RackAwareAssignor implements ConsumerGroupPartitionAssignor, ShareGroupPartitionAssignor {
    @Override
    public String name() {
        return "rack-aware-assignor";
    }

    @Override
    public GroupAssignment assign(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) throws PartitionAssignorException {
        Map<String, String> rackIdToMemberId = new HashMap<>();
        List<String> memberIds = new ArrayList<>(groupSpec.memberIds());
        for (String memberId : memberIds) {
            if (groupSpec.memberSubscription(memberId).rackId().isEmpty()) {
                throw new PartitionAssignorException("Member " + memberId + " does not have rack information available.");
            }
            rackIdToMemberId.put(
                groupSpec.memberSubscription(memberId).rackId().get(),
                memberId
            );
        }

        Map<String, Map<Uuid, Set<Integer>>> assignments = new HashMap<>();
        for (Uuid topicId : groupSpec.memberSubscription(memberIds.get(0)).subscribedTopicIds()) {
            int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            if (numPartitions == -1) {
                throw new PartitionAssignorException("Member is subscribed to a non-existent topic");
            }

            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                Set<String> racks = subscribedTopicDescriber.racksForPartition(topicId, partitionId);
                if (racks.isEmpty()) {
                    throw new PartitionAssignorException("No racks available for partition " + partitionId + " of topic " + topicId);
                }

                String assignedRack = null;
                for (String rack : racks) {
                    String memberId = rackIdToMemberId.get(rack);
                    if (memberId == null) {
                        continue;
                    }
                    assignedRack = rack;
                    break;
                }

                if (assignedRack == null) {
                    throw new PartitionAssignorException("No member found for racks " + racks + " for partition " + partitionId + " of topic " + topicId);
                }

                Map<Uuid, Set<Integer>> assignment = assignments.computeIfAbsent(
                    rackIdToMemberId.get(assignedRack),
                    k -> new HashMap<>()
                );
                Set<Integer> partitions = assignment.computeIfAbsent(
                    topicId,
                    k -> new java.util.HashSet<>()
                );
                partitions.add(partitionId);
            }
        }

        Map<String, MemberAssignment> memberAssignments = new HashMap<>();
        for (Map.Entry<String, Map<Uuid, Set<Integer>>> entry : assignments.entrySet()) {
            memberAssignments.put(entry.getKey(), new MemberAssignmentImpl(entry.getValue()));
        }
        return new GroupAssignment(memberAssignments);
    }
}
