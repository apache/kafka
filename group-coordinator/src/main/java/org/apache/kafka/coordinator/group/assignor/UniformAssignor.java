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

import org.apache.kafka.coordinator.group.common.TopicIdToPartition;
import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UniformAssignor implements PartitionAssignor {

    private static final Logger log = LoggerFactory.getLogger(UniformAssignor.class);
    public static final String UNIFORM_ASSIGNOR_NAME = "uniform";
    @Override
    public String name() {
        return UNIFORM_ASSIGNOR_NAME;
    }

    @Override
    public GroupAssignment assign(AssignmentSpec assignmentSpec) throws PartitionAssignorException {
        AbstractAssignmentBuilder assignmentBuilder;
        if (allSubscriptionsEqual(assignmentSpec.members)) {
            log.debug("Detected that all consumers were subscribed to same set of topics, invoking the "
                    + "optimized assignment algorithm");
            assignmentBuilder = new OptimizedAssignmentBuilder(assignmentSpec);
        } else {
            assignmentBuilder = new GeneralAssignmentBuilder(assignmentSpec);
        }
        return assignmentInCorrectFormat(assignmentSpec.members.keySet(), assignmentBuilder.build());
    }

    private boolean allSubscriptionsEqual(Map<String, AssignmentMemberSpec> members) {
        boolean areAllSubscriptionsEqual = true;
        List<Uuid> firstSubscriptionList = members.values().iterator().next().subscribedTopics;
        for (AssignmentMemberSpec memberSpec : members.values()) {
            if (!firstSubscriptionList.equals(memberSpec.subscribedTopics)) {
                areAllSubscriptionsEqual = false;
                break;
            }
        }
        return areAllSubscriptionsEqual;
    }
    protected GroupAssignment assignmentInCorrectFormat(Set<String> membersKeySet, Map<String, List<TopicIdToPartition>> computedAssignment) {
        Map<String, MemberAssignment> members = new HashMap<>();
        if (computedAssignment.isEmpty()) {
            return new GroupAssignment(members);
        }
        for (String member : membersKeySet) {
            List<TopicIdToPartition> assignment = computedAssignment.get(member);
            Map<Uuid, Set<Integer>> topicToSetOfPartitions = new HashMap<>();
            for (TopicIdToPartition topicIdPartition : assignment) {
                Uuid topicId = topicIdPartition.topicId();
                Integer partition = topicIdPartition.partition();
                topicToSetOfPartitions.computeIfAbsent(topicId, k -> new HashSet<>());
                topicToSetOfPartitions.get(topicId).add(partition);
            }
            members.put(member, new MemberAssignment(topicToSetOfPartitions));
        }
        GroupAssignment finalAssignment = new GroupAssignment(members);
        System.out.println("Final group assignment is " + finalAssignment);
        return finalAssignment;
    }
    protected static abstract class AbstractAssignmentBuilder {

        final Map<Uuid, AssignmentTopicMetadata> metadataPerTopic;
        final Map<String, AssignmentMemberSpec> metadataPerMember;

        AbstractAssignmentBuilder(AssignmentSpec assignmentSpec) {
            this.metadataPerTopic = assignmentSpec.topics;
            this.metadataPerMember = assignmentSpec.members;
        }

        /**
         * Builds the assignment.
         *
         * @return Map from each member to the list of partitions assigned to them.
         */
        abstract Map<String, List<TopicIdToPartition>> build();

        protected List<TopicIdToPartition> getAllTopicPartitions(List<Uuid> listAllTopics) {
            List<TopicIdToPartition> allPartitions = new ArrayList<>();
            for (Uuid topic : listAllTopics) {
                int partitionCount = metadataPerTopic.get(topic).numPartitions;
                for (int i = 0; i < partitionCount; ++i) {
                    allPartitions.add(new TopicIdToPartition(topic, i, null));
                }
            }
            return allPartitions;
        }
    }
}
