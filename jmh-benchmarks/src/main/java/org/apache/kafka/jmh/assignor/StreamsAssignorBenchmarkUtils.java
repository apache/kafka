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
package org.apache.kafka.jmh.assignor;

import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredInternalTopic;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;


public class StreamsAssignorBenchmarkUtils {

    /**
     * Creates a GroupSpec from the given StreamsGroupMembers.
     *
     * @param members               The StreamsGroupMembers.
     * @param assignmentConfigs     The assignment configs.
     *
     * @return The new GroupSpec.
     */
    public static GroupSpec createGroupSpec(
        Map<String, StreamsGroupMember> members,
        Map<String, String> assignmentConfigs
    ) {
        Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();

        // Prepare the member spec for all members.
        for (Map.Entry<String, StreamsGroupMember> memberEntry : members.entrySet()) {
            String memberId = memberEntry.getKey();
            StreamsGroupMember member = memberEntry.getValue();

            memberSpecs.put(memberId, new AssignmentMemberSpec(
                member.instanceId(),
                member.rackId(),
                Map.of(),
                Map.of(),
                Map.of(),
                member.processId(),
                member.clientTags(),
                Map.of(),
                Map.of()
            ));
        }

        return new GroupSpecImpl(
            memberSpecs,
            assignmentConfigs
        );
    }

    /**
     * Creates a StreamsGroupMembers map where all members have the same topic subscriptions.
     *
     * @param memberCount           The number of members in the group.
     * @param membersPerProcess     The number of members per process.
     *
     * @return The new StreamsGroupMembers map.
     */
    public static Map<String, StreamsGroupMember> createStreamsMembers(
        int memberCount,
        int membersPerProcess
    ) {
        Map<String, StreamsGroupMember> members = new HashMap<>();

        for (int i = 0; i < memberCount; i++) {
            String memberId = "member-" + i;
            String processId = "process-" + i / membersPerProcess;

            members.put(memberId, StreamsGroupMember.Builder.withDefaults(memberId)
                    .setProcessId(processId)
                    .build());
        }

        return members;
    }

    /**
     * Creates a subtopology map with the given number of partitions per topic and a list of topic names.
     * For simplicity, each subtopology is associated with a single topic, and every second subtopology
     * is stateful (i.e., has a changelog topic).
     *
     * The number of topics a subtopology is associated with is irrelevant, and
     * so is the number of changelog topics.
     *
     * @param partitionsPerTopic The number of partitions per topic, implies the number of tasks for the subtopology.
     * @param allTopicNames All topics names.
     * @return A sorted map of subtopology IDs to ConfiguredSubtopology objects.
     */
    public static SortedMap<String, ConfiguredSubtopology> createSubtopologyMap(
        int partitionsPerTopic,
        List<String> allTopicNames
    ) {
        TreeMap<String, ConfiguredSubtopology> subtopologyMap = new TreeMap<>();
        for (int i = 0; i < allTopicNames.size(); i++) {
            String topicName = allTopicNames.get(i);
            if (i % 2 == 0) {
                subtopologyMap.put(topicName + "_subtopology", new ConfiguredSubtopology(partitionsPerTopic, Set.of(topicName), Map.of(), Set.of(), Map.of(
                    topicName + "_changelog", new ConfiguredInternalTopic(
                        topicName + "_changelog",
                        partitionsPerTopic,
                        Optional.empty(),
                        Map.of()
                    )
                )));
            } else {
                subtopologyMap.put(topicName + "_subtopology", new ConfiguredSubtopology(partitionsPerTopic, Set.of(topicName), Map.of(), Set.of(), Map.of()));
            }
        }
        return subtopologyMap;
    }
}
