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

import org.apache.kafka.coordinator.group.api.streams.assignor.AssignmentConfigs;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberMetadataAndStateImpl;
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
     * @param taskOffsets           The reported offset sums per member, empty for members reporting none.
     *
     * @return The new GroupSpec.
     */
    public static GroupSpec createGroupSpec(
        Map<String, StreamsGroupMember> members,
        AssignmentConfigs assignmentConfigs,
        Map<String, Map<String, Map<Integer, Long>>> taskOffsets
    ) {
        Map<String, MemberMetadataAndStateImpl> memberSpecs = new HashMap<>();

        // Prepare the member spec for all members.
        for (Map.Entry<String, StreamsGroupMember> memberEntry : members.entrySet()) {
            String memberId = memberEntry.getKey();
            StreamsGroupMember member = memberEntry.getValue();

            memberSpecs.put(memberId, new MemberMetadataAndStateImpl(
                member.instanceId(),
                member.rackId(),
                member.processId(),
                member.clientTags(),
                Map.of(),
                Map.of(),
                Map.of(),
                taskOffsets.getOrDefault(memberId, Map.of()),
                Map.of()
            ));
        }

        return new GroupSpecImpl(
            memberSpecs,
            assignmentConfigs
        );
    }

    /**
     * Creates the offset sums the members report for the state they hold on local disk.
     *
     * Tasks of stateful subtopologies are spread round-robin over the members, so every member reports the tasks
     * it would have owned in an earlier generation. Each dormant replica makes a following member report the same
     * task with a lower offset sum, so several members compete as candidates for it and the candidate ranking
     * actually has something to sort.
     *
     * @param memberIds         The members to spread the tasks over, in a stable order.
     * @param subtopologyMap    The subtopologies; only the stateful ones get offsets.
     * @param dormantReplicas   The number of members reporting a task on top of the one owning it.
     *
     * @return The reported offset sums, per member.
     */
    public static Map<String, Map<String, Map<Integer, Long>>> createTaskOffsets(
        List<String> memberIds,
        SortedMap<String, ConfiguredSubtopology> subtopologyMap,
        int dormantReplicas
    ) {
        Map<String, Map<String, Map<Integer, Long>>> taskOffsets = new HashMap<>();

        int taskIndex = 0;
        for (Map.Entry<String, ConfiguredSubtopology> subtopologyEntry : subtopologyMap.entrySet()) {
            ConfiguredSubtopology subtopology = subtopologyEntry.getValue();
            if (subtopology.stateChangelogTopics().isEmpty()) {
                continue;
            }

            for (int partition = 0; partition < subtopology.numberOfTasks(); partition++) {
                for (int replica = 0; replica <= dormantReplicas; replica++) {
                    String memberId = memberIds.get((taskIndex + replica) % memberIds.size());
                    // The owner holds the most caught-up state, every dormant copy a little less.
                    long offsetSum = (long) (dormantReplicas + 1 - replica) * 1_000_000L + taskIndex;

                    taskOffsets
                        .computeIfAbsent(memberId, id -> new HashMap<>())
                        .computeIfAbsent(subtopologyEntry.getKey(), id -> new HashMap<>())
                        .put(partition, offsetSum);
                }
                taskIndex++;
            }
        }

        return taskOffsets;
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
