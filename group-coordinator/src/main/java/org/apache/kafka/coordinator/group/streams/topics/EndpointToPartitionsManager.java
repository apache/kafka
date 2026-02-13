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

package org.apache.kafka.coordinator.group.streams.topics;

import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class EndpointToPartitionsManager {

    private EndpointToPartitionsManager() {
    }

    /**
     * Creates endpoint-to-partitions mapping for a member if the member has a user endpoint.
     * Returns empty if the member has no user endpoint.
     *
     * @param streamsGroupMember The streams group member.
     * @param streamsGroup       The streams group.
     * @param metadataImage      The metadata image.
     * @return An Optional containing the EndpointToPartitions if the member has an endpoint, empty otherwise.
     */
    public static Optional<StreamsGroupHeartbeatResponseData.EndpointToPartitions> maybeEndpointToPartitions(
        final StreamsGroupMember streamsGroupMember,
        final StreamsGroup streamsGroup,
        final CoordinatorMetadataImage metadataImage
    ) {
        Optional<StreamsGroupMemberMetadataValue.Endpoint> endpointOptional = streamsGroupMember.userEndpoint();
        if (endpointOptional.isEmpty()) {
            return Optional.empty();
        }

        StreamsGroupMemberMetadataValue.Endpoint endpoint = endpointOptional.get();
        StreamsGroupHeartbeatResponseData.Endpoint responseEndpoint = new StreamsGroupHeartbeatResponseData.Endpoint();
        responseEndpoint.setHost(endpoint.host());
        responseEndpoint.setPort(endpoint.port());

        return Optional.of(endpointToPartitions(streamsGroupMember, responseEndpoint, streamsGroup, metadataImage));
    }

    public static StreamsGroupHeartbeatResponseData.EndpointToPartitions endpointToPartitions(final StreamsGroupMember streamsGroupMember,
                                                                                              final StreamsGroupHeartbeatResponseData.Endpoint responseEndpoint,
                                                                                              final StreamsGroup streamsGroup,
                                                                                              final CoordinatorMetadataImage metadataImage) {
        StreamsGroupHeartbeatResponseData.EndpointToPartitions endpointToPartitions = new StreamsGroupHeartbeatResponseData.EndpointToPartitions();
        Map<String, Set<Integer>> activeTasks = streamsGroupMember.assignedTasks().activeTasksWithEpochs().entrySet().stream()
            .collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                entry -> entry.getValue().keySet()
            ));
        Map<String, Set<Integer>> standbyTasks = streamsGroupMember.assignedTasks().standbyTasks();
        endpointToPartitions.setUserEndpoint(responseEndpoint);
        Map<String, ConfiguredSubtopology> configuredSubtopologies = streamsGroup.configuredTopology().flatMap(ConfiguredTopology::subtopologies).get();
        List<StreamsGroupHeartbeatResponseData.TopicPartition> activeTopicPartitions = topicPartitions(activeTasks, configuredSubtopologies, metadataImage);
        List<StreamsGroupHeartbeatResponseData.TopicPartition> standbyTopicPartitions = topicPartitions(standbyTasks, configuredSubtopologies, metadataImage);
        endpointToPartitions.setActivePartitions(activeTopicPartitions);
        endpointToPartitions.setStandbyPartitions(standbyTopicPartitions);
        return endpointToPartitions;
    }

    private static List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitions(final Map<String, Set<Integer>> tasks,
                                                                                          final Map<String, ConfiguredSubtopology> configuredSubtopologies,
                                                                                          final CoordinatorMetadataImage metadataImage) {
        List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitionsForTasks = new ArrayList<>();
        for (Map.Entry<String, Set<Integer>> taskEntry : tasks.entrySet()) {
            String subtopologyId = taskEntry.getKey();
            ConfiguredSubtopology configuredSubtopology = configuredSubtopologies.get(subtopologyId);
            Set<String> sourceTopics = configuredSubtopology.sourceTopics();
            Set<String> repartitionSourceTopics = configuredSubtopology.repartitionSourceTopics().keySet();
            Set<String> allSourceTopic = new HashSet<>(sourceTopics);
            allSourceTopic.addAll(repartitionSourceTopics);
            List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitionList = topicPartitionListForTask(taskEntry.getValue(), allSourceTopic, metadataImage);
            topicPartitionsForTasks.addAll(topicPartitionList);
        }
        return topicPartitionsForTasks;
    }

    private static List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitionListForTask(final Set<Integer> taskSet,
                                                                                                    final Set<String> topicNames,
                                                                                                    final CoordinatorMetadataImage metadataImage) {
        return topicNames.stream().map(topic -> {
            Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadata = metadataImage.topicMetadata(topic);
            if (topicMetadata.isEmpty()) {
                throw new IllegalStateException("Topic " + topic + " not found in metadata image");
            }
            int numPartitionsForTopic = topicMetadata.get().partitionCount();
            StreamsGroupHeartbeatResponseData.TopicPartition tp = new StreamsGroupHeartbeatResponseData.TopicPartition();
            tp.setTopic(topic);
            List<Integer> tpPartitions = new ArrayList<>(taskSet);
            if (numPartitionsForTopic < taskSet.size()) {
                Collections.sort(tpPartitions);
                tp.setPartitions(tpPartitions.subList(0, numPartitionsForTopic));
            } else {
                tp.setPartitions(tpPartitions);
            }
            return tp;
        }).toList();
    }
}