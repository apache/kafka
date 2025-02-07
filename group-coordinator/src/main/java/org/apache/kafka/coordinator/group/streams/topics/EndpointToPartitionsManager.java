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
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TopicMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class EndpointToPartitionsManager {

    private EndpointToPartitionsManager() {}

    public static StreamsGroupHeartbeatResponseData.EndpointToPartitions endpointToPartitions(final StreamsGroupMember streamsGroupMember,
                                                                                       final StreamsGroupHeartbeatResponseData.Endpoint responseEndpoint,
                                                                                       final StreamsGroup streamsGroup) {
        StreamsGroupHeartbeatResponseData.EndpointToPartitions endpointToPartitions = new StreamsGroupHeartbeatResponseData.EndpointToPartitions();
        List<StreamsGroupHeartbeatResponseData.TopicPartition> activeTopicPartitions = new ArrayList<>();
        List<StreamsGroupHeartbeatResponseData.TopicPartition> standbyTopicPartitions = new ArrayList<>();
        for (Map.Entry<String, ConfiguredSubtopology> entry : streamsGroup.configuredTopology().subtopologies().entrySet()) {
            ConfiguredSubtopology configuredSubtopology = entry.getValue();
            endpointToPartitions.setUserEndpoint(responseEndpoint);
            Set<String> sourceTopics = configuredSubtopology.sourceTopics();
            Set<String> repartitionSourceTopics = configuredSubtopology.repartitionSourceTopics().keySet();

            final Map<String, TopicMetadata> groupTopicMetadata = streamsGroup.partitionMetadata();
            Set<Map.Entry<String, Set<Integer>>> taskEntrySet = streamsGroupMember.assignedActiveTasks().entrySet();
            List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitionList = getTopicPartitions(taskEntrySet, sourceTopics, repartitionSourceTopics, groupTopicMetadata);
            activeTopicPartitions.addAll(topicPartitionList);

            Set<Map.Entry<String, Set<Integer>>> standbyTaskEntrySet = streamsGroupMember.assignedStandbyTasks().entrySet();
            List<StreamsGroupHeartbeatResponseData.TopicPartition> standbyList = getTopicPartitions(standbyTaskEntrySet, sourceTopics, repartitionSourceTopics, groupTopicMetadata);
            standbyTopicPartitions.addAll(standbyList);
        }
        endpointToPartitions.setActivePartitions(activeTopicPartitions);
        endpointToPartitions.setStandbyPartitions(standbyTopicPartitions);
        return endpointToPartitions;
    }

    private static List<StreamsGroupHeartbeatResponseData.TopicPartition> getTopicPartitions(final Set<Map.Entry<String, Set<Integer>>> taskEntrySet,
                                                                                      final Set<String> topicNames,
                                                                                      final Set<String> repartitionTopicNames,
                                                                                      final Map<String, TopicMetadata> groupTopicMetadata) {
        final List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitionsForTasks = new ArrayList<>();
        for (Map.Entry<String, Set<Integer>> taskEntry : taskEntrySet) {
            List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitionList =
                    Stream.concat(topicNames.stream(), repartitionTopicNames.stream()).map(topic -> {
                        int numPartitionsForTopic = groupTopicMetadata.get(topic).numPartitions();
                        StreamsGroupHeartbeatResponseData.TopicPartition tp = new StreamsGroupHeartbeatResponseData.TopicPartition();
                        tp.setTopic(topic);
                        List<Integer> tpPartitions = new ArrayList<>(taskEntry.getValue());
                        if (numPartitionsForTopic < taskEntry.getValue().size()) {
                            Collections.sort(tpPartitions);
                            tp.setPartitions(tpPartitions.subList(0, numPartitionsForTopic));
                        } else {
                            tp.setPartitions(tpPartitions);
                        }
                        return tp;
                    }).toList();
            topicPartitionsForTasks.addAll(topicPartitionList);
        }
        return topicPartitionsForTasks;
    }
}
