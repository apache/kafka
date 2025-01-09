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


    public EndpointToPartitionsManager() {}

    public StreamsGroupHeartbeatResponseData.EndpointToPartitions endpointToPartitions(final StreamsGroupMember streamsGroupMember,
                                                                                       final StreamsGroupHeartbeatResponseData.Endpoint responseEndpoint,
                                                                                       final StreamsGroup streamsGroup) {

        StreamsGroupHeartbeatResponseData.EndpointToPartitions endpointToPartitions = new StreamsGroupHeartbeatResponseData.EndpointToPartitions();
        List<StreamsGroupHeartbeatResponseData.TopicPartition> allTopicPartitions = new ArrayList<>();
        for (Map.Entry<String, ConfiguredSubtopology> entry : streamsGroup.configuredTopology().subtopologies().entrySet()) {
            ConfiguredSubtopology configuredSubtopology = entry.getValue();
            endpointToPartitions.setUserEndpoint(responseEndpoint);
            final Map<String, TopicMetadata> groupTopicMetadata = streamsGroup.partitionMetadata();
            getActiveTaskTopicPartitions(configuredSubtopology, groupTopicMetadata, streamsGroupMember, allTopicPartitions);
        }
        endpointToPartitions.setPartitions(allTopicPartitions);
        return endpointToPartitions;
    }

    private void getActiveTaskTopicPartitions(final ConfiguredSubtopology configuredSubtopology,
                                              final Map<String, TopicMetadata> groupTopicMetadata,
                                              final StreamsGroupMember streamsGroupMember,
                                              final List<StreamsGroupHeartbeatResponseData.TopicPartition> allTopicPartitions) {

        List<String> standbyTopicNames = configuredSubtopology.nonSourceChangelogTopics().stream().map(ConfiguredInternalTopic::name).toList();

        for (Map.Entry<String, Set<Integer>> taskEntry : streamsGroupMember.assignedActiveTasks().entrySet()) {
            Set<Integer> taskPartitions = taskEntry.getValue();
            List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitionList =
                    Stream.concat(
                            configuredSubtopology.sourceTopics().stream(),
                            configuredSubtopology.repartitionSourceTopics().keySet().stream()
            ).map(topic -> {
                int numPartitionsForTopic = groupTopicMetadata.get(topic).numPartitions();
                StreamsGroupHeartbeatResponseData.TopicPartition tp = new StreamsGroupHeartbeatResponseData.TopicPartition();
                tp.setTopic(topic);
                List<Integer> tpPartitions = new ArrayList<>(taskPartitions);
                if (numPartitionsForTopic < taskPartitions.size()) {
                    Collections.sort(tpPartitions);
                    tp.setPartitions(tpPartitions.subList(0, numPartitionsForTopic));
                } else {
                    tp.setPartitions(tpPartitions);
                }
                return tp;
            }).toList();
            allTopicPartitions.addAll(topicPartitionList);
        }
    }

}
