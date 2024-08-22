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
package org.apache.kafka.raft.utils;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.ReplicaKey;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;


public class BeginQuorumEpochRpc {
    public static BeginQuorumEpochRequestData singletonBeginQuorumEpochRequest(
            TopicPartition topicPartition,
            String clusterId,
            int leaderEpoch,
            int leaderId,
            Endpoints leaderEndpoints,
            ReplicaKey voterKey
    ) {
        return new BeginQuorumEpochRequestData()
                .setClusterId(clusterId)
                .setVoterId(voterKey.id())
                .setTopics(
                        Collections.singletonList(
                                new BeginQuorumEpochRequestData.TopicData()
                                        .setTopicName(topicPartition.topic())
                                        .setPartitions(
                                                Collections.singletonList(
                                                        new BeginQuorumEpochRequestData.PartitionData()
                                                                .setPartitionIndex(topicPartition.partition())
                                                                .setLeaderEpoch(leaderEpoch)
                                                                .setLeaderId(leaderId)
                                                                .setVoterDirectoryId(voterKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
                                                )
                                        )
                        )
                )
                .setLeaderEndpoints(leaderEndpoints.toBeginQuorumEpochRequest());
    }

    public static BeginQuorumEpochResponseData singletonBeginQuorumEpochResponse(
            ListenerName listenerName,
            short apiVersion,
            Errors topLevelError,
            TopicPartition topicPartition,
            Errors partitionLevelError,
            int leaderEpoch,
            int leaderId,
            Endpoints endpoints
    ) {
        BeginQuorumEpochResponseData response = new BeginQuorumEpochResponseData()
                .setErrorCode(topLevelError.code())
                .setTopics(
                        Collections.singletonList(
                                new BeginQuorumEpochResponseData.TopicData()
                                        .setTopicName(topicPartition.topic())
                                        .setPartitions(
                                                Collections.singletonList(
                                                        new BeginQuorumEpochResponseData.PartitionData()
                                                                .setErrorCode(partitionLevelError.code())
                                                                .setLeaderId(leaderId)
                                                                .setLeaderEpoch(leaderEpoch)
                                                )
                                        )
                        )
                );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                BeginQuorumEpochResponseData.NodeEndpointCollection nodeEndpoints =
                        new BeginQuorumEpochResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                        new BeginQuorumEpochResponseData.NodeEndpoint()
                                .setNodeId(leaderId)
                                .setHost(address.get().getHostString())
                                .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

    public static boolean hasValidTopicPartition(BeginQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                data.topics().get(0).partitions().size() == 1 &&
                data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    public static boolean hasValidTopicPartition(BeginQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    public static Optional<ReplicaKey> beginQuorumEpochRequestVoterKey(
            BeginQuorumEpochRequestData request,
            BeginQuorumEpochRequestData.PartitionData partition
    ) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), partition.voterDirectoryId()));
        }
    }
}
