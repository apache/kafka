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
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.raft.LeaderState;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.ReplicaKey;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DescribeQuorumRpc {
    public static DescribeQuorumRequestData singletonDescribeQuorumRequest(
            TopicPartition topicPartition
    ) {
        return new DescribeQuorumRequestData()
                .setTopics(
                        Collections.singletonList(
                                new DescribeQuorumRequestData.TopicData()
                                        .setTopicName(topicPartition.topic())
                                        .setPartitions(
                                                Collections.singletonList(
                                                        new DescribeQuorumRequestData.PartitionData()
                                                                .setPartitionIndex(topicPartition.partition())
                                                )
                                        )
                        )
                );
    }

    public static DescribeQuorumResponseData singletonDescribeQuorumResponse(
            short apiVersion,
            TopicPartition topicPartition,
            int leaderId,
            int leaderEpoch,
            long highWatermark,
            Collection<LeaderState.ReplicaState> voters,
            Collection<LeaderState.ReplicaState> observers,
            long currentTimeMs
    ) {
        DescribeQuorumResponseData response = new DescribeQuorumResponseData()
                .setTopics(
                        Collections.singletonList(
                                new DescribeQuorumResponseData.TopicData()
                                        .setTopicName(topicPartition.topic())
                                        .setPartitions(
                                                Collections.singletonList(
                                                        new DescribeQuorumResponseData.PartitionData()
                                                                .setPartitionIndex(topicPartition.partition())
                                                                .setErrorCode(Errors.NONE.code())
                                                                .setLeaderId(leaderId)
                                                                .setLeaderEpoch(leaderEpoch)
                                                                .setHighWatermark(highWatermark)
                                                                .setCurrentVoters(toReplicaStates(apiVersion, leaderId, voters, currentTimeMs))
                                                                .setObservers(toReplicaStates(apiVersion, leaderId, observers, currentTimeMs))))));
        if (apiVersion >= 2) {
            DescribeQuorumResponseData.NodeCollection nodes = new DescribeQuorumResponseData.NodeCollection(voters.size());
            for (LeaderState.ReplicaState voter : voters) {
                nodes.add(
                        new DescribeQuorumResponseData.Node()
                                .setNodeId(voter.replicaKey().id())
                                .setListeners(voter.listeners().toDescribeQuorumResponseListeners())
                );
            }
            response.setNodes(nodes);
        }
        return response;
    }

    private static List<DescribeQuorumResponseData.ReplicaState> toReplicaStates(
            short apiVersion,
            int leaderId,
            Collection<LeaderState.ReplicaState> states,
            long currentTimeMs
    ) {
        return states
                .stream()
                .map(replicaState -> toReplicaState(apiVersion, leaderId, replicaState, currentTimeMs))
                .collect(Collectors.toList());
    }

    private static DescribeQuorumResponseData.ReplicaState toReplicaState(
            short apiVersion,
            int leaderId,
            LeaderState.ReplicaState replicaState,
            long currentTimeMs
    ) {
        final long lastCaughtUpTimestamp;
        final long lastFetchTimestamp;
        if (replicaState.replicaKey().id() == leaderId) {
            lastCaughtUpTimestamp = currentTimeMs;
            lastFetchTimestamp = currentTimeMs;
        } else {
            lastCaughtUpTimestamp = replicaState.lastCaughtUpTimestamp();
            lastFetchTimestamp = replicaState.lastFetchTimestamp();
        }
        DescribeQuorumResponseData.ReplicaState replicaStateData = new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(replicaState.replicaKey().id())
                .setLogEndOffset(replicaState.endOffset().map(LogOffsetMetadata::offset).orElse(-1L))
                .setLastCaughtUpTimestamp(lastCaughtUpTimestamp)
                .setLastFetchTimestamp(lastFetchTimestamp);

        if (apiVersion >= 2) {
            replicaStateData.setReplicaDirectoryId(replicaState.replicaKey().directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));
        }
        return replicaStateData;
    }

    public static boolean hasValidTopicPartition(DescribeQuorumRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }
}
