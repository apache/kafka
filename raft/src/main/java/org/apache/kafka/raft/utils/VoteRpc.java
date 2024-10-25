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
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.ReplicaKey;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;

public class VoteRpc {
    public static VoteRequestData singletonVoteRequest(
            TopicPartition topicPartition,
            String clusterId,
            int candidateEpoch,
            ReplicaKey candidateKey,
            ReplicaKey voterKey,
            int lastEpoch,
            long lastEpochEndOffset
    ) {
        return new VoteRequestData()
                .setClusterId(clusterId)
                .setVoterId(voterKey.id())
                .setTopics(
                        Collections.singletonList(
                                new VoteRequestData.TopicData()
                                        .setTopicName(topicPartition.topic())
                                        .setPartitions(
                                                Collections.singletonList(
                                                        new VoteRequestData.PartitionData()
                                                                .setPartitionIndex(topicPartition.partition())
                                                                .setCandidateEpoch(candidateEpoch)
                                                                .setCandidateId(candidateKey.id())
                                                                .setCandidateDirectoryId(
                                                                        candidateKey
                                                                                .directoryId()
                                                                                .orElse(ReplicaKey.NO_DIRECTORY_ID)
                                                                )
                                                                .setVoterDirectoryId(
                                                                        voterKey
                                                                                .directoryId()
                                                                                .orElse(ReplicaKey.NO_DIRECTORY_ID)
                                                                )
                                                                .setLastOffsetEpoch(lastEpoch)
                                                                .setLastOffset(lastEpochEndOffset)
                                                )
                                        )
                        )
                );
    }

    public static VoteResponseData singletonVoteResponse(
            ListenerName listenerName,
            short apiVersion,
            Errors topLevelError,
            TopicPartition topicPartition,
            Errors partitionLevelError,
            int leaderEpoch,
            int leaderId,
            boolean voteGranted,
            Endpoints endpoints
    ) {
        VoteResponseData response = new VoteResponseData()
                .setErrorCode(topLevelError.code())
                .setTopics(Collections.singletonList(
                        new VoteResponseData.TopicData()
                                .setTopicName(topicPartition.topic())
                                .setPartitions(Collections.singletonList(
                                        new VoteResponseData.PartitionData()
                                                .setErrorCode(partitionLevelError.code())
                                                .setLeaderId(leaderId)
                                                .setLeaderEpoch(leaderEpoch)
                                                .setVoteGranted(voteGranted)))));

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                VoteResponseData.NodeEndpointCollection nodeEndpoints = new VoteResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                        new VoteResponseData.NodeEndpoint()
                                .setNodeId(leaderId)
                                .setHost(address.get().getHostString())
                                .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

    public static Optional<ReplicaKey> voteRequestVoterKey(
            VoteRequestData request,
            VoteRequestData.PartitionData partition
    ) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), partition.voterDirectoryId()));
        }
    }

    public static AddRaftVoterRequestData addVoterRequest(
            String clusterId,
            int timeoutMs,
            ReplicaKey voter,
            Endpoints listeners
    ) {
        return new AddRaftVoterRequestData()
                .setClusterId(clusterId)
                .setTimeoutMs(timeoutMs)
                .setVoterId(voter.id())
                .setVoterDirectoryId(voter.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
                .setListeners(listeners.toAddVoterRequest());
    }

    public static AddRaftVoterResponseData addVoterResponse(
            Errors error,
            String errorMessage
    ) {
        errorMessage = errorMessage == null ? error.message() : errorMessage;

        return new AddRaftVoterResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(errorMessage);
    }

    public static RemoveRaftVoterRequestData removeVoterRequest(
            String clusterId,
            ReplicaKey voter
    ) {
        return new RemoveRaftVoterRequestData()
                .setClusterId(clusterId)
                .setVoterId(voter.id())
                .setVoterDirectoryId(voter.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));
    }

    public static RemoveRaftVoterResponseData removeVoterResponse(
            Errors error,
            String errorMessage
    ) {
        errorMessage = errorMessage == null ? error.message() : errorMessage;

        return new RemoveRaftVoterResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(errorMessage);
    }

    public static UpdateRaftVoterRequestData updateVoterRequest(
            String clusterId,
            ReplicaKey voter,
            int epoch,
            SupportedVersionRange supportedVersions,
            Endpoints endpoints
    ) {
        UpdateRaftVoterRequestData request = new UpdateRaftVoterRequestData()
                .setClusterId(clusterId)
                .setCurrentLeaderEpoch(epoch)
                .setVoterId(voter.id())
                .setVoterDirectoryId(voter.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
                .setListeners(endpoints.toUpdateVoterRequest());

        request.kRaftVersionFeature()
                .setMinSupportedVersion(supportedVersions.min())
                .setMaxSupportedVersion(supportedVersions.max());

        return request;
    }

    public static UpdateRaftVoterResponseData updateVoterResponse(
            Errors error,
            ListenerName listenerName,
            LeaderAndEpoch leaderAndEpoch,
            Endpoints endpoints
    ) {
        UpdateRaftVoterResponseData response = new UpdateRaftVoterResponseData()
                .setErrorCode(error.code());

        response.currentLeader()
                .setLeaderId(leaderAndEpoch.leaderId().orElse(-1))
                .setLeaderEpoch(leaderAndEpoch.epoch());

        Optional<InetSocketAddress> address = endpoints.address(listenerName);
        if (address.isPresent()) {
            response.currentLeader()
                    .setHost(address.get().getHostString())
                    .setPort(address.get().getPort());
        }

        return response;
    }

    public static Optional<ReplicaKey> addVoterRequestVoterKey(AddRaftVoterRequestData request) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), request.voterDirectoryId()));
        }
    }

    public static Optional<ReplicaKey> removeVoterRequestVoterKey(RemoveRaftVoterRequestData request) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), request.voterDirectoryId()));
        }
    }

    public static Optional<ReplicaKey> updateVoterRequestVoterKey(UpdateRaftVoterRequestData request) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), request.voterDirectoryId()));
        }
    }

    public static boolean hasValidTopicPartition(VoteResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                data.topics().get(0).partitions().size() == 1 &&
                data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    public static boolean hasValidTopicPartition(VoteRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                data.topics().get(0).partitions().size() == 1 &&
                data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }
}
