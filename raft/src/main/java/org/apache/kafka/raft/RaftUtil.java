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
package org.apache.kafka.raft;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

@SuppressWarnings({ "ClassDataAbstractionCoupling", "ClassFanOutComplexity" })
public class RaftUtil {

    public static ApiMessage errorResponse(ApiKeys apiKey, Errors error) {
        switch (apiKey) {
            case VOTE:
                return new VoteResponseData().setErrorCode(error.code());
            case BEGIN_QUORUM_EPOCH:
                return new BeginQuorumEpochResponseData().setErrorCode(error.code());
            case END_QUORUM_EPOCH:
                return new EndQuorumEpochResponseData().setErrorCode(error.code());
            case FETCH:
                return new FetchResponseData().setErrorCode(error.code());
            case FETCH_SNAPSHOT:
                return new FetchSnapshotResponseData().setErrorCode(error.code());
            case API_VERSIONS:
                return new ApiVersionsResponseData().setErrorCode(error.code());
            case UPDATE_RAFT_VOTER:
                return new UpdateRaftVoterResponseData().setErrorCode(error.code());
            default:
                throw new IllegalArgumentException("Received response for unexpected request type: " + apiKey);
        }
    }

    public static FetchRequestData singletonFetchRequest(
        TopicPartition topicPartition,
        Uuid topicId,
        Consumer<FetchRequestData.FetchPartition> partitionConsumer
    ) {
        FetchRequestData.FetchPartition fetchPartition =
            new FetchRequestData.FetchPartition()
                .setPartition(topicPartition.partition());
        partitionConsumer.accept(fetchPartition);

        FetchRequestData.FetchTopic fetchTopic =
            new FetchRequestData.FetchTopic()
                .setTopic(topicPartition.topic())
                .setTopicId(topicId)
                .setPartitions(Collections.singletonList(fetchPartition));

        return new FetchRequestData()
            .setTopics(Collections.singletonList(fetchTopic));
    }

    public static FetchResponseData singletonFetchResponse(
        ListenerName listenerName,
        short apiVersion,
        TopicPartition topicPartition,
        Uuid topicId,
        Errors topLevelError,
        int leaderId,
        Endpoints endpoints,
        Consumer<FetchResponseData.PartitionData> partitionConsumer
    ) {
        FetchResponseData.PartitionData fetchablePartition =
            new FetchResponseData.PartitionData();

        fetchablePartition.setPartitionIndex(topicPartition.partition());

        partitionConsumer.accept(fetchablePartition);

        FetchResponseData.FetchableTopicResponse fetchableTopic =
            new FetchResponseData.FetchableTopicResponse()
                .setTopic(topicPartition.topic())
                .setTopicId(topicId)
                .setPartitions(Collections.singletonList(fetchablePartition));

        FetchResponseData response = new FetchResponseData();

        if (apiVersion >= 17) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                FetchResponseData.NodeEndpointCollection nodeEndpoints = new FetchResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new FetchResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response
            .setErrorCode(topLevelError.code())
            .setResponses(Collections.singletonList(fetchableTopic));
    }

    public static VoteRequestData singletonVoteRequest(
        TopicPartition topicPartition,
        String clusterId,
        int replicaEpoch,
        ReplicaKey replicaKey,
        ReplicaKey voterKey,
        int lastEpoch,
        long lastEpochEndOffset,
        boolean preVote
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
                                    .setReplicaEpoch(replicaEpoch)
                                    .setReplicaId(replicaKey.id())
                                    .setReplicaDirectoryId(
                                        replicaKey
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
                                    .setPreVote(preVote)
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
        VoteResponseData.PartitionData partitionData = new VoteResponseData.PartitionData()
            .setErrorCode(partitionLevelError.code())
            .setLeaderId(leaderId)
            .setLeaderEpoch(leaderEpoch)
            .setVoteGranted(voteGranted);

        VoteResponseData response = new VoteResponseData()
            .setErrorCode(topLevelError.code())
            .setTopics(Collections.singletonList(
                new VoteResponseData.TopicData()
                    .setTopicName(topicPartition.topic())
                    .setPartitions(Collections.singletonList(partitionData))));

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

    public static FetchSnapshotRequestData singletonFetchSnapshotRequest(
        String clusterId,
        ReplicaKey replicaKey,
        TopicPartition topicPartition,
        int epoch,
        OffsetAndEpoch offsetAndEpoch,
        int maxBytes,
        long position
    ) {
        FetchSnapshotRequestData.SnapshotId snapshotId = new FetchSnapshotRequestData.SnapshotId()
            .setEndOffset(offsetAndEpoch.offset())
            .setEpoch(offsetAndEpoch.epoch());

        FetchSnapshotRequestData.PartitionSnapshot partitionSnapshot = new FetchSnapshotRequestData.PartitionSnapshot()
            .setPartition(topicPartition.partition())
            .setCurrentLeaderEpoch(epoch)
            .setSnapshotId(snapshotId)
            .setPosition(position)
            .setReplicaDirectoryId(replicaKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));

        return new FetchSnapshotRequestData()
            .setClusterId(clusterId)
            .setReplicaId(replicaKey.id())
            .setMaxBytes(maxBytes)
            .setTopics(
                Collections.singletonList(
                    new FetchSnapshotRequestData.TopicSnapshot()
                        .setName(topicPartition.topic())
                        .setPartitions(Collections.singletonList(partitionSnapshot))
                )
            );
    }

    /**
     * Creates a FetchSnapshotResponseData with a single PartitionSnapshot for the topic partition.
     *
     * The partition index will already be populated when calling operator.
     *
     * @param listenerName the listener used to accept the request
     * @param apiVersion the api version of the request
     * @param topicPartition the topic partition to include
     * @param leaderId the id of the leader
     * @param endpoints the endpoints of the leader
     * @param operator unary operator responsible for populating all of the appropriate fields
     * @return the created fetch snapshot response data
     */
    public static FetchSnapshotResponseData singletonFetchSnapshotResponse(
        ListenerName listenerName,
        short apiVersion,
        TopicPartition topicPartition,
        int leaderId,
        Endpoints endpoints,
        UnaryOperator<FetchSnapshotResponseData.PartitionSnapshot> operator
    ) {
        FetchSnapshotResponseData.PartitionSnapshot partitionSnapshot = operator.apply(
            new FetchSnapshotResponseData.PartitionSnapshot().setIndex(topicPartition.partition())
        );

        FetchSnapshotResponseData response = new FetchSnapshotResponseData()
            .setTopics(
                Collections.singletonList(
                    new FetchSnapshotResponseData.TopicSnapshot()
                        .setName(topicPartition.topic())
                        .setPartitions(Collections.singletonList(partitionSnapshot))
                )
            );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                FetchSnapshotResponseData.NodeEndpointCollection nodeEndpoints =
                    new FetchSnapshotResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new FetchSnapshotResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

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

    public static EndQuorumEpochRequestData singletonEndQuorumEpochRequest(
        TopicPartition topicPartition,
        String clusterId,
        int leaderEpoch,
        int leaderId,
        List<ReplicaKey> preferredReplicaKeys
    ) {
        List<Integer> preferredSuccessors = preferredReplicaKeys
                .stream()
                .map(ReplicaKey::id)
                .collect(Collectors.toList());

        List<EndQuorumEpochRequestData.ReplicaInfo> preferredCandidates = preferredReplicaKeys
                .stream()
                .map(replicaKey -> new EndQuorumEpochRequestData.ReplicaInfo()
                    .setCandidateId(replicaKey.id())
                    .setCandidateDirectoryId(replicaKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
                )
                .collect(Collectors.toList());

        return new EndQuorumEpochRequestData()
            .setClusterId(clusterId)
            .setTopics(
                Collections.singletonList(
                    new EndQuorumEpochRequestData.TopicData()
                        .setTopicName(topicPartition.topic())
                        .setPartitions(
                            Collections.singletonList(
                                new EndQuorumEpochRequestData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition())
                                    .setLeaderEpoch(leaderEpoch)
                                    .setLeaderId(leaderId)
                                    .setPreferredSuccessors(preferredSuccessors)
                                    .setPreferredCandidates(preferredCandidates)
                            )
                        )
                )
            );

    }

    public static EndQuorumEpochResponseData singletonEndQuorumEpochResponse(
        ListenerName listenerName,
        short apiVersion,
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId,
        Endpoints endpoints
    ) {
        EndQuorumEpochResponseData response = new EndQuorumEpochResponseData()
                   .setErrorCode(topLevelError.code())
                   .setTopics(Collections.singletonList(
                       new EndQuorumEpochResponseData.TopicData()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(Collections.singletonList(
                               new EndQuorumEpochResponseData.PartitionData()
                                   .setErrorCode(partitionLevelError.code())
                                   .setLeaderId(leaderId)
                                   .setLeaderEpoch(leaderEpoch)
                           )))
                   );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                EndQuorumEpochResponseData.NodeEndpointCollection nodeEndpoints =
                    new EndQuorumEpochResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new EndQuorumEpochResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }


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

    static boolean hasValidTopicPartition(FetchRequestData data, TopicPartition topicPartition, Uuid topicId) {
        return data.topics().size() == 1 &&
            data.topics().get(0).topicId().equals(topicId) &&
            data.topics().get(0).partitions().size() == 1 &&
            data.topics().get(0).partitions().get(0).partition() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(FetchResponseData data, TopicPartition topicPartition, Uuid topicId) {
        return data.responses().size() == 1 &&
            data.responses().get(0).topicId().equals(topicId) &&
            data.responses().get(0).partitions().size() == 1 &&
            data.responses().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(VoteResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(VoteRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(DescribeQuorumRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }
}
