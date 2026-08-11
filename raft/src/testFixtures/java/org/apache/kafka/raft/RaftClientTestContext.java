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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.ControlRecordUtils;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.FetchSnapshotResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.internals.BatchBuilder;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.SnapshotReader;


import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.raft.LeaderState.CHECK_QUORUM_TIMEOUT_FACTOR;
import static org.apache.kafka.raft.RaftUtil.hasValidTopicPartition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RaftClientTestContext extends SharedRaftClientContext {
    public final RecordSerde<String> serde = RaftClientContextBuilder.SERDE;
    final int electionBackoffMaxMs = RaftClientContextBuilder.ELECTION_BACKOFF_MAX_MS;
    final int fetchMaxWaitMs = RaftClientContextBuilder.FETCH_MAX_WAIT_MS;
    final int fetchTimeoutMs = RaftClientContextBuilder.FETCH_TIMEOUT_MS;
    final int checkQuorumTimeoutMs = (int) (fetchTimeoutMs * CHECK_QUORUM_TIMEOUT_FACTOR);
    final int beginQuorumEpochTimeoutMs = fetchTimeoutMs / 2;
    final int retryBackoffMs = RaftClientContextBuilder.RETRY_BACKOFF_MS;

    int requestTimeoutMs;
    int appendLingerMs;

    final Metrics metrics;
    public final ExternalKRaftMetrics externalKRaftMetrics;
    final MockMessageQueue messageQueue;
    final MockListener listener;
    final Set<Integer> bootstrapIds;
    // Used to determine if the local kraft client was configured to always flush
    final boolean canBecomeVoter;

    private static final int NUMBER_FETCH_TIMEOUTS_IN_UPDATE_VOTER_SET_PERIOD = 1;

    @SuppressWarnings("ParameterNumber")
    RaftClientTestContext(
        String clusterId,
        OptionalInt localId,
        Uuid localDirectoryId,
        KRaftVersion kraftVersion,
        KafkaRaftClient<String> client,
        MockLog log,
        MockNetworkChannel channel,
        MockMessageQueue messageQueue,
        MockTime time,
        MockQuorumStateStore quorumStateStore,
        VoterSet startingVoters,
        Set<Integer> bootstrapIds,
        RaftProtocol raftProtocol,
        boolean canBecomeVoter,
        Metrics metrics,
        ExternalKRaftMetrics externalKRaftMetrics,
        MockListener listener,
        int fetchMaxBytes
    ) {
        super(clusterId, localId, localDirectoryId, kraftVersion, client, log, channel, time,
            quorumStateStore, startingVoters, raftProtocol, fetchMaxBytes);
        this.messageQueue = messageQueue;
        this.bootstrapIds = bootstrapIds;
        this.canBecomeVoter = canBecomeVoter;
        this.metrics = metrics;
        this.externalKRaftMetrics = externalKRaftMetrics;
        this.listener = listener;
    }

    int electionTimeoutMs() {
        return electionTimeoutMs;
    }

    int requestTimeoutMs() {
        return requestTimeoutMs;
    }

    int appendLingerMs() {
        return appendLingerMs;
    }

    MemoryRecords buildBatch(
        long baseOffset,
        int epoch,
        List<String> records
    ) {
        return buildBatch(time.milliseconds(), baseOffset, epoch, records);
    }

    static MemoryRecords buildBatch(
        long timestamp,
        long baseOffset,
        int epoch,
        List<String> records
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        BatchBuilder<String> builder = new BatchBuilder<>(
            buffer,
            RaftClientContextBuilder.SERDE,
            Compression.NONE,
            baseOffset,
            timestamp,
            epoch,
            512
        );

        for (String record : records) {
            builder.appendRecord(record, null);
        }

        return builder.build();
    }

    public ReplicaKey localReplicaKey() {
        return raftProtocol.isReconfigSupported() ?
            ReplicaKey.of(localIdOrThrow(), localDirectoryId) :
            ReplicaKey.of(localIdOrThrow(), ReplicaKey.NO_DIRECTORY_ID);
    }

    void assertVotedCandidate(int epoch, int candidateId) {
        ReplicaKey candidateKey = ReplicaKey.of(candidateId, ReplicaKey.NO_DIRECTORY_ID);
        assertVotedCandidate(epoch, candidateKey);
    }

    void assertVotedCandidate(int epoch, ReplicaKey candidateKey) {
        assertEquals(
            ElectionState.withVotedCandidate(
                epoch,
                persistedVotedKey(candidateKey, kraftVersion),
                expectedVoters()
            ),
            quorumStateStore.readElectionState().get()
        );
    }

    public void assertElectedLeader(int epoch, int leaderId) {
        assertEquals(
            ElectionState.withElectedLeader(epoch, leaderId, Optional.empty(), expectedVoters()),
            quorumStateStore.readElectionState().get()
        );
    }

    @Override
    void expectAndGrantVotes(int epoch) throws Exception {
        super.expectAndGrantVotes(epoch);
        assertElectedLeader(epoch, localIdOrThrow());
    }

    public void assertElectedLeaderAndVotedKey(int epoch, int leaderId, ReplicaKey candidateKey) {
        assertEquals(
            ElectionState.withElectedLeader(
                epoch,
                leaderId,
                Optional.of(persistedVotedKey(candidateKey, kraftVersion)),
                expectedVoters()
            ),
            quorumStateStore.readElectionState().get()
        );
    }

    private static ReplicaKey persistedVotedKey(ReplicaKey replicaKey, KRaftVersion kraftVersion) {
        if (kraftVersion.isReconfigSupported()) {
            return replicaKey;
        }

        return ReplicaKey.of(replicaKey.id(), ReplicaKey.NO_DIRECTORY_ID);
    }

    void assertUnknownLeaderAndNoVotedCandidate(int epoch) {
        assertEquals(
            ElectionState.withUnknownLeader(epoch, expectedVoters()),
            quorumStateStore.readElectionState().get());
    }

    void assertResignedLeader(int epoch, int leaderId) {
        assertTrue(client.quorum().isResigned());
        assertEquals(
            ElectionState.withElectedLeader(epoch, leaderId, Optional.empty(), expectedVoters()),
            quorumStateStore.readElectionState().get()
        );
    }

    // Voters are only written to ElectionState in KRaftVersion 0
    private Set<Integer> expectedVoters() {
        return kraftVersion.isReconfigSupported() ? Set.of() : startingVoters.voterIds();
    }

    DescribeQuorumResponseData collectDescribeQuorumResponse() {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.DESCRIBE_QUORUM);
        assertEquals(1, sentMessages.size());
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertInstanceOf(
            DescribeQuorumResponseData.class,
            raftMessage.data(),
            "Unexpected request type " + raftMessage.data());
        return (DescribeQuorumResponseData) raftMessage.data();
    }

    void assertSentDescribeQuorumResponse(
        int leaderId,
        int leaderEpoch,
        long highWatermark,
        List<ReplicaState> voterStates,
        List<ReplicaState> observerStates
    ) {
        assertSentDescribeQuorumResponse(Errors.NONE, leaderId, leaderEpoch, highWatermark, voterStates, observerStates);
    }

    void assertSentDescribeQuorumResponse(
        Errors error,
        int leaderId,
        int leaderEpoch,
        long highWatermark,
        List<ReplicaState> voterStates,
        List<ReplicaState> observerStates
    ) {
        DescribeQuorumResponseData response = collectDescribeQuorumResponse();

        DescribeQuorumResponseData.PartitionData partitionData = new DescribeQuorumResponseData.PartitionData()
            .setErrorCode(error.code())
            .setLeaderId(leaderId)
            .setLeaderEpoch(leaderEpoch)
            .setHighWatermark(highWatermark)
            .setCurrentVoters(voterStates)
            .setObservers(observerStates);

        if (!error.equals(Errors.NONE)) {
            partitionData.setErrorMessage(error.message());
        }

        DescribeQuorumResponseData.NodeCollection nodes = new DescribeQuorumResponseData.NodeCollection(0);
        if (raftProtocol.describeQuorumRpcVersion() >= 2) {
            nodes = new DescribeQuorumResponseData.NodeCollection(voterStates.size());
            for (ReplicaState voterState : voterStates) {
                nodes.add(new DescribeQuorumResponseData.Node()
                    .setNodeId(voterState.replicaId())
                    .setListeners(startingVoters.listeners(voterState.replicaId()).toDescribeQuorumResponseListeners()));
            }
        }

        DescribeQuorumResponseData expectedResponse = DescribeQuorumResponse.singletonResponse(
            metadataPartition,
            partitionData,
            nodes
        );

        List<ReplicaState> sortedVoters = response
            .topics()
            .get(0)
            .partitions()
            .get(0)
            .currentVoters()
            .stream()
            .sorted(Comparator.comparingInt(ReplicaState::replicaId))
            .collect(Collectors.toList());
        response.topics().get(0).partitions().get(0).setCurrentVoters(sortedVoters);
        response.nodes().sort(Comparator.comparingInt(DescribeQuorumResponseData.Node::nodeId));

        assertEquals(expectedResponse, response);
    }

    RaftRequest.Outbound assertSentPreVoteRequest(int epoch, int lastEpoch, long lastEpochOffset, int numVoteReceivers) {
        List<RaftRequest.Outbound> voteRequests = collectPreVoteRequests(epoch, lastEpoch, lastEpochOffset);
        assertEquals(numVoteReceivers, voteRequests.size());
        return voteRequests.iterator().next();
    }

    RaftRequest.Outbound assertSentVoteRequest(int epoch, int lastEpoch, long lastEpochOffset, int numVoteReceivers) {
        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch, lastEpoch, lastEpochOffset);
        assertEquals(numVoteReceivers, voteRequests.size());
        return voteRequests.iterator().next();
    }

    void assertSentVoteResponse(Errors error) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.VOTE);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(VoteResponseData.class, raftMessage.data());
        VoteResponseData response = (VoteResponseData) raftMessage.data();

        assertEquals(error, Errors.forCode(response.errorCode()));
    }

    void assertSentVoteResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId,
        boolean voteGranted
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.VOTE);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(VoteResponseData.class, raftMessage.data());
        VoteResponseData response = (VoteResponseData) raftMessage.data();
        assertTrue(hasValidTopicPartition(response, metadataPartition));

        VoteResponseData.PartitionData partitionResponse = response.topics().get(0).partitions().get(0);

        String leaderIdDebugLog = "Leader Id: " + leaderId +
            " Partition response leader Id: " + partitionResponse.leaderId();
        assertEquals(voteGranted, partitionResponse.voteGranted());
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId(), leaderIdDebugLog);
        assertEquals(epoch, partitionResponse.leaderEpoch());

        if (raftProtocol.isReconfigSupported() && leaderId.isPresent()) {
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId.getAsInt());
            Endpoints responseEndpoints = Endpoints.fromVoteResponse(
                channel.listenerName(),
                leaderId.getAsInt(),
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }
    }

    @Override
    List<RaftRequest.Outbound> collectVoteRequests(int epoch, int lastEpoch, long lastEpochOffset) {
        List<RaftRequest.Outbound> voteRequests = super.collectVoteRequests(epoch, lastEpoch, lastEpochOffset);
        for (RaftRequest.Outbound raftMessage : voteRequests) {
            verifyVoteRequest((VoteRequestData) raftMessage.data(), false, epoch, lastEpoch, lastEpochOffset);
        }
        return voteRequests;
    }

    @Override
    List<RaftRequest.Outbound> collectPreVoteRequests(int epoch, int lastEpoch, long lastEpochOffset) {
        List<RaftRequest.Outbound> voteRequests = super.collectPreVoteRequests(epoch, lastEpoch, lastEpochOffset);
        for (RaftRequest.Outbound raftMessage : voteRequests) {
            verifyVoteRequest((VoteRequestData) raftMessage.data(), true, epoch, lastEpoch, lastEpochOffset);
        }
        return voteRequests;
    }

    private void verifyVoteRequest(
        VoteRequestData request,
        boolean expectedPreVote,
        int epoch,
        int lastEpoch,
        long lastEpochOffset
    ) {
        VoteRequestData.PartitionData partitionRequest = unwrap(request);
        assertEquals(expectedPreVote, partitionRequest.preVote());
        assertEquals(epoch, partitionRequest.replicaEpoch());
        assertEquals(localIdOrThrow(), partitionRequest.replicaId());
        assertEquals(lastEpoch, partitionRequest.lastOffsetEpoch());
        assertEquals(lastEpochOffset, partitionRequest.lastOffset());
    }

    private VoteRequestData.PartitionData unwrap(VoteRequestData voteRequest) {
        assertTrue(hasValidTopicPartition(voteRequest, metadataPartition));
        return voteRequest.topics().get(0).partitions().get(0);
    }

    /**
     * Advance time and complete an empty fetch to reset the fetch timer.
     * This is used to expire the update voter set timer without also expiring the fetch timer,
     * which is needed for add, remove, and update voter tests.
     * For voters and observers, polling after exiting this method expires the update voter set timer.
     * @param epoch - the current epoch
     * @param leaderId - the leader id
     * @param expireUpdateVoterSetTimer - if true, advance time again to expire this timer
     */
    void advanceTimeAndCompleteFetch(
        int epoch,
        int leaderId,
        boolean expireUpdateVoterSetTimer
    ) throws Exception {
        for (int i = 0; i < NUMBER_FETCH_TIMEOUTS_IN_UPDATE_VOTER_SET_PERIOD; i++) {
            time.sleep(fetchTimeoutMs - 1);
            pollUntilRequest();
            final var fetchRequest = assertSentFetchRequest();
            assertFetchRequestData(
                fetchRequest,
                epoch,
                log.endOffset().offset(),
                log.lastFetchedEpoch(),
                client.highWatermark()
            );

            deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                fetchResponse(
                    epoch,
                    leaderId,
                    MemoryRecords.EMPTY,
                    log.endOffset().offset(),
                    Errors.NONE
                )
            );
            // poll kraft to handle the fetch response
            client.poll();
        }
        if (expireUpdateVoterSetTimer) {
            time.sleep(fetchTimeoutMs - 1);
        }
    }

    List<RaftRequest.Outbound> assertSentBeginQuorumEpochRequest(int epoch, Set<Integer> destinationIds) {
        List<RaftRequest.Outbound> requests = collectBeginEpochRequests(epoch);
        assertEquals(destinationIds.size(), requests.size());
        assertEquals(destinationIds, requests.stream().map(r -> r.destination().id()).collect(Collectors.toSet()));

        return requests;
    }

    void assertSentBeginQuorumEpochResponse(
        Errors responseError
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.BEGIN_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(BeginQuorumEpochResponseData.class, raftMessage.data());
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(responseError, Errors.forCode(response.errorCode()));

        if (!response.topics().isEmpty()) {
            BeginQuorumEpochResponseData.PartitionData partitionResponse = response
                .topics()
                .get(0)
                .partitions()
                .get(0);
            if (raftProtocol.isReconfigSupported() && partitionResponse.leaderId() >= 0) {
                int leaderId = partitionResponse.leaderId();
                Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId);
                Endpoints responseEndpoints = Endpoints.fromBeginQuorumEpochResponse(
                    channel.listenerName(),
                    leaderId,
                    response.nodeEndpoints()
                );
                assertEquals(expectedLeaderEndpoints, responseEndpoints);
            }
        }
    }

    void assertSentBeginQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.BEGIN_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(BeginQuorumEpochResponseData.class, raftMessage.data());
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        BeginQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
        assertEquals(
            partitionError,
            Errors.forCode(partitionResponse.errorCode()),
            "Leader Id: " + leaderId +
            " Partition response leader Id: " + partitionResponse.leaderId()
        );

        if (raftProtocol.isReconfigSupported() && leaderId.isPresent()) {
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId.getAsInt());
            Endpoints responseEndpoints = Endpoints.fromBeginQuorumEpochResponse(
                channel.listenerName(),
                leaderId.getAsInt(),
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }
    }

    RaftRequest.Outbound assertSentEndQuorumEpochRequest(int epoch, int destinationId) {
        List<RaftRequest.Outbound> endQuorumRequests = collectEndQuorumRequests(
            epoch,
            Set.of(destinationId),
            Optional.empty()
        );
        assertEquals(1, endQuorumRequests.size());
        return endQuorumRequests.get(0);
    }

    void assertSentEndQuorumEpochResponse(
        Errors responseError
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.END_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(EndQuorumEpochResponseData.class, raftMessage.data());
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) raftMessage.data();
        assertEquals(responseError, Errors.forCode(response.errorCode()));

        if (!response.topics().isEmpty()) {
            EndQuorumEpochResponseData.PartitionData partitionResponse = response
                .topics()
                .get(0)
                .partitions()
                .get(0);
            if (raftProtocol.isReconfigSupported() && partitionResponse.leaderId() >= 0) {
                int leaderId = partitionResponse.leaderId();
                Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId);
                Endpoints responseEndpoints = Endpoints.fromEndQuorumEpochResponse(
                    channel.listenerName(),
                    leaderId,
                    response.nodeEndpoints()
                );
                assertEquals(expectedLeaderEndpoints, responseEndpoints);
            }
        }
    }

    void assertSentEndQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.END_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(EndQuorumEpochResponseData.class, raftMessage.data());
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        EndQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
        assertEquals(partitionError, Errors.forCode(partitionResponse.errorCode()));

        if (raftProtocol.isReconfigSupported() && leaderId.isPresent()) {
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId.getAsInt());
            Endpoints responseEndpoints = Endpoints.fromEndQuorumEpochResponse(
                channel.listenerName(),
                leaderId.getAsInt(),
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }
    }

    RaftRequest.Outbound assertSentFetchRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.FETCH));
        assertEquals(1, sentRequests.size());
        return sentRequests.get(0);
    }

    RaftRequest.Outbound assertSentFetchRequest(
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch,
        OptionalLong highWatermark
    ) {
        List<RaftRequest.Outbound> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());

        RaftRequest.Outbound raftRequest = sentMessages.get(0);
        assertFetchRequestData(raftRequest, epoch, fetchOffset, lastFetchedEpoch, highWatermark);
        return raftRequest;
    }

    FetchResponseData.PartitionData assertFetchResponseData(RaftResponse.Outbound message) {
        assertEquals(ApiKeys.FETCH.id, message.data().apiKey());
        FetchResponseData response = (FetchResponseData) message.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        assertEquals(1, response.responses().size());
        assertEquals(metadataPartition.topic(), response.responses().get(0).topic());
        assertEquals(1, response.responses().get(0).partitions().size());

        FetchResponseData.PartitionData partitionResponse = response.responses().get(0).partitions().get(0);
        if (raftProtocol.isReconfigSupported() && partitionResponse.currentLeader().leaderId() >= 0) {
            int leaderId = partitionResponse.currentLeader().leaderId();
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId);
            Endpoints responseEndpoints = Endpoints.fromFetchResponse(
                channel.listenerName(),
                leaderId,
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }
        return partitionResponse;
    }

    FetchResponseData.PartitionData assertSentFetchPartitionResponse() {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH);
        assertEquals(
            1,
            sentMessages.size(),
            "Found unexpected sent messages " + sentMessages
        );
        return assertFetchResponseData(sentMessages.get(0));
    }

    void assertSentFetchPartitionResponse(Errors topLevelError) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH);
        assertEquals(
            1,
            sentMessages.size(),
            "Found unexpected sent messages " + sentMessages
        );
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertEquals(ApiKeys.FETCH.id, raftMessage.data().apiKey());
        FetchResponseData response = (FetchResponseData) raftMessage.data();
        assertEquals(topLevelError, Errors.forCode(response.errorCode()));
    }

    MemoryRecords assertSentFetchPartitionResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId
    ) {
        FetchResponseData.PartitionData partitionResponse = assertSentFetchPartitionResponse();
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.currentLeader().leaderId());
        assertEquals(-1, partitionResponse.divergingEpoch().endOffset());
        assertEquals(-1, partitionResponse.divergingEpoch().epoch());
        assertEquals(-1, partitionResponse.snapshotId().endOffset());
        assertEquals(-1, partitionResponse.snapshotId().epoch());
        return (MemoryRecords) partitionResponse.records();
    }

    MemoryRecords assertSentFetchPartitionResponse(
        long highWatermark,
        int leaderEpoch
    ) {
        FetchResponseData.PartitionData partitionResponse = assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(leaderEpoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(highWatermark, partitionResponse.highWatermark());
        assertEquals(-1, partitionResponse.divergingEpoch().endOffset());
        assertEquals(-1, partitionResponse.divergingEpoch().epoch());
        assertEquals(-1, partitionResponse.snapshotId().endOffset());
        assertEquals(-1, partitionResponse.snapshotId().epoch());
        return (MemoryRecords) partitionResponse.records();
    }

    RaftRequest.Outbound assertSentFetchSnapshotRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.FETCH_SNAPSHOT));
        assertEquals(1, sentRequests.size());

        return sentRequests.get(0);
    }

    void assertSentFetchSnapshotResponse(Errors responseError) {
        assertSentFetchSnapshotResponse(responseError, metadataPartition);
    }

    Optional<FetchSnapshotResponseData.PartitionSnapshot> assertSentFetchSnapshotResponse(
        TopicPartition topicPartition
    ) {
        return assertSentFetchSnapshotResponse(Errors.NONE, topicPartition);
    }

    Optional<FetchSnapshotResponseData.PartitionSnapshot> assertSentFetchSnapshotResponse(
        Errors responseError,
        TopicPartition topicPartition
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH_SNAPSHOT);
        assertEquals(1, sentMessages.size());

        RaftMessage message = sentMessages.get(0);
        assertInstanceOf(FetchSnapshotResponseData.class, message.data());

        FetchSnapshotResponseData response = (FetchSnapshotResponseData) message.data();
        assertEquals(responseError, Errors.forCode(response.errorCode()));

        Optional<FetchSnapshotResponseData.PartitionSnapshot> result =
            FetchSnapshotResponse.forTopicPartition(response, topicPartition);

        if (result.isPresent() && raftProtocol.isReconfigSupported() && result.get().currentLeader().leaderId() >= 0) {
            int leaderId = result.get().currentLeader().leaderId();
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId);
            Endpoints responseEndpoints = Endpoints.fromFetchSnapshotResponse(
                channel.listenerName(),
                leaderId,
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }

        return result;
    }

    RaftRequest.Outbound assertSentApiVersionsRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.API_VERSIONS));
        assertEquals(1, sentRequests.size());

        return sentRequests.get(0);
    }

    RaftRequest.Outbound assertSentAddVoterRequest(
        ReplicaKey replicaKey,
        Endpoints endpoints
    ) {
        final var sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.ADD_RAFT_VOTER));
        assertEquals(1, sentRequests.size());

        final var request = sentRequests.get(0);
        assertInstanceOf(AddRaftVoterRequestData.class, request.data());

        final var addRaftVoterRequestData = (AddRaftVoterRequestData) request.data();
        assertEquals(clusterId, addRaftVoterRequestData.clusterId());
        assertEquals(replicaKey.id(), addRaftVoterRequestData.voterId());
        assertEquals(replicaKey.directoryId().get(), addRaftVoterRequestData.voterDirectoryId());
        assertEquals(endpoints, Endpoints.fromAddVoterRequest(addRaftVoterRequestData.listeners()));
        assertFalse(addRaftVoterRequestData.ackWhenCommitted());

        return request;
    }

    AddRaftVoterResponseData assertSentAddVoterResponse(Errors error) {
        List<RaftResponse.Outbound> sentResponses = drainSentResponses(ApiKeys.ADD_RAFT_VOTER);
        assertEquals(1, sentResponses.size());

        RaftResponse.Outbound response = sentResponses.get(0);
        assertInstanceOf(AddRaftVoterResponseData.class, response.data());

        AddRaftVoterResponseData addVoterResponse = (AddRaftVoterResponseData) response.data();
        if (Errors.NONE.equals(error)) {
            assertEquals(error, Errors.forCode(addVoterResponse.errorCode()));
            assertNull(addVoterResponse.errorMessage());
        } else {
            assertEquals(error, Errors.forCode(addVoterResponse.errorCode()));
        }
        return addVoterResponse;
    }

    RaftRequest.Outbound assertSentRemoveVoterRequest(
        ReplicaKey replicaKey
    ) {
        final var sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.REMOVE_RAFT_VOTER));
        assertEquals(1, sentRequests.size());

        final var request = sentRequests.get(0);
        assertInstanceOf(RemoveRaftVoterRequestData.class, request.data());

        final var removeRaftVoterRequestData = (RemoveRaftVoterRequestData) request.data();
        assertEquals(clusterId, removeRaftVoterRequestData.clusterId());
        assertEquals(replicaKey.id(), removeRaftVoterRequestData.voterId());
        assertEquals(replicaKey.directoryId().get(), removeRaftVoterRequestData.voterDirectoryId());

        return request;
    }

    RemoveRaftVoterResponseData assertSentRemoveVoterResponse(Errors error) {
        List<RaftResponse.Outbound> sentResponses = drainSentResponses(ApiKeys.REMOVE_RAFT_VOTER);
        assertEquals(1, sentResponses.size());

        RaftResponse.Outbound response = sentResponses.get(0);
        assertInstanceOf(RemoveRaftVoterResponseData.class, response.data());

        RemoveRaftVoterResponseData removeVoterResponse = (RemoveRaftVoterResponseData) response.data();
        if (Errors.NONE.equals(error)) {
            assertEquals(error, Errors.forCode(removeVoterResponse.errorCode()));
            assertNull(removeVoterResponse.errorMessage());
        } else {
            assertEquals(error, Errors.forCode(removeVoterResponse.errorCode()));
        }
        return removeVoterResponse;
    }

    RaftRequest.Outbound assertSentUpdateVoterRequest(
        ReplicaKey replicaKey,
        int epoch,
        SupportedVersionRange supportedVersions,
        Endpoints endpoints
    ) {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.UPDATE_RAFT_VOTER));
        assertEquals(1, sentRequests.size());

        RaftRequest.Outbound request = sentRequests.get(0);
        assertInstanceOf(UpdateRaftVoterRequestData.class, request.data());

        UpdateRaftVoterRequestData updateVoterRequest = (UpdateRaftVoterRequestData) request.data();
        assertEquals(clusterId, updateVoterRequest.clusterId());
        assertEquals(epoch, updateVoterRequest.currentLeaderEpoch());
        assertEquals(replicaKey.id(), updateVoterRequest.voterId());
        assertEquals(replicaKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID), updateVoterRequest.voterDirectoryId());
        assertEquals(endpoints, Endpoints.fromUpdateVoterRequest(updateVoterRequest.listeners()));
        assertEquals(supportedVersions.min(), updateVoterRequest.kRaftVersionFeature().minSupportedVersion());
        assertEquals(supportedVersions.max(), updateVoterRequest.kRaftVersionFeature().maxSupportedVersion());

        return request;
    }

    UpdateRaftVoterResponseData assertSentUpdateVoterResponse(
        Errors error,
        OptionalInt leaderId,
        int epoch
    ) {
        List<RaftResponse.Outbound> sentResponses = drainSentResponses(ApiKeys.UPDATE_RAFT_VOTER);
        assertEquals(1, sentResponses.size());

        RaftResponse.Outbound response = sentResponses.get(0);
        assertInstanceOf(UpdateRaftVoterResponseData.class, response.data());

        UpdateRaftVoterResponseData updateVoterResponse = (UpdateRaftVoterResponseData) response.data();
        assertEquals(error, Errors.forCode(updateVoterResponse.errorCode()));
        assertEquals(leaderId.orElse(-1), updateVoterResponse.currentLeader().leaderId());
        assertEquals(epoch, updateVoterResponse.currentLeader().leaderEpoch());

        if (updateVoterResponse.currentLeader().leaderId() >= 0) {
            int id = updateVoterResponse.currentLeader().leaderId();
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(id);
            Endpoints responseEndpoints = Endpoints.fromInetSocketAddresses(
                Map.of(
                    channel.listenerName(),
                    InetSocketAddress.createUnresolved(
                        updateVoterResponse.currentLeader().host(),
                        updateVoterResponse.currentLeader().port()
                    )
                )
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }
        return updateVoterResponse;
    }

    List<RaftRequest.Outbound> collectEndQuorumRequests(
        int epoch,
        Set<Integer> destinationIdSet,
        Optional<List<ReplicaKey>> preferredCandidates
    ) {
        List<RaftRequest.Outbound> endQuorumRequests = new ArrayList<>();
        Set<Integer> collectedDestinationIdSet = new HashSet<>();

        Optional<List<Integer>> preferredSuccessorsOpt = preferredCandidates
            .map(list -> list.stream().map(ReplicaKey::id).collect(Collectors.toList()));

        for (RaftRequest.Outbound raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof EndQuorumEpochRequestData request) {

                EndQuorumEpochRequestData.PartitionData partitionRequest =
                    request.topics().get(0).partitions().get(0);

                assertEquals(epoch, partitionRequest.leaderEpoch());
                assertEquals(localIdOrThrow(), partitionRequest.leaderId());
                preferredSuccessorsOpt.ifPresent(preferredSuccessors ->
                    assertEquals(preferredSuccessors, partitionRequest.preferredSuccessors())
                );
                preferredCandidates.ifPresent(preferred ->
                    assertEquals(
                        preferred,
                        partitionRequest
                            .preferredCandidates()
                            .stream()
                            .map(replica -> ReplicaKey.of(replica.candidateId(), replica.candidateDirectoryId()))
                            .collect(Collectors.toList())
                    )
                );

                collectedDestinationIdSet.add(raftMessage.destination().id());
                endQuorumRequests.add(raftMessage);
            }
        }
        assertEquals(destinationIdSet, collectedDestinationIdSet);
        return endQuorumRequests;
    }

    void discoverLeaderAsObserver(
        int leaderId,
        int epoch,
        OptionalLong highWatermark
    ) throws Exception {
        pollUntilRequest();
        RaftRequest.Outbound fetchRequest = assertSentFetchRequest();
        int destinationId = fetchRequest.destination().id();
        assertTrue(
            startingVoters.voterIds().contains(destinationId) || bootstrapIds.contains(destinationId),
            String.format("id %d is not in sets %s or %s", destinationId, startingVoters, bootstrapIds)
        );
        assertFetchRequestData(fetchRequest, 0, 0L, 0, highWatermark);

        deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NONE)
        );
        client.poll();
        assertElectedLeader(epoch, leaderId);
    }

    @Override
    List<RaftRequest.Outbound> collectBeginEpochRequests(int epoch) {
        List<RaftRequest.Outbound> requests = super.collectBeginEpochRequests(epoch);
        for (RaftRequest.Outbound raftRequest : requests) {
            assertInstanceOf(BeginQuorumEpochRequestData.class, raftRequest.data());
            assertNotEquals(localIdOrThrow(), raftRequest.destination().id());
            BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) raftRequest.data();

            BeginQuorumEpochRequestData.PartitionData partitionRequest =
                request.topics().get(0).partitions().get(0);

            assertEquals(epoch, partitionRequest.leaderEpoch());
            assertEquals(localIdOrThrow(), partitionRequest.leaderId());
        }
        return requests;
    }

    public static InetSocketAddress mockAddress(int id) {
        return InetSocketAddress.createUnresolved("localhost", 9990 + id);
    }

    EndQuorumEpochResponseData endEpochResponse(
        int epoch,
        OptionalInt leaderId
    ) {
        return RaftUtil.singletonEndQuorumEpochResponse(
            channel.listenerName(),
            raftProtocol.endQuorumEpochRpcVersion(),
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId.orElse(-1),
            leaderId.isPresent() ? startingVoters.listeners(leaderId.getAsInt()) : Endpoints.empty()
        );
    }

    EndQuorumEpochRequestData endEpochRequest(
        int epoch,
        int leaderId,
        List<ReplicaKey> preferredCandidates
    ) {
        return endEpochRequest(
            clusterId,
            epoch,
            leaderId,
            preferredCandidates
        );
    }

    EndQuorumEpochRequestData endEpochRequest(
        String clusterId,
        int epoch,
        int leaderId,
        List<ReplicaKey> preferredCandidates
    ) {
        return RaftUtil.singletonEndQuorumEpochRequest(
            metadataPartition,
            clusterId,
            epoch,
            leaderId,
            preferredCandidates
        );
    }

    BeginQuorumEpochRequestData beginEpochRequest(int epoch, int leaderId) {
        return beginEpochRequest(clusterId, epoch, leaderId);
    }

    BeginQuorumEpochRequestData beginEpochRequest(int epoch, int leaderId, Endpoints endpoints) {
        ReplicaKey localReplicaKey = raftProtocol.isReconfigSupported() ?
            ReplicaKey.of(localIdOrThrow(), localDirectoryId) :
            ReplicaKey.of(-1, ReplicaKey.NO_DIRECTORY_ID);

        return beginEpochRequest(clusterId, epoch, leaderId, endpoints, localReplicaKey);
    }

    BeginQuorumEpochRequestData beginEpochRequest(String clusterId, int epoch, int leaderId) {
        ReplicaKey localReplicaKey = raftProtocol.isReconfigSupported() ?
            ReplicaKey.of(localIdOrThrow(), localDirectoryId) :
            ReplicaKey.of(-1, ReplicaKey.NO_DIRECTORY_ID);

        return beginEpochRequest(clusterId, epoch, leaderId, localReplicaKey);
    }

    BeginQuorumEpochRequestData beginEpochRequest(
        String clusterId,
        int epoch,
        int leaderId,
        ReplicaKey voterKey
    ) {
        return beginEpochRequest(
            clusterId,
            epoch,
            leaderId,
            startingVoters.listeners(leaderId),
            voterKey
        );
    }

    BeginQuorumEpochRequestData beginEpochRequest(
        String clusterId,
        int epoch,
        int leaderId,
        Endpoints endpoints,
        ReplicaKey voterKey
    ) {
        return RaftUtil.singletonBeginQuorumEpochRequest(
            metadataPartition,
            clusterId,
            epoch,
            leaderId,
            endpoints,
            voterKey
        );
    }

    VoteRequestData preVoteRequest(
        int epoch,
        ReplicaKey candidateKey,
        int lastEpoch,
        long lastEpochOffset
    ) {
        return voteRequest(
            clusterId,
            epoch,
            candidateKey,
            lastEpoch,
            lastEpochOffset,
            true
        );
    }

    static void assertMatchingRecords(
        String[] expected,
        Records actual
    ) {
        List<Record> recordList = Utils.toList(actual.records());
        assertEquals(expected.length, recordList.size());
        for (int i = 0; i < expected.length; i++) {
            Record record = recordList.get(i);
            assertEquals(expected[i], Utils.utf8(record.value()),
                "Record at offset " + record.offset() + " does not match expected");
        }
    }

    static void verifyLeaderChangeMessage(
        int leaderId,
        List<Integer> voters,
        List<Integer> grantingVoters,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.LEADER_CHANGE, ControlRecordType.parse(recordKey));

        LeaderChangeMessage leaderChangeMessage = ControlRecordUtils.deserializeLeaderChangeMessage(recordValue);
        assertEquals(leaderId, leaderChangeMessage.leaderId());
        assertEquals(
            voters
                .stream()
                .map(voterId -> new Voter().setVoterId(voterId))
                .collect(Collectors.toSet()),
            new HashSet<>(leaderChangeMessage.voters())
        );
        assertEquals(
            grantingVoters
                .stream()
                .map(voterId -> new Voter().setVoterId(voterId))
                .collect(Collectors.toSet()),
            new HashSet<>(leaderChangeMessage.grantingVoters())
        );
    }

    void assertFetchRequestData(
        RaftRequest.Outbound message,
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch,
        OptionalLong highWatermark
    ) {
        assertInstanceOf(
            FetchRequestData.class,
            message.data(),
            "unexpected request type " + message.data());
        FetchRequestData request = (FetchRequestData) message.data();
        assertEquals(fetchMaxWaitMs, request.maxWaitMs());

        assertEquals(1, request.topics().size());
        assertEquals(metadataPartition.topic(), request.topics().get(0).topic());
        assertEquals(1, request.topics().get(0).partitions().size());

        FetchRequestData.FetchPartition fetchPartition = request.topics().get(0).partitions().get(0);
        assertEquals(epoch, fetchPartition.currentLeaderEpoch());
        assertEquals(fetchOffset, fetchPartition.fetchOffset());
        assertEquals(lastFetchedEpoch, fetchPartition.lastFetchedEpoch());
        assertEquals(localId.orElse(-1), request.replicaState().replicaId());
        assertEquals(highWatermark.orElse(-1), fetchPartition.highWatermark());

        // Assert that voters have flushed up to the fetch offset
        if ((localId.isPresent() && startingVoters.voterIds().contains(localId.getAsInt())) ||
            canBecomeVoter
        ) {
            assertEquals(
                log.firstUnflushedOffset(),
                fetchOffset,
                String.format(
                    "expected voters have the fetch offset (%s) be the same as the unflushed offset (%s)",
                    log.firstUnflushedOffset(),
                    fetchOffset
                )
            );
        } else {
            assertFalse(log.flushedSinceLastChecked(), "KRaft client should not explicitly flush when it is an observer");
        }
    }

    FetchResponseData fetchResponse(
        int epoch,
        int leaderId,
        Records records,
        long highWatermark,
        Errors error
    ) {
        return RaftUtil.singletonFetchResponse(
            channel.listenerName(),
            raftProtocol.fetchRpcVersion(),
            metadataPartition,
            metadataTopicId,
            Errors.NONE,
            leaderId,
            startingVoters.listeners(leaderId),
            partitionData -> {
                partitionData
                    .setRecords(records)
                    .setErrorCode(error.code())
                    .setHighWatermark(highWatermark);

                partitionData.currentLeader()
                    .setLeaderEpoch(epoch)
                    .setLeaderId(leaderId);
            }
        );
    }

    FetchResponseData divergingFetchResponse(
        int epoch,
        int leaderId,
        long divergingEpochEndOffset,
        int divergingEpoch,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(
            channel.listenerName(),
            raftProtocol.fetchRpcVersion(),
            metadataPartition,
            metadataTopicId,
            Errors.NONE,
            leaderId,
            startingVoters.listeners(leaderId),
            partitionData -> {
                partitionData.setHighWatermark(highWatermark);

                partitionData.currentLeader()
                    .setLeaderEpoch(epoch)
                    .setLeaderId(leaderId);

                partitionData.divergingEpoch()
                    .setEpoch(divergingEpoch)
                    .setEndOffset(divergingEpochEndOffset);

                partitionData.setRecords(MemoryRecords.EMPTY);
            }
        );
    }

    FetchResponseData snapshotFetchResponse(
        int epoch,
        int leaderId,
        OffsetAndEpoch snapshotId,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(
            channel.listenerName(),
            raftProtocol.fetchRpcVersion(),
            metadataPartition,
            metadataTopicId,
            Errors.NONE,
            leaderId,
            startingVoters.listeners(leaderId),
            partitionData -> {
                partitionData.setHighWatermark(highWatermark);

                partitionData.currentLeader()
                    .setLeaderEpoch(epoch)
                    .setLeaderId(leaderId);

                partitionData.snapshotId()
                    .setEpoch(snapshotId.epoch())
                    .setEndOffset(snapshotId.offset());

                partitionData.setRecords(MemoryRecords.EMPTY);
            }
        );
    }

    FetchSnapshotResponseData fetchSnapshotResponse(
        int leaderId,
        UnaryOperator<FetchSnapshotResponseData.PartitionSnapshot> operator
    ) {
        return RaftUtil.singletonFetchSnapshotResponse(
            channel.listenerName(),
            raftProtocol.fetchSnapshotRpcVersion(),
            metadataPartition,
            leaderId,
            startingVoters.listeners(leaderId),
            operator
        );
    }

    AddRaftVoterRequestData addVoterRequest(
        int timeoutMs,
        ReplicaKey voter,
        Endpoints endpoints
    ) {
        return addVoterRequest(
            clusterId,
            timeoutMs,
            voter,
            endpoints
        );
    }

    AddRaftVoterRequestData addVoterRequest(
        String clusterId,
        int timeoutMs,
        ReplicaKey voter,
        Endpoints endpoints
    ) {
        return RaftUtil.addVoterRequest(
            clusterId,
            timeoutMs,
            voter,
            endpoints,
            true
        );
    }

    RemoveRaftVoterRequestData removeVoterRequest(ReplicaKey voter) {
        return removeVoterRequest(clusterId, voter);
    }

    RemoveRaftVoterRequestData removeVoterRequest(String cluster, ReplicaKey voter) {
        return RaftUtil.removeVoterRequest(cluster, voter);
    }

    UpdateRaftVoterRequestData updateVoterRequest(
        ReplicaKey voter,
        SupportedVersionRange supportedVersions,
        Endpoints endpoints
    ) {
        return updateVoterRequest(clusterId, voter, currentEpoch(), supportedVersions, endpoints);
    }

    UpdateRaftVoterRequestData updateVoterRequest(
        String clusterId,
        ReplicaKey voter,
        int epoch,
        SupportedVersionRange supportedVersions,
        Endpoints endpoints
    ) {
        return RaftUtil.updateVoterRequest(clusterId, voter, epoch, supportedVersions, endpoints);
    }

    UpdateRaftVoterResponseData updateVoterResponse(
        Errors error,
        LeaderAndEpoch leaderAndEpoch
    ) {
        return RaftUtil.updateVoterResponse(
            error,
            channel.listenerName(),
            leaderAndEpoch,
            leaderAndEpoch.leaderId().isPresent() ?
                startingVoters.listeners(leaderAndEpoch.leaderId().getAsInt()) :
                Endpoints.empty()
        );
    }

    @Override
    public void advanceLocalLeaderHighWatermarkToLogEndOffset() throws InterruptedException {
        assertEquals(localId, currentLeader());
        long localLogEndOffset = log.endOffset().offset();

        Iterable<ReplicaKey> followers = () -> startingVoters
            .voterKeys()
            .stream()
            .filter(voterKey -> voterKey.id() != localId.getAsInt())
            .iterator();

        // Send a request from every voter
        for (ReplicaKey follower : followers) {
            deliverRequest(
                fetchRequest(currentEpoch(), follower, localLogEndOffset, currentEpoch(), 0)
            );

            pollUntilResponse();
            assertSentFetchPartitionResponse(Errors.NONE, currentEpoch(), localId);
        }

        pollUntil(() -> OptionalLong.of(localLogEndOffset).equals(client.highWatermark()));
    }

    static class MockListener implements RaftClient.Listener<String> {
        private final List<Batch<String>> commits = new ArrayList<>();
        private final List<BatchReader<String>> savedBatches = new ArrayList<>();
        private final Map<Integer, Long> claimedEpochStartOffsets = new HashMap<>();
        private LeaderAndEpoch currentLeaderAndEpoch = LeaderAndEpoch.UNKNOWN;
        private final OptionalInt localId;
        private Optional<SnapshotReader<String>> snapshot = Optional.empty();
        private Optional<SnapshotReader<String>> bootstrapSnapshot = Optional.empty();
        private boolean readCommit = true;

        MockListener(OptionalInt localId) {
            this.localId = localId;
        }

        int numCommittedBatches() {
            return commits.size();
        }

        Long claimedEpochStartOffset(int epoch) {
            return claimedEpochStartOffsets.get(epoch);
        }

        LeaderAndEpoch currentLeaderAndEpoch() {
            return currentLeaderAndEpoch;
        }

        List<Batch<String>> committedBatches() {
            return commits;
        }

        Batch<String> lastCommit() {
            if (commits.isEmpty()) {
                return null;
            } else {
                return commits.get(commits.size() - 1);
            }
        }

        OptionalLong lastCommitOffset() {
            if (commits.isEmpty()) {
                return OptionalLong.empty();
            } else {
                return OptionalLong.of(commits.get(commits.size() - 1).lastOffset());
            }
        }

        Optional<VoterSet> lastCommittedVoterSet() {
            return commits.stream()
                .flatMap(batch -> batch.controlRecords().stream())
                .flatMap(controlRecord -> {
                    if (controlRecord.type() == ControlRecordType.KRAFT_VOTERS) {
                        return Stream.of((VotersRecord) controlRecord.message());
                    } else {
                        return Stream.empty();
                    }
                })
                .reduce((accumulated, current) -> current)
                .map(VoterSet::fromVotersRecord);
        }

        OptionalInt currentClaimedEpoch() {
            if (localId.isPresent() && currentLeaderAndEpoch.isLeader(localId.getAsInt())) {
                return OptionalInt.of(currentLeaderAndEpoch.epoch());
            } else {
                return OptionalInt.empty();
            }
        }

        List<String> commitWithLastOffset(long lastOffset) {
            return commits.stream()
                .filter(batch -> batch.lastOffset() == lastOffset)
                .findFirst()
                .map(Batch::records)
                .orElse(null);
        }

        Optional<SnapshotReader<String>> drainHandledSnapshot() {
            Optional<SnapshotReader<String>> temp = snapshot;
            snapshot = Optional.empty();
            return temp;
        }

        void updateReadCommit(boolean readCommit) {
            this.readCommit = readCommit;

            if (readCommit) {
                for (BatchReader<String> batch : savedBatches) {
                    readBatch(batch);
                }

                savedBatches.clear();
            }
        }

        void readBatch(BatchReader<String> reader) {
            try (reader) {
                while (reader.hasNext()) {
                    long nextOffset = lastCommitOffset().isPresent() ?
                        lastCommitOffset().getAsLong() + 1 : 0L;
                    Batch<String> batch = reader.next();
                    // We expect monotonic offsets, but not necessarily sequential
                    // offsets since control records will be filtered.
                    assertTrue(batch.baseOffset() >= nextOffset,
                        "Received non-monotonic commit " + batch +
                            ". We expected an offset at least as large as " + nextOffset);
                    commits.add(batch);
                }
            }
        }

        @Override
        public void handleLeaderChange(LeaderAndEpoch leaderAndEpoch) {
            // We record the current committed offset as the claimed epoch's start
            // offset. This is useful to verify that the `handleLeaderChange` callback
            // was not received early on the leader.
            assertTrue(
                leaderAndEpoch.epoch() >= currentLeaderAndEpoch.epoch(),
                String.format("new epoch (%d) not >= than old epoch (%d)", leaderAndEpoch.epoch(), currentLeaderAndEpoch.epoch())
            );
            assertNotEquals(currentLeaderAndEpoch, leaderAndEpoch);
            this.currentLeaderAndEpoch = leaderAndEpoch;

            currentClaimedEpoch().ifPresent(claimedEpoch -> {
                long claimedEpochStartOffset = lastCommitOffset().isPresent() ?
                    lastCommitOffset().getAsLong() : 0L;
                this.claimedEpochStartOffsets.put(leaderAndEpoch.epoch(), claimedEpochStartOffset);
            });
        }

        @Override
        public void handleCommit(BatchReader<String> reader) {
            if (readCommit) {
                readBatch(reader);
            } else {
                savedBatches.add(reader);
            }
        }

        @Override
        public void handleLoadSnapshot(SnapshotReader<String> reader) {
            snapshot = handleLoadSnapshotOrBootstrap(snapshot, reader);
        }

        @Override
        public void handleLoadBootstrap(SnapshotReader<String> reader) {
            bootstrapSnapshot = handleLoadSnapshotOrBootstrap(bootstrapSnapshot, reader);
        }

        private Optional<SnapshotReader<String>> handleLoadSnapshotOrBootstrap(
            Optional<SnapshotReader<String>> previousSnapshot,
            SnapshotReader<String> reader
        ) {
            previousSnapshot.ifPresent(s -> assertDoesNotThrow(s::close));
            commits.clear();
            savedBatches.clear();
            return Optional.of(reader);
        }

        Optional<SnapshotReader<String>> drainHandledBootstrapSnapshot() {
            Optional<SnapshotReader<String>> temp = bootstrapSnapshot;
            bootstrapSnapshot = Optional.empty();
            return temp;
        }
    }

}
