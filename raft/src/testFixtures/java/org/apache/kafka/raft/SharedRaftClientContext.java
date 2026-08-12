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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.test.TestCondition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * The shared machinery for driving a mock {@link KafkaRaftClient} through the raft protocol.
 * {@link RaftClientTestContext} extends this and overrides the relevant helpers to add protocol
 * assertions, calling {@code super} for the shared work.
 */
public abstract class SharedRaftClientContext {
    final TopicPartition metadataPartition = RaftClientContextBuilder.METADATA_PARTITION;
    final Uuid metadataTopicId = Uuid.METADATA_TOPIC_ID;
    final int fetchMaxBytes;

    int electionTimeoutMs;

    final MockQuorumStateStore quorumStateStore;
    final String clusterId;
    final OptionalInt localId;
    public final Uuid localDirectoryId;
    public final KRaftVersion kraftVersion;
    public final KafkaRaftClient<String> client;
    public final MockLog log;
    final MockNetworkChannel channel;
    final MockTime time;
    final VoterSet startingVoters;
    // Used to determine which RPC request and response to construct
    final RaftProtocol raftProtocol;

    private final List<RaftResponse.Outbound> sentResponses = new ArrayList<>();
    private final List<Throwable> uncaughtExceptions = new ArrayList<>();

    private static final int MAX_POLLS = 50;

    @SuppressWarnings("ParameterNumber")
    SharedRaftClientContext(
        String clusterId,
        OptionalInt localId,
        Uuid localDirectoryId,
        KRaftVersion kraftVersion,
        KafkaRaftClient<String> client,
        MockLog log,
        MockNetworkChannel channel,
        MockTime time,
        MockQuorumStateStore quorumStateStore,
        VoterSet startingVoters,
        RaftProtocol raftProtocol,
        int fetchMaxBytes
    ) {
        this.clusterId = clusterId;
        this.localId = localId;
        this.localDirectoryId = localDirectoryId;
        this.kraftVersion = kraftVersion;
        this.client = client;
        this.log = log;
        this.channel = channel;
        this.time = time;
        this.quorumStateStore = quorumStateStore;
        this.startingVoters = startingVoters;
        this.raftProtocol = raftProtocol;
        this.fetchMaxBytes = fetchMaxBytes;
    }

    public void unattachedToCandidate() throws Exception {
        time.sleep(electionTimeoutMs * 2L);
        expectAndGrantPreVotes(currentEpoch());
    }

    public void unattachedToLeader() throws Exception {
        int currentEpoch = currentEpoch();
        unattachedToCandidate();
        expectAndGrantVotes(currentEpoch + 1);
        expectBeginEpoch(currentEpoch + 1);
    }

    public OptionalInt currentLeader() {
        return currentLeaderAndEpoch().leaderId();
    }

    public int currentEpoch() {
        return currentLeaderAndEpoch().epoch();
    }

    LeaderAndEpoch currentLeaderAndEpoch() {
        ElectionState election = quorumStateStore.readElectionState().get();
        return new LeaderAndEpoch(election.optionalLeaderId(), election.epoch());
    }

    void expectAndGrantVotes(int epoch) throws Exception {
        pollUntilRequest();

        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch,
            log.lastFetchedEpoch(), log.endOffset().offset());

        for (RaftRequest.Outbound request : voteRequests) {
            VoteResponseData voteResponse = voteResponse(true, OptionalInt.empty(), epoch);
            deliverResponse(request.correlationId(), request.destination(), voteResponse);
        }

        pollUntil(() -> client.quorum().isLeader());
    }

    void expectAndGrantPreVotes(int epoch) throws Exception {
        pollUntilRequest();

        List<RaftRequest.Outbound> voteRequests = collectPreVoteRequests(
            epoch,
            log.lastFetchedEpoch(),
            log.endOffset().offset()
        );

        for (RaftRequest.Outbound request : voteRequests) {
            if (!raftProtocol.isPreVoteSupported()) {
                deliverResponse(
                    request.correlationId(),
                    request.destination(),
                    RaftUtil.errorResponse(ApiKeys.VOTE, Errors.UNSUPPORTED_VERSION)
                );
            } else {
                VoteResponseData voteResponse = voteResponse(true, OptionalInt.empty(), epoch);
                deliverResponse(request.correlationId(), request.destination(), voteResponse);
            }
        }

        pollUntil(() -> client.quorum().isCandidate());
    }

    int localIdOrThrow() {
        return localId.orElseThrow(() -> new AssertionError("Required local id is not defined"));
    }

    private void expectBeginEpoch(int epoch) throws Exception {
        pollUntilRequest();
        for (RaftRequest.Outbound request : collectBeginEpochRequests(epoch)) {
            BeginQuorumEpochResponseData beginEpochResponse = beginEpochResponse(epoch, localIdOrThrow());
            deliverResponse(request.correlationId(), request.destination(), beginEpochResponse);
            poll();
        }
    }

    /**
     * Asserts that no uncaught exceptions occurred in async callbacks (e.g., CompletionStage.whenComplete).
     * This method is automatically called by the poll() wrapper method, but can also be called directly
     * by tests to check for async exceptions at any point.
     *
     * @throws AssertionError if any uncaught exceptions were captured
     */
    public void assertNoAsyncExceptions() {
        if (!uncaughtExceptions.isEmpty()) {
            Throwable first = uncaughtExceptions.get(0);
            uncaughtExceptions.clear();
            throw new AssertionError("Uncaught exception in async callback", first);
        }
    }

    /**
     * Poll for new events and check for any uncaught exceptions in async callbacks.
     * This is a wrapper around client.poll() that also calls assertNoAsyncExceptions().
     */
    public void poll() {
        client.poll();
        assertNoAsyncExceptions();
    }

    public void pollUntil(TestCondition condition) throws InterruptedException {
        try {
            for (int remaining = MAX_POLLS; remaining > 0; remaining--) {
                poll();
                if (condition.conditionMet()) {
                    return;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new IllegalStateException(
            String.format("Condition not met within %d polls", MAX_POLLS)
        );
    }

    public void pollUntilResponse() throws InterruptedException {
        pollUntil(() -> !sentResponses.isEmpty());
    }

    void pollUntilRequest() throws InterruptedException {
        pollUntil(channel::hasSentRequests);
    }

    List<RaftRequest.Outbound> collectPreVoteRequests(
        int epoch,
        int lastEpoch,
        long lastEpochOffset
    ) {
        return collectVoteRequestMessages();
    }

    List<RaftRequest.Outbound> collectVoteRequests(
        int epoch,
        int lastEpoch,
        long lastEpochOffset
    ) {
        return collectVoteRequestMessages();
    }

    private List<RaftRequest.Outbound> collectVoteRequestMessages() {
        List<RaftRequest.Outbound> voteRequests = new ArrayList<>();
        for (RaftRequest.Outbound raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof VoteRequestData) {
                voteRequests.add(raftMessage);
            }
        }
        return voteRequests;
    }

    public void deliverRequest(ApiMessage request) {
        short version = raftRequestVersion(request);
        deliverRequest(request, version);
    }

    void deliverRequest(ApiMessage request, short version) {
        deliverRequest(inboundRequest(request, version));
    }

    public RaftRequest.Inbound inboundRequest(ApiMessage request) {
        return inboundRequest(request, raftRequestVersion(request));
    }

    RaftRequest.Inbound inboundRequest(ApiMessage request, short version) {
        return new RaftRequest.Inbound(
            channel.listenerName(),
            channel.newCorrelationId(),
            version,
            request,
            time.milliseconds()
        );
    }

    private void deliverRequest(RaftRequest.Inbound inboundRequest) {
        client.handle(inboundRequest).whenComplete((response, exception) -> {
            if (exception != null) {
                uncaughtExceptions.add(exception);
            } else {
                sentResponses.add(response);
            }
        });
    }

    void deliverResponse(int correlationId, Node source, ApiMessage response) {
        channel.mockReceive(new RaftResponse.Inbound(correlationId, response, source));
    }

    List<RaftResponse.Outbound> drainSentResponses(
        ApiKeys apiKey
    ) {
        List<RaftResponse.Outbound> res = new ArrayList<>();
        Iterator<RaftResponse.Outbound> iterator = sentResponses.iterator();
        while (iterator.hasNext()) {
            RaftResponse.Outbound response = iterator.next();
            if (response.data().apiKey() == apiKey.id) {
                res.add(response);
                iterator.remove();
            }
        }
        return res;
    }

    List<RaftRequest.Outbound> collectBeginEpochRequests(int epoch) {
        return new ArrayList<>(channel.drainSentRequests(Optional.of(ApiKeys.BEGIN_QUORUM_EPOCH)));
    }

    BeginQuorumEpochResponseData beginEpochResponse(int epoch, int leaderId) {
        return RaftUtil.singletonBeginQuorumEpochResponse(
            channel.listenerName(),
            raftProtocol.beginQuorumEpochRpcVersion(),
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId,
            startingVoters.listeners(leaderId)
        );
    }

    VoteRequestData voteRequest(
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
            false
        );
    }

    VoteRequestData voteRequest(
        String clusterId,
        int epoch,
        ReplicaKey candidateKey,
        int lastEpoch,
        long lastEpochOffset,
        boolean preVote
    ) {
        ReplicaKey localReplicaKey = raftProtocol.isReconfigSupported() ?
            ReplicaKey.of(localIdOrThrow(), localDirectoryId) :
            ReplicaKey.of(-1, ReplicaKey.NO_DIRECTORY_ID);

        return voteRequest(
            clusterId,
            epoch,
            candidateKey,
            localReplicaKey,
            lastEpoch,
            lastEpochOffset,
            preVote
        );
    }

    VoteRequestData voteRequest(
        String clusterId,
        int epoch,
        ReplicaKey candidateKey,
        ReplicaKey voterKey,
        int lastEpoch,
        long lastEpochOffset,
        boolean preVote
    ) {
        return RaftUtil.singletonVoteRequest(
                metadataPartition,
                clusterId,
                epoch,
                candidateKey,
                voterKey,
                lastEpoch,
                lastEpochOffset,
                preVote
        );
    }

    VoteResponseData voteResponse(boolean voteGranted, OptionalInt leaderId, int epoch) {
        return voteResponse(Errors.NONE, voteGranted, leaderId, epoch, raftProtocol.voteRpcVersion());
    }

    VoteResponseData voteResponse(Errors error, OptionalInt leaderId, int epoch) {
        return voteResponse(error, false, leaderId, epoch, raftProtocol.voteRpcVersion());
    }

    VoteResponseData voteResponse(Errors error, boolean voteGranted, OptionalInt leaderId, int epoch, short version) {
        return RaftUtil.singletonVoteResponse(
            channel.listenerName(),
            version,
            Errors.NONE,
            metadataPartition,
            error,
            epoch,
            leaderId.orElse(-1),
            voteGranted,
            leaderId.isPresent() ? startingVoters.listeners(leaderId.getAsInt()) : Endpoints.empty()
        );
    }

    public FetchRequestData fetchRequest(
        int epoch,
        ReplicaKey replicaKey,
        long fetchOffset,
        int lastFetchedEpoch,
        int maxWaitTimeMs
    ) {
        return fetchRequest(
            epoch,
            replicaKey,
            fetchOffset,
            lastFetchedEpoch,
            OptionalLong.of(Long.MAX_VALUE),
            maxWaitTimeMs
        );
    }

    FetchRequestData fetchRequest(
        int epoch,
        ReplicaKey replicaKey,
        long fetchOffset,
        int lastFetchedEpoch,
        OptionalLong highWatermark,
        int maxWaitTimeMs
    ) {
        return fetchRequest(
            epoch,
            clusterId,
            replicaKey,
            fetchOffset,
            lastFetchedEpoch,
            highWatermark,
            maxWaitTimeMs
        );
    }

    FetchRequestData fetchRequest(
        int epoch,
        String clusterId,
        ReplicaKey replicaKey,
        long fetchOffset,
        int lastFetchedEpoch,
        OptionalLong highWatermark,
        int maxWaitTimeMs
    ) {
        FetchRequestData request = RaftUtil.singletonFetchRequest(
            metadataPartition,
            metadataTopicId,
            fetchPartition -> {
                fetchPartition
                    .setCurrentLeaderEpoch(epoch)
                    .setLastFetchedEpoch(lastFetchedEpoch)
                    .setFetchOffset(fetchOffset)
                    .setHighWatermark(highWatermark.orElse(-1));
                if (raftProtocol.isReconfigSupported()) {
                    fetchPartition
                        .setReplicaDirectoryId(replicaKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));
                }
            }
        );
        return request
            .setMaxWaitMs(maxWaitTimeMs)
            .setClusterId(clusterId)
            .setMaxBytes(fetchMaxBytes)
            .setReplicaState(
                new FetchRequestData.ReplicaState().setReplicaId(replicaKey.id())
            );
    }

    public DescribeQuorumRequestData describeQuorumRequest() {
        return RaftUtil.singletonDescribeQuorumRequest(metadataPartition);
    }

    private short raftRequestVersion(ApiMessage request) {
        if (request instanceof FetchRequestData) {
            return raftProtocol.fetchRpcVersion();
        } else if (request instanceof FetchSnapshotRequestData) {
            return raftProtocol.fetchSnapshotRpcVersion();
        } else if (request instanceof VoteRequestData) {
            return raftProtocol.voteRpcVersion();
        } else if (request instanceof BeginQuorumEpochRequestData) {
            return raftProtocol.beginQuorumEpochRpcVersion();
        } else if (request instanceof EndQuorumEpochRequestData) {
            return raftProtocol.endQuorumEpochRpcVersion();
        } else if (request instanceof DescribeQuorumRequestData) {
            return raftProtocol.describeQuorumRpcVersion();
        } else if (request instanceof AddRaftVoterRequestData) {
            return raftProtocol.addVoterRpcVersion();
        } else if (request instanceof RemoveRaftVoterRequestData) {
            return raftProtocol.removeVoterRpcVersion();
        } else if (request instanceof UpdateRaftVoterRequestData) {
            return raftProtocol.updateVoterRpcVersion();
        } else {
            throw new IllegalArgumentException(String.format("Request %s is not a raft request", request));
        }
    }

    public void advanceLocalLeaderHighWatermarkToLogEndOffset() throws InterruptedException {
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
            drainSentResponses(ApiKeys.FETCH);
        }

        pollUntil(() -> OptionalLong.of(localLogEndOffset).equals(client.highWatermark()));
    }

    /**
     * Determines what versions of RPCs are in use. Note, these are ordered from oldest to newest, and are
     * cumulative. E.g. KIP_1186_PROTOCOL includes KIP_996_PROTOCOL, KIP_853_PROTOCOL, and KIP_595_PROTOCOL changes
     */
    public enum RaftProtocol {
        // kraft support
        KIP_595_PROTOCOL,
        // dynamic quorum reconfiguration support
        KIP_853_PROTOCOL,
        // preVote support
        KIP_996_PROTOCOL,
        // HWM in FETCH request support
        KIP_1166_PROTOCOL,
        // autoJoin support
        KIP_1186_PROTOCOL;

        boolean isReconfigSupported() {
            return isAtLeast(KIP_853_PROTOCOL);
        }

        boolean isPreVoteSupported() {
            return isAtLeast(KIP_996_PROTOCOL);
        }

        short describeQuorumRpcVersion() {
            if (isAtLeast(KIP_853_PROTOCOL)) {
                return 2;
            } else {
                return 1;
            }
        }

        short fetchRpcVersion() {
            if (isAtLeast(KIP_1166_PROTOCOL)) {
                return 18;
            } else if (isAtLeast(KIP_853_PROTOCOL)) {
                return 17;
            } else {
                return 16;
            }
        }

        short fetchSnapshotRpcVersion() {
            if (isAtLeast(KIP_853_PROTOCOL)) {
                return 1;
            } else {
                return 0;
            }
        }

        short voteRpcVersion() {
            if (isAtLeast(KIP_996_PROTOCOL)) {
                return 2;
            } else if (isAtLeast(KIP_853_PROTOCOL)) {
                return 1;
            } else {
                return 0;
            }
        }

        short beginQuorumEpochRpcVersion() {
            if (isAtLeast(KIP_853_PROTOCOL)) {
                return 1;
            } else {
                return 0;
            }
        }

        short endQuorumEpochRpcVersion() {
            if (isAtLeast(KIP_853_PROTOCOL)) {
                return 1;
            } else {
                return 0;
            }
        }

        short addVoterRpcVersion() {
            if (isAtLeast(KIP_1186_PROTOCOL)) {
                return 1;
            } else if (isAtLeast(KIP_853_PROTOCOL)) {
                return 0;
            } else {
                throw new IllegalStateException("Reconfiguration must be enabled by calling withRaftProtocol(KIP_853_PROTOCOL)");
            }
        }

        short removeVoterRpcVersion() {
            if (isAtLeast(KIP_853_PROTOCOL)) {
                return 0;
            } else {
                throw new IllegalStateException("Reconfiguration must be enabled by calling withRaftProtocol(KIP_853_PROTOCOL)");
            }
        }

        short updateVoterRpcVersion() {
            if (isAtLeast(KIP_853_PROTOCOL)) {
                return 0;
            } else {
                throw new IllegalStateException("Reconfiguration must be enabled by calling withRaftProtocol(KIP_853_PROTOCOL)");
            }
        }

        private boolean isAtLeast(RaftProtocol otherRpc) {
            return this.compareTo(otherRpc) >= 0;
        }
    }
}
