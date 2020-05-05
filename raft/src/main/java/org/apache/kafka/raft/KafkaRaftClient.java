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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchQuorumRecordsRequestData;
import org.apache.kafka.common.message.FetchQuorumRecordsResponseData;
import org.apache.kafka.common.message.FindQuorumRequestData;
import org.apache.kafka.common.message.FindQuorumResponseData;
import org.apache.kafka.common.message.LeaderChangeMessageData;
import org.apache.kafka.common.message.LeaderChangeMessageData.Voter;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.ConnectionCache.HostInfo;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class implements a Kafkaesque version of the Raft protocol. Leader election
 * is more or less pure Raft, but replication is driven by replica fetching and we use Kafka's
 * log reconciliation protocol to truncate the log to a common point following each leader
 * election.
 *
 * Like Zookeeper, this protocol distinguishes between voters and observers. Voters are
 * the only ones who are eligible to handle protocol requests and they are the only ones
 * who take part in elections. The protocol does not yet support dynamic quorum changes.
 *
 * There are seven request types in this protocol:
 *
 * 1) {@link VoteRequestData}: Sent by valid voters when their election timeout expires and they
 *    become a candidate. This request includes the last offset in the log which electors use
 *    to tell whether or not to grant the vote.
 *
 * 2) {@link BeginQuorumEpochRequestData}: Sent by the leader of an epoch only to valid voters to
 *    assert its leadership of the new epoch. This request will be retried indefinitely for
 *    each voter until it acknowledges the request or a new election occurs.
 *
 *    This is not needed in usual Raft because the leader can use an empty data push
 *    to achieve the same purpose. The Kafka Raft implementation, however, is driven by
 *    fetch requests from followers, so there must be a way to find the new leader after
 *    an election has completed.
 *
 *    We might consider replacing this API and let followers use FindLeader even if they
 *    are voters.
 *
 * 3) {@link EndQuorumEpochRequestData}: Sent by the leader of an epoch to valid voters in order to
 *    gracefully resign from the current epoch. This causes remaining voters to immediately
 *    begin a new election.
 *
 * 4) {@link FetchQuorumRecordsRequestData}: This is basically the same as the usual Fetch API in
 *    Kafka, however the protocol implements it as a separate request type because there
 *    is additional metadata which we need to piggyback on responses. Unlike partition replication,
 *    we also piggyback truncation detection on this API rather than through a separate state.
 *
 * 5) {@link FindQuorumRequestData}: Sent by observers in order to find the leader. The leader
 *    is responsible for pushing BeginQuorumEpoch requests to other votes, but it is the responsibility
 *    of observers to find the current leader themselves. We could probably use one of the Fetch
 *    APIs for the same purpose, but we separated it initially for clarity.
 *
 */
public class KafkaRaftClient implements RaftClient {
    private final AtomicReference<GracefulShutdown> shutdown = new AtomicReference<>();
    private final Logger logger;
    private final Time time;
    private final Timer electionTimer;
    private final int electionTimeoutMs;
    private final int electionJitterMs;
    private final int retryBackoffMs;
    private final int requestTimeoutMs;
    private final long bootTimestamp;
    private final InetSocketAddress advertisedListener;
    private final NetworkChannel channel;
    private final ReplicatedLog log;
    private final QuorumState quorum;
    private final Random random = new Random();
    private final ConnectionCache connections;

    private BlockingQueue<PendingAppendRequest> unsentAppends;
    private DistributedStateMachine stateMachine;

    public KafkaRaftClient(NetworkChannel channel,
                           ReplicatedLog log,
                           QuorumState quorum,
                           Time time,
                           InetSocketAddress advertisedListener,
                           List<InetSocketAddress> bootstrapServers,
                           int electionTimeoutMs,
                           int electionJitterMs,
                           int retryBackoffMs,
                           int requestTimeoutMs,
                           LogContext logContext) {
        this.channel = channel;
        this.log = log;
        this.quorum = quorum;
        this.time = time;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.retryBackoffMs = retryBackoffMs;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionJitterMs = electionJitterMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.bootTimestamp = time.milliseconds();
        this.advertisedListener = advertisedListener;
        this.logger = logContext.logger(KafkaRaftClient.class);
        this.connections = new ConnectionCache(channel, bootstrapServers,
            retryBackoffMs, requestTimeoutMs, logContext);
        this.unsentAppends = new ArrayBlockingQueue<>(10);
        this.connections.maybeUpdate(quorum.localId, new HostInfo(advertisedListener, bootTimestamp));
    }

    private void applyCommittedRecordsToStateMachine() {
        quorum.highWatermark().ifPresent(highWatermark -> {
            while (stateMachine.position().offset < highWatermark && shutdown.get() == null) {
                OffsetAndEpoch position = stateMachine.position();
                Records records = readCommitted(position);
                logger.trace("Applying committed records at {} to the state machine", position);
                stateMachine.apply(records);
            }
        });
    }

    private void updateFollowerHighWatermark(FollowerState state, OptionalLong highWatermarkOpt) {
        highWatermarkOpt.ifPresent(highWatermark -> {
            long newHighWatermark = Math.min(endOffset().offset, highWatermark);
            state.updateHighWatermark(OptionalLong.of(newHighWatermark));
            highWatermarkOpt.ifPresent(log::updateHighWatermark);
            logger.trace("Follower high watermark updated to {}", newHighWatermark);
            applyCommittedRecordsToStateMachine();
        });
    }

    private void updateLeaderEndOffset(LeaderState state) {
        if (state.updateLocalEndOffset(log.endOffset())) {
            logger.trace("Leader high watermark updated to {} after end offset updated to {}",
                    state.highWatermark(), log.endOffset());
            applyCommittedRecordsToStateMachine();
        }
    }

    private void updateReplicaEndOffset(LeaderState state, int replicaId, long endOffset) {
        if (state.updateEndOffset(replicaId, endOffset)) {
            logger.trace("Leader high watermark updated to {} after replica {} end offset updated to {}",
                    state.highWatermark(), replicaId, endOffset);
            applyCommittedRecordsToStateMachine();
        }
    }

    @Override
    public void initialize(DistributedStateMachine stateMachine) throws IOException {
        this.stateMachine = stateMachine;
        quorum.initialize(log.endOffset());

        if (quorum.isLeader()) {
            electionTimer.reset(Long.MAX_VALUE);
            onBecomeLeader(quorum.leaderStateOrThrow());
        } else if (quorum.isCandidate()) {
            // If the quorum consists of a single node, we can become leader immediately
            electionTimer.reset(electionTimeoutMs);
            maybeBecomeLeader(quorum.candidateStateOrThrow());
        } else if (quorum.isFollower()) {
            FollowerState state = quorum.followerStateOrThrow();
            if (quorum.isVoter() && quorum.epoch() == 0) {
                // If we're initializing for the first time, become a candidate immediately
                becomeCandidate();
            } else {
                electionTimer.reset(electionTimeoutMs);
                if (state.hasLeader())
                    onBecomeFollowerOfElectedLeader(quorum.followerStateOrThrow());
            }
        }
    }

    private OffsetAndEpoch endOffset() {
        return new OffsetAndEpoch(log.endOffset(), log.lastFetchedEpoch());
    }

    private void resetConnections() {
        connections.resetAll();
    }

    private void onBecomeLeader(LeaderState state) {
        stateMachine.becomeLeader(quorum.epoch());
        updateLeaderEndOffset(state);

        // Add a control message for faster high watermark advance.
        appendControlRecord(MemoryRecords.withLeaderChangeMessage(
            time.milliseconds(),
            quorum.epoch(),
            new LeaderChangeMessageData()
                .setLeaderId(state.election().leaderId())
                .setVoters(
                    state.followers().stream().map(
                        follower -> new Voter().setVoterId(follower)).collect(Collectors.toList())))
        );

        log.assignEpochStartOffset(quorum.epoch(), log.endOffset());
        electionTimer.reset(Long.MAX_VALUE);
        resetConnections();
    }

    private void appendControlRecord(Records controlRecord) {
        if (shutdown.get() != null)
            throw new IllegalStateException("Cannot append records while we are shutting down");
        log.appendAsLeader(controlRecord, quorum.epoch());
    }

    private void maybeBecomeLeader(CandidateState state) throws IOException {
        if (state.isVoteGranted()) {
            long endOffset = log.endOffset();
            LeaderState leaderState = quorum.becomeLeader(endOffset);
            onBecomeLeader(leaderState);
        }
    }

    private void becomeCandidate() throws IOException {
        electionTimer.reset(electionTimeoutMs);
        CandidateState state = quorum.becomeCandidate();
        maybeBecomeLeader(state);
        resetConnections();
    }

    private void becomeUnattachedFollower(int epoch) throws IOException {
        boolean isLeader = quorum.isLeader();
        if (quorum.becomeUnattachedFollower(epoch)) {
            resetConnections();

            if (isLeader) {
                electionTimer.reset(electionTimeoutMs);
            }
        }
    }

    private void becomeVotedFollower(int candidateId, int epoch) throws IOException {
        if (quorum.becomeVotedFollower(epoch, candidateId)) {
            electionTimer.reset(electionTimeoutMs);
            resetConnections();
        }
    }

    private void onBecomeFollowerOfElectedLeader(FollowerState state) {
        stateMachine.becomeFollower(state.epoch());
        electionTimer.reset(electionTimeoutMs);
        resetConnections();
    }

    private void becomeFollower(int leaderId, int epoch) throws IOException {
        if (quorum.becomeFollower(epoch, leaderId)) {
            onBecomeFollowerOfElectedLeader(quorum.followerStateOrThrow());
        }
    }

    private void maybeBeginFetching(OffsetAndEpoch prevEpochEndOffset) {
        log.truncateToEndOffset(prevEpochEndOffset);
    }

    private VoteResponseData buildVoteResponse(Errors error, boolean voteGranted) {
        return new VoteResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil())
                .setVoteGranted(voteGranted);
    }

    private VoteResponseData handleVoteRequest(VoteRequestData request) throws IOException {
        Optional<Exception> errorOpt = handleInvalidVoterOnlyRequest(request.candidateId(), request.candidateEpoch());
        if (errorOpt.isPresent()) {
            return buildVoteResponse(Errors.forException(errorOpt.get()), false);
        }

        if (quorum.isLeader()) {
            logger.debug("Ignoring vote request {} since we are the leader of epoch {}",
                    request, quorum.epoch());
            return buildVoteResponse(Errors.NONE, false);
        } else if (quorum.isCandidate()) {
            logger.debug("Ignoring vote request {} since we are a candidate of epoch {}",
                    request, quorum.epoch());
            return buildVoteResponse(Errors.NONE, false);
        } else {
            FollowerState state = quorum.followerStateOrThrow();
            int candidateId = request.candidateId();
            final boolean voteGranted;
            if (state.hasLeader()) {
                voteGranted = false;
            } else if (state.hasVoted()) {
                voteGranted = state.isVotedCandidate(candidateId);
            } else {
                OffsetAndEpoch lastEpochEndOffset = new OffsetAndEpoch(request.lastEpochEndOffset(), request.lastEpoch());
                voteGranted = lastEpochEndOffset.compareTo(endOffset()) >= 0;
            }

            if (voteGranted)
                becomeVotedFollower(candidateId, request.candidateEpoch());
            return buildVoteResponse(Errors.NONE, voteGranted);
        }
    }

    private void handleVoteResponse(int remoteNodeId, VoteResponseData response) throws IOException {
        if (quorum.isLeader()) {
            logger.debug("Ignoring vote response {} since we already became leader for epoch {}",
                    response, quorum.epoch());
        } else if (quorum.isFollower()) {
            logger.debug("Ignoring vote response {} since we are now a follower for epoch {}",
                    response, quorum.epoch());
        } else {
            CandidateState state = quorum.candidateStateOrThrow();
            if (response.voteGranted()) {
                state.voteGrantedBy(remoteNodeId);
                maybeBecomeLeader(state);
            } else {
                state.voteRejectedBy(remoteNodeId);
                if (state.isVoteRejected()) {
                    logger.info("A majority of voters rejected our candidacy, so we will become a follower");
                    becomeUnattachedFollower(quorum.epoch());

                    // We will retry our candidacy after a short backoff if no leader is elected
                    electionTimer.reset(retryBackoffMs + randomElectionJitterMs());
                }
            }
        }
    }

    private int randomElectionJitterMs() {
        if (electionJitterMs == 0)
            return 0;
        return random.nextInt(electionJitterMs);
    }

    private BeginQuorumEpochResponseData buildBeginQuorumEpochResponse(Errors error) {
        return new BeginQuorumEpochResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil());
    }

    private BeginQuorumEpochResponseData handleBeginQuorumEpochRequest(BeginQuorumEpochRequestData request) throws IOException {
        Optional<Exception> errorOpt = handleInvalidVoterOnlyRequest(request.leaderId(), request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildBeginQuorumEpochResponse(Errors.forException(errorOpt.get()));
        }

        int requestLeaderId = request.leaderId();
        int requestEpoch = request.leaderEpoch();
        becomeFollower(requestLeaderId, requestEpoch);
        return buildBeginQuorumEpochResponse(Errors.NONE);
    }

    private void handleBeginQuorumEpochResponse(int remoteNodeId, BeginQuorumEpochResponseData response) {
        LeaderState state = quorum.leaderStateOrThrow();
        state.addEndorsementFrom(remoteNodeId);
    }

    private EndQuorumEpochResponseData buildEndQuorumEpochResponse(Errors error) {
        return new EndQuorumEpochResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil());
    }

    private EndQuorumEpochResponseData handleEndQuorumEpochRequest(EndQuorumEpochRequestData request) throws IOException {
        Optional<Exception> errorOpt = handleInvalidVoterOnlyRequest(request.leaderId(), request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildEndQuorumEpochResponse(Errors.forException(errorOpt.get()));
        }

        // Regardless of our current state, we will become a candidate.
        // Do not become a candidate immediately though.
        electionTimer.reset(randomElectionJitterMs());
        return buildEndQuorumEpochResponse(Errors.NONE);
    }

    private FetchQuorumRecordsResponseData buildFetchQuorumRecordsResponse(
        Errors error,
        Records records,
        OptionalLong highWatermark
    ) throws IOException {
        return new FetchQuorumRecordsResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil())
                .setRecords(RaftUtil.serializeRecords(records))
                .setHighWatermark(highWatermark.orElse(-1L));
    }

    private FetchQuorumRecordsResponseData buildOutOfRangeFetchQuorumRecordsResponse(
        OffsetAndEpoch nextOffsetAndEpoch,
        OptionalLong highWatermark
    ) {
        return new FetchQuorumRecordsResponseData()
            .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code())
            .setLeaderEpoch(quorum.epoch())
            .setLeaderId(quorum.leaderIdOrNil())
            .setNextFetchOffset(nextOffsetAndEpoch.offset)
            .setNextFetchOffsetEpoch(nextOffsetAndEpoch.epoch)
            .setRecords(ByteBuffer.wrap(new byte[0]))
            .setHighWatermark(highWatermark.orElse(-1L));
    }


    private FetchQuorumRecordsResponseData handleFetchQuorumRecordsRequest(
        FetchQuorumRecordsRequestData request
    ) throws IOException {
        Optional<Exception> errorOpt = handleInvalidLeaderOnlyRequest(request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildFetchQuorumRecordsResponse(Errors.forException(errorOpt.get()), MemoryRecords.EMPTY,
                    OptionalLong.empty());
        }

        LeaderState state = quorum.leaderStateOrThrow();
        long fetchOffset = request.fetchOffset();
        int lastFetchedEpoch = request.lastFetchedEpoch();
        int replicaId = request.replicaId();
        OptionalLong highWatermark = state.highWatermark();

        Optional<OffsetAndEpoch> nextOffsetOpt = validateFetchOffsetAndEpoch(fetchOffset, lastFetchedEpoch);
        if (nextOffsetOpt.isPresent()) {
            return buildOutOfRangeFetchQuorumRecordsResponse(nextOffsetOpt.get(), highWatermark);
        } else if (quorum.isVoter(replicaId)) {
            // Voters can read to the end of the log
            updateReplicaEndOffset(state, replicaId, fetchOffset);
            state.highWatermark().ifPresent(log::updateHighWatermark);
            Records records = log.read(fetchOffset, OptionalLong.empty());
            return buildFetchQuorumRecordsResponse(Errors.NONE, records, highWatermark);
        } else {
            Records records = highWatermark.isPresent() ?
                    log.read(fetchOffset, highWatermark) :
                    MemoryRecords.EMPTY;
            return buildFetchQuorumRecordsResponse(Errors.NONE, records, highWatermark);
        }
    }

    /**
     * Check whether a fetch offset and epoch is valid. Return the offset and epoch to truncate
     * to in case it is not.
     */
    private Optional<OffsetAndEpoch> validateFetchOffsetAndEpoch(long fetchOffset, int lastFetchedEpoch) {
        if (lastFetchedEpoch < 0) {
            return Optional.empty();
        }

        OffsetAndEpoch endOffsetAndEpoch = log.endOffsetForEpoch(lastFetchedEpoch)
            .orElse(new OffsetAndEpoch(-1L, -1));
        if (endOffsetAndEpoch.epoch != lastFetchedEpoch ||
            endOffsetAndEpoch.offset < fetchOffset) {
            return Optional.of(new OffsetAndEpoch(endOffsetAndEpoch.offset, endOffsetAndEpoch.epoch));
        } else {
            return Optional.empty();
        }
    }

    private OptionalInt optionalLeaderId(int leaderIdOrNil) {
        if (leaderIdOrNil < 0)
            return OptionalInt.empty();
        return OptionalInt.of(leaderIdOrNil);
    }

    private void handleFetchQuorumRecordsResponse(FetchQuorumRecordsResponseData response) {
        FollowerState state = quorum.followerStateOrThrow();

        if (response.errorCode() == Errors.OFFSET_OUT_OF_RANGE.code()) {
            if (response.nextFetchOffset() < 0 || response.nextFetchOffsetEpoch() < 0) {
                logger.warn("Leader returned an unknown truncation offset to our FetchQuorumRecords request");
            } else {
                OffsetAndEpoch endOffset = new OffsetAndEpoch(
                    response.nextFetchOffset(), response.nextFetchOffsetEpoch());
                maybeBeginFetching(endOffset);
            }
        } else {
            ByteBuffer recordsBuffer = response.records();
            log.appendAsFollower(MemoryRecords.readableRecords(recordsBuffer));
            logger.trace("Follower end offset updated to {} after append", endOffset());
            OptionalLong highWatermark = response.highWatermark() < 0 ?
                OptionalLong.empty() : OptionalLong.of(response.highWatermark());
            updateFollowerHighWatermark(state, highWatermark);
        }

        electionTimer.reset(electionTimeoutMs);
    }

    private OptionalLong maybeAppendAsLeader(LeaderState state, Records records) {
        if (state.highWatermark().isPresent() && stateMachine.accept(records)) {
            Long baseOffset = log.appendAsLeader(records, quorum.epoch());
            updateLeaderEndOffset(state);
            logger.trace("Leader appended records at base offset {}, new end offset is {}", baseOffset, endOffset());
            return OptionalLong.of(baseOffset);
        }
        return OptionalLong.empty();
    }

    private FindQuorumResponseData handleFindQuorumRequest(FindQuorumRequestData request) {
        // Only voters are allowed to handle FindLeader requests
        Errors error = Errors.NONE;
        if (shutdown.get() != null) {
            error = Errors.BROKER_NOT_AVAILABLE;
        } else {
            HostInfo hostInfo = new HostInfo(
                new InetSocketAddress(request.host(), request.port()),
                request.bootTimestamp()
            );
            if (quorum.isVoter(request.replicaId())) {
                connections.maybeUpdate(request.replicaId(), hostInfo);
            }
        }

        List<FindQuorumResponseData.Voter> voters = new ArrayList<>();
        for (Map.Entry<Integer, Optional<HostInfo>> voterEntry : connections.allVoters().entrySet()) {
            FindQuorumResponseData.Voter voter = new FindQuorumResponseData.Voter();
            voter.setVoterId(voterEntry.getKey());
            voterEntry.getValue().ifPresent(hostInfo -> {
                voter.setHost(hostInfo.address.getHostString())
                    .setPort(hostInfo.address.getPort())
                    .setBootTimestamp(hostInfo.bootTimestamp);
            });
            voters.add(voter);
        }

        return new FindQuorumResponseData()
            .setErrorCode(error.code())
            .setLeaderEpoch(quorum.epoch())
            .setLeaderId(quorum.leaderIdOrNil())
            .setVoters(voters);
    }

    private void handleFindQuorumResponse(FindQuorumResponseData response) throws IOException {
        // We only need to become a follower if the current leader is not elected
        OptionalInt leaderIdOpt = optionalLeaderId(response.leaderId());
        if (leaderIdOpt.isPresent() && leaderIdOpt.getAsInt() != quorum.localId) {
            becomeFollower(leaderIdOpt.getAsInt(), response.leaderEpoch());
        }
    }

    private void updateVoterConnections(FindQuorumResponseData response) {
        for (FindQuorumResponseData.Voter voter : response.voters()) {
            if (voter.host() == null || voter.host().isEmpty())
                continue;
            InetSocketAddress voterAddress = new InetSocketAddress(voter.host(), voter.port());
            connections.maybeUpdate(voter.voterId(),
                new HostInfo(voterAddress, voter.bootTimestamp()));
        }
    }

    private boolean hasInconsistentLeader(int epoch, OptionalInt leaderId) {
        // Only elected leaders are sent in the request/response header, so if we have an elected
        // leaderId, it should be consistent with what is in the message.
        if (epoch != quorum.epoch()) {
            return false;
        } else {
            if (!quorum.leaderId().isPresent())
                return false;
            if (!leaderId.isPresent())
                return false;
            return !quorum.leaderId().equals(leaderId);
        }
    }

    private void becomeFollower(int epoch, OptionalInt leaderId) throws IOException {
        if (leaderId.isPresent()) {
            becomeFollower(leaderId.getAsInt(), epoch);
        } else {
            becomeUnattachedFollower(epoch);
        }
    }

    private boolean maybeHandleError(RaftResponse.Inbound response, long currentTimeMs) throws IOException {
        ConnectionCache.ConnectionState connection = connections.getOrCreate(response.sourceId());

        final ApiMessage data = response.data();
        final Errors responseError;
        final int responseEpoch;
        final OptionalInt responseLeaderId;

        if (data instanceof FetchQuorumRecordsResponseData) {
            FetchQuorumRecordsResponseData fetchRecordsResponse = (FetchQuorumRecordsResponseData) data;
            responseError = Errors.forCode(fetchRecordsResponse.errorCode());

            if (responseError == Errors.OFFSET_OUT_OF_RANGE) {
                connection.onResponseReceived(response.requestId);
                return false;
            }

            responseEpoch = fetchRecordsResponse.leaderEpoch();
            responseLeaderId = optionalLeaderId(fetchRecordsResponse.leaderId());
        } else if (data instanceof VoteResponseData) {
            VoteResponseData voteResponse = (VoteResponseData) data;
            responseError = Errors.forCode(voteResponse.errorCode());
            responseEpoch = voteResponse.leaderEpoch();
            responseLeaderId = optionalLeaderId(voteResponse.leaderId());
        } else if (data instanceof BeginQuorumEpochResponseData) {
            BeginQuorumEpochResponseData beginEpochResponse = (BeginQuorumEpochResponseData) data;
            responseError = Errors.forCode(beginEpochResponse.errorCode());
            responseEpoch = beginEpochResponse.leaderEpoch();
            responseLeaderId = optionalLeaderId(beginEpochResponse.leaderId());
        } else if (data instanceof EndQuorumEpochResponseData) {
            EndQuorumEpochResponseData endEpochResponse = (EndQuorumEpochResponseData) data;
            responseError = Errors.forCode(endEpochResponse.errorCode());
            responseEpoch = endEpochResponse.leaderEpoch();
            responseLeaderId = optionalLeaderId(endEpochResponse.leaderId());
        } else if (data instanceof FindQuorumResponseData) {
            FindQuorumResponseData findQuorumResponse = (FindQuorumResponseData) data;
            responseError = Errors.forCode(findQuorumResponse.errorCode());
            responseEpoch = findQuorumResponse.leaderEpoch();
            responseLeaderId = optionalLeaderId(findQuorumResponse.leaderId());

            // TODO: Find a new home for this. The main point is that we need to do it even
            // if the response has an error
            updateVoterConnections(findQuorumResponse);
        } else {
            throw new IllegalStateException("Received unexpected response " + response);
        }

        connection.onResponse(response.requestId, responseError, currentTimeMs);
        if (handleNonMatchingResponseLeaderAndEpoch(responseEpoch, responseLeaderId)) {
            return true;
        } else if (responseError != Errors.NONE) {
            if (quorum.isObserver() && responseError == Errors.BROKER_NOT_AVAILABLE)
                becomeUnattachedFollower(quorum.epoch());

            logger.debug("Discarding response {} with error {}", data, responseError);
            return true;
        } else {
            return false;
        }
    }

    private boolean handleNonMatchingResponseLeaderAndEpoch(int epoch, OptionalInt leaderId) throws IOException {
        if (epoch < quorum.epoch()) {
            return true;
        } else if (epoch > quorum.epoch()) {
            // For any request type, if the response indicates a higher epoch, we bump our local
            // epoch and become a follower. Responses only include elected leaders.
            GracefulShutdown gracefulShutdown = shutdown.get();
            if (gracefulShutdown != null) {
                gracefulShutdown.onEpochUpdate(epoch);
            } else {
                becomeFollower(epoch, leaderId);
            }

            return true;
        } else if (hasInconsistentLeader(epoch, leaderId)) {
            throw new IllegalStateException("Received response with leader " + leaderId +
                    " which is inconsistent with current leader " + quorum.leaderId());
        }
        return false;
    }

    private void handleResponse(RaftResponse.Inbound response) throws IOException {
        // The response epoch matches the local epoch, so we can handle the response
        ApiMessage responseData = response.data();
        if (responseData instanceof FetchQuorumRecordsResponseData) {
            handleFetchQuorumRecordsResponse((FetchQuorumRecordsResponseData) responseData);
        } else if (responseData instanceof VoteResponseData) {
            handleVoteResponse(response.sourceId(), (VoteResponseData) responseData);
        } else if (responseData instanceof BeginQuorumEpochResponseData) {
            handleBeginQuorumEpochResponse(response.sourceId(), (BeginQuorumEpochResponseData) responseData);
        } else if (responseData instanceof EndQuorumEpochResponseData) {
            // Nothing to do for now
        } else if (responseData instanceof FindQuorumResponseData) {
            handleFindQuorumResponse((FindQuorumResponseData) responseData);
        } else {
            throw new IllegalStateException("Received unexpected response " + response);
        }
    }

    private Optional<Exception> handleInvalidVoterOnlyRequest(int remoteNodeId, int requestEpoch) throws IOException {
        if (quorum.isObserver()) {
            return Optional.of(Errors.INVALID_REQUEST.exception());
        } else if (!quorum.isVoter(remoteNodeId)) {
            return Optional.of(Errors.INVALID_REQUEST.exception());
        } else if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH.exception());
        } else if (shutdown.get() != null) {
            shutdown.get().onEpochUpdate(requestEpoch);
            return Optional.of(Errors.BROKER_NOT_AVAILABLE.exception());
        }

        // TODO: seems like a weird place to do this...
        if (requestEpoch > quorum.epoch()) {
            becomeUnattachedFollower(requestEpoch);
        }
        return Optional.empty();
    }

    private Optional<Exception> handleInvalidLeaderOnlyRequest(int requestEpoch) {
        if (quorum.isObserver()) {
            return Optional.of(new KafkaException("Observers are not allowed to receive requests"));
        } else if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH.exception());
        } else if (requestEpoch > quorum.epoch()) {
            // We cannot be the leader of an epoch we are not aware of
            return Optional.of(Errors.UNKNOWN_LEADER_EPOCH.exception());
        } else if (!quorum.isLeader()) {
            return Optional.of(Errors.NOT_LEADER_FOR_PARTITION.exception());
        } else if (shutdown.get() != null) {
            return Optional.of(Errors.BROKER_NOT_AVAILABLE.exception());
        }
        return Optional.empty();
    }

    private void handleRequest(RaftRequest.Inbound request) throws IOException {
        ApiMessage requestData = request.data();
        final ApiMessage responseData;
        if (requestData instanceof FetchQuorumRecordsRequestData) {
            responseData = handleFetchQuorumRecordsRequest((FetchQuorumRecordsRequestData) requestData);
        } else if (requestData instanceof VoteRequestData) {
            responseData = handleVoteRequest((VoteRequestData) requestData);
        } else if (requestData instanceof BeginQuorumEpochRequestData) {
            responseData = handleBeginQuorumEpochRequest((BeginQuorumEpochRequestData) requestData);
        } else if (requestData instanceof EndQuorumEpochRequestData) {
            responseData = handleEndQuorumEpochRequest((EndQuorumEpochRequestData) requestData);
        } else if (requestData instanceof FindQuorumRequestData) {
            responseData = handleFindQuorumRequest((FindQuorumRequestData) requestData);
        } else {
            throw new IllegalStateException("Unexpected request type " + requestData);
        }

        channel.send(new RaftResponse.Outbound(request.requestId(), responseData));
    }

    private void handleInboundMessage(RaftMessage message, long currentTimeMs) throws IOException {
        if (message instanceof RaftRequest.Inbound) {
            handleRequest((RaftRequest.Inbound) message);
        } else if (message instanceof RaftResponse.Inbound) {
            RaftResponse.Inbound response = (RaftResponse.Inbound) message;
            logger.debug("Received response from {}: {}", response.sourceId(), response.data);
            boolean responseHandled = maybeHandleError(response, currentTimeMs);
            if (!responseHandled)
                handleResponse(response);
        } else {
            throw new IllegalStateException("Unexpected message " + message);
        }
    }

    private OptionalInt maybeSendRequest(long currentTimeMs,
                                         int destinationId,
                                         Supplier<ApiMessage> requestData) throws IOException {
        ConnectionCache.ConnectionState connection = connections.getOrCreate(destinationId);
        if (quorum.isObserver() && connection.hasRequestTimedOut(currentTimeMs)) {
            // Observers need to proactively find the leader if there is a request timeout
            becomeUnattachedFollower(quorum.epoch());
        } else if (connection.isReady(currentTimeMs)) {
            int requestId = channel.newRequestId();
            ApiMessage request = requestData.get();
            logger.debug("Sending request with id {} to {}: {}", requestId, destinationId, request);

            channel.send(new RaftRequest.Outbound(requestId, request, destinationId, currentTimeMs));
            connection.onRequestSent(requestId, time.milliseconds());
            return OptionalInt.of(requestId);
        }
        return OptionalInt.empty();
    }

    private EndQuorumEpochRequestData buildEndQuorumEpochRequest() {
        return new EndQuorumEpochRequestData()
                .setReplicaId(quorum.localId)
                .setLeaderId(quorum.leaderIdOrNil())
                .setLeaderEpoch(quorum.epoch());
    }

    private void maybeSendEndQuorumEpoch(long currentTimeMs) throws IOException {
        for (Integer voterId : quorum.remoteVoters()) {
            maybeSendRequest(currentTimeMs, voterId, this::buildEndQuorumEpochRequest);
        }
    }

    private BeginQuorumEpochRequestData buildBeginQuorumEpochRequest() {
        return new BeginQuorumEpochRequestData()
                .setLeaderId(quorum.localId)
                .setLeaderEpoch(quorum.epoch());
    }

    private void maybeSendBeginQuorumEpochToFollowers(long currentTimeMs, LeaderState state) throws IOException {
        for (Integer followerId : state.nonEndorsingFollowers()) {
            maybeSendRequest(currentTimeMs, followerId, this::buildBeginQuorumEpochRequest);
        }
    }

    private VoteRequestData buildVoteRequest() {
        OffsetAndEpoch endOffset = endOffset();
        return new VoteRequestData()
                .setCandidateEpoch(quorum.epoch())
                .setCandidateId(quorum.localId)
                .setLastEpoch(endOffset.epoch)
                .setLastEpochEndOffset(endOffset.offset);
    }

    private void maybeSendVoteRequestToVoters(long currentTimeMs, CandidateState state) throws IOException {
        for (Integer voterId : state.remainingVoters()) {
            maybeSendRequest(currentTimeMs, voterId, this::buildVoteRequest);
        }
    }

    private FetchQuorumRecordsRequestData buildFetchQuorumRecordsRequest() {
        return new FetchQuorumRecordsRequestData()
            .setLeaderEpoch(quorum.epoch())
            .setReplicaId(quorum.localId)
            .setFetchOffset(log.endOffset())
            .setLastFetchedEpoch(log.lastFetchedEpoch());
    }

    private void maybeSendFetchQuorumRecords(long currentTimeMs, int leaderId) throws IOException {
        maybeSendRequest(currentTimeMs, leaderId, this::buildFetchQuorumRecordsRequest);
    }

    private FindQuorumRequestData buildFindQuorumRequest() {
        return new FindQuorumRequestData()
            .setReplicaId(quorum.localId)
            .setHost(advertisedListener.getHostString())
            .setPort(advertisedListener.getPort())
            .setBootTimestamp(bootTimestamp);
    }

    private void maybeSendFindQuorum(long currentTimeMs) throws IOException {
        // Find a ready member of the quorum to send FindLeader to. Any voter can receive the
        // FindLeader, but we only send one request at a time.
        OptionalInt readyNodeIdOpt = connections.findReadyBootstrapServer(currentTimeMs);
        if (readyNodeIdOpt.isPresent()) {
            maybeSendRequest(currentTimeMs, readyNodeIdOpt.getAsInt(), this::buildFindQuorumRequest);
        }
    }

    private void maybeSendRequests(long currentTimeMs) throws IOException {
        if (quorum.isLeader()) {
            LeaderState state = quorum.leaderStateOrThrow();
            maybeSendBeginQuorumEpochToFollowers(currentTimeMs, state);
        } else if (quorum.isCandidate()) {
            CandidateState state = quorum.candidateStateOrThrow();
            maybeSendVoteRequestToVoters(currentTimeMs, state);
        } else {
            FollowerState state = quorum.followerStateOrThrow();
            if (quorum.isObserver() && (!state.hasLeader() || electionTimer.isExpired())) {
                maybeSendFindQuorum(currentTimeMs);
            } else if (state.hasLeader()) {
                int leaderId = state.leaderId();
                maybeSendFetchQuorumRecords(currentTimeMs, leaderId);
            }
        }

        if (connections.hasUnknownVoterEndpoints()) {
            maybeSendFindQuorum(currentTimeMs);
        }
    }

    public boolean isRunning() {
        GracefulShutdown gracefulShutdown = shutdown.get();
        return gracefulShutdown == null || !gracefulShutdown.isFinished();
    }

    private void pollShutdown(GracefulShutdown shutdown) throws IOException {
        // Graceful shutdown allows a leader or candidate to resign its leadership without
        // awaiting expiration of the election timeout. During shutdown, we no longer update
        // quorum state. All we do is check for epoch updates and try to send EndQuorumEpoch request
        // to finish our term. We consider the term finished if we are a follower or if one of
        // the remaining voters bumps the existing epoch.

        shutdown.timer.update();

        if (quorum.isFollower() || quorum.remoteVoters().isEmpty()) {
            // Shutdown immediately if we are a follower or we are the only voter
            shutdown.finished.set(true);
        } else if (!shutdown.isFinished()) {
            long currentTimeMs = shutdown.timer.currentTimeMs();
            maybeSendEndQuorumEpoch(currentTimeMs);

            List<RaftMessage> inboundMessages = channel.receive(shutdown.timer.remainingMs());
            for (RaftMessage message : inboundMessages)
                handleInboundMessage(message, currentTimeMs);
        }
    }

    public void poll() throws IOException {
        GracefulShutdown gracefulShutdown = shutdown.get();
        if (gracefulShutdown != null) {
            pollShutdown(gracefulShutdown);
        } else {
            // This call is a bit annoying, but perhaps justifiable if we need to acquire a lock
            electionTimer.update();

            if (quorum.isVoter() && electionTimer.isExpired()) {
                logger.debug("Become candidate due to election timeout");
                becomeCandidate();
            }

            maybeSendRequests(electionTimer.currentTimeMs());
            maybeSendOrHandleAppendRequest(electionTimer.currentTimeMs());

            // TODO: Receive time needs to take into account backing off operations that still need doing
            List<RaftMessage> inboundMessages = channel.receive(electionTimer.remainingMs());
            electionTimer.update();

            for (RaftMessage message : inboundMessages)
                handleInboundMessage(message, electionTimer.currentTimeMs());
        }
    }

    private void maybeSendOrHandleAppendRequest(long currentTimeMs) {
        PendingAppendRequest unsentAppend = unsentAppends.poll();
        if (unsentAppend == null || unsentAppend.isCancelled())
            return;

        if (unsentAppend.isTimedOut(currentTimeMs)) {
            unsentAppend.fail(new TimeoutException());
        } else if (quorum.isLeader()) {
            LeaderState leaderState = quorum.leaderStateOrThrow();
            int epoch = quorum.epoch();
            OptionalLong baseOffsetOpt = maybeAppendAsLeader(leaderState, unsentAppend.records);
            if (baseOffsetOpt.isPresent()) {
                unsentAppend.complete(new OffsetAndEpoch(baseOffsetOpt.getAsLong(), epoch));
            } else {
                unsentAppend.fail(new InvalidRequestException("Leader refused the append"));
            }
        }
    }

    /**
     * Append a set of records to the log. Successful completion of the future indicates a success of
     * the append, with the uncommitted base offset and epoch.
     *
     * @param records The records to write to the log
     * @return The uncommitted base offset and epoch of the appended records
     */
    @Override
    public CompletableFuture<OffsetAndEpoch> append(Records records) {
        if (shutdown.get() != null)
            throw new IllegalStateException("Cannot append records while we are shutting down");

        CompletableFuture<OffsetAndEpoch> future = new CompletableFuture<>();
        PendingAppendRequest pendingAppendRequest = new PendingAppendRequest(
            records, future, time.milliseconds(), requestTimeoutMs);

        if (!unsentAppends.offer(pendingAppendRequest)) {
            future.completeExceptionally(new KafkaException("Failed to append records since the unsent " +
                "append queue is full"));
        }
        return future;
    }

    /**
     * Read from the local log. This will only return records which have been committed to the quorum.
     * @param offsetAndEpoch The first offset to read from and the previous consumed epoch
     * @return A set of records beginning at the request offset
     */
    private Records readCommitted(OffsetAndEpoch offsetAndEpoch) {
        Optional<OffsetAndEpoch> endOffset = log.endOffsetForEpoch(offsetAndEpoch.epoch);
        if (!endOffset.isPresent() || offsetAndEpoch.offset > endOffset.get().offset) {
            throw new LogTruncationException("The requested offset and epoch " + offsetAndEpoch +
                    " are not in range. The closest offset we found is " + endOffset + ".");
        }

        OptionalLong highWatermark = quorum.highWatermark();
        if (highWatermark.isPresent()) {
            return log.read(offsetAndEpoch.offset, highWatermark);
        } else {
            // We are in the middle of an election or we have not yet discovered the leader
            return MemoryRecords.EMPTY;
        }
    }

    @Override
    public void shutdown(int timeoutMs) {
        // TODO: Safe to access epoch? Need to reset connections to be able to send EndQuorumEpoch? Block until shutdown completes?
        shutdown.set(new GracefulShutdown(timeoutMs, quorum.epoch()));
        channel.wakeup();
    }

    public OptionalLong highWatermark() {
        return quorum.highWatermark();
    }

    private class GracefulShutdown {
        final int epoch;
        final Timer timer;
        final AtomicBoolean finished = new AtomicBoolean(false);

        public GracefulShutdown(int shutdownTimeoutMs, int epoch) {
            this.timer = time.timer(shutdownTimeoutMs);
            this.epoch = epoch;
        }

        public void onEpochUpdate(int epoch) {
            // Shutdown is complete once the epoch has been bumped, which indicates
            // that a new election has been started.

            if (epoch > this.epoch)
                finished.set(true);
        }

        public boolean isFinished() {
            return succeeded() || failed();
        }

        public boolean succeeded() {
            return finished.get();
        }

        public boolean failed() {
            return timer.isExpired();
        }

    }

    private static class PendingAppendRequest {
        private final Records records;
        private final CompletableFuture<OffsetAndEpoch> future;
        private final long createTimeMs;
        private final long requestTimeoutMs;

        private PendingAppendRequest(Records records,
                                     CompletableFuture<OffsetAndEpoch> future,
                                     long createTimeMs,
                                     long requestTimeoutMs) {
            this.records = records;
            this.future = future;
            this.createTimeMs = createTimeMs;
            this.requestTimeoutMs = requestTimeoutMs;
        }

        public void complete(OffsetAndEpoch offsetAndEpoch) {
            future.complete(offsetAndEpoch);
        }

        public void fail(Exception e) {
            future.completeExceptionally(e);
        }

        public boolean isTimedOut(long currentTimeMs) {
            return currentTimeMs > createTimeMs + requestTimeoutMs;
        }

        public boolean isCancelled() {
            return future.isCancelled();
        }
    }

}
