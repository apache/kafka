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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EndQuorumEpochResponse;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.FetchSnapshotResponse;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.RequestManager.ConnectionState;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.BatchMemoryPool;
import org.apache.kafka.raft.internals.BlockingMessageQueue;
import org.apache.kafka.raft.internals.CloseListener;
import org.apache.kafka.raft.internals.FuturePurgatory;
import org.apache.kafka.raft.internals.KafkaRaftMetrics;
import org.apache.kafka.raft.internals.MemoryBatchReader;
import org.apache.kafka.raft.internals.RecordsBatchReader;
import org.apache.kafka.raft.internals.ThresholdPurgatory;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.kafka.raft.RaftUtil.hasValidTopicPartition;

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
 * These are the APIs in this protocol:
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
 * 3) {@link EndQuorumEpochRequestData}: Sent by the leader of an epoch to valid voters in order to
 *    gracefully resign from the current epoch. This causes remaining voters to immediately
 *    begin a new election.
 *
 * 4) {@link FetchRequestData}: This is the same as the usual Fetch API in Kafka, but we piggyback
 *    some additional metadata on responses (i.e. current leader and epoch). Unlike partition replication,
 *    we also piggyback truncation detection on this API rather than through a separate truncation state.
 *
 */
public class KafkaRaftClient<T> implements RaftClient<T> {
    private static final int RETRY_BACKOFF_BASE_MS = 100;
    private static final int FETCH_MAX_WAIT_MS = 1000;
    static final int MAX_BATCH_SIZE = 1024 * 1024;

    private final AtomicReference<GracefulShutdown> shutdown = new AtomicReference<>();
    private final Logger logger;
    private final LogContext logContext;
    private final Time time;
    private final int fetchMaxWaitMs;
    private final OptionalInt nodeId;
    private final NetworkChannel channel;
    private final ReplicatedLog log;
    private final Random random;
    private final FuturePurgatory<Long> appendPurgatory;
    private final FuturePurgatory<Long> fetchPurgatory;
    private final RecordSerde<T> serde;
    private final MemoryPool memoryPool;
    private final RaftMessageQueue messageQueue;
    private final QuorumStateStore quorumStateStore;
    private final Metrics metrics;

    private final List<ListenerContext> listenerContexts = new ArrayList<>();
    private final ConcurrentLinkedQueue<Listener<T>> pendingListeners = new ConcurrentLinkedQueue<>();

    private volatile BatchAccumulator<T> accumulator;
    private RequestManager requestManager;
    private QuorumState quorum;
    private KafkaRaftMetrics kafkaRaftMetrics;
    private RaftConfig raftConfig;

    /**
     * Create a new instance.
     *
     * Note that if the node ID is empty, then the the client will behave as a
     * non-participating observer.
     */
    public KafkaRaftClient(
        RecordSerde<T> serde,
        NetworkChannel channel,
        ReplicatedLog log,
        QuorumStateStore quorumStateStore,
        Time time,
        Metrics metrics,
        ExpirationService expirationService,
        LogContext logContext,
        OptionalInt nodeId
    ) {
        this(serde,
            channel,
            new BlockingMessageQueue(),
            log,
            quorumStateStore,
            new BatchMemoryPool(5, MAX_BATCH_SIZE),
            time,
            metrics,
            expirationService,
            FETCH_MAX_WAIT_MS,
            nodeId,
            logContext,
            new Random());
    }

    KafkaRaftClient(
        RecordSerde<T> serde,
        NetworkChannel channel,
        RaftMessageQueue messageQueue,
        ReplicatedLog log,
        QuorumStateStore quorumStateStore,
        MemoryPool memoryPool,
        Time time,
        Metrics metrics,
        ExpirationService expirationService,
        int fetchMaxWaitMs,
        OptionalInt nodeId,
        LogContext logContext,
        Random random
    ) {
        this.serde = serde;
        this.channel = channel;
        this.messageQueue = messageQueue;
        this.log = log;
        this.quorumStateStore = quorumStateStore;
        this.memoryPool = memoryPool;
        this.fetchPurgatory = new ThresholdPurgatory<>(expirationService);
        this.appendPurgatory = new ThresholdPurgatory<>(expirationService);
        this.time = time;
        this.nodeId = nodeId;
        this.metrics = metrics;
        this.fetchMaxWaitMs = fetchMaxWaitMs;
        this.logContext = logContext;
        this.logger = logContext.logger(KafkaRaftClient.class);
        this.random = random;
    }

    private void updateFollowerHighWatermark(
        FollowerState state,
        OptionalLong highWatermarkOpt
    ) {
        highWatermarkOpt.ifPresent(highWatermark -> {
            long newHighWatermark = Math.min(endOffset().offset, highWatermark);
            if (state.updateHighWatermark(OptionalLong.of(newHighWatermark))) {
                logger.debug("Follower high watermark updated to {}", newHighWatermark);
                log.updateHighWatermark(new LogOffsetMetadata(newHighWatermark));
                maybeFireHandleCommit(newHighWatermark);
            }
        });
    }

    private void updateLeaderEndOffsetAndTimestamp(
        LeaderState state,
        long currentTimeMs
    ) {
        final LogOffsetMetadata endOffsetMetadata = log.endOffset();

        if (state.updateLocalState(currentTimeMs, endOffsetMetadata)) {
            onUpdateLeaderHighWatermark(state, currentTimeMs);
        }

        fetchPurgatory.maybeComplete(endOffsetMetadata.offset, currentTimeMs);
    }

    private void onUpdateLeaderHighWatermark(
        LeaderState state,
        long currentTimeMs
    ) {
        state.highWatermark().ifPresent(highWatermark -> {
            logger.debug("Leader high watermark updated to {}", highWatermark);
            log.updateHighWatermark(highWatermark);

            // After updating the high watermark, we first clear the append
            // purgatory so that we have an opportunity to route the pending
            // records still held in memory directly to the listener
            appendPurgatory.maybeComplete(highWatermark.offset, currentTimeMs);

            // It is also possible that the high watermark is being updated
            // for the first time following the leader election, so we need
            // to give lagging listeners an opportunity to catch up as well
            maybeFireHandleCommit(highWatermark.offset);
        });
    }

    private void maybeFireHandleCommit(long highWatermark) {
        maybeFireHandleCommit(listenerContexts, highWatermark);
    }

    private void maybeFireHandleCommit(List<ListenerContext> listenerContexts, long highWatermark) {
        for (ListenerContext listenerContext : listenerContexts) {
            OptionalLong nextExpectedOffsetOpt = listenerContext.nextExpectedOffset();
            if (!nextExpectedOffsetOpt.isPresent()) {
                continue;
            }

            long nextExpectedOffset = nextExpectedOffsetOpt.getAsLong();
            if (nextExpectedOffset < highWatermark) {
                LogFetchInfo readInfo = log.read(nextExpectedOffset, Isolation.COMMITTED);
                listenerContext.fireHandleCommit(nextExpectedOffset, readInfo.records);
            }
        }
    }

    private void maybeFireHandleCommit(long baseOffset, int epoch, List<T> records) {
        for (ListenerContext listenerContext : listenerContexts) {
            OptionalLong nextExpectedOffsetOpt = listenerContext.nextExpectedOffset();
            if (!nextExpectedOffsetOpt.isPresent()) {
                continue;
            }

            long nextExpectedOffset = nextExpectedOffsetOpt.getAsLong();
            if (nextExpectedOffset == baseOffset) {
                listenerContext.fireHandleCommit(baseOffset, epoch, records);
            }
        }
    }

    private void maybeFireHandleClaim(LeaderState state) {
        int leaderEpoch = state.epoch();
        long epochStartOffset = state.epochStartOffset();
        for (ListenerContext listenerContext : listenerContexts) {
            listenerContext.maybeFireHandleClaim(leaderEpoch, epochStartOffset);
        }
    }

    private void fireHandleResign() {
        for (ListenerContext listenerContext : listenerContexts) {
            listenerContext.fireHandleResign();
        }
    }

    @Override
    public void initialize(RaftConfig raftConfig) throws IOException {
        this.raftConfig = raftConfig;
        Set<Integer> quorumVoterIds = raftConfig.quorumVoterIds();
        this.requestManager = new RequestManager(quorumVoterIds, raftConfig.retryBackoffMs(),
                raftConfig.requestTimeoutMs(), random);

        Map<Integer, InetSocketAddress> voterAddresses = raftConfig.quorumVoterConnections();
        for (Map.Entry<Integer, InetSocketAddress> voterAddressEntry : voterAddresses.entrySet()) {
            channel.updateEndpoint(voterAddressEntry.getKey(), voterAddressEntry.getValue());
        }

        QuorumState quorumState = new QuorumState(
                nodeId,
                quorumVoterIds,
                raftConfig.electionTimeoutMs(),
                raftConfig.fetchTimeoutMs(),
                quorumStateStore,
                time,
                logContext,
                random);
        quorumState.initialize(new OffsetAndEpoch(log.endOffset().offset, log.lastFetchedEpoch()));
        this.quorum = quorumState;
        this.kafkaRaftMetrics = new KafkaRaftMetrics(metrics, "raft", quorum);
        kafkaRaftMetrics.updateNumUnknownVoterConnections(quorum.remoteVoters().size());

        long currentTimeMs = time.milliseconds();
        if (quorum.isLeader()) {
            throw new IllegalStateException("Voter cannot initialize as a Leader");
        } else if (quorum.isCandidate()) {
            onBecomeCandidate(currentTimeMs);
        } else if (quorum.isFollower()) {
            onBecomeFollower(currentTimeMs);
        }

        // When there is only a single voter, become candidate immediately
        if (quorum.isVoter()
            && quorum.remoteVoters().isEmpty()
            && !quorum.isLeader()
            && !quorum.isCandidate()) {
            transitionToCandidate(currentTimeMs);
        }
    }

    @Override
    public void register(Listener<T> listener) {
        pendingListeners.add(listener);
        wakeup();
    }

    private OffsetAndEpoch endOffset() {
        return new OffsetAndEpoch(log.endOffset().offset, log.lastFetchedEpoch());
    }

    private void resetConnections() {
        requestManager.resetAll();
    }

    private void onBecomeLeader(long currentTimeMs) {
        LeaderState state = quorum.leaderStateOrThrow();

        log.initializeLeaderEpoch(quorum.epoch());

        // The high watermark can only be advanced once we have written a record
        // from the new leader's epoch. Hence we write a control message immediately
        // to ensure there is no delay committing pending data.
        appendLeaderChangeMessage(state, currentTimeMs);
        updateLeaderEndOffsetAndTimestamp(state, currentTimeMs);

        resetConnections();

        kafkaRaftMetrics.maybeUpdateElectionLatency(currentTimeMs);

        accumulator = new BatchAccumulator<>(
            quorum.epoch(),
            log.endOffset().offset,
            raftConfig.appendLingerMs(),
            MAX_BATCH_SIZE,
            memoryPool,
            time,
            CompressionType.NONE,
            serde
        );
    }

    private static List<Voter> convertToVoters(Set<Integer> voterIds) {
        return voterIds.stream()
            .map(follower -> new Voter().setVoterId(follower))
            .collect(Collectors.toList());
    }

    private void appendLeaderChangeMessage(LeaderState state, long currentTimeMs) {
        List<Voter> voters = convertToVoters(state.followers());
        List<Voter> grantingVoters = convertToVoters(state.grantingVoters());

        // Adding the leader to the voters as any voter always votes for itself.
        voters.add(new Voter().setVoterId(state.election().leaderId()));

        LeaderChangeMessage leaderChangeMessage = new LeaderChangeMessage()
            .setLeaderId(state.election().leaderId())
            .setVoters(voters)
            .setGrantingVoters(grantingVoters);

        MemoryRecords records = MemoryRecords.withLeaderChangeMessage(
            currentTimeMs, quorum.epoch(), leaderChangeMessage);

        appendAsLeader(records);
        flushLeaderLog(state, currentTimeMs);
    }

    private void flushLeaderLog(LeaderState state, long currentTimeMs) {
        // We update the end offset before flushing so that parked fetches can return sooner
        updateLeaderEndOffsetAndTimestamp(state, currentTimeMs);
        log.flush();
    }

    private boolean maybeTransitionToLeader(CandidateState state, long currentTimeMs) throws IOException {
        if (state.isVoteGranted()) {
            long endOffset = log.endOffset().offset;
            quorum.transitionToLeader(endOffset);
            onBecomeLeader(currentTimeMs);
            return true;
        } else {
            return false;
        }
    }

    private void onBecomeCandidate(long currentTimeMs) throws IOException {
        CandidateState state = quorum.candidateStateOrThrow();
        if (!maybeTransitionToLeader(state, currentTimeMs)) {
            resetConnections();
            kafkaRaftMetrics.updateElectionStartMs(currentTimeMs);
        }
    }

    private void maybeResignLeadership() {
        if (quorum.isLeader()) {
            fireHandleResign();
        }

        if (accumulator != null) {
            accumulator.close();
            accumulator = null;
        }
    }

    private void transitionToCandidate(long currentTimeMs) throws IOException {
        maybeResignLeadership();
        quorum.transitionToCandidate();
        onBecomeCandidate(currentTimeMs);
    }

    private void transitionToUnattached(int epoch) throws IOException {
        maybeResignLeadership();
        quorum.transitionToUnattached(epoch);
        resetConnections();
    }

    private void transitionToResigned(List<Integer> preferredSuccessors) {
        fetchPurgatory.completeAllExceptionally(Errors.BROKER_NOT_AVAILABLE.exception("The broker is shutting down"));
        quorum.transitionToResigned(preferredSuccessors);
        resetConnections();
    }

    private void transitionToVoted(int candidateId, int epoch) throws IOException {
        maybeResignLeadership();
        quorum.transitionToVoted(epoch, candidateId);
        resetConnections();
    }

    private void onBecomeFollower(long currentTimeMs) {
        kafkaRaftMetrics.maybeUpdateElectionLatency(currentTimeMs);

        resetConnections();

        // After becoming a follower, we need to complete all pending fetches so that
        // they can be resent to the leader without waiting for their expiration
        fetchPurgatory.completeAllExceptionally(new NotLeaderOrFollowerException(
            "Cannot process the fetch request because the node is no longer the leader."));

        // Clearing the append purgatory should complete all future exceptionally since this node is no longer the leader
        appendPurgatory.completeAllExceptionally(new NotLeaderOrFollowerException(
            "Failed to receive sufficient acknowledgments for this append before leader change."));
    }

    private void transitionToFollower(
        int epoch,
        int leaderId,
        long currentTimeMs
    ) throws IOException {
        maybeResignLeadership();
        quorum.transitionToFollower(epoch, leaderId);
        onBecomeFollower(currentTimeMs);
    }

    private VoteResponseData buildVoteResponse(Errors partitionLevelError, boolean voteGranted) {
        return VoteResponse.singletonResponse(
            Errors.NONE,
            log.topicPartition(),
            partitionLevelError,
            quorum.epoch(),
            quorum.leaderIdOrSentinel(),
            voteGranted);
    }

    /**
     * Handle a Vote request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#INVALID_REQUEST} if the last epoch or offset are invalid
     */
    private VoteResponseData handleVoteRequest(
        RaftRequest.Inbound requestMetadata
    ) throws IOException {
        VoteRequestData request = (VoteRequestData) requestMetadata.data;

        if (!hasValidTopicPartition(request, log.topicPartition())) {
            // Until we support multi-raft, we treat topic partition mismatches as invalid requests
            return new VoteResponseData().setErrorCode(Errors.INVALID_REQUEST.code());
        }

        VoteRequestData.PartitionData partitionRequest =
            request.topics().get(0).partitions().get(0);

        int candidateId = partitionRequest.candidateId();
        int candidateEpoch = partitionRequest.candidateEpoch();

        int lastEpoch = partitionRequest.lastOffsetEpoch();
        long lastEpochEndOffset = partitionRequest.lastOffset();
        if (lastEpochEndOffset < 0 || lastEpoch < 0 || lastEpoch >= candidateEpoch) {
            return buildVoteResponse(Errors.INVALID_REQUEST, false);
        }

        Optional<Errors> errorOpt = validateVoterOnlyRequest(candidateId, candidateEpoch);
        if (errorOpt.isPresent()) {
            return buildVoteResponse(errorOpt.get(), false);
        }

        if (candidateEpoch > quorum.epoch()) {
            transitionToUnattached(candidateEpoch);
        }

        final boolean voteGranted;
        if (quorum.isLeader()) {
            logger.debug("Rejecting vote request {} with epoch {} since we are already leader on that epoch",
                    request, candidateEpoch);
            voteGranted = false;
        } else if (quorum.isCandidate()) {
            logger.debug("Rejecting vote request {} with epoch {} since we are already candidate on that epoch",
                    request, candidateEpoch);
            voteGranted = false;
        } else if (quorum.isResigned()) {
            logger.debug("Rejecting vote request {} with epoch {} since we have resigned as candidate/leader in this epoch",
                request, candidateEpoch);
            voteGranted = false;
        } else if (quorum.isFollower()) {
            FollowerState state = quorum.followerStateOrThrow();
            logger.debug("Rejecting vote request {} with epoch {} since we already have a leader {} on that epoch",
                request, candidateEpoch, state.leaderId());
            voteGranted = false;
        } else if (quorum.isVoted()) {
            VotedState state = quorum.votedStateOrThrow();
            voteGranted = state.votedId() == candidateId;

            if (!voteGranted) {
                logger.debug("Rejecting vote request {} with epoch {} since we already have voted for " +
                    "another candidate {} on that epoch", request, candidateEpoch, state.votedId());
            }
        } else if (quorum.isUnattached()) {
            OffsetAndEpoch lastEpochEndOffsetAndEpoch = new OffsetAndEpoch(lastEpochEndOffset, lastEpoch);
            voteGranted = lastEpochEndOffsetAndEpoch.compareTo(endOffset()) >= 0;

            if (voteGranted) {
                transitionToVoted(candidateId, candidateEpoch);
            }
        } else {
            throw new IllegalStateException("Unexpected quorum state " + quorum);
        }

        logger.info("Vote request {} is {}", request, voteGranted ? "granted" : "rejected");
        return buildVoteResponse(Errors.NONE, voteGranted);
    }

    private boolean handleVoteResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) throws IOException {
        int remoteNodeId = responseMetadata.sourceId();
        VoteResponseData response = (VoteResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (!hasValidTopicPartition(response, log.topicPartition())) {
            return false;
        }

        VoteResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        Errors error = Errors.forCode(partitionResponse.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(partitionResponse.leaderId());
        int responseEpoch = partitionResponse.leaderEpoch();

        Optional<Boolean> handled = maybeHandleCommonResponse(
            error, responseLeaderId, responseEpoch, currentTimeMs);
        if (handled.isPresent()) {
            return handled.get();
        } else if (error == Errors.NONE) {
            if (quorum.isLeader()) {
                logger.debug("Ignoring vote response {} since we already became leader for epoch {}",
                    partitionResponse, quorum.epoch());
            } else if (quorum.isCandidate()) {
                CandidateState state = quorum.candidateStateOrThrow();
                if (partitionResponse.voteGranted()) {
                    state.recordGrantedVote(remoteNodeId);
                    maybeTransitionToLeader(state, currentTimeMs);
                } else {
                    state.recordRejectedVote(remoteNodeId);

                    // If our vote is rejected, we go immediately to the random backoff. This
                    // ensures that we are not stuck waiting for the election timeout when the
                    // vote has become gridlocked.
                    if (state.isVoteRejected() && !state.isBackingOff()) {
                        logger.info("Insufficient remaining votes to become leader (rejected by {}). " +
                            "We will backoff before retrying election again", state.rejectingVoters());

                        state.startBackingOff(
                            currentTimeMs,
                            binaryExponentialElectionBackoffMs(state.retries())
                        );
                    }

                }
            } else {
                logger.debug("Ignoring vote response {} since we are no longer a candidate in epoch {}",
                    partitionResponse, quorum.epoch());
            }
            return true;
        } else {
            return handleUnexpectedError(error, responseMetadata);
        }
    }

    private int binaryExponentialElectionBackoffMs(int retries) {
        if (retries <= 0) {
            throw new IllegalArgumentException("Retries " + retries + " should be larger than zero");
        }
        // upper limit exponential co-efficients at 20 to avoid overflow
        return Math.min(RETRY_BACKOFF_BASE_MS * random.nextInt(2 << Math.min(20, retries - 1)),
                raftConfig.electionBackoffMaxMs());
    }

    private int strictExponentialElectionBackoffMs(int positionInSuccessors, int totalNumSuccessors) {
        if (positionInSuccessors <= 0 || positionInSuccessors >= totalNumSuccessors) {
            throw new IllegalArgumentException("Position " + positionInSuccessors + " should be larger than zero" +
                    " and smaller than total number of successors " + totalNumSuccessors);
        }

        int retryBackOffBaseMs = raftConfig.electionBackoffMaxMs() >> (totalNumSuccessors - 1);
        return Math.min(raftConfig.electionBackoffMaxMs(), retryBackOffBaseMs << (positionInSuccessors - 1));
    }

    private BeginQuorumEpochResponseData buildBeginQuorumEpochResponse(Errors partitionLevelError) {
        return BeginQuorumEpochResponse.singletonResponse(
            Errors.NONE,
            log.topicPartition(),
            partitionLevelError,
            quorum.epoch(),
            quorum.leaderIdOrSentinel());
    }

    /**
     * Handle a BeginEpoch request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     */
    private BeginQuorumEpochResponseData handleBeginQuorumEpochRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) throws IOException {
        BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) requestMetadata.data;

        if (!hasValidTopicPartition(request, log.topicPartition())) {
            // Until we support multi-raft, we treat topic partition mismatches as invalid requests
            return new BeginQuorumEpochResponseData().setErrorCode(Errors.INVALID_REQUEST.code());
        }

        BeginQuorumEpochRequestData.PartitionData partitionRequest =
            request.topics().get(0).partitions().get(0);

        int requestLeaderId = partitionRequest.leaderId();
        int requestEpoch = partitionRequest.leaderEpoch();

        Optional<Errors> errorOpt = validateVoterOnlyRequest(requestLeaderId, requestEpoch);
        if (errorOpt.isPresent()) {
            return buildBeginQuorumEpochResponse(errorOpt.get());
        }

        maybeTransition(OptionalInt.of(requestLeaderId), requestEpoch, currentTimeMs);
        return buildBeginQuorumEpochResponse(Errors.NONE);
    }

    private boolean handleBeginQuorumEpochResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) throws IOException {
        int remoteNodeId = responseMetadata.sourceId();
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (!hasValidTopicPartition(response, log.topicPartition())) {
            return false;
        }

        BeginQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        Errors partitionError = Errors.forCode(partitionResponse.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(partitionResponse.leaderId());
        int responseEpoch = partitionResponse.leaderEpoch();

        Optional<Boolean> handled = maybeHandleCommonResponse(
            partitionError, responseLeaderId, responseEpoch, currentTimeMs);
        if (handled.isPresent()) {
            return handled.get();
        } else if (partitionError == Errors.NONE) {
            if (quorum.isLeader()) {
                LeaderState state = quorum.leaderStateOrThrow();
                state.addAcknowledgementFrom(remoteNodeId);
            } else {
                logger.debug("Ignoring BeginQuorumEpoch response {} since " +
                    "this node is not the leader anymore", response);
            }
            return true;
        } else {
            return handleUnexpectedError(partitionError, responseMetadata);
        }
    }

    private EndQuorumEpochResponseData buildEndQuorumEpochResponse(Errors partitionLevelError) {
        return EndQuorumEpochResponse.singletonResponse(
            Errors.NONE,
            log.topicPartition(),
            partitionLevelError,
            quorum.epoch(),
            quorum.leaderIdOrSentinel());
    }

    /**
     * Handle an EndEpoch request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     */
    private EndQuorumEpochResponseData handleEndQuorumEpochRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) throws IOException {
        EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) requestMetadata.data;

        if (!hasValidTopicPartition(request, log.topicPartition())) {
            // Until we support multi-raft, we treat topic partition mismatches as invalid requests
            return new EndQuorumEpochResponseData().setErrorCode(Errors.INVALID_REQUEST.code());
        }

        EndQuorumEpochRequestData.PartitionData partitionRequest =
            request.topics().get(0).partitions().get(0);

        int requestEpoch = partitionRequest.leaderEpoch();
        int requestLeaderId = partitionRequest.leaderId();

        Optional<Errors> errorOpt = validateVoterOnlyRequest(requestLeaderId, requestEpoch);
        if (errorOpt.isPresent()) {
            return buildEndQuorumEpochResponse(errorOpt.get());
        }
        maybeTransition(OptionalInt.of(requestLeaderId), requestEpoch, currentTimeMs);

        if (quorum.isFollower()) {
            FollowerState state = quorum.followerStateOrThrow();
            if (state.leaderId() == requestLeaderId) {
                List<Integer> preferredSuccessors = partitionRequest.preferredSuccessors();
                long electionBackoffMs = endEpochElectionBackoff(preferredSuccessors);
                logger.debug("Overriding follower fetch timeout to {} after receiving " +
                    "EndQuorumEpoch request from leader {} in epoch {}", electionBackoffMs,
                    requestLeaderId, requestEpoch);
                state.overrideFetchTimeout(currentTimeMs, electionBackoffMs);
            }
        }
        return buildEndQuorumEpochResponse(Errors.NONE);
    }

    private long endEpochElectionBackoff(List<Integer> preferredSuccessors) {
        // Based on the priority inside the preferred successors, choose the corresponding delayed
        // election backoff time based on strict exponential mechanism so that the most up-to-date
        // voter has a higher chance to be elected. If the node's priority is highest, become
        // candidate immediately instead of waiting for next poll.
        int position = preferredSuccessors.indexOf(quorum.localIdOrThrow());
        if (position <= 0) {
            return 0;
        } else {
            return strictExponentialElectionBackoffMs(position, preferredSuccessors.size());
        }
    }

    private boolean handleEndQuorumEpochResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) throws IOException {
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (!hasValidTopicPartition(response, log.topicPartition())) {
            return false;
        }

        EndQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        Errors partitionError = Errors.forCode(partitionResponse.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(partitionResponse.leaderId());
        int responseEpoch = partitionResponse.leaderEpoch();

        Optional<Boolean> handled = maybeHandleCommonResponse(
            partitionError, responseLeaderId, responseEpoch, currentTimeMs);
        if (handled.isPresent()) {
            return handled.get();
        } else if (partitionError == Errors.NONE) {
            ResignedState resignedState = quorum.resignedStateOrThrow();
            resignedState.acknowledgeResignation(responseMetadata.sourceId());
            return true;
        } else {
            return handleUnexpectedError(partitionError, responseMetadata);
        }
    }

    private FetchResponseData buildFetchResponse(
        Errors error,
        Records records,
        Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
        Optional<LogOffsetMetadata> highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(log.topicPartition(), Errors.NONE, partitionData -> {
            partitionData
                .setRecordSet(records)
                .setErrorCode(error.code())
                .setLogStartOffset(log.startOffset())
                .setHighWatermark(highWatermark
                    .map(offsetMetadata -> offsetMetadata.offset)
                    .orElse(-1L));

            partitionData.currentLeader()
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrSentinel());

            divergingEpoch.ifPresent(partitionData::setDivergingEpoch);
        });
    }

    private FetchResponseData buildEmptyFetchResponse(
        Errors error,
        Optional<LogOffsetMetadata> highWatermark
    ) {
        return buildFetchResponse(error, MemoryRecords.EMPTY, Optional.empty(), highWatermark);
    }

    /**
     * Handle a Fetch request. The fetch offset and last fetched epoch are always
     * validated against the current log. In the case that they do not match, the response will
     * indicate the diverging offset/epoch. A follower is expected to truncate its log in this
     * case and resend the fetch.
     *
     * This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     * - {@link Errors#INVALID_REQUEST} if the request epoch is larger than the leader's current epoch
     *     or if either the fetch offset or the last fetched epoch is invalid
     */
    private CompletableFuture<FetchResponseData> handleFetchRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) {
        FetchRequestData request = (FetchRequestData) requestMetadata.data;

        if (!hasValidTopicPartition(request, log.topicPartition())) {
            // Until we support multi-raft, we treat topic partition mismatches as invalid requests
            return completedFuture(new FetchResponseData().setErrorCode(Errors.INVALID_REQUEST.code()));
        }

        FetchRequestData.FetchPartition fetchPartition = request.topics().get(0).partitions().get(0);
        if (request.maxWaitMs() < 0
            || fetchPartition.fetchOffset() < 0
            || fetchPartition.lastFetchedEpoch() < 0
            || fetchPartition.lastFetchedEpoch() > fetchPartition.currentLeaderEpoch()) {
            return completedFuture(buildEmptyFetchResponse(
                Errors.INVALID_REQUEST, Optional.empty()));
        }

        FetchResponseData response = tryCompleteFetchRequest(request.replicaId(), fetchPartition, currentTimeMs);
        FetchResponseData.FetchablePartitionResponse partitionResponse =
            response.responses().get(0).partitionResponses().get(0);

        if (partitionResponse.errorCode() != Errors.NONE.code()
            || partitionResponse.recordSet().sizeInBytes() > 0
            || request.maxWaitMs() == 0) {
            return completedFuture(response);
        }

        CompletableFuture<Long> future = fetchPurgatory.await(
            fetchPartition.fetchOffset(),
            request.maxWaitMs());

        return future.handle((completionTimeMs, exception) -> {
            if (exception != null) {
                Throwable cause = exception instanceof ExecutionException ?
                    exception.getCause() : exception;

                // If the fetch timed out in purgatory, it means no new data is available,
                // and we will complete the fetch successfully. Otherwise, if there was
                // any other error, we need to return it.
                Errors error = Errors.forException(cause);
                if (error != Errors.REQUEST_TIMED_OUT) {
                    logger.debug("Failed to handle fetch from {} at {} due to {}",
                        request.replicaId(), fetchPartition.fetchOffset(), error);
                    return buildEmptyFetchResponse(error, Optional.empty());
                }
            }

            // FIXME: `completionTimeMs`, which can be null
            logger.trace("Completing delayed fetch from {} starting at offset {} at {}",
                request.replicaId(), fetchPartition.fetchOffset(), completionTimeMs);

            try {
                return tryCompleteFetchRequest(request.replicaId(), fetchPartition, time.milliseconds());
            } catch (Exception e) {
                logger.error("Caught unexpected error in fetch completion of request {}", request, e);
                return buildEmptyFetchResponse(Errors.UNKNOWN_SERVER_ERROR, Optional.empty());
            }
        });
    }

    private FetchResponseData tryCompleteFetchRequest(
        int replicaId,
        FetchRequestData.FetchPartition request,
        long currentTimeMs
    ) {
        Optional<Errors> errorOpt = validateLeaderOnlyRequest(request.currentLeaderEpoch());
        if (errorOpt.isPresent()) {
            return buildEmptyFetchResponse(errorOpt.get(), Optional.empty());
        }

        long fetchOffset = request.fetchOffset();
        int lastFetchedEpoch = request.lastFetchedEpoch();
        LeaderState state = quorum.leaderStateOrThrow();
        Optional<OffsetAndEpoch> divergingEpochOpt = validateFetchOffsetAndEpoch(fetchOffset, lastFetchedEpoch);

        if (divergingEpochOpt.isPresent()) {
            Optional<FetchResponseData.EpochEndOffset> divergingEpoch =
                divergingEpochOpt.map(offsetAndEpoch -> new FetchResponseData.EpochEndOffset()
                    .setEpoch(offsetAndEpoch.epoch)
                    .setEndOffset(offsetAndEpoch.offset));
            return buildFetchResponse(Errors.NONE, MemoryRecords.EMPTY, divergingEpoch, state.highWatermark());
        } else {
            LogFetchInfo info = log.read(fetchOffset, Isolation.UNCOMMITTED);

            if (state.updateReplicaState(replicaId, currentTimeMs, info.startOffsetMetadata)) {
                onUpdateLeaderHighWatermark(state, currentTimeMs);
            }

            return buildFetchResponse(Errors.NONE, info.records, Optional.empty(), state.highWatermark());
        }
    }

    /**
     * Check whether a fetch offset and epoch is valid. Return the diverging epoch, which
     * is the largest epoch such that subsequent records are known to diverge.
     */
    private Optional<OffsetAndEpoch> validateFetchOffsetAndEpoch(long fetchOffset, int lastFetchedEpoch) {
        if (fetchOffset == 0 && lastFetchedEpoch == 0) {
            return Optional.empty();
        }

        OffsetAndEpoch endOffsetAndEpoch = log.endOffsetForEpoch(lastFetchedEpoch)
            .orElse(new OffsetAndEpoch(-1L, -1));
        if (endOffsetAndEpoch.epoch != lastFetchedEpoch || endOffsetAndEpoch.offset < fetchOffset) {
            return Optional.of(endOffsetAndEpoch);
        } else {
            return Optional.empty();
        }
    }

    private static OptionalInt optionalLeaderId(int leaderIdOrNil) {
        if (leaderIdOrNil < 0)
            return OptionalInt.empty();
        return OptionalInt.of(leaderIdOrNil);
    }

    private boolean handleFetchResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) throws IOException {
        FetchResponseData response = (FetchResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (!RaftUtil.hasValidTopicPartition(response, log.topicPartition())) {
            return false;
        }

        FetchResponseData.FetchablePartitionResponse partitionResponse =
            response.responses().get(0).partitionResponses().get(0);

        FetchResponseData.LeaderIdAndEpoch currentLeaderIdAndEpoch = partitionResponse.currentLeader();
        OptionalInt responseLeaderId = optionalLeaderId(currentLeaderIdAndEpoch.leaderId());
        int responseEpoch = currentLeaderIdAndEpoch.leaderEpoch();
        Errors error = Errors.forCode(partitionResponse.errorCode());

        Optional<Boolean> handled = maybeHandleCommonResponse(
            error, responseLeaderId, responseEpoch, currentTimeMs);
        if (handled.isPresent()) {
            return handled.get();
        }

        FollowerState state = quorum.followerStateOrThrow();
        if (error == Errors.NONE) {
            FetchResponseData.EpochEndOffset divergingEpoch = partitionResponse.divergingEpoch();
            if (divergingEpoch.epoch() >= 0) {
                // The leader is asking us to truncate before continuing
                OffsetAndEpoch divergingOffsetAndEpoch = new OffsetAndEpoch(
                    divergingEpoch.endOffset(), divergingEpoch.epoch());

                state.highWatermark().ifPresent(highWatermark -> {
                    if (divergingOffsetAndEpoch.offset < highWatermark.offset) {
                        throw new KafkaException("The leader requested truncation to offset " +
                            divergingOffsetAndEpoch.offset + ", which is below the current high watermark" +
                            " " + highWatermark);
                    }
                });

                log.truncateToEndOffset(divergingOffsetAndEpoch).ifPresent(truncationOffset -> {
                    logger.info("Truncated to offset {} from Fetch response from leader {}",
                        truncationOffset, quorum.leaderIdOrSentinel());
                });
            } else if (partitionResponse.snapshotId().epoch() >= 0 ||
                       partitionResponse.snapshotId().endOffset() >= 0) {
                // The leader is asking us to fetch a snapshot

                if (partitionResponse.snapshotId().epoch() < 0) {
                    logger.error(
                        "The leader sent a snapshot id with a valid end offset {} but with an invalid epoch {}",
                        partitionResponse.snapshotId().endOffset(),
                        partitionResponse.snapshotId().epoch()
                    );
                    return false;
                } else if (partitionResponse.snapshotId().endOffset() < 0) {
                    logger.error(
                        "The leader sent a snapshot id with a valid epoch {} but with an invalid end offset {}",
                        partitionResponse.snapshotId().epoch(),
                        partitionResponse.snapshotId().endOffset()
                    );
                    return false;
                } else {
                    OffsetAndEpoch snapshotId = new OffsetAndEpoch(
                        partitionResponse.snapshotId().endOffset(),
                        partitionResponse.snapshotId().epoch()
                    );

                    state.setFetchingSnapshot(Optional.of(log.createSnapshot(snapshotId)));
                }
            } else {
                Records records = (Records) partitionResponse.recordSet();
                if (records.sizeInBytes() > 0) {
                    appendAsFollower(records);
                }
                OptionalLong highWatermark = partitionResponse.highWatermark() < 0 ?
                    OptionalLong.empty() : OptionalLong.of(partitionResponse.highWatermark());
                updateFollowerHighWatermark(state, highWatermark);
            }

            state.resetFetchTimeout(currentTimeMs);
            return true;
        } else {
            return handleUnexpectedError(error, responseMetadata);
        }
    }

    private void appendAsFollower(
        Records records
    ) {
        LogAppendInfo info = log.appendAsFollower(records);
        log.flush();

        OffsetAndEpoch endOffset = endOffset();
        kafkaRaftMetrics.updateFetchedRecords(info.lastOffset - info.firstOffset + 1);
        kafkaRaftMetrics.updateLogEnd(endOffset);
        logger.trace("Follower end offset updated to {} after append", endOffset);
    }

    private LogAppendInfo appendAsLeader(
        Records records
    ) {
        LogAppendInfo info = log.appendAsLeader(records, quorum.epoch());
        OffsetAndEpoch endOffset = endOffset();
        kafkaRaftMetrics.updateAppendRecords(info.lastOffset - info.firstOffset + 1);
        kafkaRaftMetrics.updateLogEnd(endOffset);
        logger.trace("Leader appended records at base offset {}, new end offset is {}", info.firstOffset, endOffset);
        return info;
    }

    private DescribeQuorumResponseData handleDescribeQuorumRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) {
        DescribeQuorumRequestData describeQuorumRequestData = (DescribeQuorumRequestData) requestMetadata.data;
        if (!hasValidTopicPartition(describeQuorumRequestData, log.topicPartition())) {
            return DescribeQuorumRequest.getPartitionLevelErrorResponse(
                describeQuorumRequestData, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        }

        if (!quorum.isLeader()) {
            return DescribeQuorumRequest.getTopLevelErrorResponse(Errors.INVALID_REQUEST);
        }

        LeaderState leaderState = quorum.leaderStateOrThrow();
        return DescribeQuorumResponse.singletonResponse(log.topicPartition(),
            leaderState.localId(),
            leaderState.epoch(),
            leaderState.highWatermark().isPresent() ? leaderState.highWatermark().get().offset : -1,
            convertToReplicaStates(leaderState.getVoterEndOffsets()),
            convertToReplicaStates(leaderState.getObserverStates(currentTimeMs))
        );
    }

    private FetchSnapshotResponseData handleFetchSnapshotRequest(
        RaftRequest.Inbound requestMetadata
    ) throws IOException {
        FetchSnapshotRequestData data = (FetchSnapshotRequestData) requestMetadata.data;

        if (data.topics().size() != 1 && data.topics().get(0).partitions().size() != 1) {
            return FetchSnapshotResponse.withTopLevelError(Errors.INVALID_REQUEST);
        }

        Optional<FetchSnapshotRequestData.PartitionSnapshot> partitionSnapshotOpt = FetchSnapshotRequest
            .forTopicPartition(data, log.topicPartition());
        if (!partitionSnapshotOpt.isPresent()) {
            // The Raft client assumes that there is only one topic partition.
            TopicPartition unknownTopicPartition = new TopicPartition(
                data.topics().get(0).name(),
                data.topics().get(0).partitions().get(0).partition()
            );

            return FetchSnapshotResponse.singleton(
                unknownTopicPartition,
                responsePartitionSnapshot -> {
                    return responsePartitionSnapshot
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                }
            );
        }

        FetchSnapshotRequestData.PartitionSnapshot partitionSnapshot = partitionSnapshotOpt.get();
        Optional<Errors> leaderValidation = validateLeaderOnlyRequest(
                partitionSnapshot.currentLeaderEpoch()
        );
        if (leaderValidation.isPresent()) {
            return FetchSnapshotResponse.singleton(
                log.topicPartition(),
                responsePartitionSnapshot -> {
                    return addQuorumLeader(responsePartitionSnapshot)
                        .setErrorCode(leaderValidation.get().code());
                }
            );
        }

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(
            partitionSnapshot.snapshotId().endOffset(),
            partitionSnapshot.snapshotId().epoch()
        );
        Optional<RawSnapshotReader> snapshotOpt = log.readSnapshot(snapshotId);
        if (!snapshotOpt.isPresent()) {
            return FetchSnapshotResponse.singleton(
                log.topicPartition(),
                responsePartitionSnapshot -> {
                    return addQuorumLeader(responsePartitionSnapshot)
                        .setErrorCode(Errors.SNAPSHOT_NOT_FOUND.code());
                }
            );
        }

        try (RawSnapshotReader snapshot = snapshotOpt.get()) {
            if (partitionSnapshot.position() < 0 || partitionSnapshot.position() >= snapshot.sizeInBytes()) {
                return FetchSnapshotResponse.singleton(
                    log.topicPartition(),
                    responsePartitionSnapshot -> {
                        return addQuorumLeader(responsePartitionSnapshot)
                            .setErrorCode(Errors.POSITION_OUT_OF_RANGE.code());
                    }
                );
            }

            int maxSnapshotSize;
            try {
                maxSnapshotSize = Math.toIntExact(snapshot.sizeInBytes());
            } catch (ArithmeticException e) {
                maxSnapshotSize = Integer.MAX_VALUE;
            }

            ByteBuffer buffer = ByteBuffer.allocate(Math.min(data.maxBytes(), maxSnapshotSize));
            snapshot.read(buffer, partitionSnapshot.position());
            buffer.flip();

            long snapshotSize = snapshot.sizeInBytes();

            return FetchSnapshotResponse.singleton(
                log.topicPartition(),
                responsePartitionSnapshot -> {
                    addQuorumLeader(responsePartitionSnapshot)
                        .snapshotId()
                        .setEndOffset(snapshotId.offset)
                        .setEpoch(snapshotId.epoch);

                    return responsePartitionSnapshot
                        .setSize(snapshotSize)
                        .setPosition(partitionSnapshot.position())
                        .setBytes(buffer);
                }
            );
        }
    }

    private boolean handleFetchSnapshotResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) throws IOException {
        FetchSnapshotResponseData data = (FetchSnapshotResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(data.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (data.topics().size() != 1 && data.topics().get(0).partitions().size() != 1) {
            return false;
        }

        Optional<FetchSnapshotResponseData.PartitionSnapshot> partitionSnapshotOpt = FetchSnapshotResponse
            .forTopicPartition(data, log.topicPartition());
        if (!partitionSnapshotOpt.isPresent()) {
            return false;
        }

        FetchSnapshotResponseData.PartitionSnapshot partitionSnapshot = partitionSnapshotOpt.get();

        FetchSnapshotResponseData.LeaderIdAndEpoch currentLeaderIdAndEpoch = partitionSnapshot.currentLeader();
        OptionalInt responseLeaderId = optionalLeaderId(currentLeaderIdAndEpoch.leaderId());
        int responseEpoch = currentLeaderIdAndEpoch.leaderEpoch();
        Errors error = Errors.forCode(partitionSnapshot.errorCode());

        Optional<Boolean> handled = maybeHandleCommonResponse(
            error, responseLeaderId, responseEpoch, currentTimeMs);
        if (handled.isPresent()) {
            return handled.get();
        }

        FollowerState state = quorum.followerStateOrThrow();

        if (Errors.forCode(partitionSnapshot.errorCode()) == Errors.SNAPSHOT_NOT_FOUND ||
            partitionSnapshot.snapshotId().endOffset() < 0 ||
            partitionSnapshot.snapshotId().epoch() < 0) {

            /* The leader deleted the snapshot before the follower could download it. Start over by
             * reseting the fetching snapshot state and sending another fetch request.
             */
            logger.trace(
                "Leader doesn't know about snapshot id {}, returned error {} and snapshot id {}",
                state.fetchingSnapshot(),
                partitionSnapshot.errorCode(),
                partitionSnapshot.snapshotId()
            );
            state.setFetchingSnapshot(Optional.empty());
            state.resetFetchTimeout(currentTimeMs);
            return true;
        }

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(
            partitionSnapshot.snapshotId().endOffset(),
            partitionSnapshot.snapshotId().epoch()
        );

        RawSnapshotWriter snapshot;
        if (state.fetchingSnapshot().isPresent()) {
            snapshot = state.fetchingSnapshot().get();
        } else {
            throw new IllegalStateException(String.format("Received unexpected fetch snapshot response: %s", partitionSnapshot));
        }

        if (!snapshot.snapshotId().equals(snapshotId)) {
            throw new IllegalStateException(String.format("Received fetch snapshot response with an invalid id. Expected %s; Received %s", snapshot.snapshotId(), snapshotId));
        }
        if (snapshot.sizeInBytes() != partitionSnapshot.position()) {
            throw new IllegalStateException(String.format("Received fetch snapshot response with an invalid position. Expected %s; Received %s", snapshot.sizeInBytes(), partitionSnapshot.position()));
        }

        snapshot.append(partitionSnapshot.bytes());

        if (snapshot.sizeInBytes() == partitionSnapshot.size()) {
            // Finished fetching the snapshot.
            snapshot.freeze();
            state.setFetchingSnapshot(Optional.empty());
        }

        state.resetFetchTimeout(currentTimeMs);
        return true;
    }

    List<ReplicaState> convertToReplicaStates(Map<Integer, Long> replicaEndOffsets) {
        return replicaEndOffsets.entrySet().stream()
                   .map(entry -> new ReplicaState()
                                     .setReplicaId(entry.getKey())
                                     .setLogEndOffset(entry.getValue()))
                   .collect(Collectors.toList());
    }

    private boolean hasConsistentLeader(int epoch, OptionalInt leaderId) {
        // Only elected leaders are sent in the request/response header, so if we have an elected
        // leaderId, it should be consistent with what is in the message.
        if (leaderId.isPresent() && leaderId.getAsInt() == quorum.localIdOrSentinel()) {
            // The response indicates that we should be the leader, so we verify that is the case
            return quorum.isLeader();
        } else {
            return epoch != quorum.epoch()
                || !leaderId.isPresent()
                || !quorum.leaderId().isPresent()
                || leaderId.equals(quorum.leaderId());
        }
    }

    /**
     * Handle response errors that are common across request types.
     *
     * @param error Error from the received response
     * @param leaderId Optional leaderId from the response
     * @param epoch Epoch received from the response
     * @param currentTimeMs Current epoch time in milliseconds
     * @return Optional value indicating whether the error was handled here and the outcome of
     *    that handling. Specifically:
     *
     *    - Optional.empty means that the response was not handled here and the custom
     *        API handler should be applied
     *    - Optional.of(true) indicates that the response was successfully handled here and
     *        the request does not need to be retried
     *    - Optional.of(false) indicates that the response was handled here, but that the request
     *        will need to be retried
     */
    private Optional<Boolean> maybeHandleCommonResponse(
        Errors error,
        OptionalInt leaderId,
        int epoch,
        long currentTimeMs
    ) throws IOException {
        if (epoch < quorum.epoch() || error == Errors.UNKNOWN_LEADER_EPOCH) {
            // We have a larger epoch, so the response is no longer relevant
            return Optional.of(true);
        } else if (epoch > quorum.epoch()
            || error == Errors.FENCED_LEADER_EPOCH
            || error == Errors.NOT_LEADER_OR_FOLLOWER) {

            // The response indicates that the request had a stale epoch, but we need
            // to validate the epoch from the response against our current state.
            maybeTransition(leaderId, epoch, currentTimeMs);
            return Optional.of(true);
        } else if (epoch == quorum.epoch()
            && leaderId.isPresent()
            && !quorum.hasLeader()) {

            // Since we are transitioning to Follower, we will only forward the
            // request to the handler if there is no error. Otherwise, we will let
            // the request be retried immediately (if needed) after the transition.
            // This handling allows an observer to discover the leader and append
            // to the log in the same Fetch request.
            transitionToFollower(epoch, leaderId.getAsInt(), currentTimeMs);
            if (error == Errors.NONE) {
                return Optional.empty();
            } else {
                return Optional.of(true);
            }
        } else if (error == Errors.BROKER_NOT_AVAILABLE) {
            return Optional.of(false);
        } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL) {
            // For now we treat this as a fatal error. Once we have support for quorum
            // reassignment, this error could suggest that either we or the recipient of
            // the request just has stale voter information, which means we can retry
            // after backing off.
            throw new IllegalStateException("Received error indicating inconsistent voter sets");
        } else if (error == Errors.INVALID_REQUEST) {
            throw new IllegalStateException("Received unexpected invalid request error");
        }

        return Optional.empty();
    }

    private void maybeTransition(
        OptionalInt leaderId,
        int epoch,
        long currentTimeMs
    ) throws IOException {
        if (!hasConsistentLeader(epoch, leaderId)) {
            throw new IllegalStateException("Received request or response with leader " + leaderId +
                " and epoch " + epoch + " which is inconsistent with current leader " +
                quorum.leaderId() + " and epoch " + quorum.epoch());
        } else if (epoch > quorum.epoch()) {
            if (leaderId.isPresent()) {
                transitionToFollower(epoch, leaderId.getAsInt(), currentTimeMs);
            } else {
                transitionToUnattached(epoch);
            }
        } else if (leaderId.isPresent() && !quorum.hasLeader()) {
            // The request or response indicates the leader of the current epoch,
            // which is currently unknown
            transitionToFollower(epoch, leaderId.getAsInt(), currentTimeMs);
        }
    }

    private boolean handleTopLevelError(Errors error, RaftResponse.Inbound response) {
        if (error == Errors.BROKER_NOT_AVAILABLE) {
            return false;
        } else if (error == Errors.CLUSTER_AUTHORIZATION_FAILED) {
            throw new ClusterAuthorizationException("Received cluster authorization error in response " + response);
        } else {
            return handleUnexpectedError(error, response);
        }
    }

    private boolean handleUnexpectedError(Errors error, RaftResponse.Inbound response) {
        logger.error("Unexpected error {} in {} response: {}",
            error, ApiKeys.forId(response.data.apiKey()), response);
        return false;
    }

    private void handleResponse(RaftResponse.Inbound response, long currentTimeMs) throws IOException {
        // The response epoch matches the local epoch, so we can handle the response
        ApiKeys apiKey = ApiKeys.forId(response.data.apiKey());
        final boolean handledSuccessfully;

        switch (apiKey) {
            case FETCH:
                handledSuccessfully = handleFetchResponse(response, currentTimeMs);
                break;

            case VOTE:
                handledSuccessfully = handleVoteResponse(response, currentTimeMs);
                break;

            case BEGIN_QUORUM_EPOCH:
                handledSuccessfully = handleBeginQuorumEpochResponse(response, currentTimeMs);
                break;

            case END_QUORUM_EPOCH:
                handledSuccessfully = handleEndQuorumEpochResponse(response, currentTimeMs);
                break;

            case FETCH_SNAPSHOT:
                handledSuccessfully = handleFetchSnapshotResponse(response, currentTimeMs);
                break;

            default:
                throw new IllegalArgumentException("Received unexpected response type: " + apiKey);
        }

        ConnectionState connection = requestManager.getOrCreate(response.sourceId());
        if (handledSuccessfully) {
            connection.onResponseReceived(response.correlationId);
        } else {
            connection.onResponseError(response.correlationId, currentTimeMs);
        }
    }

    /**
     * Validate a request which is only valid between voters. If an error is
     * present in the returned value, it should be returned in the response.
     */
    private Optional<Errors> validateVoterOnlyRequest(int remoteNodeId, int requestEpoch) {
        if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH);
        } else if (remoteNodeId < 0) {
            return Optional.of(Errors.INVALID_REQUEST);
        } else if (quorum.isObserver() || !quorum.isVoter(remoteNodeId)) {
            return Optional.of(Errors.INCONSISTENT_VOTER_SET);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Validate a request which is intended for the current quorum leader.
     * If an error is present in the returned value, it should be returned
     * in the response.
     */
    private Optional<Errors> validateLeaderOnlyRequest(int requestEpoch) {
        if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH);
        } else if (requestEpoch > quorum.epoch()) {
            return Optional.of(Errors.UNKNOWN_LEADER_EPOCH);
        } else if (!quorum.isLeader()) {
            // In general, non-leaders do not expect to receive requests
            // matching their own epoch, but it is possible when observers
            // are using the Fetch API to find the result of an election.
            return Optional.of(Errors.NOT_LEADER_OR_FOLLOWER);
        } else if (shutdown.get() != null) {
            return Optional.of(Errors.BROKER_NOT_AVAILABLE);
        } else {
            return Optional.empty();
        }
    }

    private void handleRequest(RaftRequest.Inbound request, long currentTimeMs) throws IOException {
        ApiKeys apiKey = ApiKeys.forId(request.data.apiKey());
        final CompletableFuture<? extends ApiMessage> responseFuture;

        switch (apiKey) {
            case FETCH:
                responseFuture = handleFetchRequest(request, currentTimeMs);
                break;

            case VOTE:
                responseFuture = completedFuture(handleVoteRequest(request));
                break;

            case BEGIN_QUORUM_EPOCH:
                responseFuture = completedFuture(handleBeginQuorumEpochRequest(request, currentTimeMs));
                break;

            case END_QUORUM_EPOCH:
                responseFuture = completedFuture(handleEndQuorumEpochRequest(request, currentTimeMs));
                break;

            case DESCRIBE_QUORUM:
                responseFuture = completedFuture(handleDescribeQuorumRequest(request, currentTimeMs));
                break;

            case FETCH_SNAPSHOT:
                responseFuture = completedFuture(handleFetchSnapshotRequest(request));
                break;

            default:
                throw new IllegalArgumentException("Unexpected request type " + apiKey);
        }

        responseFuture.whenComplete((response, exception) -> {
            final ApiMessage message;
            if (response != null) {
                message = response;
            } else {
                message = RaftUtil.errorResponse(apiKey, Errors.forException(exception));
            }

            RaftResponse.Outbound responseMessage = new RaftResponse.Outbound(request.correlationId(), message);
            request.completion.complete(responseMessage);
            logger.trace("Sent response {} to inbound request {}", responseMessage, request);
        });
    }

    private void handleInboundMessage(RaftMessage message, long currentTimeMs) throws IOException {
        logger.trace("Received inbound message {}", message);

        if (message instanceof RaftRequest.Inbound) {
            RaftRequest.Inbound request = (RaftRequest.Inbound) message;
            handleRequest(request, currentTimeMs);
        } else if (message instanceof RaftResponse.Inbound) {
            RaftResponse.Inbound response = (RaftResponse.Inbound) message;
            ConnectionState connection = requestManager.getOrCreate(response.sourceId());
            if (connection.isResponseExpected(response.correlationId)) {
                handleResponse(response, currentTimeMs);
            } else {
                logger.debug("Ignoring response {} since it is no longer needed", response);
            }
        } else {
            throw new IllegalArgumentException("Unexpected message " + message);
        }
    }

    /**
     * Attempt to send a request. Return the time to wait before the request can be retried.
     */
    private long maybeSendRequest(
        long currentTimeMs,
        int destinationId,
        Supplier<ApiMessage> requestSupplier
    )  {
        ConnectionState connection = requestManager.getOrCreate(destinationId);

        if (connection.isBackingOff(currentTimeMs)) {
            long remainingBackoffMs = connection.remainingBackoffMs(currentTimeMs);
            logger.debug("Connection for {} is backing off for {} ms", destinationId, remainingBackoffMs);
            return remainingBackoffMs;
        }

        if (connection.isReady(currentTimeMs)) {
            int correlationId = channel.newCorrelationId();
            ApiMessage request = requestSupplier.get();

            RaftRequest.Outbound requestMessage = new RaftRequest.Outbound(
                correlationId,
                request,
                destinationId,
                currentTimeMs
            );

            requestMessage.completion.whenComplete((response, exception) -> {
                if (exception != null) {
                    ApiKeys api = ApiKeys.forId(request.apiKey());
                    Errors error = Errors.forException(exception);
                    ApiMessage errorResponse = RaftUtil.errorResponse(api, error);

                    response = new RaftResponse.Inbound(
                        correlationId,
                        errorResponse,
                        destinationId
                    );
                }

                messageQueue.add(response);
            });

            channel.send(requestMessage);
            logger.trace("Sent outbound request: {}", requestMessage);
            connection.onRequestSent(correlationId, currentTimeMs);
            return Long.MAX_VALUE;
        }

        return connection.remainingRequestTimeMs(currentTimeMs);
    }

    private EndQuorumEpochRequestData buildEndQuorumEpochRequest(
        ResignedState state
    ) {
        return EndQuorumEpochRequest.singletonRequest(
            log.topicPartition(),
            quorum.epoch(),
            quorum.localIdOrThrow(),
            state.preferredSuccessors()
        );
    }

    private long maybeSendRequests(
        long currentTimeMs,
        Set<Integer> destinationIds,
        Supplier<ApiMessage> requestSupplier
    ) {
        long minBackoffMs = Long.MAX_VALUE;
        for (Integer destinationId : destinationIds) {
            long backoffMs = maybeSendRequest(currentTimeMs, destinationId, requestSupplier);
            if (backoffMs < minBackoffMs) {
                minBackoffMs = backoffMs;
            }
        }
        return minBackoffMs;
    }

    private BeginQuorumEpochRequestData buildBeginQuorumEpochRequest() {
        return BeginQuorumEpochRequest.singletonRequest(
            log.topicPartition(),
            quorum.epoch(),
            quorum.localIdOrThrow()
        );
    }

    private VoteRequestData buildVoteRequest() {
        OffsetAndEpoch endOffset = endOffset();
        return VoteRequest.singletonRequest(
            log.topicPartition(),
            quorum.epoch(),
            quorum.localIdOrThrow(),
            endOffset.epoch,
            endOffset.offset
        );
    }

    private FetchRequestData buildFetchRequest() {
        FetchRequestData request = RaftUtil.singletonFetchRequest(log.topicPartition(), fetchPartition -> {
            fetchPartition
                .setCurrentLeaderEpoch(quorum.epoch())
                .setLastFetchedEpoch(log.lastFetchedEpoch())
                .setFetchOffset(log.endOffset().offset);
        });
        return request
            .setMaxWaitMs(fetchMaxWaitMs)
            .setReplicaId(quorum.localIdOrSentinel());
    }

    private long maybeSendAnyVoterFetch(long currentTimeMs) {
        OptionalInt readyVoterIdOpt = requestManager.findReadyVoter(currentTimeMs);
        if (readyVoterIdOpt.isPresent()) {
            return maybeSendRequest(
                currentTimeMs,
                readyVoterIdOpt.getAsInt(),
                this::buildFetchRequest
            );
        } else {
            return requestManager.backoffBeforeAvailableVoter(currentTimeMs);
        }
    }

    private FetchSnapshotRequestData buildFetchSnapshotRequest(OffsetAndEpoch snapshotId, long snapshotSize) {
        FetchSnapshotRequestData.SnapshotId requestSnapshotId = new FetchSnapshotRequestData.SnapshotId()
            .setEpoch(snapshotId.epoch)
            .setEndOffset(snapshotId.offset);

        FetchSnapshotRequestData request = FetchSnapshotRequest.singleton(
            log.topicPartition(),
            snapshotPartition -> {
                return snapshotPartition
                    .setCurrentLeaderEpoch(quorum.epoch())
                    .setSnapshotId(requestSnapshotId)
                    .setPosition(snapshotSize);
            }
        );

        return request.setReplicaId(quorum.localIdOrSentinel());
    }

    private FetchSnapshotResponseData.PartitionSnapshot addQuorumLeader(
        FetchSnapshotResponseData.PartitionSnapshot partitionSnapshot
    ) {
        partitionSnapshot.currentLeader()
            .setLeaderEpoch(quorum.epoch())
            .setLeaderId(quorum.leaderIdOrSentinel());

        return partitionSnapshot;
    }

    public boolean isRunning() {
        GracefulShutdown gracefulShutdown = shutdown.get();
        return gracefulShutdown == null || !gracefulShutdown.isFinished();
    }

    public boolean isShuttingDown() {
        GracefulShutdown gracefulShutdown = shutdown.get();
        return gracefulShutdown != null && !gracefulShutdown.isFinished();
    }

    private void appendBatch(
        LeaderState state,
        BatchAccumulator.CompletedBatch<T> batch,
        long appendTimeMs
    ) {
        try {
            int epoch = state.epoch();
            LogAppendInfo info = appendAsLeader(batch.data);
            OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(info.lastOffset, epoch);
            CompletableFuture<Long> future = appendPurgatory.await(
                offsetAndEpoch.offset + 1, Integer.MAX_VALUE);

            future.whenComplete((commitTimeMs, exception) -> {
                int numRecords = batch.records.size();
                if (exception != null) {
                    logger.debug("Failed to commit {} records at {}", numRecords, offsetAndEpoch, exception);
                } else {
                    long elapsedTime = Math.max(0, commitTimeMs - appendTimeMs);
                    double elapsedTimePerRecord = (double) elapsedTime / numRecords;
                    kafkaRaftMetrics.updateCommitLatency(elapsedTimePerRecord, appendTimeMs);
                    logger.debug("Completed commit of {} records at {}", numRecords, offsetAndEpoch);
                    maybeFireHandleCommit(batch.baseOffset, epoch, batch.records);
                }
            });
        } finally {
            batch.release();
        }
    }

    private long maybeAppendBatches(
        LeaderState state,
        long currentTimeMs
    ) {
        long timeUnitFlush = accumulator.timeUntilDrain(currentTimeMs);
        if (timeUnitFlush <= 0) {
            List<BatchAccumulator.CompletedBatch<T>> batches = accumulator.drain();
            Iterator<BatchAccumulator.CompletedBatch<T>> iterator = batches.iterator();

            try {
                while (iterator.hasNext()) {
                    BatchAccumulator.CompletedBatch<T> batch = iterator.next();
                    appendBatch(state, batch, currentTimeMs);
                }
                flushLeaderLog(state, currentTimeMs);
            } finally {
                // Release and discard any batches which failed to be appended
                while (iterator.hasNext()) {
                    iterator.next().release();
                }
            }
        }
        return timeUnitFlush;
    }

    private long pollResigned(long currentTimeMs) throws IOException {
        ResignedState state = quorum.resignedStateOrThrow();
        long endQuorumBackoffMs = maybeSendRequests(
            currentTimeMs,
            state.unackedVoters(),
            () -> buildEndQuorumEpochRequest(state)
        );

        GracefulShutdown shutdown = this.shutdown.get();
        final long stateTimeoutMs;
        if (shutdown != null) {
            // If we are shutting down, then we will remain in the resigned state
            // until either the shutdown expires or an election bumps the epoch
            stateTimeoutMs = shutdown.remainingTimeMs();
        } else if (state.hasElectionTimeoutExpired(currentTimeMs)) {
            transitionToCandidate(currentTimeMs);
            stateTimeoutMs = 0L;
        } else {
            stateTimeoutMs = state.remainingElectionTimeMs(currentTimeMs);
        }

        return Math.min(stateTimeoutMs, endQuorumBackoffMs);
    }

    private long pollLeader(long currentTimeMs) {
        LeaderState state = quorum.leaderStateOrThrow();
        maybeFireHandleClaim(state);

        GracefulShutdown shutdown = this.shutdown.get();
        if (shutdown != null) {
            transitionToResigned(state.nonLeaderVotersByDescendingFetchOffset());
            return 0L;
        }

        long timeUntilFlush = maybeAppendBatches(
            state,
            currentTimeMs
        );

        long timeUntilSend = maybeSendRequests(
            currentTimeMs,
            state.nonAcknowledgingVoters(),
            this::buildBeginQuorumEpochRequest
        );

        return Math.min(timeUntilFlush, timeUntilSend);
    }

    private long maybeSendVoteRequests(
        CandidateState state,
        long currentTimeMs
    ) {
        // Continue sending Vote requests as long as we still have a chance to win the election
        if (!state.isVoteRejected()) {
            return maybeSendRequests(
                currentTimeMs,
                state.unrecordedVoters(),
                this::buildVoteRequest
            );
        }
        return Long.MAX_VALUE;
    }

    private long pollCandidate(long currentTimeMs) throws IOException {
        CandidateState state = quorum.candidateStateOrThrow();
        GracefulShutdown shutdown = this.shutdown.get();

        if (shutdown != null) {
            // If we happen to shutdown while we are a candidate, we will continue
            // with the current election until one of the following conditions is met:
            //  1) we are elected as leader (which allows us to resign)
            //  2) another leader is elected
            //  3) the shutdown timer expires
            long minRequestBackoffMs = maybeSendVoteRequests(state, currentTimeMs);
            return Math.min(shutdown.remainingTimeMs(), minRequestBackoffMs);
        } else if (state.isBackingOff()) {
            if (state.isBackoffComplete(currentTimeMs)) {
                logger.info("Re-elect as candidate after election backoff has completed");
                transitionToCandidate(currentTimeMs);
                return 0L;
            }
            return state.remainingBackoffMs(currentTimeMs);
        } else if (state.hasElectionTimeoutExpired(currentTimeMs)) {
            long backoffDurationMs = binaryExponentialElectionBackoffMs(state.retries());
            logger.debug("Election has timed out, backing off for {}ms before becoming a candidate again",
                backoffDurationMs);
            state.startBackingOff(currentTimeMs, backoffDurationMs);
            return backoffDurationMs;
        } else {
            long minRequestBackoffMs = maybeSendVoteRequests(state, currentTimeMs);
            return Math.min(minRequestBackoffMs, state.remainingElectionTimeMs(currentTimeMs));
        }
    }

    private long pollFollower(long currentTimeMs) throws IOException {
        FollowerState state = quorum.followerStateOrThrow();
        if (quorum.isVoter()) {
            return pollFollowerAsVoter(state, currentTimeMs);
        } else {
            return pollFollowerAsObserver(state, currentTimeMs);
        }
    }

    private long pollFollowerAsVoter(FollowerState state, long currentTimeMs) throws IOException {
        GracefulShutdown shutdown = this.shutdown.get();
        if (shutdown != null) {
            // If we are a follower, then we can shutdown immediately. We want to
            // skip the transition to candidate in any case.
            return 0;
        } else if (state.hasFetchTimeoutExpired(currentTimeMs)) {
            logger.info("Become candidate due to fetch timeout");
            transitionToCandidate(currentTimeMs);
            return 0L;
        } else {
            long backoffMs = maybeSendFetchOrFetchSnapshot(state, currentTimeMs);

            return Math.min(backoffMs, state.remainingFetchTimeMs(currentTimeMs));
        }
    }

    private long pollFollowerAsObserver(FollowerState state, long currentTimeMs) throws IOException {
        if (state.hasFetchTimeoutExpired(currentTimeMs)) {
            return maybeSendAnyVoterFetch(currentTimeMs);
        } else {
            final long backoffMs;

            // If the current leader is backing off due to some failure or if the
            // request has timed out, then we attempt to send the Fetch to another
            // voter in order to discover if there has been a leader change.
            ConnectionState connection = requestManager.getOrCreate(state.leaderId());
            if (connection.hasRequestTimedOut(currentTimeMs)) {
                backoffMs = maybeSendAnyVoterFetch(currentTimeMs);
                connection.reset();
            } else if (connection.isBackingOff(currentTimeMs)) {
                backoffMs = maybeSendAnyVoterFetch(currentTimeMs);
            } else {
                backoffMs = maybeSendFetchOrFetchSnapshot(state, currentTimeMs);
            }

            return Math.min(backoffMs, state.remainingFetchTimeMs(currentTimeMs));
        }
    }

    private long maybeSendFetchOrFetchSnapshot(FollowerState state, long currentTimeMs) throws IOException {
        final Supplier<ApiMessage> requestSupplier;

        if (state.fetchingSnapshot().isPresent()) {
            RawSnapshotWriter snapshot = state.fetchingSnapshot().get();
            long snapshotSize = snapshot.sizeInBytes();

            requestSupplier = () -> buildFetchSnapshotRequest(snapshot.snapshotId(), snapshotSize);
        } else {
            requestSupplier = this::buildFetchRequest;
        }

        return maybeSendRequest(currentTimeMs, state.leaderId(), requestSupplier);
    }

    private long pollVoted(long currentTimeMs) throws IOException {
        VotedState state = quorum.votedStateOrThrow();
        GracefulShutdown shutdown = this.shutdown.get();

        if (shutdown != null) {
            // If shutting down, then remain in this state until either the
            // shutdown completes or an epoch bump forces another state transition
            return shutdown.remainingTimeMs();
        } else if (state.hasElectionTimeoutExpired(currentTimeMs)) {
            transitionToCandidate(currentTimeMs);
            return 0L;
        } else {
            return state.remainingElectionTimeMs(currentTimeMs);
        }
    }

    private long pollUnattached(long currentTimeMs) throws IOException {
        UnattachedState state = quorum.unattachedStateOrThrow();
        if (quorum.isVoter()) {
            return pollUnattachedAsVoter(state, currentTimeMs);
        } else {
            return pollUnattachedAsObserver(state, currentTimeMs);
        }
    }

    private long pollUnattachedAsVoter(UnattachedState state, long currentTimeMs) throws IOException {
        GracefulShutdown shutdown = this.shutdown.get();
        if (shutdown != null) {
            // If shutting down, then remain in this state until either the
            // shutdown completes or an epoch bump forces another state transition
            return shutdown.remainingTimeMs();
        } else if (state.hasElectionTimeoutExpired(currentTimeMs)) {
            transitionToCandidate(currentTimeMs);
            return 0L;
        } else {
            return state.remainingElectionTimeMs(currentTimeMs);
        }
    }

    private long pollUnattachedAsObserver(UnattachedState state, long currentTimeMs) {
        long fetchBackoffMs = maybeSendAnyVoterFetch(currentTimeMs);
        return Math.min(fetchBackoffMs, state.remainingElectionTimeMs(currentTimeMs));
    }

    private long pollCurrentState(long currentTimeMs) throws IOException {
        if (quorum.isLeader()) {
            return pollLeader(currentTimeMs);
        } else if (quorum.isCandidate()) {
            return pollCandidate(currentTimeMs);
        } else if (quorum.isFollower()) {
            return pollFollower(currentTimeMs);
        } else if (quorum.isVoted()) {
            return pollVoted(currentTimeMs);
        } else if (quorum.isUnattached()) {
            return pollUnattached(currentTimeMs);
        } else if (quorum.isResigned()) {
            return pollResigned(currentTimeMs);
        } else {
            throw new IllegalStateException("Unexpected quorum state " + quorum);
        }
    }

    private void pollListeners() {
        // Register any listeners added since the last poll
        while (!pendingListeners.isEmpty()) {
            Listener<T> listener = pendingListeners.poll();
            listenerContexts.add(new ListenerContext(listener));
        }

        // Check listener progress to see if reads are expected
        quorum.highWatermark().ifPresent(highWatermarkMetadata -> {
            long highWatermark = highWatermarkMetadata.offset;

            List<ListenerContext> listenersToUpdate = listenerContexts.stream()
                .filter(listenerContext -> {
                    OptionalLong nextExpectedOffset = listenerContext.nextExpectedOffset();
                    return nextExpectedOffset.isPresent() && nextExpectedOffset.getAsLong() < highWatermark;
                })
                .collect(Collectors.toList());

            maybeFireHandleCommit(listenersToUpdate, highWatermarkMetadata.offset);
        });
    }

    private boolean maybeCompleteShutdown(long currentTimeMs) {
        GracefulShutdown shutdown = this.shutdown.get();
        if (shutdown == null) {
            return false;
        }

        shutdown.update(currentTimeMs);
        if (shutdown.hasTimedOut()) {
            shutdown.failWithTimeout();
            return true;
        }

        if (quorum.isObserver()
            || quorum.remoteVoters().isEmpty()
            || quorum.hasRemoteLeader()) {

            shutdown.complete();
            return true;
        }

        return false;
    }

    private void wakeup() {
        messageQueue.wakeup();
    }

    /**
     * Handle an inbound request. The response will be returned through
     * {@link RaftRequest.Inbound#completion}.
     *
     * @param request The inbound request
     */
    public void handle(RaftRequest.Inbound request) {
        messageQueue.add(Objects.requireNonNull(request));
    }

    /**
     * Poll for new events. This allows the client to handle inbound
     * requests and send any needed outbound requests.
     *
     * @throws IOException for any IO errors encountered
     */
    public void poll() throws IOException {
        pollListeners();

        long currentTimeMs = time.milliseconds();
        if (maybeCompleteShutdown(currentTimeMs)) {
            return;
        }

        long pollTimeoutMs = pollCurrentState(currentTimeMs);
        kafkaRaftMetrics.updatePollStart(currentTimeMs);

        RaftMessage message = messageQueue.poll(pollTimeoutMs);

        currentTimeMs = time.milliseconds();
        kafkaRaftMetrics.updatePollEnd(currentTimeMs);

        if (message != null) {
            handleInboundMessage(message, currentTimeMs);
        }
    }

    @Override
    public Long scheduleAppend(int epoch, List<T> records) {
        BatchAccumulator<T> accumulator = this.accumulator;
        if (accumulator == null) {
            return Long.MAX_VALUE;
        }

        boolean isFirstAppend = accumulator.isEmpty();
        Long offset = accumulator.append(epoch, records);

        // Wakeup the network channel if either this is the first append
        // or the accumulator is ready to drain now. Checking for the first
        // append ensures that we give the IO thread a chance to observe
        // the linger timeout so that it can schedule its own wakeup in case
        // there are no additional appends.
        if (isFirstAppend || accumulator.needsDrain(time.milliseconds())) {
            wakeup();
        }
        return offset;
    }

    @Override
    public CompletableFuture<Void> shutdown(int timeoutMs) {
        logger.info("Beginning graceful shutdown");
        CompletableFuture<Void> shutdownComplete = new CompletableFuture<>();
        shutdown.set(new GracefulShutdown(timeoutMs, shutdownComplete));
        wakeup();
        return shutdownComplete;
    }

    @Override
    public SnapshotWriter<T> createSnapshot(OffsetAndEpoch snapshotId) throws IOException {
        return new SnapshotWriter<>(
            log.createSnapshot(snapshotId),
            MAX_BATCH_SIZE,
            memoryPool,
            time,
            CompressionType.NONE,
            serde
        );
    }

    @Override
    public void close() {
        if (kafkaRaftMetrics != null) {
            kafkaRaftMetrics.close();
        }
    }

    QuorumState quorum() {
        return quorum;
    }

    public OptionalLong highWatermark() {
        return quorum.highWatermark().isPresent() ? OptionalLong.of(quorum.highWatermark().get().offset) : OptionalLong.empty();
    }

    private class GracefulShutdown {
        final Timer finishTimer;
        final CompletableFuture<Void> completeFuture;

        public GracefulShutdown(long shutdownTimeoutMs,
                                CompletableFuture<Void> completeFuture) {
            this.finishTimer = time.timer(shutdownTimeoutMs);
            this.completeFuture = completeFuture;
        }

        public void update(long currentTimeMs) {
            finishTimer.update(currentTimeMs);
        }

        public boolean hasTimedOut() {
            return finishTimer.isExpired();
        }

        public boolean isFinished() {
            return completeFuture.isDone();
        }

        public long remainingTimeMs() {
            return finishTimer.remainingMs();
        }

        public void failWithTimeout() {
            logger.warn("Graceful shutdown timed out after {}ms", finishTimer.timeoutMs());
            completeFuture.completeExceptionally(
                new TimeoutException("Timeout expired before graceful shutdown completed"));
        }

        public void complete() {
            logger.info("Graceful shutdown completed");
            completeFuture.complete(null);
        }
    }

    private final class ListenerContext implements CloseListener<BatchReader<T>> {
        private final RaftClient.Listener<T> listener;
        // This field is used only by the Raft IO thread
        private int claimedEpoch = 0;

        // These fields are visible to both the Raft IO thread and the listener
        // and are protected through synchronization on this `ListenerContext` instance
        private BatchReader<T> lastSent = null;
        private long lastAckedOffset = 0;

        private ListenerContext(Listener<T> listener) {
            this.listener = listener;
        }

        /**
         * Get the last acked offset, which is one greater than the offset of the
         * last record which was acked by the state machine.
         */
        public synchronized long lastAckedOffset() {
            return lastAckedOffset;
        }

        /**
         * Get the next expected offset, which might be larger than the last acked
         * offset if there are inflight batches which have not been acked yet.
         * Note that when fetching from disk, we may not know the last offset of
         * inflight data until it has been processed by the state machine. In this case,
         * we delay sending additional data until the state machine has read to the
         * end and the last offset is determined.
         */
        public synchronized OptionalLong nextExpectedOffset() {
            if (lastSent != null) {
                OptionalLong lastSentOffset = lastSent.lastOffset();
                if (lastSentOffset.isPresent()) {
                    return OptionalLong.of(lastSentOffset.getAsLong() + 1);
                } else {
                    return OptionalLong.empty();
                }
            } else {
                return OptionalLong.of(lastAckedOffset);
            }
        }

        /**
         * This API is used for committed records that have been received through
         * replication. In general, followers will write new data to disk before they
         * know whether it has been committed. Rather than retaining the uncommitted
         * data in memory, we let the state machine read the records from disk.
         */
        public void fireHandleCommit(long baseOffset, Records records) {
            BufferSupplier bufferSupplier = BufferSupplier.create();
            RecordsBatchReader<T> reader = new RecordsBatchReader<>(baseOffset, records,
                serde, bufferSupplier, this);
            fireHandleCommit(reader);
        }

        /**
         * This API is used for committed records originating from {@link #scheduleAppend(int, List)}
         * on this instance. In this case, we are able to save the original record objects,
         * which saves the need to read them back from disk. This is a nice optimization
         * for the leader which is typically doing more work than all of the followers.
         */
        public void fireHandleCommit(long baseOffset, int epoch, List<T> records) {
            BatchReader.Batch<T> batch = new BatchReader.Batch<>(baseOffset, epoch, records);
            MemoryBatchReader<T> reader = new MemoryBatchReader<>(Collections.singletonList(batch), this);
            fireHandleCommit(reader);
        }

        private void fireHandleCommit(BatchReader<T> reader) {
            synchronized (this) {
                this.lastSent = reader;
            }
            listener.handleCommit(reader);
        }

        void maybeFireHandleClaim(int epoch, long epochStartOffset) {
            // We can fire `handleClaim` as soon as the listener has caught
            // up to the start of the leader epoch. This guarantees that the
            // state machine has seen the full committed state before it becomes
            // leader and begins writing to the log.
            if (epoch > claimedEpoch && lastAckedOffset() >= epochStartOffset) {
                claimedEpoch = epoch;
                listener.handleClaim(epoch);
            }
        }

        void fireHandleResign() {
            listener.handleResign();
        }

        public synchronized void onClose(BatchReader<T> reader) {
            OptionalLong lastOffset = reader.lastOffset();
            if (lastOffset.isPresent()) {
                lastAckedOffset = lastOffset.getAsLong() + 1;
            }

            if (lastSent == reader) {
                lastSent = null;
                wakeup();
            }
        }

    }

}
