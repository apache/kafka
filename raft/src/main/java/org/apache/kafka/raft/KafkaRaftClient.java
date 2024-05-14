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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.memory.MemoryPool;
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
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.record.UnalignedRecords;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EndQuorumEpochResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.FetchSnapshotResponse;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.RequestManager.ConnectionState;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.BatchMemoryPool;
import org.apache.kafka.raft.internals.BlockingMessageQueue;
import org.apache.kafka.raft.internals.CloseListener;
import org.apache.kafka.raft.internals.FuturePurgatory;
import org.apache.kafka.raft.internals.KRaftControlRecordStateMachine;
import org.apache.kafka.raft.internals.KafkaRaftMetrics;
import org.apache.kafka.raft.internals.MemoryBatchReader;
import org.apache.kafka.raft.internals.RecordsBatchReader;
import org.apache.kafka.raft.internals.ReplicaKey;
import org.apache.kafka.raft.internals.ThresholdPurgatory;
import org.apache.kafka.raft.internals.VoterSet;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.NotifyingRawSnapshotWriter;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.IdentityHashMap;
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
 * 4) {@link FetchRequestData}: This is the same as the usual Fetch API in Kafka, but we add snapshot
 *    check before responding, and we also piggyback some additional metadata on responses (i.e. current
 *    leader and epoch). Unlike partition replication, we also piggyback truncation detection on this API
 *    rather than through a separate truncation state.
 *
 * 5) {@link FetchSnapshotRequestData}: Sent by the follower to the epoch leader in order to fetch a snapshot.
 *    This happens when a FetchResponse includes a snapshot ID due to the follower's log end offset being less
 *    than the leader's log start offset. This API is similar to the Fetch API since the snapshot is stored
 *    as FileRecords, but we use {@link UnalignedRecords} in FetchSnapshotResponse because the records
 *    are not necessarily offset-aligned.
 */
final public class KafkaRaftClient<T> implements RaftClient<T> {
    private static final int RETRY_BACKOFF_BASE_MS = 100;
    public static final int MAX_FETCH_WAIT_MS = 500;
    public static final int MAX_BATCH_SIZE_BYTES = 8 * 1024 * 1024;
    public static final int MAX_FETCH_SIZE_BYTES = MAX_BATCH_SIZE_BYTES;

    private final OptionalInt nodeId;
    private final Uuid nodeDirectoryId;
    private final AtomicReference<GracefulShutdown> shutdown = new AtomicReference<>();
    private final LogContext logContext;
    private final Logger logger;
    private final Time time;
    private final int fetchMaxWaitMs;
    private final String clusterId;
    private final NetworkChannel channel;
    private final ReplicatedLog log;
    private final Random random;
    private final FuturePurgatory<Long> appendPurgatory;
    private final FuturePurgatory<Long> fetchPurgatory;
    private final RecordSerde<T> serde;
    private final MemoryPool memoryPool;
    private final RaftMessageQueue messageQueue;
    private final QuorumConfig quorumConfig;
    private final RaftMetadataLogCleanerManager snapshotCleaner;

    private final Map<Listener<T>, ListenerContext> listenerContexts = new IdentityHashMap<>();
    private final ConcurrentLinkedQueue<Registration<T>> pendingRegistrations = new ConcurrentLinkedQueue<>();

    // These components need to be initialized by the method initialize() because they depend on
    // the voter set
    /*
     * The key invariant for the kraft control record state machine is that it has always read to
     * the LEO. This is achieved by:
     *
     * 1. reading the entire partition (snapshot and log) at start up,
     * 2. updating the state when a snapshot is replaced, because of FETCH_SNAPSHOT, on the
     *    followers
     * 3. updating the state when the leader (call to append()) or follower (FETCH) appends to the
     *    log
     * 4. truncate new entries when a follower truncates their log
     * 5. truncate old entries when a snapshot gets generated
     */
    private volatile KRaftControlRecordStateMachine partitionState;
    private volatile KafkaRaftMetrics kafkaRaftMetrics;
    private volatile QuorumState quorum;
    private volatile RequestManager requestManager;

    /**
     * Create a new instance.
     *
     * Note that if the node ID is empty, then the client will behave as a
     * non-participating observer.
     */
    public KafkaRaftClient(
        OptionalInt nodeId,
        Uuid nodeDirectoryId,
        RecordSerde<T> serde,
        NetworkChannel channel,
        ReplicatedLog log,
        Time time,
        ExpirationService expirationService,
        LogContext logContext,
        String clusterId,
        QuorumConfig quorumConfig
    ) {
        this(
            nodeId,
            nodeDirectoryId,
            serde,
            channel,
            new BlockingMessageQueue(),
            log,
            new BatchMemoryPool(5, MAX_BATCH_SIZE_BYTES),
            time,
            expirationService,
            MAX_FETCH_WAIT_MS,
            clusterId,
            logContext,
            new Random(),
            quorumConfig
        );
    }

    KafkaRaftClient(
        OptionalInt nodeId,
        Uuid nodeDirectoryId,
        RecordSerde<T> serde,
        NetworkChannel channel,
        RaftMessageQueue messageQueue,
        ReplicatedLog log,
        MemoryPool memoryPool,
        Time time,
        ExpirationService expirationService,
        int fetchMaxWaitMs,
        String clusterId,
        LogContext logContext,
        Random random,
        QuorumConfig quorumConfig
    ) {
        this.nodeId = nodeId;
        this.nodeDirectoryId = nodeDirectoryId;
        this.logContext = logContext;
        this.serde = serde;
        this.channel = channel;
        this.messageQueue = messageQueue;
        this.log = log;
        this.memoryPool = memoryPool;
        this.fetchPurgatory = new ThresholdPurgatory<>(expirationService);
        this.appendPurgatory = new ThresholdPurgatory<>(expirationService);
        this.time = time;
        this.clusterId = clusterId;
        this.fetchMaxWaitMs = fetchMaxWaitMs;
        this.logger = logContext.logger(KafkaRaftClient.class);
        this.random = random;
        this.quorumConfig = quorumConfig;
        this.snapshotCleaner = new RaftMetadataLogCleanerManager(logger, time, 60000, log::maybeClean);
    }

    private void updateFollowerHighWatermark(
        FollowerState state,
        OptionalLong highWatermarkOpt
    ) {
        highWatermarkOpt.ifPresent(highWatermark -> {
            long newHighWatermark = Math.min(endOffset().offset(), highWatermark);
            if (state.updateHighWatermark(OptionalLong.of(newHighWatermark))) {
                logger.debug("Follower high watermark updated to {}", newHighWatermark);
                log.updateHighWatermark(new LogOffsetMetadata(newHighWatermark));
                updateListenersProgress(newHighWatermark);
            }
        });
    }

    private void updateLeaderEndOffsetAndTimestamp(
        LeaderState<T> state,
        long currentTimeMs
    ) {
        final LogOffsetMetadata endOffsetMetadata = log.endOffset();

        if (state.updateLocalState(endOffsetMetadata)) {
            onUpdateLeaderHighWatermark(state, currentTimeMs);
        }

        fetchPurgatory.maybeComplete(endOffsetMetadata.offset, currentTimeMs);
    }

    private void onUpdateLeaderHighWatermark(
        LeaderState<T> state,
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
            updateListenersProgress(highWatermark.offset);
        });
    }

    private void updateListenersProgress(long highWatermark) {
        for (ListenerContext listenerContext : listenerContexts.values()) {
            listenerContext.nextExpectedOffset().ifPresent(nextExpectedOffset -> {
                // Send snapshot to the listener, if the listener is at the beginning of the log and there is a snapshot,
                // or the listener is trying to read an offset for which there isn't a segment in the log.
                if (nextExpectedOffset < highWatermark &&
                    ((nextExpectedOffset == 0 && latestSnapshot().isPresent()) ||
                     nextExpectedOffset < log.startOffset())
                ) {
                    SnapshotReader<T> snapshot = latestSnapshot().orElseThrow(() -> new IllegalStateException(
                        String.format(
                            "Snapshot expected since next offset of %s is %d, log start offset is %d and high-watermark is %d",
                            listenerContext.listenerName(),
                            nextExpectedOffset,
                            log.startOffset(),
                            highWatermark
                        )
                    ));
                    listenerContext.fireHandleSnapshot(snapshot);
                }
            });

            // Re-read the expected offset in case the snapshot had to be reloaded
            listenerContext.nextExpectedOffset().ifPresent(nextExpectedOffset -> {
                if (nextExpectedOffset < highWatermark) {
                    LogFetchInfo readInfo = log.read(nextExpectedOffset, Isolation.COMMITTED);
                    listenerContext.fireHandleCommit(nextExpectedOffset, readInfo.records);
                }
            });
        }
    }

    private Optional<SnapshotReader<T>> latestSnapshot() {
        return log.latestSnapshot().map(reader ->
            RecordsSnapshotReader.of(reader,
                serde,
                BufferSupplier.create(),
                MAX_BATCH_SIZE_BYTES,
                true /* Validate batch CRC*/
            )
        );
    }

    private void maybeFireHandleCommit(long baseOffset, int epoch, long appendTimestamp, int sizeInBytes, List<T> records) {
        for (ListenerContext listenerContext : listenerContexts.values()) {
            listenerContext.nextExpectedOffset().ifPresent(nextOffset -> {
                if (nextOffset == baseOffset) {
                    listenerContext.fireHandleCommit(baseOffset, epoch, appendTimestamp, sizeInBytes, records);
                }
            });
        }
    }

    private void maybeFireLeaderChange(LeaderState<T> state) {
        for (ListenerContext listenerContext : listenerContexts.values()) {
            listenerContext.maybeFireLeaderChange(quorum.leaderAndEpoch(), state.epochStartOffset());
        }
    }

    private void maybeFireLeaderChange() {
        for (ListenerContext listenerContext : listenerContexts.values()) {
            listenerContext.maybeFireLeaderChange(quorum.leaderAndEpoch());
        }
    }

    public void initialize(
        Map<Integer, InetSocketAddress> voterAddresses,
        String listenerName,
        QuorumStateStore quorumStateStore,
        Metrics metrics
    ) {
        partitionState = new KRaftControlRecordStateMachine(
            Optional.of(VoterSet.fromInetSocketAddresses(listenerName, voterAddresses)),
            log,
            serde,
            BufferSupplier.create(),
            MAX_BATCH_SIZE_BYTES,
            logContext
        );
        // Read the entire log
        logger.info("Reading KRaft snapshot and log as part of the initialization");
        partitionState.updateState();

        VoterSet lastVoterSet = partitionState.lastVoterSet();
        requestManager = new RequestManager(
            lastVoterSet.voterIds(),
            quorumConfig.retryBackoffMs(),
            quorumConfig.requestTimeoutMs(),
            random
        );

        quorum = new QuorumState(
            nodeId,
            nodeDirectoryId,
            partitionState::lastVoterSet,
            partitionState::lastKraftVersion,
            quorumConfig.electionTimeoutMs(),
            quorumConfig.fetchTimeoutMs(),
            quorumStateStore,
            time,
            logContext,
            random
        );

        kafkaRaftMetrics = new KafkaRaftMetrics(metrics, "raft", quorum);
        // All Raft voters are statically configured and known at startup
        // so there are no unknown voter connections. Report this metric as 0.
        kafkaRaftMetrics.updateNumUnknownVoterConnections(0);

        for (Integer voterId : lastVoterSet.voterIds()) {
            channel.updateEndpoint(voterId, lastVoterSet.voterAddress(voterId, listenerName).get());
        }

        quorum.initialize(new OffsetAndEpoch(log.endOffset().offset, log.lastFetchedEpoch()));

        long currentTimeMs = time.milliseconds();
        if (quorum.isLeader()) {
            throw new IllegalStateException("Voter cannot initialize as a Leader");
        } else if (quorum.isCandidate()) {
            onBecomeCandidate(currentTimeMs);
        } else if (quorum.isFollower()) {
            onBecomeFollower(currentTimeMs);
        }

        // When there is only a single voter, become candidate immediately
        if (quorum.isOnlyVoter() && !quorum.isCandidate()) {
            transitionToCandidate(currentTimeMs);
        }
    }

    @Override
    public void register(Listener<T> listener) {
        pendingRegistrations.add(Registration.register(listener));
        wakeup();
    }

    @Override
    public void unregister(Listener<T> listener) {
        pendingRegistrations.add(Registration.unregister(listener));
        // No need to wake up the polling thread. It is a removal so the updates can be
        // delayed until the polling thread wakes up for other reasons.
    }

    @Override
    public LeaderAndEpoch leaderAndEpoch() {
        if (isInitialized()) {
            return quorum.leaderAndEpoch();
        } else {
            return LeaderAndEpoch.UNKNOWN;
        }
    }

    @Override
    public OptionalInt nodeId() {
        return nodeId;
    }

    private OffsetAndEpoch endOffset() {
        return new OffsetAndEpoch(log.endOffset().offset, log.lastFetchedEpoch());
    }

    private void resetConnections() {
        requestManager.resetAll();
    }

    private void onBecomeLeader(long currentTimeMs) {
        long endOffset = log.endOffset().offset;

        BatchAccumulator<T> accumulator = new BatchAccumulator<>(
            quorum.epoch(),
            endOffset,
            quorumConfig.appendLingerMs(),
            MAX_BATCH_SIZE_BYTES,
            memoryPool,
            time,
            CompressionType.NONE,
            serde
        );

        LeaderState<T> state = quorum.transitionToLeader(endOffset, accumulator);

        log.initializeLeaderEpoch(quorum.epoch());

        // The high watermark can only be advanced once we have written a record
        // from the new leader's epoch. Hence we write a control message immediately
        // to ensure there is no delay committing pending data.
        state.appendLeaderChangeMessage(currentTimeMs);

        resetConnections();
        kafkaRaftMetrics.maybeUpdateElectionLatency(currentTimeMs);
    }

    private void flushLeaderLog(LeaderState<T> state, long currentTimeMs) {
        // We update the end offset before flushing so that parked fetches can return sooner.
        updateLeaderEndOffsetAndTimestamp(state, currentTimeMs);
        log.flush(false);
    }

    private boolean maybeTransitionToLeader(CandidateState state, long currentTimeMs) {
        if (state.isVoteGranted()) {
            onBecomeLeader(currentTimeMs);
            return true;
        } else {
            return false;
        }
    }

    private void onBecomeCandidate(long currentTimeMs) {
        CandidateState state = quorum.candidateStateOrThrow();
        if (!maybeTransitionToLeader(state, currentTimeMs)) {
            resetConnections();
            kafkaRaftMetrics.updateElectionStartMs(currentTimeMs);
        }
    }

    private void transitionToCandidate(long currentTimeMs) {
        quorum.transitionToCandidate();
        maybeFireLeaderChange();
        onBecomeCandidate(currentTimeMs);
    }

    private void transitionToUnattached(int epoch) {
        quorum.transitionToUnattached(epoch);
        maybeFireLeaderChange();
        resetConnections();
    }

    private void transitionToResigned(List<Integer> preferredSuccessors) {
        fetchPurgatory.completeAllExceptionally(
            Errors.NOT_LEADER_OR_FOLLOWER.exception("Not handling request since this node is resigning"));
        quorum.transitionToResigned(preferredSuccessors);
        maybeFireLeaderChange();
        resetConnections();
    }

    private void transitionToVoted(ReplicaKey candidateKey, int epoch) {
        quorum.transitionToVoted(epoch, candidateKey);
        maybeFireLeaderChange();
        resetConnections();
    }

    private void onBecomeFollower(long currentTimeMs) {
        kafkaRaftMetrics.maybeUpdateElectionLatency(currentTimeMs);

        resetConnections();

        // After becoming a follower, we need to complete all pending fetches so that
        // they can be re-sent to the leader without waiting for their expirations
        fetchPurgatory.completeAllExceptionally(new NotLeaderOrFollowerException(
            "Cannot process the fetch request because the node is no longer the leader."));

        // Clearing the append purgatory should complete all futures exceptionally since this node is no longer the leader
        appendPurgatory.completeAllExceptionally(new NotLeaderOrFollowerException(
            "Failed to receive sufficient acknowledgments for this append before leader change."));
    }

    private void transitionToFollower(
        int epoch,
        int leaderId,
        long currentTimeMs
    ) {
        quorum.transitionToFollower(epoch, leaderId);
        maybeFireLeaderChange();
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
     * - {@link Errors#INCONSISTENT_CLUSTER_ID} if the cluster id is presented in request
     *      but different from this node
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#INVALID_REQUEST} if the last epoch or offset are invalid
     */
    private VoteResponseData handleVoteRequest(
        RaftRequest.Inbound requestMetadata
    ) {
        VoteRequestData request = (VoteRequestData) requestMetadata.data;

        if (!hasValidClusterId(request.clusterId())) {
            return new VoteResponseData().setErrorCode(Errors.INCONSISTENT_CLUSTER_ID.code());
        }

        if (!hasValidTopicPartition(request, log.topicPartition())) {
            // Until we support multi-raft, we treat individual topic partition mismatches as invalid requests
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

        OffsetAndEpoch lastEpochEndOffsetAndEpoch = new OffsetAndEpoch(lastEpochEndOffset, lastEpoch);
        ReplicaKey candidateKey = ReplicaKey.of(candidateId, Optional.empty());
        boolean voteGranted = quorum.canGrantVote(
            candidateKey,
            lastEpochEndOffsetAndEpoch.compareTo(endOffset()) >= 0
        );

        if (voteGranted && quorum.isUnattached()) {
            transitionToVoted(candidateKey, candidateEpoch);
        }

        logger.info("Vote request {} with epoch {} is {}", request, candidateEpoch, voteGranted ? "granted" : "rejected");
        return buildVoteResponse(Errors.NONE, voteGranted);
    }

    private boolean handleVoteResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) {
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
                quorumConfig.electionBackoffMaxMs());
    }

    private int strictExponentialElectionBackoffMs(int positionInSuccessors, int totalNumSuccessors) {
        if (positionInSuccessors <= 0 || positionInSuccessors >= totalNumSuccessors) {
            throw new IllegalArgumentException("Position " + positionInSuccessors + " should be larger than zero" +
                    " and smaller than total number of successors " + totalNumSuccessors);
        }

        int retryBackOffBaseMs = quorumConfig.electionBackoffMaxMs() >> (totalNumSuccessors - 1);
        return Math.min(quorumConfig.electionBackoffMaxMs(), retryBackOffBaseMs << (positionInSuccessors - 1));
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
     * - {@link Errors#INCONSISTENT_CLUSTER_ID} if the cluster id is presented in request
     *      but different from this node
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     */
    private BeginQuorumEpochResponseData handleBeginQuorumEpochRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) {
        BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) requestMetadata.data;

        if (!hasValidClusterId(request.clusterId())) {
            return new BeginQuorumEpochResponseData().setErrorCode(Errors.INCONSISTENT_CLUSTER_ID.code());
        }

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
    ) {
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
                LeaderState<T> state = quorum.leaderStateOrThrow();
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
     * - {@link Errors#INCONSISTENT_CLUSTER_ID} if the cluster id is presented in request
     *      but different from this node
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     */
    private EndQuorumEpochResponseData handleEndQuorumEpochRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) {
        EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) requestMetadata.data;

        if (!hasValidClusterId(request.clusterId())) {
            return new EndQuorumEpochResponseData().setErrorCode(Errors.INCONSISTENT_CLUSTER_ID.code());
        }

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
    ) {
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
        ValidOffsetAndEpoch validOffsetAndEpoch,
        Optional<LogOffsetMetadata> highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(log.topicPartition(), log.topicId(), Errors.NONE, partitionData -> {
            partitionData
                .setRecords(records)
                .setErrorCode(error.code())
                .setLogStartOffset(log.startOffset())
                .setHighWatermark(
                    highWatermark.map(offsetMetadata -> offsetMetadata.offset).orElse(-1L)
                );

            partitionData.currentLeader()
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrSentinel());

            switch (validOffsetAndEpoch.kind()) {
                case DIVERGING:
                    partitionData.divergingEpoch()
                        .setEpoch(validOffsetAndEpoch.offsetAndEpoch().epoch())
                        .setEndOffset(validOffsetAndEpoch.offsetAndEpoch().offset());
                    break;
                case SNAPSHOT:
                    partitionData.snapshotId()
                        .setEpoch(validOffsetAndEpoch.offsetAndEpoch().epoch())
                        .setEndOffset(validOffsetAndEpoch.offsetAndEpoch().offset());
                    break;
                default:
            }
        });
    }

    private FetchResponseData buildEmptyFetchResponse(
        Errors error,
        Optional<LogOffsetMetadata> highWatermark
    ) {
        return buildFetchResponse(
            error,
            MemoryRecords.EMPTY,
            ValidOffsetAndEpoch.valid(),
            highWatermark
        );
    }

    private boolean hasValidClusterId(String requestClusterId) {
        // We don't enforce the cluster id if it is not provided.
        if (requestClusterId == null) {
            return true;
        }
        return clusterId.equals(requestClusterId);
    }

    /**
     * Handle a Fetch request. The fetch offset and last fetched epoch are always
     * validated against the current log. In the case that they do not match, the response will
     * indicate the diverging offset/epoch. A follower is expected to truncate its log in this
     * case and resend the fetch.
     *
     * This API may return the following errors:
     *
     * - {@link Errors#INCONSISTENT_CLUSTER_ID} if the cluster id is presented in request
     *     but different from this node
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

        if (!hasValidClusterId(request.clusterId())) {
            return completedFuture(new FetchResponseData().setErrorCode(Errors.INCONSISTENT_CLUSTER_ID.code()));
        }

        if (!hasValidTopicPartition(request, log.topicPartition(), log.topicId())) {
            // Until we support multi-raft, we treat topic partition mismatches as invalid requests
            return completedFuture(new FetchResponseData().setErrorCode(Errors.INVALID_REQUEST.code()));
        }
        // If the ID is valid, we can set the topic name.
        request.topics().get(0).setTopic(log.topicPartition().topic());

        FetchRequestData.FetchPartition fetchPartition = request.topics().get(0).partitions().get(0);
        if (request.maxWaitMs() < 0
            || fetchPartition.fetchOffset() < 0
            || fetchPartition.lastFetchedEpoch() < 0
            || fetchPartition.lastFetchedEpoch() > fetchPartition.currentLeaderEpoch()) {
            return completedFuture(
                buildEmptyFetchResponse(Errors.INVALID_REQUEST, Optional.empty())
            );
        }

        int replicaId = FetchRequest.replicaId(request);
        FetchResponseData response = tryCompleteFetchRequest(replicaId, fetchPartition, currentTimeMs);
        FetchResponseData.PartitionData partitionResponse =
            response.responses().get(0).partitions().get(0);

        if (partitionResponse.errorCode() != Errors.NONE.code()
            || FetchResponse.recordsSize(partitionResponse) > 0
            || request.maxWaitMs() == 0
            || isPartitionDiverged(partitionResponse)
            || isPartitionSnapshotted(partitionResponse)) {
            // Reply immediately if any of the following is true
            // 1. The response contains an error
            // 2. There are records in the response
            // 3. The fetching replica doesn't want to wait for the partition to contain new data
            // 4. The fetching replica needs to truncate because the log diverged
            // 5. The fetching replica needs to fetch a snapshot
            return completedFuture(response);
        }

        CompletableFuture<Long> future = fetchPurgatory.await(
            fetchPartition.fetchOffset(),
            request.maxWaitMs());

        return future.handle((completionTimeMs, exception) -> {
            if (exception != null) {
                Throwable cause = exception instanceof ExecutionException ?
                    exception.getCause() : exception;

                Errors error = Errors.forException(cause);
                if (error == Errors.REQUEST_TIMED_OUT) {
                    // Note that for this case the calling thread is the expiration service thread and not the
                    // polling thread.
                    //
                    // If the fetch request timed out in purgatory, it means no new data is available,
                    // just return the original fetch response.
                    return response;
                } else {
                    // If there was any error other than REQUEST_TIMED_OUT, return it.
                    logger.info("Failed to handle fetch from {} at {} due to {}",
                        replicaId, fetchPartition.fetchOffset(), error);
                    return buildEmptyFetchResponse(error, Optional.empty());
                }
            }

            // FIXME: `completionTimeMs`, which can be null
            logger.trace("Completing delayed fetch from {} starting at offset {} at {}",
                replicaId, fetchPartition.fetchOffset(), completionTimeMs);

            // It is safe to call tryCompleteFetchRequest because only the polling thread completes this
            // future successfully. This is true because only the polling thread appends record batches to
            // the log from maybeAppendBatches.
            return tryCompleteFetchRequest(replicaId, fetchPartition, time.milliseconds());
        });
    }

    private FetchResponseData tryCompleteFetchRequest(
        int replicaId,
        FetchRequestData.FetchPartition request,
        long currentTimeMs
    ) {
        try {
            Optional<Errors> errorOpt = validateLeaderOnlyRequest(request.currentLeaderEpoch());
            if (errorOpt.isPresent()) {
                return buildEmptyFetchResponse(errorOpt.get(), Optional.empty());
            }

            long fetchOffset = request.fetchOffset();
            int lastFetchedEpoch = request.lastFetchedEpoch();
            LeaderState<T> state = quorum.leaderStateOrThrow();

            Optional<OffsetAndEpoch> latestSnapshotId = log.latestSnapshotId();
            final ValidOffsetAndEpoch validOffsetAndEpoch;
            if (fetchOffset == 0 && latestSnapshotId.isPresent()) {
                // If the follower has an empty log and a snapshot exist, it is always more efficient
                // to reply with a snapshot id (FETCH_SNAPSHOT) instead of fetching from the log segments.
                validOffsetAndEpoch = ValidOffsetAndEpoch.snapshot(latestSnapshotId.get());
            } else {
                validOffsetAndEpoch = log.validateOffsetAndEpoch(fetchOffset, lastFetchedEpoch);
            }

            final Records records;
            if (validOffsetAndEpoch.kind() == ValidOffsetAndEpoch.Kind.VALID) {
                LogFetchInfo info = log.read(fetchOffset, Isolation.UNCOMMITTED);

                if (state.updateReplicaState(replicaId, currentTimeMs, info.startOffsetMetadata)) {
                    onUpdateLeaderHighWatermark(state, currentTimeMs);
                }

                records = info.records;
            } else {
                records = MemoryRecords.EMPTY;
            }

            return buildFetchResponse(Errors.NONE, records, validOffsetAndEpoch, state.highWatermark());
        } catch (Exception e) {
            logger.error("Caught unexpected error in fetch completion of request {}", request, e);
            return buildEmptyFetchResponse(Errors.UNKNOWN_SERVER_ERROR, Optional.empty());
        }
    }

    private static boolean isPartitionDiverged(FetchResponseData.PartitionData partitionResponseData) {
        FetchResponseData.EpochEndOffset divergingEpoch = partitionResponseData.divergingEpoch();

        return divergingEpoch.epoch() != -1 || divergingEpoch.endOffset() != -1;
    }

    private static boolean isPartitionSnapshotted(FetchResponseData.PartitionData partitionResponseData) {
        FetchResponseData.SnapshotId snapshotId = partitionResponseData.snapshotId();

        return snapshotId.epoch() != -1 || snapshotId.endOffset() != -1;
    }

    private static OptionalInt optionalLeaderId(int leaderIdOrNil) {
        if (leaderIdOrNil < 0)
            return OptionalInt.empty();
        return OptionalInt.of(leaderIdOrNil);
    }

    private static String listenerName(Listener<?> listener) {
        return String.format("%s@%d", listener.getClass().getTypeName(), System.identityHashCode(listener));
    }

    private boolean handleFetchResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) {
        FetchResponseData response = (FetchResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (!RaftUtil.hasValidTopicPartition(response, log.topicPartition(), log.topicId())) {
            return false;
        }
        // If the ID is valid, we can set the topic name.
        response.responses().get(0).setTopic(log.topicPartition().topic());

        FetchResponseData.PartitionData partitionResponse =
            response.responses().get(0).partitions().get(0);

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
                final OffsetAndEpoch divergingOffsetAndEpoch = new OffsetAndEpoch(
                    divergingEpoch.endOffset(), divergingEpoch.epoch());

                state.highWatermark().ifPresent(highWatermark -> {
                    if (divergingOffsetAndEpoch.offset() < highWatermark.offset) {
                        throw new KafkaException("The leader requested truncation to offset " +
                            divergingOffsetAndEpoch.offset() + ", which is below the current high watermark" +
                            " " + highWatermark);
                    }
                });

                long truncationOffset = log.truncateToEndOffset(divergingOffsetAndEpoch);
                logger.info(
                    "Truncated to offset {} from Fetch response from leader {}",
                    truncationOffset,
                    quorum.leaderIdOrSentinel()
                );

                // Update the internal listener to the new end offset
                partitionState.truncateNewEntries(truncationOffset);
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
                    final OffsetAndEpoch snapshotId = new OffsetAndEpoch(
                        partitionResponse.snapshotId().endOffset(),
                        partitionResponse.snapshotId().epoch()
                    );

                    // Do not validate the snapshot id against the local replicated log since this
                    // snapshot is expected to reference offsets and epochs greater than the log
                    // end offset and high-watermark.
                    state.setFetchingSnapshot(log.createNewSnapshotUnchecked(snapshotId));
                    logger.info(
                        "Fetching snapshot {} from Fetch response from leader {}",
                        snapshotId,
                        quorum.leaderIdOrSentinel()
                    );
                }
            } else {
                Records records = FetchResponse.recordsOrFail(partitionResponse);
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
        if (quorum.isVoter()) {
            // the leader only requires that voters have flushed their log before sending
            // a Fetch request
            log.flush(false);
        }

        partitionState.updateState();

        OffsetAndEpoch endOffset = endOffset();
        kafkaRaftMetrics.updateFetchedRecords(info.lastOffset - info.firstOffset + 1);
        kafkaRaftMetrics.updateLogEnd(endOffset);
        logger.trace("Follower end offset updated to {} after append", endOffset);
    }

    private LogAppendInfo appendAsLeader(
        Records records
    ) {
        LogAppendInfo info = log.appendAsLeader(records, quorum.epoch());

        partitionState.updateState();

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
            return DescribeQuorumResponse.singletonErrorResponse(
                log.topicPartition(),
                Errors.NOT_LEADER_OR_FOLLOWER
            );
        }

        LeaderState<T> leaderState = quorum.leaderStateOrThrow();
        return DescribeQuorumResponse.singletonResponse(
            log.topicPartition(),
            leaderState.describeQuorum(currentTimeMs)
        );
    }

    /**
     * Handle a FetchSnapshot request, similar to the Fetch request but we use {@link UnalignedRecords}
     * in response because the records are not necessarily offset-aligned.
     *
     * This API may return the following errors:
     *
     * - {@link Errors#INCONSISTENT_CLUSTER_ID} if the cluster id is presented in request
     *     but different from this node
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     * - {@link Errors#INVALID_REQUEST} if the request epoch is larger than the leader's current epoch
     *     or if either the fetch offset or the last fetched epoch is invalid
     * - {@link Errors#SNAPSHOT_NOT_FOUND} if the request snapshot id does not exists
     * - {@link Errors#POSITION_OUT_OF_RANGE} if the request snapshot offset out of range
     */
    private FetchSnapshotResponseData handleFetchSnapshotRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) {
        FetchSnapshotRequestData data = (FetchSnapshotRequestData) requestMetadata.data;

        if (!hasValidClusterId(data.clusterId())) {
            return new FetchSnapshotResponseData().setErrorCode(Errors.INCONSISTENT_CLUSTER_ID.code());
        }

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
                responsePartitionSnapshot -> responsePartitionSnapshot
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
            );
        }

        FetchSnapshotRequestData.PartitionSnapshot partitionSnapshot = partitionSnapshotOpt.get();
        Optional<Errors> leaderValidation = validateLeaderOnlyRequest(
                partitionSnapshot.currentLeaderEpoch()
        );
        if (leaderValidation.isPresent()) {
            return FetchSnapshotResponse.singleton(
                log.topicPartition(),
                responsePartitionSnapshot -> addQuorumLeader(responsePartitionSnapshot)
                    .setErrorCode(leaderValidation.get().code())
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
                responsePartitionSnapshot -> addQuorumLeader(responsePartitionSnapshot)
                    .setErrorCode(Errors.SNAPSHOT_NOT_FOUND.code())
            );
        }

        RawSnapshotReader snapshot = snapshotOpt.get();
        long snapshotSize = snapshot.sizeInBytes();
        if (partitionSnapshot.position() < 0 || partitionSnapshot.position() >= snapshotSize) {
            return FetchSnapshotResponse.singleton(
                log.topicPartition(),
                responsePartitionSnapshot -> addQuorumLeader(responsePartitionSnapshot)
                    .setErrorCode(Errors.POSITION_OUT_OF_RANGE.code())
            );
        }

        if (partitionSnapshot.position() > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                String.format(
                    "Trying to fetch a snapshot with size (%d) and a position (%d) larger than %d",
                    snapshotSize,
                    partitionSnapshot.position(),
                    Integer.MAX_VALUE
                )
            );
        }

        int maxSnapshotSize;
        try {
            maxSnapshotSize = Math.toIntExact(snapshotSize);
        } catch (ArithmeticException e) {
            maxSnapshotSize = Integer.MAX_VALUE;
        }

        UnalignedRecords records = snapshot.slice(partitionSnapshot.position(), Math.min(data.maxBytes(), maxSnapshotSize));

        LeaderState<T> state = quorum.leaderStateOrThrow();
        state.updateCheckQuorumForFollowingVoter(data.replicaId(), currentTimeMs);

        return FetchSnapshotResponse.singleton(
            log.topicPartition(),
            responsePartitionSnapshot -> {
                addQuorumLeader(responsePartitionSnapshot)
                    .snapshotId()
                    .setEndOffset(snapshotId.offset())
                    .setEpoch(snapshotId.epoch());

                return responsePartitionSnapshot
                    .setSize(snapshotSize)
                    .setPosition(partitionSnapshot.position())
                    .setUnalignedRecords(records);
            }
        );
    }

    private boolean handleFetchSnapshotResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) {
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
             * resetting the fetching snapshot state and sending another fetch request.
             */
            logger.info(
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
            throw new IllegalStateException(
                String.format("Received unexpected fetch snapshot response: %s", partitionSnapshot)
            );
        }

        if (!snapshot.snapshotId().equals(snapshotId)) {
            throw new IllegalStateException(
                String.format(
                    "Received fetch snapshot response with an invalid id. Expected %s; Received %s",
                    snapshot.snapshotId(),
                    snapshotId
                )
            );
        }
        if (snapshot.sizeInBytes() != partitionSnapshot.position()) {
            throw new IllegalStateException(
                String.format(
                    "Received fetch snapshot response with an invalid position. Expected %d; Received %d",
                    snapshot.sizeInBytes(),
                    partitionSnapshot.position()
                )
            );
        }

        final UnalignedMemoryRecords records;
        if (partitionSnapshot.unalignedRecords() instanceof MemoryRecords) {
            records = new UnalignedMemoryRecords(((MemoryRecords) partitionSnapshot.unalignedRecords()).buffer());
        } else if (partitionSnapshot.unalignedRecords() instanceof UnalignedMemoryRecords) {
            records = (UnalignedMemoryRecords) partitionSnapshot.unalignedRecords();
        } else {
            throw new IllegalStateException(String.format("Received unexpected fetch snapshot response: %s", partitionSnapshot));
        }
        snapshot.append(records);

        if (snapshot.sizeInBytes() == partitionSnapshot.size()) {
            // Finished fetching the snapshot.
            snapshot.freeze();
            state.setFetchingSnapshot(Optional.empty());

            if (log.truncateToLatestSnapshot()) {
                logger.info(
                    "Fully truncated the log at ({}, {}) after downloading snapshot {} from leader {}",
                    log.endOffset(),
                    log.lastFetchedEpoch(),
                    snapshot.snapshotId(),
                    quorum.leaderIdOrSentinel()
                );

                // This will always reload the snapshot because the internal next offset
                // is always less than the snapshot id just downloaded.
                partitionState.updateState();

                updateFollowerHighWatermark(state, OptionalLong.of(log.highWatermark().offset));
            } else {
                throw new IllegalStateException(
                    String.format(
                        "Full log truncation expected but didn't happen. Snapshot of %s, log end offset %s, last fetched %d",
                        snapshot.snapshotId(),
                        log.endOffset(),
                        log.lastFetchedEpoch()
                    )
                );
            }
        }

        state.resetFetchTimeout(currentTimeMs);
        return true;
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
    ) {
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
    ) {
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

    private void handleResponse(RaftResponse.Inbound response, long currentTimeMs) {
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
     * Validate common state for requests to establish leadership.
     *
     * These include the Vote, BeginQuorumEpoch and EndQuorumEpoch RPCs. If an error is present in
     * the returned value, it should be returned in the response.
     */
    private Optional<Errors> validateVoterOnlyRequest(int remoteNodeId, int requestEpoch) {
        if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH);
        } else if (remoteNodeId < 0) {
            return Optional.of(Errors.INVALID_REQUEST);
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

    private void handleRequest(RaftRequest.Inbound request, long currentTimeMs) {
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
                responseFuture = completedFuture(handleFetchSnapshotRequest(request, currentTimeMs));
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

    private void handleInboundMessage(RaftMessage message, long currentTimeMs) {
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
            clusterId,
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
            clusterId,
            quorum.epoch(),
            quorum.localIdOrThrow()
        );
    }

    private VoteRequestData buildVoteRequest() {
        OffsetAndEpoch endOffset = endOffset();
        return VoteRequest.singletonRequest(
            log.topicPartition(),
            clusterId,
            quorum.epoch(),
            quorum.localIdOrThrow(),
            endOffset.epoch(),
            endOffset.offset()
        );
    }

    private FetchRequestData buildFetchRequest() {
        FetchRequestData request = RaftUtil.singletonFetchRequest(log.topicPartition(), log.topicId(), fetchPartition -> {
            fetchPartition
                .setCurrentLeaderEpoch(quorum.epoch())
                .setLastFetchedEpoch(log.lastFetchedEpoch())
                .setFetchOffset(log.endOffset().offset);
        });
        return request
            .setMaxBytes(MAX_FETCH_SIZE_BYTES)
            .setMaxWaitMs(fetchMaxWaitMs)
            .setClusterId(clusterId)
            .setReplicaState(new FetchRequestData.ReplicaState().setReplicaId(quorum.localIdOrSentinel()));
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
            .setEpoch(snapshotId.epoch())
            .setEndOffset(snapshotId.offset());

        FetchSnapshotRequestData request = FetchSnapshotRequest.singleton(
            clusterId,
            quorum().localIdOrSentinel(),
            log.topicPartition(),
            snapshotPartition -> snapshotPartition
                .setCurrentLeaderEpoch(quorum.epoch())
                .setSnapshotId(requestSnapshotId)
                .setPosition(snapshotSize)
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
        LeaderState<T> state,
        BatchAccumulator.CompletedBatch<T> batch,
        long appendTimeMs
    ) {
        try {
            int epoch = state.epoch();
            LogAppendInfo info = appendAsLeader(batch.data);
            OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(info.lastOffset, epoch);
            CompletableFuture<Long> future = appendPurgatory.await(
                offsetAndEpoch.offset() + 1, Integer.MAX_VALUE);

            future.whenComplete((commitTimeMs, exception) -> {
                if (exception != null) {
                    logger.debug("Failed to commit {} records up to last offset {}", batch.numRecords, offsetAndEpoch, exception);
                } else {
                    long elapsedTime = Math.max(0, commitTimeMs - appendTimeMs);
                    double elapsedTimePerRecord = (double) elapsedTime / batch.numRecords;
                    kafkaRaftMetrics.updateCommitLatency(elapsedTimePerRecord, appendTimeMs);
                    logger.debug("Completed commit of {} records up to last offset {}", batch.numRecords, offsetAndEpoch);
                    batch.records.ifPresent(records -> {
                        maybeFireHandleCommit(batch.baseOffset, epoch, batch.appendTimestamp(), batch.sizeInBytes(), records);
                    });
                }
            });
        } finally {
            batch.release();
        }
    }

    private long maybeAppendBatches(
        LeaderState<T> state,
        long currentTimeMs
    ) {
        long timeUntilDrain = state.accumulator().timeUntilDrain(currentTimeMs);
        if (timeUntilDrain <= 0) {
            List<BatchAccumulator.CompletedBatch<T>> batches = state.accumulator().drain();
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
        return timeUntilDrain;
    }

    private long pollResigned(long currentTimeMs) {
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
        LeaderState<T> state = quorum.leaderStateOrThrow();
        maybeFireLeaderChange(state);

        long timeUntilCheckQuorumExpires = state.timeUntilCheckQuorumExpires(currentTimeMs);
        if (shutdown.get() != null || state.isResignRequested() || timeUntilCheckQuorumExpires == 0) {
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

        return Math.min(timeUntilFlush, Math.min(timeUntilSend, timeUntilCheckQuorumExpires));
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

    private long pollCandidate(long currentTimeMs) {
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
            logger.info("Election has timed out, backing off for {}ms before becoming a candidate again",
                backoffDurationMs);
            state.startBackingOff(currentTimeMs, backoffDurationMs);
            return backoffDurationMs;
        } else {
            long minRequestBackoffMs = maybeSendVoteRequests(state, currentTimeMs);
            return Math.min(minRequestBackoffMs, state.remainingElectionTimeMs(currentTimeMs));
        }
    }

    private long pollFollower(long currentTimeMs) {
        FollowerState state = quorum.followerStateOrThrow();
        if (quorum.isVoter()) {
            return pollFollowerAsVoter(state, currentTimeMs);
        } else {
            return pollFollowerAsObserver(state, currentTimeMs);
        }
    }

    private long pollFollowerAsVoter(FollowerState state, long currentTimeMs) {
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

    private long pollFollowerAsObserver(FollowerState state, long currentTimeMs) {
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

    private long maybeSendFetchOrFetchSnapshot(FollowerState state, long currentTimeMs) {
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

    private long pollVoted(long currentTimeMs) {
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

    private long pollUnattached(long currentTimeMs) {
        UnattachedState state = quorum.unattachedStateOrThrow();
        if (quorum.isVoter()) {
            return pollUnattachedAsVoter(state, currentTimeMs);
        } else {
            return pollUnattachedAsObserver(state, currentTimeMs);
        }
    }

    private long pollUnattachedAsVoter(UnattachedState state, long currentTimeMs) {
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

    private long pollCurrentState(long currentTimeMs) {
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
        // Apply all of the pending registration
        while (true) {
            Registration<T> registration = pendingRegistrations.poll();
            if (registration == null) {
                break;
            }

            processRegistration(registration);
        }

        // Check listener progress to see if reads are expected
        quorum.highWatermark().ifPresent(highWatermarkMetadata -> {
            updateListenersProgress(highWatermarkMetadata.offset);
        });

        // Notify the new listeners of the latest leader and epoch
        Optional<LeaderState<T>> leaderState = quorum.maybeLeaderState();
        if (leaderState.isPresent()) {
            maybeFireLeaderChange(leaderState.get());
        } else {
            maybeFireLeaderChange();
        }
    }

    private void processRegistration(Registration<T> registration) {
        Listener<T> listener = registration.listener();
        Registration.Ops ops = registration.ops();

        if (ops == Registration.Ops.REGISTER) {
            if (listenerContexts.putIfAbsent(listener, new ListenerContext(listener)) != null) {
                logger.error("Attempting to add a listener that already exists: {}", listenerName(listener));
            } else {
                logger.info("Registered the listener {}", listenerName(listener));
            }
        } else {
            if (listenerContexts.remove(listener) == null) {
                logger.error("Attempting to remove a listener that doesn't exists: {}", listenerName(listener));
            } else {
                logger.info("Unregistered the listener {}", listenerName(listener));
            }
        }
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
            || quorum.isOnlyVoter()
            || quorum.hasRemoteLeader()
        ) {
            shutdown.complete();
            return true;
        }

        return false;
    }

    /**
     * A simple timer based log cleaner
     */
    private static class RaftMetadataLogCleanerManager {
        private final Logger logger;
        private final Timer timer;
        private final long delayMs;
        private final Runnable cleaner;

        RaftMetadataLogCleanerManager(Logger logger, Time time, long delayMs, Runnable cleaner) {
            this.logger = logger;
            this.timer = time.timer(delayMs);
            this.delayMs = delayMs;
            this.cleaner = cleaner;
        }

        public long maybeClean(long currentTimeMs) {
            timer.update(currentTimeMs);
            if (timer.isExpired()) {
                try {
                    cleaner.run();
                } catch (Throwable t) {
                    logger.error("Had an error during log cleaning", t);
                }
                timer.reset(delayMs);
            }
            return timer.remainingMs();
        }
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
     */
    public void poll() {
        if (!isInitialized()) {
            throw new IllegalStateException("Replica needs to be initialized before polling");
        }

        long startPollTimeMs = time.milliseconds();
        if (maybeCompleteShutdown(startPollTimeMs)) {
            return;
        }

        long pollStateTimeoutMs = pollCurrentState(startPollTimeMs);
        long cleaningTimeoutMs = snapshotCleaner.maybeClean(startPollTimeMs);
        long pollTimeoutMs = Math.min(pollStateTimeoutMs, cleaningTimeoutMs);

        long startWaitTimeMs = time.milliseconds();
        kafkaRaftMetrics.updatePollStart(startWaitTimeMs);

        RaftMessage message = messageQueue.poll(pollTimeoutMs);

        long endWaitTimeMs = time.milliseconds();
        kafkaRaftMetrics.updatePollEnd(endWaitTimeMs);

        if (message != null) {
            handleInboundMessage(message, endWaitTimeMs);
        }

        pollListeners();
    }

    @Override
    public long scheduleAppend(int epoch, List<T> records) {
        return append(epoch, records, OptionalLong.empty(), false);
    }

    @Override
    public long scheduleAtomicAppend(int epoch, OptionalLong requiredBaseOffset, List<T> records) {
        return append(epoch, records, requiredBaseOffset, true);
    }

    private long append(int epoch, List<T> records, OptionalLong requiredBaseOffset, boolean isAtomic) {
        if (!isInitialized()) {
            throw new NotLeaderException("Append failed because the replica is not the current leader");
        }

        LeaderState<T> leaderState = quorum.<T>maybeLeaderState().orElseThrow(
            () -> new NotLeaderException("Append failed because the replica is not the current leader")
        );

        BatchAccumulator<T> accumulator = leaderState.accumulator();
        boolean isFirstAppend = accumulator.isEmpty();
        final long offset = accumulator.append(epoch, records, requiredBaseOffset, isAtomic);

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
    public void resign(int epoch) {
        if (epoch < 0) {
            throw new IllegalArgumentException("Attempt to resign from an invalid negative epoch " + epoch);
        } else if (!isInitialized()) {
            throw new IllegalStateException("Replica needs to be initialized before resigning");
        } else if (!quorum.isVoter()) {
            throw new IllegalStateException("Attempt to resign by a non-voter");
        }

        LeaderAndEpoch leaderAndEpoch = leaderAndEpoch();
        int currentEpoch = leaderAndEpoch.epoch();

        if (epoch > currentEpoch) {
            throw new IllegalArgumentException("Attempt to resign from epoch " + epoch +
                " which is larger than the current epoch " + currentEpoch);
        } else if (epoch < currentEpoch) {
            // If the passed epoch is smaller than the current epoch, then it might mean
            // that the listener has not been notified about a leader change that already
            // took place. In this case, we consider the call as already fulfilled and
            // take no further action.
            logger.debug("Ignoring call to resign from epoch {} since it is smaller than the " +
                "current epoch {}", epoch, currentEpoch);
        } else if (!leaderAndEpoch.isLeader(quorum.localIdOrThrow())) {
            throw new IllegalArgumentException("Cannot resign from epoch " + epoch +
                " since we are not the leader");
        } else {
            // Note that if we transition to another state before we have a chance to
            // request resignation, then we consider the call fulfilled.
            Optional<LeaderState<Object>> leaderStateOpt = quorum.maybeLeaderState();
            if (!leaderStateOpt.isPresent()) {
                logger.debug("Ignoring call to resign from epoch {} since this node is " +
                    "no longer the leader", epoch);
                return;
            }

            LeaderState<Object> leaderState = leaderStateOpt.get();
            if (leaderState.epoch() != epoch) {
                logger.debug("Ignoring call to resign from epoch {} since it is smaller than the " +
                    "current epoch {}", epoch, leaderState.epoch());
            } else {
                logger.info("Received user request to resign from the current epoch {}", currentEpoch);
                leaderState.requestResign();
                wakeup();
            }
        }
    }

    @Override
    public Optional<SnapshotWriter<T>> createSnapshot(
        OffsetAndEpoch snapshotId,
        long lastContainedLogTimestamp
    ) {
        if (!isInitialized()) {
            throw new IllegalStateException("Cannot create snapshot before the replica has been initialized");
        }

        return log.createNewSnapshot(snapshotId).map(writer -> {
            long lastContainedLogOffset = snapshotId.offset() - 1;

            RawSnapshotWriter wrappedWriter = new NotifyingRawSnapshotWriter(writer, offsetAndEpoch -> {
                // Trim the state in the internal listener up to the new snapshot
                partitionState.truncateOldEntries(offsetAndEpoch.offset());
            });

            return new RecordsSnapshotWriter.Builder()
                .setLastContainedLogTimestamp(lastContainedLogTimestamp)
                .setTime(time)
                .setMaxBatchSize(MAX_BATCH_SIZE_BYTES)
                .setMemoryPool(memoryPool)
                .setRawSnapshotWriter(wrappedWriter)
                .setKraftVersion(partitionState.kraftVersionAtOffset(lastContainedLogOffset))
                .setVoterSet(partitionState.voterSetAtOffset(lastContainedLogOffset))
                .build(serde);
        });
    }

    @Override
    public Optional<OffsetAndEpoch> latestSnapshotId() {
        return log.latestSnapshotId();
    }

    @Override
    public long logEndOffset() {
        return log.endOffset().offset;
    }

    @Override
    public void close() {
        log.flush(true);
        if (kafkaRaftMetrics != null) {
            kafkaRaftMetrics.close();
        }
        if (memoryPool instanceof BatchMemoryPool) {
            BatchMemoryPool batchMemoryPool = (BatchMemoryPool) memoryPool;
            batchMemoryPool.releaseRetained();
        }
    }

    @Override
    public OptionalLong highWatermark() {
        if (isInitialized() && quorum.highWatermark().isPresent()) {
            return OptionalLong.of(quorum.highWatermark().get().offset);
        } else {
            return OptionalLong.empty();
        }
    }

    // Visible only for test
    QuorumState quorum() {
        // It's okay to return null since this method is only called by tests
        return quorum;
    }

    private boolean isInitialized() {
        return partitionState != null && quorum != null && requestManager != null && kafkaRaftMetrics != null;
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

    private static final class Registration<T> {
        private final Ops ops;
        private final Listener<T> listener;

        private Registration(Ops ops, Listener<T> listener) {
            this.ops = ops;
            this.listener = listener;
        }

        private Ops ops() {
            return ops;
        }

        private Listener<T> listener() {
            return listener;
        }

        private enum Ops {
            REGISTER, UNREGISTER
        }

        private static <T> Registration<T> register(Listener<T> listener) {
            return new Registration<>(Ops.REGISTER, listener);
        }

        private static <T> Registration<T> unregister(Listener<T> listener) {
            return new Registration<>(Ops.UNREGISTER, listener);
        }
    }

    private final class ListenerContext implements CloseListener<BatchReader<T>> {
        private final RaftClient.Listener<T> listener;
        // This field is used only by the Raft IO thread
        private LeaderAndEpoch lastFiredLeaderChange = LeaderAndEpoch.UNKNOWN;

        // These fields are visible to both the Raft IO thread and the listener
        // and are protected through synchronization on this ListenerContext instance
        private BatchReader<T> lastSent = null;
        private long nextOffset = 0;

        private ListenerContext(Listener<T> listener) {
            this.listener = listener;
        }

        /**
         * Get the last acked offset, which is one greater than the offset of the
         * last record which was acked by the state machine.
         */
        private synchronized long nextOffset() {
            return nextOffset;
        }

        /**
         * Get the next expected offset, which might be larger than the last acked
         * offset if there are inflight batches which have not been acked yet.
         * Note that when fetching from disk, we may not know the last offset of
         * inflight data until it has been processed by the state machine. In this case,
         * we delay sending additional data until the state machine has read to the
         * end and the last offset is determined.
         */
        private synchronized OptionalLong nextExpectedOffset() {
            if (lastSent != null) {
                OptionalLong lastSentOffset = lastSent.lastOffset();
                if (lastSentOffset.isPresent()) {
                    return OptionalLong.of(lastSentOffset.getAsLong() + 1);
                } else {
                    return OptionalLong.empty();
                }
            } else {
                return OptionalLong.of(nextOffset);
            }
        }

        /**
         * This API is used when the Listener needs to be notified of a new snapshot. This happens
         * when the context's next offset is less than the log start offset.
         */
        private void fireHandleSnapshot(SnapshotReader<T> reader) {
            synchronized (this) {
                nextOffset = reader.snapshotId().offset();
                lastSent = null;
            }

            logger.debug("Notifying listener {} of snapshot {}", listenerName(), reader.snapshotId());
            listener.handleLoadSnapshot(reader);
        }

        /**
         * This API is used for committed records that have been received through
         * replication. In general, followers will write new data to disk before they
         * know whether it has been committed. Rather than retaining the uncommitted
         * data in memory, we let the state machine read the records from disk.
         */
        private void fireHandleCommit(long baseOffset, Records records) {
            fireHandleCommit(
                RecordsBatchReader.of(
                    baseOffset,
                    records,
                    serde,
                    BufferSupplier.create(),
                    MAX_BATCH_SIZE_BYTES,
                    this,
                    true /* Validate batch CRC*/
                )
            );
        }

        /**
         * This API is used for committed records originating from {@link #scheduleAppend(int, List)}
         * or {@link #scheduleAtomicAppend(int, OptionalLong, List)} on this instance. In this case,
         * we are able to save the original record objects, which saves the need to read them back
         * from disk. This is a nice optimization for the leader which is typically doing more work
         * than all of the * followers.
         */
        private void fireHandleCommit(
            long baseOffset,
            int epoch,
            long appendTimestamp,
            int sizeInBytes,
            List<T> records
        ) {
            Batch<T> batch = Batch.data(baseOffset, epoch, appendTimestamp, sizeInBytes, records);
            MemoryBatchReader<T> reader = MemoryBatchReader.of(Collections.singletonList(batch), this);
            fireHandleCommit(reader);
        }

        private String listenerName() {
            return KafkaRaftClient.listenerName(listener);
        }

        private void fireHandleCommit(BatchReader<T> reader) {
            synchronized (this) {
                this.lastSent = reader;
            }
            logger.debug(
                "Notifying listener {} of batch for baseOffset {} and lastOffset {}",
                listenerName(),
                reader.baseOffset(),
                reader.lastOffset()
            );
            listener.handleCommit(reader);
        }

        private void maybeFireLeaderChange(LeaderAndEpoch leaderAndEpoch) {
            if (shouldFireLeaderChange(leaderAndEpoch)) {
                lastFiredLeaderChange = leaderAndEpoch;
                logger.debug("Notifying listener {} of leader change {}", listenerName(), leaderAndEpoch);
                listener.handleLeaderChange(leaderAndEpoch);
            }
        }

        private boolean shouldFireLeaderChange(LeaderAndEpoch leaderAndEpoch) {
            if (leaderAndEpoch.equals(lastFiredLeaderChange)) {
                return false;
            } else if (leaderAndEpoch.epoch() > lastFiredLeaderChange.epoch()) {
                return true;
            } else {
                return leaderAndEpoch.leaderId().isPresent() &&
                    !lastFiredLeaderChange.leaderId().isPresent();
            }
        }

        private void maybeFireLeaderChange(LeaderAndEpoch leaderAndEpoch, long epochStartOffset) {
            // If this node is becoming the leader, then we can fire `handleLeaderChange` as soon
            // as the listener has caught up to the start of the leader epoch. This guarantees
            // that the state machine has seen the full committed state before it becomes
            // leader and begins writing to the log.
            //
            // Note that the raft client doesn't need to compare nextOffset against the high-watermark
            // to guarantee that the listener has caught up to the high-watermark. This is true because
            // the only way nextOffset can be greater than epochStartOffset is for the leader to have
            // established the new high-watermark (of at least epochStartOffset + 1) and for the listener
            // to have consumed up to that new high-watermark.
            if (shouldFireLeaderChange(leaderAndEpoch) && nextOffset() > epochStartOffset) {
                lastFiredLeaderChange = leaderAndEpoch;
                listener.handleLeaderChange(leaderAndEpoch);
            }
        }

        public synchronized void onClose(BatchReader<T> reader) {
            OptionalLong lastOffset = reader.lastOffset();

            if (lastOffset.isPresent()) {
                nextOffset = lastOffset.getAsLong() + 1;
            }

            if (lastSent == reader) {
                lastSent = null;
                wakeup();
            }
        }
    }
}
