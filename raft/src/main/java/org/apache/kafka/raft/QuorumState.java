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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.KRaftControlRecordStateMachine;
import org.apache.kafka.raft.internals.KafkaRaftMetrics;

import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;

/**
 * This class is responsible for managing the current state of this node and ensuring
 * only valid state transitions. Below we define the possible state transitions and
 * how they are triggered:
 *
 * Resigned transitions to:
 *    Unattached:  After learning of a new election with a higher epoch, or expiration of the election timeout
 *    Follower:    After discovering a leader with a larger epoch
 *
 * Unattached transitions to:
 *    Unattached:  After learning of a new election with a higher epoch or after giving a binding vote
 *    Prospective: After expiration of the election timeout
 *    Follower:    After discovering a leader with an equal or larger epoch
 *
 * Prospective transitions to:
 *    Unattached:  After learning of an election with a higher epoch, or node did not have last
 *                 known leader and loses/times out election
 *    Candidate:   After receiving a majority of PreVotes granted
 *    Follower:    After discovering a leader with a larger epoch, or node had a last known leader
 *                 and loses/times out election
 *
 * Candidate transitions to:
 *    Unattached:  After learning of a new election with a higher epoch
 *    Prospective: After expiration of the election timeout or loss of election
 *    Leader:      After receiving a majority of votes
 *
 * Leader transitions to:
 *    Unattached:  After learning of a new election with a higher epoch
 *    Resigned:    When shutting down gracefully
 *    Follower:    After discovering a leader with a larger epoch
 *
 * Follower transitions to:
 *    Unattached:  After learning of a new election with a higher epoch
 *    Prospective: After expiration of the fetch timeout
 *    Follower:    After discovering a leader with a larger epoch
 *
 * Observers follow a simpler state machine. The Prospective/Candidate/Leader/Resigned
 * states are not possible for observers, so the only transitions that are possible
 * are between Unattached and Follower.
 *
 * Unattached transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Follower:   After discovering a leader with an equal or larger epoch
 *
 * Follower transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Follower:   After discovering a leader with a larger epoch
 *
 */
public class QuorumState {
    private final OptionalInt localId;
    private final Uuid localDirectoryId;
    private final Time time;
    private final Logger log;
    private final QuorumStateStore store;
    private final KRaftControlRecordStateMachine partitionState;
    private final Endpoints localListeners;
    private final SupportedVersionRange localSupportedKRaftVersion;
    private final Random random;
    private final int electionTimeoutMs;
    private final int fetchTimeoutMs;
    private final LogContext logContext;
    private final KafkaRaftMetrics kafkaRaftMetrics;

    private volatile EpochState state;

    public QuorumState(
        OptionalInt localId,
        Uuid localDirectoryId,
        KRaftControlRecordStateMachine partitionState,
        Endpoints localListeners,
        SupportedVersionRange localSupportedKRaftVersion,
        int electionTimeoutMs,
        int fetchTimeoutMs,
        QuorumStateStore store,
        Time time,
        LogContext logContext,
        Random random,
        KafkaRaftMetrics kafkaRaftMetrics
    ) {
        this.localId = localId;
        this.localDirectoryId = localDirectoryId;
        this.partitionState = partitionState;
        this.localListeners = localListeners;
        this.localSupportedKRaftVersion = localSupportedKRaftVersion;
        this.electionTimeoutMs = electionTimeoutMs;
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.store = store;
        this.time = time;
        this.log = logContext.logger(QuorumState.class);
        this.random = random;
        this.logContext = logContext;
        this.kafkaRaftMetrics = kafkaRaftMetrics;
    }

    private ElectionState readElectionState() {
        ElectionState election;
        election = store
            .readElectionState()
            .orElseGet(
                () -> ElectionState.withUnknownLeader(0, partitionState.lastVoterSet().voterIds())
            );

        return election;
    }

    public void initialize(OffsetAndEpoch logEndOffsetAndEpoch) throws IllegalStateException {
        // We initialize in whatever state we were in on shutdown. If we were a leader
        // or candidate, probably an election was held, but we will find out about it
        // when we send Vote or BeginEpoch requests.
        ElectionState election = readElectionState();

        final EpochState initialState;
        if (election.hasVoted() && localId.isEmpty()) {
            throw new IllegalStateException(
                String.format(
                    "Initialized quorum state (%s) with a voted candidate but without a local id",
                    election
                )
            );
        } else if (election.epoch() < logEndOffsetAndEpoch.epoch()) {
            log.warn(
                "Epoch from quorum store file ({}) is {}, which is smaller than last written " +
                "epoch {} in the log",
                store.path(),
                election.epoch(),
                logEndOffsetAndEpoch.epoch()
            );
            initialState = new UnattachedState(
                time,
                logEndOffsetAndEpoch.epoch(),
                OptionalInt.empty(),
                Optional.empty(),
                partitionState.lastVoterSet().voterIds(),
                Optional.empty(),
                randomElectionTimeoutMs(),
                logContext
            );
        } else if (localId.isPresent() && election.isLeader(localId.getAsInt())) {
            // If we were previously a leader, then we will start out as resigned
            // in the same epoch. This serves two purposes:
            // 1. It ensures that we cannot vote for another leader in the same epoch.
            // 2. It protects the invariant that each record is uniquely identified by
            //    offset and epoch, which might otherwise be violated if unflushed data
            //    is lost after restarting.
            initialState = new ResignedState(
                time,
                localId.getAsInt(),
                election.epoch(),
                partitionState.lastVoterSet().voterIds(),
                randomElectionTimeoutMs(),
                Collections.emptyList(),
                localListeners,
                logContext
            );
        } else if (
            localId.isPresent() &&
            election.isVotedCandidate(ReplicaKey.of(localId.getAsInt(), localDirectoryId))
        ) {
            initialState = new CandidateState(
                time,
                localId.getAsInt(),
                localDirectoryId,
                election.epoch(),
                partitionState.lastVoterSet(),
                Optional.empty(),
                1,
                randomElectionTimeoutMs(),
                logContext
            );
        } else if (election.hasLeader()) {
            VoterSet voters = partitionState.lastVoterSet();
            Endpoints leaderEndpoints = voters.listeners(election.leaderId());
            if (leaderEndpoints.isEmpty()) {
                // Since the leader's endpoints are not known, it cannot send Fetch or
                // FetchSnapshot requests to the leader.
                //
                // Transition to unattached instead and discover the leader's endpoint through
                // Fetch requests to the bootstrap servers or from a BeginQuorumEpoch request from
                // the leader.
                log.info(
                    "The leader in election state {} is not a member of the latest voter set {}; " +
                    "transitioning to unattached instead of follower because the leader's " +
                    "endpoints are not known",
                    election,
                    voters
                );

                initialState = new UnattachedState(
                    time,
                    election.epoch(),
                    OptionalInt.of(election.leaderId()),
                    election.optionalVotedKey(),
                    partitionState.lastVoterSet().voterIds(),
                    Optional.empty(),
                    randomElectionTimeoutMs(),
                    logContext
                );
            } else {
                initialState = new FollowerState(
                    time,
                    election.epoch(),
                    election.leaderId(),
                    leaderEndpoints,
                    election.optionalVotedKey(),
                    voters.voterIds(),
                    Optional.empty(),
                    fetchTimeoutMs,
                    logContext
                );
            }
        } else {
            initialState = new UnattachedState(
                time,
                election.epoch(),
                OptionalInt.empty(),
                election.optionalVotedKey(),
                partitionState.lastVoterSet().voterIds(),
                Optional.empty(),
                randomElectionTimeoutMs(),
                logContext
            );
        }

        durableTransitionTo(initialState);
    }

    public boolean isOnlyVoter() {
        return localId.isPresent() &&
            partitionState
                .lastVoterSet()
                .isOnlyVoter(ReplicaKey.of(localId.getAsInt(), localDirectoryId));
    }

    public int localIdOrSentinel() {
        return localId.orElse(-1);
    }

    public int localIdOrThrow() {
        return localId.orElseThrow(() -> new IllegalStateException("Required local id is not present"));
    }

    public OptionalInt localId() {
        return localId;
    }

    public Uuid localDirectoryId() {
        return localDirectoryId;
    }

    public ReplicaKey localReplicaKeyOrThrow() {
        return ReplicaKey.of(localIdOrThrow(), localDirectoryId());
    }

    public VoterSet.VoterNode localVoterNodeOrThrow() {
        return VoterSet.VoterNode.of(
            localReplicaKeyOrThrow(),
            localListeners,
            localSupportedKRaftVersion
        );
    }

    public int epoch() {
        return state.epoch();
    }

    public int leaderIdOrSentinel() {
        return state.election().leaderIdOrSentinel();
    }

    public Optional<LogOffsetMetadata> highWatermark() {
        return state.highWatermark();
    }

    public OptionalInt leaderId() {
        ElectionState election = state.election();
        if (election.hasLeader())
            return OptionalInt.of(state.election().leaderId());
        else
            return OptionalInt.empty();
    }

    public Optional<ReplicaKey> votedKey() {
        return state.election().optionalVotedKey();
    }

    public boolean hasLeader() {
        return leaderId().isPresent();
    }

    public boolean hasRemoteLeader() {
        return hasLeader() && leaderIdOrSentinel() != localIdOrSentinel();
    }

    public Endpoints leaderEndpoints() {
        return state.leaderEndpoints();
    }

    public boolean isVoter() {
        if (localId.isEmpty()) {
            return false;
        }

        return partitionState
            .lastVoterSet()
            .isVoter(ReplicaKey.of(localId.getAsInt(), localDirectoryId));
    }

    public boolean isVoter(ReplicaKey nodeKey) {
        return partitionState.lastVoterSet().isVoter(nodeKey);
    }

    public boolean isObserver() {
        return !isVoter();
    }

    public void transitionToResigned(List<ReplicaKey> preferredSuccessors) {
        if (!isLeader()) {
            throw new IllegalStateException("Invalid transition to Resigned state from " + state);
        }

        // The Resigned state is a soft state which does not need to be persisted.
        // A leader will always be re-initialized in this state.
        int epoch = state.epoch();
        memoryTransitionTo(
            new ResignedState(
                time,
                localIdOrThrow(),
                epoch,
                partitionState.lastVoterSet().voterIds(),
                randomElectionTimeoutMs(),
                preferredSuccessors,
                localListeners,
                logContext
            )
        );
    }

    /**
     * Transition to the "unattached" state. This means one of the following
     * 1. the replica has found an epoch greater than the current epoch.
     * 2. the replica has transitioned from Prospective with the same epoch.
     * 3. the replica has transitioned from Resigned with current epoch + 1.
     * Note, if the replica is transitioning from unattached to add voted state and there is no epoch change,
     * it takes the route of unattachedAddVotedState instead.
     */
    public void transitionToUnattached(int epoch, OptionalInt leaderId) {
        int currentEpoch = state.epoch();
        if (epoch < currentEpoch || (epoch == currentEpoch && !isProspective())) {
            throw new IllegalStateException(
                String.format(
                    "Cannot transition to Unattached with epoch %d from current state %s",
                    epoch,
                    state
                )
            );
        }

        final long electionTimeoutMs;
        if (isObserver()) {
            electionTimeoutMs = Long.MAX_VALUE;
        } else if (isCandidate()) {
            electionTimeoutMs = candidateStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else if (isUnattached()) {
            electionTimeoutMs = unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else if (isProspective() && !prospectiveStateOrThrow().epochElection().isVoteRejected()) {
            electionTimeoutMs = prospectiveStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else if (isResigned()) {
            electionTimeoutMs = resignedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else {
            electionTimeoutMs = randomElectionTimeoutMs();
        }

        // If the local replica is transitioning to Unattached in the same epoch (i.e. from Prospective), it
        // should retain its voted key if it exists, so that it will not vote again in the same epoch.
        Optional<ReplicaKey> votedKey = epoch == currentEpoch ? votedKey() : Optional.empty();
        durableTransitionTo(new UnattachedState(
            time,
            epoch,
            leaderId,
            votedKey,
            partitionState.lastVoterSet().voterIds(),
            state.highWatermark(),
            electionTimeoutMs,
            logContext
        ));
    }

    /**
     * Grant a vote to a candidate as Unattached. The replica will transition to Unattached with votedKey
     * state in the same epoch and remain there until either the election timeout expires or it discovers the leader.
     * Note, if the replica discovers a higher epoch or is transitioning from Prospective, it takes
     * the route of transitionToUnattached instead.
     */
    public void unattachedAddVotedState(
        int epoch,
        ReplicaKey candidateKey
    ) {
        int currentEpoch = state.epoch();
        if (localId.isPresent() && candidateKey.id() == localId.getAsInt()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot add voted key (%s) to current state (%s) in epoch %d since it matches the local " +
                    "broker.id",
                    candidateKey,
                    state,
                    epoch
                )
            );
        } else if (localId.isEmpty()) {
            throw new IllegalStateException("Cannot add voted state without a replica id");
        } else if (epoch != currentEpoch || !isUnattachedNotVoted()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot add voted key (%s) to current state (%s) in epoch %d",
                    candidateKey,
                    state,
                    epoch
                )
            );
        }

        // Note that we reset the election timeout after voting for a candidate because we
        // know that the candidate has at least as good of a chance of getting elected as us
        durableTransitionTo(
            new UnattachedState(
                time,
                epoch,
                state.election().optionalLeaderId(),
                Optional.of(candidateKey),
                partitionState.lastVoterSet().voterIds(),
                state.highWatermark(),
                randomElectionTimeoutMs(),
                logContext
            )
        );
    }

    /**
     * Grant a vote to a candidate as Prospective. The replica will transition to Prospective with votedKey
     * state in the same epoch. Note, if the replica is transitioning to Prospective due to a fetch/election timeout
     * or loss of election as candidate, it takes the route of transitionToProspective instead.
     */
    public void prospectiveAddVotedState(
        int epoch,
        ReplicaKey candidateKey
    ) {
        int currentEpoch = state.epoch();
        if (localId.isPresent() && candidateKey.id() == localId.getAsInt()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot add voted key (%s) to current state (%s) in epoch %d since it matches the local " +
                    "broker.id",
                    candidateKey,
                    state,
                    epoch
                )
            );
        } else if (localId.isEmpty()) {
            throw new IllegalStateException("Cannot add voted state without a replica id");
        } else if (epoch != currentEpoch || !isProspectiveNotVoted()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot add voted key (%s) to current state (%s) in epoch %d",
                    candidateKey,
                    state,
                    epoch
                )
            );
        }

        ProspectiveState prospectiveState = prospectiveStateOrThrow();
        // Note that we reset the election timeout after voting for a candidate because we
        // know that the candidate has at least as good of a chance of getting elected as us
        durableTransitionTo(
            new ProspectiveState(
                time,
                localIdOrThrow(),
                epoch,
                state.election().optionalLeaderId(),
                state.leaderEndpoints(),
                Optional.of(candidateKey),
                partitionState.lastVoterSet(),
                state.highWatermark(),
                prospectiveState.retries(),
                randomElectionTimeoutMs(),
                logContext
            )
        );
    }

    /**
     * Become a follower of an elected leader so that we can begin fetching.
     */
    public void transitionToFollower(int epoch, int leaderId, Endpoints endpoints) {
        int currentEpoch = state.epoch();
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(
                    "Cannot transition to Follower with leader %s and epoch %s without a leader endpoint",
                    leaderId,
                    epoch
                )
            );
        } else if (localId.isPresent() && leaderId == localId.getAsInt()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot transition to Follower with leader %s and epoch %s since it matches the local node.id %s",
                    leaderId,
                    epoch,
                    localId
                )
            );
        } else if (epoch < currentEpoch) {
            throw new IllegalStateException(
                String.format(
                    "Cannot transition to Follower with leader %s and epoch %s since the current epoch %s is larger",
                    leaderId,
                    epoch,
                    currentEpoch
                )
            );
        } else if (epoch == currentEpoch) {
            if (isFollower() && state.leaderEndpoints().size() >= endpoints.size()) {
                throw new IllegalStateException(
                    String.format(
                        "Cannot transition to Follower with leader %s, epoch %s and endpoints %s from state %s",
                        leaderId,
                        epoch,
                        endpoints,
                        state
                    )
                );
            } else if (isLeader()) {
                throw new IllegalStateException(
                    String.format(
                        "Cannot transition to Follower with leader %s and epoch %s from state %s",
                        leaderId,
                        epoch,
                        state
                    )
                );
            }
        }

        // State transitions within the same epoch should preserve voted key if it exists. This prevents
        // replicas from voting multiple times in the same epoch, which could violate the Raft invariant of
        // at most one leader elected in an epoch.
        Optional<ReplicaKey> votedKey = epoch == currentEpoch ? votedKey() : Optional.empty();

        durableTransitionTo(
            new FollowerState(
                time,
                epoch,
                leaderId,
                endpoints,
                votedKey,
                partitionState.lastVoterSet().voterIds(),
                state.highWatermark(),
                fetchTimeoutMs,
                logContext
            )
        );
    }

    /**
     * Transition to the "prospective" state. This means the replica experienced a fetch/election timeout or
     * loss of election as candidate. Note, if the replica is transitioning from prospective to add voted state
     * and there is no epoch change, it takes the route of prospectiveAddVotedState instead.
     */
    public void transitionToProspective() {
        if (isObserver()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot transition to Prospective since the local id (%s) and directory id (%s) " +
                    "is not one of the voters %s",
                    localId,
                    localDirectoryId,
                    partitionState.lastVoterSet()
                )
            );
        } else if (isLeader() || isProspective()) {
            throw new IllegalStateException("Cannot transition to Prospective since the local broker.id=" + localId +
                " is state " + state);
        }

        int retries = isCandidate() ? candidateStateOrThrow().retries() + 1 : 1;

        // Durable transition is not necessary since there is no change to the persisted electionState
        memoryTransitionTo(
            new ProspectiveState(
                time,
                localIdOrThrow(),
                epoch(),
                leaderId(),
                state.leaderEndpoints(),
                votedKey(),
                partitionState.lastVoterSet(),
                state.highWatermark(),
                retries,
                randomElectionTimeoutMs(),
                logContext
            )
        );
    }

    public void transitionToCandidate() {
        checkValidTransitionToCandidate();

        int newEpoch = epoch() + 1;
        int electionTimeoutMs = randomElectionTimeoutMs();

        int retries = isProspective() ? prospectiveStateOrThrow().retries() : 1;

        durableTransitionTo(new CandidateState(
            time,
            localIdOrThrow(),
            localDirectoryId,
            newEpoch,
            partitionState.lastVoterSet(),
            state.highWatermark(),
            retries,
            electionTimeoutMs,
            logContext
        ));
    }

    private void checkValidTransitionToCandidate() {
        if (isObserver()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot transition to Candidate since the local id (%s) and directory id (%s) " +
                    "is not one of the voters %s",
                    localId,
                    localDirectoryId,
                    partitionState.lastVoterSet()
                )
            );
        }
        // Only Prospective is allowed to transition to Candidate
        if (!isProspective()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot transition to Candidate since the local broker.id=%s is state %s",
                    localId,
                    state
                )
            );
        }
    }

    public <T> LeaderState<T> transitionToLeader(long epochStartOffset, BatchAccumulator<T> accumulator) {
        if (isObserver()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot transition to Leader since the local id (%s) and directory id (%s) " +
                    "is not one of the voters %s",
                    localId,
                    localDirectoryId,
                    partitionState.lastVoterSet()
                )
            );
        } else if (!isCandidate()) {
            throw new IllegalStateException("Cannot transition to Leader from current state " + state);
        }

        CandidateState candidateState = candidateStateOrThrow();
        if (!candidateState.epochElection().isVoteGranted())
            throw new IllegalStateException("Cannot become leader without majority votes granted");

        // Note that the leader does not retain the high watermark that was known
        // in the previous state. The reason for this is to protect the monotonicity
        // of the global high watermark, which is exposed through the leader. The
        // only way a new leader can be sure that the high watermark is increasing
        // monotonically is to wait until a majority of the voters have reached the
        // starting offset of the new epoch. The downside of this is that the local
        // state machine is temporarily stalled by the advancement of the global
        // high watermark even though it only depends on local monotonicity. We
        // could address this problem by decoupling the local high watermark, but
        // we typically expect the state machine to be caught up anyway.

        LeaderState<T> state = new LeaderState<>(
            time,
            ReplicaKey.of(localIdOrThrow(), localDirectoryId),
            epoch(),
            epochStartOffset,
            partitionState.lastVoterSet(),
            partitionState.lastVoterSetOffset(),
            partitionState.lastKraftVersion(),
            candidateState.epochElection().grantingVoters(),
            accumulator,
            localListeners,
            fetchTimeoutMs,
            logContext,
            kafkaRaftMetrics
        );

        durableTransitionTo(state);
        return state;
    }

    private void durableTransitionTo(EpochState newState) {
        log.info("Attempting durable transition to {} from {}", newState, state);
        store.writeElectionState(newState.election(), partitionState.lastKraftVersion());
        memoryTransitionTo(newState);
    }

    private void memoryTransitionTo(EpochState newState) {
        if (state != null) {
            try {
                state.close();
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Failed to transition from " + state.name() + " to " + newState.name(), e);
            }
        }

        EpochState from = state;
        state = newState;
        log.info("Completed transition to {} from {}", newState, from);
    }

    private int randomElectionTimeoutMs() {
        if (electionTimeoutMs == 0)
            return 0;
        return electionTimeoutMs + random.nextInt(electionTimeoutMs);
    }

    public boolean canGrantVote(ReplicaKey replicaKey, boolean isLogUpToDate, boolean isPreVote) {
        return state.canGrantVote(replicaKey, isLogUpToDate, isPreVote);
    }

    public FollowerState followerStateOrThrow() {
        if (isFollower())
            return (FollowerState) state;
        throw new IllegalStateException("Expected to be Follower, but the current state is " + state);
    }

    public Optional<UnattachedState> maybeUnattachedState() {
        EpochState fixedState = state;
        if (fixedState instanceof UnattachedState) {
            return Optional.of((UnattachedState) fixedState);
        } else {
            return Optional.empty();
        }
    }

    public UnattachedState unattachedStateOrThrow() {
        return maybeUnattachedState().orElseThrow(
            () -> new IllegalStateException("Expected to be Unattached, but current state is " + state)
        );
    }

    public <T> LeaderState<T> leaderStateOrThrow() {
        return this.<T>maybeLeaderState()
            .orElseThrow(() -> new IllegalStateException("Expected to be Leader, but current state is " + state));
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<LeaderState<T>> maybeLeaderState() {
        EpochState fixedState = state;
        if (fixedState instanceof LeaderState) {
            return Optional.of((LeaderState<T>) fixedState);
        } else {
            return Optional.empty();
        }
    }

    public ResignedState resignedStateOrThrow() {
        if (isResigned())
            return (ResignedState) state;
        throw new IllegalStateException("Expected to be Resigned, but current state is " + state);
    }

    public Optional<ProspectiveState> maybeProspectiveState() {
        EpochState fixedState = state;
        if (fixedState instanceof ProspectiveState) {
            return Optional.of((ProspectiveState) fixedState);
        } else {
            return Optional.empty();
        }
    }

    public ProspectiveState prospectiveStateOrThrow() {
        return maybeProspectiveState().orElseThrow(
            () -> new IllegalStateException("Expected to be Prospective, but current state is " + state)
        );
    }

    public boolean isProspectiveNotVoted() {
        return maybeProspectiveState().filter(prospective -> prospective.votedKey().isEmpty()).isPresent();
    }

    public boolean isProspectiveAndVoted() {
        return maybeProspectiveState().flatMap(ProspectiveState::votedKey).isPresent();
    }

    public CandidateState candidateStateOrThrow() {
        if (isCandidate())
            return (CandidateState) state;
        throw new IllegalStateException("Expected to be Candidate, but current state is " + state);
    }

    public NomineeState nomineeStateOrThrow() {
        if (isNomineeState())
            return (NomineeState) state;
        throw new IllegalStateException("Expected to be a NomineeState (Prospective or Candidate), " +
            "but current state is " + state);
    }

    public LeaderAndEpoch leaderAndEpoch() {
        ElectionState election = state.election();
        return new LeaderAndEpoch(election.optionalLeaderId(), election.epoch());
    }

    public boolean isFollower() {
        return state instanceof FollowerState;
    }

    public boolean isUnattached() {
        return state instanceof UnattachedState;
    }

    public boolean isUnattachedNotVoted() {
        return maybeUnattachedState().filter(unattached -> unattached.votedKey().isEmpty()).isPresent();
    }

    public boolean isUnattachedAndVoted() {
        return maybeUnattachedState().flatMap(UnattachedState::votedKey).isPresent();
    }

    public boolean isLeader() {
        return state instanceof LeaderState;
    }

    public boolean isResigned() {
        return state instanceof ResignedState;
    }

    public boolean isProspective() {
        return state instanceof ProspectiveState;
    }

    public boolean isCandidate() {
        return state instanceof CandidateState;
    }

    public boolean isNomineeState() {
        return state instanceof NomineeState;
    }

    /**
     * Determines if replica in unattached or prospective state can grant a vote request.
     *
     * @param leaderId local replica's optional leader id.
     * @param votedKey local replica's optional voted key.
     * @param epoch local replica's epoch
     * @param replicaKey replicaKey of nominee which sent the vote request
     * @param isLogUpToDate whether the log of the nominee is up-to-date with the local replica's log
     * @param isPreVote whether the vote request is a PreVote request
     * @param log logger
     * @return true if the local replica can grant the vote request, false otherwise
     */
    public static boolean unattachedOrProspectiveCanGrantVote(
        OptionalInt leaderId,
        Optional<ReplicaKey> votedKey,
        int epoch,
        ReplicaKey replicaKey,
        boolean isLogUpToDate,
        boolean isPreVote,
        Logger log
    ) {
        if (isPreVote) {
            if (!isLogUpToDate) {
                log.debug(
                    "Rejecting Vote request (preVote=true) from prospective ({}) since prospective's log is not up to date with us",
                    replicaKey
                );
            }
            return isLogUpToDate;
        } else if (votedKey.isPresent()) {
            ReplicaKey votedReplicaKey = votedKey.get();
            if (votedReplicaKey.id() == replicaKey.id()) {
                return votedReplicaKey.directoryId().isEmpty() || votedReplicaKey.directoryId().equals(replicaKey.directoryId());
            }
            log.debug(
                "Rejecting Vote request (preVote=false) from candidate ({}), already have voted for another " +
                    "candidate ({}) in epoch {}",
                replicaKey,
                votedKey,
                epoch
            );
            return false;
        } else if (leaderId.isPresent()) {
            // If the leader id is known it should behave similar to the follower state
            log.debug(
                "Rejecting Vote request (preVote=false) from candidate ({}) since we already have a leader {} in epoch {}",
                replicaKey,
                leaderId.getAsInt(),
                epoch
            );
            return false;
        } else if (!isLogUpToDate) {
            log.debug(
                "Rejecting Vote request (preVote=false) from candidate ({}) since candidate's log is not up to date with us",
                replicaKey
            );
        }

        return isLogUpToDate;
    }
}
