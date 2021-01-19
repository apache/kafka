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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is responsible for managing the current state of this node and ensuring
 * only valid state transitions. Below we define the possible state transitions and
 * how they are triggered:
 *
 * Unattached|Resigned transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Voted: After granting a vote to a candidate
 *    Candidate: After expiration of the election timeout
 *    Follower: After discovering a leader with an equal or larger epoch
 *
 * Voted transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Candidate: After expiration of the election timeout
 *
 * Candidate transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Candidate: After expiration of the election timeout
 *    Leader: After receiving a majority of votes
 *
 * Leader transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Resigned: When shutting down gracefully
 *
 * Follower transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Candidate: After expiration of the fetch timeout
 *    Follower: After discovering a leader with a larger epoch
 *
 * Observers follow a simpler state machine. The Voted/Candidate/Leader/Resigned
 * states are not possible for observers, so the only transitions that are possible
 * are between Unattached and Follower.
 *
 * Unattached transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Follower: After discovering a leader with an equal or larger epoch
 *
 * Follower transitions to:
 *    Unattached: After learning of a new election with a higher epoch
 *    Follower: After discovering a leader with a larger epoch
 *
 */
public class QuorumState {
    private final OptionalInt localId;
    private final Time time;
    private final Logger log;
    private final QuorumStateStore store;
    private final Set<Integer> voters;
    private final Random random;
    private final int electionTimeoutMs;
    private final int fetchTimeoutMs;

    private volatile EpochState state;

    public QuorumState(OptionalInt localId,
                       Set<Integer> voters,
                       int electionTimeoutMs,
                       int fetchTimeoutMs,
                       QuorumStateStore store,
                       Time time,
                       LogContext logContext,
                       Random random) {
        this.localId = localId;
        this.voters = new HashSet<>(voters);
        this.electionTimeoutMs = electionTimeoutMs;
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.store = store;
        this.time = time;
        this.log = logContext.logger(QuorumState.class);
        this.random = random;
    }

    public void initialize(OffsetAndEpoch logEndOffsetAndEpoch) throws IOException, IllegalStateException {
        // We initialize in whatever state we were in on shutdown. If we were a leader
        // or candidate, probably an election was held, but we will find out about it
        // when we send Vote or BeginEpoch requests.

        ElectionState election;
        try {
            election = store.readElectionState();
            if (election == null) {
                election = ElectionState.withUnknownLeader(0, voters);
            }
        } catch (final IOException e) {
            // For exceptions during state file loading (missing or not readable),
            // we could assume the file is corrupted already and should be cleaned up.
            log.warn("Clearing local quorum state store after error loading state {}",
                store.toString(), e);
            store.clear();
            election = ElectionState.withUnknownLeader(0, voters);
        }

        final EpochState initialState;
        if (!election.voters().isEmpty() && !voters.equals(election.voters())) {
            throw new IllegalStateException("Configured voter set: " + voters
                + " is different from the voter set read from the state file: " + election.voters()
                + ". Check if the quorum configuration is up to date, "
                + "or wipe out the local state file if necessary");
        } else if (election.hasVoted() && !isVoter()) {
            String localIdDescription = localId.isPresent() ?
                localId.getAsInt() + " is not a voter" :
                "is undefined";
            throw new IllegalStateException("Initialized quorum state " + election
                + " with a voted candidate, which indicates this node was previously "
                + " a voter, but the local id " + localIdDescription);
        } else if (election.epoch < logEndOffsetAndEpoch.epoch) {
            log.warn("Epoch from quorum-state file is {}, which is " +
                "smaller than last written epoch {} in the log",
                election.epoch, logEndOffsetAndEpoch.epoch);
            initialState = new UnattachedState(
                time,
                logEndOffsetAndEpoch.epoch,
                voters,
                Optional.empty(),
                randomElectionTimeoutMs()
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
                election.epoch,
                voters,
                randomElectionTimeoutMs(),
                Collections.emptyList()
            );
        } else if (localId.isPresent() && election.isVotedCandidate(localId.getAsInt())) {
            initialState = new CandidateState(
                time,
                localId.getAsInt(),
                election.epoch,
                voters,
                Optional.empty(),
                1,
                randomElectionTimeoutMs()
            );
        } else if (election.hasVoted()) {
            initialState = new VotedState(
                time,
                election.epoch,
                election.votedId(),
                voters,
                Optional.empty(),
                randomElectionTimeoutMs()
            );
        } else if (election.hasLeader()) {
            initialState = new FollowerState(
                time,
                election.epoch,
                election.leaderId(),
                voters,
                Optional.empty(),
                fetchTimeoutMs
            );
        } else {
            initialState = new UnattachedState(
                time,
                election.epoch,
                voters,
                Optional.empty(),
                randomElectionTimeoutMs()
            );
        }

        transitionTo(initialState);
    }

    public Set<Integer> remoteVoters() {
        return voters.stream().filter(voterId -> voterId != localIdOrSentinel()).collect(Collectors.toSet());
    }

    public int localIdOrSentinel() {
        return localId.orElse(-1);
    }

    public int localIdOrThrow() {
        return localId.orElseThrow(() -> new IllegalStateException("Required local id is not present"));
    }

    public int epoch() {
        return state.epoch();
    }

    public int leaderIdOrSentinel() {
        return leaderId().orElse(-1);
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

    public boolean hasLeader() {
        return leaderId().isPresent();
    }

    public boolean hasRemoteLeader() {
        return hasLeader() && leaderIdOrSentinel() != localIdOrSentinel();
    }

    public boolean isVoter() {
        return localId.isPresent() && voters.contains(localId.getAsInt());
    }

    public boolean isVoter(int nodeId) {
        return voters.contains(nodeId);
    }

    public boolean isObserver() {
        return !isVoter();
    }

    public void transitionToResigned(List<Integer> preferredSuccessors) {
        if (!isLeader()) {
            throw new IllegalStateException("Invalid transition to Resigned state from " + state);
        }

        // The Resigned state is a soft state which does not need to be persisted.
        // A leader will always be re-initialized in this state.
        int epoch = state.epoch();
        this.state = new ResignedState(
            time,
            localIdOrThrow(),
            epoch,
            voters,
            randomElectionTimeoutMs(),
            preferredSuccessors
        );
        log.info("Completed transition to {}", state);
    }

    /**
     * Transition to the "unattached" state. This means we have found an epoch greater than
     * or equal to the current epoch, but wo do not yet know of the elected leader.
     */
    public void transitionToUnattached(int epoch) throws IOException {
        int currentEpoch = state.epoch();
        if (epoch <= currentEpoch) {
            throw new IllegalStateException("Cannot transition to Unattached with epoch= " + epoch +
                " from current state " + state);
        }

        final long electionTimeoutMs;
        if (isObserver()) {
            electionTimeoutMs = Long.MAX_VALUE;
        } else if (isCandidate()) {
            electionTimeoutMs = candidateStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else if (isVoted()) {
            electionTimeoutMs = votedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else if (isUnattached()) {
            electionTimeoutMs = unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        } else {
            electionTimeoutMs = randomElectionTimeoutMs();
        }

        transitionTo(new UnattachedState(
            time,
            epoch,
            voters,
            state.highWatermark(),
            electionTimeoutMs
        ));
    }

    /**
     * Grant a vote to a candidate and become a follower for this epoch. We will remain in this
     * state until either the election timeout expires or a leader is elected. In particular,
     * we do not begin fetching until the election has concluded and {@link #transitionToFollower(int, int)}
     * is invoked.
     */
    public void transitionToVoted(
        int epoch,
        int candidateId
    ) throws IOException {
        if (localId.isPresent() && candidateId == localId.getAsInt()) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " since it matches the local broker.id");
        } else if (isObserver()) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " since the local broker.id=" + localId + " is not a voter");
        } else if (!isVoter(candidateId)) {
            throw new IllegalStateException("Cannot transition to Voted with voterId=" + candidateId +
                " and epoch=" + epoch + " since it is not one of the voters " + voters);
        }

        int currentEpoch = state.epoch();
        if (epoch < currentEpoch) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " since the current epoch " + currentEpoch + " is larger");
        } else if (epoch == currentEpoch && !isUnattached()) {
            throw new IllegalStateException("Cannot transition to Voted with votedId=" + candidateId +
                " and epoch=" + epoch + " from the current state " + state);
        }

        // Note that we reset the election timeout after voting for a candidate because we
        // know that the candidate has at least as good of a chance of getting elected as us

        transitionTo(new VotedState(
            time,
            epoch,
            candidateId,
            voters,
            state.highWatermark(),
            randomElectionTimeoutMs()
        ));
    }

    /**
     * Become a follower of an elected leader so that we can begin fetching.
     */
    public void transitionToFollower(
        int epoch,
        int leaderId
    ) throws IOException {
        if (localId.isPresent() && leaderId == localId.getAsInt()) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " since it matches the local broker.id=" + localId);
        } else if (!isVoter(leaderId)) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " since it is not one of the voters " + voters);
        }

        int currentEpoch = state.epoch();
        if (epoch < currentEpoch) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " since the current epoch " + currentEpoch + " is larger");
        } else if (epoch == currentEpoch
            && (isFollower() || isLeader())) {
            throw new IllegalStateException("Cannot transition to Follower with leaderId=" + leaderId +
                " and epoch=" + epoch + " from state " + state);
        }

        transitionTo(new FollowerState(
            time,
            epoch,
            leaderId,
            voters,
            state.highWatermark(),
            fetchTimeoutMs
        ));
    }

    public void transitionToCandidate() throws IOException {
        if (isObserver()) {
            throw new IllegalStateException("Cannot transition to Candidate since the local broker.id=" + localId +
                " is not one of the voters " + voters);
        } else if (isLeader()) {
            throw new IllegalStateException("Cannot transition to Candidate since the local broker.id=" + localId +
                " since this node is already a Leader with state " + state);
        }

        int retries = isCandidate() ? candidateStateOrThrow().retries() + 1 : 1;
        int newEpoch = epoch() + 1;
        int electionTimeoutMs = randomElectionTimeoutMs();

        transitionTo(new CandidateState(
            time,
            localIdOrThrow(),
            newEpoch,
            voters,
            state.highWatermark(),
            retries,
            electionTimeoutMs
        ));
    }

    public void transitionToLeader(long epochStartOffset) throws IOException {
        if (isObserver()) {
            throw new IllegalStateException("Cannot transition to Leader since the local broker.id="  + localId +
                " is not one of the voters " + voters);
        } else if (!isCandidate()) {
            throw new IllegalStateException("Cannot transition to Leader from current state " + state);
        }

        CandidateState candidateState = candidateStateOrThrow();
        if (!candidateState.isVoteGranted())
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

        transitionTo(new LeaderState(
            localIdOrThrow(),
            epoch(),
            epochStartOffset,
            voters,
            candidateState.grantingVoters()
        ));
    }

    private void transitionTo(EpochState state) throws IOException {
        if (this.state != null) {
            this.state.close();
        }

        this.store.writeElectionState(state.election());
        this.state = state;
        log.info("Completed transition to {}", state);
    }

    private int randomElectionTimeoutMs() {
        if (electionTimeoutMs == 0)
            return 0;
        return electionTimeoutMs + random.nextInt(electionTimeoutMs);
    }

    public FollowerState followerStateOrThrow() {
        if (isFollower())
            return (FollowerState) state;
        throw new IllegalStateException("Expected to be Follower, but the current state is " + state);
    }

    public VotedState votedStateOrThrow() {
        if (isVoted())
            return (VotedState) state;
        throw new IllegalStateException("Expected to be Voted, but current state is " + state);
    }

    public UnattachedState unattachedStateOrThrow() {
        if (isUnattached())
            return (UnattachedState) state;
        throw new IllegalStateException("Expected to be Unattached, but current state is " + state);
    }

    public LeaderState leaderStateOrThrow() {
        if (isLeader())
            return (LeaderState) state;
        throw new IllegalStateException("Expected to be Leader, but current state is " + state);
    }

    public ResignedState resignedStateOrThrow() {
        if (isResigned())
            return (ResignedState) state;
        throw new IllegalStateException("Expected to be Resigned, but current state is " + state);
    }

    public CandidateState candidateStateOrThrow() {
        if (isCandidate())
            return (CandidateState) state;
        throw new IllegalStateException("Expected to be Candidate, but current state is " + state);
    }

    public LeaderAndEpoch leaderAndEpoch() {
        ElectionState election = state.election();
        return new LeaderAndEpoch(election.leaderIdOpt, election.epoch);
    }

    public boolean isFollower() {
        return state instanceof FollowerState;
    }

    public boolean isVoted() {
        return state instanceof VotedState;
    }

    public boolean isUnattached() {
        return state instanceof UnattachedState;
    }

    public boolean isLeader() {
        return state instanceof LeaderState;
    }

    public boolean isResigned() {
        return state instanceof ResignedState;
    }

    public boolean isCandidate() {
        return state instanceof CandidateState;
    }

}
