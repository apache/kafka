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
import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is responsible for managing the current state of this node and ensuring only
 * valid state transitions.
 */
public class QuorumState {
    public final int localId;
    private final Logger log;
    private final QuorumStateStore store;
    private final Set<Integer> voters;
    private EpochState state;

    public QuorumState(int localId,
                       Set<Integer> voters,
                       QuorumStateStore store,
                       LogContext logContext) {
        this.localId = localId;
        this.voters = new HashSet<>(voters);
        this.store = store;
        this.log = logContext.logger(QuorumState.class);
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
            log.warn("Clear local quorum state store {}", store.toString(), e);
            store.clear();
            election = ElectionState.withUnknownLeader(0, voters);
        }

        if (!election.voters().isEmpty() && !voters.equals(election.voters())) {
            throw new IllegalStateException("Configured voter set: " + voters
                + " is different from the voter set read from the state file: " + election.voters() +
                ". Check if the quorum configuration is up to date, " +
                "or wipe out the local state file if necessary");
        } else if (election.epoch < logEndOffsetAndEpoch.epoch) {
            log.warn("Epoch from quorum-state file is {}, which is " +
                "smaller than last written epoch {} in the log",
                election.epoch, logEndOffsetAndEpoch.epoch);
            state = new FollowerState(election.epoch, voters);
            becomeUnattachedFollower(logEndOffsetAndEpoch.epoch);
        } else if (election.isLeader(localId)) {
            state = new LeaderState(localId, election.epoch, logEndOffsetAndEpoch.offset, voters);
        } else if (election.isCandidate(localId)) {
            state = new CandidateState(localId, election.epoch, voters);
        } else {
            state = new FollowerState(election.epoch, voters);
            if (election.hasLeader()) {
                becomeFetchingFollower(election.epoch, election.leaderId());
            } else if (election.hasVoted()) {
                becomeVotedFollower(election.epoch, election.votedId());
            } else {
                becomeUnattachedFollower(election.epoch);
            }
        }
    }

    public Set<Integer> remoteVoters() {
        return voters.stream().filter(voterId -> voterId != localId).collect(Collectors.toSet());
    }

    public Set<Integer> allVoters() {
        return voters;
    }

    public int epoch() {
        return state.epoch();
    }

    public int leaderIdOrNil() {
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

    public boolean isLeader() {
        return state instanceof LeaderState;
    }

    public boolean isCandidate() {
        return state instanceof CandidateState;
    }

    public boolean isFollower() {
        return state instanceof FollowerState;
    }

    public boolean isVoter() {
        return voters.contains(localId);
    }

    public boolean isVoter(int nodeId) {
        return voters.contains(nodeId);
    }

    public boolean isObserver() {
        return !isVoter();
    }

    public boolean becomeUnattachedFollower(int epoch) throws IOException {
        if (isObserver())
            return becomeFollower(epoch, FollowerState::detachLeader);

        boolean transitioned = becomeFollower(epoch, FollowerState::assertNotAttached);
        if (transitioned)
            log.info("Become unattached follower in epoch {}", epoch);
        return transitioned;
    }

    /**
     * Grant a vote to a candidate and become a follower for this epoch. We will remain in this
     * state until either the election timeout expires or a leader is elected. In particular,
     * we do not begin fetching until the election has concluded and {@link #becomeFetchingFollower(int, int)}
     * is invoked.
     */
    public boolean becomeVotedFollower(int epoch, int candidateId) throws IOException {
        if (candidateId == localId)
            throw new IllegalArgumentException("Cannot become a follower of " + candidateId +
                " since that matches the local `broker.id`");
        if (!isVoter(candidateId))
            throw new IllegalArgumentException("Cannot become follower of non-voter " + candidateId);

        boolean transitioned = becomeFollower(epoch, state -> state.grantVoteTo(candidateId));
        if (transitioned)
            log.info("Become voting follower of candidate {} in epoch {}", candidateId, epoch);
        return transitioned;
    }

    /**
     * Become a follower of an elected leader so that we can begin fetching.
     */
    public boolean becomeFetchingFollower(int epoch, int leaderId) throws IOException {
        if (leaderId == localId)
            throw new IllegalArgumentException("Cannot become a follower of " + leaderId +
                " since that matches the local `broker.id`");
        if (!isVoter(leaderId))
            throw new IllegalArgumentException("Cannot become follower of non-voter " + leaderId);
        boolean transitioned = becomeFollower(epoch, state -> state.acknowledgeLeader(leaderId));
        if (transitioned) {
            log.info("Become follower of leader {} in epoch {}", leaderId, epoch);
        }

        return transitioned;
    }

    private boolean becomeFollower(int newEpoch, Function<FollowerState, Boolean> func) throws IOException {
        int currentEpoch = epoch();
        boolean stateChanged = false;

        if (newEpoch < currentEpoch) {
            throw new IllegalArgumentException("Cannot become follower in epoch " + newEpoch +
                    " since it is smaller epoch than our current epoch " + currentEpoch);
        } else if (newEpoch > currentEpoch || isCandidate()) {
            state = new FollowerState(newEpoch, voters);
            stateChanged = true;
        } else if (isLeader()) {
            throw new IllegalArgumentException("Cannot become follower of epoch " + newEpoch +
                    " since we are already the leader of this epoch");
        }

        FollowerState followerState = followerStateOrThrow();
        if (func.apply(followerState) || stateChanged) {
            store.writeElectionState(followerState.election());
            return true;
        }
        return false;
    }

    public CandidateState becomeCandidate() throws IOException {
        if (isObserver())
            throw new IllegalStateException("Cannot become candidate since we are not a voter");
        if (isLeader())
            throw new IllegalStateException("Cannot become candidate after being leader");

        int newEpoch = epoch() + 1;
        log.info("Become candidate in epoch {}", newEpoch);
        CandidateState state = new CandidateState(localId, newEpoch, voters);
        store.writeElectionState(state.election());
        this.state = state;
        return state;
    }

    public LeaderState becomeLeader(long epochStartOffset) throws IOException {
        if (isObserver())
            throw new IllegalStateException("Cannot become candidate since we are not a voter");

        CandidateState candidateState = candidateStateOrThrow();
        if (!candidateState.isVoteGranted())
            throw new IllegalStateException("Cannot become leader without majority votes granted");

        log.info("Become leader in epoch {}", epoch());
        LeaderState state = new LeaderState(localId, epoch(), epochStartOffset, voters);
        store.writeElectionState(state.election());
        this.state = state;
        return state;
    }

    public FollowerState followerStateOrThrow() {
        if (isFollower())
            return (FollowerState) state;
        throw new IllegalStateException("Expected to be a follower, but current state is " + state);
    }

    public LeaderState leaderStateOrThrow() {
        if (isLeader())
            return (LeaderState) state;
        throw new IllegalStateException("Expected to be a leader, but current state is " + state);
    }

    public CandidateState candidateStateOrThrow() {
        if (isCandidate())
            return (CandidateState) state;
        throw new IllegalStateException("Expected to be a candidate, but current state is " + state);
    }
}
