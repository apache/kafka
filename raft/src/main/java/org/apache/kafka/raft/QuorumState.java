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
import java.util.OptionalInt;
import java.util.OptionalLong;
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
    private final ElectionStore store;
    private final Set<Integer> voters;
    private EpochState state;

    public QuorumState(int localId,
                       Set<Integer> voters,
                       ElectionStore store,
                       LogContext logContext) {
        this.localId = localId;
        this.voters = new HashSet<>(voters);
        this.store = store;
        this.log = logContext.logger(QuorumState.class);
    }

    public void initialize(long endOffset) throws IOException {
        // We initialize in whatever state we were in on shutdown. If we were a leader
        // or candidate, probably an election was held, but we will find out about it
        // when we send Vote or BeginEpoch requests.

        ElectionState election = store.read();
        if (election.isLeader(localId)) {
            state = new LeaderState(localId, election.epoch, endOffset, voters);
        } else if (election.isCandidate(localId)) {
            state = new CandidateState(localId, election.epoch, voters);
        } else {
            state = new FollowerState(election.epoch);
            if (election.hasLeader()) {
                becomeFollower(election.epoch, election.leaderId());
            } else if (election.hasVoted() && election.votedId() != localId) {
                becomeVotedFollower(election.epoch, election.votedId());
            } else {
                becomeUnattachedFollower(election.epoch);
            }
        }
    }

    public Set<Integer> remoteVoters() {
        return voters.stream().filter(voterId -> voterId != localId).collect(Collectors.toSet());
    }

    public int epoch() {
        return state.epoch();
    }

    public int leaderIdOrNil() {
        return leaderId().orElse(-1);
    }

    public OptionalLong highWatermark() {
        return state.highWatermark();
    }

    public OptionalInt leaderId() {
        ElectionState election = state.election();
        if (election.hasLeader())
            return OptionalInt.of(state.election().leaderId());
        else
            return OptionalInt.empty();
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

    public boolean becomeVotedFollower(int epoch, int candidateId) throws IOException {
        if (!isVoter(candidateId))
            throw new IllegalArgumentException("Cannot become follower of non-voter " + candidateId);

        boolean transitioned = becomeFollower(epoch, state -> state.grantVoteTo(candidateId));
        if (transitioned)
            log.info("Become voting follower of candidate {} in epoch {}", candidateId, epoch);
        return transitioned;
    }

    public boolean becomeFollower(int epoch, int leaderId) throws IOException {
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
            state = new FollowerState(newEpoch);
            stateChanged = true;
        } else if (isLeader()) {
            throw new IllegalArgumentException("Cannot become follower of epoch " + newEpoch +
                    " since we are already the leader of this epoch");
        }

        FollowerState followerState = followerStateOrThrow();
        if (func.apply(followerState) || stateChanged) {
            store.write(followerState.election());
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
        store.write(state.election());
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
        store.write(state.election());
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
