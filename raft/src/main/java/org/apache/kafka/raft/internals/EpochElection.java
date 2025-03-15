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
package org.apache.kafka.raft.internals;

import org.apache.kafka.raft.ReplicaKey;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *  Tracks the votes cast by voters in an election held by a Nominee.
 */
public class EpochElection {
    private Map<Integer, VoterState> voterStates;

    public EpochElection(Set<ReplicaKey> voters) {
        this.voterStates = voters.stream()
            .collect(
                Collectors.toMap(
                    ReplicaKey::id,
                    VoterState::new
                )
            );
    }

    /**
     * Record a vote from a voter.
     *
     * @param voterId The id of the voter
     * @param isGranted true if the vote is granted, false if it is rejected
     * @return true if the voter had not been previously recorded
     */
    public boolean recordVote(int voterId, boolean isGranted) {
        VoterState voterState = getVoterStateOrThrow(voterId);
        boolean wasUnrecorded = voterState.state == VoterState.State.UNRECORDED;
        voterState.setState(
            isGranted ? VoterState.State.GRANTED : VoterState.State.REJECTED
        );
        return wasUnrecorded;
    }

    /**
     * Returns if a voter has granted the vote.
     *
     * @param voterId The id of the voter
     * @throws IllegalArgumentException if the voter is not in the set of voters
     */
    public boolean isGrantedVoter(int voterId) {
        return getVoterStateOrThrow(voterId).state == VoterState.State.GRANTED;
    }

    /**
     * Returns if a voter has rejected the vote.
     *
     * @param voterId The id of the voter
     * @throws IllegalArgumentException if the voter is not in the set of voters
     */
    public boolean isRejectedVoter(int voterId) {
        return getVoterStateOrThrow(voterId).state == VoterState.State.REJECTED;
    }

    /**
     * The set of voter ids.
     */
    public Set<Integer> voterIds() {
        return Collections.unmodifiableSet(voterStates.keySet());
    }

    /**
     * Check whether we have received enough votes to conclude the election and become leader.
     *
     * @return true if at least a majority of nodes have granted the vote
     */
    public boolean isVoteGranted() {
        return numGranted() >= majoritySize();
    }

    /**
     * Check if we have received enough rejections that it is no longer possible to reach a
     * majority of grants.
     *
     * @return true if the vote is rejected, false if the vote is already or can still be granted
     */
    public boolean isVoteRejected() {
        return numGranted() + numUnrecorded() < majoritySize();
    }

    /**
     * Get the set of voters which have not been counted as granted or rejected yet.
     *
     * @return The set of unrecorded voters
     */
    public Set<ReplicaKey> unrecordedVoters() {
        return votersOfState(VoterState.State.UNRECORDED).collect(Collectors.toSet());
    }

    /**
     * Get the set of voters that have granted our vote requests.
     *
     * @return The set of granting voters, which should always contain the localId
     */
    public Set<Integer> grantingVoters() {
        return votersOfState(VoterState.State.GRANTED).map(ReplicaKey::id).collect(Collectors.toSet());
    }

    /**
     * Get the set of voters that have rejected our candidacy.
     *
     * @return The set of rejecting voters
     */
    public Set<Integer> rejectingVoters() {
        return votersOfState(VoterState.State.REJECTED).map(ReplicaKey::id).collect(Collectors.toSet());
    }

    private VoterState getVoterStateOrThrow(int voterId) {
        VoterState voterState = voterStates.get(voterId);
        if (voterState == null) {
            throw new IllegalArgumentException("Attempt to access voter state of non-voter " + voterId);
        }
        return voterState;
    }

    private Stream<ReplicaKey> votersOfState(VoterState.State state) {
        return voterStates
            .values()
            .stream()
            .filter(voterState -> voterState.state().equals(state))
            .map(VoterState::replicaKey);
    }

    private long numGranted() {
        return votersOfState(VoterState.State.GRANTED).count();
    }

    private long numUnrecorded() {
        return votersOfState(VoterState.State.UNRECORDED).count();
    }

    private int majoritySize() {
        return voterStates.size() / 2 + 1;
    }

    @Override
    public String toString() {
        return String.format(
            "EpochElection(voterStates=%s)",
            voterStates
        );
    }

    private static final class VoterState {
        private final ReplicaKey replicaKey;
        private State state = State.UNRECORDED;

        VoterState(ReplicaKey replicaKey) {
            this.replicaKey = replicaKey;
        }

        public State state() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
        }

        public ReplicaKey replicaKey() {
            return replicaKey;
        }

        enum State {
            UNRECORDED,
            GRANTED,
            REJECTED
        }

        @Override
        public String toString() {
            return String.format(
                "VoterState(replicaKey=%s, state=%s)",
                replicaKey,
                state
            );
        }
    }
}
