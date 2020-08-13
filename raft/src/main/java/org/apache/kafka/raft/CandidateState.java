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

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

public class CandidateState implements EpochState {
    private final int localId;
    private final int epoch;
    private final int retries;
    private final Map<Integer, VoteState> voteStates = new HashMap<>();

    /**
     * The life time of a candidate state is the following:
     *
     *  1. Once started, it would keep record of the received votes.
     *  2. If majority votes granted, it can then end its life and will be replaced by a leader state;
     *  3. If majority votes rejected or election timed out, it would transit into a backing off phase;
     *     after the backoff phase completes, it would end its left and be replaced by a new candidate state with bumped retry.
     */
    private boolean isBackingOff;

    protected CandidateState(int localId, int epoch, Set<Integer> voters, int retries) {
        this.localId = localId;
        this.epoch = epoch;
        this.retries = retries;
        this.isBackingOff = false;

        for (Integer voterId : voters) {
            voteStates.put(voterId, VoteState.UNRECORDED);
        }
        voteStates.put(localId, VoteState.GRANTED);
    }

    public int localId() {
        return localId;
    }

    public int majoritySize() {
        return voteStates.size() / 2 + 1;
    }

    private long numGranted() {
        return voteStates.values().stream().filter(state -> state == VoteState.GRANTED).count();
    }

    private long numUnrecorded() {
        return voteStates.values().stream().filter(state -> state == VoteState.UNRECORDED).count();
    }

    /**
     * Check if the candidate is backing off for the next election
     */
    public boolean isBackingOff() {
        return isBackingOff;
    }

    public int retries() {
        return retries;
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
     * Record a granted vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @return true if the voter had not been previously recorded
     * @throws IllegalArgumentException if the remote node is not a voter or if the vote had already been
     *         rejected by this node
     */
    public boolean recordGrantedVote(int remoteNodeId) {
        VoteState voteState = voteStates.get(remoteNodeId);
        if (voteState == null) {
            throw new IllegalArgumentException("Attempt to grant vote to non-voter " + remoteNodeId);
        } else if (voteState == VoteState.REJECTED) {
            throw new IllegalArgumentException("Attempt to grant vote from node " + remoteNodeId +
                " which previously rejected our request");
        }
        return voteStates.put(remoteNodeId, VoteState.GRANTED) == VoteState.UNRECORDED;
    }

    /**
     * Record a rejected vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @return true if the rejected vote had not been previously recorded
     * @throws IllegalArgumentException if the remote node is not a voter or if the vote had already been
     *         granted by this node
     */
    public boolean recordRejectedVote(int remoteNodeId) {
        VoteState voteState = voteStates.get(remoteNodeId);
        if (voteState == null) {
            throw new IllegalArgumentException("Attempt to reject vote to non-voter " + remoteNodeId);
        } else if (voteState == VoteState.GRANTED) {
            throw new IllegalArgumentException("Attempt to reject vote from node " + remoteNodeId +
                " which previously granted our request");
        }

        return voteStates.put(remoteNodeId, VoteState.REJECTED) == VoteState.UNRECORDED;
    }

    /**
     * Record the current election has failed since we've either received sufficient rejecting voters or election timed out
     */
    public void startBackingOff() {
        isBackingOff = true;
    }

    /**
     * Get the set of voters which have not been counted as granted or rejected yet.
     *
     * @return The set of unrecorded voters
     */
    public Set<Integer> unrecordedVoters() {
        return voteStates.entrySet().stream()
            .filter(entry -> entry.getValue() == VoteState.UNRECORDED)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    /**
     * Get the set of voters that have rejected our candidacy.
     *
     * @return The set of rejecting voters
     */
    public Set<Integer> rejectingVoters() {
        return voteStates.entrySet().stream()
            .filter(entry -> entry.getValue() == VoteState.REJECTED)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    @Override
    public ElectionState election() {
        return ElectionState.withVotedCandidate(epoch, localId, voteStates.keySet());
    }

    @Override
    public LeaderAndEpoch leaderAndEpoch() {
        return new LeaderAndEpoch(OptionalInt.empty(), epoch);
    }

    @Override
    public int epoch() {
        return epoch;
    }

    private enum VoteState {
        UNRECORDED,
        GRANTED,
        REJECTED
    }
}
