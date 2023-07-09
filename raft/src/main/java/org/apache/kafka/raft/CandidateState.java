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
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CandidateState implements EpochState {
    private final int localId;
    private final int epoch;
    private final int retries;
    private final Map<Integer, State> voteStates = new HashMap<>();
    private final Optional<LogOffsetMetadata> highWatermark;
    private final int electionTimeoutMs;
    private final Timer electionTimer;
    private final Timer backoffTimer;
    private final Logger log;

    /**
     * The lifetime of a candidate state is the following:
     *
     *  1. Once started, it would keep record of the received votes.
     *  2. If majority votes granted, it can then end its life and will be replaced by a leader state;
     *  3. If majority votes rejected or election timed out, it would transit into a backing off phase;
     *     after the backoff phase completes, it would end its left and be replaced by a new candidate state with bumped retry.
     */
    private boolean isBackingOff;

    protected CandidateState(
        Time time,
        int localId,
        int epoch,
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark,
        int retries,
        int electionTimeoutMs,
        LogContext logContext
    ) {
        this.localId = localId;
        this.epoch = epoch;
        this.highWatermark = highWatermark;
        this.retries = retries;
        this.isBackingOff = false;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.backoffTimer = time.timer(0);
        this.log = logContext.logger(CandidateState.class);

        for (Integer voterId : voters) {
            voteStates.put(voterId, State.UNRECORDED);
        }
        voteStates.put(localId, State.GRANTED);
    }

    public int localId() {
        return localId;
    }

    public int majoritySize() {
        return voteStates.size() / 2 + 1;
    }

    private long numGranted() {
        return voteStates.values().stream().filter(state -> state == State.GRANTED).count();
    }

    private long numUnrecorded() {
        return voteStates.values().stream().filter(state -> state == State.UNRECORDED).count();
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
        State state = voteStates.get(remoteNodeId);
        if (state == null) {
            throw new IllegalArgumentException("Attempt to grant vote to non-voter " + remoteNodeId);
        } else if (state == State.REJECTED) {
            throw new IllegalArgumentException("Attempt to grant vote from node " + remoteNodeId +
                " which previously rejected our request");
        }
        return voteStates.put(remoteNodeId, State.GRANTED) == State.UNRECORDED;
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
        State state = voteStates.get(remoteNodeId);
        if (state == null) {
            throw new IllegalArgumentException("Attempt to reject vote to non-voter " + remoteNodeId);
        } else if (state == State.GRANTED) {
            throw new IllegalArgumentException("Attempt to reject vote from node " + remoteNodeId +
                " which previously granted our request");
        }

        return voteStates.put(remoteNodeId, State.REJECTED) == State.UNRECORDED;
    }

    /**
     * Record the current election has failed since we've either received sufficient rejecting voters or election timed out
     */
    public void startBackingOff(long currentTimeMs, long backoffDurationMs) {
        this.backoffTimer.update(currentTimeMs);
        this.backoffTimer.reset(backoffDurationMs);
        this.isBackingOff = true;
    }

    /**
     * Get the set of voters which have not been counted as granted or rejected yet.
     *
     * @return The set of unrecorded voters
     */
    public Set<Integer> unrecordedVoters() {
        return votersInState(State.UNRECORDED);
    }

    /**
     * Get the set of voters that have granted our vote requests.
     *
     * @return The set of granting voters, which should always contain the ID of the candidate
     */
    public Set<Integer> grantingVoters() {
        return votersInState(State.GRANTED);
    }

    /**
     * Get the set of voters that have rejected our candidacy.
     *
     * @return The set of rejecting voters
     */
    public Set<Integer> rejectingVoters() {
        return votersInState(State.REJECTED);
    }

    private Set<Integer> votersInState(State state) {
        return voteStates.entrySet().stream()
            .filter(entry -> entry.getValue() == state)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    public boolean hasElectionTimeoutExpired(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.isExpired();
    }

    public boolean isBackoffComplete(long currentTimeMs) {
        backoffTimer.update(currentTimeMs);
        return backoffTimer.isExpired();
    }

    public long remainingBackoffMs(long currentTimeMs) {
        if (!isBackingOff) {
            throw new IllegalStateException("Candidate is not currently backing off");
        }
        backoffTimer.update(currentTimeMs);
        return backoffTimer.remainingMs();
    }

    public long remainingElectionTimeMs(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.remainingMs();
    }

    @Override
    public ElectionState election() {
        return ElectionState.withVotedCandidate(epoch, localId, voteStates.keySet());
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public boolean canGrantVote(int candidateId, boolean isLogUpToDate) {
        // Still reject vote request even candidateId = localId, Although the candidate votes for
        // itself, this vote is implicit and not "granted".
        log.debug("Rejecting vote request from candidate {} since we are already candidate in epoch {}",
            candidateId, epoch);
        return false;
    }

    @Override
    public String toString() {
        return "CandidateState(" +
            "localId=" + localId +
            ", epoch=" + epoch +
            ", retries=" + retries +
            ", voteStates=" + voteStates +
            ", highWatermark=" + highWatermark +
            ", electionTimeoutMs=" + electionTimeoutMs +
            ')';
    }

    @Override
    public String name() {
        return "Candidate";
    }

    @Override
    public void close() {}

    private enum State {
        UNRECORDED,
        GRANTED,
        REJECTED
    }
}
