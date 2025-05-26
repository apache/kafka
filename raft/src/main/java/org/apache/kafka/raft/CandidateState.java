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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.internals.EpochElection;

import org.slf4j.Logger;

import java.util.Optional;

public class CandidateState implements NomineeState {
    private final int localId;
    private final Uuid localDirectoryId;
    private final int epoch;
    private final EpochElection epochElection;
    private final Optional<LogOffsetMetadata> highWatermark;
    private final int electionTimeoutMs;
    private final Timer electionTimer;
    private final Logger log;

    /**
     * The lifetime of a candidate state is the following.
     *
     *  1. Once started, it will send vote requests and keep record of the received vote responses.
     *  2. If majority votes granted, it will transition to leader state.
     *  3. If majority votes rejected, it will transition to prospective after a backoff phase.
     *  4. If election times out, it will transition immediately to prospective.
     */
    protected CandidateState(
        Time time,
        int localId,
        Uuid localDirectoryId,
        int epoch,
        VoterSet voters,
        Optional<LogOffsetMetadata> highWatermark,
        int electionTimeoutMs,
        LogContext logContext
    ) {
        if (!voters.isVoter(ReplicaKey.of(localId, localDirectoryId))) {
            throw new IllegalArgumentException(
                String.format(
                    "Local replica (%d, %s) must be in the set of voters %s",
                    localId,
                    localDirectoryId,
                    voters
                )
            );
        }

        this.localId = localId;
        this.localDirectoryId = localDirectoryId;
        this.epoch = epoch;
        this.highWatermark = highWatermark;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.log = logContext.logger(CandidateState.class);

        this.epochElection = new EpochElection(voters.voterKeys());
        epochElection.recordVote(localId, true);
    }

    @Override
    public EpochElection epochElection() {
        return epochElection;
    }

    @Override
    public boolean recordGrantedVote(int remoteNodeId) {
        if (epochElection().isRejectedVoter(remoteNodeId)) {
            throw new IllegalArgumentException("Attempt to grant vote from node " + remoteNodeId +
                " which previously rejected our request");
        }
        return epochElection().recordVote(remoteNodeId, true);
    }

    @Override
    public boolean recordRejectedVote(int remoteNodeId) {
        if (epochElection().isGrantedVoter(remoteNodeId)) {
            throw new IllegalArgumentException("Attempt to reject vote from node " + remoteNodeId +
                " which previously granted our request");
        }
        return epochElection().recordVote(remoteNodeId, false);
    }

    @Override
    public boolean hasElectionTimeoutExpired(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.isExpired();
    }

    @Override
    public long remainingElectionTimeMs(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.remainingMs();
    }

    @Override
    public ElectionState election() {
        return ElectionState.withVotedCandidate(
            epoch,
            ReplicaKey.of(localId, localDirectoryId),
            epochElection.voterIds()
        );
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public Endpoints leaderEndpoints() {
        return Endpoints.empty();
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public boolean canGrantVote(
        ReplicaKey replicaKey,
        boolean isLogUpToDate,
        boolean isPreVote
    ) {
        if (isPreVote && isLogUpToDate) {
            return true;
        }
        // Reject standard vote requests even if replicaId = localId, although the replica votes for
        // itself, this vote is implicit and not "granted".
        log.debug(
            "Rejecting Vote request (preVote={}) from replica ({}) since we are in CandidateState in epoch {} " +
                "and the replica's log is up-to-date={}",
            isPreVote,
            replicaKey,
            epoch,
            isLogUpToDate
        );
        return false;
    }

    @Override
    public String toString() {
        return String.format(
            "CandidateState(localId=%d, localDirectoryId=%s, epoch=%d, epochElection=%s, " +
            "highWatermark=%s, electionTimeoutMs=%d)",
            localId,
            localDirectoryId,
            epoch,
            epochElection(),
            highWatermark,
            electionTimeoutMs
        );
    }

    @Override
    public String name() {
        return "Candidate";
    }

    @Override
    public void close() {}
}
