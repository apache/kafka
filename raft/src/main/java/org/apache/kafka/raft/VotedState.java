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

import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.internals.ReplicaKey;
import org.slf4j.Logger;

/**
 * The "voted" state is for voters who have cast their vote for a specific candidate.
 *
 * Once a vote has been cast, it is not possible for a voter to change its vote until a
 * new election is started. If the election timeout expires before a new leader is elected,
 * then the voter will become a candidate.
 */
public class VotedState implements EpochState {
    private final int epoch;
    private final ReplicaKey votedKey;
    private final Set<Integer> voters;
    private final int electionTimeoutMs;
    private final Timer electionTimer;
    private final Optional<LogOffsetMetadata> highWatermark;
    private final Logger log;

    public VotedState(
        Time time,
        int epoch,
        ReplicaKey votedKey,
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark,
        int electionTimeoutMs,
        LogContext logContext
    ) {
        this.epoch = epoch;
        this.votedKey = votedKey;
        this.voters = voters;
        this.highWatermark = highWatermark;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.log = logContext.logger(VotedState.class);
    }

    @Override
    public ElectionState election() {
        return ElectionState.withVotedCandidate(epoch, votedKey, voters);
    }

    public ReplicaKey votedKey() {
        return votedKey;
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public String name() {
        return "Voted";
    }

    public long remainingElectionTimeMs(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.remainingMs();
    }

    public boolean hasElectionTimeoutExpired(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.isExpired();
    }

    @Override
    public boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate) {
        if (votedKey.id() == candidateKey.id()) {
            return !votedKey.directoryId().isPresent() || votedKey.directoryId().equals(candidateKey.directoryId());
        }

        log.debug(
            "Rejecting vote request from candidate ({}), already have voted for another " +
            "candidate ({}) in epoch {}",
            candidateKey,
            votedKey,
            epoch
        );

        return false;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public String toString() {
        return String.format(
            "Voted(epoch=%d, votedKey=%s, voters=%s, electionTimeoutMs=%d, highWatermark=%s)",
            epoch,
            votedKey,
            voters,
            electionTimeoutMs,
            highWatermark
        );
    }

    @Override
    public void close() {}
}
