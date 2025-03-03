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

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * A replica is "unattached" when it doesn't know the leader or the leader's endpoint.
 *
 * Typically, a replica doesn't know the leader if the KRaft topic is undergoing an election cycle.
 *
 * It is also possible for a replica to be unattached if it doesn't know the leader's endpoint.
 * This typically happens when a replica starts up and the known leader id is not part of the local
 * voter set. In that case, during startup the replica transitions to unattached instead of
 * transitioning to follower. The unattached replica discovers the leader and leader's endpoint
 * either through random Fetch requests to the bootstrap servers or through BeginQuorumEpoch
 * request from the leader.
 */

public class UnattachedState implements EpochState {
    private final int epoch;
    private final OptionalInt leaderId;
    private final Optional<ReplicaKey> votedKey;
    private final Set<Integer> voters;
    private final long electionTimeoutMs;
    private final Timer electionTimer;
    private final Optional<LogOffsetMetadata> highWatermark;
    private final Logger log;

    public UnattachedState(
        Time time,
        int epoch,
        OptionalInt leaderId,
        Optional<ReplicaKey> votedKey,
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark,
        long electionTimeoutMs,
        LogContext logContext
    ) {
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.votedKey = votedKey;
        this.voters = voters;
        this.highWatermark = highWatermark;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.log = logContext.logger(UnattachedState.class);
    }

    @Override
    public ElectionState election() {
        if (votedKey.isPresent()) {
            return ElectionState.withVotedCandidate(epoch, votedKey().get(), voters);
        } else if (leaderId.isPresent()) {
            return ElectionState.withElectedLeader(epoch, leaderId.getAsInt(), voters);
        } else {
            return ElectionState.withUnknownLeader(epoch, voters);
        }
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
    public String name() {
        return "Unattached";
    }

    public Optional<ReplicaKey> votedKey() {
        return votedKey;
    }

    public long electionTimeoutMs() {
        return electionTimeoutMs;
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
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public boolean canGrantVote(ReplicaKey replicaKey, boolean isLogUpToDate, boolean isPreVote) {
        if (isPreVote) {
            return canGrantPreVote(replicaKey, isLogUpToDate);
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
                leaderId,
                epoch
            );
            return false;
        } else if (!isLogUpToDate) {
            log.debug(
                "Rejecting Vote request (preVote=false) from candidate ({}) since candidate epoch/offset is not up to date with us",
                replicaKey
            );
        }

        return isLogUpToDate;
    }

    private boolean canGrantPreVote(ReplicaKey replicaKey, boolean isLogUpToDate) {
        if (!isLogUpToDate) {
            log.debug(
                "Rejecting Vote request (preVote=true) from replica ({}) since replica's log is not up to date with us",
                replicaKey
            );
        }

        return isLogUpToDate;
    }

    @Override
    public String toString() {
        return "Unattached(" +
            "epoch=" + epoch +
            ", votedKey=" + votedKey.map(ReplicaKey::toString).orElse("null") +
            ", voters=" + voters +
            ", electionTimeoutMs=" + electionTimeoutMs +
            ", highWatermark=" + highWatermark +
            ')';
    }

    @Override
    public void close() {}
}
