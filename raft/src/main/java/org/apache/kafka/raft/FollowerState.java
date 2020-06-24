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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

public class FollowerState implements EpochState {
    private final int epoch;
    private OptionalInt leaderIdOpt;
    private OptionalInt votedIdOpt;
    private OptionalLong highWatermark;
    private final Set<Integer> voters;

    public FollowerState(int epoch, Set<Integer> voters) {
        this.epoch = epoch;
        this.leaderIdOpt = OptionalInt.empty();
        this.votedIdOpt = OptionalInt.empty();
        this.highWatermark = OptionalLong.empty();
        this.voters = voters;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark.isPresent() ? Optional.of(new LogOffsetMetadata(highWatermark.getAsLong())) : Optional.empty();
    }

    @Override
    public ElectionState election() {
        if (votedIdOpt.isPresent())
            return ElectionState.withVotedCandidate(epoch, votedIdOpt.getAsInt(), voters);
        if (leaderIdOpt.isPresent())
            return ElectionState.withElectedLeader(epoch, leaderIdOpt.getAsInt(), voters);
        return ElectionState.withUnknownLeader(epoch, voters);
    }

    @Override
    public int epoch() {
        return epoch;
    }

    /**
     * Grant a vote to the candidate. The vote is permitted only if we had already voted for
     * the candidate or if we have no current leader and have not voted in this epoch.
     *
     * @param candidateId The candidate we are voting for
     * @return true if we had not already cast our vote
     */
    public boolean grantVoteTo(int candidateId) {
        if (candidateId < 0) {
            throw new IllegalArgumentException("Illegal negative candidateId: " + candidateId);
        } else if (hasLeader()) {
            throw new IllegalArgumentException("Cannot vote in epoch " + epoch +
                    " since we already have a known leader for epoch");
        } else if (hasVoted()) {
            if (votedIdOpt.orElse(-1) != candidateId) {
                throw new IllegalArgumentException("Cannot change vote in epoch " + epoch +
                        " from " + votedIdOpt + " to " + candidateId);
            }
            return false;
        }

        this.votedIdOpt = OptionalInt.of(candidateId);
        return true;
    }

    public boolean hasLeader() {
        return leaderIdOpt.isPresent();
    }

    public boolean hasLeader(int replicaId) {
        if (replicaId < 0)
            throw new IllegalArgumentException("Illegal negative replicaId " + replicaId);
        return leaderIdOpt.orElse(-1) == replicaId;
    }

    public boolean acknowledgeLeader(int leaderId) {
        if (leaderId < 0) {
            throw new IllegalArgumentException("Invalid negative leaderId: " + leaderId);
        } else if (hasLeader()) {
            if (leaderIdOpt.orElse(-1) != leaderId) {
                throw new IllegalArgumentException("Cannot acknowledge leader " + leaderId +
                        " in epoch " + epoch + " since we have already acknowledged " + leaderIdOpt);
            }
            return false;
        }

        votedIdOpt = OptionalInt.empty();
        leaderIdOpt = OptionalInt.of(leaderId);
        return true;
    }

    public int leaderId() {
        if (!leaderIdOpt.isPresent()) {
            throw new IllegalArgumentException("Cannot access leaderId of epoch " + epoch +
                    " since we do not know it");
        }
        return leaderIdOpt.getAsInt();
    }

    public boolean hasVoted() {
        return votedIdOpt.isPresent();
    }

    public boolean hasVotedFor(int candidateId) {
        if (candidateId < 0)
            throw new IllegalArgumentException("Illegal negative candidateId " + candidateId);
        return votedIdOpt.orElse(-1) == candidateId;
    }

    public int votedId() {
        if (!votedIdOpt.isPresent()) {
            throw new IllegalArgumentException("Cannot access voted id of epoch " + epoch +
                    " since we do not know it");
        }
        return votedIdOpt.getAsInt();
    }

    public void updateHighWatermark(OptionalLong highWatermark) {
        if (!hasLeader())
            throw new IllegalArgumentException("Cannot update high watermark without an acknowledged leader");
        if (!highWatermark.isPresent() && this.highWatermark.isPresent())
            throw new IllegalArgumentException("Attempt to overwrite current high watermark " + this.highWatermark +
                    " with unknown value");
        this.highWatermark.ifPresent(previousHighWatermark -> {
            long updatedHighWatermark = highWatermark.getAsLong();
            if (updatedHighWatermark < 0)
                throw new IllegalArgumentException("Illegal negative high watermark update");
            if (previousHighWatermark > highWatermark.getAsLong())
                throw new IllegalArgumentException("Non-monotonic update of high watermark attempted");
        });

        this.highWatermark = highWatermark;
    }

    public boolean detachLeader() {
        if (hasLeader()) {
            leaderIdOpt = OptionalInt.empty();
            return true;
        }
        return false;
    }

    public boolean assertNotAttached() {
        if (hasLeader())
            throw new IllegalArgumentException("Unattached assertion failed since we have a current leader");
        if (hasVoted())
            throw new IllegalArgumentException("Unattached assertion failed since we have a voted candidate");
        return true;
    }

    public boolean isUnattached() {
        return !hasVoted() && !hasLeader();
    }
}
