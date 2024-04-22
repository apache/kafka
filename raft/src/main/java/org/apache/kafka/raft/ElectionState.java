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

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.apache.kafka.common.Uuid;

/**
 * Encapsulate election state stored on disk after every state change.
 */
final public class ElectionState {
    private final int epoch;
    private final OptionalInt leaderId;
    private final OptionalInt votedId;
    private final Optional<Uuid> votedUuid;
    private final Set<Integer> voters;

    ElectionState(
        int epoch,
        OptionalInt leaderId,
        OptionalInt votedId,
        Optional<Uuid> votedUuid,
        Set<Integer> voters
    ) {
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.votedId = votedId;
        this.votedUuid = votedUuid;
        this.voters = voters;
    }

    public int epoch() {
        return epoch;
    }

    public boolean isLeader(int nodeId) {
        if (nodeId < 0)
            throw new IllegalArgumentException("Invalid negative nodeId: " + nodeId);
        return leaderIdOrSentinel() == nodeId;
    }

    public boolean isVotedCandidate(int nodeId) {
        if (nodeId < 0)
            throw new IllegalArgumentException("Invalid negative nodeId: " + nodeId);
        return votedId.orElse(-1) == nodeId;
    }

    public int leaderId() {
        if (!leaderId.isPresent())
            throw new IllegalStateException("Attempt to access nil leaderId");
        return leaderId.getAsInt();
    }

    public int leaderIdOrSentinel() {
        return leaderId.orElse(-1);
    }

    public OptionalInt optionalLeaderId() {
        return leaderId;
    }

    public int votedId() {
        if (!votedId.isPresent())
            throw new IllegalStateException("Attempt to access nil votedId");
        return votedId.getAsInt();
    }

    public Optional<Uuid> votedUuid() {
        return votedUuid;
    }

    // TODO: remove this and see what breaks
    public Set<Integer> voters() {
        return voters;
    }

    public boolean hasLeader() {
        return leaderId.isPresent();
    }

    public boolean hasVoted() {
        return votedId.isPresent();
    }

    @Override
    public String toString() {
        return String.format(
            "Election(epoch=%d, leaderId=%s, votedId=%s, votedUuid=%s, voters=%s)",
            epoch,
            leaderId,
            votedId,
            votedUuid,
            voters
        );
    }

    // TODO: Since I changed the implementation, confirm that raft/src/main doesn't call ElectionState's equals or hashCode
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElectionState that = (ElectionState) o;

        if (epoch != that.epoch) return false;
        if (!leaderId.equals(that.leaderId)) return false;
        if (!votedId.equals(that.votedId)) return false;
        if (!votedUuid.equals(that.votedUuid)) return false;

        return !voters.equals(that.voters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, leaderId, votedId, votedUuid, voters);
    }

    public static ElectionState withVotedCandidate(int epoch, int votedId, Optional<Uuid> votedUuid, Set<Integer> voters) {
        if (votedId < 0) {
            throw new IllegalArgumentException("Illegal voted Id " + votedId + ": must be non-negative");
        }

        return new ElectionState(epoch, OptionalInt.empty(), OptionalInt.of(votedId), votedUuid, voters);
    }

    public static ElectionState withElectedLeader(int epoch, int leaderId, Set<Integer> voters) {
        if (leaderId < 0) {
            throw new IllegalArgumentException("Illegal leader Id " + leaderId + ": must be non-negative");
        }

        return new ElectionState(epoch, OptionalInt.of(leaderId), OptionalInt.empty(), Optional.empty(), voters);
    }

    public static ElectionState withUnknownLeader(int epoch, Set<Integer> voters) {
        return new ElectionState(epoch, OptionalInt.empty(), OptionalInt.empty(), Optional.empty(), voters);
    }
}
