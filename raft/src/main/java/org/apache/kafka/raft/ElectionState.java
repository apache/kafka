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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.raft.generated.QuorumStateData;

/**
 * Encapsulate election state stored on disk after every state change.
 */
final public class ElectionState {
    private static int unknownLeaderId = -1;
    private static int notVoted = -1;
    private static Uuid noVotedDirectoryId = Uuid.ZERO_UUID;

    private final int epoch;
    private final OptionalInt leaderId;
    private final OptionalInt votedId;
    private final Optional<Uuid> votedDirectoryId;
    // This is deprecated. It is only used when writing version 0 of the quorum state file
    private final Set<Integer> voters;

    ElectionState(
        int epoch,
        OptionalInt leaderId,
        OptionalInt votedId,
        Optional<Uuid> votedDirectoryId,
        Set<Integer> voters
    ) {
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.votedId = votedId;
        this.votedDirectoryId = votedDirectoryId;
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

    /**
     * Return if the replica has voted for the given candidate.
     *
     * A replica has voted for a candidate if all of the following are true:
     * 1. the nodeId and votedId match and
     * 2. if the votedDirectoryId is set, it matches the nodeDirectoryId
     *
     * @param nodeId id of the replica
     * @param nodeDirectoryId directory id of the replica if it exist
     * @return true when the arguments match, otherwise false
     */
    public boolean isVotedCandidate(int nodeId, Optional<Uuid> nodeDirectoryId) {
        if (nodeId < 0) {
            throw new IllegalArgumentException("Invalid negative nodeId: " + nodeId);
        } else if (votedId.orElse(-1) != nodeId) {
            return false;
        } else if (!votedDirectoryId.isPresent()) {
            // when the persisted voted uuid is not present assume that we voted for this candidate;
            // this happends when the kraft version is 0.
            return true;
        }

        return votedDirectoryId.equals(nodeDirectoryId);
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

    public OptionalInt optionalVotedId() {
        return votedId;
    }

    public Optional<Uuid> votedDirectoryId() {
        return votedDirectoryId;
    }

    public boolean hasLeader() {
        return leaderId.isPresent();
    }

    public boolean hasVoted() {
        return votedId.isPresent();
    }

    public QuorumStateData toQourumStateData(short version) {
        QuorumStateData data = new QuorumStateData()
            .setLeaderEpoch(epoch)
            .setLeaderId(hasLeader() ? leaderId() : unknownLeaderId)
            .setVotedId(hasVoted() ? votedId() : notVoted);

        if (version == 0) {
            List<QuorumStateData.Voter> dataVoters = voters
                .stream()
                .map(voterId -> new QuorumStateData.Voter().setVoterId(voterId))
                .collect(Collectors.toList());
            data.setCurrentVoters(dataVoters);
        } else if (version == 1) {
            data.setVotedDirectoryId(votedDirectoryId().isPresent() ? votedDirectoryId().get() : noVotedDirectoryId);
        } else {
            throw new IllegalStateException(
                String.format(
                    "File quorum state store doesn't handle supported version %d", version
                )
            );
        }

        return data;
    }

    @Override
    public String toString() {
        return String.format(
            "Election(epoch=%d, leaderId=%s, votedId=%s, votedDirectoryId=%s, voters=%s)",
            epoch,
            leaderId,
            votedId,
            votedDirectoryId,
            voters
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElectionState that = (ElectionState) o;

        if (epoch != that.epoch) return false;
        if (!leaderId.equals(that.leaderId)) return false;
        if (!votedId.equals(that.votedId)) return false;
        if (!votedDirectoryId.equals(that.votedDirectoryId)) return false;

        return voters.equals(that.voters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, leaderId, votedId, votedDirectoryId, voters);
    }

    public static ElectionState withVotedCandidate(int epoch, int votedId, Optional<Uuid> votedDirectoryId, Set<Integer> voters) {
        if (votedId < 0) {
            throw new IllegalArgumentException("Illegal voted Id " + votedId + ": must be non-negative");
        }

        return new ElectionState(epoch, OptionalInt.empty(), OptionalInt.of(votedId), votedDirectoryId, voters);
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

    public static ElectionState fromQuorumStateData(QuorumStateData data) {
        return new ElectionState(
            data.leaderEpoch(),
            data.leaderId() == unknownLeaderId ? OptionalInt.empty() : OptionalInt.of(data.leaderId()),
            data.votedId() == notVoted ? OptionalInt.empty() : OptionalInt.of(data.votedId()),
            data.votedDirectoryId().equals(noVotedDirectoryId) ? Optional.empty() : Optional.of(data.votedDirectoryId()),
            data.currentVoters().stream().map(QuorumStateData.Voter::voterId).collect(Collectors.toSet())
        );
    }
}
