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

import org.apache.kafka.raft.generated.QuorumStateData;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Encapsulate election state stored on disk after every state change.
 */
public final class ElectionState {
    private static final int UNKNOWN_LEADER_ID = -1;
    private static final int NOT_VOTED = -1;

    private final int epoch;
    private final OptionalInt leaderId;
    private final Optional<ReplicaKey> votedKey;
    // This is deprecated. It is only used when writing version 0 of the quorum state file
    private final Set<Integer> voters;

    ElectionState(
        int epoch,
        OptionalInt leaderId,
        Optional<ReplicaKey> votedKey,
        Set<Integer> voters
    ) {
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.votedKey = votedKey;
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
     * 1. the node's id and voted id match and
     * 2. if the voted directory id is set, it matches the node's directory id
     *
     * @param nodeKey the id and directory id of the replica
     * @return true when the arguments match, otherwise false
     */
    public boolean isVotedCandidate(ReplicaKey nodeKey) {
        if (nodeKey.id() < 0) {
            throw new IllegalArgumentException("Invalid node key " + nodeKey);
        } else if (votedKey.isEmpty()) {
            return false;
        } else if (votedKey.get().id() != nodeKey.id()) {
            return false;
        } else if (votedKey.get().directoryId().isEmpty()) {
            // when the persisted voted directory id is not present assume that we voted for this candidate;
            // this happens when the kraft version is 0.
            return true;
        }

        return votedKey.get().directoryId().equals(nodeKey.directoryId());
    }

    public int leaderId() {
        if (leaderId.isEmpty())
            throw new IllegalStateException("Attempt to access nil leaderId");
        return leaderId.getAsInt();
    }

    public int leaderIdOrSentinel() {
        return leaderId.orElse(UNKNOWN_LEADER_ID);
    }

    public OptionalInt optionalLeaderId() {
        return leaderId;
    }

    public ReplicaKey votedKey() {
        if (votedKey.isEmpty()) {
            throw new IllegalStateException("Attempt to access nil votedId");
        }

        return votedKey.get();
    }

    public Optional<ReplicaKey> optionalVotedKey() {
        return votedKey;
    }

    public boolean hasLeader() {
        return leaderId.isPresent();
    }

    public boolean hasVoted() {
        return votedKey.isPresent();
    }

    public QuorumStateData toQuorumStateData(short version) {
        QuorumStateData data = new QuorumStateData()
            .setLeaderEpoch(epoch)
            .setLeaderId(leaderIdOrSentinel())
            .setVotedId(votedKey.map(ReplicaKey::id).orElse(NOT_VOTED));

        if (version == 0) {
            List<QuorumStateData.Voter> dataVoters = voters
                .stream()
                .map(voterId -> new QuorumStateData.Voter().setVoterId(voterId))
                .collect(Collectors.toList());
            data.setCurrentVoters(dataVoters);
        } else if (version == 1) {
            data.setVotedDirectoryId(
                votedKey.flatMap(ReplicaKey::directoryId).orElse(ReplicaKey.NO_DIRECTORY_ID)
            );
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
            "Election(epoch=%d, leaderId=%s, votedKey=%s, voters=%s)",
            epoch,
            leaderId,
            votedKey,
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
        if (!votedKey.equals(that.votedKey)) return false;

        return voters.equals(that.voters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, leaderId, votedKey, voters);
    }

    public static ElectionState withVotedCandidate(int epoch, ReplicaKey votedKey, Set<Integer> voters) {
        if (votedKey.id() < 0) {
            throw new IllegalArgumentException("Illegal voted Id " + votedKey.id() + ": must be non-negative");
        }

        return new ElectionState(epoch, OptionalInt.empty(), Optional.of(votedKey), voters);
    }

    public static ElectionState withElectedLeader(int epoch, int leaderId, Set<Integer> voters) {
        if (leaderId < 0) {
            throw new IllegalArgumentException("Illegal leader Id " + leaderId + ": must be non-negative");
        }

        return new ElectionState(epoch, OptionalInt.of(leaderId), Optional.empty(), voters);
    }

    public static ElectionState withUnknownLeader(int epoch, Set<Integer> voters) {
        return new ElectionState(epoch, OptionalInt.empty(), Optional.empty(), voters);
    }

    public static ElectionState fromQuorumStateData(QuorumStateData data) {
        Optional<ReplicaKey> votedKey = data.votedId() == NOT_VOTED ?
            Optional.empty() :
            Optional.of(ReplicaKey.of(data.votedId(), data.votedDirectoryId()));

        return new ElectionState(
            data.leaderEpoch(),
            data.leaderId() == UNKNOWN_LEADER_ID ? OptionalInt.empty() : OptionalInt.of(data.leaderId()),
            votedKey,
            data.currentVoters().stream().map(QuorumStateData.Voter::voterId).collect(Collectors.toSet())
        );
    }
}
