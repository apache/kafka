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

import java.util.OptionalInt;
import java.util.Set;

/**
 * Encapsulate election state stored on disk after every state change.
 */
public class ElectionState {
    public final int epoch;
    public final OptionalInt leaderIdOpt;
    public final OptionalInt votedIdOpt;
    private final Set<Integer> voters;

    ElectionState(int epoch,
                  OptionalInt leaderIdOpt,
                  OptionalInt votedIdOpt,
                  Set<Integer> voters) {
        this.epoch = epoch;
        this.leaderIdOpt = leaderIdOpt;
        this.votedIdOpt = votedIdOpt;
        this.voters = voters;
    }

    public static ElectionState withVotedCandidate(int epoch, int votedId, Set<Integer> voters) {
        if (votedId < 0)
            throw new IllegalArgumentException("Illegal voted Id " + votedId + ": must be non-negative");
        if (!voters.contains(votedId))
            throw new IllegalArgumentException("Voted candidate with id " + votedId + " is not among the valid voters");
        return new ElectionState(epoch, OptionalInt.empty(), OptionalInt.of(votedId), voters);
    }

    public static ElectionState withElectedLeader(int epoch, int leaderId, Set<Integer> voters) {
        if (leaderId < 0)
            throw new IllegalArgumentException("Illegal leader Id " + leaderId + ": must be non-negative");
        if (!voters.contains(leaderId))
            throw new IllegalArgumentException("Leader with id " + leaderId + " is not among the valid voters");
        return new ElectionState(epoch, OptionalInt.of(leaderId), OptionalInt.empty(), voters);
    }

    public static ElectionState withUnknownLeader(int epoch, Set<Integer> voters) {
        return new ElectionState(epoch, OptionalInt.empty(), OptionalInt.empty(), voters);
    }

    public boolean isLeader(int nodeId) {
        if (nodeId < 0)
            throw new IllegalArgumentException("Invalid negative nodeId: " + nodeId);
        return leaderIdOrSentinel() == nodeId;
    }

    public boolean isVotedCandidate(int nodeId) {
        if (nodeId < 0)
            throw new IllegalArgumentException("Invalid negative nodeId: " + nodeId);
        return votedIdOpt.orElse(-1) == nodeId;
    }

    public int leaderId() {
        if (!leaderIdOpt.isPresent())
            throw new IllegalStateException("Attempt to access nil leaderId");
        return leaderIdOpt.getAsInt();
    }

    public int votedId() {
        if (!votedIdOpt.isPresent())
            throw new IllegalStateException("Attempt to access nil votedId");
        return votedIdOpt.getAsInt();
    }

    public Set<Integer> voters() {
        return voters;
    }

    public boolean hasLeader() {
        return leaderIdOpt.isPresent();
    }

    public boolean hasVoted() {
        return votedIdOpt.isPresent();
    }

    public int leaderIdOrSentinel() {
        return leaderIdOpt.orElse(-1);
    }


    @Override
    public String toString() {
        return "Election(epoch=" + epoch +
                ", leaderIdOpt=" + leaderIdOpt +
                ", votedIdOpt=" + votedIdOpt +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElectionState that = (ElectionState) o;

        if (epoch != that.epoch) return false;
        if (!leaderIdOpt.equals(that.leaderIdOpt)) return false;
        return votedIdOpt.equals(that.votedIdOpt);
    }

    @Override
    public int hashCode() {
        int result = epoch;
        result = 31 * result + leaderIdOpt.hashCode();
        result = 31 * result + votedIdOpt.hashCode();
        return result;
    }
}
