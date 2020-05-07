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

/**
 * Encapsulate election state stored on disk after every state change.
 */
public class ElectionState {
    public final int epoch;
    private final OptionalInt leaderIdOpt;
    private final OptionalInt votedIdOpt;

    ElectionState(int epoch,
                  OptionalInt leaderIdOpt,
                  OptionalInt votedIdOpt) {
        this.epoch = epoch;
        this.leaderIdOpt = leaderIdOpt;
        this.votedIdOpt = votedIdOpt;
    }

    public static ElectionState withVotedCandidate(int epoch, int votedId) {
        if (votedId < 0)
            throw new IllegalArgumentException("Illegal voted Id " + votedId + ": must be non-negative");
        return new ElectionState(epoch, OptionalInt.empty(), OptionalInt.of(votedId));
    }

    public static ElectionState withElectedLeader(int epoch, int leaderId) {
        if (leaderId < 0)
            throw new IllegalArgumentException("Illegal leader Id " + leaderId + ": must be non-negative");
        return new ElectionState(epoch, OptionalInt.of(leaderId), OptionalInt.empty());
    }

    public static ElectionState withUnknownLeader(int epoch) {
        return new ElectionState(epoch, OptionalInt.empty(), OptionalInt.empty());
    }

    public boolean isLeader(int nodeId) {
        if (nodeId < 0)
            throw new IllegalArgumentException("Invalid negative nodeId: " + nodeId);
        return leaderIdOpt.orElse(-1) == nodeId;
    }

    public boolean isCandidate(int nodeId) {
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

    public boolean hasLeader() {
        return leaderIdOpt.isPresent();
    }

    public boolean hasVoted() {
        return votedIdOpt.isPresent();
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
