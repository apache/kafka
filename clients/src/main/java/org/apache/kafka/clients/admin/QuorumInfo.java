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
package org.apache.kafka.clients.admin;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * This class is used to describe the state of the quorum received in DescribeQuorumResponse.
 */
public class QuorumInfo {
    private final Integer leaderId;
    private final List<ReplicaState> voters;
    private final List<ReplicaState> observers;

    QuorumInfo(Integer leaderId, List<ReplicaState> voters, List<ReplicaState> observers) {
        this.leaderId = leaderId;
        this.voters = voters;
        this.observers = observers;
    }

    public Integer leaderId() {
        return leaderId;
    }

    public List<ReplicaState> voters() {
        return voters;
    }

    public List<ReplicaState> observers() {
        return observers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuorumInfo that = (QuorumInfo) o;
        return leaderId.equals(that.leaderId)
            && voters.equals(that.voters)
            && observers.equals(that.observers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderId, voters, observers);
    }

    @Override
    public String toString() {
        return "QuorumInfo(" +
            "leaderId=" + leaderId +
            ", voters=" + voters +
            ", observers=" + observers +
            ')';
    }

    public static class ReplicaState {
        private final int replicaId;
        private final long logEndOffset;
        private final OptionalLong lastFetchTimeMs;
        private final OptionalLong lastCaughtUpTimeMs;

        ReplicaState() {
            this(0, 0, OptionalLong.empty(), OptionalLong.empty());
        }

        ReplicaState(
            int replicaId,
            long logEndOffset,
            OptionalLong lastFetchTimeMs,
            OptionalLong lastCaughtUpTimeMs
        ) {
            this.replicaId = replicaId;
            this.logEndOffset = logEndOffset;
            this.lastFetchTimeMs = lastFetchTimeMs;
            this.lastCaughtUpTimeMs = lastCaughtUpTimeMs;
        }

        /**
         * Return the ID for this replica.
         * @return The ID for this replica
         */
        public int replicaId() {
            return replicaId;
        }

        /**
         * Return the logEndOffset known by the leader for this replica.
         * @return The logEndOffset for this replica
         */
        public long logEndOffset() {
            return logEndOffset;
        }

        /**
         * Return the lastFetchTime in milliseconds for this replica.
         * @return The value of the lastFetchTime if known, empty otherwise
         */
        public OptionalLong lastFetchTimeMs() {
            return lastFetchTimeMs;
        }

        /**
         * Return the lastCaughtUpTime in milliseconds for this replica.
         * @return The value of the lastCaughtUpTime if known, empty otherwise
         */
        public OptionalLong lastCaughtUpTimeMs() {
            return lastCaughtUpTimeMs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReplicaState that = (ReplicaState) o;
            return replicaId == that.replicaId
                && logEndOffset == that.logEndOffset
                && lastFetchTimeMs.equals(that.lastFetchTimeMs)
                && lastCaughtUpTimeMs.equals(that.lastCaughtUpTimeMs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicaId, logEndOffset, lastFetchTimeMs, lastCaughtUpTimeMs);
        }

        @Override
        public String toString() {
            return "ReplicaState(" +
                "replicaId=" + replicaId +
                ", logEndOffset=" + logEndOffset +
                ", lastFetchTimeMs=" + lastFetchTimeMs +
                ", lastCaughtUpTimeMs=" + lastCaughtUpTimeMs +
                ')';
        }
    }
}
