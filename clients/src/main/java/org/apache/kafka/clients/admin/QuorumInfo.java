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

import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * This class is used to describe the state of the quorum received in DescribeQuorumResponse.
 */
public class QuorumInfo {
    private final int leaderId;
    private final long leaderEpoch;
    private final long highWatermark;
    private final List<ReplicaState> voters;
    private final List<ReplicaState> observers;
    private final Map<Integer, Node> nodes;

    QuorumInfo(
        int leaderId,
        long leaderEpoch,
        long highWatermark,
        List<ReplicaState> voters,
        List<ReplicaState> observers,
        Map<Integer, Node> nodes
    ) {
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.highWatermark = highWatermark;
        this.voters = voters;
        this.observers = observers;
        this.nodes = nodes;
    }

    public int leaderId() {
        return leaderId;
    }

    public long leaderEpoch() {
        return leaderEpoch;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public List<ReplicaState> voters() {
        return voters;
    }

    public List<ReplicaState> observers() {
        return observers;
    }

    /**
     * @return The voter nodes in the Raft cluster, or an empty map if KIP-853 is not enabled.
     */
    public Map<Integer, Node> nodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuorumInfo that = (QuorumInfo) o;
        return leaderId == that.leaderId
            && leaderEpoch == that.leaderEpoch
            && highWatermark == that.highWatermark
            && Objects.equals(voters, that.voters)
            && Objects.equals(observers, that.observers)
            && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderId, leaderEpoch, highWatermark, voters, observers, nodes);
    }

    @Override
    public String toString() {
        return "QuorumInfo(" +
            "leaderId=" + leaderId +
            ", leaderEpoch=" + leaderEpoch +
            ", highWatermark=" + highWatermark +
            ", voters=" + voters +
            ", observers=" + observers +
            ", nodes=" + nodes +
            ')';
    }

    public static class ReplicaState {
        private final int replicaId;
        private final Uuid replicaDirectoryId;
        private final long logEndOffset;
        private final OptionalLong lastFetchTimestamp;
        private final OptionalLong lastCaughtUpTimestamp;

        ReplicaState() {
            this(0, Uuid.ZERO_UUID, 0, OptionalLong.empty(), OptionalLong.empty());
        }

        ReplicaState(
            int replicaId,
            Uuid replicaDirectoryId,
            long logEndOffset,
            OptionalLong lastFetchTimestamp,
            OptionalLong lastCaughtUpTimestamp
        ) {
            this.replicaId = replicaId;
            this.replicaDirectoryId = replicaDirectoryId;
            this.logEndOffset = logEndOffset;
            this.lastFetchTimestamp = lastFetchTimestamp;
            this.lastCaughtUpTimestamp = lastCaughtUpTimestamp;
        }

        /**
         * Return the ID for this replica.
         * @return The ID for this replica
         */
        public int replicaId() {
            return replicaId;
        }

        /**
         * Return the directory id of the replica if configured, or Uuid.ZERO_UUID if not.
         */
        public Uuid replicaDirectoryId() {
            return replicaDirectoryId;
        }

        /**
         * Return the logEndOffset known by the leader for this replica.
         * @return The logEndOffset for this replica
         */
        public long logEndOffset() {
            return logEndOffset;
        }

        /**
         * Return the last millisecond timestamp that the leader received a
         * fetch from this replica.
         * @return The value of the lastFetchTime if known, empty otherwise
         */
        public OptionalLong lastFetchTimestamp() {
            return lastFetchTimestamp;
        }

        /**
         * Return the last millisecond timestamp at which this replica was known to be
         * caught up with the leader.
         * @return The value of the lastCaughtUpTime if known, empty otherwise
         */
        public OptionalLong lastCaughtUpTimestamp() {
            return lastCaughtUpTimestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReplicaState that = (ReplicaState) o;
            return replicaId == that.replicaId
                && Objects.equals(replicaDirectoryId, that.replicaDirectoryId)
                && logEndOffset == that.logEndOffset
                && lastFetchTimestamp.equals(that.lastFetchTimestamp)
                && lastCaughtUpTimestamp.equals(that.lastCaughtUpTimestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicaId, replicaDirectoryId, logEndOffset, lastFetchTimestamp, lastCaughtUpTimestamp);
        }

        @Override
        public String toString() {
            return "ReplicaState(" +
                "replicaId=" + replicaId +
                ", replicaDirectoryId=" + replicaDirectoryId +
                ", logEndOffset=" + logEndOffset +
                ", lastFetchTimestamp=" + lastFetchTimestamp +
                ", lastCaughtUpTimestamp=" + lastCaughtUpTimestamp +
                ')';
        }
    }

    public static class Node {
        private final int nodeId;
        private final List<RaftVoterEndpoint> endpoints;

        Node(int nodeId, List<RaftVoterEndpoint> endpoints) {
            this.nodeId = nodeId;
            this.endpoints = endpoints;
        }

        public int nodeId() {
            return nodeId;
        }

        public List<RaftVoterEndpoint> endpoints() {
            return endpoints;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return nodeId == node.nodeId && Objects.equals(endpoints, node.endpoints);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, endpoints);
        }

        @Override
        public String toString() {
            return "Node{" +
                "nodeId=" + nodeId +
                ", endpoints=" + endpoints +
                '}';
        }
    }
}
