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

import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.requests.DescribeQuorumResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class LeaderState implements EpochState {
    private final int localId;
    private final int epoch;
    private final long epochStartOffset;
    private OptionalLong highWatermark = OptionalLong.empty();
    private final Map<Integer, VoterState> voterReplicaStates = new HashMap<>();
    private final Map<Integer, NodeState> observerReplicaStates = new HashMap<>();

    protected LeaderState(int localId, int epoch, long epochStartOffset, Set<Integer> voters) {
        this.localId = localId;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;

        for (int voterId : voters) {
            boolean hasEndorsedLeader = voterId == localId;
            this.voterReplicaStates.put(voterId, new VoterState(voterId, hasEndorsedLeader));
        }
    }

    @Override
    public OptionalLong highWatermark() {
        return highWatermark;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(epoch, localId, voterReplicaStates.keySet());
    }

    @Override
    public int epoch() {
        return epoch;
    }

    public Set<Integer> followers() {
        return voterReplicaStates.keySet().stream().filter(id -> id != localId).collect(Collectors.toSet());
    }

    public int localId() {
        return localId;
    }

    public Set<Integer> nonEndorsingFollowers() {
        Set<Integer> nonEndorsing = new HashSet<>();
        for (VoterState state : voterReplicaStates.values()) {
            if (!state.hasEndorsedLeader)
                nonEndorsing.add(state.nodeId);
        }
        return nonEndorsing;
    }

    private boolean updateHighWatermark() {
        // Find the largest offset which is replicated to a majority of replicas (the leader counts)
        List<NodeState> followersByDescendingFetchOffset = followersByDescendingFetchOffset();

        int indexOfHw = voterReplicaStates.size() / 2;
        OptionalLong highWatermarkUpdateOpt = followersByDescendingFetchOffset.get(indexOfHw).endOffset;

        if (highWatermarkUpdateOpt.isPresent()) {
            // When a leader is first elected, it cannot know the high watermark of the previous
            // leader. In order to avoid exposing a non-monotonically increasing value, we have
            // to wait for followers to catch up to the start of the leader's epoch.
            long highWatermarkUpdate = highWatermarkUpdateOpt.getAsLong();
            if (highWatermarkUpdate >= epochStartOffset) {
                highWatermark = OptionalLong.of(highWatermarkUpdate);
                return true;
            }
        }
        return false;
    }

    private OptionalLong quorumMajorityFetchTimestamp() {
        // Find the latest timestamp which is fetched by a majority of replicas (the leader counts)
        ArrayList<NodeState> followersByDescendingFetchTimestamp = new ArrayList<>(this.voterReplicaStates.values());
        followersByDescendingFetchTimestamp.sort(FETCH_TIMESTAMP_COMPARATOR);
        int indexOfTimestamp = voterReplicaStates.size() / 2;
        return followersByDescendingFetchTimestamp.get(indexOfTimestamp).lastFetchTimestamp;
    }

    /**
     * @return The updated lower bound of fetch timestamps for a majority of quorum; -1 indicating that we have
     *         not received fetch from the majority yet
     */
    public OptionalLong updateFetchTimestamp(int nodeId, long timestamp) {
        NodeState state = ensureValidVoter(nodeId);
        // To be resilient to system time shifts we do not strictly require the timestamp be monotonically increasing
        state.lastFetchTimestamp = OptionalLong.of(Math.max(state.lastFetchTimestamp.orElse(-1L), timestamp));
        return quorumMajorityFetchTimestamp();
    }

    public List<Integer> nonLeaderVotersByDescendingFetchOffset() {
        return followersByDescendingFetchOffset().stream()
                   .filter(state -> state.nodeId != localId)
                   .map(state -> state.nodeId).collect(Collectors.toList());
    }

    private List<ReplicaState> followersByDescendingFetchOffset() {
        List<ReplicaState> followersByDescendingFetchOffset = new ArrayList<>(this.voterReplicaStates.values());
        Collections.sort(followersByDescendingFetchOffset);
        return followersByDescendingFetchOffset;
    }

    /**
     * @return true if the high watermark is updated too
     */
    public boolean updateEndOffset(int remoteNodeId, long endOffset) {
        NodeState state = getReplicaState(remoteNodeId);

        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset > endOffset)
                throw new IllegalArgumentException("Non-monotonic update to end offset for nodeId " + remoteNodeId);
        });
        state.endOffset = OptionalLong.of(endOffset);
        if (isVoter(remoteNodeId)) {
            addEndorsementFrom(remoteNodeId);
            return updateHighWatermark();
        }
        return false;
    }

    public void addEndorsementFrom(int remoteNodeId) {
        VoterState voterState = ensureValidVoter(remoteNodeId);
        voterState.hasEndorsedLeader = true;
    }

    private VoterState ensureValidVoter(int remoteNodeId) {
        VoterState state = voterReplicaStates.get(remoteNodeId);
        if (state == null)
            throw new IllegalArgumentException("Unexpected endorsement from non-voter " + remoteNodeId);
        return state;
    }

    NodeState getReplicaState(int remoteNodeId) {
        NodeState state = voterReplicaStates.get(remoteNodeId);
        if (state == null) {
            observerReplicaStates.putIfAbsent(remoteNodeId, new NodeState(remoteNodeId));
            return observerReplicaStates.get(remoteNodeId);
        }
        return state;
    }

    List<ReplicaState> getVoterStates() {
        return voterReplicaStates.entrySet().stream().filter(entry -> entry.getKey() != localId)
            .map(entry -> new ReplicaState()
                              .setReplicaId(entry.getKey())
                              .setLogEndOffset(entry.getValue().endOffset.orElse(-1)))
            .collect(Collectors.toList());
    }

    List<ReplicaState> getObserverStates() {
        return observerReplicaStates.entrySet().stream()
                   .map(entry -> new ReplicaState()
                                     .setReplicaId(entry.getKey())
                                     .setLogEndOffset(entry.getValue().endOffset.orElse(-1)))
                   .collect(Collectors.toList());
    }

    private boolean isVoter(int remoteNodeId) {
        return voterReplicaStates.containsKey(remoteNodeId);
    }

    /**
     * Update the local end offset after a log append to the leader. Return true if this
     * update results in a bump to the high watermark.
     *
     * @param endOffset The new log end offset
     * @return true if the high watermark increased, false otherwise
     */
    public boolean updateLocalEndOffset(long endOffset) {
        return updateEndOffset(localId, endOffset);
    }

    private static class NodeState implements Comparable<NodeState> {
        final int nodeId;
        OptionalLong endOffset;
        OptionalLong lastFetchTimestamp;

        public NodeState(int nodeId) {
            this.nodeId = nodeId;
            this.endOffset = OptionalLong.empty();
            this.lastFetchTimestamp = OptionalLong.empty();
        }

        @Override
        public int compareTo(NodeState that) {
            if (this.endOffset.equals(that.endOffset))
                return Integer.compare(this.nodeId, that.nodeId);
            else if (!this.endOffset.isPresent())
                return 1;
            else if (!that.endOffset.isPresent())
                return -1;
            else
                return Long.compare(that.endOffset.getAsLong(), this.endOffset.getAsLong());
        }
    }

    private static class VoterState extends NodeState {
        boolean hasEndorsedLeader;

        public VoterState(int nodeId, boolean hasEndorsedLeader) {
            super(nodeId);
            this.hasEndorsedLeader = hasEndorsedLeader;
        }
    }

    private static final Comparator<NodeState> FETCH_TIMESTAMP_COMPARATOR = (state, that) -> {
        if (state.lastFetchTimestamp.equals(that.lastFetchTimestamp))
            return Integer.compare(state.nodeId, that.nodeId);
        else if (!state.lastFetchTimestamp.isPresent())
            return 1;
        else if (!that.lastFetchTimestamp.isPresent())
            return -1;
        else
            return Long.compare(that.lastFetchTimestamp.getAsLong(), state.lastFetchTimestamp.getAsLong());
    };
}
