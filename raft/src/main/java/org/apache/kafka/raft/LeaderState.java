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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class LeaderState implements EpochState {
    private final int localId;
    private final int epoch;
    private final long epochStartOffset;

    private Optional<LogOffsetMetadata> highWatermark;
    private final Map<Integer, VoterState> voterReplicaStates = new HashMap<>();
    private final Map<Integer, ReplicaState> observerReplicaStates = new HashMap<>();
    private final Set<Integer> grantingVoters = new HashSet<>();
    private static final long OBSERVER_SESSION_TIMEOUT_MS = 300_000L;

    protected LeaderState(
        int localId,
        int epoch,
        long epochStartOffset,
        Set<Integer> voters,
        Set<Integer> grantingVoters
    ) {
        this.localId = localId;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;
        this.highWatermark = Optional.empty();

        for (int voterId : voters) {
            boolean hasEndorsedLeader = voterId == localId;
            this.voterReplicaStates.put(voterId, new VoterState(voterId, hasEndorsedLeader));
        }
        this.grantingVoters.addAll(grantingVoters);
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
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

    public Set<Integer> grantingVoters() {
        return this.grantingVoters;
    }

    public int localId() {
        return localId;
    }

    public Set<Integer> nonEndorsingVoters() {
        Set<Integer> nonEndorsing = new HashSet<>();
        for (VoterState state : voterReplicaStates.values()) {
            if (!state.hasEndorsedLeader)
                nonEndorsing.add(state.nodeId);
        }
        return nonEndorsing;
    }

    private boolean updateHighWatermark() {
        // Find the largest offset which is replicated to a majority of replicas (the leader counts)
        List<VoterState> followersByDescendingFetchOffset = followersByDescendingFetchOffset();

        int indexOfHw = voterReplicaStates.size() / 2;
        Optional<LogOffsetMetadata> highWatermarkUpdateOpt = followersByDescendingFetchOffset.get(indexOfHw).endOffset;

        if (highWatermarkUpdateOpt.isPresent()) {
            // When a leader is first elected, it cannot know the high watermark of the previous
            // leader. In order to avoid exposing a non-monotonically increasing value, we have
            // to wait for followers to catch up to the start of the leader's epoch.
            LogOffsetMetadata highWatermarkUpdateMetadata = highWatermarkUpdateOpt.get();
            long highWatermarkUpdate = highWatermarkUpdateMetadata.offset;

            if (highWatermarkUpdate >= epochStartOffset) {
                if (highWatermark.isPresent()) {
                    LogOffsetMetadata currentHighWatermarkMetadata = highWatermark.get();
                    if (highWatermarkUpdate > currentHighWatermarkMetadata.offset
                        || (highWatermarkUpdate == currentHighWatermarkMetadata.offset &&
                            !highWatermarkUpdateMetadata.metadata.equals(currentHighWatermarkMetadata.metadata))) {
                        highWatermark = highWatermarkUpdateOpt;
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    highWatermark = highWatermarkUpdateOpt;
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Update the local replica state.
     *
     * See {@link #updateReplicaState(int, long, LogOffsetMetadata)}
     */
    public boolean updateLocalState(long fetchTimestamp, LogOffsetMetadata logOffsetMetadata) {
        return updateReplicaState(localId, fetchTimestamp, logOffsetMetadata);
    }

    /**
     * Update the replica state in terms of fetch time and log end offsets.
     *
     * @param replicaId replica id
     * @param fetchTimestamp fetch timestamp
     * @param logOffsetMetadata new log offset and metadata
     * @return true if the high watermark is updated too
     */
    public boolean updateReplicaState(int replicaId,
                                      long fetchTimestamp,
                                      LogOffsetMetadata logOffsetMetadata) {
        // Ignore fetches from negative replica id, as it indicates
        // the fetch is from non-replica. For example, a consumer.
        if (replicaId < 0) {
            return false;
        }

        ReplicaState state = getReplicaState(replicaId);
        state.updateFetchTimestamp(fetchTimestamp);
        return updateEndOffset(state, logOffsetMetadata);
    }

    public List<Integer> nonLeaderVotersByDescendingFetchOffset() {
        return followersByDescendingFetchOffset().stream()
                   .filter(state -> state.nodeId != localId)
                   .map(state -> state.nodeId)
                   .collect(Collectors.toList());
    }

    private List<VoterState> followersByDescendingFetchOffset() {
        return new ArrayList<>(this.voterReplicaStates.values())
            .stream().sorted().collect(Collectors.toList());
    }

    private boolean updateEndOffset(ReplicaState state,
                                    LogOffsetMetadata endOffsetMetadata) {
        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset.offset > endOffsetMetadata.offset)
                throw new IllegalArgumentException("Non-monotonic update to end offset for nodeId " + state.nodeId);
        });

        state.endOffset = Optional.of(endOffsetMetadata);

        if (isVoter(state.nodeId)) {
            ((VoterState) state).hasEndorsedLeader = true;
            addEndorsementFrom(state.nodeId);
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

    public long epochStartOffset() {
        return epochStartOffset;
    }

    ReplicaState getReplicaState(int remoteNodeId) {
        ReplicaState state = voterReplicaStates.get(remoteNodeId);
        if (state == null) {
            observerReplicaStates.putIfAbsent(remoteNodeId, new ReplicaState(remoteNodeId));
            return observerReplicaStates.get(remoteNodeId);
        }
        return state;
    }

    Map<Integer, Long> getVoterEndOffsets() {
        return getReplicaEndOffsets(voterReplicaStates);
    }

    Map<Integer, Long> getObserverStates(final long currentTimeMs) {
        clearInactiveObservers(currentTimeMs);
        return getReplicaEndOffsets(observerReplicaStates);
    }

    private static <R extends ReplicaState> Map<Integer, Long> getReplicaEndOffsets(
        Map<Integer, R> replicaStates) {
        return replicaStates.entrySet().stream()
                   .collect(Collectors.toMap(Map.Entry::getKey,
                       e -> e.getValue().endOffset.map(
                           logOffsetMetadata -> logOffsetMetadata.offset).orElse(-1L))
                   );
    }

    private void clearInactiveObservers(final long currentTimeMs) {
        observerReplicaStates.entrySet().removeIf(
            integerReplicaStateEntry ->
                currentTimeMs - integerReplicaStateEntry.getValue().lastFetchTimestamp.orElse(-1)
                    >= OBSERVER_SESSION_TIMEOUT_MS);
    }

    private boolean isVoter(int remoteNodeId) {
        return voterReplicaStates.containsKey(remoteNodeId);
    }

    private static class ReplicaState implements Comparable<ReplicaState> {
        final int nodeId;
        Optional<LogOffsetMetadata> endOffset;
        OptionalLong lastFetchTimestamp;

        public ReplicaState(int nodeId) {
            this.nodeId = nodeId;
            this.endOffset = Optional.empty();
            this.lastFetchTimestamp = OptionalLong.empty();
        }

        void updateFetchTimestamp(long currentFetchTimeMs) {
            // To be resilient to system time shifts we do not strictly
            // require the timestamp be monotonically increasing.
            lastFetchTimestamp = OptionalLong.of(Math.max(lastFetchTimestamp.orElse(-1L), currentFetchTimeMs));
        }

        @Override
        public int compareTo(ReplicaState that) {
            if (this.endOffset.equals(that.endOffset))
                return Integer.compare(this.nodeId, that.nodeId);
            else if (!this.endOffset.isPresent())
                return 1;
            else if (!that.endOffset.isPresent())
                return -1;
            else
                return Long.compare(that.endOffset.get().offset, this.endOffset.get().offset);
        }
    }

    private static class VoterState extends ReplicaState {
        boolean hasEndorsedLeader;

        public VoterState(int nodeId,
                          boolean hasEndorsedLeader) {
            super(nodeId);
            this.hasEndorsedLeader = hasEndorsedLeader;
        }
    }

    @Override
    public String toString() {
        return "Leader(" +
            "localId=" + localId +
            ", epoch=" + epoch +
            ", epochStartOffset=" + epochStartOffset +
            ')';
    }

    @Override
    public String name() {
        return "Leader";
    }

}
