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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.slf4j.Logger;

import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.record.ControlRecordUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * In the context of LeaderState, an acknowledged voter means one who has acknowledged the current leader by either
 * responding to a `BeginQuorumEpoch` request from the leader or by beginning to send `Fetch` requests.
 * More specifically, the set of unacknowledged voters are targets for BeginQuorumEpoch requests from the leader until
 * they acknowledge the leader.
 */
public class LeaderState<T> implements EpochState {
    static final long OBSERVER_SESSION_TIMEOUT_MS = 300_000L;

    private final int localId;
    private final int epoch;
    private final long epochStartOffset;

    private Optional<LogOffsetMetadata> highWatermark;
    private final Map<Integer, ReplicaState> voterStates = new HashMap<>();
    private final Map<Integer, ReplicaState> observerStates = new HashMap<>();
    private final Set<Integer> grantingVoters = new HashSet<>();
    private final Logger log;
    private final BatchAccumulator<T> accumulator;

    // This is volatile because resignation can be requested from an external thread.
    private volatile boolean resignRequested = false;

    protected LeaderState(
        int localId,
        int epoch,
        long epochStartOffset,
        Set<Integer> voters,
        Set<Integer> grantingVoters,
        BatchAccumulator<T> accumulator,
        LogContext logContext
    ) {
        this.localId = localId;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;
        this.highWatermark = Optional.empty();

        for (int voterId : voters) {
            boolean hasAcknowledgedLeader = voterId == localId;
            this.voterStates.put(voterId, new ReplicaState(voterId, hasAcknowledgedLeader));
        }
        this.grantingVoters.addAll(grantingVoters);
        this.log = logContext.logger(LeaderState.class);
        this.accumulator = Objects.requireNonNull(accumulator, "accumulator must be non-null");
    }

    public BatchAccumulator<T> accumulator() {
        return this.accumulator;
    }

    private static List<Voter> convertToVoters(Set<Integer> voterIds) {
        return voterIds.stream()
            .map(follower -> new Voter().setVoterId(follower))
            .collect(Collectors.toList());
    }

    public void appendLeaderChangeMessage(long currentTimeMs) {
        List<Voter> voters = convertToVoters(voterStates.keySet());
        List<Voter> grantingVoters = convertToVoters(this.grantingVoters());

        LeaderChangeMessage leaderChangeMessage = new LeaderChangeMessage()
            .setVersion(ControlRecordUtils.LEADER_CHANGE_SCHEMA_HIGHEST_VERSION)
            .setLeaderId(this.election().leaderId())
            .setVoters(voters)
            .setGrantingVoters(grantingVoters);
        
        accumulator.appendLeaderChangeMessage(leaderChangeMessage, currentTimeMs);
        accumulator.forceDrain();
    }

    public boolean isResignRequested() {
        return resignRequested;
    }

    public void requestResign() {
        this.resignRequested = true;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(epoch, localId, voterStates.keySet());
    }

    @Override
    public int epoch() {
        return epoch;
    }

    public Set<Integer> grantingVoters() {
        return this.grantingVoters;
    }

    public int localId() {
        return localId;
    }

    public Set<Integer> nonAcknowledgingVoters() {
        Set<Integer> nonAcknowledging = new HashSet<>();
        for (ReplicaState state : voterStates.values()) {
            if (!state.hasAcknowledgedLeader)
                nonAcknowledging.add(state.nodeId);
        }
        return nonAcknowledging;
    }

    private boolean updateHighWatermark() {
        // Find the largest offset which is replicated to a majority of replicas (the leader counts)
        List<ReplicaState> followersByDescendingFetchOffset = followersByDescendingFetchOffset();

        int indexOfHw = voterStates.size() / 2;
        Optional<LogOffsetMetadata> highWatermarkUpdateOpt = followersByDescendingFetchOffset.get(indexOfHw).endOffset;

        if (highWatermarkUpdateOpt.isPresent()) {

            // The KRaft protocol requires an extra condition on commitment after a leader
            // election. The leader must commit one record from its own epoch before it is
            // allowed to expose records from any previous epoch. This guarantees that its
            // log will contain the largest record (in terms of epoch/offset) in any log
            // which ensures that any future leader will have replicated this record as well
            // as all records from previous epochs that the current leader has committed.

            LogOffsetMetadata highWatermarkUpdateMetadata = highWatermarkUpdateOpt.get();
            long highWatermarkUpdateOffset = highWatermarkUpdateMetadata.offset;

            if (highWatermarkUpdateOffset > epochStartOffset) {
                if (highWatermark.isPresent()) {
                    LogOffsetMetadata currentHighWatermarkMetadata = highWatermark.get();
                    if (highWatermarkUpdateOffset > currentHighWatermarkMetadata.offset
                        || (highWatermarkUpdateOffset == currentHighWatermarkMetadata.offset &&
                            !highWatermarkUpdateMetadata.metadata.equals(currentHighWatermarkMetadata.metadata))) {
                        highWatermark = highWatermarkUpdateOpt;
                        log.trace(
                            "High watermark updated to {} based on indexOfHw {} and voters {}",
                            highWatermark,
                            indexOfHw,
                            followersByDescendingFetchOffset
                        );
                        return true;
                    } else if (highWatermarkUpdateOffset < currentHighWatermarkMetadata.offset) {
                        log.error("The latest computed high watermark {} is smaller than the current " +
                                "value {}, which suggests that one of the voters has lost committed data. " +
                                "Full voter replication state: {}", highWatermarkUpdateOffset,
                            currentHighWatermarkMetadata.offset, voterStates.values());
                        return false;
                    } else {
                        return false;
                    }
                } else {
                    highWatermark = highWatermarkUpdateOpt;
                    log.trace(
                        "High watermark set to {} based on indexOfHw {} and voters {}",
                        highWatermark,
                        indexOfHw,
                        followersByDescendingFetchOffset
                    );
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

    private List<ReplicaState> followersByDescendingFetchOffset() {
        return new ArrayList<>(this.voterStates.values()).stream()
            .sorted()
            .collect(Collectors.toList());
    }

    private boolean updateEndOffset(ReplicaState state,
                                    LogOffsetMetadata endOffsetMetadata) {
        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset.offset > endOffsetMetadata.offset) {
                if (state.nodeId == localId) {
                    throw new IllegalStateException("Detected non-monotonic update of local " +
                        "end offset: " + currentEndOffset.offset + " -> " + endOffsetMetadata.offset);
                } else {
                    log.warn("Detected non-monotonic update of fetch offset from nodeId {}: {} -> {}",
                        state.nodeId, currentEndOffset.offset, endOffsetMetadata.offset);
                }
            }
        });

        state.endOffset = Optional.of(endOffsetMetadata);
        state.hasAcknowledgedLeader = true;
        return isVoter(state.nodeId) && updateHighWatermark();
    }

    public void addAcknowledgementFrom(int remoteNodeId) {
        ReplicaState voterState = ensureValidVoter(remoteNodeId);
        voterState.hasAcknowledgedLeader = true;
    }

    private ReplicaState ensureValidVoter(int remoteNodeId) {
        ReplicaState state = voterStates.get(remoteNodeId);
        if (state == null)
            throw new IllegalArgumentException("Unexpected acknowledgement from non-voter " + remoteNodeId);
        return state;
    }

    public long epochStartOffset() {
        return epochStartOffset;
    }

    private ReplicaState getReplicaState(int remoteNodeId) {
        ReplicaState state = voterStates.get(remoteNodeId);
        if (state == null) {
            observerStates.putIfAbsent(remoteNodeId, new ReplicaState(remoteNodeId, false));
            return observerStates.get(remoteNodeId);
        }
        return state;
    }

    Map<Integer, Long> getVoterEndOffsets() {
        return getReplicaEndOffsets(voterStates);
    }

    Map<Integer, Long> getObserverStates(final long currentTimeMs) {
        clearInactiveObservers(currentTimeMs);
        return getReplicaEndOffsets(observerStates);
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
        observerStates.entrySet().removeIf(
            integerReplicaStateEntry ->
                currentTimeMs - integerReplicaStateEntry.getValue().lastFetchTimestamp.orElse(-1)
                    >= OBSERVER_SESSION_TIMEOUT_MS);
    }

    private boolean isVoter(int remoteNodeId) {
        return voterStates.containsKey(remoteNodeId);
    }

    private static class ReplicaState implements Comparable<ReplicaState> {
        final int nodeId;
        Optional<LogOffsetMetadata> endOffset;
        OptionalLong lastFetchTimestamp;
        boolean hasAcknowledgedLeader;

        public ReplicaState(int nodeId, boolean hasAcknowledgedLeader) {
            this.nodeId = nodeId;
            this.endOffset = Optional.empty();
            this.lastFetchTimestamp = OptionalLong.empty();
            this.hasAcknowledgedLeader = hasAcknowledgedLeader;
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

        @Override
        public String toString() {
            return String.format(
                "ReplicaState(nodeId=%s, endOffset=%s, lastFetchTimestamp=%s, hasAcknowledgedLeader=%s)",
                nodeId,
                endOffset,
                lastFetchTimestamp,
                hasAcknowledgedLeader 
            );
        }
    }

    @Override
    public boolean canGrantVote(int candidateId, boolean isLogUpToDate) {
        log.debug("Rejecting vote request from candidate {} since we are already leader in epoch {}",
            candidateId, epoch);
        return false;
    }

    @Override
    public String toString() {
        return String.format(
            "Leader(localId=%s, epoch=%s, epochStartOffset=%s, highWatermark=%s, voterStates=%s)",
            localId,
            epoch,
            epochStartOffset,
            highWatermark,
            voterStates
        );
    }

    @Override
    public String name() {
        return "Leader";
    }

    @Override
    public void close() {
        accumulator.close();
    }

}
