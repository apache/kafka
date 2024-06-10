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
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.ReplicaKey;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    static final double CHECK_QUORUM_TIMEOUT_FACTOR = 1.5;

    private final int localId;
    private final int epoch;
    private final long epochStartOffset;
    private final Set<Integer> grantingVoters;

    private Optional<LogOffsetMetadata> highWatermark = Optional.empty();
    private final Map<Integer, ReplicaState> voterStates = new HashMap<>();
    private final Map<Integer, ReplicaState> observerStates = new HashMap<>();
    private final Logger log;
    private final BatchAccumulator<T> accumulator;
    // The set includes all of the followers voters that FETCH or FETCH_SNAPSHOT during the current checkQuorumTimer interval.
    private final Set<Integer> fetchedVoters = new HashSet<>();
    private final Timer checkQuorumTimer;
    private final int checkQuorumTimeoutMs;

    // This is volatile because resignation can be requested from an external thread.
    private volatile boolean resignRequested = false;

    protected LeaderState(
        Time time,
        int localId,
        int epoch,
        long epochStartOffset,
        Set<Integer> voters,
        Set<Integer> grantingVoters,
        BatchAccumulator<T> accumulator,
        int fetchTimeoutMs,
        LogContext logContext
    ) {
        this.localId = localId;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;

        for (int voterId : voters) {
            boolean hasAcknowledgedLeader = voterId == localId;
            this.voterStates.put(voterId, new ReplicaState(voterId, hasAcknowledgedLeader));
        }
        this.grantingVoters = Collections.unmodifiableSet(new HashSet<>(grantingVoters));
        this.log = logContext.logger(LeaderState.class);
        this.accumulator = Objects.requireNonNull(accumulator, "accumulator must be non-null");
        // use the 1.5x of fetch timeout to tolerate some network transition time or other IO time.
        this.checkQuorumTimeoutMs = (int) (fetchTimeoutMs * CHECK_QUORUM_TIMEOUT_FACTOR);
        this.checkQuorumTimer = time.timer(checkQuorumTimeoutMs);
    }

    /**
     * Get the remaining time in milliseconds until the checkQuorumTimer expires.
     * This will happen if we didn't receive a valid fetch/fetchSnapshot request from the majority of the voters within checkQuorumTimeoutMs.
     *
     * @param currentTimeMs the current timestamp in millisecond
     * @return the remainingMs before the checkQuorumTimer expired
     */
    public long timeUntilCheckQuorumExpires(long currentTimeMs) {
        // if there's only 1 voter, it should never get expired.
        if (voterStates.size() == 1) {
            return Long.MAX_VALUE;
        }
        checkQuorumTimer.update(currentTimeMs);
        long remainingMs = checkQuorumTimer.remainingMs();
        if (remainingMs == 0) {
            log.info(
                "Did not receive fetch request from the majority of the voters within {}ms. Current fetched voters are {}.",
                checkQuorumTimeoutMs,
                fetchedVoters);
        }
        return remainingMs;
    }

    /**
     * Reset the checkQuorumTimer if we've received fetch/fetchSnapshot request from the majority of the voter
     *
     * @param id the node id
     * @param currentTimeMs the current timestamp in millisecond
     */
    public void updateCheckQuorumForFollowingVoter(int id, long currentTimeMs) {
        updateFetchedVoters(id);
        // The majority number of the voters excluding the leader. Ex: 3 voters, the value will be 1
        int majority = voterStates.size() / 2;
        if (fetchedVoters.size() >= majority) {
            fetchedVoters.clear();
            checkQuorumTimer.update(currentTimeMs);
            checkQuorumTimer.reset(checkQuorumTimeoutMs);
        }
    }

    private void updateFetchedVoters(int id) {
        if (id == localId) {
            throw new IllegalArgumentException("Received a FETCH/FETCH_SNAPSHOT request from the leader itself.");
        }

        if (isVoter(id)) {
            fetchedVoters.add(id);
        }
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
            .setVersion(ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION)
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

    private boolean maybeUpdateHighWatermark() {
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
                        Optional<LogOffsetMetadata> oldHighWatermark = highWatermark;
                        highWatermark = highWatermarkUpdateOpt;
                        logHighWatermarkUpdate(
                            oldHighWatermark,
                            highWatermarkUpdateMetadata,
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
                    Optional<LogOffsetMetadata> oldHighWatermark = highWatermark;
                    highWatermark = highWatermarkUpdateOpt;
                    logHighWatermarkUpdate(
                        oldHighWatermark,
                        highWatermarkUpdateMetadata,
                        indexOfHw,
                        followersByDescendingFetchOffset
                    );
                    return true;
                }
            }
        }
        return false;
    }

    private void logHighWatermarkUpdate(
        Optional<LogOffsetMetadata> oldHighWatermark,
        LogOffsetMetadata newHighWatermark,
        int indexOfHw,
        List<ReplicaState> followersByDescendingFetchOffset
    ) {
        if (oldHighWatermark.isPresent()) {
            log.debug(
                "High watermark set to {} from {} based on indexOfHw {} and voters {}",
                newHighWatermark,
                oldHighWatermark.get(),
                indexOfHw,
                followersByDescendingFetchOffset
            );
        } else {
            log.info(
                "High watermark set to {} for the first time for epoch {} based on indexOfHw {} and voters {}",
                newHighWatermark,
                epoch,
                indexOfHw,
                followersByDescendingFetchOffset
            );
        }
    }

    /**
     * Update the local replica state.
     *
     * @param endOffsetMetadata updated log end offset of local replica
     * @return true if the high watermark is updated as a result of this call
     */
    public boolean updateLocalState(
        LogOffsetMetadata endOffsetMetadata
    ) {
        ReplicaState state = getOrCreateReplicaState(localId);
        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset.offset > endOffsetMetadata.offset) {
                throw new IllegalStateException("Detected non-monotonic update of local " +
                    "end offset: " + currentEndOffset.offset + " -> " + endOffsetMetadata.offset);
            }
        });
        state.updateLeaderState(endOffsetMetadata);
        return maybeUpdateHighWatermark();
    }

    /**
     * Update the replica state in terms of fetch time and log end offsets.
     *
     * @param replicaId replica id
     * @param currentTimeMs current time in milliseconds
     * @param fetchOffsetMetadata new log offset and metadata
     * @return true if the high watermark is updated as a result of this call
     */
    public boolean updateReplicaState(
        int replicaId,
        long currentTimeMs,
        LogOffsetMetadata fetchOffsetMetadata
    ) {
        // Ignore fetches from negative replica id, as it indicates
        // the fetch is from non-replica. For example, a consumer.
        if (replicaId < 0) {
            return false;
        } else if (replicaId == localId) {
            throw new IllegalStateException("Remote replica ID " + replicaId + " matches the local leader ID");
        }

        ReplicaState state = getOrCreateReplicaState(replicaId);

        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset.offset > fetchOffsetMetadata.offset) {
                log.warn("Detected non-monotonic update of fetch offset from nodeId {}: {} -> {}",
                    state.nodeId, currentEndOffset.offset, fetchOffsetMetadata.offset);
            }
        });

        Optional<LogOffsetMetadata> leaderEndOffsetOpt =
            voterStates.get(localId).endOffset;

        state.updateFollowerState(
            currentTimeMs,
            fetchOffsetMetadata,
            leaderEndOffsetOpt
        );
        updateCheckQuorumForFollowingVoter(replicaId, currentTimeMs);

        return isVoter(state.nodeId) && maybeUpdateHighWatermark();
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

    private ReplicaState getOrCreateReplicaState(int remoteNodeId) {
        ReplicaState state = voterStates.get(remoteNodeId);
        if (state == null) {
            observerStates.putIfAbsent(remoteNodeId, new ReplicaState(remoteNodeId, false));
            return observerStates.get(remoteNodeId);
        }
        return state;
    }

    public DescribeQuorumResponseData.PartitionData describeQuorum(long currentTimeMs) {
        clearInactiveObservers(currentTimeMs);

        return new DescribeQuorumResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code())
            .setLeaderId(localId)
            .setLeaderEpoch(epoch)
            .setHighWatermark(highWatermark.map(offsetMetadata -> offsetMetadata.offset).orElse(-1L))
            .setCurrentVoters(describeReplicaStates(voterStates, currentTimeMs))
            .setObservers(describeReplicaStates(observerStates, currentTimeMs));
    }

    private List<DescribeQuorumResponseData.ReplicaState> describeReplicaStates(
        Map<Integer, ReplicaState> state,
        long currentTimeMs
    ) {
        return state.values().stream()
            .map(replicaState -> describeReplicaState(replicaState, currentTimeMs))
            .collect(Collectors.toList());
    }

    private DescribeQuorumResponseData.ReplicaState describeReplicaState(
        ReplicaState replicaState,
        long currentTimeMs
    ) {
        final long lastCaughtUpTimestamp;
        final long lastFetchTimestamp;
        if (replicaState.nodeId == localId) {
            lastCaughtUpTimestamp = currentTimeMs;
            lastFetchTimestamp = currentTimeMs;
        } else {
            lastCaughtUpTimestamp = replicaState.lastCaughtUpTimestamp;
            lastFetchTimestamp = replicaState.lastFetchTimestamp;
        }
        return new DescribeQuorumResponseData.ReplicaState()
            .setReplicaId(replicaState.nodeId)
            .setLogEndOffset(replicaState.endOffset.map(md -> md.offset).orElse(-1L))
            .setLastCaughtUpTimestamp(lastCaughtUpTimestamp)
            .setLastFetchTimestamp(lastFetchTimestamp);

    }

    private void clearInactiveObservers(final long currentTimeMs) {
        observerStates.entrySet().removeIf(integerReplicaStateEntry ->
            currentTimeMs - integerReplicaStateEntry.getValue().lastFetchTimestamp >= OBSERVER_SESSION_TIMEOUT_MS
        );
    }

    private boolean isVoter(int remoteNodeId) {
        return voterStates.containsKey(remoteNodeId);
    }

    private static class ReplicaState implements Comparable<ReplicaState> {
        final int nodeId;
        Optional<LogOffsetMetadata> endOffset;
        long lastFetchTimestamp;
        long lastFetchLeaderLogEndOffset;
        long lastCaughtUpTimestamp;
        boolean hasAcknowledgedLeader;

        public ReplicaState(int nodeId, boolean hasAcknowledgedLeader) {
            this.nodeId = nodeId;
            this.endOffset = Optional.empty();
            this.lastFetchTimestamp = -1;
            this.lastFetchLeaderLogEndOffset = -1;
            this.lastCaughtUpTimestamp = -1;
            this.hasAcknowledgedLeader = hasAcknowledgedLeader;
        }

        void updateLeaderState(
            LogOffsetMetadata endOffsetMetadata
        ) {
            // For the leader, we only update the end offset. The remaining fields
            // (such as the caught up time) are determined implicitly.
            this.endOffset = Optional.of(endOffsetMetadata);
        }

        void updateFollowerState(
            long currentTimeMs,
            LogOffsetMetadata fetchOffsetMetadata,
            Optional<LogOffsetMetadata> leaderEndOffsetOpt
        ) {
            // Update the `lastCaughtUpTimestamp` before we update the `lastFetchTimestamp`.
            // This allows us to use the previous value for `lastFetchTimestamp` if the
            // follower was able to catch up to `lastFetchLeaderLogEndOffset` on this fetch.
            leaderEndOffsetOpt.ifPresent(leaderEndOffset -> {
                if (fetchOffsetMetadata.offset >= leaderEndOffset.offset) {
                    lastCaughtUpTimestamp = Math.max(lastCaughtUpTimestamp, currentTimeMs);
                } else if (lastFetchLeaderLogEndOffset > 0
                    && fetchOffsetMetadata.offset >= lastFetchLeaderLogEndOffset) {
                    lastCaughtUpTimestamp = Math.max(lastCaughtUpTimestamp, lastFetchTimestamp);
                }
                lastFetchLeaderLogEndOffset = leaderEndOffset.offset;
            });

            lastFetchTimestamp = Math.max(lastFetchTimestamp, currentTimeMs);
            endOffset = Optional.of(fetchOffsetMetadata);
            hasAcknowledgedLeader = true;
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
                "ReplicaState(nodeId=%d, endOffset=%s, lastFetchTimestamp=%s, " +
                        "lastCaughtUpTimestamp=%s, hasAcknowledgedLeader=%s)",
                nodeId,
                endOffset,
                lastFetchTimestamp,
                lastCaughtUpTimestamp,
                hasAcknowledgedLeader
            );
        }
    }

    @Override
    public boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate) {
        log.debug(
            "Rejecting vote request from candidate ({}) since we are already leader in epoch {}",
            candidateKey,
            epoch
        );
        return false;
    }

    @Override
    public String toString() {
        return String.format(
            "Leader(localId=%d, epoch=%d, epochStartOffset=%d, highWatermark=%s, voterStates=%s)",
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
