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

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.internals.AddVoterHandlerState;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.RemoveVoterHandlerState;
import org.apache.kafka.server.common.KRaftVersion;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * In the context of LeaderState, an acknowledged voter means one who has acknowledged the current leader by either
 * responding to a `BeginQuorumEpoch` request from the leader or by beginning to send `Fetch` requests.
 * More specifically, the set of unacknowledged voters are targets for BeginQuorumEpoch requests from the leader until
 * they acknowledge the leader.
 */
public class LeaderState<T> implements EpochState {
    static final long OBSERVER_SESSION_TIMEOUT_MS = 300_000L;
    static final double CHECK_QUORUM_TIMEOUT_FACTOR = 1.5;

    private final ReplicaKey localReplicaKey;
    private final int epoch;
    private final long epochStartOffset;
    private final Set<Integer> grantingVoters;
    private final Endpoints localListeners;
    private final VoterSet voterSetAtEpochStart;
    // This field is non-empty if the voter set at epoch start came from a snapshot or log segment
    private final OptionalLong offsetOfVotersAtEpochStart;
    private final KRaftVersion kraftVersionAtEpochStart;

    private Optional<LogOffsetMetadata> highWatermark = Optional.empty();
    private Map<Integer, ReplicaState> voterStates = new HashMap<>();
    private Optional<AddVoterHandlerState> addVoterHandlerState = Optional.empty();
    private Optional<RemoveVoterHandlerState> removeVoterHandlerState = Optional.empty();

    private final Map<ReplicaKey, ReplicaState> observerStates = new HashMap<>();
    private final Logger log;
    private final BatchAccumulator<T> accumulator;
    // The set includes all of the followers voters that FETCH or FETCH_SNAPSHOT during the current checkQuorumTimer interval.
    private final Set<Integer> fetchedVoters = new HashSet<>();
    private final Timer checkQuorumTimer;
    private final int checkQuorumTimeoutMs;
    private final Timer beginQuorumEpochTimer;
    private final int beginQuorumEpochTimeoutMs;

    // This is volatile because resignation can be requested from an external thread.
    private volatile boolean resignRequested = false;

    protected LeaderState(
        Time time,
        ReplicaKey localReplicaKey,
        int epoch,
        long epochStartOffset,
        VoterSet voterSetAtEpochStart,
        OptionalLong offsetOfVotersAtEpochStart,
        KRaftVersion kraftVersionAtEpochStart,
        Set<Integer> grantingVoters,
        BatchAccumulator<T> accumulator,
        Endpoints localListeners,
        int fetchTimeoutMs,
        LogContext logContext
    ) {
        this.localReplicaKey = localReplicaKey;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;
        this.localListeners = localListeners;

        for (VoterSet.VoterNode voterNode: voterSetAtEpochStart.voterNodes()) {
            boolean hasAcknowledgedLeader = voterNode.isVoter(localReplicaKey);
            this.voterStates.put(
                voterNode.voterKey().id(),
                new ReplicaState(voterNode.voterKey(), hasAcknowledgedLeader, voterNode.listeners())
            );
        }
        this.grantingVoters = Set.copyOf(grantingVoters);
        this.log = logContext.logger(LeaderState.class);
        this.accumulator = Objects.requireNonNull(accumulator, "accumulator must be non-null");
        // use the 1.5x of fetch timeout to tolerate some network transition time or other IO time.
        this.checkQuorumTimeoutMs = (int) (fetchTimeoutMs * CHECK_QUORUM_TIMEOUT_FACTOR);
        this.checkQuorumTimer = time.timer(checkQuorumTimeoutMs);
        this.beginQuorumEpochTimeoutMs = fetchTimeoutMs / 2;
        this.beginQuorumEpochTimer = time.timer(0);
        this.voterSetAtEpochStart =  voterSetAtEpochStart;
        this.offsetOfVotersAtEpochStart = offsetOfVotersAtEpochStart;
        this.kraftVersionAtEpochStart = kraftVersionAtEpochStart;
    }

    public long timeUntilBeginQuorumEpochTimerExpires(long currentTimeMs) {
        beginQuorumEpochTimer.update(currentTimeMs);
        return beginQuorumEpochTimer.remainingMs();
    }

    public void resetBeginQuorumEpochTimer(long currentTimeMs) {
        beginQuorumEpochTimer.update(currentTimeMs);
        beginQuorumEpochTimer.reset(beginQuorumEpochTimeoutMs);
    }

    /**
     * Get the remaining time in milliseconds until the checkQuorumTimer expires.
     *
     * This will happen if we didn't receive a valid fetch/fetchSnapshot request from the majority
     * of the voters within checkQuorumTimeoutMs.
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
                "Did not receive fetch request from the majority of the voters within {}ms. " +
                "Current fetched voters are {}, and voters are {}",
                checkQuorumTimeoutMs,
                fetchedVoters,
                voterStates.values().stream().map(voter -> voter.replicaKey)
            );
        }
        return remainingMs;
    }

    /**
     * Reset the checkQuorumTimer if we've received fetch/fetchSnapshot request from the majority of the voter
     *
     * @param replicaKey the replica key of the voter
     * @param currentTimeMs the current timestamp in millisecond
     */
    public void updateCheckQuorumForFollowingVoter(ReplicaKey replicaKey, long currentTimeMs) {
        updateFetchedVoters(replicaKey);
        // The majority number of the voters. Ex: 2 for 3 voters, 3 for 4 voters... etc.
        int majority = (voterStates.size() / 2) + 1;
        // If the leader is in the voter set, it should be implicitly counted as part of the
        // majority, but the leader will never be a member of the fetchedVoters.
        // If the leader is not in the voter set, it is not in the majority. Then, the
        // majority can only be composed of fetched voters.
        if (voterStates.containsKey(localReplicaKey.id())) {
            majority = majority - 1;
        }

        if (fetchedVoters.size() >= majority) {
            fetchedVoters.clear();
            checkQuorumTimer.update(currentTimeMs);
            checkQuorumTimer.reset(checkQuorumTimeoutMs);
        }
    }

    private void updateFetchedVoters(ReplicaKey replicaKey) {
        if (replicaKey.id() == localReplicaKey.id()) {
            throw new IllegalArgumentException("Received a FETCH/FETCH_SNAPSHOT request from the leader itself.");
        }

        ReplicaState state = voterStates.get(replicaKey.id());
        if (state != null && state.matchesKey(replicaKey)) {
            fetchedVoters.add(replicaKey.id());
        }
    }

    public BatchAccumulator<T> accumulator() {
        return accumulator;
    }

    public Optional<AddVoterHandlerState> addVoterHandlerState() {
        return addVoterHandlerState;
    }

    public void resetAddVoterHandlerState(
        Errors error,
        String message,
        Optional<AddVoterHandlerState> state
    ) {
        addVoterHandlerState.ifPresent(
            handlerState -> handlerState
                .future()
                .complete(RaftUtil.addVoterResponse(error, message))
        );
        addVoterHandlerState = state;
    }

    public Optional<RemoveVoterHandlerState> removeVoterHandlerState() {
        return removeVoterHandlerState;
    }

    public void resetRemoveVoterHandlerState(
        Errors error,
        String message,
        Optional<RemoveVoterHandlerState> state
    ) {
        removeVoterHandlerState.ifPresent(
            handlerState -> handlerState
                .future()
                .complete(RaftUtil.removeVoterResponse(error, message))
        );
        removeVoterHandlerState = state;
    }

    public long maybeExpirePendingOperation(long currentTimeMs) {
        // First abort any expired operations
        long timeUntilAddVoterExpiration = addVoterHandlerState()
            .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
            .orElse(Long.MAX_VALUE);

        if (timeUntilAddVoterExpiration == 0) {
            resetAddVoterHandlerState(Errors.REQUEST_TIMED_OUT, null, Optional.empty());
        }

        long timeUntilRemoveVoterExpiration = removeVoterHandlerState()
            .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
            .orElse(Long.MAX_VALUE);

        if (timeUntilRemoveVoterExpiration == 0) {
            resetRemoveVoterHandlerState(Errors.REQUEST_TIMED_OUT, null, Optional.empty());
        }

        // Reread the timeouts and return the smaller of them
        return Math.min(
            addVoterHandlerState()
                .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                .orElse(Long.MAX_VALUE),
            removeVoterHandlerState()
                .map(state -> state.timeUntilOperationExpiration(currentTimeMs))
                .orElse(Long.MAX_VALUE)
        );
    }

    public boolean isOperationPending(long currentTimeMs) {
        maybeExpirePendingOperation(currentTimeMs);
        return addVoterHandlerState.isPresent() || removeVoterHandlerState.isPresent();
    }

    private static List<Voter> convertToVoters(Set<Integer> voterIds) {
        return voterIds.stream()
            .map(follower -> new Voter().setVoterId(follower))
            .collect(Collectors.toList());
    }

    public void appendStartOfEpochControlRecords(VoterSet.VoterNode localVoterNode, long currentTimeMs) {
        if (!localReplicaKey.equals(localVoterNode.voterKey())) {
            throw new IllegalArgumentException(
                String.format(
                    "Replica key %s didn't match the local key %s",
                    localVoterNode.voterKey(),
                    localReplicaKey
                )
            );
        } else if (!localListeners.equals(localVoterNode.listeners())) {
            throw new IllegalArgumentException(
                String.format(
                    "Listeners %s didn't match the local listeners %s",
                    localVoterNode.listeners(),
                    localListeners
                )
            );
        }

        List<Voter> voters = convertToVoters(voterStates.keySet());
        List<Voter> grantingVoters = convertToVoters(this.grantingVoters());

        LeaderChangeMessage leaderChangeMessage = new LeaderChangeMessage()
            .setVersion(ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION)
            .setLeaderId(this.election().leaderId())
            .setVoters(voters)
            .setGrantingVoters(grantingVoters);

        accumulator.appendControlMessages((baseOffset, epoch, compression, buffer) -> {
            try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                    buffer,
                    RecordBatch.CURRENT_MAGIC_VALUE,
                    compression,
                    TimestampType.CREATE_TIME,
                    baseOffset,
                    currentTimeMs,
                    RecordBatch.NO_PRODUCER_ID,
                    RecordBatch.NO_PRODUCER_EPOCH,
                    RecordBatch.NO_SEQUENCE,
                    false, // isTransactional
                    true,  // isControlBatch
                    epoch,
                    buffer.capacity()
                )
            ) {
                builder.appendLeaderChangeMessage(currentTimeMs, leaderChangeMessage);

                if (kraftVersionAtEpochStart.isReconfigSupported()) {
                    long offset = offsetOfVotersAtEpochStart.orElseThrow(
                        () -> new IllegalStateException(
                            String.format(
                                "The %s is %s but there is no voter set in the log or " +
                                "checkpoint %s",
                                KRaftVersion.FEATURE_NAME,
                                kraftVersionAtEpochStart,
                                voterSetAtEpochStart
                            )
                        )
                    );

                    // The leader should write the latest voters record if its local listeners are different
                    // or it has never written a voters record to the log before.
                    if (offset == -1 || voterSetAtEpochStart.voterNodeNeedsUpdate(localVoterNode)) {
                        VoterSet updatedVoterSet = voterSetAtEpochStart
                            .updateVoter(localVoterNode)
                            .orElseThrow(
                                () -> new IllegalStateException(
                                    String.format(
                                        "Update expected for leader node %s and voter set %s",
                                        localVoterNode,
                                        voterSetAtEpochStart
                                    )
                                )
                            );

                        builder.appendKRaftVersionMessage(
                            currentTimeMs,
                            new KRaftVersionRecord()
                                .setVersion(kraftVersionAtEpochStart.kraftVersionRecordVersion())
                                .setKRaftVersion(kraftVersionAtEpochStart.featureLevel())
                        );
                        builder.appendVotersMessage(
                            currentTimeMs,
                            updatedVoterSet.toVotersRecord(
                                kraftVersionAtEpochStart.votersRecordVersion()
                            )
                        );
                    }
                }

                return builder.build();
            }
        });
    }

    public long appendVotersRecord(VoterSet voters, long currentTimeMs) {
        return accumulator.appendVotersRecord(
            voters.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION),
            currentTimeMs
        );
    }

    public boolean isResignRequested() {
        return resignRequested;
    }

    public boolean isReplicaCaughtUp(ReplicaKey replicaKey, long currentTimeMs) {
        // In summary, let's consider a replica caught up for add voter, if they
        // have fetched within the last hour
        long anHourInMs = TimeUnit.HOURS.toMillis(1);
        return Optional.ofNullable(observerStates.get(replicaKey))
            .map(state ->
                state.lastCaughtUpTimestamp > 0 &&
                state.lastFetchTimestamp > 0 &&
                state.lastFetchTimestamp > currentTimeMs - anHourInMs
            )
            .orElse(false);
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
        return ElectionState.withElectedLeader(epoch, localReplicaKey.id(), voterStates.keySet());
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public Endpoints leaderEndpoints() {
        return localListeners;
    }

    Map<Integer, ReplicaState> voterStates() {
        return voterStates;
    }

    Map<ReplicaKey, ReplicaState> observerStates(final long currentTimeMs) {
        clearInactiveObservers(currentTimeMs);
        return observerStates;
    }

    public Set<Integer> grantingVoters() {
        return this.grantingVoters;
    }

    // visible for testing
    Set<ReplicaKey> nonAcknowledgingVoters() {
        Set<ReplicaKey> nonAcknowledging = new HashSet<>();
        for (ReplicaState state : voterStates.values()) {
            if (!state.hasAcknowledgedLeader) {
                nonAcknowledging.add(state.replicaKey);
            }
        }
        return nonAcknowledging;
    }

    private boolean maybeUpdateHighWatermark() {
        // Find the largest offset which is replicated to a majority of replicas (the leader counts)
        ArrayList<ReplicaState> followersByDescendingFetchOffset = followersByDescendingFetchOffset()
            .collect(Collectors.toCollection(ArrayList::new));

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
            long highWatermarkUpdateOffset = highWatermarkUpdateMetadata.offset();

            if (highWatermarkUpdateOffset > epochStartOffset) {
                if (highWatermark.isPresent()) {
                    LogOffsetMetadata currentHighWatermarkMetadata = highWatermark.get();
                    if (highWatermarkUpdateOffset > currentHighWatermarkMetadata.offset()
                        || (highWatermarkUpdateOffset == currentHighWatermarkMetadata.offset() &&
                            !highWatermarkUpdateMetadata.metadata().equals(currentHighWatermarkMetadata.metadata()))) {
                        Optional<LogOffsetMetadata> oldHighWatermark = highWatermark;
                        highWatermark = highWatermarkUpdateOpt;
                        logHighWatermarkUpdate(
                            oldHighWatermark,
                            highWatermarkUpdateMetadata,
                            indexOfHw,
                            followersByDescendingFetchOffset
                        );
                        return true;
                    } else if (highWatermarkUpdateOffset < currentHighWatermarkMetadata.offset()) {
                        log.info("The latest computed high watermark {} is smaller than the current " +
                                "value {}, which should only happen when voter set membership changes. If the voter " +
                                "set has not changed this suggests that one of the voters has lost committed data. " +
                                "Full voter replication state: {}", highWatermarkUpdateOffset,
                            currentHighWatermarkMetadata.offset(), voterStates.values());
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
     * @param lastVoterSet the up-to-date voter set
     * @return true if the high watermark is updated as a result of this call
     */
    public boolean updateLocalState(
        LogOffsetMetadata endOffsetMetadata,
        VoterSet lastVoterSet
    ) {
        ReplicaState state = getOrCreateReplicaState(localReplicaKey);
        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset.offset() > endOffsetMetadata.offset()) {
                throw new IllegalStateException("Detected non-monotonic update of local " +
                    "end offset: " + currentEndOffset.offset() + " -> " + endOffsetMetadata.offset());
            }
        });

        state.updateLeaderEndOffset(endOffsetMetadata);
        updateVoterAndObserverStates(lastVoterSet);

        return maybeUpdateHighWatermark();
    }

    /**
     * Update the replica state in terms of fetch time and log end offsets.
     *
     * @param replicaKey replica key
     * @param currentTimeMs current time in milliseconds
     * @param fetchOffsetMetadata new log offset and metadata
     * @return true if the high watermark is updated as a result of this call
     */
    public boolean updateReplicaState(
        ReplicaKey replicaKey,
        long currentTimeMs,
        LogOffsetMetadata fetchOffsetMetadata
    ) {
        // Ignore fetches from negative replica id, as it indicates
        // the fetch is from non-replica. For example, a consumer.
        if (replicaKey.id() < 0) {
            return false;
        } else if (replicaKey.id() == localReplicaKey.id()) {
            throw new IllegalStateException(
                String.format("Remote replica ID %s matches the local leader ID", replicaKey)
            );
        }

        ReplicaState state = getOrCreateReplicaState(replicaKey);

        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset.offset() > fetchOffsetMetadata.offset()) {
                log.warn("Detected non-monotonic update of fetch offset from nodeId {}: {} -> {}",
                    state.replicaKey, currentEndOffset.offset(), fetchOffsetMetadata.offset());
            }
        });

        Optional<LogOffsetMetadata> leaderEndOffsetOpt = getOrCreateReplicaState(localReplicaKey).endOffset;

        state.updateFollowerState(
            currentTimeMs,
            fetchOffsetMetadata,
            leaderEndOffsetOpt
        );
        updateCheckQuorumForFollowingVoter(replicaKey, currentTimeMs);

        return isVoter(state.replicaKey) && maybeUpdateHighWatermark();
    }

    public List<ReplicaKey> nonLeaderVotersByDescendingFetchOffset() {
        return followersByDescendingFetchOffset()
            .filter(state -> !state.matchesKey(localReplicaKey))
            .map(state -> state.replicaKey)
            .collect(Collectors.toList());
    }

    private Stream<ReplicaState> followersByDescendingFetchOffset() {
        return voterStates
            .values()
            .stream()
            .sorted();
    }

    public void addAcknowledgementFrom(int remoteNodeId) {
        ReplicaState voterState = ensureValidVoter(remoteNodeId);
        voterState.hasAcknowledgedLeader = true;
    }

    private ReplicaState ensureValidVoter(int remoteNodeId) {
        ReplicaState state = voterStates.get(remoteNodeId);
        if (state == null) {
            throw new IllegalArgumentException("Unexpected acknowledgement from non-voter " + remoteNodeId);
        }
        return state;
    }

    public long epochStartOffset() {
        return epochStartOffset;
    }

    private ReplicaState getOrCreateReplicaState(ReplicaKey replicaKey) {
        ReplicaState state = voterStates.get(replicaKey.id());
        if (state == null || !state.matchesKey(replicaKey)) {
            observerStates.putIfAbsent(replicaKey, new ReplicaState(replicaKey, false, Endpoints.empty()));
            return observerStates.get(replicaKey);
        }
        return state;
    }

    public Optional<ReplicaState> getReplicaState(ReplicaKey replicaKey) {
        ReplicaState state = voterStates.get(replicaKey.id());
        if (state == null || !state.matchesKey(replicaKey)) {
            state = observerStates.get(replicaKey);
        }

        return Optional.ofNullable(state);
    }

    /**
     * Clear observer states that have not been active for a while and are not the leader.
     */
    private void clearInactiveObservers(final long currentTimeMs) {
        observerStates.entrySet().removeIf(integerReplicaStateEntry ->
            currentTimeMs - integerReplicaStateEntry.getValue().lastFetchTimestamp >= OBSERVER_SESSION_TIMEOUT_MS &&
            !integerReplicaStateEntry.getKey().equals(localReplicaKey)
        );
    }

    private boolean isVoter(ReplicaKey remoteReplicaKey) {
        ReplicaState state = voterStates.get(remoteReplicaKey.id());
        return state != null && state.matchesKey(remoteReplicaKey);
    }

    private void updateVoterAndObserverStates(VoterSet lastVoterSet) {
        Map<Integer, ReplicaState> newVoterStates = new HashMap<>();
        Map<Integer, ReplicaState> oldVoterStates = new HashMap<>(voterStates);

        // Compute the new voter states map
        for (VoterSet.VoterNode voterNode : lastVoterSet.voterNodes()) {
            ReplicaState state = getReplicaState(voterNode.voterKey())
                .orElse(new ReplicaState(voterNode.voterKey(), false, voterNode.listeners()));

            // Remove the voter from the previous data structures
            oldVoterStates.remove(voterNode.voterKey().id());
            observerStates.remove(voterNode.voterKey());

            // Make sure that the replica key in the replica state matches the voter's
            state.setReplicaKey(voterNode.voterKey());

            // Make sure that the listeners are updated
            state.updateListeners(voterNode.listeners());
            newVoterStates.put(state.replicaKey.id(), state);
        }
        voterStates = newVoterStates;

        // Move any of the remaining old voters to observerStates
        for (ReplicaState replicaStateEntry : oldVoterStates.values()) {
            replicaStateEntry.clearListeners();
            observerStates.putIfAbsent(replicaStateEntry.replicaKey, replicaStateEntry);
        }
    }

    public static class ReplicaState implements Comparable<ReplicaState> {
        private ReplicaKey replicaKey;
        private Endpoints listeners;
        private Optional<LogOffsetMetadata> endOffset;
        private long lastFetchTimestamp;
        private long lastFetchLeaderLogEndOffset;
        private long lastCaughtUpTimestamp;
        private boolean hasAcknowledgedLeader;

        public ReplicaState(ReplicaKey replicaKey, boolean hasAcknowledgedLeader, Endpoints listeners) {
            this.replicaKey = replicaKey;
            this.listeners = listeners;
            this.endOffset = Optional.empty();
            this.lastFetchTimestamp = -1;
            this.lastFetchLeaderLogEndOffset = -1;
            this.lastCaughtUpTimestamp = -1;
            this.hasAcknowledgedLeader = hasAcknowledgedLeader;
        }

        public ReplicaKey replicaKey() {
            return replicaKey;
        }

        public Endpoints listeners() {
            return listeners;
        }

        public Optional<LogOffsetMetadata> endOffset() {
            return endOffset;
        }

        public long lastFetchTimestamp() {
            return lastFetchTimestamp;
        }

        public long lastCaughtUpTimestamp() {
            return lastCaughtUpTimestamp;
        }

        void setReplicaKey(ReplicaKey replicaKey) {
            if (this.replicaKey.id() != replicaKey.id()) {
                throw new IllegalArgumentException(
                    String.format(
                        "Attempting to update the replica key %s with a different replica id %s",
                        this.replicaKey,
                        replicaKey
                    )
                );
            } else if (this.replicaKey.directoryId().isPresent() &&
                !this.replicaKey.equals(replicaKey)
            ) {
                throw new IllegalArgumentException(
                    String.format(
                        "Attempting to update an already set directory id %s with a different directory id %s",
                        this.replicaKey,
                        replicaKey
                    )
                );
            }

            this.replicaKey = replicaKey;
        }

        void updateListeners(Endpoints listeners) {
            this.listeners = listeners;
        }

        void clearListeners() {
            updateListeners(Endpoints.empty());
        }

        boolean matchesKey(ReplicaKey replicaKey) {
            if (this.replicaKey.id() != replicaKey.id()) return false;

            if (this.replicaKey.directoryId().isPresent()) {
                return this.replicaKey.directoryId().equals(replicaKey.directoryId());
            } else {
                // it doesn't include a directory id so it matches as long as the ids match
                return true;
            }
        }

        void updateLeaderEndOffset(
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
                if (fetchOffsetMetadata.offset() >= leaderEndOffset.offset()) {
                    lastCaughtUpTimestamp = Math.max(lastCaughtUpTimestamp, currentTimeMs);
                } else if (lastFetchLeaderLogEndOffset > 0
                    && fetchOffsetMetadata.offset() >= lastFetchLeaderLogEndOffset) {
                    lastCaughtUpTimestamp = Math.max(lastCaughtUpTimestamp, lastFetchTimestamp);
                }
                lastFetchLeaderLogEndOffset = leaderEndOffset.offset();
            });

            lastFetchTimestamp = Math.max(lastFetchTimestamp, currentTimeMs);
            endOffset = Optional.of(fetchOffsetMetadata);
            hasAcknowledgedLeader = true;
        }

        @Override
        public int compareTo(ReplicaState that) {
            if (this.endOffset.equals(that.endOffset))
                return this.replicaKey.compareTo(that.replicaKey);
            else if (this.endOffset.isEmpty())
                return 1;
            else if (that.endOffset.isEmpty())
                return -1;
            else
                return Long.compare(that.endOffset.get().offset(), this.endOffset.get().offset());
        }

        @Override
        public String toString() {
            return String.format(
                "ReplicaState(replicaKey=%s, endOffset=%s, lastFetchTimestamp=%s, " +
                "lastCaughtUpTimestamp=%s, hasAcknowledgedLeader=%s)",
                replicaKey,
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
            "Leader(localReplicaKey=%s, epoch=%d, epochStartOffset=%d, highWatermark=%s, voterStates=%s)",
            localReplicaKey,
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
        resetAddVoterHandlerState(Errors.NOT_LEADER_OR_FOLLOWER, null, Optional.empty());
        resetRemoveVoterHandlerState(Errors.NOT_LEADER_OR_FOLLOWER, null, Optional.empty());

        accumulator.close();
    }
}
