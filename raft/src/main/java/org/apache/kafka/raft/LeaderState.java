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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordUtils;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.raft.internals.AddVoterHandlerState;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.KRaftVersionUpgrade;
import org.apache.kafka.raft.internals.KafkaRaftMetrics;
import org.apache.kafka.raft.internals.RemoveVoterHandlerState;
import org.apache.kafka.server.common.KRaftVersion;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
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
import java.util.concurrent.atomic.AtomicReference;
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

    private final VoterSet.VoterNode localVoterNode;
    private final int epoch;
    private final long epochStartOffset;
    private final Set<Integer> grantingVoters;
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
    // The set includes all the followers voters that FETCH or FETCH_SNAPSHOT during the current checkQuorumTimer interval.
    private final Set<Integer> fetchedVoters = new HashSet<>();
    private final Timer checkQuorumTimer;
    private final int checkQuorumTimeoutMs;
    private final Timer beginQuorumEpochTimer;
    private final int beginQuorumEpochTimeoutMs;
    private final KafkaRaftMetrics kafkaRaftMetrics;

    // This is volatile because resignation can be requested from an external thread.
    private volatile boolean resignRequested = false;

    /* Used to coordinate the upgrade of the kraft.version from 0 to 1. The upgrade is triggered by
     * the clients to RaftClient.
     *  1. if the kraft version is 0, the starting state is the Voters type. The voter set is the voters in
     *     the static voter set with the leader updated. See KRaftVersionUpgrade for details on the
     *     Voters type.
     *  2. as the leader receives UpdateRaftVoter requests, it updates the associated Voters type. Only
     *     after all of the voters have been updated will an upgrade successfully complete.
     *  3. a client of RaftClient triggers the upgrade and transition this state to the Version
     *     type. See KRaftVersionUpgrade for details on the Version type.
     *
     * All transition are coordinated using optimistic locking by always calling AtomicReference#compareAndSet
     */
    private final AtomicReference<KRaftVersionUpgrade> kraftVersionUpgradeState = new AtomicReference<>(
        KRaftVersionUpgrade.empty()
    );

    protected LeaderState(
        Time time,
        VoterSet.VoterNode localVoterNode,
        int epoch,
        long epochStartOffset,
        VoterSet voterSetAtEpochStart,
        OptionalLong offsetOfVotersAtEpochStart,
        KRaftVersion kraftVersionAtEpochStart,
        Set<Integer> grantingVoters,
        BatchAccumulator<T> accumulator,
        int fetchTimeoutMs,
        LogContext logContext,
        KafkaRaftMetrics kafkaRaftMetrics
    ) {
        if (localVoterNode.voterKey().directoryId().isEmpty()) {
            throw new IllegalArgumentException(
                String.format("Unknown local replica directory id: %s", localVoterNode)
            );
        } else if (!voterSetAtEpochStart.isVoter(localVoterNode.voterKey())) {
            throw new IllegalArgumentException(
                String.format(
                    "Local replica %s is not a voter in %s",
                    localVoterNode,
                    voterSetAtEpochStart
                )
            );
        }

        this.localVoterNode = localVoterNode;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;

        for (VoterSet.VoterNode voterNode: voterSetAtEpochStart.voterNodes()) {
            boolean hasAcknowledgedLeader = voterNode.isVoter(localVoterNode.voterKey());
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

        kafkaRaftMetrics.addLeaderMetrics();
        this.kafkaRaftMetrics = kafkaRaftMetrics;

        if (!kraftVersionAtEpochStart.isReconfigSupported()) {
            var updatedVoters = voterSetAtEpochStart
                .updateVoterIgnoringDirectoryId(localVoterNode)
                .orElseThrow(
                    () -> new IllegalStateException(
                        String.format(
                            "Unable to update voter set %s with the latest leader information %s",
                            voterSetAtEpochStart,
                            localVoterNode
                        )
                    )
                );
            kraftVersionUpgradeState.set(new KRaftVersionUpgrade.Voters(updatedVoters));
        }
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
     * Determines the set of replicas that should receive a {@code BeginQuorumEpoch} request
     * based on the elapsed time since their last fetch.
     * <p>
     * For each remote voter (excluding the local node), if the time since the last
     * fetch exceeds the configured {@code beginQuorumEpochTimeoutMs}, the replica
     * is considered to need a new quorum epoch request.
     *
     * @param currentTimeMs the current system time in milliseconds
     * @return an unmodifiable set of {@link ReplicaKey} objects representing replicas
     *         that need to receive a {@code BeginQuorumEpoch} request
     */
    public Set<ReplicaKey> needToSendBeginQuorumRequests(long currentTimeMs) {
        return voterStates.values()
            .stream()
            .filter(
                state -> state.replicaKey.id() != localVoterNode.voterKey().id() &&
                currentTimeMs - state.lastFetchTimestamp >= beginQuorumEpochTimeoutMs
            )
            .map(ReplicaState::replicaKey)
            .collect(Collectors.toUnmodifiableSet());
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
                voterStates.values()
                    .stream()
                    .map(voter -> voter.replicaKey)
                    .collect(Collectors.toUnmodifiableSet())
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
        if (voterStates.containsKey(localVoterNode.voterKey().id())) {
            majority = majority - 1;
        }

        if (fetchedVoters.size() >= majority) {
            fetchedVoters.clear();
            checkQuorumTimer.update(currentTimeMs);
            checkQuorumTimer.reset(checkQuorumTimeoutMs);
        }
    }

    private void updateFetchedVoters(ReplicaKey replicaKey) {
        if (replicaKey.id() == localVoterNode.voterKey().id()) {
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
        updateUncommittedVoterChangeMetric();
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
        updateUncommittedVoterChangeMetric();
    }

    private void updateUncommittedVoterChangeMetric() {
        kafkaRaftMetrics.updateUncommittedVoterChange(
            addVoterHandlerState.isPresent() || removeVoterHandlerState.isPresent()
        );
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

    private static MemoryRecordsBuilder createControlRecordsBuilder(
        long baseOffset,
        int epoch,
        Compression compression,
        ByteBuffer buffer,
        long currentTimeMs
    ) {
        return new MemoryRecordsBuilder(
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
        );
    }


    public void appendStartOfEpochControlRecords(long currentTimeMs) {
        List<Voter> voters = convertToVoters(voterStates.keySet());
        List<Voter> grantingVoters = convertToVoters(this.grantingVoters());

        LeaderChangeMessage leaderChangeMessage = new LeaderChangeMessage()
            .setVersion(ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION)
            .setLeaderId(this.election().leaderId())
            .setVoters(voters)
            .setGrantingVoters(grantingVoters);

        accumulator.appendControlMessages((baseOffset, epoch, compression, buffer) -> {
            try (MemoryRecordsBuilder builder = createControlRecordsBuilder(
                    baseOffset,
                    epoch,
                    compression,
                    buffer,
                    currentTimeMs
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

    public boolean compareAndSetVolatileVoters(
        KRaftVersionUpgrade.Voters oldVoters,
        KRaftVersionUpgrade.Voters newVoters
    ) {
        return kraftVersionUpgradeState.compareAndSet(oldVoters, newVoters);
    }

    public Optional<KRaftVersionUpgrade.Voters> volatileVoters() {
        return kraftVersionUpgradeState.get().toVoters();
    }

    public Optional<KRaftVersionUpgrade.Version> requestedKRaftVersion() {
        return kraftVersionUpgradeState.get().toVersion();
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

    /**
     * Upgrade the kraft version.
     *
     * This methods upgrades the kraft version to {@code newVersion}. If the version is already
     * {@code newVersion}, this is a noop operation.
     *
     * KRaft only supports upgrades, so {@code newVersion} must be greater than or equal to curent
     * kraft version {@code persistedVersion}.
     *
     * For the upgrade to succeed all of the voters in the voter set must support the new kraft
     * version. The upgrade from kraft version 0 to kraft version 1 generate one control batch
     * with one control record setting the kraft version to 1 and one voters record setting the
     * updated voter set.
     *
     * When {@code validateOnly} is true only the validation is perform and the control records are
     * not generated.
     *
     * @param currentEpoch the current epoch
     * @param newVersion the new kraft version
     * @param persistedVersion the kraft version persisted to disk
     * @param persistedVoters the set of voters persisted to disk
     * @param validateOnly determine if only validation should be performed
     * @param currentTimeMs the current time
     */
    public boolean maybeAppendUpgradedKRaftVersion(
        int currentEpoch,
        KRaftVersion newVersion,
        KRaftVersion persistedVersion,
        VoterSet persistedVoters,
        boolean validateOnly,
        long currentTimeMs
    ) {
        validateEpoch(currentEpoch);

        var pendingVersion = kraftVersionUpgradeState.get().toVersion();
        if (pendingVersion.isPresent()) {
            if (pendingVersion.get().kraftVersion().equals(newVersion)) {
                // The version match; upgrade is a noop
                return false;
            } else {
                throw new InvalidUpdateVersionException(
                    String.format(
                        "Invalid concurrent upgrade of %s from version %s to %s",
                        KRaftVersion.FEATURE_NAME,
                        pendingVersion.get(),
                        newVersion
                    )
                );
            }
        } else if (persistedVersion.equals(newVersion)) {
            return false;
        } else if (persistedVersion.isMoreThan(newVersion)) {
            throw new InvalidUpdateVersionException(
                String.format(
                    "Invalid upgrade of %s from version %s to %s because the new version is a downgrade",
                    KRaftVersion.FEATURE_NAME,
                    persistedVersion,
                    newVersion
                )
            );
        }

        // Upgrade to kraft.verion 1 is only supported; this needs to change when kraft.version 2 is added
        var inMemoryVoters = kraftVersionUpgradeState.get().toVoters().orElseThrow(() ->
            new InvalidUpdateVersionException(
                String.format(
                    "Invalid upgrade of %s from version %s to %s",
                    KRaftVersion.FEATURE_NAME,
                    persistedVersion,
                    newVersion
                )
            )
        );
        if (!inMemoryVoters.voters().voterIds().equals(persistedVoters.voterIds())) {
            throw new IllegalStateException(
                String.format(
                    "Unable to update %s to %s due to missing voters %s compared to %s",
                    KRaftVersion.FEATURE_NAME,
                    newVersion,
                    inMemoryVoters.voters().voterIds(),
                    persistedVoters.voterIds()
                )
            );
        } else if (!inMemoryVoters.voters().supportsVersion(newVersion)) {
            log.info("Not all voters support kraft version {}: {}", newVersion, inMemoryVoters.voters());
            throw new InvalidUpdateVersionException(
                String.format(
                    "Invalid upgrade of %s to %s because not all of the voters support it",
                    KRaftVersion.FEATURE_NAME,
                    newVersion
                )
            );
        } else if (
            inMemoryVoters
                .voters()
                .voterKeys()
                .stream()
                .anyMatch(voterKey -> voterKey.directoryId().isEmpty())
        ) {
            throw new IllegalStateException(
                String.format(
                    "Directory id must be known for all of the voters: %s",
                    inMemoryVoters.voters()
                )
            );
        }

        if (!validateOnly) {
            /* Note that this only supports upgrades from kraft.version 0 to kraft.version 1. When
             * kraft.version 2 is added, this logic needs to be revisited
             */
            var successful = kraftVersionUpgradeState.compareAndSet(
                inMemoryVoters,
                new KRaftVersionUpgrade.Version(newVersion)
            );
            if (!successful) {
                throw new InvalidUpdateVersionException(
                    String.format(
                        "Unable to upgrade version for %s to %s due to changing voters",
                        KRaftVersion.FEATURE_NAME,
                        newVersion
                    )
                );
            }

            // All of the validations succeeded; create control records for the upgrade
            accumulator.appendControlMessages((baseOffset, batchEpoch, compression, buffer) -> {
                try (MemoryRecordsBuilder builder = createControlRecordsBuilder(
                        baseOffset,
                        batchEpoch,
                        compression,
                        buffer,
                        currentTimeMs
                    )
                ) {
                    log.info("Appended kraft.version {} to the batch accumulator", newVersion);
                    builder.appendKRaftVersionMessage(
                        currentTimeMs,
                        new KRaftVersionRecord()
                            .setVersion(newVersion.kraftVersionRecordVersion())
                            .setKRaftVersion(newVersion.featureLevel())
                    );

                    if (!inMemoryVoters.voters().equals(persistedVoters)) {
                        log.info("Appended voter set {} to the batch accumulator", inMemoryVoters.voters());
                        builder.appendVotersMessage(
                            currentTimeMs,
                            inMemoryVoters.voters().toVotersRecord(newVersion.votersRecordVersion())
                        );
                    }

                    return builder.build();
                }
            });
        }

        return true;
    }

    private void validateEpoch(int currentEpoch) {
        if (currentEpoch < epoch()) {
            throw new NotLeaderException(
                String.format(
                    "Upgrade kraft version failed because the given epoch %s is stale. Current leader epoch is %s",
                    currentEpoch,
                    epoch()
                )
            );
        } else if (currentEpoch > epoch()) {
            throw new IllegalArgumentException(
                String.format(
                    "Attempt to append from epoch %s which is larger than the current epoch of %s",
                    currentEpoch,
                    epoch()
                )
            );
        }
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(epoch, localVoterNode.voterKey().id(), Optional.empty(), voterStates.keySet());
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public Endpoints leaderEndpoints() {
        return localVoterNode.listeners();
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
        ReplicaState state = getOrCreateReplicaState(localVoterNode.voterKey());
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
        } else if (replicaKey.id() == localVoterNode.voterKey().id()) {
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

        Optional<LogOffsetMetadata> leaderEndOffsetOpt = getOrCreateReplicaState(localVoterNode.voterKey()).endOffset;

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
            .filter(state -> !state.matchesKey(localVoterNode.voterKey()))
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
            kafkaRaftMetrics.updateNumObservers(observerStates.size());
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
            !integerReplicaStateEntry.getKey().equals(localVoterNode.voterKey())
        );
        kafkaRaftMetrics.updateNumObservers(observerStates.size());
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
        kafkaRaftMetrics.updateNumObservers(observerStates.size());
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
    public boolean canGrantVote(ReplicaKey replicaKey, boolean isLogUpToDate, boolean isPreVote) {
        log.debug(
            "Rejecting Vote request (preVote={}) from replica ({}) since we are already leader in epoch {}",
            isPreVote,
            replicaKey,
            epoch
        );
        return false;
    }

    @Override
    public String toString() {
        return String.format(
            "Leader(localVoterNode=%s, epoch=%d, epochStartOffset=%d, highWatermark=%s, voterStates=%s)",
            localVoterNode,
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
        kafkaRaftMetrics.removeLeaderMetrics();

        accumulator.close();
    }
}
