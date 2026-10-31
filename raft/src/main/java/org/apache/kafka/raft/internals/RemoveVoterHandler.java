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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.raft.LeaderState;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.RaftUtil;
import org.apache.kafka.raft.ReplicaKey;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.server.common.KRaftVersion;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * This type implements the protocol for removing a voter from a KRaft partition.
 *
 * The general algorithm for removing a voter from the voter set is:
 *
 * 1. Check that the leader has fenced the previous leader(s) by checking that the HWM is known,
 *    otherwise return the REQUEST_TIMED_OUT error.
 * 2. Check that the cluster supports kraft.version 1, otherwise return the UNSUPPORTED_VERSION error.
 * 3. Check that there are no uncommitted voter changes, otherwise return the REQUEST_TIMED_OUT error.
 * 4. Check that the voter being removed is part of the current voter set, otherwise return the
 *    VOTER_NOT_FOUND error.
 * 5. Append the updated VotersRecord to the log. The KRaft internal listener will read this
 *    uncommitted record from the log and remove the voter from the set of voters.
 * 6. Wait for the VotersRecord to commit using the majority of the new set of voters, see
 *    {@link #highWatermarkUpdated}. Return a REQUEST_TIMED_OUT error if it doesn't commit in
 *    time, see {@link ChangeVoterHandlerState#maybeExpirePendingOperation}.
 * 7. Send the RemoveVoter successful response to the client.
 * 8. Resign the leadership if the leader is not in the new voter set.
 *
 * Unlike {@link AddVoterHandler} and {@link UpdateVoterHandler}, this operation does not need to
 * contact the removed voter, so the VotersRecord is appended synchronously within
 * {@link #handleRemoveVoterRequest} instead of waiting for an API_VERSIONS round trip.
 */
public final class RemoveVoterHandler {
    private final Optional<ReplicaKey> localReplicaKey;
    private final KRaftControlRecordStateMachine partitionState;
    private final Time time;
    private final long requestTimeoutMs;
    private final Logger logger;

    /**
     * Creates a new handler for remove voter requests.
     *
     * @param nodeId this replica's node id, if it is eligible to be a voter; used together with
     *        {@code nodeDirectoryId} to detect when the leader itself is being removed, so that
     *        it can resign
     * @param nodeDirectoryId this replica's directory id, used together with {@code nodeId} to
     *        build its {@link ReplicaKey}
     * @param partitionState the KRaft partition state, used to read the currently finalized
     *        kraft.version and the log's voter set
     * @param time the time implementation, used to create the timer that bounds a pending operation
     * @param requestTimeoutMs the timeout, in milliseconds, for the appended VotersRecord to commit
     * @param logContext used to create this class's logger
     */
    public RemoveVoterHandler(
        OptionalInt nodeId,
        Uuid nodeDirectoryId,
        KRaftControlRecordStateMachine partitionState,
        Time time,
        long requestTimeoutMs,
        LogContext logContext
    ) {
        this.localReplicaKey = nodeId.isPresent() ?
            Optional.of(ReplicaKey.of(nodeId.getAsInt(), nodeDirectoryId)) :
            Optional.empty();
        this.partitionState = partitionState;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.logger = logContext.logger(RemoveVoterHandler.class);
    }

    /**
     * Handle a RemoveVoter request.
     * <p>
     * See the class documentation for the full set of steps that this method and
     * {@link #highWatermarkUpdated} perform together.
     *
     * @param leaderState the leader state
     * @param voterKey the id and directory id of the voter to remove
     * @param currentTimeMs the current time in milliseconds
     * @return a future for the RemoveVoter response; it completes immediately if the request is
     *         rejected outright, or later, once the appended VotersRecord commits, via
     *         {@link #highWatermarkUpdated}
     */
    public CompletionStage<RemoveRaftVoterResponseData> handleRemoveVoterRequest(
        LeaderState<?> leaderState,
        ReplicaKey voterKey,
        long currentTimeMs
    ) {
        var changeVoterState = leaderState.changeVoterState();
        // Check if there are any pending voter change requests
        if (
            changeVoterState.isOperationPending(
                leaderState.leaderAndEpoch(),
                leaderState.leaderEndpoints(),
                currentTimeMs
            )
        ) {
            return CompletableFuture.completedFuture(
                RaftUtil.removeVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    "Request timed out waiting for leader to handle previous voter change request"
                )
            );
        }

        // Check that the leader has established a HWM and committed the current epoch
        Optional<Long> highWatermark = leaderState.highWatermark().map(LogOffsetMetadata::offset);
        if (highWatermark.isEmpty()) {
            return CompletableFuture.completedFuture(
                RaftUtil.removeVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    "Request timed out waiting for leader to establish HWM and fence previous voter changes"
                )
            );
        }

        // Check that the cluster supports kraft.version >= 1
        KRaftVersion kraftVersion = partitionState.lastKraftVersion();
        if (!kraftVersion.isReconfigSupported()) {
            return CompletableFuture.completedFuture(
                RaftUtil.removeVoterResponse(
                    Errors.UNSUPPORTED_VERSION,
                    String.format(
                        "Cluster doesn't support removing voter because the %s feature is %s",
                        kraftVersion.featureName(),
                        kraftVersion.featureLevel()
                    )
                )
            );
        }

        // Check that there are no uncommitted VotersRecord
        Optional<LogHistory.Entry<VoterSet>> votersEntry = partitionState.lastVoterSetEntry();
        if (votersEntry.isEmpty() || votersEntry.get().offset() >= highWatermark.get()) {
            return CompletableFuture.completedFuture(
                RaftUtil.removeVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    String.format(
                        "Request timed out waiting for voters to commit the latest voter change at %s with HWM %d",
                        votersEntry.map(LogHistory.Entry::offset),
                        highWatermark.get()
                    )
                )
            );
        }

        // Remove the voter from the set of voters, returning VOTER_NOT_FOUND if it isn't a
        // current voter
        Optional<VoterSet> newVoters = votersEntry.get().value().removeVoter(voterKey);
        if (newVoters.isEmpty()) {
            return CompletableFuture.completedFuture(
                RaftUtil.removeVoterResponse(
                    Errors.VOTER_NOT_FOUND,
                    String.format(
                        "Cannot remove voter %s from the set of voters %s",
                        voterKey,
                        votersEntry.get().value().voterKeys()
                    )
                )
            );
        }

        // Append the record to the log
        RemoveVoterHandlerState state = new RemoveVoterHandlerState(
            leaderState.appendVotersRecord(newVoters.get(), currentTimeMs),
            time.timer(requestTimeoutMs)
        );
        changeVoterState.resetRemoveVoterHandlerState(Errors.UNKNOWN_SERVER_ERROR, null, Optional.of(state));

        return state.future();
    }

    /**
     * Called when the high watermark advances to check if a pending remove voter operation has
     * committed.
     * <p>
     * Once the high watermark advances past the offset of the appended VotersRecord, this
     * completes the pending RemoveVoter response with success, and resigns the leadership if the
     * leader itself is no longer part of the committed voter set.
     *
     * @param leaderState the leader state
     * @param highWatermark the new high watermark offset
     */
    public void highWatermarkUpdated(LeaderState<?> leaderState, long highWatermark) {
        var changeVoterState = leaderState.changeVoterState();

        changeVoterState
            .removeVoterHandlerState()
            .ifPresent(current -> {
                if (highWatermark > current.lastOffset()) {
                    // VotersRecord with the removed voter was committed; complete the RPC
                    changeVoterState.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.empty());

                    // Resign if the leader is not part of the new committed voter set
                    VoterSet voters = partitionState.lastVoterSet();
                    ReplicaKey localKey = localReplicaKey.orElseThrow(
                        () -> new IllegalStateException(
                            String.format(
                                "Leaders must have an id and directory id %s",
                                localReplicaKey
                            )
                        )
                    );
                    if (!voters.isVoter(localKey)) {
                        logger.info(
                            "Leader is not in the committed voter set {} resign from epoch {}",
                            voters.voterKeys(),
                            leaderState.epoch()
                        );

                        leaderState.requestResign();
                    }
                }
            });
    }
}
