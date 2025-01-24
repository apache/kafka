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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
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

/**
 * This type implements the protocol for removing a voter from a KRaft partition.
 *
 * The general algorithm for removing voter to the voter set is:
 *
 * 1. Check that the leader has fenced the previous leader(s) by checking that the HWM is known,
 *    otherwise return the REQUEST_TIMED_OUT error.
 * 2. Check that the cluster supports kraft.version 1, otherwise return the UNSUPPORTED_VERSION error.
 * 3. Check that there are no uncommitted voter changes, otherwise return the REQUEST_TIMED_OUT error.
 * 4. Append the updated VotersRecord to the log. The KRaft internal listener will read this
 *    uncommitted record from the log and remove the voter from the set of voters.
 * 5. Wait for the VotersRecord to commit using the majority of the new set of voters. Return a
 *    REQUEST_TIMED_OUT error if it doesn't commit in time.
 * 6. Send the RemoveVoter successful response to the client.
 * 7. Resign the leadership if the leader is not in the new voter set
 */
public final class RemoveVoterHandler {
    private final Optional<ReplicaKey> localReplicaKey;
    private final KRaftControlRecordStateMachine partitionState;
    private final Time time;
    private final long requestTimeoutMs;
    private final Logger logger;

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

    public CompletableFuture<RemoveRaftVoterResponseData> handleRemoveVoterRequest(
        LeaderState<?> leaderState,
        ReplicaKey voterKey,
        long currentTimeMs
    ) {
        // Check if there are any pending voter change requests
        if (leaderState.isOperationPending(currentTimeMs)) {
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

        // Remove the voter from the set of voters
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
        leaderState.resetRemoveVoterHandlerState(Errors.UNKNOWN_SERVER_ERROR, null, Optional.of(state));

        return state.future();
    }

    public void highWatermarkUpdated(LeaderState<?> leaderState) {
        leaderState.removeVoterHandlerState().ifPresent(current -> {
            leaderState.highWatermark().ifPresent(highWatermark -> {
                if (highWatermark.offset() > current.lastOffset()) {
                    // VotersRecord with the removed voter was committed; complete the RPC
                    leaderState.resetRemoveVoterHandlerState(Errors.NONE, null, Optional.empty());

                    // Resign if the leader is not part of the new committed voter set
                    VoterSet voters = partitionState.lastVoterSet();
                    ReplicaKey localKey = localReplicaKey.orElseThrow(
                        () -> new IllegalStateException(
                            String.format(
                                "Leaders mush have an id and directory id %s",
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
        });
    }
}
