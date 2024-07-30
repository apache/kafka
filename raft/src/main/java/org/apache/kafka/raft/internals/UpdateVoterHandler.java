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

import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.LeaderState;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.RaftUtil;
import org.apache.kafka.server.common.KRaftVersion;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

/**
 * TODO: document this.
 *
 * 1. Wait until there are no uncommitted add or remove voter records. Note that the implementation
 *    may just return a REQUEST_TIMED_OUT error if there are pending operations.
 * 2. Wait for the LeaderChangeMessage control record from the current epoch to get committed. Note
 *    that the implementation may just return a REQUEST_TIMED_OUT error if there are pending operations.
 * 3. Check that the updated voter supports the current kraft.version.
 * 4. If the replica id tracked doesn't have a replica directory id, update it with the replica
 *    directory id provided in the request.
 * 5. Append the updated VotersRecord to the log if the finalized kraft.version is greater than 0.
 * 6. The KRaft internal listener will read this record from the log and update the voter's
 *    information. This includes updating the endpoint used by the KRaft NetworkClient.
 * 7. Wait for the VotersRecord to commit using the majority of the new set of voters.
 * 8. Send the UpdateVoter response to the client.
 */
public final class UpdateVoterHandler {
    private final OptionalInt localId;
    private final KRaftControlRecordStateMachine partitionState;
    private final ListenerName defaultListenerName;
    private final Time time;
    private final long requestTimeoutMs;

    public UpdateVoterHandler(
        OptionalInt localId,
        KRaftControlRecordStateMachine partitionState,
        ListenerName defaultListenerName,
        Time time,
        long requestTimeoutMs
    ) {
        this.localId = localId;
        this.partitionState = partitionState;
        this.defaultListenerName = defaultListenerName;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public CompletableFuture<UpdateRaftVoterResponseData> handleUpdateVoterRequest(
        LeaderState<?> leaderState,
        ListenerName requestListenerName,
        ReplicaKey voterKey,
        Endpoints voterEndpoints,
        UpdateRaftVoterRequestData.KRaftVersionFeature supportedKraftVersions,
        long currentTimeMs
    ) {
        // Check if there are any pending voter change requests
        if (leaderState.isOperationPending(currentTimeMs)) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    requestListenerName,
                    new LeaderAndEpoch(
                        localId,
                        leaderState.epoch()
                    ),
                    leaderState.leaderEndpoints()
                )
            );
        }

        // Check that the leader has established a HWM and committed the current epoch
        Optional<Long> highWatermark = leaderState.highWatermark().map(LogOffsetMetadata::offset);
        if (!highWatermark.isPresent()) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    requestListenerName,
                    new LeaderAndEpoch(
                        localId,
                        leaderState.epoch()
                    ),
                    leaderState.leaderEndpoints()
                )
            );
        }

        // Check that the cluster supports kraft.version >= 1
        // TODO: File a jira to handle the kraft.version == 0
        KRaftVersion kraftVersion = partitionState.lastKraftVersion();
        if (!kraftVersion.isReconfigSupported()) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.UNSUPPORTED_VERSION,
                    requestListenerName,
                    new LeaderAndEpoch(
                        localId,
                        leaderState.epoch()
                    ),
                    leaderState.leaderEndpoints()
                )
            );
        }

        // Check that there are no uncommitted VotersRecord
        Optional<LogHistory.Entry<VoterSet>> votersEntry = partitionState.lastVoterSetEntry();
        if (!votersEntry.isPresent() || votersEntry.get().offset() >= highWatermark.get()) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    requestListenerName,
                    new LeaderAndEpoch(
                        localId,
                        leaderState.epoch()
                    ),
                    leaderState.leaderEndpoints()
                )
            );
        }

        // Check that the supported version range is valid
        if (!validVersionRange(kraftVersion, supportedKraftVersions)) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.INVALID_REQUEST,
                    requestListenerName,
                    new LeaderAndEpoch(
                        localId,
                        leaderState.epoch()
                    ),
                    leaderState.leaderEndpoints()
                )
            );
        }

        // Check that endpoinds includes the default listener
        if (!voterEndpoints.address(defaultListenerName).isPresent()) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.INVALID_REQUEST,
                    requestListenerName,
                    new LeaderAndEpoch(
                        localId,
                        leaderState.epoch()
                    ),
                    leaderState.leaderEndpoints()
                )
            );
        }

        // Update the voter
        Optional<VoterSet> updatedVoters = votersEntry
            .get()
            .value()
            .updateVoter(
                VoterSet.VoterNode.of(
                    voterKey,
                    voterEndpoints,
                    new SupportedVersionRange(
                        supportedKraftVersions.minSupportedVersion(),
                        supportedKraftVersions.maxSupportedVersion()
                    )
                )
            );
        if (!updatedVoters.isPresent()) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.VOTER_NOT_FOUND,
                    requestListenerName,
                    new LeaderAndEpoch(
                        localId,
                        leaderState.epoch()
                    ),
                    leaderState.leaderEndpoints()
                )
            );
        }

        UpdateVoterHandlerState state = new UpdateVoterHandlerState(
            leaderState.appendVotersRecord(updatedVoters.get(), currentTimeMs),
            requestListenerName,
            time.timer(requestTimeoutMs)
        );
        leaderState.resetUpdateVoterHandlerState(Errors.UNKNOWN_SERVER_ERROR, Optional.of(state));

        return state.future();
    }

    public void highWatermarkUpdated(LeaderState<?> leaderState) {
        leaderState.updateVoterHandlerState().ifPresent(current -> {
            leaderState.highWatermark().ifPresent(highWatermark -> {
                if (highWatermark.offset() > current.lastOffset()) {
                    // VotersRecord with the updated voter was committed; complete the RPC
                    leaderState.resetUpdateVoterHandlerState(Errors.NONE, Optional.empty());
                }
            });
        });
    }

    private boolean validVersionRange(
        KRaftVersion finalizedVersion,
        UpdateRaftVoterRequestData.KRaftVersionFeature supportedKraftVersions
    ) {
        return supportedKraftVersions.minSupportedVersion() <= finalizedVersion.featureLevel() &&
            supportedKraftVersions.maxSupportedVersion() >= finalizedVersion.featureLevel();
    }
}
