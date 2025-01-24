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
import org.apache.kafka.raft.ReplicaKey;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.server.common.KRaftVersion;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

/**
 * This type implements the protocol for updating a voter from a KRaft partition.
 *
 * 1. Check that the leader has fenced the previous leader(s) by checking that the HWM is known,
 *    otherwise return the REQUEST_TIMED_OUT error.
 * 2. Check that the cluster supports kraft.version 1, otherwise return the UNSUPPORTED_VERSION error.
 * 3. Check that there are no uncommitted voter changes, otherwise return the REQUEST_TIMED_OUT error.
 * 4. Check that the updated voter still supports the currently finalized kraft.version, otherwise
 *    return the INVALID_REQUEST error.
 * 5. Check that the updated voter is still listening on the default listener.
 * 6. Append the updated VotersRecord to the log. The KRaft internal listener will read this
 *    uncommitted record from the log and update the voter in the set of voters.
 * 7. Send the UpdateVoter successful response to the voter.
 *
 * KAFKA-16538 is going to add support for handling this RPC when the kraft.version is 0.
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
        if (highWatermark.isEmpty()) {
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

        // KAFKA-16538 will implement the case when the kraft.version is 0
        // Check that the cluster supports kraft.version >= 1
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
        if (votersEntry.isEmpty() || votersEntry.get().offset() >= highWatermark.get()) {
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
        if (voterEndpoints.address(defaultListenerName).isEmpty()) {
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
        if (updatedVoters.isEmpty()) {
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

        leaderState.appendVotersRecord(updatedVoters.get(), currentTimeMs);

        // Reply immediately and don't wait for the change to commit
        return CompletableFuture.completedFuture(
            RaftUtil.updateVoterResponse(
                Errors.NONE,
                requestListenerName,
                new LeaderAndEpoch(
                    localId,
                    leaderState.epoch()
                ),
                leaderState.leaderEndpoints()
            )
        );
    }

    private boolean validVersionRange(
        KRaftVersion finalizedVersion,
        UpdateRaftVoterRequestData.KRaftVersionFeature supportedKraftVersions
    ) {
        return supportedKraftVersions.minSupportedVersion() <= finalizedVersion.featureLevel() &&
            supportedKraftVersions.maxSupportedVersion() >= finalizedVersion.featureLevel();
    }
}
