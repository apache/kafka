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
 * 6. Update voter set with new voter configuration.
 *    a. If reconfiguration is supported, append the updated VotersRecord to the log. The KRaft internal listener will read this
 *       uncommitted record from the log and update the voter in the set of voters.
 *    b. If reconfiguration is not supported, update the in-memory information for the voter. This will get
 *       appended to the log when the cluster is upgraded to a kraft version that supports reconfiguration.
 * 7. Send the UpdateVoter successful response to the voter.
 */
public final class UpdateVoterHandler {
    private final OptionalInt localId;
    private final KRaftControlRecordStateMachine partitionState;
    private final ListenerName defaultListenerName;

    public UpdateVoterHandler(
        OptionalInt localId,
        KRaftControlRecordStateMachine partitionState,
        ListenerName defaultListenerName
    ) {
        this.localId = localId;
        this.partitionState = partitionState;
        this.defaultListenerName = defaultListenerName;
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

        // Read the in-memory volatile voters set if one exists
        KRaftVersion kraftVersion = partitionState.lastKraftVersion();
        Optional<VoterSet> voters = currentVoters(leaderState, highWatermark.get(), kraftVersion);
        if (voters.isEmpty()) {
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
        Optional<VoterSet> updatedVoters = updateVoters(
            voters.get(),
            kraftVersion,
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

        storeUpdatedVoters(leaderState, voters.get(), kraftVersion, currentTimeMs);

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

    private Optional<VoterSet> currentVoters(
        LeaderState<?> leaderState,
        long highWatermark,
        KRaftVersion kraftVersion
    ) {
        if (kraftVersion.isReconfigSupported()) {
            // Check that there are no uncommitted VotersRecord
            Optional<LogHistory.Entry<VoterSet>> votersEntry = partitionState.lastVoterSetEntry();
            if (votersEntry.isEmpty() || votersEntry.get().offset() >= highWatermark) {
                return Optional.empty();
            }

            return votersEntry.map(LogHistory.Entry::value);
        } else {
            return Optional.of(leaderState.volatileVoters().orElseGet(partitionState::lastVoterSet));
        }
    }

    private Optional<VoterSet> updateVoters(
        VoterSet voters,
        KRaftVersion kraftVersion,
        VoterSet.VoterNode updatedVoter
    ) {
        return kraftVersion.isReconfigSupported() ?
            voters.updateVoter(updatedVoter) :
            voters.unsafeUpdateVoter(updatedVoter);
    }

    private void storeUpdatedVoters(
        LeaderState<?> leaderState,
        VoterSet voters,
        KRaftVersion kraftVersion,
        long currentTimeMs
    ) {
        if (kraftVersion.isReconfigSupported()) {
            // Since the partition support reconfig then just write the update voter set directly to the log
            leaderState.appendVotersRecord(voters, currentTimeMs);
        } else {
            // Store the new voters set in the leader state since it cannot be written to the log
            leaderState.updateVolatileVoters(voters);
        }
    }
}
