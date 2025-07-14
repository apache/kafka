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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.LeaderAndEpoch;
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
    private final Logger log;

    public UpdateVoterHandler(
        OptionalInt localId,
        KRaftControlRecordStateMachine partitionState,
        ListenerName defaultListenerName,
        LogContext logContext
    ) {
        this.localId = localId;
        this.partitionState = partitionState;
        this.defaultListenerName = defaultListenerName;
        this.log = logContext.logger(getClass());
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

        // Read the voter set from the log or leader state
        KRaftVersion kraftVersion = partitionState.lastKraftVersion();
        final Optional<KRaftVersionUpgrade.Voters> inMemoryVoters;
        final Optional<VoterSet> voters;
        if (kraftVersion.isReconfigSupported()) {
            inMemoryVoters = Optional.empty();

            // Check that there are no uncommitted VotersRecord
            Optional<LogHistory.Entry<VoterSet>> votersEntry = partitionState.lastVoterSetEntry();
            if (votersEntry.isEmpty() || votersEntry.get().offset() >= highWatermark.get()) {
                voters = Optional.empty();
            } else {
                voters = votersEntry.map(LogHistory.Entry::value);
            }
        } else {
            inMemoryVoters = leaderState.volatileVoters();
            if (inMemoryVoters.isEmpty()) {
                /* This can happen if the remote voter sends an update voter request before the
                 * updated kraft version has been written to the log
                 */
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
            voters = inMemoryVoters.map(KRaftVersionUpgrade.Voters::voters);
        }
        if (voters.isEmpty()) {
            log.info("Unable to read the current voter set with kraft version {}", kraftVersion);
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

        // Check that endpoints includes the default listener
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

        return storeUpdatedVoters(
            leaderState,
            voterKey,
            inMemoryVoters,
            updatedVoters.get(),
            requestListenerName,
            currentTimeMs
        );
    }

    private boolean validVersionRange(
        KRaftVersion finalizedVersion,
        UpdateRaftVoterRequestData.KRaftVersionFeature supportedKraftVersions
    ) {
        return supportedKraftVersions.minSupportedVersion() <= finalizedVersion.featureLevel() &&
            supportedKraftVersions.maxSupportedVersion() >= finalizedVersion.featureLevel();
    }

    private Optional<VoterSet> updateVoters(
        VoterSet voters,
        KRaftVersion kraftVersion,
        VoterSet.VoterNode updatedVoter
    ) {
        return kraftVersion.isReconfigSupported() ?
            voters.updateVoter(updatedVoter) :
            voters.updateVoterIgnoringDirectoryId(updatedVoter);
    }

    private CompletableFuture<UpdateRaftVoterResponseData> storeUpdatedVoters(
        LeaderState<?> leaderState,
        ReplicaKey voterKey,
        Optional<KRaftVersionUpgrade.Voters> inMemoryVoters,
        VoterSet newVoters,
        ListenerName requestListenerName,
        long currentTimeMs
    ) {
        if (inMemoryVoters.isEmpty()) {
            // Since the partition support reconfig then just write the update voter set directly to the log
            leaderState.appendVotersRecord(newVoters, currentTimeMs);
        } else {
            // Store the new voters set in the leader state since it cannot be written to the log
            var successful = leaderState.compareAndSetVolatileVoters(
                inMemoryVoters.get(),
                new KRaftVersionUpgrade.Voters(newVoters)
            );
            if (successful) {
                log.info(
                    "Updated in-memory voters from {} to {}",
                    inMemoryVoters.get().voters(),
                    newVoters
                );
            } else {
                log.info(
                    "Unable to update in-memory voters from {} to {}",
                    inMemoryVoters.get().voters(),
                    newVoters
                );
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
        }

        // Reset the check quorum state since the leader received a successful request
        leaderState.updateCheckQuorumForFollowingVoter(voterKey, currentTimeMs);

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
}
