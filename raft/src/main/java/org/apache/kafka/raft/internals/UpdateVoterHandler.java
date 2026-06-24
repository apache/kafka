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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.LeaderState;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.RaftUtil;
import org.apache.kafka.raft.ReplicaKey;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.server.common.KRaftVersion;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
    private final KRaftControlRecordStateMachine partitionState;
    private final RequestSender requestSender;
    private final Time time;
    private final Logger logger;

    public UpdateVoterHandler(
        KRaftControlRecordStateMachine partitionState,
        RequestSender requestSender,
        Time time,
        LogContext logContext
    ) {
        this.partitionState = partitionState;
        this.requestSender = requestSender;
        this.time = time;
        this.logger = logContext.logger(getClass());
    }

    public CompletionStage<UpdateRaftVoterResponseData> handleUpdateVoterRequest(
        LeaderState<?> leaderState,
        ListenerName requestListenerName,
        ReplicaKey voterKey,
        Endpoints voterEndpoints,
        UpdateRaftVoterRequestData.KRaftVersionFeature supportedKraftVersions,
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
                RaftUtil.updateVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    requestListenerName,
                    leaderState.leaderAndEpoch(),
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
                    leaderState.leaderAndEpoch(),
                    leaderState.leaderEndpoints()
                )
            );
        }

        // Check that the supported version range is valid
        if (!validVersionRange(partitionState.lastKraftVersion(), supportedKraftVersions)) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.INVALID_REQUEST,
                    requestListenerName,
                    leaderState.leaderAndEpoch(),
                    leaderState.leaderEndpoints()
                )
            );
        }

        // Check that endpoints includes the default listener
        if (voterEndpoints.address(requestSender.listenerName()).isEmpty()) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.INVALID_REQUEST,
                    requestListenerName,
                    leaderState.leaderAndEpoch(),
                    leaderState.leaderEndpoints()
                )
            );
        }

        // Send API_VERSIONS request to new voter to test new default endpoint
        var timeout = requestSender.send(
            voterEndpoints
                .address(requestSender.listenerName())
                .map(address -> new Node(voterKey.id(), address.getHostName(), address.getPort()))
                .orElseThrow(
                    () -> new IllegalStateException(
                        String.format(
                            "Provided listeners %s do not contain a listener for %s",
                            voterEndpoints,
                            requestSender.listenerName()
                        )
                    )
                ),
            this::buildApiVersionsRequest,
            currentTimeMs
        );
        if (timeout.isEmpty()) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    requestListenerName,
                    leaderState.leaderAndEpoch(),
                    leaderState.leaderEndpoints()
                )
            );
        }

        var state = new UpdateVoterHandlerState(
            voterKey,
            voterEndpoints,
            requestListenerName,
            new SupportedVersionRange(
                supportedKraftVersions.minSupportedVersion(),
                supportedKraftVersions.maxSupportedVersion()
            ),
            time.timer(timeout.getAsLong())
        );
        changeVoterState.resetUpdateVoterHandlerState(
            Errors.UNKNOWN_SERVER_ERROR,
            leaderState.leaderAndEpoch(),
            leaderState.leaderEndpoints(),
            Optional.of(state)
        );

        return state.future();
    }

    /**
     * Handle the API_VERSIONS response for an update voter operation.
     *
     * @param leaderState the leader state
     * @param source the node that sent the response
     * @param error the error from the response
     * @param supportedKraftVersions the supported kraft version range from the response
     * @param currentTimeMs the current time in milliseconds
     * @return true if the update voter operation should continue, false if it was aborted
     */
    public boolean handleApiVersionsResponse(
        LeaderState<?> leaderState,
        Node source,
        Errors error,
        Optional<ApiVersionsResponseData.SupportedFeatureKey> supportedKraftVersions,
        long currentTimeMs
    ) {
        var changeVoterState = leaderState.changeVoterState();
        var handlerState = changeVoterState.updateVoterHandlerState();
        if (handlerState.isEmpty()) {
            // There are no pending add operation just ignore the api response
            return true;
        }

        // Check that the API_VERSIONS response matches the id of the voter getting added
        var current = handlerState.get();
        if (!current.expectingApiResponse(source.id())) {
            logger.info(
                "API_VERSIONS response is not expected from {}: voterKey is {}, lastOffset is {}",
                source,
                current.voterKey(),
                current.lastOffset()
            );

            return true;
        } else if (error != Errors.NONE) {
            // Abort operation if the API_VERSIONS returned an error
            logger.info(
                "Aborting update voter operation for {} at {} since API_VERSIONS returned an error {}",
                current.voterKey(),
                current.voterEndpoints(),
                error
            );

            changeVoterState.resetUpdateVoterHandlerState(
                Errors.REQUEST_TIMED_OUT,
                leaderState.leaderAndEpoch(),
                leaderState.leaderEndpoints(),
                Optional.empty()
            );

            return false;
        } else if (
            !Optional.of(current.supportedKraftVersions())
                .equals(supportedKraftVersions.map(this::convertToVersionRange))
        ) {
            // Check that the supported version from the ApiVersions response matches the supported
            // version from the UpdateVoter requet
            logger.error(
                "The supported kraft version from UpdateVoters {} doesn't match the supported " +
                "kraft version from ApiVersions {}",
                current.supportedKraftVersions(),
                supportedKraftVersions
            );
            changeVoterState.resetUpdateVoterHandlerState(
                Errors.INVALID_REQUEST,
                leaderState.leaderAndEpoch(),
                leaderState.leaderEndpoints(),
                Optional.empty()
            );
            return true;
        }

        // Check that the leader has established a HWM and committed the current epoch
        Optional<Long> highWatermark = leaderState.highWatermark().map(LogOffsetMetadata::offset);
        if (highWatermark.isEmpty()) {
            // This cannot happen because the update voter request handler already validated that
            // the HWMN is known
            throw new IllegalStateException("Expected the high-watermark to be known");
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
            voters = inMemoryVoters.map(KRaftVersionUpgrade.Voters::voters);
        }
        if (voters.isEmpty()) {
            /* This can happen if the remote voter sends an update voter request after the kraft
             * version has been upgraded to 1 but before the updated kraft version has been written
             * to the log.
             *
             * During this time the kraft version and the voter set have been written to the batch
             * accumulator and the leader's volatile voter set has been cleared. These updates have
             * not been written to the log. The KRaft replica's partition state is only updated when
             * the control record has been written to the log (disk).
             */
            logger.info("Unable to read the current voter set with kraft version {}", kraftVersion);
            changeVoterState.resetUpdateVoterHandlerState(
                Errors.REQUEST_TIMED_OUT,
                leaderState.leaderAndEpoch(),
                leaderState.leaderEndpoints(),
                Optional.empty()
            );
            return true;
        }

        // Update the voter
        Optional<VoterSet> updatedVoters = updateVoters(
            voters.get(),
            kraftVersion,
            VoterSet.VoterNode.of(
                current.voterKey(),
                current.voterEndpoints(),
                current.supportedKraftVersions()
            )
        );
        if (updatedVoters.isEmpty()) {
            changeVoterState.resetUpdateVoterHandlerState(
                Errors.VOTER_NOT_FOUND,
                leaderState.leaderAndEpoch(),
                leaderState.leaderEndpoints(),
                Optional.empty()
            );

            return true;
        }

        storeUpdatedVoters(
            leaderState,
            current,
            inMemoryVoters,
            updatedVoters.get(),
            currentTimeMs
        );
        return true;
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

    private void storeUpdatedVoters(
        LeaderState<?> leaderState,
        UpdateVoterHandlerState current,
        Optional<KRaftVersionUpgrade.Voters> inMemoryVoters,
        VoterSet newVoters,
        long currentTimeMs
    ) {
        var changeVoterState = leaderState.changeVoterState();

        if (inMemoryVoters.isEmpty()) {
            /* Since the partition support reconfig then just write the update voter set directly to the log.
             *
             * Complete the RPC but don't reset the handler state. This allows the follower to send a FETCH
             * request and help to commit the voter set change.
             */
            current.setLastOffset(leaderState.appendVotersRecord(newVoters, currentTimeMs));
            current.completeFuture(
                RaftUtil.updateVoterResponse(
                    Errors.NONE,
                    current.requestListenerName(),
                    leaderState.leaderAndEpoch(),
                    leaderState.leaderEndpoints()
                )
            );
        } else {
            // Store the new voters set in the leader state since it cannot be written to the log
            var successful = leaderState.compareAndSetVolatileVoters(
                inMemoryVoters.get(),
                new KRaftVersionUpgrade.Voters(newVoters)
            );
            if (successful) {
                logger.info(
                    "Updated in-memory voters from {} to {}",
                    inMemoryVoters.get().voters(),
                    newVoters
                );

                // Reset the check quorum state since the leader received a successful request
                leaderState.updateCheckQuorumForFollowingVoter(current.voterKey(), currentTimeMs);

                changeVoterState.resetUpdateVoterHandlerState(
                    Errors.NONE,
                    leaderState.leaderAndEpoch(),
                    leaderState.leaderEndpoints(),
                    Optional.empty()
                );
            } else {
                logger.info(
                    "Unable to update in-memory voters from {} to {}",
                    inMemoryVoters.get().voters(),
                    newVoters
                );

                // Fail the pending future if present
                changeVoterState.resetUpdateVoterHandlerState(
                    Errors.REQUEST_TIMED_OUT,
                    leaderState.leaderAndEpoch(),
                    leaderState.leaderEndpoints(),
                    Optional.empty()
                );
            }
        }
    }

    private ApiVersionsRequestData buildApiVersionsRequest() {
        return new ApiVersionsRequest.Builder().build().data();
    }

    private SupportedVersionRange convertToVersionRange(
        ApiVersionsResponseData.SupportedFeatureKey supportedKraftVersions
    ) {
        return new SupportedVersionRange(
            supportedKraftVersions.minVersion(),
            supportedKraftVersions.maxVersion()
        );
    }

    /**
     * Called when the high watermark is updated to check if any pending update voter operations
     * can be completed.
     *
     * @param leaderState the leader state
     * @param highWatermark the new high watermark offset
     */
    public void highWatermarkUpdated(LeaderState<?> leaderState, long highWatermark) {
        var changeVoterState = leaderState.changeVoterState();

        changeVoterState
            .updateVoterHandlerState()
            .ifPresent(current -> {
                current.lastOffset().ifPresent(lastOffset -> {
                    if (highWatermark > lastOffset) {
                        // VotersRecord with the added voter was committed; complete the RPC
                        changeVoterState.resetUpdateVoterHandlerState(
                            Errors.NONE,
                            leaderState.leaderAndEpoch(),
                            leaderState.leaderEndpoints(),
                            Optional.empty()
                        );
                    }
                });
            });
    }
}
