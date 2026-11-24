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
 * This type implements the protocol for updating a voter's endpoints and supported kraft.version
 * in a KRaft partition.
 *
 * Unlike {@link AddVoterHandler} and {@link RemoveVoterHandler}, this operation is not restricted
 * to clusters that support kraft.version 1: it can also update the leader's in-memory voter
 * information for clusters still running kraft.version 0.
 *
 * Handling the UpdateVoter request ({@link #handleUpdateVoterRequest}) does the following:
 *
 * 1. Check that there are no other pending voter change operations (add, remove or update),
 *    otherwise return the REQUEST_TIMED_OUT error.
 * 2. Check that the leader has established a HWM, otherwise return the REQUEST_TIMED_OUT error.
 * 3. Check that the request's supported kraft.version range covers the cluster's currently
 *    finalized kraft.version, otherwise return the INVALID_REQUEST error.
 * 4. Check that the updated endpoints include the default listener, otherwise return the
 *    INVALID_REQUEST error.
 * 5. Send an API_VERSIONS request to the voter's new default endpoint and record the operation as
 *    pending, returning the REQUEST_TIMED_OUT error if the request cannot be sent.
 *
 * The response is not sent synchronously; the RPC only completes once the API_VERSIONS response is
 * handled by {@link #handleApiVersionsResponse}:
 *
 * 6. Ignore the response if it doesn't come from the voter this operation is waiting on.
 * 7. Abort with the REQUEST_TIMED_OUT error if the API_VERSIONS request itself failed.
 * 8. Abort with the INVALID_REQUEST error if the supported kraft.version range in the response
 *    doesn't match the range from the UpdateVoter request.
 * 9. Read the current voter set: from the last committed VotersRecord in the log if the cluster
 *    supports kraft.version 1, or from the leader's in-memory voter set otherwise (this in-memory
 *    state is used to carry voter updates across a future kraft.version upgrade). Abort with the
 *    REQUEST_TIMED_OUT error if neither is available yet, e.g. because the kraft.version was just
 *    upgraded but the corresponding record has not been written to the log.
 * 10. Update the matching voter's entry with the new endpoints and supported kraft.version, aborting
 *     with the VOTER_NOT_FOUND error if the voter is no longer part of the set.
 * 11. Store the updated voter set:
 *     a. If the cluster supports kraft.version 1, append the updated VotersRecord to the log and
 *        immediately complete the RPC with success, without waiting for the record to commit. The
 *        pending operation is only cleared once the HWM advances past the appended record (see
 *        {@link #highWatermarkUpdated}), which allows the next voter change operation to proceed.
 *     b. Otherwise, compare-and-set the leader's in-memory voter set, completing the RPC
 *        immediately with success, or with the REQUEST_TIMED_OUT error if the in-memory state was
 *        concurrently changed by another operation.
 *     In either successful case, this also resets the leader's check quorum tracking for the
 *     voter (see {@link LeaderState#updateCheckQuorumForFollowingVoter}), since a successful
 *     UpdateVoter request is evidence that the voter is following the leader.
 *
 * A pending operation that doesn't complete before its timeout expires is also aborted with the
 * REQUEST_TIMED_OUT error, by {@link ChangeVoterHandlerState#maybeExpirePendingOperation}.
 */
public final class UpdateVoterHandler {
    private final KRaftControlRecordStateMachine partitionState;
    private final RequestSender requestSender;
    private final Time time;
    private final Logger logger;

    /**
     * Creates a new handler for update voter requests.
     *
     * @param partitionState the KRaft partition state, used to read the currently finalized
     *        kraft.version and the log's voter set
     * @param requestSender used to send the API_VERSIONS request to the voter being updated
     * @param time the time implementation, used to create the timer that bounds a pending operation
     * @param logContext used to create this class's logger
     */
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

    /**
     * Handle an UpdateVoter request.
     * <p>
     * See the class documentation for the full set of steps that this method and
     * {@link #handleApiVersionsResponse} perform together.
     *
     * @param leaderState the leader state
     * @param requestListenerName the listener the request was received on, used to build the response
     * @param voterKey the id and directory id of the voter to update
     * @param voterEndpoints the updated endpoints for the voter
     * @param supportedKraftVersions the kraft.version range supported by the voter
     * @param currentTimeMs the current time in milliseconds
     * @return a future for the UpdateVoter response; it completes immediately if the request is
     *         rejected outright, or later, once the API_VERSIONS round trip finishes, via
     *         {@link #handleApiVersionsResponse}
     */
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
        if (changeVoterState.isOperationPending(
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
        if (leaderState.highWatermark().isEmpty()) {
            return CompletableFuture.completedFuture(
                RaftUtil.updateVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
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
     * Handle the API_VERSIONS response for a pending update voter operation.
     * <p>
     * This may abort the pending operation, completing its future with an error, if the response
     * doesn't come from the expected voter, if the API_VERSIONS request failed, if the supported
     * kraft.version range doesn't match the one from the UpdateVoter request, if the current voter
     * set can't be read yet, or if the voter is no longer part of the voter set. Otherwise, it
     * stores the updated voter set, see {@link #storeUpdatedVoters}.
     *
     * @param leaderState the leader state
     * @param source the node that sent the response
     * @param error the error from the response
     * @param supportedKraftVersions the supported kraft version range from the response
     * @param currentTimeMs the current time in milliseconds
     * @return false only when the API_VERSIONS request itself failed, which is the only case where
     *         the caller (see {@code KafkaRaftClient#handleResponse}) should treat this as an
     *         unsuccessful response for request-tracking purposes; true otherwise, including when
     *         this method aborts the pending update voter operation for another reason
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
            // There is no pending update operation; just ignore the API_VERSIONS response
            return true;
        }

        // Check that the API_VERSIONS response matches the id of the voter getting updated
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
            // version from the UpdateVoter request
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

        completeUpdateVoter(leaderState, changeVoterState, current, currentTimeMs);
        return true;
    }

    /**
     * Validates the kraft.version range and current voter set, then applies the update, once the
     * API_VERSIONS response has already been matched to a pending update voter operation.
     */
    private void completeUpdateVoter(
        LeaderState<?> leaderState,
        ChangeVoterHandlerState changeVoterState,
        UpdateVoterHandlerState current,
        long currentTimeMs
    ) {
        var kraftVersion = partitionState.lastKraftVersion();
        if (!validVersionRange(kraftVersion, current.supportedKraftVersions())) {
            // Check that the supported version range is valid
            logger.info(
                "Aborting update voter operation for {} at {} since kraft.version range {} doesn't match {}",
                current.voterKey(),
                current.voterEndpoints(),
                current.supportedKraftVersions(),
                kraftVersion
            );

            changeVoterState.resetUpdateVoterHandlerState(
                Errors.INVALID_REQUEST,
                leaderState.leaderAndEpoch(),
                leaderState.leaderEndpoints(),
                Optional.empty()
            );
            return;
        }

        // Check that the leader has established a HWM and committed the current epoch
        Optional<Long> highWatermark = leaderState.highWatermark().map(LogOffsetMetadata::offset);
        if (highWatermark.isEmpty()) {
            // This cannot happen because the update voter request handler already validated that
            // the HWM is known
            throw new IllegalStateException("Expected the high-watermark to be known");
        }

        // Read the voter set from the log if the cluster supports kraft.version 1, otherwise from
        // the leader's in-memory voter set
        final Optional<KRaftVersionUpgrade.Voters> inMemoryVoters;
        final Optional<VoterSet> voters;
        if (kraftVersion.isReconfigSupported()) {
            inMemoryVoters = Optional.empty();

            // Only use the last voter set entry if it has already committed; an uncommitted
            // entry is treated as not yet available
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
            /* This can happen for two reasons:
             *
             * 1. The cluster just upgraded to a kraft.version that supports reconfiguration, but
             *    the updated kraft.version and voter set have only been written to the batch
             *    accumulator, not yet the log. During this time the leader's volatile voter set
             *    has already been cleared, but the log doesn't have a committed VotersRecord yet.
             *    The KRaft replica's partition state is only updated once the control record has
             *    been written to the log (disk).
             * 2. The cluster already supports kraft.version 1, but the last VotersRecord in the
             *    log has not committed yet.
             *
             * In both cases the leader doesn't have a definitive voter set to update yet, so ask
             * the voter to retry.
             */
            logger.info("Unable to read the current voter set with kraft version {}", kraftVersion);
            changeVoterState.resetUpdateVoterHandlerState(
                Errors.REQUEST_TIMED_OUT,
                leaderState.leaderAndEpoch(),
                leaderState.leaderEndpoints(),
                Optional.empty()
            );
            return;
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

            return;
        }

        storeUpdatedVoters(
            leaderState,
            current,
            inMemoryVoters,
            updatedVoters.get(),
            currentTimeMs
        );
        return;
    }

    private boolean validVersionRange(
        KRaftVersion finalizedVersion,
        SupportedVersionRange supportedKraftVersions
    ) {
        return supportedKraftVersions.min() <= finalizedVersion.featureLevel() &&
            supportedKraftVersions.max() >= finalizedVersion.featureLevel();
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
            /* Since the partition supports reconfig, write the updated voter set directly to the log.
             *
             * Complete the RPC but don't reset the handler state. This allows the follower to send a FETCH
             * request and help to commit the voter set change.
             */
            current.setLastOffset(leaderState.appendVotersRecord(newVoters, currentTimeMs));

            // Reset the check quorum state since the leader received a successful request
            leaderState.updateCheckQuorumForFollowingVoter(current.voterKey(), currentTimeMs);

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

                // Fail the pending future so the client can retry
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
     * Called when the high watermark advances to check if a pending update voter operation can be
     * cleared.
     * <p>
     * When the cluster supports kraft.version 1, {@link #storeUpdatedVoters} completes the
     * UpdateVoter RPC as soon as the updated VotersRecord is appended to the log, without waiting
     * for it to commit, but leaves the operation marked as pending so that no other voter change
     * operation can start. This method clears that pending state, allowing the next voter change
     * request to be accepted, once the high watermark advances past the offset of that record.
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
                        // The VotersRecord with the updated voter was committed; clear the
                        // pending operation to allow other voter changes. The RPC response was
                        // already sent when the record was appended, so this does not complete
                        // the future again.
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
