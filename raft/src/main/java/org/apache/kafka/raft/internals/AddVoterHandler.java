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
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
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
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * This type implements the protocol for adding a voter to a KRaft partition.
 *
 * The general algorithm for adding a voter to the voter set is:
 *
 * 1. Check that the leader has fenced the previous leader(s) by checking that the HWM is known,
 *    otherwise return the REQUEST_TIMED_OUT error.
 * 2. Check that the cluster supports kraft.version 1, otherwise return the UNSUPPORTED_VERSION error.
 * 3. Check that there are no uncommitted voter changes, otherwise return the REQUEST_TIMED_OUT error.
 * 4. Check that the new voter's id is not part of the existing voter set, otherwise return the
 *    DUPLICATE_VOTER error.
 * 5. Send an API_VERSIONS RPC to the first (default) listener to discover the supported
 *    kraft.version of the new voter, and record the operation as pending, returning the
 *    REQUEST_TIMED_OUT error if the request cannot be sent.
 *
 * The response is not sent synchronously; the rest of the algorithm runs once the API_VERSIONS
 * response is handled by {@link #handleApiVersionsResponse}:
 *
 * 6. Check that the new voter supports the current kraft.version, otherwise return the
 *    INVALID_REQUEST error.
 * 7. Check that the new voter is caught up to the log end offset of the leader, otherwise return
 *    a REQUEST_TIMED_OUT error.
 * 8. Append the updated VotersRecord to the log. The KRaft internal listener will read this
 *    uncommitted record from the log and add the new voter to the set of voters.
 * 9. If the request did not ask to wait for the change to commit ({@code ackWhenCommitted} is
 *    false), send the AddVoter successful response immediately, without waiting for the record
 *    to commit.
 * 10. Otherwise, wait for the VotersRecord to commit using the majority of the new set of
 *     voters, then send the AddVoter successful response; return a REQUEST_TIMED_OUT error if it
 *     doesn't commit in time. Either way, the pending operation is only cleared, via
 *     {@link #highWatermarkUpdated}, once the HWM advances past the offset of the appended
 *     record, which allows the next voter change operation to proceed.
 *
 * A pending operation that doesn't complete before its timeout expires is also aborted with the
 * REQUEST_TIMED_OUT error, by {@link ChangeVoterHandlerState#maybeExpirePendingOperation}.
 *
 * The algorithm above could be improved as part of KAFKA-17147. Instead of returning an error
 * immediately for 1., 2. and 7., KRaft can wait with a timeout until those invariants are true.
 */
public final class AddVoterHandler {
    private final KRaftControlRecordStateMachine partitionState;
    private final RequestSender requestSender;
    private final Time time;
    private final Logger logger;

    /**
     * Creates a new handler for add voter requests.
     *
     * @param partitionState the KRaft partition state, used to read the currently finalized
     *        kraft.version and the log's voter set
     * @param requestSender used to send the API_VERSIONS request to the voter being added
     * @param time the time implementation, used to create the timer that bounds a pending operation
     * @param logContext used to create this class's logger
     */
    public AddVoterHandler(
        KRaftControlRecordStateMachine partitionState,
        RequestSender requestSender,
        Time time,
        LogContext logContext
    ) {
        this.partitionState = partitionState;
        this.requestSender = requestSender;
        this.time = time;
        this.logger = logContext.logger(AddVoterHandler.class);
    }

    /**
     * Handle an AddVoter request.
     * <p>
     * See the class documentation for the full set of steps that this method and
     * {@link #handleApiVersionsResponse} perform together.
     *
     * @param leaderState the leader state
     * @param voterKey the id and directory id of the voter to add
     * @param voterEndpoints the endpoints of the voter to add
     * @param ackWhenCommitted if true, the response is withheld until the voter change commits;
     *        if false, the response is sent as soon as the change is appended to the log
     * @param currentTimeMs the current time in milliseconds
     * @return a future for the AddVoter response; it completes immediately if the request is
     *         rejected outright, or later, once the API_VERSIONS round trip (and, depending on
     *         {@code ackWhenCommitted}, the commit) finishes
     */
    public CompletionStage<AddRaftVoterResponseData> handleAddVoterRequest(
        LeaderState<?> leaderState,
        ReplicaKey voterKey,
        Endpoints voterEndpoints,
        boolean ackWhenCommitted,
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
                RaftUtil.addVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    "Request timed out waiting for leader to handle previous voter change request"
                )
            );
        }

        // Check that the leader has established a HWM and committed the current epoch
        Optional<Long> highWatermark = leaderState.highWatermark().map(LogOffsetMetadata::offset);
        if (highWatermark.isEmpty()) {
            return CompletableFuture.completedFuture(
                RaftUtil.addVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    "Request timed out waiting for leader to establish HWM and fence previous voter changes"
                )
            );
        }

        // Check that the cluster supports kraft.version >= 1
        KRaftVersion kraftVersion = partitionState.lastKraftVersion();
        if (!kraftVersion.isReconfigSupported()) {
            return CompletableFuture.completedFuture(
                RaftUtil.addVoterResponse(
                    Errors.UNSUPPORTED_VERSION,
                    String.format(
                        "Cluster doesn't support adding voter because the %s feature is %s",
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
                RaftUtil.addVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    String.format(
                        "Request timed out waiting for voters to commit the latest voter change at %s with HWM %d",
                        votersEntry.map(LogHistory.Entry::offset),
                        highWatermark.get()
                    )
                )
            );
        }

        // Check that the new voter id is not part of the current voter set
        VoterSet voters = votersEntry.get().value();
        if (voters.voterIds().contains(voterKey.id())) {
            return CompletableFuture.completedFuture(
                RaftUtil.addVoterResponse(
                    Errors.DUPLICATE_VOTER,
                    String.format(
                        "The voter id for %s is already part of the set of voters %s.",
                        voterKey,
                        voters.voterKeys()
                    )
                )
            );
        }

        // Send API_VERSIONS request to new voter to discover their supported kraft.version range
        OptionalLong timeout = requestSender.send(
            voterEndpoints
                .address(requestSender.listenerName())
                .map(address -> new Node(voterKey.id(), address.getHostName(), address.getPort()))
                .orElseThrow(
                    () -> new IllegalArgumentException(
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
                RaftUtil.addVoterResponse(
                    Errors.REQUEST_TIMED_OUT,
                    String.format("New voter %s is not ready to receive requests", voterKey)
                )
            );
        }

        AddVoterHandlerState state = new AddVoterHandlerState(
            voterKey,
            voterEndpoints,
            ackWhenCommitted,
            time.timer(timeout.getAsLong())
        );
        changeVoterState.resetAddVoterHandlerState(
            Errors.UNKNOWN_SERVER_ERROR,
            null,
            Optional.of(state)
        );

        return state.future();
    }

    /**
     * Handle the API_VERSIONS response for a pending add voter operation.
     * <p>
     * This may abort the pending operation, completing its future with an error, if the response
     * doesn't come from the expected voter, if the API_VERSIONS request failed, if the new
     * voter's supported kraft.version range doesn't cover the cluster's finalized kraft.version,
     * or if the voter isn't caught up to the leader's log end offset. Otherwise, it appends the
     * updated VotersRecord to the log and, if {@code ackWhenCommitted} is false, completes the
     * response immediately.
     *
     * @param leaderState the leader state
     * @param source the node that sent the response
     * @param error the error from the response
     * @param supportedKraftVersions the supported kraft version range from the response
     * @param currentTimeMs the current time in milliseconds
     * @return false only when the API_VERSIONS request itself failed, which is the only case
     *         where the caller (see {@code KafkaRaftClient#handleResponse}) should treat this as
     *         an unsuccessful response for request-tracking purposes; true otherwise, including
     *         when this method aborts the pending add voter operation for another reason
     */
    public boolean handleApiVersionsResponse(
        LeaderState<?> leaderState,
        Node source,
        Errors error,
        Optional<ApiVersionsResponseData.SupportedFeatureKey> supportedKraftVersions,
        long currentTimeMs
    ) {
        var changeVoterState = leaderState.changeVoterState();
        var handlerState = changeVoterState.addVoterHandlerState();
        if (handlerState.isEmpty()) {
            // There is no pending add operation; just ignore the API_VERSIONS response
            return true;
        }

        // Check that the API_VERSIONS response matches the id of the voter getting added
        AddVoterHandlerState current = handlerState.get();
        if (!current.expectingApiResponse(source.id())) {
            logger.info(
                "API_VERSIONS response is not expected from {}: voterKey is {}, lastOffset is {}",
                source,
                current.voterKey(),
                current.lastOffset()
            );

            return true;
        }

        // Abort operation if the API_VERSIONS returned an error
        if (error != Errors.NONE) {
            logger.info(
                "Aborting add voter operation for {} at {} since API_VERSIONS returned an error {}",
                current.voterKey(),
                current.voterEndpoints(),
                error
            );

            changeVoterState.resetAddVoterHandlerState(
                Errors.REQUEST_TIMED_OUT,
                String.format(
                    "Aborted add voter operation for %s since API_VERSIONS returned an error %s",
                    current.voterKey(),
                    error
                ),
                Optional.empty()
            );

            return false;
        }

        // Check that the new voter supports the kraft.version for reconfiguration
        KRaftVersion kraftVersion = partitionState.lastKraftVersion();
        if (!validVersionRange(kraftVersion, supportedKraftVersions)) {
            logger.info(
                "Aborting add voter operation for {} at {} since kraft.version range {} doesn't " +
                "support reconfiguration",
                current.voterKey(),
                current.voterEndpoints(),
                supportedKraftVersions
            );

            changeVoterState.resetAddVoterHandlerState(
                Errors.INVALID_REQUEST,
                String.format(
                    "Aborted add voter operation for %s since the %s range %s doesn't " +
                    "support the finalized version %s",
                    current.voterKey(),
                    KRaftVersion.FEATURE_NAME,
                    supportedKraftVersions
                        .map(
                            range -> String.format(
                                "(min: %s, max: %s",
                                range.minVersion(),
                                range.maxVersion()
                            )
                        )
                        .orElse("(min: 0, max: 0)"),
                    kraftVersion.featureLevel()
                ),
                Optional.empty()
            );

            return true;
        }

        // Check that the new voter is caught up to the LEO to avoid delays in HWM increases
        if (!leaderState.isReplicaCaughtUp(current.voterKey(), currentTimeMs)) {
            logger.info(
                "Aborting add voter operation for {} at {} since it is lagging behind: {}",
                current.voterKey(),
                current.voterEndpoints(),
                leaderState.getReplicaState(current.voterKey())
            );

            changeVoterState.resetAddVoterHandlerState(
                Errors.REQUEST_TIMED_OUT,
                String.format(
                    "Aborted add voter operation for %s since it is lagging behind",
                    current.voterKey()
                ),
                Optional.empty()
            );

            return true;
        }

        // Add the new voter to the set of voters and append the record to the log
        VoterSet newVoters = partitionState
            .lastVoterSet()
            .addVoter(
                VoterSet.VoterNode.of(
                    current.voterKey(),
                    current.voterEndpoints(),
                    new SupportedVersionRange(
                        supportedKraftVersions.get().minVersion(),
                        supportedKraftVersions.get().maxVersion()
                    )
                )
            )
            .orElseThrow(() ->
                new IllegalStateException(
                    String.format(
                        "Unable to add %s to the set of voters %s",
                        current.voterKey(),
                        partitionState.lastVoterSet()
                    )
                )
            );
        current.setLastOffset(leaderState.appendVotersRecord(newVoters, currentTimeMs));
        if (!current.ackWhenCommitted()) {
            // complete the future to send response, but do not reset the state,
            // since the new voter set is not yet committed
            current.completeFuture(RaftUtil.addVoterResponse(Errors.NONE, null));
        }
        return true;
    }

    /**
     * Called when the high watermark advances to check if a pending add voter operation can be
     * cleared.
     * <p>
     * If the AddVoter request asked to wait for the commit ({@code ackWhenCommitted} is true),
     * this is also when the response is completed with success. Otherwise, the response was
     * already sent by {@link #handleApiVersionsResponse} as soon as the VotersRecord was
     * appended, and this method only clears the pending operation, allowing the next voter
     * change request to be accepted, once the high watermark advances past the offset of that
     * record.
     *
     * @param leaderState the leader state
     * @param highWatermark the new high watermark offset
     */
    public void highWatermarkUpdated(LeaderState<?> leaderState, long highWatermark) {
        var changeVoterState = leaderState.changeVoterState();

        changeVoterState
            .addVoterHandlerState()
            .ifPresent(current ->
                current.lastOffset().ifPresent(lastOffset -> {
                    if (highWatermark > lastOffset) {
                        // The VotersRecord with the added voter was committed. If the request
                        // asked to wait for the commit, this completes the RPC with success;
                        // otherwise the response was already sent and this just clears the
                        // pending operation.
                        changeVoterState
                            .resetAddVoterHandlerState(Errors.NONE, null, Optional.empty());
                    }
                })
            );
    }

    private ApiVersionsRequestData buildApiVersionsRequest() {
        return new ApiVersionsRequest.Builder().build().data();
    }

    private boolean validVersionRange(
        KRaftVersion finalizedVersion,
        Optional<ApiVersionsResponseData.SupportedFeatureKey> supportedKraftVersions
    ) {
        return supportedKraftVersions.isPresent() &&
            (supportedKraftVersions.get().minVersion() <= finalizedVersion.featureLevel() &&
             supportedKraftVersions.get().maxVersion() >= finalizedVersion.featureLevel());
    }
}
