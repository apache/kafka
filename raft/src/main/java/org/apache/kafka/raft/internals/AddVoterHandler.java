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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.queue.KafkaDeadlineEventQueue;
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
 *    kraft.version of the new voter.
 * 6. Check that the new voter supports the current kraft.version, otherwise return the
 *    INVALID_REQUEST error.
 * 7. Check that the new voter is caught up to the log end offset of the leader, otherwise return
 *    a REQUEST_TIMED_OUT error.
 * 8. Append the updated VotersRecord to the log. The KRaft internal listener will read this
 *    uncommitted record from the log and add the new voter to the set of voters.
 * 9. Wait for the VotersRecord to commit using the majority of the new set of voters. Return a
 *    REQUEST_TIMED_OUT error if it doesn't commit in time.
 * 10. Send the AddVoter successful response to the client.
 *
 * The algorithm above could be improved as part of KAFKA-17147. Instead of returning an error
 * immediately for 1., 2. and 7., KRaft can wait with a timeout until those invariants are true.
 */
public final class AddVoterHandler {
    private final KRaftControlRecordStateMachine partitionState;
    private final RequestSender requestSender;
    private final Time time;
    private final Logger logger;
    private final long requestTimeoutConfig;
    private final KafkaDeadlineEventQueue<DeadlineTaskManager.DeferredTask> eventQueue;
    private final DeadlineTaskManager deadlineTaskManager;

    public AddVoterHandler(
        KRaftControlRecordStateMachine partitionState,
        RequestSender requestSender,
        Time time,
        LogContext logContext,
        long requestTimeoutConfig,
        KafkaDeadlineEventQueue<DeadlineTaskManager.DeferredTask> eventQueue
    ) {
        this.partitionState = partitionState;
        this.requestSender = requestSender;
        this.time = time;
        this.logger = logContext.logger(AddVoterHandler.class);
        this.requestTimeoutConfig = requestTimeoutConfig;
        this.eventQueue = eventQueue;
        this.deadlineTaskManager = new DeadlineTaskManager(time, this.eventQueue);
    }

    public CompletableFuture<AddRaftVoterResponseData> handleAddVoterRequest(
        LeaderState<?> leaderState,
        ReplicaKey voterKey,
        Endpoints voterEndpoints,
        boolean ackWhenCommitted,
        long currentTimeMs,
        long requestTimeoutMs
    ) {
        // FIXME: think how to do
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

        Optional<LogHistory.Entry<VoterSet>> votersEntry = partitionState.lastVoterSetEntry();
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

        Timer timer = time.timer(Math.max(0, Math.min(requestTimeoutMs, requestTimeoutConfig)));
        long requestDeadlineMs = timer.deadlineMs();
        // There are three cases we can't delay the request
        // 1. There is already same tim to wait
        // 2. already timeout
        // 3. need to ack but there is uncommitted voter
        // FIXME: split error msg
        if (requestDeadlineMs <= time.milliseconds() || (!ackWhenCommitted && hasUnCommitedVoter(leaderState))) {
            return CompletableFuture.completedFuture(
                    RaftUtil.addVoterResponse(
                            Errors.REQUEST_TIMED_OUT,
                            "Request timeout"
                    )
            );
        }

        AddVoterHandlerState state = new AddVoterHandlerState(
                voterKey,
                voterEndpoints,
                ackWhenCommitted,
                timer // the time need to finish apiRequest and voterResponse
        );

        deadlineTaskManager.addTask("deffer addVoterRequest",
                new DeadlineTaskManager.DeferredTask(timer.deadlineMs(), () -> {
                    leaderState.resetAddVoterHandlerState(Errors.UNKNOWN_SERVER_ERROR, "", Optional.of(state));
                    // Send API_VERSIONS request to new voter to discover their supported kraft.version range
                    long sendTime = time.milliseconds();
                    logger.debug("Send ApiVersionRequest from deffered event");
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
                            sendTime
                    );
                    if (timeout.isEmpty()) {
                        state.future().complete(RaftUtil.addVoterResponse(Errors.REQUEST_TIMED_OUT,
                                String.format("New voter %s is not ready to receive requests", voterKey)));
                    }
                }, () -> state.future().complete(RaftUtil.addVoterResponse(Errors.REQUEST_TIMED_OUT, "Request timeout kkkk"))
                ),
                timer.remainingMs() // apiVersionResponse timeout
        );
        deadlineTaskManager.poll(currentTimeMs);

        return state.future();
    }

    public boolean handleApiVersionsResponse(
        LeaderState<?> leaderState,
        Node source,
        Errors error,
        Optional<ApiVersionsResponseData.SupportedFeatureKey> supportedKraftVersions,
        long currentTimeMs
    ) {
        logger.debug("handleApiVersionsResponse from AddVoterHandle");
        Optional<AddVoterHandlerState> handlerState = leaderState.addVoterHandlerState();
        if (handlerState.isEmpty()) {
            // There are no pending add operation just ignore the api response
            return true;
        }
        logger.debug("ZZZZZ");
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

            leaderState.resetAddVoterHandlerState(
                Errors.REQUEST_TIMED_OUT,
                String.format(
                    "Aborted add voter operation for since API_VERSIONS returned an error %s",
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

            leaderState.resetAddVoterHandlerState(
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

            leaderState.resetAddVoterHandlerState(
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
            if (hasUnCommitedVoter(leaderState)) {
                // If the state need to be acked immediately, it is disallowed when there is any uncommitted voter on leader
                current.future().complete(RaftUtil.addVoterResponse(Errors.REQUEST_TIMED_OUT,
                        "Request timed out waiting for leader to handle previous voter change request"));
            } else {
                // complete the future to send response, but do not reset the state,
                // since the new voter set is not yet committed
                current.future().complete(RaftUtil.addVoterResponse(Errors.NONE, null));
            }
        }
        return true;
    }

    public void highWatermarkUpdated(LeaderState<?> leaderState) {
        leaderState.addVoterHandlerState().ifPresent(current ->
            leaderState.highWatermark().ifPresent(highWatermark ->
                current.lastOffset().ifPresent(lastOffset -> {
                    if (highWatermark.offset() > lastOffset) {
                        // VotersRecord with the added voter was committed; complete the RPC
                        leaderState.resetAddVoterHandlerState(Errors.NONE, null, Optional.empty());
                        deadlineTaskManager.poll(time.milliseconds());
                    }
                })
            )
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

    private boolean hasUnCommitedVoter(LeaderState<?> leaderState) {
        Optional<LogHistory.Entry<VoterSet>> lastVoterSet = partitionState.lastVoterSetEntry();
        Optional<Long> highWatermark = leaderState.highWatermark().map(LogOffsetMetadata::offset);
        if (highWatermark.isEmpty() || lastVoterSet.isEmpty()) {
            return true;
        }

        return lastVoterSet.get().offset() >= highWatermark.get();
    }
}
