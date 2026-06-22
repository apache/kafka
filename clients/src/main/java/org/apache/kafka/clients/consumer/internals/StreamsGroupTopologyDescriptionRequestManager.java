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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupTopologyDescriptionUpdateRequest;
import org.apache.kafka.common.requests.StreamsGroupTopologyDescriptionUpdateResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.Objects;

public class StreamsGroupTopologyDescriptionRequestManager implements RequestManager {

    private final Logger logger;
    private final Time time;
    private final String groupId;
    private final StreamsRebalanceData streamsRebalanceData;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final RequestState pushRequestState;

    private long nextPushTimeMs = 0L;

    public StreamsGroupTopologyDescriptionRequestManager(final LogContext logContext,
                                                         final Time time,
                                                         final long retryBackoffMs,
                                                         final long retryBackoffMaxMs,
                                                         final String groupId,
                                                         final StreamsRebalanceData streamsRebalanceData,
                                                         final CoordinatorRequestManager coordinatorRequestManager) {
        this.logger = logContext.logger(getClass());
        this.time = Objects.requireNonNull(time);
        this.groupId = Objects.requireNonNull(groupId);
        this.streamsRebalanceData = Objects.requireNonNull(streamsRebalanceData);
        this.coordinatorRequestManager = Objects.requireNonNull(coordinatorRequestManager);
        this.pushRequestState = new RequestState(
            logContext,
            StreamsGroupTopologyDescriptionRequestManager.class.getSimpleName(),
            retryBackoffMs,
            retryBackoffMaxMs);
    }

    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        if (!shouldSendTopologyDescriptionUpdate(currentTimeMs)) {
            return NetworkClientDelegate.PollResult.EMPTY;
        }

        final StreamsGroupTopologyDescriptionUpdateRequestData data = new StreamsGroupTopologyDescriptionUpdateRequestData()
            .setGroupId(groupId)
            .setMemberId(streamsRebalanceData.memberId())
            .setTopologyEpoch(streamsRebalanceData.topologyEpoch())
            .setTopologyDescription(streamsRebalanceData.wireTopologyDescription());

        final NetworkClientDelegate.UnsentRequest unsent = new NetworkClientDelegate.UnsentRequest(
            new StreamsGroupTopologyDescriptionUpdateRequest.Builder(data),
            coordinatorRequestManager.coordinator()
        );
        unsent.whenComplete((response, exception) -> onResponse(response, exception));

        pushRequestState.onSendAttempt(currentTimeMs);
        return new NetworkClientDelegate.PollResult(Collections.singletonList(unsent));
    }

    @Override
    public long maximumTimeToWait(final long currentTimeMs) {
        if (!streamsRebalanceData.topologyPushRequired()) {
            return Long.MAX_VALUE;
        }
        final long backoffRemainingMs = pushRequestState.remainingBackoffMs(currentTimeMs);
        final long throttleRemainingMs = Math.max(0L, nextPushTimeMs - currentTimeMs);
        final long waitMs = Math.max(backoffRemainingMs, throttleRemainingMs);
        if (waitMs > 0L) {
            return waitMs;
        }
        return shouldSendTopologyDescriptionUpdate(currentTimeMs) ? 0L : Long.MAX_VALUE;
    }

    private boolean shouldSendTopologyDescriptionUpdate(final long currentTimeMs) {
        if (!pushRequestState.canSendRequest(currentTimeMs) || currentTimeMs < nextPushTimeMs) {
            return false;
        }
        if (!streamsRebalanceData.topologyPushRequired() || streamsRebalanceData.wireTopologyDescription() == null) {
            return false;
        }
        final String memberId = streamsRebalanceData.memberId();
        if (memberId == null || memberId.isEmpty()) {
            return false;
        }
        return coordinatorRequestManager.coordinator().isPresent();
    }

    private void onResponse(final ClientResponse response, final Throwable exception) {
        final long responseTimeMs = time.milliseconds();

        if (exception != null) {
            pushRequestState.onFailedAttempt(responseTimeMs);
            logger.warn("Topology description push failed with exception; will retry on next poll", exception);
            return;
        }

        final StreamsGroupTopologyDescriptionUpdateResponse body =
            (StreamsGroupTopologyDescriptionUpdateResponse) response.responseBody();
        final Errors error = Errors.forCode(body.data().errorCode());
        final String errorMessage = body.data().errorMessage();

        if (body.data().throttleTimeMs() > 0) {
            nextPushTimeMs = responseTimeMs + body.data().throttleTimeMs();
        }

        switch (error) {
            case NONE:
                pushRequestState.onSuccessfulAttempt(responseTimeMs);
                streamsRebalanceData.setTopologyPushRequired(false);
                break;

            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
                pushRequestState.onFailedAttempt(responseTimeMs);
                logInfo(
                    String.format("Coordinator error %s pushing topology description. Will rediscover and retry", error),
                    errorMessage
                );
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, responseTimeMs);
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                pushRequestState.onFailedAttempt(responseTimeMs);
                logInfo("Coordinator is loading; will retry on next poll", errorMessage);
                break;

            case UNKNOWN_MEMBER_ID:
                pushRequestState.onSuccessfulAttempt(responseTimeMs);
                logInfo(
                    "Topology description push rejected with UNKNOWN_MEMBER_ID; heartbeat will trigger rejoin",
                    errorMessage
                );
                streamsRebalanceData.setTopologyPushRequired(false);
                break;

            case STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED:
            case INVALID_REQUEST:
            case UNSUPPORTED_VERSION:
            case GROUP_ID_NOT_FOUND:
            case GROUP_AUTHORIZATION_FAILED:
            default:
                pushRequestState.onSuccessfulAttempt(responseTimeMs);
                logger.warn("Topology description push failed with {}: {}", error, errorMessage);
                streamsRebalanceData.setTopologyPushRequired(false);
                break;
        }
    }

    private void logInfo(final String message, final String errorMessage) {
        logger.info("{}: {}", message, errorMessage);
    }
}
