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
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.Objects;

public class StreamsGroupTopologyDescriptionRequestManager implements RequestManager {

    private final Logger logger;
    private final Time time;
    private final String groupId;
    private final StreamsRebalanceData streamsRebalanceData;
    private final CoordinatorRequestManager coordinatorRequestManager;

    private boolean inflight = false;
    private long nextPushTimeMs = 0L;

    public StreamsGroupTopologyDescriptionRequestManager(final LogContext logContext,
                                                         final Time time,
                                                         final String groupId,
                                                         final StreamsRebalanceData streamsRebalanceData,
                                                         final CoordinatorRequestManager coordinatorRequestManager) {
        this.logger = logContext.logger(getClass());
        this.time = Objects.requireNonNull(time);
        this.groupId = Objects.requireNonNull(groupId);
        this.streamsRebalanceData = Objects.requireNonNull(streamsRebalanceData);
        this.coordinatorRequestManager = Objects.requireNonNull(coordinatorRequestManager);
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
        unsent.whenComplete((response, exception) -> onResponse(response, exception, currentTimeMs));

        inflight = true;
        return new NetworkClientDelegate.PollResult(Collections.singletonList(unsent));
    }

    @Override
    public long maximumTimeToWait(final long currentTimeMs) {
        if (!streamsRebalanceData.topologyPushRequired()) {
            return Long.MAX_VALUE;
        }
        if (currentTimeMs < nextPushTimeMs) {
            return nextPushTimeMs - currentTimeMs;
        }
        return shouldSendTopologyDescriptionUpdate(currentTimeMs) ? 0L : Long.MAX_VALUE;
    }

    private boolean shouldSendTopologyDescriptionUpdate(final long currentTimeMs) {
        if (inflight || currentTimeMs < nextPushTimeMs) {
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

    private void onResponse(final ClientResponse response, final Throwable exception, final long requestTimeMs) {
        inflight = false;

        if (exception != null) {
            logger.warn("Topology description push failed with exception; will retry on next poll", exception);
            return;
        }

        final StreamsGroupTopologyDescriptionUpdateResponse body =
            (StreamsGroupTopologyDescriptionUpdateResponse) response.responseBody();
        final Errors error = Errors.forCode(body.data().errorCode());

        if (body.data().throttleTimeMs() > 0) {
            nextPushTimeMs = requestTimeMs + body.data().throttleTimeMs();
        }

        switch (error) {
            case NONE:
                streamsRebalanceData.setTopologyPushRequired(false);
                break;

            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
                logInfo(
                    String.format("Coordinator error %s pushing topology description. Will rediscover and retry", error),
                    body
                );
                coordinatorRequestManager.markCoordinatorUnknown(error.message(), requestTimeMs);
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                logInfo("Coordinator is loading; will retry on next poll", body);
                break;

            case UNKNOWN_MEMBER_ID:
                logInfo(
                    "Topology description push rejected with UNKNOWN_MEMBER_ID; heartbeat will trigger rejoin",
                    body
                );
                streamsRebalanceData.setTopologyPushRequired(false);
                break;

            case STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED:
            case INVALID_REQUEST:
            case UNSUPPORTED_VERSION:
            case GROUP_ID_NOT_FOUND:
            case GROUP_AUTHORIZATION_FAILED:
            default:
                logger.warn("Topology description push failed with {}: {}",
                    error, body.data().errorMessage());
                streamsRebalanceData.setTopologyPushRequired(false);
                break;
        }
    }

    private void logInfo(final String message, final StreamsGroupTopologyDescriptionUpdateResponse response) {
        logger.info("{}: {}", message, response.data().errorMessage());
    }
}
