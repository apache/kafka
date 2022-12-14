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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

/**
 * This is responsible for timing to send the next {@link FindCoordinatorRequest} based on the following criteria:
 *
 * Whether there is an existing coordinator.
 * Whether there is an inflight request.
 * Whether the backoff timer has expired.
 * The {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult} contains either a wait timer
 * or a singleton list of {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest}.
 *
 * The {@link FindCoordinatorRequest} will be handled by the {@link FindCoordinatorRequestHandler} callback, which
 * subsequently invokes {@code onResponse} to handle the exception and response. Note that the coordinator node will be
 * marked {@code null} upon receiving a failure.
 */
public class CoordinatorRequestManager implements RequestManager {
    private static final long COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS = 60 * 1000;
    private final Logger log;
    private final ErrorEventHandler nonRetriableErrorHandler;
    private final String groupId;

    private final RequestState coordinatorRequestState;
    private long timeMarkedUnknownMs = -1L; // starting logging a warning only after unable to connect for a while
    private long totalDisconnectedMin = 0;
    private Node coordinator;

    public CoordinatorRequestManager(final LogContext logContext,
                                     final ConsumerConfig config,
                                     final ErrorEventHandler errorHandler,
                                     final String groupId) {
        Objects.requireNonNull(groupId);
        this.log = logContext.logger(this.getClass());
        this.nonRetriableErrorHandler = errorHandler;
        this.groupId = groupId;
        this.coordinatorRequestState = new RequestState(config);
    }

    // Visible for testing
    CoordinatorRequestManager(final LogContext logContext,
                              final ErrorEventHandler errorHandler,
                              final String groupId,
                              final RequestState coordinatorRequestState) {
        Objects.requireNonNull(groupId);
        this.log = logContext.logger(this.getClass());
        this.nonRetriableErrorHandler = errorHandler;
        this.groupId = groupId;
        this.coordinatorRequestState = coordinatorRequestState;
    }

    /**
     * Poll for the FindCoordinator request.
     * If we don't need to discover a coordinator, this method will return a PollResult with Long.MAX_VALUE backoff time and an empty list.
     * If we are still backing off from a previous attempt, this method will return a PollResult with the remaining backoff time and an empty list.
     * Otherwise, this returns will return a PollResult with a singleton list of UnsentRequest and Long.MAX_VALUE backoff time.
     * Note that this method does not involve any actual network IO, and it only determines if we need to send a new request or not.
     *
     * @param currentTimeMs current time in ms.
     * @return {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult}. This will not be {@code null}.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        if (this.coordinator != null) {
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList());
        }

        if (coordinatorRequestState.canSendRequest(currentTimeMs)) {
            NetworkClientDelegate.UnsentRequest request = makeFindCoordinatorRequest(currentTimeMs);
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.singletonList(request));
        }

        return new NetworkClientDelegate.PollResult(
                coordinatorRequestState.remainingBackoffMs(currentTimeMs),
                Collections.emptyList());
    }

    private NetworkClientDelegate.UnsentRequest makeFindCoordinatorRequest(final long currentTimeMs) {
        coordinatorRequestState.updateLastSend(currentTimeMs);
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                .setKey(this.groupId);
        return NetworkClientDelegate.UnsentRequest.makeUnsentRequest(
                new FindCoordinatorRequest.Builder(data),
                new FindCoordinatorRequestHandler(),
                null);
    }

    /**
     * Mark the current coordinator null.
     *
     * @param cause         why the coordinator is marked unknown.
     * @param currentTimeMs the current time in ms.
     */
    protected void markCoordinatorUnknown(final String cause, final long currentTimeMs) {
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}. "
                    + "Rediscovery will be attempted.", this.coordinator, cause);
            this.coordinator = null;
            timeMarkedUnknownMs = currentTimeMs;
            totalDisconnectedMin = 0;
        } else {
            long durationOfOngoingDisconnectMs = Math.max(0, currentTimeMs - timeMarkedUnknownMs);
            long currDisconnectMin = durationOfOngoingDisconnectMs / COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS;
            if (currDisconnectMin > this.totalDisconnectedMin) {
                log.debug("Consumer has been disconnected from the group coordinator for {}ms", durationOfOngoingDisconnectMs);
                totalDisconnectedMin = currDisconnectMin;
            }
        }
    }

    private void onSuccessfulResponse(final FindCoordinatorResponseData.Coordinator coordinator) {
        // use MAX_VALUE - node.id as the coordinator id to allow separate connections
        // for the coordinator in the underlying network client layer
        int coordinatorConnectionId = Integer.MAX_VALUE - coordinator.nodeId();
        this.coordinator = new Node(
                coordinatorConnectionId,
                coordinator.host(),
                coordinator.port());
        log.info("Discovered group coordinator {}", coordinator);
        coordinatorRequestState.reset();
    }

    private void onFailedCoordinatorResponse(final Exception exception, final long currentTimeMs) {
        coordinatorRequestState.updateLastFailedAttempt(currentTimeMs);
        markCoordinatorUnknown("FindCoordinator failed with exception", currentTimeMs);

        if (exception instanceof RetriableException) {
            log.debug("FindCoordinator request failed due to retriable exception", exception);
            return;
        }

        if (exception == Errors.GROUP_AUTHORIZATION_FAILED.exception()) {
            log.debug("FindCoordinator request failed due to authorization error {}", exception.getMessage());
            nonRetriableErrorHandler.handle(GroupAuthorizationException.forGroupId(this.groupId));
            return;
        }

        log.warn("FindCoordinator request failed due to fatal exception", exception);
        nonRetriableErrorHandler.handle(exception);
    }

    /**
     * Handles the response and exception upon completing the {@link FindCoordinatorRequest}. This is invoked in the callback
     * {@link FindCoordinatorRequestHandler}. If the response was successful, a coordinator node will be updated. If the
     * response failed due to errors, the current coordinator will be marked unknown.
     *
     * @param currentTimeMs current time ins ms.
     * @param response      the response for finding the coordinator. null if an exception is thrown.
     * @param e             the exception, null if a valid response is received.
     */
    protected void onResponse(final long currentTimeMs, final FindCoordinatorResponse response, final Exception e) {
        // handles Runtime exception
        if (e != null) {
            onFailedCoordinatorResponse(e, currentTimeMs);
            return;
        }

        Optional<FindCoordinatorResponseData.Coordinator> coordinator = response.coordinatorByKey(this.groupId);
        if (!coordinator.isPresent()) {
            String msg = String.format("Response did not contain expected coordinator section for groupId: %s", this.groupId);
            onFailedCoordinatorResponse(new IllegalStateException(msg), currentTimeMs);
            return;
        }

        FindCoordinatorResponseData.Coordinator node = coordinator.get();
        if (node.errorCode() != Errors.NONE.code()) {
            onFailedCoordinatorResponse(Errors.forCode(node.errorCode()).exception(), currentTimeMs);
            return;
        }
        onSuccessfulResponse(node);
    }

    /**
     * Returns the current coordinator node.
     *
     * @return the current coordinator node.
     */
    public Optional<Node> coordinator() {
        return Optional.ofNullable(this.coordinator);
    }

    private class FindCoordinatorRequestHandler extends NetworkClientDelegate.AbstractRequestFutureCompletionHandler {
        @Override
        public void handleResponse(final ClientResponse r, final Exception e) {
            CoordinatorRequestManager.this.onResponse(
                    r.receivedTimeMs(), (FindCoordinatorResponse) r.responseBody(),
                    e);
        }
    }
}
