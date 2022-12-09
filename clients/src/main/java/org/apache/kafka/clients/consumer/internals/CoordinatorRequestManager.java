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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

/**
 * Handles the timing of the next FindCoordinatorRequest based on the {@link RequestState}. It checks for:
 * 1. If there's an existing coordinator.
 * 2. If there is an inflight request
 * 3. If the backoff timer has expired
 *
 * The {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult} contains either a wait
 * timer, or a singleton list of
 * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest}.
 *
 * The FindCoordinatorResponse will be handled by the {@link FindCoordinatorRequestHandler} callback, which
 * subsequently invokes {@code onResponse} to handle the exceptions and responses. Note that, the coordinator node
 * will be marked {@code null} upon receiving a failure.
 */
public class CoordinatorRequestManager implements RequestManager {

    private final Logger log;
    private final ErrorEventHandler errorHandler;
    private final long rebalanceTimeoutMs;
    private final String groupId;

    private final RequestState coordinatorRequestState;
    private long timeMarkedUnknownMs = -1L; // starting logging a warning only after unable to connect for a while
    private Node coordinator;


    public CoordinatorRequestManager(final LogContext logContext,
                                     final ConsumerConfig config,
                                     final ErrorEventHandler errorHandler,
                                     final String groupId,
                                     final long rebalanceTimeoutMs) {
        Objects.requireNonNull(groupId);
        this.log = logContext.logger(this.getClass());
        this.errorHandler = errorHandler;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.coordinatorRequestState = new RequestState(config);
    }

    // Visible for testing
    CoordinatorRequestManager(final LogContext logContext,
                              final ErrorEventHandler errorHandler,
                              final String groupId,
                              final long rebalanceTimeoutMs,
                              final RequestState coordinatorRequestState) {
        Objects.requireNonNull(groupId);
        this.log = logContext.logger(this.getClass());
        this.errorHandler = errorHandler;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.coordinatorRequestState = coordinatorRequestState;
    }

    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        if (this.coordinator != null) {
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList());
        }

        if (coordinatorRequestState.canSendRequest(currentTimeMs)) {
            NetworkClientDelegate.UnsentRequest request = makeFindCoordinatorRequest(currentTimeMs);
            return new NetworkClientDelegate.PollResult(0, Collections.singletonList(request));
        }

        return new NetworkClientDelegate.PollResult(
                coordinatorRequestState.remainingBackoffMs(currentTimeMs),
                new ArrayList<>());
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
     * Mark the current coordinator null and return the old coordinator. Return an empty Optional
     * if the current coordinator is unknown.
     * @param cause why the coordinator is marked unknown
     */
    protected void markCoordinatorUnknown(final String cause, final long currentTimeMs) {
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}. "
                            + "Rediscovery will be attempted.", this.coordinator, cause);
            this.coordinator = null;
            timeMarkedUnknownMs = currentTimeMs;
        } else {
            long durationOfOngoingDisconnect = Math.max(0, currentTimeMs - timeMarkedUnknownMs);
            if (durationOfOngoingDisconnect > this.rebalanceTimeoutMs)
                log.debug("Consumer has been disconnected from the group coordinator for {}ms",
                        durationOfOngoingDisconnect);
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

    private void onFailedCoordinatorResponse(
            final Exception exception,
            final long currentTimeMs) {
        coordinatorRequestState.updateLastFailedAttempt(currentTimeMs);
        markCoordinatorUnknown("coordinator unavailable", currentTimeMs);

        if (exception == Errors.GROUP_AUTHORIZATION_FAILED.exception()) {
            log.debug("FindCoordinator request failed due to authorization error {}", exception.getMessage());
            errorHandler.handle(GroupAuthorizationException.forGroupId(this.groupId));
            return;
        }

        if (exception instanceof RetriableException) {
            log.debug("FindCoordinator request failed due to retriable exception", exception);
            return;
        }

        log.warn("FindCoordinator request failed due to fatal exception", exception);
        errorHandler.handle(exception);
    }

    public void onResponse(final long currentTimeMs, final FindCoordinatorResponse response, final Exception e) {
        // handle Runtime exception
        if (e != null) {
            onFailedCoordinatorResponse(e, currentTimeMs);
            return;
        }

        Optional<FindCoordinatorResponseData.Coordinator> coordinator = response.getCoordinatorByKey(this.groupId);
        if (!coordinator.isPresent()) {
            String msg = String.format("Coordinator not found for groupId: %s", this.groupId);
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

    public Node coordinator() {
        return this.coordinator;
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
