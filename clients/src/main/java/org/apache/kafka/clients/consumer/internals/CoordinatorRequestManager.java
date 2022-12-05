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
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Handles the timing of the next FindCoordinatorRequest based on the {@link CoordinatorRequestState}. It checks for:
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
    private final Time time;
    private final long requestTimeoutMs;
    private final ErrorEventHandler errorHandler;
    private final long rebalanceTimeoutMs;
    private final String groupId;

    private final CoordinatorRequestState coordinatorRequestState;
    private long timeMarkedUnknownMs = -1L; // starting logging a warning only after unable to connect for a while
    private Node coordinator;


    public CoordinatorRequestManager(final Time time,
                                     final LogContext logContext,
                                     final ConsumerConfig config,
                                     final ErrorEventHandler errorHandler,
                                     final String groupId,
                                     final long rebalanceTimeoutMs) {
        Objects.requireNonNull(groupId);
        this.time = time;
        this.log = logContext.logger(this.getClass());
        this.errorHandler = errorHandler;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.coordinatorRequestState = new CoordinatorRequestState(config);
    }

    // Visible for testing
    CoordinatorRequestManager(final Time time,
                              final LogContext logContext,
                              final ErrorEventHandler errorHandler,
                              final String groupId,
                              final long rebalanceTimeoutMs,
                              final long requestTimeoutMs,
                              final CoordinatorRequestState coordinatorRequestState) {
        Objects.requireNonNull(groupId);
        this.time = time;
        this.log = logContext.logger(this.getClass());
        this.errorHandler = errorHandler;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.requestTimeoutMs = requestTimeoutMs;
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
                this.time.timer(requestTimeoutMs),
                new FindCoordinatorRequest.Builder(data),
                new FindCoordinatorRequestHandler());
    }

    /**
     * Mark the current coordinator null and return the old coordinator. Return an empty Optional
     * if the current coordinator is unknown.
     * @param cause why the coordinator is marked unknown
     * @return Optional coordinator node that can be null.
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

    private void handleSuccessFindCoordinatorResponse(
            final FindCoordinatorResponseData.Coordinator coordinator,
            final long currentTimeMS) {
        // use MAX_VALUE - node.id as the coordinator id to allow separate connections
        // for the coordinator in the underlying network client layer
        int coordinatorConnectionId = Integer.MAX_VALUE - coordinator.nodeId();

        this.coordinator = new Node(
                coordinatorConnectionId,
                coordinator.host(),
                coordinator.port());
        log.info("Discovered group coordinator {}", coordinator);
        coordinatorRequestState.reset();
        return;
    }

    private void handleFailedCoordinatorResponse(
            final FindCoordinatorResponseData.Coordinator response,
            final long currentTimeMS) {
        Errors error = Errors.forCode(response.errorCode());
        log.debug("FindCoordinator request failed due to {}", error.toString());
        coordinatorRequestState.updateLastFailedAttempt(currentTimeMS);
        markCoordinatorUnknown("coordinator unavailable", currentTimeMS);
        if (!(error.exception() instanceof RetriableException)) {
            log.info("FindCoordinator request hit fatal exception", error.exception());
            // Remember the exception if fatal so we can ensure
            // it gets thrown by the main thread
            errorHandler.handle(error.exception());
            return;
        }

        if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
            errorHandler.handle(GroupAuthorizationException.forGroupId(this.groupId));
            return;
        }

        log.debug("Group coordinator lookup failed: {}", response.errorMessage());
        errorHandler.handle(error.exception());

        log.debug("Coordinator discovery failed, refreshing metadata", error.exception());
    }

    public void onResponse(final FindCoordinatorResponse response, Throwable t) {
        long currentTimeMs = time.milliseconds();
        // handle Runtime exception
        if (t != null) {
            log.error("FindCoordinator request failed due to {}", t.getMessage());
            markCoordinatorUnknown("coordinator unavailable", currentTimeMs);
            return;
        }

        List<FindCoordinatorResponseData.Coordinator> coordinators = response.getCoordinatorByKey(this.groupId);
        if (coordinators.size() != 1) {
            coordinatorRequestState.updateLastFailedAttempt(currentTimeMs);
            String msg = String.format("Group coordinator lookup failed: Response should contain exactly one " +
                    "coordinator, it has %d", coordinators.size());
            log.error(msg);
            errorHandler.handle(new IllegalStateException(msg));
            return;
        }

        FindCoordinatorResponseData.Coordinator node = coordinators.get(0);
        if (node.errorCode() != Errors.NONE.code()) {
            coordinatorRequestState.updateLastFailedAttempt(currentTimeMs);
            handleFailedCoordinatorResponse(node, currentTimeMs);
            return;
        }
        handleSuccessFindCoordinatorResponse(node, currentTimeMs);
    }

    public Node coordinator() {
        return this.coordinator;
    }

    // Visible for testing
    static class CoordinatorRequestState {
        final static int RECONNECT_BACKOFF_EXP_BASE = 2;
        final static double RECONNECT_BACKOFF_JITTER = 0.2;
        private final ExponentialBackoff exponentialBackoff;
        private long lastSentMs = -1;
        private long lastReceivedMs = -1;
        private int numAttempts = 0;
        private long backoffMs = 0;

        public CoordinatorRequestState(ConsumerConfig config) {
            this.exponentialBackoff = new ExponentialBackoff(
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    RECONNECT_BACKOFF_EXP_BASE,
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    RECONNECT_BACKOFF_JITTER);
        }

        // Visible for testing
        CoordinatorRequestState(final int reconnectBackoffMs,
                                final int reconnectBackoffExpBase,
                                final int reconnectBackoffMaxMs,
                                final int jitter) {
            this.exponentialBackoff = new ExponentialBackoff(
                    reconnectBackoffMs,
                    reconnectBackoffExpBase,
                    reconnectBackoffMaxMs,
                    jitter);
        }

        public void reset() {
            this.lastSentMs = -1;
            this.lastReceivedMs = -1;
            this.numAttempts = 0;
            this.backoffMs = exponentialBackoff.backoff(0);
        }

        public boolean canSendRequest(final long currentTimeMs) {
            if (this.lastSentMs == -1) {
                // no request has been sent
                return true;
            }

            // TODO: I think there's a case when we want to resend the FindCoordinator when we haven't received
            //  anything yet.
            if (this.lastReceivedMs == -1 ||
                    this.lastReceivedMs < this.lastSentMs) {
                // there is an inflight request
                return false;
            }

            return requestBackoffExpired(currentTimeMs);
        }

        public void updateLastSend(final long currentTimeMs) {
            // Here we update the timer everytime we try to send a request. Also increment number of attempts.
            this.lastSentMs = currentTimeMs;
        }

        public void updateLastFailedAttempt(final long currentTimeMs) {
            this.lastReceivedMs = currentTimeMs;
            this.backoffMs = exponentialBackoff.backoff(numAttempts);
            this.numAttempts++;
        }

        private boolean requestBackoffExpired(final long currentTimeMs) {
            return (currentTimeMs - this.lastReceivedMs) >= this.backoffMs;
        }

        private long remainingBackoffMs(final long currentTimeMs) {
            return Math.max(0, this.backoffMs - (currentTimeMs - this.lastReceivedMs));
        }
    }

    private class FindCoordinatorRequestHandler extends NetworkClientDelegate.AbstractRequestFutureCompletionHandler {

        @Override
        public void handleResponse(ClientResponse r, Throwable t) {
            CoordinatorRequestManager.this.onResponse((FindCoordinatorResponse) r.responseBody(), t);
        }
    }
}
