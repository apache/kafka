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
import java.util.Optional;

public class CoordinatorRequestManager implements RequestManager {

    private final Logger log;
    private final Time time;
    private final long requestTimeoutMs;
    private final ErrorEventHandler errorHandler;
    private final long rebalanceTimeoutMs;
    private final Optional<String> groupId;

    private final CoordinatorRequestState coordinatorRequestState;
    private long timeMarkedUnknownMs = -1L; // starting logging a warning only after unable to connect for a while
    private Node coordinator;


    public CoordinatorRequestManager(final Time time,
                                     final LogContext logContext,
                                     final ConsumerConfig config,
                                     final ErrorEventHandler errorHandler,
                                     final Optional<String> groupId,
                                     final long rebalanceTimeoutMs) {
        this.time = time;
        this.log = logContext.logger(this.getClass());
        this.errorHandler = errorHandler;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.coordinatorRequestState = new CoordinatorRequestState(config);
    }

    CoordinatorRequestManager(final Time time,
                              final LogContext logContext,
                              final ErrorEventHandler errorHandler,
                              final Optional<String> groupId,
                              final long rebalanceTimeoutMs,
                              final long requestTimeoutMs,
                              final CoordinatorRequestState coordinatorRequestState) {
        this.time = time;
        this.log = logContext.logger(this.getClass());
        this.errorHandler = errorHandler;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.coordinatorRequestState = coordinatorRequestState;
    }

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (coordinatorRequestState.canSendRequest(currentTimeMs)) {
            NetworkClientDelegate.UnsentRequest request = makeFindCoordinatorRequest(currentTimeMs);
            return new NetworkClientDelegate.PollResult(-1, Collections.singletonList(request));
        }

        return new NetworkClientDelegate.PollResult(
                coordinatorRequestState.remainingBackoffMs(currentTimeMs),
                new ArrayList<>());
    }

    private NetworkClientDelegate.UnsentRequest makeFindCoordinatorRequest(final long currentTimeMs) {
        coordinatorRequestState.updateLastSend(currentTimeMs);
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                .setKey(this.groupId.orElse(null));
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
    protected Optional<Node> markCoordinatorUnknown(final String cause) {
        Node oldCoordinator = this.coordinator;
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}. "
                            + "Rediscovery will be attempted.", this.coordinator, cause);
            this.coordinator = null;
            timeMarkedUnknownMs = time.milliseconds();
        } else {
            long durationOfOngoingDisconnect = Math.max(0, time.milliseconds() - timeMarkedUnknownMs);
            if (durationOfOngoingDisconnect > this.rebalanceTimeoutMs)
                log.debug("Consumer has been disconnected from the group coordinator for {}ms",
                        durationOfOngoingDisconnect);
        }
        return Optional.ofNullable(oldCoordinator);
    }

    private void handleSuccessFindCoordinatorResponse(final FindCoordinatorResponse response) {
        final long currentTimeMS = time.milliseconds();
        List<FindCoordinatorResponseData.Coordinator> coordinators = response.coordinators();
        if (coordinators.size() != 1) {
            coordinatorRequestState.updateLastFailedAttempt(currentTimeMS);
            log.error(
                    "Group coordinator lookup failed: Invalid response containing more than a single coordinator");
            errorHandler.handle(new IllegalStateException(
                    "Group coordinator lookup failed: Invalid response containing more than a single coordinator"));
        }
        FindCoordinatorResponseData.Coordinator coordinatorData = coordinators.get(0);
        Errors error = Errors.forCode(coordinatorData.errorCode());
        if (error == Errors.NONE) {
            // use MAX_VALUE - node.id as the coordinator id to allow separate connections
            // for the coordinator in the underlying network client layer
            int coordinatorConnectionId = Integer.MAX_VALUE - coordinatorData.nodeId();

            this.coordinator = new Node(
                    coordinatorConnectionId,
                    coordinatorData.host(),
                    coordinatorData.port());
            log.info("Discovered group coordinator {}", coordinator);
            coordinatorRequestState.reset();
            return;
        }

        coordinatorRequestState.updateLastFailedAttempt(currentTimeMS);
        if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
            errorHandler.handle(GroupAuthorizationException.forGroupId(this.groupId.orElse(null)));
            return;
        }

        log.debug("Group coordinator lookup failed: {}", coordinatorData.errorMessage());
        errorHandler.handle(error.exception());
    }

    private void handleFailedCoordinatorResponse(final FindCoordinatorResponse response) {
        log.debug("FindCoordinator request failed due to {}", response.error().toString());

        if (!(response.error().exception() instanceof RetriableException)) {
            log.info("FindCoordinator request hit fatal exception", response.error().exception());
            // Remember the exception if fatal so we can ensure
            // it gets thrown by the main thread
            errorHandler.handle(response.error().exception());
        }

        log.debug("Coordinator discovery failed, refreshing metadata", response.error().exception());
    }

    /**
     * Handle 3 cases of FindCoordinatorResponse: success, failure, timedout
     *
     * @param response FindCoordinator response
     */
    public void onResponse(final FindCoordinatorResponse response) {
        if (response.hasError()) {
            coordinatorRequestState.updateLastFailedAttempt(time.milliseconds());
            handleFailedCoordinatorResponse(response);
            return;
        }
        handleSuccessFindCoordinatorResponse(response);
    }

    public Node coordinator() {
        return coordinator;
    }

    // Visible for testing
    static class CoordinatorRequestState {
        final static int RECONNECT_BACKOFF_EXP_BASE = 2;
        final static double RECONNECT_BACKOFF_JITTER = 0.2;
        private long lastSentMs = -1;
        private long lastReceivedMs = -1;
        private int numAttempts = 0;
        private long backoffMs = 0;
        private final ExponentialBackoff exponentialBackoff;

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
            CoordinatorRequestManager.this.onResponse((FindCoordinatorResponse) r.responseBody());
        }
    }
}
