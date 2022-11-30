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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

public class CoordinatorManager {
    final static int RECONNECT_BACKOFF_EXP_BASE = 2;
    final static double RECONNECT_BACKOFF_JITTER = 0.0;

    private final Logger log;
    private final Time time;
    private final long requestTimeoutMs;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final ExponentialBackoff exponentialBackoff;
    private final long rebalanceTimeoutMs;
    private final Optional<String> groupId;

    private CoordinatorRequestState coordinatorRequestState;
    private long lastTimeOfConnectionMs = -1L; // starting logging a warning only after unable to connect for a while
    private Node coordinator;


    public CoordinatorManager(final Time time,
                              final LogContext logContext,
                              final ConsumerConfig config,
                              final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                              final Optional<String> groupId,
                              final long rebalanceTimeoutMs) {
        this.time = time;
        this.log = logContext.logger(this.getClass());
        this.backgroundEventQueue = backgroundEventQueue;
        this.exponentialBackoff = new ExponentialBackoff(
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                RECONNECT_BACKOFF_EXP_BASE,
                config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                RECONNECT_BACKOFF_JITTER);
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
    }

    /**
     * Returns a non-empty UnsentRequest we need send a FindCoordinatorRequest. These conditions are:
     * 1. The request has not been sent
     * 2. If the previous request failed, and the retryBackoff has expired
     * @return Optional UnsentRequest.  Empty if we are not allowed to send a request.
     */
    public Optional<NetworkClientDelegate.UnsentRequest> tryFindCoordinator() {
        if (this.coordinatorRequestState == null) {
            this.coordinatorRequestState = new CoordinatorRequestState();
        }

        if (coordinatorRequestState.lastSentMs == -1) {
            // no request has been sent
            return makeFindCoordinatorRequest();
        }

        if (coordinatorRequestState.lastReceivedMs == -1 ||
                coordinatorRequestState.lastReceivedMs < coordinatorRequestState.lastSentMs) {
            // there is an inflight request
            return Optional.empty();
        }

        if (!coordinatorRequestState.requestBackoffExpired()) {
            // retryBackoff
            return Optional.empty();
        }

        return makeFindCoordinatorRequest();
    }

    private Optional<NetworkClientDelegate.UnsentRequest> makeFindCoordinatorRequest() {
        return NetworkClientDelegate.UnsentRequest.makeUnsentRequest(
                this.time.timer(requestTimeoutMs),
                newFindCoordinatorRequest(),
                new FindCoordinatorRequestHandler());
    }

    /**
     * Mark the current coordinator null and return the old coordinator. Return an empty Optional
     * if the current coordinator is unknown.
     * @param cause why the coordinator is marked unknown
     * @return Optional coordinator node that can be null.
     */
    protected Optional<Node> markCoordinatorUnknown(String cause) {
        Node oldCoordinator = this.coordinator;
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}. "
                            + "Rediscovery will be attempted.", this.coordinator, cause);
            this.coordinator = null;
            lastTimeOfConnectionMs = time.milliseconds();
        } else {
            long durationOfOngoingDisconnect = Math.max(0, time.milliseconds() - lastTimeOfConnectionMs);
            if (durationOfOngoingDisconnect > this.rebalanceTimeoutMs)
                log.debug("Consumer has been disconnected from the group coordinator for {}ms",
                        durationOfOngoingDisconnect);
        }
        return Optional.ofNullable(oldCoordinator);
    }

    private FindCoordinatorRequest.Builder newFindCoordinatorRequest() {
        coordinatorRequestState.updateLastSend();
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                .setKey(this.groupId.orElse(null));
        return new FindCoordinatorRequest.Builder(data);
    }

    void handleSuccessFindCoordinatorResponse(FindCoordinatorResponse response) {
        List<FindCoordinatorResponseData.Coordinator> coordinators = response.coordinators();
        if (coordinators.size() != 1) {
            log.error(
                    "Group coordinator lookup failed: Invalid response containing more than a single coordinator");
            enqueueErrorEvent(new IllegalStateException(
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

        if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
            enqueueErrorEvent(GroupAuthorizationException.forGroupId(this.groupId.orElse(null)));
            return;
        }

        log.debug("Group coordinator lookup failed: {}", coordinatorData.errorMessage());
        enqueueErrorEvent(error.exception());
    }

    void handleFailedCoordinatorResponse(FindCoordinatorResponse response) {
        log.debug("FindCoordinator request failed due to {}", response.error().toString());

        if (!(response.error().exception() instanceof RetriableException)) {
            log.info("FindCoordinator request hit fatal exception", response.error().exception());
            // Remember the exception if fatal so we can ensure
            // it gets thrown by the main thread
            enqueueErrorEvent(response.error().exception());
        }

        log.debug("Coordinator discovery failed, refreshing metadata", response.error().exception());
    }

    /**
     * Handle 3 cases of FindCoordinatorResponse: success, failure, timedout
     *
     * @param response FindCoordinator response
     */
    public void onResponse(FindCoordinatorResponse response) {
        coordinatorRequestState.updateLastReceived();
        if (response.hasError()) {
            handleFailedCoordinatorResponse(response);
            return;
        }
        handleSuccessFindCoordinatorResponse(response);
    }

    private void enqueueErrorEvent(Exception e) {
        backgroundEventQueue.add(new ErrorBackgroundEvent(e));
    }

    public Node coordinator() {
        return coordinator;
    }

    private class CoordinatorRequestState {
        private long lastSentMs;
        private long lastReceivedMs;
        private int numAttempts;
        public CoordinatorRequestState() {
            this.lastSentMs = -1;
            this.lastReceivedMs = -1;
            this.numAttempts = 0;
        }

        public void reset() {
            this.lastSentMs = -1;
            this.lastReceivedMs = -1;
        }

        public void updateLastSend() {
            // Here we update the timer everytime we try to send a request. Also increment number of attempts.
            this.numAttempts++;
            this.lastSentMs = time.milliseconds();
        }

        public void updateLastReceived() {
            this.lastReceivedMs = time.milliseconds();
        }

        private boolean requestBackoffExpired() {
            return (time.milliseconds() - this.lastReceivedMs) >= (exponentialBackoff.backoff(numAttempts));
        }
    }

    private class FindCoordinatorRequestHandler extends NetworkClientDelegate.AbstractRequestFutureCompletionHandler {

        @Override
        public void handleResponse(ClientResponse r, Throwable t) {
            CoordinatorManager.this.onResponse((FindCoordinatorResponse) r.responseBody());
        }
    }
}
