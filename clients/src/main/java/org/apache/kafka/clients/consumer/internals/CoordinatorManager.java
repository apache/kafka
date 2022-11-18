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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
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
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final NetworkClientUtils networkClientUtil;
    private Node coordinator = null;
    protected final KafkaClient client;
    private ExponentialBackoff exponentialBackoff;
    private long lastTimeOfConnectionMs = -1L; // starting logging a warning only after unable to connect for a while
    private CoordinatorRequestState coordinatorRequestState;

    private long rebalanceTimeoutMs;
    private Optional<String> groupId;

    public CoordinatorManager(final Time time,
                              final LogContext logContext,
                              final ConsumerConfig config,
                              final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                              final KafkaClient client,
                              final Optional<String> groupId,
                              final long rebalanceTimeoutMs) {
        this.time = time;
        this.log = logContext.logger(this.getClass());
        this.client = client;
        this.backgroundEventQueue = backgroundEventQueue;
        this.exponentialBackoff = new ExponentialBackoff(
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                RECONNECT_BACKOFF_EXP_BASE,
                config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                RECONNECT_BACKOFF_JITTER);
        this.coordinatorRequestState = new CoordinatorRequestState();
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.networkClientUtil = new NetworkClientUtils(time, client);
    }

    public boolean coordinatorUnknown() {
        return checkAndGetCoordinator() == null;
    }

    public boolean ensureCoordinatorReady() {
        // coordinator found
        if (!coordinatorUnknown()) {
            coordinatorRequestState.reset();
            return true;
        }

        if (coordinatorRequestState.lastSentMs == -1) {
            // no request has been sent
            lookupCoordinator();
            return false;
        }

        if (coordinatorRequestState.lastReceivedMs == -1 ||
                coordinatorRequestState.lastReceivedMs < coordinatorRequestState.lastSentMs) {
            // there is an inflight request
            return false;
        }

        if (!coordinatorRequestState.requestBackoffExpired()) {
            // retryBackoff
            return false;
        }

        lookupCoordinator();

        if (coordinator != null && isUnavailable(coordinator)) {
            // we found the coordinator, but the connection has failed, so mark
            // it dead and backoff before retrying discovery
            markCoordinatorUnknown("coordinator unavailable");
        }

        return !coordinatorUnknown();
    }

    protected long backoffMs() {
        return exponentialBackoff.backoff(coordinatorRequestState.numAttempts);
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     *
     * @return the current coordinator or null if it is unknown
     */
    protected Node checkAndGetCoordinator() {
        if (coordinator != null && isUnavailable(coordinator)) {
            markCoordinatorUnknown(true, "coordinator unavailable");
            return null;
        }
        return this.coordinator;
    }

    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     */
    public boolean isUnavailable(Node node) {
        return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
    }

    protected void markCoordinatorUnknown(Errors error) {
        markCoordinatorUnknown(false, "error response " + error.name());
    }

    protected void markCoordinatorUnknown(String cause) {
        markCoordinatorUnknown(false, cause);
    }

    protected void markCoordinatorUnknown(boolean isDisconnected, String cause) {
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}. "
                            + "isDisconnected: {}. Rediscovery will be attempted.", this.coordinator,
                    cause, isDisconnected);
            Node oldCoordinator = this.coordinator;

            // Mark the coordinator dead before disconnecting requests since the callbacks for any pending
            // requests may attempt to do likewise. This also prevents new requests from being sent to the
            // coordinator while the disconnect is in progress.
            this.coordinator = null;

            // Disconnect from the coordinator to ensure that there are no in-flight requests remaining.
            // Pending callbacks will be invoked with a DisconnectException on the next call to poll.
            if (!isDisconnected) {
                log.info("Requesting disconnect from last known coordinator {}", oldCoordinator);
                client.disconnect(oldCoordinator.idString());
            }

            lastTimeOfConnectionMs = time.milliseconds();
        } else {
            long durationOfOngoingDisconnect = time.milliseconds() - lastTimeOfConnectionMs;
            if (durationOfOngoingDisconnect > this.rebalanceTimeoutMs)
                log.warn("Consumer has been disconnected from the group coordinator for {}ms", durationOfOngoingDisconnect);
        }
    }

    /**
     * Send a FindCoordinatorRequest
     */
    private void sendFindCoordinatorRequest(Node node) {
        log.debug("Sending FindCoordinator request to broker {}", node);
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                .setKey(this.groupId.orElse(null));
        FindCoordinatorRequest.Builder requestBuilder = new FindCoordinatorRequest.Builder(data);
        ClientRequest req = client.newClientRequest(
                node.idString(),
                requestBuilder,
                time.milliseconds(),
                true);
        coordinatorRequestState.lastSentMs = time.milliseconds();
        networkClientUtil.trySend(req, node);
    }

    private void lookupCoordinator() {
        coordinatorRequestState.incrAttempts();
        // find a node to ask about the coordinator
        Node node = networkClientUtil.leastLoadedNode();
        if (node == null) {
            log.debug("No broker available to send FindCoordinator request");
            enqueueErrorEvent(Errors.BROKER_NOT_AVAILABLE.exception());
            return;
        }
        sendFindCoordinatorRequest(node);
    }

    public void handleSuccessFindCoordinatorResponse(FindCoordinatorResponse resp) {
        List<FindCoordinatorResponseData.Coordinator> coordinators = resp.coordinators();
        if (coordinators.size() != 1) {
            log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
            enqueueErrorEvent(new IllegalStateException("Group coordinator lookup failed: Invalid response containing" +
                    " more than " +
                    "a" +
                    " single coordinator"));
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
            // ensure coordinator is ready here
            client.ready(coordinator, time.milliseconds());
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

    public void handleFailedCoordinatorResponse(FindCoordinatorResponse response) {
        log.debug("FindCoordinator request failed due to {}", response.error().toString());

        if (!(response.error().exception() instanceof RetriableException)) {
            log.info("FindCoordinator request hit fatal exception", response.error());
            // Remember the exception if fatal so we can ensure
            // it gets thrown by the main thread
            enqueueErrorEvent(response.error().exception());
        }

        log.debug("Coordinator discovery failed, refreshing metadata", response.error());
    }

    /**
     * Handle 3 cases of FindCoordinatorResponse: success, failure, timedout
     *
     * @param response FindCoordinator response
     */
    public void onResponse(FindCoordinatorResponse response) {
        coordinatorRequestState.lastReceivedMs = time.milliseconds();
        if (response.hasError()) {
            handleFailedCoordinatorResponse(response);
            return;
        }
        handleSuccessFindCoordinatorResponse(response);
    }

    private void enqueueErrorEvent(Exception e) {
        backgroundEventQueue.add(new ErrorBackgroundEvent(e));
    }

    private class CoordinatorRequestState {
        long lastSentMs;
        long lastReceivedMs;
        int numAttempts;
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
            this.lastSentMs = time.milliseconds();
        }

        public void updateLastReceived() {
            this.lastReceivedMs = time.milliseconds();
        }

        public void incrAttempts() {
            this.numAttempts++;
        }

        private boolean requestBackoffExpired() {
            return (time.milliseconds() - this.lastReceivedMs) >= (exponentialBackoff.backoff(numAttempts));
        }
    }
}
