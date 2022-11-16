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
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;

public class CoordinatorStateMachine {
    private final Logger log;
    private final Time time;
    private Node coordinator = null;
    private State currentState = State.UNKNOWN;
    protected final ConsumerNetworkClient client;
    private final GroupRebalanceConfig rebalanceConfig;
    private long lastTimeOfConnectionMs = -1L; // starting logging a warning only after unable to connect for a while
    private Optional<RequestFuture<Void>> findCoordinatorFuture = Optional.empty();

    public CoordinatorStateMachine(Time time,
                                   GroupRebalanceConfig rebalanceConfig,
                                   LogContext logContext,
                                   ConsumerNetworkClient client) {
        this.time = time;
        this.log = logContext.logger(this.getClass());
        this.client = client;
        this.rebalanceConfig = rebalanceConfig;
    }

    enum State {
        UNKNOWN,
        FIND_COORDINATOR,
        CONNECTED
    }

    public void run() {
        switch (currentState) {
            case UNKNOWN:
                break;
            case FIND_COORDINATOR:
                if (!coordinatorUnknown()) {
                    transititionToConnected();
                    break;
                }
                break;

            case CONNECTED:
                if (coordinatorUnknown()) {
                    currentState = State.UNKNOWN;
                }
                break;
        }
    }

    private void transititionToConnected() {
        currentState = State.CONNECTED;
    }

    private void transitionToUnknown() {
        currentState = State.UNKNOWN;
    }

    public boolean coordinatorUnknown() {
        return checkAndGetCoordinator() == null;
    }

    public void findCoordinator() {
        if (!coordinatorUnknown()) {
            this.currentState = State.CONNECTED;
        }
        currentState = State.FIND_COORDINATOR;
        lookupCoordinator();
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     *
     * @return the current coordinator or null if it is unknown
     */
    protected Node checkAndGetCoordinator() {
        if (coordinator != null && client.isUnavailable(coordinator)) {
            markCoordinatorUnknown(true, "coordinator unavailable");
            return null;
        }
        return this.coordinator;
    }

    protected synchronized void markCoordinatorUnknown(Errors error) {
        markCoordinatorUnknown(false, "error response " + error.name());
    }

    protected synchronized void markCoordinatorUnknown(String cause) {
        markCoordinatorUnknown(false, cause);
    }

    protected synchronized void markCoordinatorUnknown(boolean isDisconnected, String cause) {
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
                client.disconnectAsync(oldCoordinator);
            }

            lastTimeOfConnectionMs = time.milliseconds();
        } else {
            long durationOfOngoingDisconnect = time.milliseconds() - lastTimeOfConnectionMs;
            if (durationOfOngoingDisconnect > rebalanceConfig.rebalanceTimeoutMs)
                log.warn("Consumer has been disconnected from the group coordinator for {}ms", durationOfOngoingDisconnect);
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
        // initiate the group metadata request
        log.debug("Sending FindCoordinator request to broker {}", node);
        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                .setKey(this.rebalanceConfig.groupId);
        FindCoordinatorRequest.Builder requestBuilder = new FindCoordinatorRequest.Builder(data);
        return client.send(node, requestBuilder)
                .compose(new FindCoordinatorResponseHandler());
    }

    protected RequestFuture<Void> lookupCoordinator() {
        if (!findCoordinatorFuture.isPresent()) {
            return findCoordinatorFuture.get();
        }
        // find a node to ask about the coordinator
        Node node = this.client.leastLoadedNode();
        if (node == null) {
            log.debug("No broker available to send FindCoordinator request");
            return RequestFuture.noBrokersAvailable();
        } else {
            this.findCoordinatorFuture = Optional.ofNullable(sendFindCoordinatorRequest(node));
        }
        return findCoordinatorFuture.get();
    }

    public boolean handleSuccessFindCoordinatorResponse(FindCoordinatorResponse resp) {
        List<FindCoordinatorResponseData.Coordinator> coordinators = resp.coordinators();
        if (coordinators.size() != 1) {
            log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
            throw new IllegalStateException("Group coordinator lookup failed: Invalid response containing more than a" +
                    " single coordinator");
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
            client.tryConnect(coordinator);
            this.currentState = State.CONNECTED;
            return true;
            // heartbeat.resetSessionTimeout();
        } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
            this.currentState = State.UNKNOWN;
            throw GroupAuthorizationException.forGroupId(rebalanceConfig.groupId);
        } else {
            log.debug("Group coordinator lookup failed: {}", coordinatorData.errorMessage());
            this.currentState = State.UNKNOWN;
            throw error.exception();
        }
    }


    public boolean handleFailedCoordinatorResponse(FindCoordinatorResponse response) {
        log.debug("FindCoordinator request failed due to {}", response.error().toString());
        this.currentState = State.UNKNOWN;

        if (!(response.error().exception() instanceof RetriableException)) {
            // Remember the exception if fatal so we can ensure
            //    } it gets thrown by the main thread
            throw response.error().exception();
        }
        return false;
    }

    private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received FindCoordinator response {}", resp);

            List<FindCoordinatorResponseData.Coordinator> coordinators = ((FindCoordinatorResponse) resp.responseBody()).coordinators();
            if (coordinators.size() != 1) {
                log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
                future.raise(new IllegalStateException("Group coordinator lookup failed: Invalid response containing more than a single coordinator"));
            }
            FindCoordinatorResponseData.Coordinator coordinatorData = coordinators.get(0);
            Errors error = Errors.forCode(coordinatorData.errorCode());
            if (error == Errors.NONE) {
                synchronized (CoordinatorStateMachine.this) {
                    // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                    // for the coordinator in the underlying network client layer
                    int coordinatorConnectionId = Integer.MAX_VALUE - coordinatorData.nodeId();

                    CoordinatorStateMachine.this.coordinator = new Node(
                            coordinatorConnectionId,
                            coordinatorData.host(),
                            coordinatorData.port());
                    log.info("Discovered group coordinator {}", coordinator);
                    client.tryConnect(coordinator);
                    //heartbeat.resetSessionTimeout();
                }
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                log.debug("Group coordinator lookup failed: {}", coordinatorData.errorMessage());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            log.debug("FindCoordinator request failed due to {}", e.toString());

            if (!(e instanceof RetriableException)) {
                // Remember the exception if fatal so we can ensure it gets thrown by the main thread
                //fatalFindCoordinatorException = e;
            }

            super.onFailure(e, future);
        }
    }

}
