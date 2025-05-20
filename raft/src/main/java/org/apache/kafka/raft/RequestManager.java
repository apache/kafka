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
package org.apache.kafka.raft;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;

/**
 * The request manager keeps tracks of the connection with remote replicas.
 *
 * When sending a request update this type by calling {@code onRequestSent(Node, long, long)}. When
 * the RPC returns a response, update this manager with {@code onResponseResult(Node, long, boolean, long)}.
 *
 * Connections start in the ready state ({@code isReady(Node, long)} returns true).
 *
 * When a request times out or completes successfully the collection will transition back to the
 * ready state.
 *
 * When a request completes with an error it still transitions to the backoff state until
 * {@code retryBackoffMs}.
 */
public class RequestManager {
    private record NodeAndRequestKey(String nodeId, ApiKeys requestKey) {
        private static NodeAndRequestKey of(Node node, ApiKeys requestKey) {
            return new NodeAndRequestKey(node.idString(), requestKey);
        }
    }
    private final Map<NodeAndRequestKey, RequestState> inflightRequests = new HashMap<>();
    private final ArrayList<Node> bootstrapServers;

    private final int retryBackoffMs;
    private final int requestTimeoutMs;
    private final Random random;

    public RequestManager(
        Collection<Node> bootstrapServers,
        int retryBackoffMs,
        int requestTimeoutMs,
        Random random
    ) {
        this.bootstrapServers = new ArrayList<>(bootstrapServers);
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.random = random;
    }

    /**
     * Returns true if there are any in-flight requests for a set of request types.
     *
     * This is useful for satisfying the invariant that there is only one pending Fetch
     * and FetchSnapshot request.
     * If there are more than one pending fetch request, it is possible for the follower to write
     * the same offset twice.
     *
     * @param currentTimeMs the current time
     * @param requestKeys the set request types check for pending requests
     * @return true if the request manager is tracking at least one request of the given types
     */
    public boolean hasAnyInflightRequest(long currentTimeMs, Set<ApiKeys> requestKeys) {
        boolean result = false;

        final var iterator = inflightRequests.entrySet().iterator();
        while (iterator.hasNext()) {
            final var entry = iterator.next();
            final var nodeAndRequestKey = entry.getKey();
            final var requestState = entry.getValue();
            if (requestState.hasRequestTimedOut(currentTimeMs)) {
                // Mark the node as ready after request timeout
                iterator.remove();
            } else if (requestState.isBackoffComplete(currentTimeMs)) {
                // Mark the node as ready after completed backoff
                iterator.remove();
            } else if (requestKeys.contains(nodeAndRequestKey.requestKey) &&
                requestState.hasInflightRequest(currentTimeMs)) {
                // If there is at least one inflight request, it is enough
                // to stop checking the rest of the inflightRequests
                result = true;
                break;
            }
        }

        return result;
    }

    /**
     * Returns a random bootstrap node that is ready to receive a request.
     *
     * This method doesn't return a node if there is at least one request pending. In general this
     * method is used to send Fetch requests. Fetch requests have the invariant that there can
     * only be one pending Fetch or FetchSnapshot request for the LEO.
     *
     * @param currentTimeMs the current time
     * @return a random ready bootstrap node
     */
    public Optional<Node> findReadyBootstrapServer(long currentTimeMs) {
        // Check that there are no inflight fetch or fetch snapshot requests
        // across any of the known nodes not just the bootstrap servers
        if (hasAnyInflightRequest(currentTimeMs, Set.of(ApiKeys.FETCH, ApiKeys.FETCH_SNAPSHOT))) {
            return Optional.empty();
        }

        int startIndex = random.nextInt(bootstrapServers.size());
        Optional<Node> result = Optional.empty();
        for (int i = 0; i < bootstrapServers.size(); i++) {
            int index = (startIndex + i) % bootstrapServers.size();
            Node node = bootstrapServers.get(index);

            if (isReady(node, currentTimeMs, ApiKeys.FETCH) &&
                isReady(node, currentTimeMs, ApiKeys.FETCH_SNAPSHOT)) {
                result = Optional.of(node);
                break;
            }
        }

        return result;
    }

    /**
     * Computes the amount of time needed to wait before a bootstrap server is ready for a Fetch
     * request.
     *
     * If there is a connection with a pending request it returns the amount of time to wait until
     * the request times out.
     *
     * Returns zero, if there are no pending requests and at least one of the bootstrap servers is
     * ready.
     *
     * If all the bootstrap servers are backing off and there are no pending requests, return
     * the minimum amount of time until a bootstrap server becomes ready.
     *
     * @param currentTimeMs the current time
     * @return the amount of time to wait until bootstrap server can accept a Fetch request
     */
    public long backoffBeforeAvailableBootstrapServer(long currentTimeMs) {
        long minBackoffMs = retryBackoffMs;

        final var iterator = inflightRequests.entrySet().iterator();
        while (iterator.hasNext()) {
            final var entry = iterator.next();
            final var requestType = entry.getKey().requestKey;
            final var requestState = entry.getValue();
            if (requestState.hasRequestTimedOut(currentTimeMs)) {
                // Mark the node as ready after request timeout
                iterator.remove();
            } else if (requestState.isBackoffComplete(currentTimeMs)) {
                // Mark the node as ready after completed backoff
                iterator.remove();
            } else if ((requestType == ApiKeys.FETCH || requestType == ApiKeys.FETCH_SNAPSHOT) &&
                requestState.hasInflightRequest(currentTimeMs)) {
                // There can be at most one inflight fetch or fetch snapshot request
                return requestState.remainingRequestTimeMs(currentTimeMs);
            } else if ((requestType == ApiKeys.FETCH || requestType == ApiKeys.FETCH_SNAPSHOT) &&
                requestState.isBackingOff(currentTimeMs)) {
                minBackoffMs = Math.min(minBackoffMs, requestState.remainingBackoffMs(currentTimeMs));
            }
        }

        // There are no inflight fetch requests so check if there is a ready bootstrap server
        for (Node node : bootstrapServers) {
            if (isReady(node, currentTimeMs, ApiKeys.FETCH) &&
                isReady(node, currentTimeMs, ApiKeys.FETCH_SNAPSHOT)) {
                return 0L;
            }
        }

        // There are no ready bootstrap servers and inflight fetch requests, return the backoff
        return minBackoffMs;
    }

    /**
     * Returns true if a request to the given node with the given request type has timed out.
     *
     * @param node the destination of the request
     * @param timeMs the current time
     * @param requestKey the type of request to check
     * @return true if the request has timed out, false otherwise
     */
    public boolean hasRequestTimedOut(Node node, long timeMs, ApiKeys requestKey) {
        RequestState state = inflightRequests.get(NodeAndRequestKey.of(node, requestKey));
        if (state == null) {
            return false;
        }

        return state.hasRequestTimedOut(timeMs);
    }

    /**
     * Returns true if a request to the given node with the given request type is ready to be sent.
     *
     * @param node the destination of the request
     * @param timeMs the current time
     * @param requestKey the type of request to check
     * @return true if the request is ready to be sent, false otherwise
     */
    public boolean isReady(Node node, long timeMs, ApiKeys requestKey) {
        RequestState state = inflightRequests.get(NodeAndRequestKey.of(node, requestKey));
        if (state == null) {
            return true;
        }

        boolean ready = state.isReady(timeMs);
        if (ready) {
            reset(node, requestKey);
        }

        return ready;
    }

    /**
     * Returns true if a request to the given node with the given request type is backing off.
     *
     * @param node the destination of the request
     * @param timeMs the current time
     * @param requestKey the type of request to check
     * @return true if the request is backing off, false otherwise
     */
    public boolean isBackingOff(Node node, long timeMs, ApiKeys requestKey) {
        RequestState state = inflightRequests.get(NodeAndRequestKey.of(node, requestKey));
        if (state == null) {
            return false;
        }

        return state.isBackingOff(timeMs);
    }

    /**
     * Returns the amount of time to wait in milliseconds before a request to the given
     * node with the given request type is considered timed out.
     *
     * @param node the destination of the request
     * @param timeMs the current time
     * @param requestKey the type of request to check
     * @return 0 if there is no in-flight request, or the amount of time left
     */
    public long remainingRequestTimeMs(Node node, long timeMs, ApiKeys requestKey) {
        RequestState state = inflightRequests.get(NodeAndRequestKey.of(node, requestKey));
        if (state == null) {
            return 0;
        }

        return state.remainingRequestTimeMs(timeMs);
    }

    /**
     * Returns the amount of time to wait in milliseconds before a request to the given
     * node with the given request type can be retried.
     *
     * @param node the destination of the request
     * @param timeMs the current time
     * @param requestKey the type of request to check
     * @return 0 if the request is not backing off, or the amount of time left in the backoff
     */
    public long remainingBackoffMs(Node node, long timeMs, ApiKeys requestKey) {
        RequestState state = inflightRequests.get(NodeAndRequestKey.of(node, requestKey));
        if (state == null) {
            return 0;
        }

        return state.remainingBackoffMs(timeMs);
    }

    /**
     * Returns whether the manager expects a response for the given request key and correlation id
     * from the given node.
     *
     * @param node the node to check
     * @param correlationId the correlation id of the request
     * @param requestKey the type of request to check
     * @return true if a response is expected, false otherwise
     */
    public boolean isResponseExpected(Node node, long correlationId, ApiKeys requestKey) {
        RequestState state = inflightRequests.get(NodeAndRequestKey.of(node, requestKey));
        if (state == null) {
            return false;
        }

        return state.isResponseExpected(correlationId);
    }

    /**
     * Updates the manager when a response is received.
     *
     * @param node the source of the response
     * @param correlationId the correlation id of the response
     * @param success true if the request was successful, false otherwise
     * @param timeMs the current time
     * @param requestKey the api key for the request
     */
    public void onResponseResult(
        Node node,
        long correlationId,
        boolean success,
        long timeMs,
        ApiKeys requestKey
    ) {
        if (isResponseExpected(node, correlationId, requestKey)) {
            if (success) {
                // Mark the connection as ready by resetting it
                reset(node, requestKey);
            } else {
                // Backoff the connection
                inflightRequests.get(NodeAndRequestKey.of(node, requestKey))
                    .onResponseError(correlationId, timeMs);
            }
        }
    }

    /**
     * Updates the manager when a request is sent.
     *
     * @param node the destination of the request
     * @param correlationId the correlation id of the request
     * @param timeMs the current time
     * @param requestKey the api key for the request
     */
    public void onRequestSent(Node node, long correlationId, long timeMs, ApiKeys requestKey) {
        RequestState state = inflightRequests.computeIfAbsent(
            NodeAndRequestKey.of(node, requestKey),
            key -> new RequestState(node, retryBackoffMs, requestTimeoutMs)
        );

        state.onRequestSent(correlationId, timeMs);
    }

    /**
     * Updates the manager when a request is completed or times out.
     *
     * @param node the destination of the request
     * @param requestKey the api key for the request
     */
    public void reset(Node node, ApiKeys requestKey) {
        inflightRequests.remove(NodeAndRequestKey.of(node, requestKey));
    }

    public void resetAll() {
        inflightRequests.clear();
    }

    private enum State {
        AWAITING_RESPONSE,
        BACKING_OFF,
        READY
    }

    private static final class RequestState {
        private final Node node;
        private final int retryBackoffMs;
        private final int requestTimeoutMs;

        private State state = State.READY;
        private long lastSendTimeMs = 0L;
        private long lastFailTimeMs = 0L;
        private OptionalLong inFlightCorrelationId = OptionalLong.empty();

        private RequestState(
            Node node,
            int retryBackoffMs,
            int requestTimeoutMs
        ) {
            this.node = node;
            this.retryBackoffMs = retryBackoffMs;
            this.requestTimeoutMs = requestTimeoutMs;
        }

        private boolean isBackoffComplete(long timeMs) {
            return state == State.BACKING_OFF && timeMs >= lastFailTimeMs + retryBackoffMs;
        }

        boolean hasRequestTimedOut(long timeMs) {
            return state == State.AWAITING_RESPONSE && timeMs >= lastSendTimeMs + requestTimeoutMs;
        }

        boolean isReady(long timeMs) {
            if (isBackoffComplete(timeMs) || hasRequestTimedOut(timeMs)) {
                state = State.READY;
            }
            return state == State.READY;
        }

        boolean isBackingOff(long timeMs) {
            if (state != State.BACKING_OFF) {
                return false;
            } else {
                return !isBackoffComplete(timeMs);
            }
        }

        private boolean hasInflightRequest(long timeMs) {
            if (state != State.AWAITING_RESPONSE) {
                return false;
            } else {
                return !hasRequestTimedOut(timeMs);
            }
        }

        long remainingRequestTimeMs(long timeMs) {
            if (hasInflightRequest(timeMs)) {
                return lastSendTimeMs + requestTimeoutMs - timeMs;
            } else {
                return 0;
            }
        }

        long remainingBackoffMs(long timeMs) {
            if (isBackingOff(timeMs)) {
                return lastFailTimeMs + retryBackoffMs - timeMs;
            } else {
                return 0;
            }
        }

        boolean isResponseExpected(long correlationId) {
            return inFlightCorrelationId.isPresent() && inFlightCorrelationId.getAsLong() == correlationId;
        }

        void onResponseError(long correlationId, long timeMs) {
            inFlightCorrelationId.ifPresent(inflightRequestId -> {
                if (inflightRequestId == correlationId) {
                    lastFailTimeMs = timeMs;
                    state = State.BACKING_OFF;
                    inFlightCorrelationId = OptionalLong.empty();
                }
            });
        }

        void onRequestSent(long correlationId, long timeMs) {
            lastSendTimeMs = timeMs;
            inFlightCorrelationId = OptionalLong.of(correlationId);
            state = State.AWAITING_RESPONSE;
        }

        @Override
        public String toString() {
            return String.format(
                "RequestState(node=%s, state=%s, lastSendTimeMs=%d, lastFailTimeMs=%d, inFlightCorrelationId=%s)",
                node,
                state,
                lastSendTimeMs,
                lastFailTimeMs,
                inFlightCorrelationId.isPresent() ? inFlightCorrelationId.getAsLong() : "undefined"
            );
        }
    }
}
