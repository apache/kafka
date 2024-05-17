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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import org.apache.kafka.common.Node;

public class RequestManager {
    private final Map<String, ConnectionState> connections = new HashMap<>();
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

    public Optional<Node> findReadyBootstrapServer(long currentTimeMs) {
        int startIndex = random.nextInt(bootstrapServers.size());
        Optional<Node> res = Optional.empty();
        for (int i = 0; i < bootstrapServers.size(); i++) {
            int index = (startIndex + i) % bootstrapServers.size();
            Node node = bootstrapServers.get(index);

            if (isReady(node, currentTimeMs)) {
                res = Optional.of(node);
            } else if (hasInflightRequest(node, currentTimeMs)) {
                res = Optional.empty();
                break;
            }
        }

        return res;
    }

    public long backoffBeforeAvailableBootstrapServer(long currentTimeMs) {
        long minBackoffMs = Long.MAX_VALUE;
        for (Node node : bootstrapServers) {
            if (isReady(node, currentTimeMs)) {
                return 0L;
            } else if (isBackingOff(node, currentTimeMs)) {
                minBackoffMs = Math.min(minBackoffMs, remainingBackoffMs(node, currentTimeMs));
            } else {
                minBackoffMs = Math.min(minBackoffMs, remainingRequestTimeMs(node, currentTimeMs));
            }
        }

        return minBackoffMs;
    }

    public boolean hasRequestTimedOut(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return false;
        }

        return state.hasRequestTimedOut(timeMs);
    }

    public boolean isReady(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return true;
        }

        boolean ready = state.isReady(timeMs);
        if (ready) {
            reset(node);
        }

        return ready;
    }

    public boolean isBackingOff(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return false;
        }

        return state.isBackingOff(timeMs);
    }

    public long remainingRequestTimeMs(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return 0;
        }

        return state.remainingRequestTimeMs(timeMs);
    }

    public long remainingBackoffMs(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return 0;
        }

        return  state.remainingBackoffMs(timeMs);
    }

    public boolean isResponseExpected(Node node, long correlationId) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return false;
        }

        return state.isResponseExpected(correlationId);
    }

    public void onResponseReceived(Node node, long correlationId) {
        if (isResponseExpected(node, correlationId)) {
            reset(node);
        }
    }

    public void onResponseError(Node node, long correlationId, long timeMs) {
        if (isResponseExpected(node, correlationId)) {
            connections.get(node.idString()).onResponseError(correlationId, timeMs);
        }
    }

    public void onRequestSent(Node node, long correlationId, long timeMs) {
        ConnectionState state = connections.computeIfAbsent(
            node.idString(),
            key -> new ConnectionState(node, retryBackoffMs, requestTimeoutMs)
        );

        state.onRequestSent(correlationId, timeMs);
    }

    public void reset(Node node) {
        connections.remove(node.idString());
    }

    public void resetAll() {
        connections.clear();
    }

    private boolean hasInflightRequest(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return false;
        }

        return state.hasInflightRequest(timeMs);
    }

    private enum State {
        AWAITING_RESPONSE,
        BACKING_OFF,
        READY
    }

    private final static class ConnectionState {
        private final Node node;
        private final int retryBackoffMs;
        private final int requestTimeoutMs;

        private State state = State.READY;
        private long lastSendTimeMs = 0L;
        private long lastFailTimeMs = 0L;
        private OptionalLong inFlightCorrelationId = OptionalLong.empty();

        private ConnectionState(
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
                "ConnectionState(node=%s, state=%s, lastSendTimeMs=%d, lastFailTimeMs=%d, inFlightCorrelationId=%d)",
                node,
                state,
                lastSendTimeMs,
                lastFailTimeMs,
                inFlightCorrelationId
            );
        }
    }
}
