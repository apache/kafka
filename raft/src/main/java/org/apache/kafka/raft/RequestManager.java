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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;

public class RequestManager {
    private final Map<Integer, ConnectionState> connections = new HashMap<>();
    private final List<Integer> voters = new ArrayList<>();

    private final int retryBackoffMs;
    private final int requestTimeoutMs;
    private final Random random;

    public RequestManager(Set<Integer> voterIds,
                          int retryBackoffMs,
                          int requestTimeoutMs,
                          Random random) {

        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.voters.addAll(voterIds);
        this.random = random;

        for (Integer voterId: voterIds) {
            ConnectionState connection = new ConnectionState(voterId);
            connections.put(voterId, connection);
        }
    }

    public ConnectionState getOrCreate(int id) {
        return connections.computeIfAbsent(id, key -> new ConnectionState(id));
    }

    public OptionalInt findReadyVoter(long currentTimeMs) {
        int startIndex = random.nextInt(voters.size());
        OptionalInt res = OptionalInt.empty();
        for (int i = 0; i < voters.size(); i++) {
            int index = (startIndex + i) % voters.size();
            Integer voterId = voters.get(index);
            ConnectionState connection = connections.get(voterId);
            boolean isReady = connection.isReady(currentTimeMs);

            if (isReady) {
                res = OptionalInt.of(voterId);
            } else if (connection.inFlightCorrelationId.isPresent()) {
                res = OptionalInt.empty();
                break;
            }
        }
        return res;
    }

    public long backoffBeforeAvailableVoter(long currentTimeMs) {
        long minBackoffMs = Long.MAX_VALUE;
        for (Integer voterId : voters) {
            ConnectionState connection = connections.get(voterId);
            if (connection.isReady(currentTimeMs)) {
                return 0L;
            } else if (connection.isBackingOff(currentTimeMs)) {
                minBackoffMs = Math.min(minBackoffMs, connection.remainingBackoffMs(currentTimeMs));
            } else {
                minBackoffMs = Math.min(minBackoffMs, connection.remainingRequestTimeMs(currentTimeMs));
            }
        }
        return minBackoffMs;
    }

    public void resetAll() {
        for (ConnectionState connectionState : connections.values())
            connectionState.reset();
    }

    private enum State {
        AWAITING_REQUEST,
        BACKING_OFF,
        READY
    }

    public class ConnectionState {
        private final long id;
        private State state = State.READY;
        private long lastSendTimeMs = 0L;
        private long lastFailTimeMs = 0L;
        private Optional<Long> inFlightCorrelationId = Optional.empty();

        public ConnectionState(long id) {
            this.id = id;
        }

        private boolean isBackoffComplete(long timeMs) {
            return state == State.BACKING_OFF && timeMs >= lastFailTimeMs + retryBackoffMs;
        }

        boolean hasRequestTimedOut(long timeMs) {
            return state == State.AWAITING_REQUEST && timeMs >= lastSendTimeMs + requestTimeoutMs;
        }

        public long id() {
            return id;
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

        boolean hasInflightRequest(long timeMs) {
            if (state != State.AWAITING_REQUEST) {
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

        void onResponseError(long correlationId, long timeMs) {
            inFlightCorrelationId.ifPresent(inflightRequestId -> {
                if (inflightRequestId == correlationId) {
                    lastFailTimeMs = timeMs;
                    state = State.BACKING_OFF;
                    inFlightCorrelationId = Optional.empty();
                }
            });
        }

        void onResponseReceived(long correlationId, long timeMs) {
            inFlightCorrelationId.ifPresent(inflightRequestId -> {
                if (inflightRequestId == correlationId) {
                    state = State.READY;
                    inFlightCorrelationId = Optional.empty();
                }
            });
        }

        void onRequestSent(long correlationId, long timeMs) {
            lastSendTimeMs = timeMs;
            inFlightCorrelationId = Optional.of(correlationId);
            state = State.AWAITING_REQUEST;
        }

        /**
         * Ignore in-flight requests or backoff and become available immediately. This is used
         * when there is a state change which usually means in-flight requests are obsolete
         * and we need to send new requests.
         */
        void reset() {
            state = State.READY;
            inFlightCorrelationId = Optional.empty();
        }

        @Override
        public String toString() {
            return "ConnectionState(" +
                "id=" + id +
                ", state=" + state +
                ", lastSendTimeMs=" + lastSendTimeMs +
                ", lastFailTimeMs=" + lastFailTimeMs +
                ", inFlightCorrelationId=" + inFlightCorrelationId +
                ')';
        }
    }

}
