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

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

public class ConnectionCache {
    private final Map<Integer, ConnectionState> connections = new HashMap<>();
    private final Map<Integer, ConnectionState> bootstrapConnections = new HashMap<>();

    private final NetworkChannel channel;
    private final Logger log;
    private final int retryBackoffMs;
    private final int requestTimeoutMs;

    public ConnectionCache(NetworkChannel channel,
                           List<InetSocketAddress> bootstrapServers,
                           int retryBackoffMs,
                           int requestTimeoutMs,
                           LogContext logContext) {
        this.channel = channel;
        this.log = logContext.logger(ConnectionCache.class);
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;

        // Mimic logic in `Cluster.bootstrap` until we think of something smarter
        int nodeId = -1;
        for (InetSocketAddress address : bootstrapServers) {
            ConnectionState connection = new ConnectionState(nodeId);
            connection.maybeUpdate(new HostInfo(address, 0L));
            bootstrapConnections.put(nodeId, connection);
            channel.updateEndpoint(nodeId, address);
            nodeId--;
        }
    }

    public ConnectionState getOrCreate(int id) {
        if (id < 0) {
            return bootstrapConnections.get(id);
        } else {
            return connections.computeIfAbsent(id, key -> new ConnectionState(id));
        }
    }

    public OptionalInt findReadyBootstrapServer(long currentTimeMs) {
        // TODO: This logic is important. We need something smarter.
        for (Map.Entry<Integer, ConnectionState> connectionEntry : bootstrapConnections.entrySet()) {
            int nodeId = connectionEntry.getKey();
            ConnectionState connection = connectionEntry.getValue();
            if (connection.isReady(currentTimeMs)) {
                return OptionalInt.of(nodeId);
            } else if (connection.inFlightCorrelationId.isPresent()) {
                return OptionalInt.empty();
            }
        }
        return OptionalInt.empty();
    }

    public boolean hasUnknownVoterEndpoints() {
        return connections.values().stream().anyMatch(cxn -> cxn.hostInfo == null);
    }

    public HostInfo maybeUpdate(int id, HostInfo update) {
        ConnectionState connectionState = getOrCreate(id);

        if (connectionState.maybeUpdate(update)) {
            channel.updateEndpoint(id, update.address);
        }

        return connectionState.hostInfo;
    }

    public Map<Integer, Optional<HostInfo>> allVoters() {
        return connections.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> entry.getKey(),
                entry -> entry.getValue().hostInfo()));
    }

    public void resetAll() {
        for (ConnectionState connectionState : connections.values())
            connectionState.reset();
    }

    public static class HostInfo {
        public final InetSocketAddress address;
        public final long bootTimestamp;

        public HostInfo(InetSocketAddress address, long bootTimestamp) {
            this.address = address;
            this.bootTimestamp = bootTimestamp;
        }

        @Override
        public String toString() {
            return "HostInfo(" +
                "address=" + address +
                ", bootTimestamp=" + bootTimestamp +
                ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HostInfo hostInfo = (HostInfo) o;
            return bootTimestamp == hostInfo.bootTimestamp &&
                Objects.equals(address, hostInfo.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, bootTimestamp);
        }
    }

    private enum State {
        AWAITING_REQUEST,
        BACKING_OFF,
        READY
    }

    public class ConnectionState {
        private final long id;
        private HostInfo hostInfo = null;
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

        Optional<HostInfo> hostInfo() {
            return Optional.ofNullable(hostInfo);
        }

        boolean isReady(long timeMs) {
            if (hostInfo == null) {
                return false;
            } else if (isBackoffComplete(timeMs) || hasRequestTimedOut(timeMs)) {
                state = State.READY;
            }

            return state == State.READY;
        }

        boolean maybeUpdate(HostInfo update) {
            if (hostInfo == null || hostInfo.bootTimestamp < update.bootTimestamp) {
                hostInfo = update;
                reset();
                log.info("Update connection info for node {} to {}", id, hostInfo);
                return true;
            }
            return false;
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

        void onResponseReceived(long correlationId) {
            inFlightCorrelationId.ifPresent(inflightRequestId -> {
                if (inflightRequestId == correlationId) {
                    state = State.READY;
                    inFlightCorrelationId = Optional.empty();
                }
            });
        }

        void onResponse(long correlationId, Errors error, long timeMs) {
            if (error != Errors.NONE) {
                onResponseError(correlationId, timeMs);
            } else {
                onResponseReceived(correlationId);
            }
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
    }

}
