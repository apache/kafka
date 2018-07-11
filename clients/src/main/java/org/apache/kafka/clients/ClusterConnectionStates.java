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
package org.apache.kafka.clients;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.errors.AuthenticationException;

import java.util.HashMap;
import java.util.Map;

/**
 * The state of our connection to each node in the cluster.
 *
 */
final class ClusterConnectionStates {
    private final long reconnectBackoffInitMs;
    private final long reconnectBackoffMaxMs;
    private final static int RECONNECT_BACKOFF_EXP_BASE = 2;
    private final double reconnectBackoffMaxExp;
    private final Map<String, NodeConnectionState> nodeState;

    public ClusterConnectionStates(long reconnectBackoffMs, long reconnectBackoffMaxMs) {
        this.reconnectBackoffInitMs = reconnectBackoffMs;
        this.reconnectBackoffMaxMs = reconnectBackoffMaxMs;
        this.reconnectBackoffMaxExp = Math.log(this.reconnectBackoffMaxMs / (double) Math.max(reconnectBackoffMs, 1)) / Math.log(RECONNECT_BACKOFF_EXP_BASE);
        this.nodeState = new HashMap<>();
    }

    /**
     * Return true iff we can currently initiate a new connection. This will be the case if we are not
     * connected and haven't been connected for at least the minimum reconnection backoff period.
     * @param id the connection id to check
     * @param now the current time in ms
     * @return true if we can initiate a new connection
     */
    public boolean canConnect(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null)
            return true;
        else
            return state.state.isDisconnected() &&
                   now - state.lastConnectAttemptMs >= state.reconnectBackoffMs;
    }

    /**
     * Return true if we are disconnected from the given node and can't re-establish a connection yet.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public boolean isBlackedOut(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null)
            return false;
        else
            return state.state.isDisconnected() &&
                   now - state.lastConnectAttemptMs < state.reconnectBackoffMs;
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long connectionDelay(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null) return 0;
        long timeWaited = now - state.lastConnectAttemptMs;
        if (state.state.isDisconnected()) {
            return Math.max(state.reconnectBackoffMs - timeWaited, 0);
        } else {
            // When connecting or connected, we should be able to delay indefinitely since other events (connection or
            // data acked) will cause a wakeup once data can be sent.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Return true if a specific connection establishment is currently underway
     * @param id The id of the node to check
     */
    public boolean isConnecting(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state == ConnectionState.CONNECTING;
    }

    /**
     * Enter the connecting state for the given connection.
     * @param id the id of the connection
     * @param now the current time
     */
    public void connecting(String id, long now) {
        if (nodeState.containsKey(id)) {
            NodeConnectionState node = nodeState.get(id);
            node.lastConnectAttemptMs = now;
            node.state = ConnectionState.CONNECTING;
        } else {
            nodeState.put(id, new NodeConnectionState(ConnectionState.CONNECTING, now,
                this.reconnectBackoffInitMs));
        }
    }

    /**
     * Enter the disconnected state for the given node.
     * @param id the connection we have disconnected
     * @param now the current time
     */
    public void disconnected(String id, long now) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.DISCONNECTED;
        nodeState.lastConnectAttemptMs = now;
        updateReconnectBackoff(nodeState);
    }

    /**
     * Indicate that the connection is throttled until the specified deadline.
     * @param id the connection to be throttled
     * @param throttleUntilTimeMs the throttle deadline in milliseconds
     */
    public void throttle(String id, long throttleUntilTimeMs) {
        NodeConnectionState state = nodeState.get(id);
        // The throttle deadline should never regress.
        if (state != null && state.throttleUntilTimeMs < throttleUntilTimeMs) {
            state.throttleUntilTimeMs = throttleUntilTimeMs;
        }
    }

    /**
     * Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long throttleDelayMs(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state != null && state.throttleUntilTimeMs > now) {
            return state.throttleUntilTimeMs - now;
        } else {
            return 0;
        }
    }

    /**
     * Return the number of milliseconds to wait, based on the connection state and the throttle time, before
     * attempting to send data. If the connection has been established but being throttled, return throttle delay.
     * Otherwise, return connection delay.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long pollDelayMs(String id, long now) {
        long throttleDelayMs = throttleDelayMs(id, now);
        if (isConnected(id) && throttleDelayMs > 0) {
            return throttleDelayMs;
        } else {
            return connectionDelay(id, now);
        }
    }

    /**
     * Enter the checking_api_versions state for the given node.
     * @param id the connection identifier
     */
    public void checkingApiVersions(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.CHECKING_API_VERSIONS;
    }

    /**
     * Enter the ready state for the given node.
     * @param id the connection identifier
     */
    public void ready(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.READY;
        nodeState.authenticationException = null;
        resetReconnectBackoff(nodeState);
    }

    /**
     * Enter the authentication failed state for the given node.
     * @param id the connection identifier
     * @param now the current time
     * @param exception the authentication exception
     */
    public void authenticationFailed(String id, long now, AuthenticationException exception) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.authenticationException = exception;
        nodeState.state = ConnectionState.AUTHENTICATION_FAILED;
        nodeState.lastConnectAttemptMs = now;
        updateReconnectBackoff(nodeState);
    }

    /**
     * Return true if the connection is in the READY state and currently not throttled.
     *
     * @param id the connection identifier
     * @param now the current time
     */
    public boolean isReady(String id, long now) {
        return isReady(nodeState.get(id), now);
    }

    private boolean isReady(NodeConnectionState state, long now) {
        return state != null && state.state == ConnectionState.READY && state.throttleUntilTimeMs <= now;
    }

    /**
     * Return true if there is at least one node with connection in the READY state and not throttled. Returns false
     * otherwise.
     *
     * @param now the current time
     */
    public boolean hasReadyNodes(long now) {
        for (Map.Entry<String, NodeConnectionState> entry : nodeState.entrySet()) {
            if (isReady(entry.getValue(), now)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if the connection has been established
     * @param id The id of the node to check
     */
    public boolean isConnected(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state.isConnected();
    }

    /**
     * Return true if the connection has been disconnected
     * @param id The id of the node to check
     */
    public boolean isDisconnected(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state.isDisconnected();
    }

    /**
     * Return authentication exception if an authentication error occurred
     * @param id The id of the node to check
     */
    public AuthenticationException authenticationException(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null ? state.authenticationException : null;
    }

    /**
     * Resets the failure count for a node and sets the reconnect backoff to the base
     * value configured via reconnect.backoff.ms
     *
     * @param nodeState The node state object to update
     */
    private void resetReconnectBackoff(NodeConnectionState nodeState) {
        nodeState.failedAttempts = 0;
        nodeState.reconnectBackoffMs = this.reconnectBackoffInitMs;
    }

    /**
     * Update the node reconnect backoff exponentially.
     * The delay is reconnect.backoff.ms * 2**(failures - 1) * (+/- 20% random jitter)
     * Up to a (pre-jitter) maximum of reconnect.backoff.max.ms
     *
     * @param nodeState The node state object to update
     */
    private void updateReconnectBackoff(NodeConnectionState nodeState) {
        if (this.reconnectBackoffMaxMs > this.reconnectBackoffInitMs) {
            nodeState.failedAttempts += 1;
            double backoffExp = Math.min(nodeState.failedAttempts - 1, this.reconnectBackoffMaxExp);
            double backoffFactor = Math.pow(RECONNECT_BACKOFF_EXP_BASE, backoffExp);
            long reconnectBackoffMs = (long) (this.reconnectBackoffInitMs * backoffFactor);
            // Actual backoff is randomized to avoid connection storms.
            double randomFactor = ThreadLocalRandom.current().nextDouble(0.8, 1.2);
            nodeState.reconnectBackoffMs = (long) (randomFactor * reconnectBackoffMs);
        }
    }

    /**
     * Remove the given node from the tracked connection states. The main difference between this and `disconnected`
     * is the impact on `connectionDelay`: it will be 0 after this call whereas `reconnectBackoffMs` will be taken
     * into account after `disconnected` is called.
     *
     * @param id the connection to remove
     */
    public void remove(String id) {
        nodeState.remove(id);
    }

    /**
     * Get the state of a given connection.
     * @param id the id of the connection
     * @return the state of our connection
     */
    public ConnectionState connectionState(String id) {
        return nodeState(id).state;
    }

    /**
     * Get the state of a given node.
     * @param id the connection to fetch the state for
     */
    private NodeConnectionState nodeState(String id) {
        NodeConnectionState state = this.nodeState.get(id);
        if (state == null)
            throw new IllegalStateException("No entry found for connection " + id);
        return state;
    }

    /**
     * The state of our connection to a node.
     */
    private static class NodeConnectionState {

        ConnectionState state;
        AuthenticationException authenticationException;
        long lastConnectAttemptMs;
        long failedAttempts;
        long reconnectBackoffMs;
        // Connection is being throttled if current time < throttleUntilTimeMs.
        long throttleUntilTimeMs;

        public NodeConnectionState(ConnectionState state, long lastConnectAttempt, long reconnectBackoffMs) {
            this.state = state;
            this.authenticationException = null;
            this.lastConnectAttemptMs = lastConnectAttempt;
            this.failedAttempts = 0;
            this.reconnectBackoffMs = reconnectBackoffMs;
            this.throttleUntilTimeMs = 0;
        }

        public String toString() {
            return "NodeState(" + state + ", " + lastConnectAttemptMs + ", " + failedAttempts + ", " + throttleUntilTimeMs + ")";
        }
    }
}
