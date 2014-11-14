/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import java.util.HashMap;
import java.util.Map;

/**
 * The state of our connection to each node in the cluster.
 * 
 */
final class ClusterConnectionStates {
    private final long reconnectBackoffMs;
    private final Map<Integer, NodeConnectionState> nodeState;

    public ClusterConnectionStates(long reconnectBackoffMs) {
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.nodeState = new HashMap<Integer, NodeConnectionState>();
    }

    /**
     * Return true iff we can currently initiate a new connection to the given node. This will be the case if we are not
     * connected and haven't been connected for at least the minimum reconnection backoff period.
     * @param node The node id to check
     * @param now The current time in MS
     * @return true if we can initiate a new connection
     */
    public boolean canConnect(int node, long now) {
        NodeConnectionState state = nodeState.get(node);
        if (state == null)
            return true;
        else
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs >= this.reconnectBackoffMs;
    }

    /**
     * Return true if we are disconnected from the given node and can't re-establish a connection yet
     * @param node The node to check
     * @param now The current time in ms
     */
    public boolean isBlackedOut(int node, long now) {
        NodeConnectionState state = nodeState.get(node);
        if (state == null)
            return false;
        else
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs < this.reconnectBackoffMs;
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     * @param node The node to check
     * @param now The current time in ms
     */
    public long connectionDelay(int node, long now) {
        NodeConnectionState state = nodeState.get(node);
        if (state == null) return 0;
        long timeWaited = now - state.lastConnectAttemptMs;
        if (state.state == ConnectionState.DISCONNECTED) {
            return Math.max(this.reconnectBackoffMs - timeWaited, 0);
        }
        else {
            // When connecting or connected, we should be able to delay indefinitely since other events (connection or
            // data acked) will cause a wakeup once data can be sent.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Enter the connecting state for the given node.
     * @param node The id of the node we are connecting to
     * @param now The current time.
     */
    public void connecting(int node, long now) {
        nodeState.put(node, new NodeConnectionState(ConnectionState.CONNECTING, now));
    }

    /**
     * Return true iff we have a connection to the give node
     * @param node The id of the node to check
     */
    public boolean isConnected(int node) {
        NodeConnectionState state = nodeState.get(node);
        return state != null && state.state == ConnectionState.CONNECTED;
    }

    /**
     * Return true iff we are in the process of connecting to the given node
     * @param node The id of the node
     */
    public boolean isConnecting(int node) {
        NodeConnectionState state = nodeState.get(node);
        return state != null && state.state == ConnectionState.CONNECTING;
    }

    /**
     * Enter the connected state for the given node
     * @param node The node we have connected to
     */
    public void connected(int node) {
        nodeState(node).state = ConnectionState.CONNECTED;
    }

    /**
     * Enter the disconnected state for the given node
     * @param node The node we have disconnected from
     */
    public void disconnected(int node) {
        nodeState(node).state = ConnectionState.DISCONNECTED;
    }

    /**
     * Get the state of our connection to the given state
     * @param node The id of the node
     * @return The state of our connection
     */
    private NodeConnectionState nodeState(int node) {
        NodeConnectionState state = this.nodeState.get(node);
        if (state == null)
            throw new IllegalStateException("No entry found for node " + node);
        return state;
    }
}