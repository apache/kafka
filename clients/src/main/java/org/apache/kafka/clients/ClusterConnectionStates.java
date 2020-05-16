/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import java.util.HashMap;
import java.util.Map;

/**
 * The state of our connection to each node in the cluster.
 * 我们到集群中的每个节点连接的状态。
 */
final class ClusterConnectionStates {
    //重连间隔
    private final long reconnectBackoffMs;
    //节点状态
    private final Map<String, NodeConnectionState> nodeState;

    public ClusterConnectionStates(long reconnectBackoffMs) {
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.nodeState = new HashMap<String, NodeConnectionState>();
    }

    /**
     * 如果我们当前初始化一个新的连接返回true，如果我们没有连接并且至少在最小重新连接间隔时间段内没有连接，就会是这种情况。
     * Return true iff we can currently initiate a new connection. This will be the case if we are not
     * connected and haven't been connected for at least the minimum reconnection backoff period.
     *
     * @param id  The connection id to check
     * @param now The current time in MS
     * @return true if we can initiate a new connection
     */
    public boolean canConnect(String id, long now) {
        //拿到节点连接状态
        NodeConnectionState state = nodeState.get(id);
        //如果不存在可以建立连接
        if (state == null)
            return true;
        else
            //如果状态为没有连接并且失去连接时间大于重连间隔
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs >= this.reconnectBackoffMs;
    }

    /**
     * 如果给定节点断开连接或者不能重建连接就返回true
     * Return true if we are disconnected from the given node and can't re-establish a connection yet
     *
     * @param id  The connection to check
     * @param now The current time in ms
     */
    public boolean isBlackedOut(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null)
            return false;
        else
            //如果断开连接并且小于重连时间表示目前不能连接
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs < this.reconnectBackoffMs;
    }

    /**
     * 返回需要等待的毫秒时间，基于连接状态，在发送之前重试。当断开连接，重连的间隔时间，当连接或者已经连接，处理慢速/停滞连接。
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param id  The connection to check
     * @param now The current time in ms
     */
    public long connectionDelay(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        //等待时间0秒
        if (state == null) return 0;
        //计算出来等待时间
        long timeWaited = now - state.lastConnectAttemptMs;
        //如果已经连接就返回最大值
        if (state.state == ConnectionState.DISCONNECTED) {
            //得到重连的最大值
            return Math.max(this.reconnectBackoffMs - timeWaited, 0);
        } else {
            // When connecting or connected, we should be able to delay indefinitely since other events (connection or
            // data acked) will cause a wakeup once data can be sent.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Enter the connecting state for the given connection.
     *
     * @param id  The id of the connection
     * @param now The current time.
     */
    public void connecting(String id, long now) {
        //连接的节点存储到集合中
        nodeState.put(id, new NodeConnectionState(ConnectionState.CONNECTING, now));
    }

    /**
     * Return true iff a specific connection is connected
     *
     * @param id The id of the connection to check
     */
    public boolean isConnected(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state == ConnectionState.CONNECTED;
    }

    /**
     * Enter the connected state for the given connection
     *
     * @param id The connection identifier
     */
    public void connected(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.CONNECTED;
    }

    /**
     * Enter the disconnected state for the given node
     *
     * @param id  The connection we have disconnected
     * @param now The current time
     */
    public void disconnected(String id, long now) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.DISCONNECTED;
        nodeState.lastConnectAttemptMs = now;
    }

    /**
     * Remove the given node from the tracked connection states. The main difference between this and `disconnected`
     * is the impact on `connectionDelay`: it will be 0 after this call whereas `reconnectBackoffMs` will be taken
     * into account after `disconnected` is called.
     *
     * @param id The connection to remove
     */
    public void remove(String id) {
        nodeState.remove(id);
    }

    /**
     * Get the state of a given connection
     *
     * @param id The id of the connection
     * @return The state of our connection
     */
    public ConnectionState connectionState(String id) {
        return nodeState(id).state;
    }

    /**
     * Get the state of a given node
     *
     * @param id The connection to fetch the state for
     */
    private NodeConnectionState nodeState(String id) {
        NodeConnectionState state = this.nodeState.get(id);
        if (state == null)
            throw new IllegalStateException("No entry found for connection " + id);
        return state;
    }

    /**
     * The state of our connection to a node
     * 一个节点的连接状态信息
     */
    private static class NodeConnectionState {

        //连接状态
        ConnectionState state;
        //最后的连接重试时间
        long lastConnectAttemptMs;

        public NodeConnectionState(ConnectionState state, long lastConnectAttempt) {
            this.state = state;
            this.lastConnectAttemptMs = lastConnectAttempt;
        }

        public String toString() {
            return "NodeState(" + state + ", " + lastConnectAttemptMs + ")";
        }
    }
}
