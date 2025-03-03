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

package org.apache.kafka.common.test;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.ServerSocketFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class PreboundSocketFactoryManager implements AutoCloseable {

    private class PreboundSocketFactory implements ServerSocketFactory {
        private final int nodeId;

        private PreboundSocketFactory(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public ServerSocketChannel openServerSocket(
                String listenerName,
                InetSocketAddress socketAddress,
                int listenBacklogSize,
                int recvBufferSize
        ) throws IOException {
            ServerSocketChannel socketChannel = getSocketForListenerAndMarkAsUsed(
                nodeId,
                listenerName);
            if (socketChannel != null) {
                return socketChannel;
            }
            return ServerSocketFactory.INSTANCE.openServerSocket(
                listenerName,
                socketAddress,
                listenBacklogSize,
                recvBufferSize);
        }
    }

    /**
     * True if this manager is closed.
     */
    private boolean closed = false;

    /**
     * Maps node IDs to socket factory objects.
     * Protected by the object lock.
     */
    private final Map<Integer, PreboundSocketFactory> factories = new HashMap<>();

    /**
     * Maps node IDs to maps of listener names to ports.
     * Protected by the object lock.
     */
    private final Map<Integer, Map<String, ServerSocketChannel>> sockets = new HashMap<>();

    /**
     * Maps node IDs to set of the listeners that were used.
     * Protected by the object lock.
     */
    private final Map<Integer, Set<String>> usedSockets = new HashMap<>();

    /**
     * Get a socket from this manager, mark it as used, and return it.
     *
     * @param nodeId        The ID of the node.
     * @param listener      The listener for the socket.
     *
     * @return              null if the socket was not found; the socket, otherwise.
     */
    public synchronized ServerSocketChannel getSocketForListenerAndMarkAsUsed(
        int nodeId,
        String listener
    ) {
        Map<String, ServerSocketChannel> socketsForNode = sockets.get(nodeId);
        if (socketsForNode == null) {
            return null;
        }
        ServerSocketChannel socket = socketsForNode.get(listener);
        if (socket == null) {
            return null;
        }
        usedSockets.computeIfAbsent(nodeId, __ -> new HashSet<>()).add(listener);
        return socket;
    }

    /**
     * Get or create a socket factory object associated with a given node ID.
     *
     * @param nodeId        The ID of the node.
     *
     * @return              The socket factory.
     */
    public synchronized ServerSocketFactory getOrCreateSocketFactory(int nodeId) {
        return factories.computeIfAbsent(nodeId, __ -> new PreboundSocketFactory(nodeId));
    }

    /**
     * Get a specific port number. The port will be created if it does not already exist.
     *
     * @param nodeId        The ID of the node.
     * @param listener      The listener for the socket.
     *
     * @return              The port number.
     */
    public synchronized int getOrCreatePortForListener(
        int nodeId,
        String listener
    ) throws IOException {
        Map<String, ServerSocketChannel> socketsForNode =
            sockets.computeIfAbsent(nodeId, __ -> new HashMap<>());
        ServerSocketChannel socketChannel = socketsForNode.get(listener);
        if (socketChannel == null) {
            if (closed) {
                throw new RuntimeException("Cannot open new socket: manager is closed.");
            }
            socketChannel = ServerSocketFactory.INSTANCE.openServerSocket(
                listener,
                new InetSocketAddress(0),
                -1,
                -1);
            socketsForNode.put(listener, socketChannel);
        }
        InetSocketAddress socketAddress = (InetSocketAddress) socketChannel.getLocalAddress();
        return socketAddress.getPort();
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        // Close all sockets that haven't been used by a SocketServer. (We don't want to close the
        // ones that have been used by a SocketServer because that is the responsibility of that
        // SocketServer.)
        for (Entry<Integer, Map<String, ServerSocketChannel>> socketsEntry : sockets.entrySet()) {
            Set<String> usedListeners = usedSockets.getOrDefault(
                socketsEntry.getKey(), Collections.emptySet());
            for (Entry<String, ServerSocketChannel> entry : socketsEntry.getValue().entrySet()) {
                if (!usedListeners.contains(entry.getKey())) {
                    Utils.closeQuietly(entry.getValue(), "serverSocketChannel");
                }
            }
        }
    }
}
