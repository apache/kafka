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

package org.apache.kafka.server;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;

public interface ServerSocketFactory {
    ServerSocketFactory INSTANCE = new KafkaServerSocketFactory();
    ServerSocketChannel openServerSocket(
        String listenerName,
        InetSocketAddress socketAddress,
        int listenBacklogSize,
        int recvBufferSize
    ) throws IOException;

    class KafkaServerSocketFactory implements ServerSocketFactory {

        @Override
        public ServerSocketChannel openServerSocket(
                String listenerName,
                InetSocketAddress socketAddress,
                int listenBacklogSize,
                int recvBufferSize
        ) throws IOException {
            ServerSocketChannel socketChannel = ServerSocketChannel.open();
            try {
                socketChannel.configureBlocking(false);
                if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) {
                    socketChannel.socket().setReceiveBufferSize(recvBufferSize);
                }
                socketChannel.socket().bind(socketAddress, listenBacklogSize);
            } catch (SocketException e) {
                Utils.closeQuietly(socketChannel, "server socket");
                throw new KafkaException(String.format("Socket server failed to bind to %s:%d: %s.",
                    socketAddress.getHostString(), socketAddress.getPort(), e.getMessage()), e);
            }
            return socketChannel;
        }
    }
}
