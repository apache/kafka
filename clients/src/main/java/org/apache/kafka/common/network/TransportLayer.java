/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.network;

/*
 * Transport layer for underlying communication.
 * At very basic level it is wrapper around SocketChannel and can be used as substitue for SocketChannel
 * and other network Channel implementations.
 * As NetworkClient replaces BlockingChannel and other implementations we will be using KafkaChannel as
 * a network I/O channel.
 */
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.GatheringByteChannel;

import java.security.Principal;


public interface TransportLayer extends ScatteringByteChannel, GatheringByteChannel {

    /**
     * Returns true if the channel has handshake and authentication done.
     */
    boolean ready();

    /**
     * Finishes the process of connecting a socket channel.
     */
    void finishConnect() throws IOException;

    /**
     * disconnect socketChannel
     */
    void disconnect();

    /**
     * Tells whether or not this channel's network socket is connected.
     */
    boolean isConnected();

    /**
     * returns underlying socketChannel
     */
    SocketChannel socketChannel();


    /**
     * Performs SSL handshake hence is a no-op for the non-secure
     * implementation
     * @throws IOException
    */
    void handshake() throws IOException;

    /**
     * Returns true if there are any pending writes
     */
    boolean hasPendingWrites();

    /**
     * returns SSLSession.getPeerPrinicpal if SSLTransportLayer used
     * for non-secure returns a "ANONYMOUS" as the peerPrincipal
     */
    Principal peerPrincipal() throws IOException;

    void addInterestOps(int ops);

    void removeInterestOps(int ops);

    boolean isMute();
}
