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
 * Transport layer for underlying communication
 */
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import javax.net.ssl.SSLSession;
import java.security.Principal;


public interface TransportLayer {

    /**
     * Closes this channel
     *
     * @throws IOException If and I/O error occurs
     */
    void close() throws IOException;


    /**
     * Tells wheather or not this channel is open.
     */
    boolean isOpen();

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     */
    int write(ByteBuffer src) throws IOException;

    long write(ByteBuffer[] srcs) throws IOException;

    long write(ByteBuffer[] srcs, int offset, int length) throws IOException;

    int read(ByteBuffer dst) throws IOException;

    long read(ByteBuffer[] dsts) throws IOException;

    long read(ByteBuffer[] dsts, int offset, int length) throws IOException;

    boolean isReady();

    boolean finishConnect() throws IOException;

    SocketChannel socketChannel();

    /**
     * Performs SSL handshake hence is a no-op for the non-secure
     * implementation
     * @param read Unused in non-secure implementation
     * @param write Unused in non-secure implementation
     * @return Always return 0
     * @throws IOException
    */
    int handshake(boolean read, boolean write) throws IOException;

    DataInputStream inStream() throws IOException;

    DataOutputStream outStream() throws IOException;

    boolean flush(ByteBuffer buffer) throws IOException;


    /**
     * returns SSLSession.getPeerPrinicpal if SSLTransportLayer used
     * for non-secure returns a "ANONYMOUS" as the peerPrincipal
     */
    Principal peerPrincipal() throws IOException;

    /**
     * returns a SSL Session after the handshake is established
     * throws IlleagalStateException if the handshake is not established
     * throws UnsupportedOperationException for non-secure implementation
     */
    SSLSession sslSession() throws IllegalStateException, UnsupportedOperationException;
}
