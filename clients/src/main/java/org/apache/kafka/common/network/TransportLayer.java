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

import java.security.Principal;


public interface TransportLayer {

    /**
     * Returns true if the channel has handshake and authenticaiton done.
     */
    boolean isReady();

    /**
     * calls internal socketChannel.finishConnect()
     */
    void finishConnect() throws IOException;

    /**
     * disconnect socketChannel
     */
    void disconnect();

    /**
     * returns underlying socketChannel
     */
    SocketChannel socketChannel();

    /**
     * returns true if socketchannel is open.
     */
    boolean isOpen();

    public void close() throws IOException;

    /**
     * returns true if there are any pending bytes needs to be written to channel.
     */
    boolean pending();

    /**
     * Performs SSL handshake hence is a no-op for the non-secure
     * implementation
     * @throws IOException
    */
    void handshake() throws IOException;


    /**
     * Reads sequence of bytes from the channel to given buffer
     */
    public int read(ByteBuffer dst) throws IOException;

    public long read(ByteBuffer[] dsts) throws IOException;

    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException;

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     */
    public int write(ByteBuffer src) throws IOException;

    public long write(ByteBuffer[] srcs) throws IOException;

    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException;



    /**
     * returns SSLSession.getPeerPrinicpal if SSLTransportLayer used
     * for non-secure returns a "ANONYMOUS" as the peerPrincipal
     */
    Principal peerPrincipal() throws IOException;

    void addInterestOps(int ops);

    void removeInterestOps(int ops);

}
