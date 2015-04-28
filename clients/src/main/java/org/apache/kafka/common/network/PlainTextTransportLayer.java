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
 * Transport layer for PLAINTEXT communication
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import java.security.Principal;
import com.sun.security.auth.UserPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainTextTransportLayer implements TransportLayer {
    private static final Logger log = LoggerFactory.getLogger(PlainTextTransportLayer.class);
    SocketChannel socketChannel = null;
    DataInputStream inStream = null;
    DataOutputStream outStream = null;

    public PlainTextTransportLayer(SocketChannel socketChannel) throws IOException {
        this.socketChannel = socketChannel;

    }


    /**
     * Closes this channel
     *
     * @throws IOException If and I/O error occurs
     */
    public void close() throws IOException {
        socketChannel.socket().close();
        socketChannel.close();
    }

    /**
    * Flushes the buffer to the network, non blocking
    * @param buf ByteBuffer
    * @return boolean true if the buffer has been emptied out, false otherwise
    * @throws IOException
    */
    public boolean flush(ByteBuffer buf) throws IOException {
        int remaining = buf.remaining();
        if (remaining > 0) {
            int written = socketChannel.write(buf);
            return written >= remaining;
        }
        return true;
    }

    /**
     * Tells wheter or not this channel is open.
     */
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     */
    public int write(ByteBuffer src) throws IOException {
        return socketChannel.write(src);
    }

    public long write(ByteBuffer[] srcs) throws IOException {
        return socketChannel.write(srcs);
    }

    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return socketChannel.write(srcs, offset, length);
    }

    public int read(ByteBuffer dst) throws IOException {
        return socketChannel.read(dst);
    }

    public long read(ByteBuffer[] dsts) throws IOException {
        return socketChannel.read(dsts);
    }

    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return socketChannel.read(dsts, offset, length);
    }

    public boolean isReady() {
        return true;
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    public boolean finishConnect() throws IOException {
        return socketChannel.finishConnect();
    }

    /**
     * Performs SSL handshake hence is a no-op for the non-secure
     * implementation
     * @param read Unused in non-secure implementation
     * @param write Unused in non-secure implementation
     * @return Always return 0
     * @throws IOException
    */
    public int handshake(boolean read, boolean write) throws IOException {
        return 0;
    }

    public DataInputStream inStream() throws IOException {
        if (inStream == null)
            this.inStream = new DataInputStream(socketChannel.socket().getInputStream());
        return inStream;
    }

    public DataOutputStream outStream() throws IOException {
        if (outStream == null)
         this.outStream = new DataOutputStream(socketChannel.socket().getOutputStream());
        return outStream;
    }

    public Principal getPeerPrincipal() {
        return new UserPrincipal("ANONYMOUS");
    }

}
