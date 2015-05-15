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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainTextTransportLayer implements TransportLayer {
    private static final Logger log = LoggerFactory.getLogger(PlainTextTransportLayer.class);
    private SelectionKey key;
    private SocketChannel socketChannel;
    private DataInputStream inStream;
    private DataOutputStream outStream;
    private final Principal principal = new KafkaPrincipal("ANONYMOUS");

    public PlainTextTransportLayer(SelectionKey key) throws IOException {
        this.key = key;
        this.socketChannel = (SocketChannel) key.channel();
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

    public void disconnect() {
        key.cancel();
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

    public void finishConnect() throws IOException {
        socketChannel.finishConnect();
        int ops = key.interestOps();
        ops &= ~SelectionKey.OP_CONNECT;
        ops |= SelectionKey.OP_READ;
        key.interestOps(ops);
    }

    /**
     * Performs SSL handshake hence is a no-op for the non-secure
     * implementation
     * @throws IOException
    */
    public void handshake() throws IOException {}

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

    public Principal peerPrincipal() throws IOException {
        return principal;
    }

    public void addInterestOps(int ops) {
        key.interestOps(key.interestOps() | ops);
    }

    public void removeInterestOps(int ops) {
        key.interestOps(key.interestOps() & ~ops);
    }

}
