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


import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Channel implements ScatteringByteChannel, GatheringByteChannel {
    private static final Logger log = LoggerFactory.getLogger(Channel.class);
    private TransportLayer transportLayer;
    private Authenticator authenticator;


    public Channel(TransportLayer transportLayer, Authenticator authenticator) throws IOException {
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
    }

    public void close() throws IOException {
        transportLayer.close();
        authenticator.close();
    }

    /**
     * returns user principal for the session
     * In case of PLAINTEXT and No Authentication returns ANONYMOUS as the userPrincipal
     * If SSL used without any SASL Authentication returns SSLSession.peerPrincipal
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    public void connect() throws IOException {
        if (transportLayer.isReady() && authenticator.isComplete())
            return;
        if (!transportLayer.isReady())
            transportLayer.handshake();
        if (transportLayer.isReady() && !authenticator.isComplete())
            authenticator.authenticate();
    }

    public void disconnect() {
        transportLayer.disconnect();
    }

    public boolean isOpen() {
        return transportLayer.socketChannel().isOpen();
    }

    public SocketChannel socketChannel() {
        return transportLayer.socketChannel();
    }

    public TransportLayer transportLayer() {
        return transportLayer;
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        return transportLayer.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return transportLayer.write(srcs);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return transportLayer.write(srcs, offset, length);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return transportLayer.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return transportLayer.read(dsts);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return transportLayer.read(dsts, offset, length);
    }

    public void finishConnect() throws IOException {
        transportLayer.finishConnect();
    }

    public void addInterestOps(int ops) {
        transportLayer.addInterestOps(ops);
    }

    public void removeInterestOps(int ops) {
        transportLayer.removeInterestOps(ops);
    }

    public void mute() {
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute() {
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean isReady() {
        return transportLayer.isReady() && authenticator.isComplete();
    }

    public DataInputStream getInputStream() throws IOException {
        return transportLayer.inStream();
    }

    public DataOutputStream getOutputStream() throws IOException {
        return transportLayer.outStream();
    }
}
