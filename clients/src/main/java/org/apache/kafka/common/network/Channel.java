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

import java.net.Socket;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Channel {
    private static final Logger log = LoggerFactory.getLogger(Channel.class);
    private final String id;
    public TransportLayer transportLayer;
    private Authenticator authenticator;
    private NetworkReceive receive;
    private Send send;
    private int maxReceiveSize;

    public Channel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
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

    /**
     * Does handshake of transportLayer and Authentication using configured authenticator
     */
    public void prepare() throws IOException {
        if (transportLayer.ready() && authenticator.complete())
            return;
        if (!transportLayer.ready())
            transportLayer.handshake();
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        transportLayer.disconnect();
    }


    public void finishConnect() throws IOException {
        transportLayer.finishConnect();
    }

    public String id() {
        return id;
    }

    public void mute() {
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute() {
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket == null)
            return "[unconnected socket]";
        else if (socket.getInetAddress() != null)
            return socket.getInetAddress().toString();
        else
            return socket.getLocalAddress().toString();
    }

    public void setSend(Send send) {
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        this.send = send;
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }

        long x = receive(receive);
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        return result;
    }

    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    private long receive(NetworkReceive receive) throws IOException {
        long result = receive.readFrom(transportLayer);
        return result;
    }

    private boolean send(Send send) throws IOException {
        send.writeTo(transportLayer);
        if (send.completed()) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
        }
        return send.completed();
    }

}
