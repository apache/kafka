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
package org.apache.kafka.common.network;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.ssl.SslFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A simple server that takes size delimited byte arrays and just echos them back to the sender.
 */
class EchoServer extends Thread {
    public final int port;
    private final ServerSocket serverSocket;
    private final List<Thread> threads;
    private final List<Socket> sockets;
    private final SslFactory sslFactory;
    private final AtomicBoolean renegotiate = new AtomicBoolean();

    public EchoServer(SecurityProtocol securityProtocol, Map<String, ?> configs) throws Exception {
        switch (securityProtocol) {
            case SSL:
                this.sslFactory = new SslFactory(Mode.SERVER);
                this.sslFactory.configure(configs);
                SSLContext sslContext = this.sslFactory.sslContext();
                this.serverSocket = sslContext.getServerSocketFactory().createServerSocket(0);
                break;
            case PLAINTEXT:
                this.serverSocket = new ServerSocket(0);
                this.sslFactory = null;
                break;
            default:
                throw new IllegalArgumentException("Unsupported securityProtocol " + securityProtocol);
        }
        this.port = this.serverSocket.getLocalPort();
        this.threads = Collections.synchronizedList(new ArrayList<Thread>());
        this.sockets = Collections.synchronizedList(new ArrayList<Socket>());
    }

    public void renegotiate() {
        renegotiate.set(true);
    }

    @Override
    public void run() {
        try {
            while (true) {
                final Socket socket = serverSocket.accept();
                sockets.add(socket);
                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            DataInputStream input = new DataInputStream(socket.getInputStream());
                            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                            while (socket.isConnected() && !socket.isClosed()) {
                                int size = input.readInt();
                                if (renegotiate.get()) {
                                    renegotiate.set(false);
                                    ((SSLSocket) socket).startHandshake();
                                }
                                byte[] bytes = new byte[size];
                                input.readFully(bytes);
                                output.writeInt(size);
                                output.write(bytes);
                                output.flush();
                            }
                        } catch (IOException e) {
                            // ignore
                        } finally {
                            try {
                                socket.close();
                            } catch (IOException e) {
                                // ignore
                            }
                        }
                    }
                };
                thread.start();
                threads.add(thread);
            }
        } catch (IOException e) {
            // ignore
        }
    }

    public void closeConnections() throws IOException {
        for (Socket socket : sockets)
            socket.close();
    }

    public void close() throws IOException, InterruptedException {
        this.serverSocket.close();
        closeConnections();
        for (Thread t : threads)
            t.join();
        join();
    }
}
