/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.List;



/**
 * A simple server that takes size delimited byte arrays and just echos them back to the sender.
 */
class EchoServer extends Thread {
    public final int port;
    private final ServerSocket serverSocket;
    private final List<Thread> threads;
    private final List<Socket> sockets;
    private SecurityProtocol protocol = SecurityProtocol.PLAINTEXT;
    private SSLFactory sslFactory;

    public EchoServer(Map<String, ?> configs) throws Exception {
        this.protocol =  configs.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) ?
            SecurityProtocol.valueOf((String) configs.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) : SecurityProtocol.PLAINTEXT;
        if (protocol == SecurityProtocol.SSL) {
            this.sslFactory = new SSLFactory(SSLFactory.Mode.SERVER);
            this.sslFactory.configure(configs);
            this.serverSocket = sslFactory.createSSLServerSocketFactory().createServerSocket(0);
        } else {
            this.serverSocket = new ServerSocket(0);
        }
        this.port = this.serverSocket.getLocalPort();
        this.threads = Collections.synchronizedList(new ArrayList<Thread>());
        this.sockets = Collections.synchronizedList(new ArrayList<Socket>());
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
