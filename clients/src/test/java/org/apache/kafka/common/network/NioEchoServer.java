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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;

/**
 * Non-blocking EchoServer implementation that uses ChannelBuilder to create channels
 * with the configured security protocol.
 *
 */
public class NioEchoServer extends Thread {
    private final int port;
    private final ServerSocketChannel serverSocketChannel;
    private final List<SocketChannel> newChannels;
    private final List<SocketChannel> socketChannels;
    private final AcceptorThread acceptorThread;
    private final Selector selector;
    private final ConcurrentLinkedQueue<NetworkSend> inflightSends = new ConcurrentLinkedQueue<NetworkSend>();

    public NioEchoServer(SecurityProtocol securityProtocol, Map<String, ?> configs, String serverHost) throws Exception {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(new InetSocketAddress(serverHost, 0));
        this.port = serverSocketChannel.socket().getLocalPort();
        this.socketChannels = Collections.synchronizedList(new ArrayList<SocketChannel>());
        this.newChannels = Collections.synchronizedList(new ArrayList<SocketChannel>());
        ChannelBuilder channelBuilder = ChannelBuilders.create(securityProtocol, Mode.SERVER, LoginType.SERVER, configs, null, true);
        this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder);
        setName("echoserver");
        setDaemon(true);
        acceptorThread = new AcceptorThread();
    }

    public int port() {
        return port;
    }

    @Override
    public void run() {
        try {
            acceptorThread.start();
            while (serverSocketChannel.isOpen()) {
                selector.poll(1000);
                for (SocketChannel socketChannel : newChannels) {
                    String id = id(socketChannel);
                    selector.register(id, socketChannel);
                    socketChannels.add(socketChannel);
                }
                newChannels.clear();
                while (true) {
                    NetworkSend send = inflightSends.peek();
                    if (send != null && !selector.channel(send.destination()).hasSend()) {
                        send = inflightSends.poll();
                        selector.send(send);
                    } else
                        break;
                }
                List<NetworkReceive> completedReceives = selector.completedReceives();
                for (NetworkReceive rcv : completedReceives) {
                    NetworkSend send = new NetworkSend(rcv.source(), rcv.payload());
                    if (!selector.channel(send.destination()).hasSend())
                        selector.send(send);
                    else
                        inflightSends.add(send);
                }
            }
        } catch (IOException e) {
            // ignore
        }
    }

    private String id(SocketChannel channel) {
        return channel.socket().getLocalAddress().getHostAddress() + ":" + channel.socket().getLocalPort() + "-" +
                channel.socket().getInetAddress().getHostAddress() + ":" + channel.socket().getPort();
    }

    public void closeConnections() throws IOException {
        for (SocketChannel channel : socketChannels)
            channel.close();
        socketChannels.clear();
    }

    public void close() throws IOException, InterruptedException {
        this.serverSocketChannel.close();
        closeConnections();
        acceptorThread.interrupt();
        acceptorThread.join();
        interrupt();
        join();
    }

    private class AcceptorThread extends Thread {
        public AcceptorThread() throws IOException {
            setName("acceptor");
        }
        public void run() {
            try {
                java.nio.channels.Selector acceptSelector = java.nio.channels.Selector.open();
                serverSocketChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
                while (serverSocketChannel.isOpen()) {
                    if (acceptSelector.select(1000) > 0) {
                        Iterator<SelectionKey> it = acceptSelector.selectedKeys().iterator();
                        while (it.hasNext()) {
                            SelectionKey key = it.next();
                            if (key.isAcceptable()) {
                                SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
                                socketChannel.configureBlocking(false);
                                newChannels.add(socketChannel);
                                selector.wakeup();
                            }
                            it.remove();
                        }
                    }
                }
            } catch (IOException e) {
                // ignore
            }
        }
    }
}