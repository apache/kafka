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

import org.apache.kafka.common.utils.LeakTester;
import org.apache.kafka.common.utils.PredicateLeakTester;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ProtocolFamily;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;

/**
 * A {@link LeakTester} which attaches to the static {@link NetworkContext} to automatically register sockets created
 * by Kafka clients and servers for leak testing. Only one instance of this object may be open concurrently, so tests
 * running concurrently should share one instance.
 */
public class NetworkContextLeakTester implements LeakTester, ExtensionContext.Store.CloseableResource {

    private final LeakTester tester;
    private final Utils.UncheckedCloseable context;

    public NetworkContextLeakTester() {
        RecordingSelectorProvider selectorProvider = new RecordingSelectorProvider();
        RecordingSocketFactory socketFactory = new RecordingSocketFactory();
        RecordingServerSocketFactory serverSocketFactory = new RecordingServerSocketFactory();
        tester = LeakTester.combine(
                selectorProvider.datagramTester,
                selectorProvider.selectorTester,
                selectorProvider.pipeTester,
                selectorProvider.socketChannelTester,
                selectorProvider.serverSocketTester,
                socketFactory.socketTester,
                serverSocketFactory.serverSocketTester
        );
        context = NetworkContext.install(selectorProvider, socketFactory, serverSocketFactory);
    }

    public LeakTest start() {
        return tester.start();
    }

    @Override
    public void close() {
        context.close();
    }

    private static class RecordingSelectorProvider extends SelectorProvider {

        private final SelectorProvider selectorProvider;
        private final PredicateLeakTester<DatagramChannel> datagramTester;
        private final PredicateLeakTester<Pipe> pipeTester;
        private final PredicateLeakTester<AbstractSelector> selectorTester;
        private final PredicateLeakTester<ServerSocketChannel> serverSocketTester;
        private final PredicateLeakTester<SocketChannel> socketChannelTester;

        private RecordingSelectorProvider() {
            selectorProvider = SelectorProvider.provider();
            datagramTester = new PredicateLeakTester<>(AbstractInterruptibleChannel::isOpen, DatagramChannel.class);
            pipeTester = new PredicateLeakTester<>(pipe -> pipe.source().isOpen() || pipe.sink().isOpen(), Pipe.class);
            selectorTester = new PredicateLeakTester<>(Selector::isOpen, AbstractSelector.class);
            serverSocketTester = new PredicateLeakTester<>(AbstractInterruptibleChannel::isOpen, ServerSocketChannel.class);
            socketChannelTester = new PredicateLeakTester<>(AbstractInterruptibleChannel::isOpen, SocketChannel.class);
        }

        @Override
        public DatagramChannel openDatagramChannel() throws IOException {
            return datagramTester.open(selectorProvider.openDatagramChannel());
        }

        @Override
        public DatagramChannel openDatagramChannel(ProtocolFamily family) throws IOException {
            return datagramTester.open(selectorProvider.openDatagramChannel(family));
        }

        @Override
        public Pipe openPipe() throws IOException {
            return pipeTester.open(selectorProvider.openPipe());
        }

        @Override
        public AbstractSelector openSelector() throws IOException {
            return selectorTester.open(selectorProvider.openSelector());
        }

        @Override
        public ServerSocketChannel openServerSocketChannel() throws IOException {
            return serverSocketTester.open(selectorProvider.openServerSocketChannel());
        }

        @Override
        public SocketChannel openSocketChannel() throws IOException {
            return socketChannelTester.open(selectorProvider.openSocketChannel());
        }
    }

    private static class RecordingSocketFactory extends SocketFactory {

        private final SocketFactory factory;
        private final PredicateLeakTester<Socket> socketTester;

        private RecordingSocketFactory() {
            this.factory = SocketFactory.getDefault();
            socketTester = new PredicateLeakTester<>(socket -> !socket.isClosed(), Socket.class);
        }

        @Override
        public Socket createSocket() throws IOException {
            return socketTester.open(factory.createSocket());
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
            return socketTester.open(factory.createSocket(host, port));
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException, UnknownHostException {
            return socketTester.open(factory.createSocket(host, port, localHost, localPort));
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            return socketTester.open(factory.createSocket(host, port));
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            return socketTester.open(factory.createSocket(address, port, localAddress, localPort));
        }
    }

    private static class RecordingServerSocketFactory extends ServerSocketFactory {

        private final ServerSocketFactory factory;
        private final PredicateLeakTester<ServerSocket> serverSocketTester;

        private RecordingServerSocketFactory() {
            factory = ServerSocketFactory.getDefault();
            serverSocketTester = new PredicateLeakTester<>(serverSocket -> !serverSocket.isClosed(), ServerSocket.class);
        }

        @Override
        public ServerSocket createServerSocket(int port) throws IOException {
            return serverSocketTester.open(factory.createServerSocket(port));
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog) throws IOException {
            return serverSocketTester.open(factory.createServerSocket(port, backlog));
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress) throws IOException {
            return serverSocketTester.open(factory.createServerSocket(port, backlog, ifAddress));
        }
    }
}
