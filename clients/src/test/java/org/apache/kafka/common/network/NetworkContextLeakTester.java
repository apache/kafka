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

import java.io.IOException;
import java.net.ProtocolFamily;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.function.Supplier;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * A {@link LeakTester} which attaches to the static {@link TestNetworkContext} to automatically register sockets
 * created by Kafka clients and servers for leak testing.
 */
public class NetworkContextLeakTester implements LeakTester {

    public NetworkContextLeakTester() {
    }

    public LeakTest start() {
        return RecordingNetworkDecorator.INSTANCE.start();
    }

    private static class RecordingNetworkDecorator implements TestNetworkContext.Decorator, LeakTester {

        private static final RecordingNetworkDecorator INSTANCE = TestNetworkContext.install(RecordingNetworkDecorator::new);

        private final LeakTester tester;
        private final RecordingSelectorProvider selectorProvider;
        private final RecordingSocketFactory socketFactory;
        private final RecordingServerSocketFactory serverSocketFactory;

        private RecordingNetworkDecorator(Supplier<TestNetworkContext.Decorator> inner) {
            selectorProvider = new RecordingSelectorProvider(() -> inner.get().provider());
            socketFactory = new RecordingSocketFactory(() -> inner.get().factory());
            serverSocketFactory = new RecordingServerSocketFactory(() -> inner.get().serverFactory());
            tester = LeakTester.combine(
                    selectorProvider.datagramTester,
                    selectorProvider.selectorTester,
                    selectorProvider.pipeTester,
                    selectorProvider.socketChannelTester,
                    selectorProvider.serverSocketTester,
                    socketFactory.socketTester,
                    serverSocketFactory.serverSocketTester,
                    serverSocketFactory.socketTester
            );
        }

        @Override
        public SelectorProvider provider() {
            return selectorProvider;
        }

        @Override
        public SocketFactory factory() {
            return socketFactory;
        }

        @Override
        public ServerSocketFactory serverFactory() {
            return serverSocketFactory;
        }

        @Override
        public LeakTest start() {
            return tester.start();
        }
    }

    private static class RecordingSelectorProvider extends TestNetworkContext.SelectorProviderDecorator {

        private final PredicateLeakTester<DatagramChannel> datagramTester;
        private final PredicateLeakTester<Pipe> pipeTester;
        private final PredicateLeakTester<AbstractSelector> selectorTester;
        private final PredicateLeakTester<ServerSocketChannel> serverSocketTester;
        private final PredicateLeakTester<SocketChannel> socketChannelTester;

        private RecordingSelectorProvider(Supplier<SelectorProvider> inner) {
            super(inner);
            datagramTester = new PredicateLeakTester<>(AbstractInterruptibleChannel::isOpen, DatagramChannel.class);
            pipeTester = new PredicateLeakTester<>(pipe -> pipe.source().isOpen() || pipe.sink().isOpen(), Pipe.class);
            selectorTester = new PredicateLeakTester<>(Selector::isOpen, AbstractSelector.class);
            serverSocketTester = new PredicateLeakTester<>(AbstractInterruptibleChannel::isOpen, ServerSocketChannel.class);
            socketChannelTester = new PredicateLeakTester<>(AbstractInterruptibleChannel::isOpen, SocketChannel.class);
        }

        @Override
        public DatagramChannel openDatagramChannel() throws IOException {
            return datagramTester.open(super.openDatagramChannel());
        }

        @Override
        public DatagramChannel openDatagramChannel(ProtocolFamily family) throws IOException {
            return datagramTester.open(super.openDatagramChannel(family));
        }

        @Override
        public Pipe openPipe() throws IOException {
            return pipeTester.open(super.openPipe());
        }

        @Override
        public AbstractSelector openSelector() throws IOException {
            return selectorTester.open(super.openSelector());
        }

        @Override
        public ServerSocketChannel openServerSocketChannel() throws IOException {
            ServerSocketChannel original = super.openServerSocketChannel();
            ServerSocketChannel spy = spy(original);
            try {
                doAnswer(invocation -> socketChannelTester.open((SocketChannel) invocation.callRealMethod())).when(spy).accept();
            } catch (IOException e) {
                return serverSocketTester.open(original);
            }
            return serverSocketTester.open(spy);
        }

        @Override
        public SocketChannel openSocketChannel() throws IOException {
            return socketChannelTester.open(super.openSocketChannel());
        }
    }

    private static class RecordingSocketFactory extends TestNetworkContext.SocketFactoryDecorator {

        private final PredicateLeakTester<Socket> socketTester;

        private RecordingSocketFactory(Supplier<SocketFactory> inner) {
            super(inner);
            socketTester = new PredicateLeakTester<>(socket -> !socket.isClosed(), Socket.class);
        }

        @Override
        public Socket intercept(Socket socket) {
            return socketTester.open(socket);
        }
    }

    private static class RecordingServerSocketFactory extends TestNetworkContext.ServerSocketFactoryDecorator {

        private final PredicateLeakTester<ServerSocket> serverSocketTester;
        private final PredicateLeakTester<Socket> socketTester;

        private RecordingServerSocketFactory(Supplier<ServerSocketFactory> inner) {
            super(inner);
            serverSocketTester = new PredicateLeakTester<>(serverSocket -> !serverSocket.isClosed(), ServerSocket.class);
            socketTester = new PredicateLeakTester<>(socket -> !socket.isClosed(), Socket.class);
        }

        @Override
        protected ServerSocket intercept(ServerSocket serverSocket) {
            ServerSocket spy = spy(serverSocket);
            try {
                doAnswer(invocation -> socketTester.open((Socket) invocation.callRealMethod())).when(spy).accept();
            } catch (IOException e) {
                return serverSocketTester.open(serverSocket);
            }
            return serverSocketTester.open(spy);
        }
    }
}
