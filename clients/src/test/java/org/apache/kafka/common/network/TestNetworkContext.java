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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ProtocolFamily;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.Channel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

/**
 * A utility for manipulating the {@link NetworkContext} during tests.
 * <p>The entry-point into this class is {@link #install(Function)} which installs a new {@link Decorator} into the
 * network context. Decorators are responsible for processing arguments, calling inner decorators, and processing the
 * return value from inner decorators. The root decorator is the default behavior of the network context, providing
 * real network resources.
 * <p>Multiple decorators can be installed concurrently. Installation is non-atomic, meaning that sockets may be
 * created without a Decorator after installation.
 * <p>This class is thread-safe.
 */
public class TestNetworkContext {

    /**
     * A single layer of the network context. Multiple Decorators form a singly-linked-list to compose behavior.
     */
    public interface Decorator {
        /**
         * It is recommended to construct a single subclass of {@link SelectorProviderDecorator} with overridden
         * behavior, and then return that instance in this method.
         * @return the {@link SelectorProvider} to use for {@link java.nio.channels} instances.
         */
        SelectorProvider provider();

        /**
         * It is recommended to construct a single subclass of {@link SocketFactoryDecorator} with overridden
         * behavior, and then return that instance in this method.
         * @return the {@link SocketFactory} to use for {@link java.net.Socket} instances.
         */
        SocketFactory factory();

        /**
         * It is recommended to construct a single subclass of {@link ServerSocketFactoryDecorator} with overridden
         * behavior, and then return that instance in this method.
         * @return the {@link ServerSocketFactory} to use for {@link java.net.ServerSocket} instances.
         */
        ServerSocketFactory serverFactory();
    }

    /**
     * Install a new decorator into the network context
     * @param newDecorator Function for constructing a new {@link Decorator} object. The argument to this function is a
     *                    supplier of the inner Decorator that should be called by the returned Decorator. The return
     *                    value of this supplier is non-null, but can change at any time, and so should not be cached.
     * @param <T> The subtype of the new decorator being constructed.
     * @return The decorator that was constructed by calling {@code newDecorator}
     */
    public static synchronized <T extends Decorator> T install(Function<Supplier<Decorator>, T> newDecorator) {
        return LazyNetworkContext.install(newDecorator);
    }

    /**
     * Lazy holder for the static state of the TestNetworkContext.
     * <p>Used to delay locking the {@link NetworkContext} until the first time a decorator is installed.
     */
    private static final class LazyNetworkContext {
        private static final AtomicReference<Decorator> ACTIVE;
        private static AtomicReference<Decorator> lastInstalled;

        static {
            ACTIVE = new AtomicReference<>(RootDecorator.INSTANCE);
            lastInstalled = new AtomicReference<>(RootDecorator.INSTANCE);
            // These instances are "fixed" as the NetworkContext can only be installed once.
            // Their behavior can be changed by updating ACTIVE
            SelectorProviderDecorator provider = new SelectorProviderDecorator(() -> ACTIVE.get().provider());
            SocketFactoryDecorator factory = new SocketFactoryDecorator(() -> ACTIVE.get().factory());
            ServerSocketFactoryDecorator serverFactory = new ServerSocketFactoryDecorator(() -> ACTIVE.get().serverFactory());
            NetworkContext.install(provider, factory, serverFactory);
        }

        public static synchronized <T extends Decorator> T install(Function<Supplier<Decorator>, T> newDecorator) {
            AtomicReference<Decorator> last = lastInstalled;
            T decorator = newDecorator.apply(last::get);
            ACTIVE.set(decorator);
            lastInstalled = new AtomicReference<>(decorator);
            return decorator;
        }
    }

    @SuppressWarnings("checkstyle:useNetworkContext")
    private static class RootDecorator implements Decorator {

        private static final RootDecorator INSTANCE = new RootDecorator();

        private RootDecorator() {
        }

        @Override
        public SelectorProvider provider() {
            return SelectorProvider.provider();
        }

        @Override
        public SocketFactory factory() {
            return SocketFactory.getDefault();
        }

        @Override
        public ServerSocketFactory serverFactory() {
            return ServerSocketFactory.getDefault();
        }
    }

    public static class SelectorProviderDecorator extends SelectorProvider {

        private final Supplier<SelectorProvider> inner;

        public SelectorProviderDecorator(Supplier<SelectorProvider> inner) {
            this.inner = inner;
        }

        @Override
        public DatagramChannel openDatagramChannel() throws IOException {
            return inner.get().openDatagramChannel();
        }

        @Override
        public DatagramChannel openDatagramChannel(ProtocolFamily family) throws IOException {
            return inner.get().openDatagramChannel(family);
        }

        @Override
        public Pipe openPipe() throws IOException {
            return inner.get().openPipe();
        }

        @Override
        public AbstractSelector openSelector() throws IOException {
            return inner.get().openSelector();
        }

        @Override
        public ServerSocketChannel openServerSocketChannel() throws IOException {
            return inner.get().openServerSocketChannel();
        }

        @Override
        public SocketChannel openSocketChannel() throws IOException {
            return inner.get().openSocketChannel();
        }

        @Override
        public Channel inheritedChannel() throws IOException {
            return inner.get().inheritedChannel();
        }
    }

    public static class SocketFactoryDecorator extends SocketFactory {

        private final Supplier<SocketFactory> inner;

        public SocketFactoryDecorator(Supplier<SocketFactory> inner) {
            this.inner = inner;
        }

        protected Socket intercept(Socket socket) {
            return socket;
        }

        @Override
        public Socket createSocket() throws IOException {
            return intercept(inner.get().createSocket());
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
            return intercept(inner.get().createSocket(host, port));
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException, UnknownHostException {
            return intercept(inner.get().createSocket(host, port, localHost, localPort));
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            return intercept(inner.get().createSocket(host, port));
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            return intercept(inner.get().createSocket(address, port, localAddress, localPort));
        }
    }

    public static class ServerSocketFactoryDecorator extends ServerSocketFactory {

        private final Supplier<ServerSocketFactory> inner;

        public ServerSocketFactoryDecorator(Supplier<ServerSocketFactory> inner) {
            this.inner = inner;
        }

        protected ServerSocket intercept(ServerSocket serverSocket) {
            return serverSocket;
        }

        @Override
        public ServerSocket createServerSocket() throws IOException {
            return intercept(inner.get().createServerSocket());
        }

        @Override
        public ServerSocket createServerSocket(int port) throws IOException {
            return intercept(inner.get().createServerSocket(port));
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog) throws IOException {
            return intercept(inner.get().createServerSocket(port, backlog));
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress) throws IOException {
            return intercept(inner.get().createServerSocket(port, backlog, ifAddress));
        }
    }
}
