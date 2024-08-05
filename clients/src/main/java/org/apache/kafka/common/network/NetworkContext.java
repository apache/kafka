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

import java.nio.channels.spi.SelectorProvider;
import java.util.ConcurrentModificationException;
import java.util.Objects;
import java.util.concurrent.Semaphore;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

/**
 * Store and distribute static factories for {@link java.net} sockets and {@link java.nio.channels} channel instances.
 * <p>By default, this class is a no-op, and distributes the default factories provided by the system.
 * <p>In testing environments, {@link #install(SelectorProvider, SocketFactory, ServerSocketFactory)} can be used to
 * replace the default factories with custom factories for performing assertions.
 */
@SuppressWarnings("checkstyle:useNetworkContext")
public class NetworkContext {

    private static final Semaphore LOCK = new Semaphore(1);
    private static volatile SelectorProvider selectorProvider = SelectorProvider.provider();
    private static volatile SocketFactory socketFactory = SocketFactory.getDefault();
    private static volatile ServerSocketFactory serverSocketFactory = ServerSocketFactory.getDefault();

    /**
     * Get the current provider for channels, which may change at any time.
     * Use the result as soon as possible to avoid using a stale provider.
     * @return The {@link SelectorProvider} factory for {@link java.nio.channels} implementations, non-null
     */
    public static SelectorProvider provider() {
        return selectorProvider;
    }

    /**
     * Get the current factory for client sockets, which may change at any time.
     * Use the result as soon as possible to avoid using a stale factory.
     * @return The {@link SocketFactory} factory for {@link java.net.Socket} implementations, non-null
     */
    public static SocketFactory factory() {
        return socketFactory;
    }

    /**
     * Get the current factory for server sockets, which may change at any time.
     * Use the result as soon as possible to avoid using a stale factory.
     * @return The {@link ServerSocketFactory} factory for {@link java.net.ServerSocket} implementations, non-null
     */
    public static ServerSocketFactory serverFactory() {
        return serverSocketFactory;
    }

    /**
     * Permanently install alternative factories for network resources. Can only be called once, and subsequent calls
     * will fail with {@link ConcurrentModificationException}
     * <p>This is meant for use only in tests. Installation is non-atomic, so network resources may be created with the
     * default factories after installation.
     * @param newSelectorProvider A provider for NIO selectors and sockets, non-null
     * @param newSocketFactory A provider for client-side TCP sockets, non-null
     * @param newServerSocketFactory A provider for server-side TCP sockets, non-null
     * @throws ConcurrentModificationException if called more than once.
     * @throws NullPointerException if any argument is null.
     */
    protected static void install(
            SelectorProvider newSelectorProvider,
            SocketFactory newSocketFactory,
            ServerSocketFactory newServerSocketFactory
    ) {
        Objects.requireNonNull(newSelectorProvider, "SelectorProvider must be non-null");
        Objects.requireNonNull(newSocketFactory, "SocketFactory must be non-null");
        Objects.requireNonNull(newServerSocketFactory, "ServerSocketFactory must be non-null");
        if (!LOCK.tryAcquire()) {
            throw new ConcurrentModificationException("The network context is already in-use");
        }
        selectorProvider = newSelectorProvider;
        socketFactory = newSocketFactory;
        serverSocketFactory = newServerSocketFactory;
    }
}
