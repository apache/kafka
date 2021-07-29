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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class PlaintextChannelBuilder implements ChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(PlaintextChannelBuilder.class);
    private final ListenerName listenerName;
    private Map<String, ?> configs;

    /**
     * Constructs a plaintext channel builder. ListenerName is non-null whenever
     * it's instantiated in the broker and null otherwise.
     */
    public PlaintextChannelBuilder(ListenerName listenerName) {
        this.listenerName = listenerName;
    }

    public void configure(Map<String, ?> configs) throws KafkaException {
        this.configs = configs;
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize,
                                     MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) throws KafkaException {
        try {
            PlaintextTransportLayer transportLayer = buildTransportLayer(key);
            Supplier<Authenticator> authenticatorCreator = () -> new PlaintextAuthenticator(configs, transportLayer, listenerName);
            return buildChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                    memoryPool != null ? memoryPool : MemoryPool.NONE, metadataRegistry);
        } catch (Exception e) {
            log.warn("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    // visible for testing
    KafkaChannel buildChannel(String id, TransportLayer transportLayer, Supplier<Authenticator> authenticatorCreator,
                              int maxReceiveSize, MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) {
        return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize, memoryPool, metadataRegistry);
    }

    protected PlaintextTransportLayer buildTransportLayer(SelectionKey key) throws IOException {
        return new PlaintextTransportLayer(key);
    }

    @Override
    public void close() {}

    private static class PlaintextAuthenticator implements Authenticator {
        private final PlaintextTransportLayer transportLayer;
        private final KafkaPrincipalBuilder principalBuilder;
        private final ListenerName listenerName;

        private PlaintextAuthenticator(Map<String, ?> configs, PlaintextTransportLayer transportLayer, ListenerName listenerName) {
            this.transportLayer = transportLayer;
            this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, null, null);
            this.listenerName = listenerName;
        }

        @Override
        public void authenticate() {}

        @Override
        public KafkaPrincipal principal() {
            InetAddress clientAddress = transportLayer.socketChannel().socket().getInetAddress();
            // listenerName should only be null in Client mode where principal() should not be called
            if (listenerName == null)
                throw new IllegalStateException("Unexpected call to principal() when listenerName is null");
            return principalBuilder.build(new PlaintextAuthenticationContext(clientAddress, listenerName.value()));
        }

        @Override
        public Optional<KafkaPrincipalSerde> principalSerde() {
            return principalBuilder instanceof KafkaPrincipalSerde ? Optional.of((KafkaPrincipalSerde) principalBuilder) : Optional.empty();
        }

        @Override
        public boolean complete() {
            return true;
        }

        @Override
        public void close() {
            if (principalBuilder instanceof Closeable)
                Utils.closeQuietly((Closeable) principalBuilder, "principal builder");
        }
    }

}
