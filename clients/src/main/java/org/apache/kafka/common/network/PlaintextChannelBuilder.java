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
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class PlaintextChannelBuilder implements ChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(PlaintextChannelBuilder.class);
    private final ListenerName listenerName;
    private Map<String, ?> configs;

    /**
     * Constructs a PLAINTEXT channel builder. ListenerName is provided only
     * for server channel builder and will be null for client channel builder.
     */
    public PlaintextChannelBuilder(ListenerName listenerName) {
        this.listenerName = listenerName;
    }

    public void configure(Map<String, ?> configs) throws KafkaException {
        this.configs = configs;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Collections.emptySet();
    }

    @Override
    public boolean validate(Map<String, ?> configs) {
        return false;
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        throw new IllegalStateException("PlaintextChannelBuilder is not reconfigurable");
    }

    @Override
    public ListenerName listenerName() {
        return listenerName;
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool) throws KafkaException {
        try {
            PlaintextTransportLayer transportLayer = new PlaintextTransportLayer(key);
            PlaintextAuthenticator authenticator = new PlaintextAuthenticator(configs, transportLayer);
            return new KafkaChannel(id, transportLayer, authenticator, maxReceiveSize,
                    memoryPool != null ? memoryPool : MemoryPool.NONE);
        } catch (Exception e) {
            log.warn("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    @Override
    public void close() {}

    private static class PlaintextAuthenticator implements Authenticator {
        private final PlaintextTransportLayer transportLayer;
        private final KafkaPrincipalBuilder principalBuilder;

        private PlaintextAuthenticator(Map<String, ?> configs, PlaintextTransportLayer transportLayer) {
            this.transportLayer = transportLayer;
            this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, transportLayer, this, null);
        }

        @Override
        public void authenticate() throws IOException {}

        @Override
        public KafkaPrincipal principal() {
            InetAddress clientAddress = transportLayer.socketChannel().socket().getInetAddress();
            return principalBuilder.build(new PlaintextAuthenticationContext(clientAddress));
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
