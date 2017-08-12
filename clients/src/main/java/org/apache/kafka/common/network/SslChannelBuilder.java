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
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SslChannelBuilder implements ChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(SslChannelBuilder.class);
    private SslFactory sslFactory;
    private PrincipalBuilder principalBuilder;
    private Mode mode;
    private Map<String, ?> configs;

    public SslChannelBuilder(Mode mode) {
        this.mode = mode;
    }

    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            this.configs = configs;
            this.sslFactory = new SslFactory(mode);
            this.sslFactory.configure(this.configs);
            this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool) throws KafkaException {
        try {
            SslTransportLayer transportLayer = buildTransportLayer(sslFactory, id, key, peerHost(key));
            Authenticator authenticator = new DefaultAuthenticator();
            authenticator.configure(transportLayer, this.principalBuilder, this.configs);
            return new KafkaChannel(id, transportLayer, authenticator, maxReceiveSize, memoryPool != null ? memoryPool : MemoryPool.NONE);
        } catch (Exception e) {
            log.info("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    public void close()  {
        this.principalBuilder.close();
    }

    protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key, String host) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        return SslTransportLayer.create(id, key,
            sslFactory.createSslEngine(host, socketChannel.socket().getPort()));
    }

    /**
     * Returns host/IP address of remote host without reverse DNS lookup to be used as the host
     * for creating SSL engine. This is used as a hint for session reuse strategy and also for
     * hostname verification of server hostnames.
     * <p>
     * Scenarios:
     * <ul>
     *   <li>Server-side
     *   <ul>
     *     <li>Server accepts connection from a client. Server knows only client IP
     *     address. We want to avoid reverse DNS lookup of the client IP address since the server
     *     does not verify or use client hostname. The IP address can be used directly.</li>
     *   </ul>
     *   </li>
     *   <li>Client-side
     *   <ul>
     *     <li>Client connects to server using hostname. No lookup is necessary
     *     and the hostname should be used to create the SSL engine. This hostname is validated
     *     against the hostname in SubjectAltName (dns) or CommonName in the certificate if
     *     hostname verification is enabled. Authentication fails if hostname does not match.</li>
     *     <li>Client connects to server using IP address, but certificate contains only
     *     SubjectAltName (dns). Use of reverse DNS lookup to determine hostname introduces
     *     a security vulnerability since authentication would be reliant on a secure DNS.
     *     Hence hostname verification should fail in this case.</li>
     *     <li>Client connects to server using IP address and certificate contains
     *     SubjectAltName (ipaddress). This could be used when Kafka is on a private network.
     *     If reverse DNS lookup is used, authentication would succeed using IP address if lookup
     *     fails and IP address is used, but authentication would fail if lookup succeeds and
     *     dns name is used. For consistency and to avoid dependency on a potentially insecure
     *     DNS, reverse DNS lookup should be avoided and the IP address specified by the client for
     *     connection should be used to create the SSL engine.</li>
     *   </ul></li>
     * </ul>
     */
    private String peerHost(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        return new InetSocketAddress(socketChannel.socket().getInetAddress(), 0).getHostString();
    }
}
