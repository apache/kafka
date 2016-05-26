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
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.security.authenticator.SaslServerAuthenticator;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslChannelBuilder implements ChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(SaslChannelBuilder.class);

    private final SecurityProtocol securityProtocol;
    private final String clientSaslMechanism;
    private final Mode mode;
    private final LoginType loginType;
    private final boolean handshakeRequestEnable;

    private LoginManager loginManager;
    private SslFactory sslFactory;
    private Map<String, ?> configs;
    private KerberosShortNamer kerberosShortNamer;

    public SaslChannelBuilder(Mode mode, LoginType loginType, SecurityProtocol securityProtocol, String clientSaslMechanism, boolean handshakeRequestEnable) {
        this.mode = mode;
        this.loginType = loginType;
        this.securityProtocol = securityProtocol;
        this.handshakeRequestEnable = handshakeRequestEnable;
        this.clientSaslMechanism = clientSaslMechanism;
    }

    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            this.configs = configs;
            boolean hasKerberos;
            if (mode == Mode.SERVER) {
                List<String> enabledMechanisms = (List<String>) this.configs.get(SaslConfigs.SASL_ENABLED_MECHANISMS);
                hasKerberos = enabledMechanisms == null || enabledMechanisms.contains(SaslConfigs.GSSAPI_MECHANISM);
            } else {
                hasKerberos = clientSaslMechanism.equals(SaslConfigs.GSSAPI_MECHANISM);
            }

            if (hasKerberos) {
                String defaultRealm;
                try {
                    defaultRealm = JaasUtils.defaultKerberosRealm();
                } catch (Exception ke) {
                    defaultRealm = "";
                }
                @SuppressWarnings("unchecked")
                List<String> principalToLocalRules = (List<String>) configs.get(SaslConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES);
                if (principalToLocalRules != null)
                    kerberosShortNamer = KerberosShortNamer.fromUnparsedRules(defaultRealm, principalToLocalRules);
            }
            this.loginManager = LoginManager.acquireLoginManager(loginType, hasKerberos, configs);

            if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
                // Disable SSL client authentication as we are using SASL authentication
                this.sslFactory = new SslFactory(mode, "none");
                this.sslFactory.configure(configs);
            }
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize) throws KafkaException {
        try {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            TransportLayer transportLayer = buildTransportLayer(id, key, socketChannel);
            Authenticator authenticator;
            if (mode == Mode.SERVER)
                authenticator = new SaslServerAuthenticator(id, loginManager.subject(), kerberosShortNamer,
                        socketChannel.socket().getLocalAddress().getHostName(), maxReceiveSize);
            else
                authenticator = new SaslClientAuthenticator(id, loginManager.subject(), loginManager.serviceName(),
                        socketChannel.socket().getInetAddress().getHostName(), clientSaslMechanism, handshakeRequestEnable);
            // Both authenticators don't use `PrincipalBuilder`, so we pass `null` for now. Reconsider if this changes.
            authenticator.configure(transportLayer, null, this.configs);
            return new KafkaChannel(id, transportLayer, authenticator, maxReceiveSize);
        } catch (Exception e) {
            log.info("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    public void close()  {
        if (this.loginManager != null)
            this.loginManager.release();
    }

    protected TransportLayer buildTransportLayer(String id, SelectionKey key, SocketChannel socketChannel) throws IOException {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
            return SslTransportLayer.create(id, key,
                sslFactory.createSslEngine(socketChannel.socket().getInetAddress().getHostName(), socketChannel.socket().getPort()));
        } else {
            return new PlaintextTransportLayer(key);
        }
    }

}
