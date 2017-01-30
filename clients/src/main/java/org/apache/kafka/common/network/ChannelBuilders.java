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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.DefaultPrincipalBuilder;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

public class ChannelBuilders {

    private ChannelBuilders() { }

    /**
     * @param securityProtocol the securityProtocol
     * @param contextType the contextType, it must be non-null if `securityProtocol` is SASL_*; it is ignored otherwise
     * @param config client config
     * @param listenerName the listenerName if contextType is SERVER or null otherwise
     * @param clientSaslMechanism SASL mechanism if mode is CLIENT, ignored otherwise
     * @param saslHandshakeRequestEnable flag to enable Sasl handshake requests; disabled only for SASL
     *             inter-broker connections with inter-broker protocol version < 0.10
     * @return the configured `ChannelBuilder`
     * @throws IllegalArgumentException if `mode` invariants described above is not maintained
     */
    public static ChannelBuilder clientChannelBuilder(SecurityProtocol securityProtocol,
            JaasContext.Type contextType,
            AbstractConfig config,
            ListenerName listenerName,
            String clientSaslMechanism,
            boolean saslHandshakeRequestEnable) {

        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            if (contextType == null)
                throw new IllegalArgumentException("`contextType` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
            if (clientSaslMechanism == null)
                throw new IllegalArgumentException("`clientSaslMechanism` must be non-null in client mode if `securityProtocol` is `" + securityProtocol + "`");
        }
        return create(securityProtocol, Mode.CLIENT, contextType, config, listenerName, clientSaslMechanism,
                saslHandshakeRequestEnable, null);
    }

    /**
     * @param listenerName the listenerName
     * @param securityProtocol the securityProtocol
     * @param config server config
     * @param credentialCache Credential cache for SASL/SCRAM if SCRAM is enabled
     * @return the configured `ChannelBuilder`
     */
    public static ChannelBuilder serverChannelBuilder(ListenerName listenerName,
                                                      SecurityProtocol securityProtocol,
                                                      AbstractConfig config,
                                                      CredentialCache credentialCache) {
        return create(securityProtocol, Mode.SERVER, JaasContext.Type.SERVER, config, listenerName, null,
                true, credentialCache);
    }

    private static ChannelBuilder create(SecurityProtocol securityProtocol,
                                         Mode mode,
                                         JaasContext.Type contextType,
                                         AbstractConfig config,
                                         ListenerName listenerName,
                                         String clientSaslMechanism,
                                         boolean saslHandshakeRequestEnable,
                                         CredentialCache credentialCache) {
        Map<String, ?> configs;
        if (listenerName == null)
            configs = config.values();
        else
            configs = config.valuesWithPrefixOverride(listenerName.configPrefix());

        ChannelBuilder channelBuilder;
        switch (securityProtocol) {
            case SSL:
                requireNonNullMode(mode, securityProtocol);
                channelBuilder = new SslChannelBuilder(mode);
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT:
                requireNonNullMode(mode, securityProtocol);
                JaasContext jaasContext = JaasContext.load(contextType, listenerName, configs);
                channelBuilder = new SaslChannelBuilder(mode, jaasContext, securityProtocol,
                        clientSaslMechanism, saslHandshakeRequestEnable, credentialCache);
                break;
            case PLAINTEXT:
            case TRACE:
                channelBuilder = new PlaintextChannelBuilder();
                break;
            default:
                throw new IllegalArgumentException("Unexpected securityProtocol " + securityProtocol);
        }

        channelBuilder.configure(configs);
        return channelBuilder;
    }

    /**
     * Returns a configured `PrincipalBuilder`.
     */
    static PrincipalBuilder createPrincipalBuilder(Map<String, ?> configs) {
        // this is a server-only config so it will always be null on the client
        Class<?> principalBuilderClass = (Class<?>) configs.get(SslConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG);
        PrincipalBuilder principalBuilder;
        if (principalBuilderClass == null)
            principalBuilder = new DefaultPrincipalBuilder();
        else
            principalBuilder = (PrincipalBuilder) Utils.newInstance(principalBuilderClass);
        principalBuilder.configure(configs);
        return principalBuilder;
    }

    private static void requireNonNullMode(Mode mode, SecurityProtocol securityProtocol) {
        if (mode == null)
            throw new IllegalArgumentException("`mode` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
    }

}
