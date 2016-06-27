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

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.auth.DefaultPrincipalBuilder;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

public class ChannelBuilders {

    private ChannelBuilders() { }

    /**
     * @param securityProtocol the securityProtocol
     * @param mode the mode, it must be non-null if `securityProtocol` is not `PLAINTEXT`;
     *             it is ignored otherwise
     * @param loginType the loginType, it must be non-null if `securityProtocol` is SASL_*; it is ignored otherwise
     * @param configs client/server configs
     * @param clientSaslMechanism SASL mechanism if mode is CLIENT, ignored otherwise
     * @param saslHandshakeRequestEnable flag to enable Sasl handshake requests; disabled only for SASL
     *             inter-broker connections with inter-broker protocol version < 0.10
     * @return the configured `ChannelBuilder`
     * @throws IllegalArgumentException if `mode` invariants described above is not maintained
     */
    public static ChannelBuilder create(SecurityProtocol securityProtocol,
                                        Mode mode,
                                        LoginType loginType,
                                        Map<String, ?> configs,
                                        String clientSaslMechanism,
                                        boolean saslHandshakeRequestEnable) {
        ChannelBuilder channelBuilder;

        switch (securityProtocol) {
            case SSL:
                requireNonNullMode(mode, securityProtocol);
                channelBuilder = new SslChannelBuilder(mode);
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT:
                requireNonNullMode(mode, securityProtocol);
                if (loginType == null)
                    throw new IllegalArgumentException("`loginType` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
                if (mode == Mode.CLIENT && clientSaslMechanism == null)
                    throw new IllegalArgumentException("`clientSaslMechanism` must be non-null in client mode if `securityProtocol` is `" + securityProtocol + "`");
                channelBuilder = new SaslChannelBuilder(mode, loginType, securityProtocol, clientSaslMechanism, saslHandshakeRequestEnable);
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
