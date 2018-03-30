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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.token.delegation.internal.DelegationTokenCache;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
        return create(securityProtocol, Mode.CLIENT, contextType, config, listenerName, false, clientSaslMechanism,
                saslHandshakeRequestEnable, null, null);
    }

    /**
     * @param listenerName the listenerName
     * @param securityProtocol the securityProtocol
     * @param config server config
     * @param credentialCache Credential cache for SASL/SCRAM if SCRAM is enabled
     * @return the configured `ChannelBuilder`
     */
    public static ChannelBuilder serverChannelBuilder(ListenerName listenerName,
                                                      boolean isInterBrokerListener,
                                                      SecurityProtocol securityProtocol,
                                                      AbstractConfig config,
                                                      CredentialCache credentialCache,
                                                      DelegationTokenCache tokenCache) {
        return create(securityProtocol, Mode.SERVER, JaasContext.Type.SERVER, config, listenerName,
                isInterBrokerListener, null, true, credentialCache, tokenCache);
    }

    private static ChannelBuilder create(SecurityProtocol securityProtocol,
                                         Mode mode,
                                         JaasContext.Type contextType,
                                         AbstractConfig config,
                                         ListenerName listenerName,
                                         boolean isInterBrokerListener,
                                         String clientSaslMechanism,
                                         boolean saslHandshakeRequestEnable,
                                         CredentialCache credentialCache,
                                         DelegationTokenCache tokenCache) {
        Map<String, ?> configs;
        if (listenerName == null)
            configs = config.values();
        else
            configs = config.valuesWithPrefixOverride(listenerName.configPrefix());

        ChannelBuilder channelBuilder;
        switch (securityProtocol) {
            case SSL:
                requireNonNullMode(mode, securityProtocol);
                channelBuilder = new SslChannelBuilder(mode, listenerName, isInterBrokerListener);
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT:
                requireNonNullMode(mode, securityProtocol);
                Map<String, JaasContext> jaasContexts;
                if (mode == Mode.SERVER) {
                    List<String> enabledMechanisms = (List<String>) configs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG);
                    jaasContexts = new HashMap<>(enabledMechanisms.size());
                    for (String mechanism : enabledMechanisms)
                        jaasContexts.put(mechanism, JaasContext.loadServerContext(listenerName, mechanism, configs));
                } else {
                    // Use server context for inter-broker client connections and client context for other clients
                    JaasContext jaasContext = contextType == JaasContext.Type.CLIENT ? JaasContext.loadClientContext(configs) :
                            JaasContext.loadServerContext(listenerName, clientSaslMechanism, configs);
                    jaasContexts = Collections.singletonMap(clientSaslMechanism, jaasContext);
                }
                channelBuilder = new SaslChannelBuilder(mode,
                        jaasContexts,
                        securityProtocol,
                        listenerName,
                        isInterBrokerListener,
                        clientSaslMechanism,
                        saslHandshakeRequestEnable,
                        credentialCache,
                        tokenCache);
                break;
            case PLAINTEXT:
                channelBuilder = new PlaintextChannelBuilder(listenerName);
                break;
            default:
                throw new IllegalArgumentException("Unexpected securityProtocol " + securityProtocol);
        }

        channelBuilder.configure(configs);
        return channelBuilder;
    }

    private static void requireNonNullMode(Mode mode, SecurityProtocol securityProtocol) {
        if (mode == null)
            throw new IllegalArgumentException("`mode` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
    }

    // Use FQN to avoid deprecated import warnings
    @SuppressWarnings("deprecation")
    private static org.apache.kafka.common.security.auth.PrincipalBuilder createPrincipalBuilder(
            Class<?> principalBuilderClass, Map<String, ?> configs) {
        org.apache.kafka.common.security.auth.PrincipalBuilder principalBuilder;
        if (principalBuilderClass == null)
            principalBuilder = new org.apache.kafka.common.security.auth.DefaultPrincipalBuilder();
        else
            principalBuilder = (org.apache.kafka.common.security.auth.PrincipalBuilder) Utils.newInstance(principalBuilderClass);
        principalBuilder.configure(configs);
        return principalBuilder;
    }

    @SuppressWarnings("deprecation")
    public static KafkaPrincipalBuilder createPrincipalBuilder(Map<String, ?> configs,
                                                               TransportLayer transportLayer,
                                                               Authenticator authenticator,
                                                               KerberosShortNamer kerberosShortNamer) {
        Class<?> principalBuilderClass = (Class<?>) configs.get(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG);
        final KafkaPrincipalBuilder builder;

        if (principalBuilderClass == null || principalBuilderClass == DefaultKafkaPrincipalBuilder.class) {
            builder = new DefaultKafkaPrincipalBuilder(kerberosShortNamer);
        } else if (KafkaPrincipalBuilder.class.isAssignableFrom(principalBuilderClass)) {
            builder = (KafkaPrincipalBuilder) Utils.newInstance(principalBuilderClass);
        } else if (org.apache.kafka.common.security.auth.PrincipalBuilder.class.isAssignableFrom(principalBuilderClass)) {
            org.apache.kafka.common.security.auth.PrincipalBuilder oldPrincipalBuilder =
                    createPrincipalBuilder(principalBuilderClass, configs);
            builder = DefaultKafkaPrincipalBuilder.fromOldPrincipalBuilder(authenticator, transportLayer,
                    oldPrincipalBuilder, kerberosShortNamer);
        } else {
            throw new InvalidConfigurationException("Type " + principalBuilderClass.getName() + " is not " +
                    "an instance of " + org.apache.kafka.common.security.auth.PrincipalBuilder.class.getName() + " or " +
                    KafkaPrincipalBuilder.class.getName());
        }

        if (builder instanceof Configurable)
            ((Configurable) builder).configure(configs);

        return builder;
    }

}
