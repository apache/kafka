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
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

public class ChannelBuilders {
    private static final Logger log = LoggerFactory.getLogger(ChannelBuilders.class);

    private ChannelBuilders() { }

    /**
     * @param securityProtocol the securityProtocol
     * @param contextType the contextType, it must be non-null if `securityProtocol` is SASL_*; it is ignored otherwise
     * @param config client config
     * @param listenerName the listenerName if contextType is SERVER or null otherwise
     * @param clientSaslMechanism SASL mechanism if mode is CLIENT, ignored otherwise
     * @param time the time instance
     * @param saslHandshakeRequestEnable flag to enable Sasl handshake requests; disabled only for SASL
     *             inter-broker connections with inter-broker protocol version < 0.10
     * @param logContext the log context instance
     *
     * @return the configured `ChannelBuilder`
     * @throws IllegalArgumentException if `mode` invariants described above is not maintained
     */
    public static ChannelBuilder clientChannelBuilder(
            SecurityProtocol securityProtocol,
            JaasContext.Type contextType,
            AbstractConfig config,
            ListenerName listenerName,
            String clientSaslMechanism,
            Time time,
            boolean saslHandshakeRequestEnable,
            LogContext logContext) {

        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            if (contextType == null)
                throw new IllegalArgumentException("`contextType` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
            if (clientSaslMechanism == null)
                throw new IllegalArgumentException("`clientSaslMechanism` must be non-null in client mode if `securityProtocol` is `" + securityProtocol + "`");
        }
        return create(securityProtocol, Mode.CLIENT, contextType, config, listenerName, false, clientSaslMechanism,
                saslHandshakeRequestEnable, null, null, time, logContext, null);
    }

    /**
     * @param listenerName the listenerName
     * @param isInterBrokerListener whether or not this listener is used for inter-broker requests
     * @param securityProtocol the securityProtocol
     * @param config server config
     * @param credentialCache Credential cache for SASL/SCRAM if SCRAM is enabled
     * @param tokenCache Delegation token cache
     * @param time the time instance
     * @param logContext the log context instance
     * @param apiVersionSupplier supplier for ApiVersions responses sent prior to authentication
     *
     * @return the configured `ChannelBuilder`
     */
    public static ChannelBuilder serverChannelBuilder(ListenerName listenerName,
                                                      boolean isInterBrokerListener,
                                                      SecurityProtocol securityProtocol,
                                                      AbstractConfig config,
                                                      CredentialCache credentialCache,
                                                      DelegationTokenCache tokenCache,
                                                      Time time,
                                                      LogContext logContext,
                                                      Supplier<ApiVersionsResponse> apiVersionSupplier) {
        return create(securityProtocol, Mode.SERVER, JaasContext.Type.SERVER, config, listenerName,
                isInterBrokerListener, null, true, credentialCache,
                tokenCache, time, logContext, apiVersionSupplier);
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
                                         DelegationTokenCache tokenCache,
                                         Time time,
                                         LogContext logContext,
                                         Supplier<ApiVersionsResponse> apiVersionSupplier) {
        Map<String, Object> configs = channelBuilderConfigs(config, listenerName);

        ChannelBuilder channelBuilder;
        switch (securityProtocol) {
            case SSL:
                requireNonNullMode(mode, securityProtocol);
                channelBuilder = new SslChannelBuilder(mode, listenerName, isInterBrokerListener, logContext);
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT:
                requireNonNullMode(mode, securityProtocol);
                Map<String, JaasContext> jaasContexts;
                String sslClientAuthOverride = null;
                if (mode == Mode.SERVER) {
                    @SuppressWarnings("unchecked")
                    List<String> enabledMechanisms = (List<String>) configs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG);
                    jaasContexts = new HashMap<>(enabledMechanisms.size());
                    for (String mechanism : enabledMechanisms)
                        jaasContexts.put(mechanism, JaasContext.loadServerContext(listenerName, mechanism, configs));

                    // SSL client authentication is enabled in brokers for SASL_SSL only if listener-prefixed config is specified.
                    if (listenerName != null && securityProtocol == SecurityProtocol.SASL_SSL) {
                        String configuredClientAuth = (String) configs.get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);
                        String listenerClientAuth = (String) config.originalsWithPrefix(listenerName.configPrefix(), true)
                                .get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);

                        // If `ssl.client.auth` is configured at the listener-level, we don't set an override and SslFactory
                        // uses the value from `configs`. If not, we propagate `sslClientAuthOverride=NONE` to SslFactory and
                        // it applies the override to the latest configs when it is configured or reconfigured. `Note that
                        // ssl.client.auth` cannot be dynamically altered.
                        if (listenerClientAuth == null) {
                            sslClientAuthOverride = SslClientAuth.NONE.name().toLowerCase(Locale.ROOT);
                            if (configuredClientAuth != null && !configuredClientAuth.equalsIgnoreCase(SslClientAuth.NONE.name())) {
                                log.warn("Broker configuration '{}' is applied only to SSL listeners. Listener-prefixed configuration can be used" +
                                        " to enable SSL client authentication for SASL_SSL listeners. In future releases, broker-wide option without" +
                                        " listener prefix may be applied to SASL_SSL listeners as well. All configuration options intended for specific" +
                                        " listeners should be listener-prefixed.", BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);
                            }
                        }
                    }
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
                        tokenCache,
                        sslClientAuthOverride,
                        time,
                        logContext,
                        apiVersionSupplier);
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

    /**
     * @return a mutable RecordingMap. The elements got from RecordingMap are marked as "used".
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> channelBuilderConfigs(final AbstractConfig config, final ListenerName listenerName) {
        Map<String, Object> parsedConfigs;
        if (listenerName == null)
            parsedConfigs = (Map<String, Object>) config.values();
        else
            parsedConfigs = config.valuesWithPrefixOverride(listenerName.configPrefix());

        config.originals().entrySet().stream()
            .filter(e -> !parsedConfigs.containsKey(e.getKey())) // exclude already parsed configs
            // exclude already parsed listener prefix configs
            .filter(e -> !(listenerName != null && e.getKey().startsWith(listenerName.configPrefix()) &&
                parsedConfigs.containsKey(e.getKey().substring(listenerName.configPrefix().length()))))
            // exclude keys like `{mechanism}.some.prop` if "listener.name." prefix is present and key `some.prop` exists in parsed configs.
            .filter(e -> !(listenerName != null && parsedConfigs.containsKey(e.getKey().substring(e.getKey().indexOf('.') + 1))))
            .forEach(e -> parsedConfigs.put(e.getKey(), e.getValue()));
        return parsedConfigs;
    }

    private static void requireNonNullMode(Mode mode, SecurityProtocol securityProtocol) {
        if (mode == null)
            throw new IllegalArgumentException("`mode` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
    }

    public static KafkaPrincipalBuilder createPrincipalBuilder(Map<String, ?> configs,
                                                               KerberosShortNamer kerberosShortNamer,
                                                               SslPrincipalMapper sslPrincipalMapper) {
        Class<?> principalBuilderClass = (Class<?>) configs.get(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG);
        final KafkaPrincipalBuilder builder;

        if (principalBuilderClass == null || principalBuilderClass == DefaultKafkaPrincipalBuilder.class) {
            builder = new DefaultKafkaPrincipalBuilder(kerberosShortNamer, sslPrincipalMapper);
        } else if (KafkaPrincipalBuilder.class.isAssignableFrom(principalBuilderClass)) {
            builder = (KafkaPrincipalBuilder) Utils.newInstance(principalBuilderClass);
        } else {
            throw new InvalidConfigurationException("Type " + principalBuilderClass.getName() + " is not " +
                    "an instance of " + KafkaPrincipalBuilder.class.getName());
        }

        if (builder instanceof Configurable)
            ((Configurable) builder).configure(configs);

        return builder;
    }

}
