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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.Login;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.DefaultLogin;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.security.authenticator.SaslClientCallbackHandler;
import org.apache.kafka.common.security.authenticator.SaslServerAuthenticator;
import org.apache.kafka.common.security.authenticator.SaslServerCallbackHandler;
import org.apache.kafka.common.security.kerberos.KerberosClientCallbackHandler;
import org.apache.kafka.common.security.kerberos.KerberosLogin;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerRefreshingLogin;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramServerCallbackHandler;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class SaslChannelBuilder implements ChannelBuilder, ListenerReconfigurable {
    static final String GSS_NATIVE_PROP = "sun.security.jgss.native";

    private final SecurityProtocol securityProtocol;
    private final ListenerName listenerName;
    private final boolean isInterBrokerListener;
    private final String clientSaslMechanism;
    private final Mode mode;
    private final Map<String, JaasContext> jaasContexts;
    private final boolean handshakeRequestEnable;
    private final CredentialCache credentialCache;
    private final DelegationTokenCache tokenCache;
    private final Map<String, LoginManager> loginManagers;
    private final Map<String, Subject> subjects;

    private SslFactory sslFactory;
    private Map<String, ?> configs;
    private KerberosShortNamer kerberosShortNamer;
    private Map<String, AuthenticateCallbackHandler> saslCallbackHandlers;
    private Map<String, Long> connectionsMaxReauthMsByMechanism;
    private final Time time;
    private final LogContext logContext;
    private final Logger log;

    public SaslChannelBuilder(Mode mode,
                              Map<String, JaasContext> jaasContexts,
                              SecurityProtocol securityProtocol,
                              ListenerName listenerName,
                              boolean isInterBrokerListener,
                              String clientSaslMechanism,
                              boolean handshakeRequestEnable,
                              CredentialCache credentialCache,
                              DelegationTokenCache tokenCache,
                              Time time,
                              LogContext logContext) {
        this.mode = mode;
        this.jaasContexts = jaasContexts;
        this.loginManagers = new HashMap<>(jaasContexts.size());
        this.subjects = new HashMap<>(jaasContexts.size());
        this.securityProtocol = securityProtocol;
        this.listenerName = listenerName;
        this.isInterBrokerListener = isInterBrokerListener;
        this.handshakeRequestEnable = handshakeRequestEnable;
        this.clientSaslMechanism = clientSaslMechanism;
        this.credentialCache = credentialCache;
        this.tokenCache = tokenCache;
        this.saslCallbackHandlers = new HashMap<>();
        this.connectionsMaxReauthMsByMechanism = new HashMap<>();
        this.time = time;
        this.logContext = logContext;
        this.log = logContext.logger(getClass());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            this.configs = configs;
            if (mode == Mode.SERVER) {
                createServerCallbackHandlers(configs);
                createConnectionsMaxReauthMsMap(configs);
            } else
                createClientCallbackHandler(configs);
            for (Map.Entry<String, AuthenticateCallbackHandler> entry : saslCallbackHandlers.entrySet()) {
                String mechanism = entry.getKey();
                entry.getValue().configure(configs, mechanism, jaasContexts.get(mechanism).configurationEntries());
            }

            Class<? extends Login> defaultLoginClass = defaultLoginClass();
            if (mode == Mode.SERVER && jaasContexts.containsKey(SaslConfigs.GSSAPI_MECHANISM)) {
                String defaultRealm;
                try {
                    defaultRealm = defaultKerberosRealm();
                } catch (Exception ke) {
                    defaultRealm = "";
                }
                List<String> principalToLocalRules = (List<String>) configs.get(BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG);
                if (principalToLocalRules != null)
                    kerberosShortNamer = KerberosShortNamer.fromUnparsedRules(defaultRealm, principalToLocalRules);
            }
            for (Map.Entry<String, JaasContext> entry : jaasContexts.entrySet()) {
                String mechanism = entry.getKey();
                // With static JAAS configuration, use KerberosLogin if Kerberos is enabled. With dynamic JAAS configuration,
                // use KerberosLogin only for the LoginContext corresponding to GSSAPI
                LoginManager loginManager = LoginManager.acquireLoginManager(entry.getValue(), mechanism, defaultLoginClass, configs);
                loginManagers.put(mechanism, loginManager);
                Subject subject = loginManager.subject();
                subjects.put(mechanism, subject);
                if (mode == Mode.SERVER && mechanism.equals(SaslConfigs.GSSAPI_MECHANISM))
                    maybeAddNativeGssapiCredentials(subject);
            }
            if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
                // Disable SSL client authentication as we are using SASL authentication
                this.sslFactory = new SslFactory(mode, "none", isInterBrokerListener);
                this.sslFactory.configure(configs);
            }
        } catch (Throwable e) {
            close();
            throw new KafkaException(e);
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return securityProtocol == SecurityProtocol.SASL_SSL ? SslConfigs.RECONFIGURABLE_CONFIGS : Collections.emptySet();
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL)
            sslFactory.validateReconfiguration(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL)
            sslFactory.reconfigure(configs);
    }

    @Override
    public ListenerName listenerName() {
        return listenerName;
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize,
                                     MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) throws KafkaException {
        try {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            Socket socket = socketChannel.socket();
            TransportLayer transportLayer = buildTransportLayer(id, key, socketChannel, metadataRegistry);
            Supplier<Authenticator> authenticatorCreator;
            if (mode == Mode.SERVER) {
                authenticatorCreator = () -> buildServerAuthenticator(configs,
                        Collections.unmodifiableMap(saslCallbackHandlers),
                        id,
                        transportLayer,
                        Collections.unmodifiableMap(subjects),
                        Collections.unmodifiableMap(connectionsMaxReauthMsByMechanism),
                        metadataRegistry);
            } else {
                LoginManager loginManager = loginManagers.get(clientSaslMechanism);
                authenticatorCreator = () -> buildClientAuthenticator(configs,
                        saslCallbackHandlers.get(clientSaslMechanism),
                        id,
                        socket.getInetAddress().getHostName(),
                        loginManager.serviceName(),
                        transportLayer,
                        subjects.get(clientSaslMechanism));
            }
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                memoryPool != null ? memoryPool : MemoryPool.NONE, metadataRegistry);
        } catch (Exception e) {
            log.info("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    @Override
    public void close()  {
        for (LoginManager loginManager : loginManagers.values())
            loginManager.release();
        loginManagers.clear();
        for (AuthenticateCallbackHandler handler : saslCallbackHandlers.values())
            handler.close();
        if (sslFactory != null) sslFactory.close();
    }

    // Visible to override for testing
    protected TransportLayer buildTransportLayer(String id, SelectionKey key, SocketChannel socketChannel,
                                                 ChannelMetadataRegistry metadataRegistry) throws IOException {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
            return SslTransportLayer.create(id, key,
                sslFactory.createSslEngine(socketChannel.socket().getInetAddress().getHostName(),
                    socketChannel.socket().getPort()),
                metadataRegistry);
        } else {
            return new PlaintextTransportLayer(key);
        }
    }

    // Visible to override for testing
    protected SaslServerAuthenticator buildServerAuthenticator(Map<String, ?> configs,
                                                               Map<String, AuthenticateCallbackHandler> callbackHandlers,
                                                               String id,
                                                               TransportLayer transportLayer,
                                                               Map<String, Subject> subjects,
                                                               Map<String, Long> connectionsMaxReauthMsByMechanism,
                                                               ChannelMetadataRegistry metadataRegistry) {
        return new SaslServerAuthenticator(configs, callbackHandlers, id, subjects,
                kerberosShortNamer, listenerName, securityProtocol, transportLayer,
                connectionsMaxReauthMsByMechanism, metadataRegistry, time);
    }

    // Visible to override for testing
    protected SaslClientAuthenticator buildClientAuthenticator(Map<String, ?> configs,
                                                               AuthenticateCallbackHandler callbackHandler,
                                                               String id,
                                                               String serverHost,
                                                               String servicePrincipal,
                                                               TransportLayer transportLayer, Subject subject) {
        return new SaslClientAuthenticator(configs, callbackHandler, id, subject, servicePrincipal,
                serverHost, clientSaslMechanism, handshakeRequestEnable, transportLayer, time, logContext);
    }

    // Package private for testing
    Map<String, LoginManager> loginManagers() {
        return loginManagers;
    }

    private static String defaultKerberosRealm() {
        // see https://issues.apache.org/jira/browse/HADOOP-10848 for details
        return new KerberosPrincipal("tmp", 1).getRealm();
    }

    private void createClientCallbackHandler(Map<String, ?> configs) {
        @SuppressWarnings("unchecked")
        Class<? extends AuthenticateCallbackHandler> clazz = (Class<? extends AuthenticateCallbackHandler>) configs.get(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS);
        if (clazz == null)
            clazz = clientCallbackHandlerClass();
        AuthenticateCallbackHandler callbackHandler = Utils.newInstance(clazz);
        saslCallbackHandlers.put(clientSaslMechanism, callbackHandler);
    }

    private void createServerCallbackHandlers(Map<String, ?> configs) {
        for (String mechanism : jaasContexts.keySet()) {
            AuthenticateCallbackHandler callbackHandler;
            String prefix = ListenerName.saslMechanismPrefix(mechanism);
            @SuppressWarnings("unchecked")
            Class<? extends AuthenticateCallbackHandler> clazz =
                    (Class<? extends AuthenticateCallbackHandler>) configs.get(prefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS);
            if (clazz != null)
                callbackHandler = Utils.newInstance(clazz);
            else if (mechanism.equals(PlainSaslServer.PLAIN_MECHANISM))
                callbackHandler = new PlainServerCallbackHandler();
            else if (ScramMechanism.isScram(mechanism))
                callbackHandler = new ScramServerCallbackHandler(credentialCache.cache(mechanism, ScramCredential.class), tokenCache);
            else if (mechanism.equals(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM))
                callbackHandler = new OAuthBearerUnsecuredValidatorCallbackHandler();
            else
                callbackHandler = new SaslServerCallbackHandler();
            saslCallbackHandlers.put(mechanism, callbackHandler);
        }
    }

    private void createConnectionsMaxReauthMsMap(Map<String, ?> configs) {
        for (String mechanism : jaasContexts.keySet()) {
            String prefix = ListenerName.saslMechanismPrefix(mechanism);
            Long connectionsMaxReauthMs = (Long) configs.get(prefix + BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS);
            if (connectionsMaxReauthMs == null)
                connectionsMaxReauthMs = (Long) configs.get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS);
            if (connectionsMaxReauthMs != null)
                connectionsMaxReauthMsByMechanism.put(mechanism, connectionsMaxReauthMs);
        }
    }

    protected Class<? extends Login> defaultLoginClass() {
        if (jaasContexts.containsKey(SaslConfigs.GSSAPI_MECHANISM))
            return KerberosLogin.class;
        if (OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(clientSaslMechanism))
            return OAuthBearerRefreshingLogin.class;
        return DefaultLogin.class;
    }

    private Class<? extends AuthenticateCallbackHandler> clientCallbackHandlerClass() {
        switch (clientSaslMechanism) {
            case SaslConfigs.GSSAPI_MECHANISM:
                return KerberosClientCallbackHandler.class;
            case OAuthBearerLoginModule.OAUTHBEARER_MECHANISM:
                return OAuthBearerSaslClientCallbackHandler.class;
            default:
                return SaslClientCallbackHandler.class;
        }
    }

    // As described in http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/jgss-features.html:
    // "To enable Java GSS to delegate to the native GSS library and its list of native mechanisms,
    // set the system property "sun.security.jgss.native" to true"
    // "In addition, when performing operations as a particular Subject, for example, Subject.doAs(...)
    // or Subject.doAsPrivileged(...), the to-be-used GSSCredential should be added to Subject's
    // private credential set. Otherwise, the GSS operations will fail since no credential is found."
    private void maybeAddNativeGssapiCredentials(Subject subject) {
        boolean usingNativeJgss = Boolean.getBoolean(GSS_NATIVE_PROP);
        if (usingNativeJgss && subject.getPrivateCredentials(GSSCredential.class).isEmpty()) {

            final String servicePrincipal = SaslClientAuthenticator.firstPrincipal(subject);
            KerberosName kerberosName;
            try {
                kerberosName = KerberosName.parse(servicePrincipal);
            } catch (IllegalArgumentException e) {
                throw new KafkaException("Principal has name with unexpected format " + servicePrincipal);
            }
            final String servicePrincipalName = kerberosName.serviceName();
            final String serviceHostname = kerberosName.hostName();

            try {
                GSSManager manager = gssManager();
                // This Oid is used to represent the Kerberos version 5 GSS-API mechanism. It is defined in
                // RFC 1964.
                Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
                GSSName gssName = manager.createName(servicePrincipalName + "@" + serviceHostname, GSSName.NT_HOSTBASED_SERVICE);
                GSSCredential cred = manager.createCredential(gssName,
                        GSSContext.INDEFINITE_LIFETIME, krb5Mechanism, GSSCredential.ACCEPT_ONLY);
                subject.getPrivateCredentials().add(cred);
                log.info("Configured native GSSAPI private credentials for {}@{}", serviceHostname, serviceHostname);
            } catch (GSSException ex) {
                log.warn("Cannot add private credential to subject; clients authentication may fail", ex);
            }
        }
    }

    // Visibility to override for testing
    protected GSSManager gssManager() {
        return GSSManager.getInstance();
    }

    // Visibility for testing
    protected Subject subject(String saslMechanism) {
        return subjects.get(saslMechanism);
    }
}
