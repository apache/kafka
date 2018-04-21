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
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.plain.internal.PlainSaslServer;
import org.apache.kafka.common.security.plain.internal.PlainServerCallbackHandler;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internal.ScramMechanism;
import org.apache.kafka.common.security.scram.internal.ScramServerCallbackHandler;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.security.token.delegation.internal.DelegationTokenCache;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

public class SaslChannelBuilder implements ChannelBuilder, ListenerReconfigurable {
    private static final Logger log = LoggerFactory.getLogger(SaslChannelBuilder.class);

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

    public SaslChannelBuilder(Mode mode,
                              Map<String, JaasContext> jaasContexts,
                              SecurityProtocol securityProtocol,
                              ListenerName listenerName,
                              boolean isInterBrokerListener,
                              String clientSaslMechanism,
                              boolean handshakeRequestEnable,
                              CredentialCache credentialCache,
                              DelegationTokenCache tokenCache) {
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
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            this.configs = configs;
            if (mode == Mode.SERVER)
                createServerCallbackHandlers(configs);
            else
                createClientCallbackHandler(configs);
            for (Map.Entry<String, AuthenticateCallbackHandler> entry : saslCallbackHandlers.entrySet()) {
                String mechanism = entry.getKey();
                entry.getValue().configure(configs, mechanism, jaasContexts.get(mechanism).configurationEntries());
            }

            Class<? extends Login> defaultLoginClass = DefaultLogin.class;
            if (jaasContexts.containsKey(SaslConfigs.GSSAPI_MECHANISM)) {
                String defaultRealm;
                try {
                    defaultRealm = defaultKerberosRealm();
                } catch (Exception ke) {
                    defaultRealm = "";
                }
                @SuppressWarnings("unchecked")
                List<String> principalToLocalRules = (List<String>) configs.get(BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG);
                if (principalToLocalRules != null)
                    kerberosShortNamer = KerberosShortNamer.fromUnparsedRules(defaultRealm, principalToLocalRules);
                defaultLoginClass = KerberosLogin.class;
            }
            for (Map.Entry<String, JaasContext> entry : jaasContexts.entrySet()) {
                String mechanism = entry.getKey();
                // With static JAAS configuration, use KerberosLogin if Kerberos is enabled. With dynamic JAAS configuration,
                // use KerberosLogin only for the LoginContext corresponding to GSSAPI
                LoginManager loginManager = LoginManager.acquireLoginManager(entry.getValue(), mechanism, defaultLoginClass, configs);
                loginManagers.put(mechanism, loginManager);
                subjects.put(mechanism, loginManager.subject());
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
        return securityProtocol == SecurityProtocol.SASL_SSL ? SslConfigs.RECONFIGURABLE_CONFIGS : Collections.<String>emptySet();
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
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool) throws KafkaException {
        try {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            Socket socket = socketChannel.socket();
            TransportLayer transportLayer = buildTransportLayer(id, key, socketChannel);
            Authenticator authenticator;
            if (mode == Mode.SERVER) {
                authenticator = buildServerAuthenticator(configs,
                        saslCallbackHandlers,
                        id,
                        transportLayer,
                        subjects);
            } else {
                LoginManager loginManager = loginManagers.get(clientSaslMechanism);
                authenticator = buildClientAuthenticator(configs,
                        saslCallbackHandlers.get(clientSaslMechanism),
                        id,
                        socket.getInetAddress().getHostName(),
                        loginManager.serviceName(),
                        transportLayer,
                        subjects.get(clientSaslMechanism));
            }
            return new KafkaChannel(id, transportLayer, authenticator, maxReceiveSize, memoryPool != null ? memoryPool : MemoryPool.NONE);
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
    }

    private TransportLayer buildTransportLayer(String id, SelectionKey key, SocketChannel socketChannel) throws IOException {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
            return SslTransportLayer.create(id, key,
                sslFactory.createSslEngine(socketChannel.socket().getInetAddress().getHostName(), socketChannel.socket().getPort()));
        } else {
            return new PlaintextTransportLayer(key);
        }
    }

    // Visible to override for testing
    protected SaslServerAuthenticator buildServerAuthenticator(Map<String, ?> configs,
                                                               Map<String, AuthenticateCallbackHandler> callbackHandlers,
                                                               String id,
                                                               TransportLayer transportLayer,
                                                               Map<String, Subject> subjects) throws IOException {
        return new SaslServerAuthenticator(configs, callbackHandlers, id, subjects,
                kerberosShortNamer, listenerName, securityProtocol, transportLayer);
    }

    // Visible to override for testing
    protected SaslClientAuthenticator buildClientAuthenticator(Map<String, ?> configs,
                                                               AuthenticateCallbackHandler callbackHandler,
                                                               String id,
                                                               String serverHost,
                                                               String servicePrincipal,
                                                               TransportLayer transportLayer, Subject subject) throws IOException {
        return new SaslClientAuthenticator(configs, callbackHandler, id, subject, servicePrincipal,
                serverHost, clientSaslMechanism, handshakeRequestEnable, transportLayer);
    }

    // Package private for testing
    Map<String, LoginManager> loginManagers() {
        return loginManagers;
    }

    private static String defaultKerberosRealm() throws ClassNotFoundException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        //TODO Find a way to avoid using these proprietary classes as access to Java 9 will block access by default
        //due to the Jigsaw module system

        Object kerbConf;
        Class<?> classRef;
        Method getInstanceMethod;
        Method getDefaultRealmMethod;
        if (Java.isIbmJdk()) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        getInstanceMethod = classRef.getMethod("getInstance", new Class[0]);
        kerbConf = getInstanceMethod.invoke(classRef, new Object[0]);
        getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm", new Class[0]);
        return (String) getDefaultRealmMethod.invoke(kerbConf, new Object[0]);
    }

    private void createClientCallbackHandler(Map<String, ?> configs) {
        Class<? extends AuthenticateCallbackHandler> clazz = (Class<? extends AuthenticateCallbackHandler>) configs.get(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS);
        if (clazz == null)
            clazz = clientSaslMechanism.equals(SaslConfigs.GSSAPI_MECHANISM) ? KerberosClientCallbackHandler.class : SaslClientCallbackHandler.class;
        AuthenticateCallbackHandler callbackHandler = Utils.newInstance(clazz);
        saslCallbackHandlers.put(clientSaslMechanism, callbackHandler);
    }

    private void createServerCallbackHandlers(Map<String, ?> configs) throws ClassNotFoundException {
        for (String mechanism : jaasContexts.keySet()) {
            AuthenticateCallbackHandler callbackHandler;
            String prefix = ListenerName.saslMechanismPrefix(mechanism);
            Class<? extends AuthenticateCallbackHandler> clazz =
                    (Class<? extends AuthenticateCallbackHandler>) configs.get(prefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS);
            if (clazz != null)
                callbackHandler = Utils.newInstance(clazz);
            else if (mechanism.equals(PlainSaslServer.PLAIN_MECHANISM))
                callbackHandler = new PlainServerCallbackHandler();
            else if (ScramMechanism.isScram(mechanism))
                callbackHandler = new ScramServerCallbackHandler(credentialCache.cache(mechanism, ScramCredential.class), tokenCache);
            else
                callbackHandler = new SaslServerCallbackHandler();
            saslCallbackHandlers.put(mechanism, callbackHandler);
        }
    }
}
