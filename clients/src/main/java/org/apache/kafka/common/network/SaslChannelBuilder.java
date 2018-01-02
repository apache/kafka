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
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.security.authenticator.SaslServerAuthenticator;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.utils.Java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

public class SaslChannelBuilder implements ChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(SaslChannelBuilder.class);

    private final SecurityProtocol securityProtocol;
    private final ListenerName listenerName;
    private final String clientSaslMechanism;
    private final Mode mode;
    private final JaasContext jaasContext;
    private final boolean handshakeRequestEnable;
    private final CredentialCache credentialCache;

    private LoginManager loginManager;
    private SslFactory sslFactory;
    private Map<String, ?> configs;
    private KerberosShortNamer kerberosShortNamer;

    public SaslChannelBuilder(Mode mode,
                              JaasContext jaasContext,
                              SecurityProtocol securityProtocol,
                              ListenerName listenerName,
                              String clientSaslMechanism,
                              boolean handshakeRequestEnable,
                              CredentialCache credentialCache) {
        this.mode = mode;
        this.jaasContext = jaasContext;
        this.securityProtocol = securityProtocol;
        this.listenerName = listenerName;
        this.handshakeRequestEnable = handshakeRequestEnable;
        this.clientSaslMechanism = clientSaslMechanism;
        this.credentialCache = credentialCache;
    }

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            this.configs = configs;
            boolean hasKerberos;
            if (mode == Mode.SERVER) {
                @SuppressWarnings("unchecked")
                List<String> enabledMechanisms = (List<String>) this.configs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG);
                hasKerberos = enabledMechanisms == null || enabledMechanisms.contains(SaslConfigs.GSSAPI_MECHANISM);
            } else {
                hasKerberos = clientSaslMechanism.equals(SaslConfigs.GSSAPI_MECHANISM);
            }

            if (hasKerberos) {
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
            }
            this.loginManager = LoginManager.acquireLoginManager(jaasContext, hasKerberos, configs);

            if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
                // Disable SSL client authentication as we are using SASL authentication
                this.sslFactory = new SslFactory(mode, "none");
                this.sslFactory.configure(configs);
            }
        } catch (Exception e) {
            close();
            throw new KafkaException(e);
        }
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool) throws KafkaException {
        try {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            Socket socket = socketChannel.socket();
            TransportLayer transportLayer = buildTransportLayer(id, key, socketChannel);
            Authenticator authenticator;
            if (mode == Mode.SERVER)
                authenticator = buildServerAuthenticator(configs, id, transportLayer, loginManager.subject());
            else
                authenticator = buildClientAuthenticator(configs, id, socket.getInetAddress().getHostName(),
                        loginManager.serviceName(), transportLayer, loginManager.subject());
            return new KafkaChannel(id, transportLayer, authenticator, maxReceiveSize, memoryPool != null ? memoryPool : MemoryPool.NONE);
        } catch (Exception e) {
            log.info("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    @Override
    public void close()  {
        if (loginManager != null) {
            loginManager.release();
            loginManager = null;
        }
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
    protected SaslServerAuthenticator buildServerAuthenticator(Map<String, ?> configs, String id,
            TransportLayer transportLayer, Subject subject) throws IOException {
        return new SaslServerAuthenticator(configs, id, jaasContext, subject,
                kerberosShortNamer, credentialCache, listenerName, securityProtocol, transportLayer);
    }

    // Visible to override for testing
    protected SaslClientAuthenticator buildClientAuthenticator(Map<String, ?> configs, String id,
            String serverHost, String servicePrincipal, TransportLayer transportLayer, Subject subject) throws IOException {
        return new SaslClientAuthenticator(configs, id, subject, servicePrincipal,
                serverHost, clientSaslMechanism, handshakeRequestEnable, transportLayer);
    }

    // Package private for testing
    LoginManager loginManager() {
        return loginManager;
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
}
