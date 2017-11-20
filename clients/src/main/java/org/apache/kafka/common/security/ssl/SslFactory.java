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
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class SslFactory implements Reconfigurable {

    private final Mode mode;
    private final String clientAuthConfigOverride;
    private final boolean clientAndServer;

    private String protocol;
    private String provider;
    private String kmfAlgorithm;
    private String tmfAlgorithm;
    private SecurityStore keystore = null;
    private SecurityStore truststore;
    private String[] cipherSuites;
    private String[] enabledProtocols;
    private String endpointIdentification;
    private SecureRandom secureRandomImplementation;
    private SSLContext sslContext;
    private boolean needClientAuth;
    private boolean wantClientAuth;

    public SslFactory(Mode mode) {
        this(mode, null);
    }

    public SslFactory(Mode mode, String clientAuthConfigOverride) {
        this(mode, clientAuthConfigOverride, false);
    }

    public SslFactory(Mode mode, String clientAuthConfigOverride, boolean clientAndServer) {
        this.mode = mode;
        this.clientAuthConfigOverride = clientAuthConfigOverride;
        this.clientAndServer = clientAndServer;
    }

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        this.protocol =  (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);
        this.provider = (String) configs.get(SslConfigs.SSL_PROVIDER_CONFIG);

        @SuppressWarnings("unchecked")
        List<String> cipherSuitesList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuitesList != null)
            this.cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);

        @SuppressWarnings("unchecked")
        List<String> enabledProtocolsList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (enabledProtocolsList != null)
            this.enabledProtocols = enabledProtocolsList.toArray(new String[enabledProtocolsList.size()]);

        String endpointIdentification = (String) configs.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        if (endpointIdentification != null)
            this.endpointIdentification = endpointIdentification;

        String secureRandomImplementation = (String) configs.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        if (secureRandomImplementation != null) {
            try {
                this.secureRandomImplementation = SecureRandom.getInstance(secureRandomImplementation);
            } catch (GeneralSecurityException e) {
                throw new KafkaException(e);
            }
        }

        String clientAuthConfig = clientAuthConfigOverride;
        if (clientAuthConfig == null)
            clientAuthConfig = (String) configs.get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);
        if (clientAuthConfig != null) {
            if (clientAuthConfig.equals("required"))
                this.needClientAuth = true;
            else if (clientAuthConfig.equals("requested"))
                this.wantClientAuth = true;
        }

        this.kmfAlgorithm = (String) configs.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        this.tmfAlgorithm = (String) configs.get(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);

        this.keystore = createKeystore((String) configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                       (String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                       (Password) configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                       (Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));

        this.truststore = createTruststore((String) configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                         (String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                         (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        try {
            this.sslContext = createSSLContext(keystore, false);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public boolean validate(Map<String, ?> configs) {
        maybeCreateNewSSLContext(configs, clientAndServer, true);
        return true; // fails with exception while creating context if configs are invalid
    }

    @Override
    public void reconfigure(Map<String, ?> configs) throws KafkaException {
        SSLContext newSslContext = maybeCreateNewSSLContext(configs, clientAndServer, false);
        if (newSslContext != null)
            this.sslContext = newSslContext;
    }

    private SSLContext maybeCreateNewSSLContext(Map<String, ?> configs, boolean verifyKeystore, boolean validateOnly) throws KafkaException {

        boolean keystoreChanged = Objects.equals(configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG), keystore.type) ||
                Objects.equals(configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG), keystore.path) ||
                Objects.equals(configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), keystore.password) ||
                Objects.equals(configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG), keystore.keyPassword);

        if (keystoreChanged) {
            SecurityStore keystore = createKeystore((String) configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                    (String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                    (Password) configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                    (Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));

            try {
                SSLContext sslContext = createSSLContext(keystore, verifyKeystore);
                if (!validateOnly)
                    this.keystore = keystore;
                return sslContext;
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        } else
            return null;
    }

    // package access for testing
    SSLContext createSSLContext(SecurityStore keystore, boolean verifyKeystore) throws GeneralSecurityException, IOException  {
        SSLContext sslContext;
        if (provider != null)
            sslContext = SSLContext.getInstance(protocol, provider);
        else
            sslContext = SSLContext.getInstance(protocol);

        KeyManager[] keyManagers = null;
        if (keystore != null) {
            String kmfAlgorithm = this.kmfAlgorithm != null ? this.kmfAlgorithm : KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
            KeyStore ks = keystore.load();
            Password keyPassword = keystore.keyPassword != null ? keystore.keyPassword : keystore.password;
            kmf.init(ks, keyPassword.value().toCharArray());
            keyManagers = kmf.getKeyManagers();
        }

        String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        KeyStore ts = truststore == null ? null : truststore.load();
        tmf.init(ts);

        sslContext.init(keyManagers, tmf.getTrustManagers(), this.secureRandomImplementation);
        if (verifyKeystore) {
            SSLConfigValidatorEngine.validate(this, sslContext);
            if (!CertificateEntries.certificateEntries(this.keystore.load()).equals(CertificateEntries.certificateEntries(keystore.load()))) {
                throw new ConfigException("Keystore DN or SubjectAltNames do not match");
            }
        }
        return sslContext;
    }

    public SSLEngine createSslEngine(String peerHost, int peerPort) {
        return createSslEngine(sslContext, peerHost, peerPort);
    }

    private SSLEngine createSslEngine(SSLContext sslContext, String peerHost, int peerPort) {
        SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
        if (cipherSuites != null) sslEngine.setEnabledCipherSuites(cipherSuites);
        if (enabledProtocols != null) sslEngine.setEnabledProtocols(enabledProtocols);

        // SSLParameters#setEndpointIdentificationAlgorithm enables endpoint validation
        // only in client mode. Hence, validation is enabled only for clients.
        if (mode == Mode.SERVER) {
            sslEngine.setUseClientMode(false);
            if (needClientAuth)
                sslEngine.setNeedClientAuth(needClientAuth);
            else
                sslEngine.setWantClientAuth(wantClientAuth);
        } else {
            sslEngine.setUseClientMode(true);
            SSLParameters sslParams = sslEngine.getSSLParameters();
            sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
            sslEngine.setSSLParameters(sslParams);
        }
        return sslEngine;
    }

    /**
     * Returns a configured SSLContext.
     * @return SSLContext.
     */
    public SSLContext sslContext() {
        return sslContext;
    }

    private SecurityStore createKeystore(String type, String path, Password password, Password keyPassword) {
        if (path == null && password != null) {
            throw new KafkaException("SSL key store is not specified, but key store password is specified.");
        } else if (path != null && password == null) {
            throw new KafkaException("SSL key store is specified, but key store password is not specified.");
        } else if (path != null && password != null) {
            return new SecurityStore(type, path, password, keyPassword);
        } else
            return null;
    }

    private SecurityStore createTruststore(String type, String path, Password password) {
        if (path == null && password != null) {
            throw new KafkaException("SSL trust store is not specified, but trust store password is specified.");
        } else if (path != null) {
            return new SecurityStore(type, path, password, null);
        } else
            return null;
    }

    // package access for testing
    static class SecurityStore {
        private final String type;
        private final String path;
        private final Password password;
        private final Password keyPassword;

        SecurityStore(String type, String path, Password password, Password keyPassword) {
            Objects.requireNonNull(type, "type must not be null");
            this.type = type;
            this.path = path;
            this.password = password;
            this.keyPassword = keyPassword;
        }

        KeyStore load() throws GeneralSecurityException, IOException {
            FileInputStream in = null;
            try {
                KeyStore ks = KeyStore.getInstance(type);
                in = new FileInputStream(path);
                // If a password is not set access to the truststore is still available, but integrity checking is disabled.
                char[] passwordChars = password != null ? password.value().toCharArray() : null;
                ks.load(in, passwordChars);
                return ks;
            } finally {
                if (in != null) in.close();
            }
        }
    }

    static class SSLConfigValidatorEngine {
        private static final ByteBuffer EMPTY_BUF = ByteBuffer.allocate(0);
        private final SSLEngine sslEngine;
        private SSLEngineResult handshakeResult;
        private final ByteBuffer appBuffer;
        private final ByteBuffer netBuffer;

        static void validate(SslFactory sslFactory, SSLContext sslContext) throws SSLException {
            SSLConfigValidatorEngine clientEngine = new SSLConfigValidatorEngine(sslFactory, sslContext, Mode.CLIENT);
            SSLConfigValidatorEngine serverEngine = new SSLConfigValidatorEngine(sslFactory, sslContext, Mode.SERVER);
            clientEngine.beginHandshake();
            serverEngine.beginHandshake();
            while (!serverEngine.complete() || !clientEngine.complete()) {
                clientEngine.handshake(serverEngine);
                serverEngine.handshake(clientEngine);
            }
        }

        private SSLConfigValidatorEngine(SslFactory sslFactory, SSLContext sslContext, Mode mode) {
            this.sslEngine = sslFactory.createSslEngine(sslContext, "localhost", 0); // these hints are not used for validation
            sslEngine.setUseClientMode(mode == Mode.CLIENT);
            appBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
            netBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        }

        void beginHandshake() throws SSLException {
            sslEngine.beginHandshake();
        }

        void handshake(SSLConfigValidatorEngine peerEngine) throws SSLException {
            SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
            while (true) {
                switch (handshakeStatus) {
                    case NEED_WRAP:
                        handshakeResult = sslEngine.wrap(EMPTY_BUF, netBuffer);
                        switch (handshakeResult.getStatus()) {
                            case OK: break;
                            case BUFFER_OVERFLOW:
                                netBuffer.compact();
                                Utils.ensureCapacity(netBuffer, sslEngine.getSession().getPacketBufferSize());
                                netBuffer.flip();
                                break;
                            case BUFFER_UNDERFLOW: throw new SSLException("Unexpected status: CLOSED");
                            case CLOSED: throw new SSLException("Unexpected status: CLOSED");
                        }
                        return;
                    case NEED_UNWRAP:
                        if (peerEngine.netBuffer.position() == 0)
                            return;
                        peerEngine.netBuffer.flip();
                        handshakeResult = sslEngine.unwrap(peerEngine.netBuffer, appBuffer);
                        peerEngine.netBuffer.compact();
                        handshakeStatus = handshakeResult.getHandshakeStatus();
                        switch (handshakeResult.getStatus()) {
                            case OK: break;
                            case BUFFER_OVERFLOW:
                                Utils.ensureCapacity(appBuffer, sslEngine.getSession().getApplicationBufferSize());
                                break;
                            case BUFFER_UNDERFLOW:
                                Utils.ensureCapacity(netBuffer, sslEngine.getSession().getPacketBufferSize());
                                break;
                            case CLOSED: throw new SSLException("Unexpected status: CLOSED");
                        }
                        break;
                    case NEED_TASK:
                        sslEngine.getDelegatedTask().run();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    case FINISHED:
                        return;
                    case NOT_HANDSHAKING:
                        if (handshakeResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED)
                            throw new SSLException("Did not finish handshake");
                        return;
                    default:
                        throw new IllegalStateException("Unexpected handshake status " + handshakeStatus);
                }
            }
        }

        boolean complete() {
            return sslEngine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED ||
                    sslEngine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
        }
    }

    static class CertificateEntries {
        private final Principal subjectDn;
        private final Collection<List<?>> subjectAltNames;

        static List<CertificateEntries> certificateEntries(KeyStore keystore) throws GeneralSecurityException, IOException {
            Enumeration<String> aliases = keystore.aliases();
            List<CertificateEntries> entries = new ArrayList<>();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                Certificate cert  = keystore.getCertificate(alias);
                if (cert instanceof X509Certificate)
                    entries.add(new CertificateEntries((X509Certificate) cert));
            }
            return entries;
        }

        CertificateEntries(X509Certificate cert) throws GeneralSecurityException {
            this.subjectDn = cert.getSubjectDN();
            this.subjectAltNames = cert.getSubjectAlternativeNames();
        }

        @Override
        public int hashCode() {
            return Objects.hash(subjectDn, subjectAltNames);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CertificateEntries))
                return false;
            CertificateEntries other = (CertificateEntries) obj;
            return Objects.equals(subjectDn, other.subjectDn) &&
                    Objects.equals(subjectAltNames, other.subjectAltNames);
        }

        @Override
        public String toString() {
            return "subjectDn=" + subjectDn + "subjectAltNames=" + subjectAltNames;
        }
    }
}
