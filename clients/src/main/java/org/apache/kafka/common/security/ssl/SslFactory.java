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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;


public class SslFactory implements Reconfigurable {
    private final Mode mode;
    private final String clientAuthConfigOverride;
    private final boolean keystoreVerifiableUsingTruststore;

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
        this(mode, null, false);
    }

    public SslFactory(Mode mode, String clientAuthConfigOverride, boolean keystoreVerifiableUsingTruststore) {
        this.mode = mode;
        this.clientAuthConfigOverride = clientAuthConfigOverride;
        this.keystoreVerifiableUsingTruststore = keystoreVerifiableUsingTruststore;
    }

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        this.protocol =  (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);
        this.provider = (String) configs.get(SslConfigs.SSL_PROVIDER_CONFIG);

        @SuppressWarnings("unchecked")
        List<String> cipherSuitesList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuitesList != null && !cipherSuitesList.isEmpty())
            this.cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);

        @SuppressWarnings("unchecked")
        List<String> enabledProtocolsList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (enabledProtocolsList != null && !enabledProtocolsList.isEmpty())
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
            this.sslContext = createSSLContext(keystore, truststore);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
        try {
            SecurityStore newKeystore = maybeCreateNewKeystore(configs);
            SecurityStore newTruststore = maybeCreateNewTruststore(configs);
            if (newKeystore != null || newTruststore != null) {
                SecurityStore keystore = newKeystore != null ? newKeystore : this.keystore;
                SecurityStore truststore = newTruststore != null ? newTruststore : this.truststore;
                createSSLContext(keystore, truststore);
            }
        } catch (Exception e) {
            throw new ConfigException("Validation of dynamic config update failed", e);
        }
    }

    @Override
    public void reconfigure(Map<String, ?> configs) throws KafkaException {
        SecurityStore newKeystore = maybeCreateNewKeystore(configs);
        SecurityStore newTruststore = maybeCreateNewTruststore(configs);
        if (newKeystore != null || newTruststore != null) {
            try {
                SecurityStore keystore = newKeystore != null ? newKeystore : this.keystore;
                SecurityStore truststore = newTruststore != null ? newTruststore : this.truststore;
                this.sslContext = createSSLContext(keystore, truststore);
                this.keystore = keystore;
                this.truststore = truststore;
            } catch (Exception e) {
                throw new ConfigException("Reconfiguration of SSL keystore/truststore failed", e);
            }
        }
    }

    private SecurityStore maybeCreateNewKeystore(Map<String, ?> configs) {
        boolean keystoreChanged = !Objects.equals(configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG), keystore.type) ||
                !Objects.equals(configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG), keystore.path) ||
                !Objects.equals(configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), keystore.password) ||
                !Objects.equals(configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG), keystore.keyPassword);

        if (keystoreChanged) {
            return createKeystore((String) configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                    (String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                    (Password) configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                    (Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        } else
            return null;
    }

    private SecurityStore maybeCreateNewTruststore(Map<String, ?> configs) {
        boolean truststoreChanged = !Objects.equals(configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG), truststore.type) ||
                !Objects.equals(configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG), truststore.path) ||
                !Objects.equals(configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG), truststore.password);

        if (truststoreChanged) {
            return createTruststore((String) configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                    (String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                    (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        } else
            return null;
    }

    // package access for testing
    SSLContext createSSLContext(SecurityStore keystore, SecurityStore truststore) throws GeneralSecurityException, IOException  {
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
        boolean verifyKeystore = keystore != null && keystore != this.keystore;
        boolean verifyTruststore = truststore != null && truststore != this.truststore;
        if (verifyKeystore || verifyTruststore) {
            if (this.keystore == null)
                throw new ConfigException("Cannot add SSL keystore to an existing listener for which no keystore was configured.");
            if (keystoreVerifiableUsingTruststore) {
                SSLConfigValidatorEngine.validate(this, sslContext, this.sslContext);
                SSLConfigValidatorEngine.validate(this, this.sslContext, sslContext);
            }
            if (verifyKeystore &&
                    !CertificateEntries.create(this.keystore.load()).equals(CertificateEntries.create(keystore.load()))) {
                throw new ConfigException("Keystore DistinguishedName or SubjectAltNames do not match");
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
            return null; // path == null, clients may use this path with brokers that don't require client auth
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

        /**
         * Loads this keystore
         * @return the keystore
         * @throws KafkaException if the file could not be read or if the keystore could not be loaded
         *   using the specified configs (e.g. if the password or keystore type is invalid)
         */
        KeyStore load() {
            try (InputStream in = Files.newInputStream(Paths.get(path))) {
                KeyStore ks = KeyStore.getInstance(type);
                // If a password is not set access to the truststore is still available, but integrity checking is disabled.
                char[] passwordChars = password != null ? password.value().toCharArray() : null;
                ks.load(in, passwordChars);
                return ks;
            } catch (GeneralSecurityException | IOException e) {
                throw new KafkaException("Failed to load SSL keystore " + path + " of type " + type, e);
            }
        }
    }

    /**
     * Validator used to verify dynamic update of keystore used in inter-broker communication.
     * The validator checks that a successful handshake can be performed using the keystore and
     * truststore configured on this SslFactory.
     */
    private static class SSLConfigValidatorEngine {
        private static final ByteBuffer EMPTY_BUF = ByteBuffer.allocate(0);
        private final SSLEngine sslEngine;
        private SSLEngineResult handshakeResult;
        private ByteBuffer appBuffer;
        private ByteBuffer netBuffer;

        static void validate(SslFactory sslFactory, SSLContext clientSslContext, SSLContext serverSslContext) throws SSLException {
            SSLConfigValidatorEngine clientEngine = new SSLConfigValidatorEngine(sslFactory, clientSslContext, Mode.CLIENT);
            SSLConfigValidatorEngine serverEngine = new SSLConfigValidatorEngine(sslFactory, serverSslContext, Mode.SERVER);
            try {
                clientEngine.beginHandshake();
                serverEngine.beginHandshake();
                while (!serverEngine.complete() || !clientEngine.complete()) {
                    clientEngine.handshake(serverEngine);
                    serverEngine.handshake(clientEngine);
                }
            } finally {
                clientEngine.close();
                serverEngine.close();
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
                                netBuffer = Utils.ensureCapacity(netBuffer, sslEngine.getSession().getPacketBufferSize());
                                netBuffer.flip();
                                break;
                            case BUFFER_UNDERFLOW:
                            case CLOSED:
                            default:
                                throw new SSLException("Unexpected handshake status: " + handshakeResult.getStatus());
                        }
                        return;
                    case NEED_UNWRAP:
                        if (peerEngine.netBuffer.position() == 0) // no data to unwrap, return to process peer
                            return;
                        peerEngine.netBuffer.flip(); // unwrap the data from peer
                        handshakeResult = sslEngine.unwrap(peerEngine.netBuffer, appBuffer);
                        peerEngine.netBuffer.compact();
                        handshakeStatus = handshakeResult.getHandshakeStatus();
                        switch (handshakeResult.getStatus()) {
                            case OK: break;
                            case BUFFER_OVERFLOW:
                                appBuffer = Utils.ensureCapacity(appBuffer, sslEngine.getSession().getApplicationBufferSize());
                                break;
                            case BUFFER_UNDERFLOW:
                                netBuffer = Utils.ensureCapacity(netBuffer, sslEngine.getSession().getPacketBufferSize());
                                break;
                            case CLOSED:
                            default:
                                throw new SSLException("Unexpected handshake status: " + handshakeResult.getStatus());
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

        void close() {
            sslEngine.closeOutbound();
            try {
                sslEngine.closeInbound();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    static class CertificateEntries {
        private final Principal subjectPrincipal;
        private final Set<List<?>> subjectAltNames;

        static List<CertificateEntries> create(KeyStore keystore) throws GeneralSecurityException, IOException {
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
            this.subjectPrincipal = cert.getSubjectX500Principal();
            Collection<List<?>> altNames = cert.getSubjectAlternativeNames();
            // use a set for comparison
            this.subjectAltNames = altNames != null ? new HashSet<>(altNames) : Collections.<List<?>>emptySet();
        }

        @Override
        public int hashCode() {
            return Objects.hash(subjectPrincipal, subjectAltNames);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CertificateEntries))
                return false;
            CertificateEntries other = (CertificateEntries) obj;
            return Objects.equals(subjectPrincipal, other.subjectPrincipal) &&
                    Objects.equals(subjectAltNames, other.subjectAltNames);
        }

        @Override
        public String toString() {
            return "subjectPrincipal=" + subjectPrincipal +
                    ", subjectAltNames=" + subjectAltNames;
        }
    }
}
