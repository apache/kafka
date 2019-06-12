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
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;

public class SslFactory implements Reconfigurable {
    private static final Logger log = LoggerFactory.getLogger(SslFactory.class);

    private final Mode mode;
    private final String clientAuthConfigOverride;
    private final boolean keystoreVerifiableUsingTruststore;
    private String endpointIdentification;
    private SslEngineBuilder sslEngineBuilder;

    public SslFactory(Mode mode) {
        this(mode, null, false);
    }

    /**
     * Create an SslFactory.
     *
     * @param mode                                  Whether to use client or server mode.
     * @param clientAuthConfigOverride              The value to override ssl.client.auth with, or null
     *                                              if we don't want to override it.
     * @param keystoreVerifiableUsingTruststore     True if we should require the keystore to be verifiable
     *                                              using the truststore.
     */
    public SslFactory(Mode mode,
                      String clientAuthConfigOverride,
                      boolean keystoreVerifiableUsingTruststore) {
        this.mode = mode;
        this.clientAuthConfigOverride = clientAuthConfigOverride;
        this.keystoreVerifiableUsingTruststore = keystoreVerifiableUsingTruststore;
    }

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        if (sslEngineBuilder != null) {
            throw new IllegalStateException("SslFactory was already configured.");
        }
        this.endpointIdentification = (String) configs.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);

        Map<String, Object> nextConfigs = new HashMap<>();
        copyMapEntries(nextConfigs, configs, SslConfigs.NON_RECONFIGURABLE_CONFIGS);
        copyMapEntries(nextConfigs, configs, SslConfigs.RECONFIGURABLE_CONFIGS);
        if (clientAuthConfigOverride != null) {
            nextConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, clientAuthConfigOverride);
        }
        SslEngineBuilder builder = new SslEngineBuilder(nextConfigs);
        if (keystoreVerifiableUsingTruststore) {
            try {
                SslEngineValidator.validate(builder, builder);
            } catch (Exception e) {
                throw new ConfigException("A client SSLEngine created with the provided settings " +
                        "can't connect to a server SSLEngine created with those settings.", e);
            }
        }
        this.sslEngineBuilder = builder;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> newConfigs) {
        createNewSslEngineBuilder(newConfigs);
    }

    @Override
    public void reconfigure(Map<String, ?> newConfigs) throws KafkaException {
        this.sslEngineBuilder = createNewSslEngineBuilder(newConfigs);
    }

    private SslEngineBuilder createNewSslEngineBuilder(Map<String, ?> newConfigs) {
        if (sslEngineBuilder == null) {
            throw new IllegalStateException("SslFactory has not been configured.");
        }
        Map<String, Object> nextConfigs = new HashMap<>(sslEngineBuilder.configs());
        copyMapEntries(nextConfigs, newConfigs, SslConfigs.RECONFIGURABLE_CONFIGS);
        if (clientAuthConfigOverride != null) {
            nextConfigs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, clientAuthConfigOverride);
        }
        if (!sslEngineBuilder.shouldBeRebuilt(nextConfigs)) {
            return sslEngineBuilder;
        }
        try {
            SslEngineBuilder newSslEngineBuilder = new SslEngineBuilder(nextConfigs);
            if (sslEngineBuilder.keystore() == null) {
                if (newSslEngineBuilder.keystore() != null) {
                    throw new ConfigException("Cannot add SSL keystore to an existing listener for " +
                            "which no keystore was configured.");
                }
            } else {
                if (newSslEngineBuilder.keystore() == null) {
                    throw new ConfigException("Cannot remove the SSL keystore from an existing listener for " +
                            "which a keystore was configured.");
                }
                if (!CertificateEntries.create(sslEngineBuilder.keystore().load()).equals(
                        CertificateEntries.create(newSslEngineBuilder.keystore().load()))) {
                    throw new ConfigException("Keystore DistinguishedName or SubjectAltNames do not match");
                }
            }
            if (sslEngineBuilder.truststore() == null && newSslEngineBuilder.truststore() != null) {
                throw new ConfigException("Cannot add SSL truststore to an existing listener for which no " +
                        "truststore was configured.");
            }
            if (keystoreVerifiableUsingTruststore) {
                if (sslEngineBuilder.truststore() != null || sslEngineBuilder.keystore() != null) {
                    SslEngineValidator.validate(sslEngineBuilder, newSslEngineBuilder);
                }
            }
            return newSslEngineBuilder;
        } catch (Exception e) {
            log.debug("Validation of dynamic config update of SSLFactory failed.", e);
            throw new ConfigException("Validation of dynamic config update of SSLFactory failed: " + e);
        }
    }

    public SSLEngine createSslEngine(String peerHost, int peerPort) {
        if (sslEngineBuilder == null) {
            throw new IllegalStateException("SslFactory has not been configured.");
        }
        return sslEngineBuilder.createSslEngine(mode, peerHost, peerPort, endpointIdentification);
    }

    @Deprecated
    public SSLContext sslContext() {
        return sslEngineBuilder.sslContext();
    }

    public SslEngineBuilder sslEngineBuilder() {
        return sslEngineBuilder;
    }

    /**
     * Copy entries from one map into another.
     *
     * @param destMap   The map to copy entries into.
     * @param srcMap    The map to copy entries from.
     * @param keySet    Only entries with these keys will be copied.
     * @param <K>       The map key type.
     * @param <V>       The map value type.
     */
    private static <K, V> void copyMapEntries(Map<K, V> destMap,
                                              Map<K, ? extends V> srcMap,
                                              Set<K> keySet) {
        for (K k : keySet) {
            if (srcMap.containsKey(k)) {
                destMap.put(k, srcMap.get(k));
            }
        }
    }

    static class CertificateEntries {
        private final Principal subjectPrincipal;
        private final Set<List<?>> subjectAltNames;

        static List<CertificateEntries> create(KeyStore keystore) throws GeneralSecurityException {
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
            this.subjectAltNames = altNames != null ? new HashSet<>(altNames) : Collections.emptySet();
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

    /**
     * Validator used to verify dynamic update of keystore used in inter-broker communication.
     * The validator checks that a successful handshake can be performed using the keystore and
     * truststore configured on this SslFactory.
     */
    private static class SslEngineValidator {
        private static final ByteBuffer EMPTY_BUF = ByteBuffer.allocate(0);
        private final SSLEngine sslEngine;
        private SSLEngineResult handshakeResult;
        private ByteBuffer appBuffer;
        private ByteBuffer netBuffer;

        static void validate(SslEngineBuilder oldEngineBuilder,
                             SslEngineBuilder newEngineBuilder) throws SSLException {
            validate(createSslEngineForValidation(oldEngineBuilder, Mode.SERVER),
                    createSslEngineForValidation(newEngineBuilder, Mode.CLIENT));
            validate(createSslEngineForValidation(newEngineBuilder, Mode.SERVER),
                    createSslEngineForValidation(oldEngineBuilder, Mode.CLIENT));
        }

        private static SSLEngine createSslEngineForValidation(SslEngineBuilder sslEngineBuilder, Mode mode) {
            // Use empty hostname, disable hostname verification
            return sslEngineBuilder.createSslEngine(mode, "", 0, "");
        }

        static void validate(SSLEngine clientEngine, SSLEngine serverEngine) throws SSLException {
            SslEngineValidator clientValidator = new SslEngineValidator(clientEngine);
            SslEngineValidator serverValidator = new SslEngineValidator(serverEngine);
            try {
                clientValidator.beginHandshake();
                serverValidator.beginHandshake();
                while (!serverValidator.complete() || !clientValidator.complete()) {
                    clientValidator.handshake(serverValidator);
                    serverValidator.handshake(clientValidator);
                }
            } finally {
                clientValidator.close();
                serverValidator.close();
            }
        }

        private SslEngineValidator(SSLEngine engine) {
            this.sslEngine = engine;
            appBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
            netBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        }

        void beginHandshake() throws SSLException {
            sslEngine.beginHandshake();
        }
        void handshake(SslEngineValidator peerValidator) throws SSLException {
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
                        if (peerValidator.netBuffer.position() == 0) // no data to unwrap, return to process peer
                            return;
                        peerValidator.netBuffer.flip(); // unwrap the data from peer
                        handshakeResult = sslEngine.unwrap(peerValidator.netBuffer, appBuffer);
                        peerValidator.netBuffer.compact();
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
}
