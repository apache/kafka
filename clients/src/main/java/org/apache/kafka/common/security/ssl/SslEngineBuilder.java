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
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SslEngineBuilder {
    private static final Logger log = LoggerFactory.getLogger(SslEngineBuilder.class);

    private final Map<String, ?> configs;
    private final String protocol;
    private final String provider;
    private final String kmfAlgorithm;
    private final String tmfAlgorithm;
    private final SecurityStore keystore;
    private final SecurityStore truststore;
    private final String[] cipherSuites;
    private final String[] enabledProtocols;
    private final SecureRandom secureRandomImplementation;
    private final SSLContext sslContext;
    private final SslClientAuth sslClientAuth;

    @SuppressWarnings("unchecked")
    SslEngineBuilder(Map<String, ?> configs) {
        this.configs = Collections.unmodifiableMap(configs);
        this.protocol = (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);
        this.provider = (String) configs.get(SslConfigs.SSL_PROVIDER_CONFIG);

        List<String> cipherSuitesList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuitesList != null && !cipherSuitesList.isEmpty()) {
            this.cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);
        } else {
            this.cipherSuites = null;
        }

        List<String> enabledProtocolsList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (enabledProtocolsList != null && !enabledProtocolsList.isEmpty()) {
            this.enabledProtocols = enabledProtocolsList.toArray(new String[enabledProtocolsList.size()]);
        } else {
            this.enabledProtocols = null;
        }

        this.secureRandomImplementation = createSecureRandom((String)
                configs.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG));

        this.sslClientAuth = createSslClientAuth((String) configs.get(
                BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG));

        this.kmfAlgorithm = (String) configs.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        this.tmfAlgorithm = (String) configs.get(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);

        this.keystore = createKeystore((String) configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                (String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));

        this.truststore = createTruststore((String) configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                (String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));

        this.sslContext = createSSLContext();
    }

    private static SslClientAuth createSslClientAuth(String key) {
        SslClientAuth auth = SslClientAuth.forConfig(key);
        if (auth != null) {
            return auth;
        }
        log.warn("Unrecognized client authentication configuration {}.  Falling " +
                "back to NONE.  Recognized client authentication configurations are {}.",
                key, String.join(", ", SslClientAuth.VALUES.stream().
                        map(a -> a.name()).collect(Collectors.toList())));
        return SslClientAuth.NONE;
    }

    private static SecureRandom createSecureRandom(String key) {
        if (key == null) {
            return null;
        }
        try {
            return SecureRandom.getInstance(key);
        } catch (GeneralSecurityException e) {
            throw new KafkaException(e);
        }
    }

    private SSLContext createSSLContext() {
        try {
            SSLContext sslContext;
            if (provider != null)
                sslContext = SSLContext.getInstance(protocol, provider);
            else
                sslContext = SSLContext.getInstance(protocol);

            KeyManager[] keyManagers = null;
            if (keystore != null || kmfAlgorithm != null) {
                String kmfAlgorithm = this.kmfAlgorithm != null ?
                        this.kmfAlgorithm : KeyManagerFactory.getDefaultAlgorithm();
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
                if (keystore != null) {
                    KeyStore ks = keystore.load();
                    Password keyPassword = keystore.keyPassword != null ? keystore.keyPassword : keystore.password;
                    kmf.init(ks, keyPassword.value().toCharArray());
                } else {
                    kmf.init(null, null);
                }
                keyManagers = kmf.getKeyManagers();
            }

            String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
            KeyStore ts = truststore == null ? null : truststore.load();
            tmf.init(ts);

            sslContext.init(keyManagers, tmf.getTrustManagers(), this.secureRandomImplementation);
            log.debug("Created SSL context with keystore {}, truststore {}", keystore, truststore);
            return sslContext;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private static SecurityStore createKeystore(String type, String path, Password password, Password keyPassword) {
        if (path == null && password != null) {
            throw new KafkaException("SSL key store is not specified, but key store password is specified.");
        } else if (path != null && password == null) {
            throw new KafkaException("SSL key store is specified, but key store password is not specified.");
        } else if (path != null && password != null) {
            return new SecurityStore(type, path, password, keyPassword);
        } else
            return null; // path == null, clients may use this path with brokers that don't require client auth
    }

    private static SecurityStore createTruststore(String type, String path, Password password) {
        if (path == null && password != null) {
            throw new KafkaException("SSL trust store is not specified, but trust store password is specified.");
        } else if (path != null) {
            return new SecurityStore(type, path, password, null);
        } else
            return null;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> configs() {
        return (Map<String, Object>) configs;
    }

    public SecurityStore keystore() {
        return keystore;
    }

    public SecurityStore truststore() {
        return truststore;
    }

    /**
     * Create a new SSLEngine object.
     *
     * @param mode      Whether to use client or server mode.
     * @param peerHost  The peer host to use. This is used in client mode if endpoint validation is enabled.
     * @param peerPort  The peer port to use. This is a hint and not used for validation.
     * @param endpointIdentification Endpoint identification algorithm for client mode.
     * @return          The new SSLEngine.
     */
    public SSLEngine createSslEngine(Mode mode, String peerHost, int peerPort, String endpointIdentification) {
        SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
        if (cipherSuites != null) sslEngine.setEnabledCipherSuites(cipherSuites);
        if (enabledProtocols != null) sslEngine.setEnabledProtocols(enabledProtocols);

        if (mode == Mode.SERVER) {
            sslEngine.setUseClientMode(false);
            switch (sslClientAuth) {
                case REQUIRED:
                    sslEngine.setNeedClientAuth(true);
                    break;
                case REQUESTED:
                    sslEngine.setWantClientAuth(true);
                    break;
                case NONE:
                    break;
            }
            sslEngine.setUseClientMode(false);
        } else {
            sslEngine.setUseClientMode(true);
            SSLParameters sslParams = sslEngine.getSSLParameters();
            // SSLParameters#setEndpointIdentificationAlgorithm enables endpoint validation
            // only in client mode. Hence, validation is enabled only for clients.
            sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
            sslEngine.setSSLParameters(sslParams);
        }
        return sslEngine;
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    /**
     * Returns true if this SslEngineBuilder needs to be rebuilt.
     *
     * @param nextConfigs       The configuration we want to use.
     * @return                  True only if this builder should be rebuilt.
     */
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        if (!nextConfigs.equals(configs)) {
            return true;
        }
        if (truststore != null && truststore.modified()) {
            return true;
        }
        if (keystore != null && keystore.modified()) {
            return true;
        }
        return false;
    }

    // package access for testing
    static class SecurityStore {
        private final String type;
        private final String path;
        private final Password password;
        private final Password keyPassword;
        private final Long fileLastModifiedMs;

        SecurityStore(String type, String path, Password password, Password keyPassword) {
            Objects.requireNonNull(type, "type must not be null");
            this.type = type;
            this.path = path;
            this.password = password;
            this.keyPassword = keyPassword;
            fileLastModifiedMs = lastModifiedMs(path);
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

        private Long lastModifiedMs(String path) {
            try {
                return Files.getLastModifiedTime(Paths.get(path)).toMillis();
            } catch (IOException e) {
                log.error("Modification time of key store could not be obtained: " + path, e);
                return null;
            }
        }

        boolean modified() {
            Long modifiedMs = lastModifiedMs(path);
            return modifiedMs != null && !Objects.equals(modifiedMs, this.fileLastModifiedMs);
        }

        @Override
        public String toString() {
            return "SecurityStore(" +
                    "path=" + path +
                    ", modificationTime=" + (fileLastModifiedMs == null ? null : new Date(fileLastModifiedMs)) + ")";
        }
    }
}
