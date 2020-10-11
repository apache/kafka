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
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

public final class DefaultSslEngineFactory implements SslEngineFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultSslEngineFactory.class);
    public static final String PEM_TYPE = "PEM";

    private Map<String, ?> configs;
    private String protocol;
    private String provider;
    private String kmfAlgorithm;
    private String tmfAlgorithm;
    private SecurityStore keystore;
    private SecurityStore truststore;
    private String[] cipherSuites;
    private String[] enabledProtocols;
    private SecureRandom secureRandomImplementation;
    private SSLContext sslContext;
    private SslClientAuth sslClientAuth;


    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
        return createSslEngine(Mode.CLIENT, peerHost, peerPort, endpointIdentification);
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        return createSslEngine(Mode.SERVER, peerHost, peerPort, null);
    }

    @Override
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

    @Override
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public KeyStore keystore() {
        return this.keystore != null ? this.keystore.get() : null;
    }

    @Override
    public KeyStore truststore() {
        return this.truststore != null ? this.truststore.get() : null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = Collections.unmodifiableMap(configs);
        this.protocol = (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);
        this.provider = (String) configs.get(SslConfigs.SSL_PROVIDER_CONFIG);
        SecurityUtils.addConfiguredSecurityProviders(this.configs);

        List<String> cipherSuitesList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuitesList != null && !cipherSuitesList.isEmpty()) {
            this.cipherSuites = cipherSuitesList.toArray(new String[0]);
        } else {
            this.cipherSuites = null;
        }

        List<String> enabledProtocolsList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (enabledProtocolsList != null && !enabledProtocolsList.isEmpty()) {
            this.enabledProtocols = enabledProtocolsList.toArray(new String[0]);
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
                (Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_KEY_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG));

        this.truststore = createTruststore((String) configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                (String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG));

        this.sslContext = createSSLContext(keystore, truststore);
    }

    @Override
    public void close() {
        this.sslContext = null;
    }

    //For Test only
    public SSLContext sslContext() {
        return this.sslContext;
    }

    private SSLEngine createSslEngine(Mode mode, String peerHost, int peerPort, String endpointIdentification) {
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
    private static SslClientAuth createSslClientAuth(String key) {
        SslClientAuth auth = SslClientAuth.forConfig(key);
        if (auth != null) {
            return auth;
        }
        log.warn("Unrecognized client authentication configuration {}.  Falling " +
                "back to NONE.  Recognized client authentication configurations are {}.",
                key, String.join(", ", SslClientAuth.VALUES.stream().
                        map(Enum::name).collect(Collectors.toList())));
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

    private SSLContext createSSLContext(SecurityStore keystore, SecurityStore truststore) {
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
                    kmf.init(keystore.get(), keystore.keyPassword());
                } else {
                    kmf.init(null, null);
                }
                keyManagers = kmf.getKeyManagers();
            }

            String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
            KeyStore ts = truststore == null ? null : truststore.get();
            tmf.init(ts);

            sslContext.init(keyManagers, tmf.getTrustManagers(), this.secureRandomImplementation);
            log.debug("Created SSL context with keystore {}, truststore {}, provider {}.",
                    keystore, truststore, sslContext.getProvider().getName());
            return sslContext;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    // Visibility to override for testing
    protected SecurityStore createKeystore(String type, String path, Password password, Password keyPassword, Password privateKey, Password certificateChain) {
        if (privateKey != null) {
            if (!PEM_TYPE.equals(type))
                throw new InvalidConfigurationException("SSL private key can be specified only for PEM, but key store type is " + type + ".");
            else if (certificateChain == null)
                throw new InvalidConfigurationException("SSL private key is specified, but certificate chain is not specified.");
            else if (path != null)
                throw new InvalidConfigurationException("Both SSL key store location and separate private key are specified.");
            else if (password != null)
                throw new InvalidConfigurationException("SSL key store password cannot be specified with PEM format, only key password may be specified.");
            else
                return new PemStore(certificateChain, privateKey, keyPassword);
        } else if (certificateChain != null) {
            throw new InvalidConfigurationException("SSL certificate chain is specified, but private key is not specified");
        } else if (PEM_TYPE.equals(type) && path != null) {
            if (password != null)
                throw new InvalidConfigurationException("SSL key store password cannot be specified with PEM format, only key password may be specified");
            else if (keyPassword == null)
                throw new InvalidConfigurationException("SSL PEM key store is specified, but key password is not specified.");
            else
                return new FileBasedPemStore(path, keyPassword, true);
        } else if (path == null && password != null) {
            throw new InvalidConfigurationException("SSL key store is not specified, but key store password is specified.");
        } else if (path != null && password == null) {
            throw new InvalidConfigurationException("SSL key store is specified, but key store password is not specified.");
        } else if (path != null && password != null) {
            return new FileBasedStore(type, path, password, keyPassword, true);
        } else
            return null; // path == null, clients may use this path with brokers that don't require client auth
    }

    private static SecurityStore createTruststore(String type, String path, Password password, Password trustStoreCerts) {
        if (trustStoreCerts != null) {
            if (!PEM_TYPE.equals(type))
                throw new InvalidConfigurationException("SSL trust store certs can be specified only for PEM, but trust store type is " + type + ".");
            else if (path != null)
                throw new InvalidConfigurationException("Both SSL trust store location and separate trust certificates are specified.");
            else if (password != null)
                throw new InvalidConfigurationException("SSL trust store password cannot be specified for PEM format.");
            else
                return new PemStore(trustStoreCerts);
        } else if (PEM_TYPE.equals(type) && path != null) {
            if (password != null)
                throw new InvalidConfigurationException("SSL trust store password cannot be specified for PEM format.");
            else
                return new FileBasedPemStore(path, null, false);
        } else if (path == null && password != null) {
            throw new InvalidConfigurationException("SSL trust store is not specified, but trust store password is specified.");
        } else if (path != null) {
            return new FileBasedStore(type, path, password, null, false);
        } else
            return null;
    }

    static interface SecurityStore {
        KeyStore get();
        char[] keyPassword();
        boolean modified();
    }

    // package access for testing
    static class FileBasedStore implements SecurityStore {
        private final String type;
        protected final String path;
        private final Password password;
        protected final Password keyPassword;
        private final Long fileLastModifiedMs;
        private final KeyStore keyStore;

        FileBasedStore(String type, String path, Password password, Password keyPassword, boolean isKeyStore) {
            Objects.requireNonNull(type, "type must not be null");
            this.type = type;
            this.path = path;
            this.password = password;
            this.keyPassword = keyPassword;
            fileLastModifiedMs = lastModifiedMs(path);
            this.keyStore = load(isKeyStore);
        }

        @Override
        public KeyStore get() {
            return keyStore;
        }

        @Override
        public char[] keyPassword() {
            Password passwd = keyPassword != null ? keyPassword : password;
            return passwd == null ? null : passwd.value().toCharArray();
        }

        /**
         * Loads this keystore
         * @return the keystore
         * @throws KafkaException if the file could not be read or if the keystore could not be loaded
         *   using the specified configs (e.g. if the password or keystore type is invalid)
         */
        protected KeyStore load(boolean isKeyStore) {
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

        public boolean modified() {
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

    static class FileBasedPemStore extends FileBasedStore {
        FileBasedPemStore(String path, Password keyPassword, boolean isKeyStore) {
            super(PEM_TYPE, path, null, keyPassword, isKeyStore);
        }

        @Override
        protected KeyStore load(boolean isKeyStore) {
            try {
                Password storeContents = new Password(Utils.readFileAsString(path));
                PemStore pemStore = isKeyStore ? new PemStore(storeContents, storeContents, keyPassword) :
                    new PemStore(storeContents);
                return pemStore.keyStore;
            } catch (Exception e) {
                throw new InvalidConfigurationException("Failed to load PEM SSL keystore " + path, e);
            }
        }
    }

    static class PemStore implements SecurityStore {
        private static final PemParser CERTIFICATE_PARSER = new PemParser("CERTIFICATE");
        private static final PemParser PRIVATE_KEY_PARSER = new PemParser("PRIVATE KEY");
        private static final List<KeyFactory> KEY_FACTORIES = Arrays.asList(
                keyFactory("RSA"),
                keyFactory("DSA"),
                keyFactory("EC")
        );

        private final char[] keyPassword;
        private final KeyStore keyStore;

        PemStore(Password certificateChain, Password privateKey, Password keyPassword) {
            this.keyPassword = keyPassword == null ? null : keyPassword.value().toCharArray();
            keyStore = createKeyStoreFromPem(privateKey.value(), certificateChain.value(), this.keyPassword);
        }

        PemStore(Password trustStoreCerts) {
            this.keyPassword = null;
            keyStore = createTrustStoreFromPem(trustStoreCerts.value());
        }

        @Override
        public KeyStore get() {
            return keyStore;
        }

        @Override
        public char[] keyPassword() {
            return keyPassword;
        }

        @Override
        public boolean modified() {
            return false;
        }

        private KeyStore createKeyStoreFromPem(String privateKeyPem, String certChainPem, char[] keyPassword) {
            try {
                KeyStore ks = KeyStore.getInstance("PKCS12");
                ks.load(null, null);
                Key key = privateKey(privateKeyPem, keyPassword);
                Certificate[] certChain = certs(certChainPem);
                ks.setKeyEntry("kafka", key, keyPassword, certChain);
                return ks;
            } catch (Exception e) {
                throw new InvalidConfigurationException("Invalid PEM keystore configs", e);
            }
        }

        private KeyStore createTrustStoreFromPem(String trustedCertsPem) {
            try {
                KeyStore ts = KeyStore.getInstance("PKCS12");
                ts.load(null, null);
                Certificate[] certs = certs(trustedCertsPem);
                for (int i = 0; i < certs.length; i++) {
                    ts.setCertificateEntry("kafka" + i, certs[i]);
                }
                return ts;
            } catch (InvalidConfigurationException e) {
                throw e;
            } catch (Exception e) {
                throw new InvalidConfigurationException("Invalid PEM keystore configs", e);
            }
        }

        private Certificate[] certs(String pem) throws GeneralSecurityException {
            List<byte[]> certEntries = CERTIFICATE_PARSER.pemEntries(pem);
            if (certEntries.isEmpty())
                throw new InvalidConfigurationException("At least one certificate expected, but none found");

            Certificate[] certs = new Certificate[certEntries.size()];
            for (int i = 0; i < certs.length; i++) {
                certs[i] = CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(certEntries.get(i)));
            }
            return certs;
        }

        private PrivateKey privateKey(String pem, char[] keyPassword) throws Exception {
            List<byte[]> keyEntries = PRIVATE_KEY_PARSER.pemEntries(pem);
            if (keyEntries.isEmpty())
                throw new InvalidConfigurationException("Private key not provided");
            if (keyEntries.size() != 1)
                throw new InvalidConfigurationException("Expected one private key, but found " + keyEntries.size());

            byte[] keyBytes = keyEntries.get(0);
            PKCS8EncodedKeySpec keySpec;
            if (keyPassword == null) {
                keySpec = new PKCS8EncodedKeySpec(keyBytes);
            } else {
                EncryptedPrivateKeyInfo keyInfo = new EncryptedPrivateKeyInfo(keyBytes);
                String algorithm = keyInfo.getAlgName();
                SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(algorithm);
                SecretKey pbeKey = keyFactory.generateSecret(new PBEKeySpec(keyPassword));
                Cipher cipher = Cipher.getInstance(algorithm);
                cipher.init(Cipher.DECRYPT_MODE, pbeKey, keyInfo.getAlgParameters());
                keySpec = keyInfo.getKeySpec(cipher);
            }

            InvalidKeySpecException firstException = null;
            for (KeyFactory factory : KEY_FACTORIES) {
                try {
                    return factory.generatePrivate(keySpec);
                } catch (InvalidKeySpecException e) {
                    if (firstException == null)
                        firstException = e;
                }
            }
            throw new InvalidConfigurationException("Private key could not be loaded", firstException);
        }

        private static KeyFactory keyFactory(String algorithm) {
            try {
                return KeyFactory.getInstance(algorithm);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Could not create key factory for algorithm " + algorithm, e);
            }
        }
    }

    /**
     * Parser to process certificate/private key entries from PEM files
     * Examples:
     *   -----BEGIN CERTIFICATE-----
     *   Base64 cert
     *   -----END CERTIFICATE-----
     *
     *   -----BEGIN ENCRYPTED PRIVATE KEY-----
     *   Base64 private key
     *   -----END ENCRYPTED PRIVATE KEY-----
     *   Additional data may be included before headers, so we match all entries within the PEM.
     */
    static class PemParser {
        private final String name;
        private final Pattern pattern;

        PemParser(String name) {
            this.name = name;
            String beginOrEndFormat = "-+%s\\s*.*%s[^-]*-+\\s+";
            String nameIgnoreSpace = name.replace(" ", "\\s+");

            String encodingParams = "\\s*[^\\r\\n]*:[^\\r\\n]*[\\r\\n]+";
            String base64Pattern = "([a-zA-Z0-9/+=\\s]*)";
            String patternStr =  String.format(beginOrEndFormat, "BEGIN", nameIgnoreSpace) +
                String.format("(?:%s)*", encodingParams) +
                base64Pattern +
                String.format(beginOrEndFormat, "END", nameIgnoreSpace);
            pattern = Pattern.compile(patternStr);
        }

        private List<byte[]> pemEntries(String pem) {
            Matcher matcher = pattern.matcher(pem + "\n"); // allow last newline to be omitted in value
            List<byte[]>  entries = new ArrayList<>();
            while (matcher.find()) {
                String base64Str = matcher.group(1).replaceAll("\\s", "");
                entries.add(Base64.getDecoder().decode(base64Str));
            }
            if (entries.isEmpty())
                throw new InvalidConfigurationException("No matching " + name + " entries in PEM file");
            return entries;
        }
    }
}
