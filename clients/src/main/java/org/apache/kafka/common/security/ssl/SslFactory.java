/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.config.types.Password;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

public class SslFactory implements Configurable {

    private final Mode mode;
    private final String clientAuthConfigOverride;

    private String protocol;
    private String provider;
    private String kmfAlgorithm;
    private String tmfAlgorithm;
    private SecurityStore keystore = null;
    private Password keyPassword;
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
        this.mode = mode;
        this.clientAuthConfigOverride = clientAuthConfigOverride;
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
            clientAuthConfig = (String) configs.get(SslConfigs.SSL_CLIENT_AUTH_CONFIG);
        if (clientAuthConfig != null) {
            if (clientAuthConfig.equals("required"))
                this.needClientAuth = true;
            else if (clientAuthConfig.equals("requested"))
                this.wantClientAuth = true;
        }

        this.kmfAlgorithm = (String) configs.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        this.tmfAlgorithm = (String) configs.get(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);

        createKeystore((String) configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                       (String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                       (Password) configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                       (Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));

        createTruststore((String) configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                         (String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                         (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        try {
            this.sslContext = createSSLContext();
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }


    private SSLContext createSSLContext() throws GeneralSecurityException, IOException  {
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
            Password keyPassword = this.keyPassword != null ? this.keyPassword : keystore.password;
            kmf.init(ks, keyPassword.value().toCharArray());
            keyManagers = kmf.getKeyManagers();
        }

        String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        KeyStore ts = truststore == null ? null : truststore.load();
        tmf.init(ts);

        sslContext.init(keyManagers, tmf.getTrustManagers(), this.secureRandomImplementation);
        return sslContext;
    }

    public SSLEngine createSslEngine(String peerHost, int peerPort) {
        SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
        if (cipherSuites != null) sslEngine.setEnabledCipherSuites(cipherSuites);
        if (enabledProtocols != null) sslEngine.setEnabledProtocols(enabledProtocols);

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

    private void createKeystore(String type, String path, Password password, Password keyPassword) {
        if (path == null && password != null) {
            throw new KafkaException("SSL key store is not specified, but key store password is specified.");
        } else if (path != null && password == null) {
            throw new KafkaException("SSL key store is specified, but key store password is not specified.");
        } else if (path != null && password != null) {
            this.keystore = new SecurityStore(type, path, password);
            this.keyPassword = keyPassword;
        }
    }

    private void createTruststore(String type, String path, Password password) {
        if (path == null && password != null) {
            throw new KafkaException("SSL trust store is not specified, but trust store password is specified.");
        } else if (path != null && password == null) {
            throw new KafkaException("SSL trust store is specified, but trust store password is not specified.");
        } else if (path != null && password != null) {
            this.truststore = new SecurityStore(type, path, password);
        }
    }

    private class SecurityStore {
        private final String type;
        private final String path;
        private final Password password;

        private SecurityStore(String type, String path, Password password) {
            this.type = type == null ? KeyStore.getDefaultType() : type;
            this.path = path;
            this.password = password;
        }

        private KeyStore load() throws GeneralSecurityException, IOException {
            FileInputStream in = null;
            try {
                KeyStore ks = KeyStore.getInstance(type);
                in = new FileInputStream(path);
                ks.load(in, password.value().toCharArray());
                return ks;
            } finally {
                if (in != null) in.close();
            }
        }
    }

}
