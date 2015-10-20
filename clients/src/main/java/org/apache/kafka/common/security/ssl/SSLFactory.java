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
import org.apache.kafka.common.config.SSLConfigs;
import org.apache.kafka.common.network.Mode;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import java.util.List;
import java.util.Map;

public class SSLFactory implements Configurable {

    private String protocol;
    private String provider;
    private String kmfAlgorithm;
    private String tmfAlgorithm;
    private SecurityStore keystore = null;
    private String keyPassword;
    private SecurityStore truststore;
    private String[] cipherSuites;
    private String[] enabledProtocols;
    private String endpointIdentification;
    private SSLContext sslContext;
    private boolean needClientAuth;
    private boolean wantClientAuth;
    private final Mode mode;


    public SSLFactory(Mode mode) {
        this.mode = mode;
    }

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        this.protocol =  (String) configs.get(SSLConfigs.SSL_PROTOCOL_CONFIG);
        this.provider = (String) configs.get(SSLConfigs.SSL_PROVIDER_CONFIG);

        if (configs.get(SSLConfigs.SSL_CIPHER_SUITES_CONFIG) != null) {
            List<String> cipherSuitesList = (List<String>) configs.get(SSLConfigs.SSL_CIPHER_SUITES_CONFIG);
            if (!cipherSuitesList.isEmpty())
                this.cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);
        }

        if (configs.get(SSLConfigs.SSL_ENABLED_PROTOCOLS_CONFIG) != null) {
            List<String> enabledProtocolsList = (List<String>) configs.get(SSLConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
            this.enabledProtocols = enabledProtocolsList.toArray(new String[enabledProtocolsList.size()]);
        }

        if (configs.containsKey(SSLConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)) {
            this.endpointIdentification = (String) configs.get(SSLConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        }

        if (configs.containsKey(SSLConfigs.SSL_CLIENT_AUTH_CONFIG)) {
            String clientAuthConfig = (String) configs.get(SSLConfigs.SSL_CLIENT_AUTH_CONFIG);
            if (clientAuthConfig.equals("required"))
                this.needClientAuth = true;
            else if (clientAuthConfig.equals("requested"))
                this.wantClientAuth = true;
        }

        this.kmfAlgorithm = (String) configs.get(SSLConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        this.tmfAlgorithm = (String) configs.get(SSLConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);

        createKeystore((String) configs.get(SSLConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                       (String) configs.get(SSLConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                       (String) configs.get(SSLConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                       (String) configs.get(SSLConfigs.SSL_KEY_PASSWORD_CONFIG));

        createTruststore((String) configs.get(SSLConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                         (String) configs.get(SSLConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                         (String) configs.get(SSLConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
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
            String keyPassword = this.keyPassword != null ? this.keyPassword : keystore.password;
            kmf.init(ks, keyPassword.toCharArray());
            keyManagers = kmf.getKeyManagers();
        }

        String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        KeyStore ts = truststore == null ? null : truststore.load();
        tmf.init(ts);

        sslContext.init(keyManagers, tmf.getTrustManagers(), null);
        return sslContext;
    }

    public SSLEngine createSSLEngine(String peerHost, int peerPort) {
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

    private void createKeystore(String type, String path, String password, String keyPassword) {
        if (path == null && password != null) {
            throw new KafkaException("SSL key store is not specified, but key store password is specified.");
        } else if (path != null && password == null) {
            throw new KafkaException("SSL key store is specified, but key store password is not specified.");
        } else if (path != null && password != null) {
            this.keystore = new SecurityStore(type, path, password);
            this.keyPassword = keyPassword;
        }
    }

    private void createTruststore(String type, String path, String password) {
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
        private final String password;

        private SecurityStore(String type, String path, String password) {
            this.type = type == null ? KeyStore.getDefaultType() : type;
            this.path = path;
            this.password = password;
        }

        private KeyStore load() throws GeneralSecurityException, IOException {
            FileInputStream in = null;
            try {
                KeyStore ks = KeyStore.getInstance(type);
                in = new FileInputStream(path);
                ks.load(in, password.toCharArray());
                return ks;
            } finally {
                if (in != null) in.close();
            }
        }
    }

}
