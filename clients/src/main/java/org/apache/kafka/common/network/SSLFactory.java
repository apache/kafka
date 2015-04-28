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
package org.apache.kafka.common.network;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.*;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SecurityConfig;


public class SSLFactory  {

    public enum Mode { CLIENT, SERVER };
    private String protocol;
    private String provider;
    private String kmfAlgorithm;
    private String tmfAlgorithm;
    private SecurityStore keystore = null;
    private String keyPassword;
    private SecurityStore truststore;
    private String[] cipherSuites;
    private String[] enabledProtocols;
    private SSLContext sslContext;
    private boolean requireClientCert;
    private Mode mode;


    public SSLFactory(Mode mode) {
        this.mode = mode;
    }


    public void init(SecurityConfig securityConfig) throws IOException, GeneralSecurityException {
        this.protocol =  securityConfig.getString(SecurityConfig.SSL_PROTOCOL_CONFIG);
        if (securityConfig.getList(SecurityConfig.SSL_CIPHER_SUITES_CONFIG) != null)
         this.cipherSuites = (String[]) securityConfig.getList(SecurityConfig.SSL_CIPHER_SUITES_CONFIG).toArray();
        if (securityConfig.getList(SecurityConfig.SSL_ENABLED_PROTOCOLS_CONFIG) != null)
            this.enabledProtocols = (String[]) securityConfig.getList(SecurityConfig.SSL_ENABLED_PROTOCOLS_CONFIG).toArray();
        this.requireClientCert = securityConfig.getBoolean(SecurityConfig.SSL_CLIENT_REQUIRE_CERT_CONFIG);
        this.kmfAlgorithm = securityConfig.getString(SecurityConfig.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        this.tmfAlgorithm = securityConfig.getString(SecurityConfig.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);
        if ((mode == Mode.CLIENT && requireClientCert) || (mode == Mode.SERVER))
            createKeystore(securityConfig.getString(SecurityConfig.SSL_KEYSTORE_TYPE_CONFIG),
                                           securityConfig.getString(SecurityConfig.SSL_KEYSTORE_LOCATION_CONFIG),
                                           securityConfig.getString(SecurityConfig.SSL_KEYSTORE_PASSWORD_CONFIG),
                                           securityConfig.getString(SecurityConfig.SSL_KEY_PASSWORD_CONFIG));
        createTruststore(securityConfig.getString(SecurityConfig.SSL_TRUSTSTORE_TYPE_CONFIG),
                securityConfig.getString(SecurityConfig.SSL_TRUSTSTORE_LOCATION_CONFIG),
                securityConfig.getString(SecurityConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG));

        this.sslContext = createSSLContext();

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
        if (mode == Mode.SERVER) {
            sslEngine.setUseClientMode(false);
        } else {
            sslEngine.setUseClientMode(true);
            sslEngine.setNeedClientAuth(requireClientCert);
        }
        if (enabledProtocols != null) sslEngine.setEnabledProtocols(enabledProtocols);
        return sslEngine;
    }

    /**
     * Returns a configured SSLServerSocketFactory.
     *
     * @return the configured SSLSocketFactory.
     * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
     * be initialized.
     * @throws IOException thrown if and IO error occurred while loading
     * the server keystore.
     */
    public SSLServerSocketFactory createSSLServerSocketFactory() throws GeneralSecurityException, IOException {
        if (mode != Mode.SERVER) {
            throw new IllegalStateException("Factory is in CLIENT mode");
        }
        return sslContext.getServerSocketFactory();
    }

    /**
     * Returns if client certificates are required or not.
     *
     * @return if client certificates are required or not.
     */
    public boolean isClientCertRequired() {
        return requireClientCert;
    }


    private void createKeystore(String type, String path, String password, String keyPassword) {
        if (path == null && password != null) {
            throw new KafkaException("SSL key store password is not specified.");
        } else if (path != null && password == null) {
            throw new KafkaException("SSL key store is not specified, but key store password is specified.");
        } else if (path != null && password != null) {
            this.keystore = new SecurityStore(type, path, password);
            this.keyPassword = keyPassword;
        }
    }

    private void createTruststore(String type, String path, String password) {
        if (path == null && password != null) {
            throw new KafkaException("SSL key store password is not specified.");
        } else if (path != null && password == null) {
            throw new KafkaException("SSL key store is not specified, but key store password is specified.");
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

    public void close() {
    }
}
