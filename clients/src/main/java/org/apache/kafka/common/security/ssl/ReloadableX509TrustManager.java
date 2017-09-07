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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

class ReloadableX509TrustManager extends X509ExtendedTrustManager implements X509TrustManager {
    private static final Logger log = LoggerFactory.getLogger(ReloadableX509TrustManager.class);

    private final SecurityStore trustStore;
    private final TrustManagerFactory tmf;
    private X509TrustManager trustManager;
    private long lastReload = 0L;

    private KeyStore trustKeyStore;

    public ReloadableX509TrustManager(SecurityStore trustStore, TrustManagerFactory tmf) {
        this.trustStore = trustStore;
        this.tmf = tmf;
    }

    public KeyStore getTrustKeyStore() {
        return trustKeyStore;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        reloadTrustManager();
        if (trustManager == null) {
            throw new CertificateException("Trust manager not initialized.");
        }
        trustManager.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        if (trustManager == null) {
            reloadTrustManager();
        }
        if (trustManager == null) {
            throw new CertificateException("Trust manager not initialized.");
        }
        trustManager.checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        reloadTrustManager();

        if (trustManager == null) {
            return new X509Certificate[0];
        }
        return trustManager.getAcceptedIssuers();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
        reloadTrustManager();
        if (trustManager == null) {
            throw new CertificateException("Trust manager not initialized.");
        }
        ((X509ExtendedTrustManager) trustManager).checkClientTrusted(x509Certificates, s, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
        if (trustManager == null) {
            reloadTrustManager();
        }
        if (trustManager == null) {
            throw new CertificateException("Trust manager not initialized.");
        }
        ((X509ExtendedTrustManager) trustManager).checkServerTrusted(x509Certificates, s, socket);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
        reloadTrustManager();
        if (trustManager == null) {
            throw new CertificateException("Trust manager not initialized.");
        }
        ((X509ExtendedTrustManager) trustManager).checkClientTrusted(x509Certificates, s, sslEngine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
        if (trustManager == null) {
            reloadTrustManager();
        }
        if (trustManager == null) {
            throw new CertificateException("Trust manager not initialized.");
        }
        ((X509ExtendedTrustManager) trustManager).checkServerTrusted(x509Certificates, s, sslEngine);
    }

    private void reloadTrustManager() {
        try {
            if (trustManager == null || trustStore.getLastModified() >= lastReload) {
                trustKeyStore = trustStore.load();

                Enumeration<String> alias = trustKeyStore.aliases();
                log.info("Trust manager reloaded.");
                StringBuilder logMessage = new StringBuilder("List of trusted certs: ");
                if (alias.hasMoreElements()) {
                    logMessage.append(alias.nextElement());
                }
                while (alias.hasMoreElements()) {
                    logMessage.append(", ").append(alias.nextElement());
                }
                log.debug(logMessage.toString());

                tmf.init(trustKeyStore);

                trustManager = null;
                TrustManager[] tms = tmf.getTrustManagers();
                for (int i = 0; i < tms.length; i++) {
                    if (tms[i] instanceof X509TrustManager) {
                        trustManager = (X509TrustManager) tms[i];
                    }
                }

                if (trustManager == null) {
                    throw new NoSuchAlgorithmException("No X509TrustManager in TrustManagerFactory");
                }

                lastReload = System.currentTimeMillis();
                // getLastModified() returns timestamp rounded to 1000 ms. Do the same here for lastReload.
                lastReload -= lastReload % 1000;
            }
        } catch (GeneralSecurityException gsEx) {
            log.error("Failed to reload trust manager due to security exception. {}", gsEx.getMessage());
        } catch (IOException ioEx) {
            log.error("Failed to reload trust manager due to IO exception. {}", ioEx.getMessage());
        }
    }
}
