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
package org.apache.kafka.common.test;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SslManager {

    private static final Logger log = LoggerFactory.getLogger(SslManager.class);
    private static final Map<String, Object> SSL_CONFIG = new HashMap<>();
    private static final Object LOCK = new Object();
    private static final File KEY_STORE_FILE;
    public static final File TRUST_STORE_FILE;
    public static final String CLUSTER_TRUSTSTORE_PASSWORD = "cluster-truststore-password";
    

    static {
        try {
            KEY_STORE_FILE = TestUtils.tempFile("kafka.cluster.keystore", ".jks");
            TRUST_STORE_FILE = TestUtils.tempFile("kafka.server.truststore", ".jks");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> getOrCreateGlobalSslConfig() {
        if (SSL_CONFIG.isEmpty()) {
            synchronized (LOCK) {
                if (SSL_CONFIG.isEmpty()) {
                    SSL_CONFIG.putAll(createUnifiedSslConfig());
                }
            }
        }
        return new HashMap<>(SSL_CONFIG);
    }

    private static Map<String, Object> createUnifiedSslConfig() {
        try {
            KeyPair clusterKeyPair = TestSslUtils.generateKeyPair("RSA");
            String[] hostNames = {"localhost", "127.0.0.1"};
            X509Certificate clusterCert = TestSslUtils.generateSignedCertificate(
                "CN=kafka-cluster, O=Kafka Test Cluster",
                clusterKeyPair,
                0,
                365,
                null,
                null,
                "SHA256withRSA",
                false,
                true,
                true,
                hostNames
            );

            Password keyStorePassword = new Password("cluster-keystore-password");
            Password keyPassword = new Password("cluster-key-password");

            TestSslUtils.createKeyStore(
                KEY_STORE_FILE.getPath(),
                keyStorePassword,
                keyPassword,
                "kafka-cluster",
                clusterKeyPair.getPrivate(),
                clusterCert
            );
            
            Password trustStorePassword = new Password(CLUSTER_TRUSTSTORE_PASSWORD);
            TestSslUtils.createTrustStore(
                TRUST_STORE_FILE.getPath(),
                trustStorePassword,
                Map.of("kafka-cluster", clusterCert)
            );
            log.info("Created unified SSL config - KeyStore: {}, TrustStore: {}", KEY_STORE_FILE.getPath(), TRUST_STORE_FILE.getPath());
            return Map.ofEntries(
                Map.entry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KEY_STORE_FILE.getPath()),
                Map.entry(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword),
                Map.entry(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword),
                Map.entry(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS"),

                Map.entry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TRUST_STORE_FILE.getPath()),
                Map.entry(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword),
                Map.entry(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS"),

                Map.entry(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2"),
                Map.entry(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, List.of("TLSv1.2")),

                Map.entry(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, "SunX509"),
                Map.entry(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "PKIX"),
                Map.entry(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
            );
        } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public static void close() throws IOException {
        SSL_CONFIG.clear();
        if (KEY_STORE_FILE != null) {
            Utils.delete(KEY_STORE_FILE);
        }
        if (TRUST_STORE_FILE != null) {
            Utils.delete(TRUST_STORE_FILE);
        }
    }
}