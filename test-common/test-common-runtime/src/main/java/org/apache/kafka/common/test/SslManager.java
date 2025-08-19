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
import java.util.List;
import java.util.Map;

public class SslManager {

    private static final Logger log = LoggerFactory.getLogger(SslManager.class);
    private File keyStoreFile;
    private File trustStoreFile;
    public static final String CLUSTER_TRUSTSTORE_PASSWORD = "cluster-truststore-password";

    public Map<String, Object> createSslConfig() {
        try {
            keyStoreFile = TestUtils.tempFile("kafka.cluster.keystore", ".jks");
            trustStoreFile = TestUtils.tempFile("kafka.server.truststore", ".jks");
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
                keyStoreFile.getPath(),
                keyStorePassword,
                keyPassword,
                "kafka-cluster",
                clusterKeyPair.getPrivate(),
                clusterCert
            );
            
            Password trustStorePassword = new Password(CLUSTER_TRUSTSTORE_PASSWORD);
            TestSslUtils.createTrustStore(
                trustStoreFile.getPath(),
                trustStorePassword,
                Map.of("kafka-cluster", clusterCert)
            );
            log.info("Created unified SSL config - KeyStore: {}, TrustStore: {}", keyStoreFile.getPath(), trustStoreFile.getPath());
            return Map.ofEntries(
                Map.entry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreFile.getPath()),
                Map.entry(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword),
                Map.entry(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword),
                Map.entry(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS"),

                Map.entry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreFile.getPath()),
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
    
    public String trustStoreLocation() {
        return trustStoreFile != null ? trustStoreFile.getAbsolutePath() : null;
    }

    public void close() throws IOException {
        if (keyStoreFile != null) {
            Utils.delete(keyStoreFile);
        }
        if (trustStoreFile != null) {
            Utils.delete(trustStoreFile);
        }
    }
}