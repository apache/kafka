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

import org.apache.kafka.common.config.types.Password;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileInputStream;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSslUtilsTest {

    @ValueSource(strings = {"RSA", "EC"})
    @ParameterizedTest
    void testGenerateKeyPair(String algorithm) throws Exception {
        KeyPair keyPair = TestSslUtils.generateKeyPair(algorithm);
        assertEquals(algorithm, keyPair.getPrivate().getAlgorithm());
        assertEquals(algorithm, keyPair.getPublic().getAlgorithm());
    }

    @Test
    void testGenerateSignedCertificate() throws Exception {
        KeyPair keyPair = TestSslUtils.generateKeyPair("RSA");
        String[] hostNames = {"localhost", "127.0.0.1"};
        X509Certificate cert = TestSslUtils.generateSignedCertificate(
            "CN=test-server, O=Test Org",
            keyPair,
            0, 365,
            null, null,
            "SHA256withRSA",
            false, 
            true, 
            false, 
            hostNames
        );

        assertEquals("O=Test Org,CN=test-server", cert.getSubjectX500Principal().getName());
        Collection<List<?>> sanEntries = cert.getSubjectAlternativeNames();
        assertFalse(cert.getSubjectAlternativeNames().isEmpty());
        List<String> dnsNames = sanEntries.stream()
                .filter(entry -> (Integer) entry.get(0) == 2)
                .map(entry -> (String) entry.get(1))
                .toList();

        assertEquals(List.of("localhost", "127.0.0.1"), dnsNames);
    }

    @Test
    void testCreateKeyStore() throws Exception {
        KeyPair keyPair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate cert = TestSslUtils.generateSignedCertificate(
            "CN=keystore-test",
            keyPair,
            0,
            365,
            null,
            null,
            "SHA256withRSA",
            false,
            true,
            false,
            new String[]{"localhost"}
        );

        File keyStoreFile = TestUtils.tempFile("test-keystore", ".jks");
        Password storePassword = new Password("store-password");
        Password keyPassword = new Password("key-password");

        TestSslUtils.createKeyStore(
            keyStoreFile.getPath(),
            storePassword, keyPassword,
            "test-alias",
            keyPair.getPrivate(),
            cert
        );

        KeyStore loadedKS = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(keyStoreFile)) {
            loadedKS.load(fis, storePassword.value().toCharArray());
        }

        assertTrue(Collections.list(loadedKS.aliases()).contains("test-alias"));

        Key retrievedKey = loadedKS.getKey("test-alias", keyPassword.value().toCharArray());
        assertEquals(keyPair.getPrivate(), retrievedKey);

        Certificate retrievedCert = loadedKS.getCertificate("test-alias");
        assertEquals(cert, retrievedCert);
    }

    @Test
    void testCreateTrustStore() throws Exception {
        KeyPair keyPair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate cert = TestSslUtils.generateSignedCertificate(
            "CN=truststore-test",
            keyPair,
            0,
            365,
            null,
            null,
            "SHA256withRSA",
            false,
            true,
            false,
            new String[]{"localhost"}
        );

        File trustStoreFile = TestUtils.tempFile("test-truststore", ".jks");
        Password trustStorePassword = new Password("trust-password");
        Map<String, X509Certificate> certs = Map.of("trusted-cert", cert);

        TestSslUtils.createTrustStore(
            trustStoreFile.getPath(),
            trustStorePassword,
            certs
        );

        KeyStore loadedTS = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(trustStoreFile)) {
            loadedTS.load(fis, trustStorePassword.value().toCharArray());
        }

        assertTrue(Collections.list(loadedTS.aliases()).contains("trusted-cert"));
        Certificate retrievedCert = loadedTS.getCertificate("trusted-cert");
        assertEquals(cert, retrievedCert);
    }
}