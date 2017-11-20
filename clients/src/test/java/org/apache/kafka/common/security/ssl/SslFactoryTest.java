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

import java.io.File;
import java.security.KeyStore;
import java.util.Map;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.common.network.Mode;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A set of tests for the selector over ssl. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SslFactoryTest {

    @Test
    public void testSslFactoryConfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertNotNull(engine);
        String[] expectedProtocols = {"TLSv1.2"};
        assertArrayEquals(expectedProtocols, engine.getEnabledProtocols());
        assertEquals(false, engine.getUseClientMode());
    }

    @Test
    public void testSslFactoryWithoutPasswordConfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        // unset the password
        serverSslConfig.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        try {
            sslFactory.configure(serverSslConfig);
        } catch (Exception e) {
            fail("An exception was thrown when configuring the truststore without a password: " + e);
        }
    }

    @Test
    public void testClientMode() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> clientSslConfig = TestSslUtils.createSslConfig(false, true, Mode.CLIENT, trustStoreFile, "client");
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(clientSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertTrue(engine.getUseClientMode());
    }

    @Test
    public void testKeyStoreTrustStoreValidation() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, trustStoreFile, "server");
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        sslFactory.createSSLContext(securityStore(serverSslConfig), true);
    }

    @Test
    public void testUntrustedKeyStoreValidation() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, trustStoreFile, "server");
        Map<String, Object> untrustedConfig = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, File.createTempFile("truststore", ".jks"), "server");
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        try {
            sslFactory.createSSLContext(securityStore(untrustedConfig), true);
            fail("Validation did not fail with untrusted keystore");
        } catch (SSLHandshakeException e) {
            // Expected exception
        }
    }

    @Test
    public void testCertificateEntriesValidation() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, trustStoreFile, "server");
        Map<String, Object> newCnConfig = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, File.createTempFile("truststore", ".jks"), "server", "Another CN");
        KeyStore ks1 = securityStore(serverSslConfig).load();
        KeyStore ks2 = securityStore(serverSslConfig).load();
        assertEquals(SslFactory.CertificateEntries.certificateEntries(ks1), SslFactory.CertificateEntries.certificateEntries(ks2));

        // Use different alias name, validation should succeed
        ks2.setCertificateEntry("another", ks1.getCertificate("localhost"));
        assertEquals(SslFactory.CertificateEntries.certificateEntries(ks1), SslFactory.CertificateEntries.certificateEntries(ks2));

        KeyStore ks3 = securityStore(newCnConfig).load();
        assertNotEquals(SslFactory.CertificateEntries.certificateEntries(ks1), SslFactory.CertificateEntries.certificateEntries(ks3));
    }

    private SslFactory.SecurityStore securityStore(Map<String, Object> sslConfig) {
        return new SslFactory.SecurityStore(
                (String) sslConfig.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                (String) sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                (Password) sslConfig.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                (Password) sslConfig.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG)
        );
    }

}
