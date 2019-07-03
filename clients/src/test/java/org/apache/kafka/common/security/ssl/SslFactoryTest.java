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
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.Provider;
import java.util.Arrays;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.ssl.mock.TestKeyManagerFactory;
import org.apache.kafka.common.security.ssl.mock.TestProvider;
import org.apache.kafka.common.security.ssl.mock.TestTrustManagerFactory;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.common.network.Mode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.security.Security;

public class SslFactoryTest {
    @Test
    public void testSslFactoryConfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig =
                TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertNotNull(engine);
        assertEquals(Utils.mkSet("TLSv1.2"), Utils.mkSet(engine.getEnabledProtocols()));
        assertEquals(false, engine.getUseClientMode());
    }

    @Test
    public void testSslFactoryWithCustomKeyManagerConfiguration() throws Exception {
        Provider provider = new TestProvider();
        Security.addProvider(provider);
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(
                TestKeyManagerFactory.ALGORITHM,
                TestTrustManagerFactory.ALGORITHM
        );
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        assertNotNull("SslEngineBuilder not created", sslFactory.sslEngineBuilder());
        Security.removeProvider(provider.getName());
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
        Map<String, Object> clientSslConfig =
                TestSslUtils.createSslConfig(false, true, Mode.CLIENT, trustStoreFile, "client");
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(clientSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertTrue(engine.getUseClientMode());
    }

    @Test
    public void testReconfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslConfig = TestSslUtils.
                createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig);
        SslEngineBuilder sslEngineBuilder = sslFactory.sslEngineBuilder();
        assertNotNull("SslEngineBuilder not created", sslEngineBuilder);

        // Verify that SslEngineBuilder is not recreated on reconfigure() if config and
        // file are not changed
        sslFactory.reconfigure(sslConfig);
        assertSame("SslEngineBuilder recreated unnecessarily",
                sslEngineBuilder, sslFactory.sslEngineBuilder());

        // Verify that the SslEngineBuilder is recreated on reconfigure() if config is changed
        trustStoreFile = File.createTempFile("truststore", ".jks");
        sslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SslEngineBuilder not recreated",
                sslEngineBuilder, sslFactory.sslEngineBuilder());
        sslEngineBuilder = sslFactory.sslEngineBuilder();

        // Verify that builder is recreated on reconfigure() if config is not changed, but truststore file was modified
        trustStoreFile.setLastModified(System.currentTimeMillis() + 10000);
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SslEngineBuilder not recreated",
                sslEngineBuilder, sslFactory.sslEngineBuilder());
        sslEngineBuilder = sslFactory.sslEngineBuilder();

        // Verify that builder is recreated on reconfigure() if config is not changed, but keystore file was modified
        File keyStoreFile = new File((String) sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        keyStoreFile.setLastModified(System.currentTimeMillis() + 10000);
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SslEngineBuilder not recreated",
                sslEngineBuilder, sslFactory.sslEngineBuilder());
        sslEngineBuilder = sslFactory.sslEngineBuilder();

        // Verify that builder is recreated after validation on reconfigure() if config is not changed, but keystore file was modified
        keyStoreFile.setLastModified(System.currentTimeMillis() + 15000);
        sslFactory.validateReconfiguration(sslConfig);
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SslEngineBuilder not recreated",
                sslEngineBuilder, sslFactory.sslEngineBuilder());
        sslEngineBuilder = sslFactory.sslEngineBuilder();

        // Verify that the builder is not recreated if modification time cannot be determined
        keyStoreFile.setLastModified(System.currentTimeMillis() + 20000);
        Files.delete(keyStoreFile.toPath());
        sslFactory.reconfigure(sslConfig);
        assertSame("SslEngineBuilder recreated unnecessarily",
                sslEngineBuilder, sslFactory.sslEngineBuilder());
    }

    @Test
    public void testReconfigurationWithoutTruststore() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslConfig = TestSslUtils.
            createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig);
        SSLContext sslContext = sslFactory.sslEngineBuilder().sslContext();
        assertNotNull("SSL context not created", sslContext);
        assertSame("SSL context recreated unnecessarily", sslContext,
                sslFactory.sslEngineBuilder().sslContext());
        assertFalse(sslFactory.createSslEngine("localhost", 0).getUseClientMode());

        Map<String, Object> sslConfig2 = TestSslUtils.
            createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        try {
            sslFactory.validateReconfiguration(sslConfig2);
            fail("Truststore configured dynamically for listener without previous truststore");
        } catch (ConfigException e) {
            // Expected exception
        }
    }

    @Test
    public void testReconfigurationWithoutKeystore() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslConfig = TestSslUtils.
                createSslConfig(false, true, Mode.SERVER, trustStoreFile, "server");
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig);
        SSLContext sslContext = sslFactory.sslEngineBuilder().sslContext();
        assertNotNull("SSL context not created", sslContext);
        assertSame("SSL context recreated unnecessarily", sslContext,
                sslFactory.sslEngineBuilder().sslContext());
        assertFalse(sslFactory.createSslEngine("localhost", 0).getUseClientMode());

        File newTrustStoreFile = File.createTempFile("truststore", ".jks");
        sslConfig = TestSslUtils.
                createSslConfig(false, true, Mode.SERVER, newTrustStoreFile, "server");
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SSL context not recreated", sslContext,
                sslFactory.sslEngineBuilder().sslContext());

        sslConfig = TestSslUtils.
                createSslConfig(false, true, Mode.SERVER, newTrustStoreFile, "server");
        try {
            sslFactory.validateReconfiguration(sslConfig);
            fail("Keystore configured dynamically for listener without previous keystore");
        } catch (ConfigException e) {
            // Expected exception
        }
    }

    @Test
    public void testKeyStoreTrustStoreValidation() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, trustStoreFile, "server");
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        assertNotNull("SslEngineBuilder not created", sslFactory.sslEngineBuilder());
    }

    @Test
    public void testUntrustedKeyStoreValidationFails() throws Exception {
        File trustStoreFile1 = File.createTempFile("truststore1", ".jks");
        File trustStoreFile2 = File.createTempFile("truststore2", ".jks");
        Map<String, Object> sslConfig1 = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, trustStoreFile1, "server");
        Map<String, Object> sslConfig2 = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, trustStoreFile2, "server");
        SslFactory sslFactory = new SslFactory(Mode.SERVER, null, true);
        for (String key : Arrays.asList(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG)) {
            sslConfig1.put(key, sslConfig2.get(key));
        }
        try {
            sslFactory.configure(sslConfig1);
            fail("Validation did not fail with untrusted truststore");
        } catch (ConfigException e) {
            // Expected exception
        }
    }

    @Test
    public void testKeystoreVerifiableUsingTruststore() throws Exception {
        File trustStoreFile1 = File.createTempFile("truststore1", ".jks");
        Map<String, Object> sslConfig1 = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, trustStoreFile1, "server");
        SslFactory sslFactory = new SslFactory(Mode.SERVER, null, true);
        sslFactory.configure(sslConfig1);

        File trustStoreFile2 = File.createTempFile("truststore2", ".jks");
        Map<String, Object> sslConfig2 = TestSslUtils.createSslConfig(false, true,
                Mode.SERVER, trustStoreFile2, "server");
        // Verify that `createSSLContext` fails even if certificate from new keystore is trusted by
        // the new truststore, if certificate is not trusted by the existing truststore on the `SslFactory`.
        // This is to prevent both keystores and truststores to be modified simultaneously on an inter-broker
        // listener to stores that may not work with other brokers where the update hasn't yet been performed.
        try {
            sslFactory.validateReconfiguration(sslConfig2);
            fail("ValidateReconfiguration did not fail as expected");
        } catch (ConfigException e) {
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
        KeyStore ks1 = sslKeyStore(serverSslConfig).load();
        KeyStore ks2 = sslKeyStore(serverSslConfig).load();
        assertEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks2));

        // Use different alias name, validation should succeed
        ks2.setCertificateEntry("another", ks1.getCertificate("localhost"));
        assertEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks2));

        KeyStore ks3 = sslKeyStore(newCnConfig).load();
        assertNotEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks3));
    }

    private SslEngineBuilder.SecurityStore sslKeyStore(Map<String, Object> sslConfig) {
        return new SslEngineBuilder.SecurityStore(
                (String) sslConfig.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                (String) sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                (Password) sslConfig.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                (Password) sslConfig.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG)
        );
    }

    private SslEngineBuilder.SecurityStore sslTrustStore(Map<String, Object> sslConfig) {
        return new SslEngineBuilder.SecurityStore(
                (String) sslConfig.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                (String) sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                (Password) sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
                null
        );
    }
}
