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
import java.io.IOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.FileBasedStore;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.PemStore;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.SecurityStore;
import org.apache.kafka.common.security.ssl.mock.TestKeyManagerFactory;
import org.apache.kafka.common.security.ssl.mock.TestProviderCreator;
import org.apache.kafka.common.security.ssl.mock.TestTrustManagerFactory;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.common.network.Mode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.security.Security;
import java.util.Properties;

public abstract class SslFactoryTest {
    private final String tlsProtocol;

    public SslFactoryTest(String tlsProtocol) {
        this.tlsProtocol = tlsProtocol;
    }

    @Test
    public void testSslFactoryConfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertNotNull(engine);
        assertEquals(Utils.mkSet(tlsProtocol), Utils.mkSet(engine.getEnabledProtocols()));
        assertEquals(false, engine.getUseClientMode());
    }

    @Test
    public void testSslFactoryWithCustomKeyManagerConfiguration() {
        TestProviderCreator testProviderCreator = new TestProviderCreator();
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(
                TestKeyManagerFactory.ALGORITHM,
                TestTrustManagerFactory.ALGORITHM,
                tlsProtocol
        );
        serverSslConfig.put(SecurityConfig.SECURITY_PROVIDERS_CONFIG, testProviderCreator.getClass().getName());
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        assertNotNull(sslFactory.sslEngineFactory(), "SslEngineFactory not created");
        Security.removeProvider(testProviderCreator.getProvider().getName());
    }

    @Test
    public void testSslFactoryWithoutProviderClassConfiguration() {
        // An exception is thrown as the algorithm is not registered through a provider
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(
                TestKeyManagerFactory.ALGORITHM,
                TestTrustManagerFactory.ALGORITHM,
                tlsProtocol
        );
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        assertThrows(KafkaException.class, () -> sslFactory.configure(serverSslConfig));
    }

    @Test
    public void testSslFactoryWithIncorrectProviderClassConfiguration() {
        // An exception is thrown as the algorithm is not registered through a provider
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(
                TestKeyManagerFactory.ALGORITHM,
                TestTrustManagerFactory.ALGORITHM,
                tlsProtocol
        );
        serverSslConfig.put(SecurityConfig.SECURITY_PROVIDERS_CONFIG,
                "com.fake.ProviderClass1,com.fake.ProviderClass2");
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        assertThrows(KafkaException.class, () -> sslFactory.configure(serverSslConfig));
    }

    @Test
    public void testSslFactoryWithoutPasswordConfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
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
        Map<String, Object> clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(clientSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertTrue(engine.getUseClientMode());
    }

    @Test
    public void staleSslEngineFactoryShouldBeClosed() throws IOException, GeneralSecurityException {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> clientSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        clientSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(clientSslConfig);
        TestSslUtils.TestSslEngineFactory sslEngineFactory = (TestSslUtils.TestSslEngineFactory) sslFactory.sslEngineFactory();
        assertNotNull(sslEngineFactory);
        assertFalse(sslEngineFactory.closed);

        trustStoreFile = File.createTempFile("truststore", ".jks");
        clientSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        clientSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        sslFactory.reconfigure(clientSslConfig);
        TestSslUtils.TestSslEngineFactory newSslEngineFactory = (TestSslUtils.TestSslEngineFactory) sslFactory.sslEngineFactory();
        assertNotEquals(sslEngineFactory, newSslEngineFactory);
        // the older one should be closed
        assertTrue(sslEngineFactory.closed);
    }

    @Test
    public void testReconfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig);
        SslEngineFactory sslEngineFactory = sslFactory.sslEngineFactory();
        assertNotNull(sslEngineFactory, "SslEngineFactory not created");

        // Verify that SslEngineFactory is not recreated on reconfigure() if config and
        // file are not changed
        sslFactory.reconfigure(sslConfig);
        assertSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory recreated unnecessarily");

        // Verify that the SslEngineFactory is recreated on reconfigure() if config is changed
        trustStoreFile = File.createTempFile("truststore", ".jks");
        sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        sslFactory.reconfigure(sslConfig);
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory not recreated");
        sslEngineFactory = sslFactory.sslEngineFactory();

        // Verify that builder is recreated on reconfigure() if config is not changed, but truststore file was modified
        trustStoreFile.setLastModified(System.currentTimeMillis() + 10000);
        sslFactory.reconfigure(sslConfig);
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory not recreated");
        sslEngineFactory = sslFactory.sslEngineFactory();

        // Verify that builder is recreated on reconfigure() if config is not changed, but keystore file was modified
        File keyStoreFile = new File((String) sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        keyStoreFile.setLastModified(System.currentTimeMillis() + 10000);
        sslFactory.reconfigure(sslConfig);
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory not recreated");
        sslEngineFactory = sslFactory.sslEngineFactory();

        // Verify that builder is recreated after validation on reconfigure() if config is not changed, but keystore file was modified
        keyStoreFile.setLastModified(System.currentTimeMillis() + 15000);
        sslFactory.validateReconfiguration(sslConfig);
        sslFactory.reconfigure(sslConfig);
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory not recreated");
        sslEngineFactory = sslFactory.sslEngineFactory();

        // Verify that the builder is not recreated if modification time cannot be determined
        keyStoreFile.setLastModified(System.currentTimeMillis() + 20000);
        Files.delete(keyStoreFile.toPath());
        sslFactory.reconfigure(sslConfig);
        assertSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory recreated unnecessarily");
    }

    @Test
    public void testReconfigurationWithoutTruststore() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig);
        SSLContext sslContext = ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext();
        assertNotNull(sslContext, "SSL context not created");
        assertSame(sslContext, ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext(),
                "SSL context recreated unnecessarily");
        assertFalse(sslFactory.createSslEngine("localhost", 0).getUseClientMode());

        Map<String, Object> sslConfig2 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
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
        Map<String, Object> sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig);
        SSLContext sslContext = ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext();
        assertNotNull(sslContext, "SSL context not created");
        assertSame(sslContext, ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext(),
                "SSL context recreated unnecessarily");
        assertFalse(sslFactory.createSslEngine("localhost", 0).getUseClientMode());

        File newTrustStoreFile = File.createTempFile("truststore", ".jks");
        sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(newTrustStoreFile)
                .build();
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        sslFactory.reconfigure(sslConfig);
        assertNotSame(sslContext, ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext(),
                "SSL context not recreated");

        sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(newTrustStoreFile)
                .build();
        try {
            sslFactory.validateReconfiguration(sslConfig);
            fail("Keystore configured dynamically for listener without previous keystore");
        } catch (ConfigException e) {
            // Expected exception
        }
    }

    @Test
    public void testPemReconfiguration() throws Exception {
        Properties props = new Properties();
        props.putAll(sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(null)
                .usePem(true)
                .build());
        TestSecurityConfig sslConfig = new TestSecurityConfig(props);

        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig.values());
        SslEngineFactory sslEngineFactory = sslFactory.sslEngineFactory();
        assertNotNull(sslEngineFactory, "SslEngineFactory not created");

        props.put("some.config", "some.value");
        sslConfig = new TestSecurityConfig(props);
        sslFactory.reconfigure(sslConfig.values());
        assertSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory recreated unnecessarily");

        props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG,
                new Password(((Password) props.get(SslConfigs.SSL_KEYSTORE_KEY_CONFIG)).value() + " "));
        sslConfig = new TestSecurityConfig(props);
        sslFactory.reconfigure(sslConfig.values());
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory not recreated");
        sslEngineFactory = sslFactory.sslEngineFactory();

        props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
                new Password(((Password) props.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG)).value() + " "));
        sslConfig = new TestSecurityConfig(props);
        sslFactory.reconfigure(sslConfig.values());
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory not recreated");
        sslEngineFactory = sslFactory.sslEngineFactory();

        props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
                new Password(((Password) props.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG)).value() + " "));
        sslConfig = new TestSecurityConfig(props);
        sslFactory.reconfigure(sslConfig.values());
        assertNotSame(sslEngineFactory, sslFactory.sslEngineFactory(), "SslEngineFactory not recreated");
        sslEngineFactory = sslFactory.sslEngineFactory();
    }

    @Test
    public void testKeyStoreTrustStoreValidation() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        assertNotNull(sslFactory.sslEngineFactory(), "SslEngineFactory not created");
    }

    @Test
    public void testUntrustedKeyStoreValidationFails() throws Exception {
        File trustStoreFile1 = File.createTempFile("truststore1", ".jks");
        File trustStoreFile2 = File.createTempFile("truststore2", ".jks");
        Map<String, Object> sslConfig1 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile1)
                .build();
        Map<String, Object> sslConfig2 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile2)
                .build();
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
        verifyKeystoreVerifiableUsingTruststore(false, tlsProtocol);
    }

    @Test
    public void testPemKeystoreVerifiableUsingTruststore() throws Exception {
        verifyKeystoreVerifiableUsingTruststore(true, tlsProtocol);
    }

    private void verifyKeystoreVerifiableUsingTruststore(boolean usePem, String tlsProtocol) throws Exception {
        File trustStoreFile1 = usePem ? null : File.createTempFile("truststore1", ".jks");
        Map<String, Object> sslConfig1 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile1)
                .usePem(usePem)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.SERVER, null, true);
        sslFactory.configure(sslConfig1);

        File trustStoreFile2 = usePem ? null : File.createTempFile("truststore2", ".jks");
        Map<String, Object> sslConfig2 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile2)
                .usePem(usePem)
                .build();
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
        verifyCertificateEntriesValidation(false, tlsProtocol);
    }

    @Test
    public void testPemCertificateEntriesValidation() throws Exception {
        verifyCertificateEntriesValidation(true, tlsProtocol);
    }

    private void verifyCertificateEntriesValidation(boolean usePem, String tlsProtocol) throws Exception {
        File trustStoreFile = usePem ? null : File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .usePem(usePem)
                .build();
        File newTrustStoreFile = usePem ? null : File.createTempFile("truststore", ".jks");
        Map<String, Object> newCnConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(newTrustStoreFile)
                .cn("Another CN")
                .usePem(usePem)
                .build();
        KeyStore ks1 = sslKeyStore(serverSslConfig);
        KeyStore ks2 = sslKeyStore(serverSslConfig);
        assertEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks2));

        // Use different alias name, validation should succeed
        ks2.setCertificateEntry("another", ks1.getCertificate("localhost"));
        assertEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks2));

        KeyStore ks3 = sslKeyStore(newCnConfig);
        assertNotEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks3));
    }

    /**
     * Tests client side ssl.engine.factory configuration is used when specified
     */
    @Test
    public void testClientSpecifiedSslEngineFactoryUsed() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        clientSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(clientSslConfig);
        assertTrue(sslFactory.sslEngineFactory() instanceof TestSslUtils.TestSslEngineFactory,
            "SslEngineFactory must be of expected type");
    }

    @Test
    public void testEngineFactoryClosed() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        clientSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(clientSslConfig);
        TestSslUtils.TestSslEngineFactory engine = (TestSslUtils.TestSslEngineFactory) sslFactory.sslEngineFactory();
        assertFalse(engine.closed);
        sslFactory.close();
        assertTrue(engine.closed);
    }

    /**
     * Tests server side ssl.engine.factory configuration is used when specified
     */
    @Test
    public void testServerSpecifiedSslEngineFactoryUsed() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        serverSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        assertTrue(sslFactory.sslEngineFactory() instanceof TestSslUtils.TestSslEngineFactory,
            "SslEngineFactory must be of expected type");
    }

    /**
     * Tests invalid ssl.engine.factory configuration
     */
    @Test
    public void testInvalidSslEngineFactory() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        clientSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, String.class);
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        assertThrows(ClassCastException.class, () -> sslFactory.configure(clientSslConfig));
    }

    @Test
    public void testUsedConfigs() throws IOException, GeneralSecurityException {
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(File.createTempFile("truststore", ".jks"))
                .useClientCert(false)
                .build();
        serverSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        TestSecurityConfig securityConfig = new TestSecurityConfig(serverSslConfig);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(securityConfig.values());
        assertFalse(securityConfig.unused().contains(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG));
    }

    private KeyStore sslKeyStore(Map<String, Object> sslConfig) {
        SecurityStore store;
        if (sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG) != null) {
            store = new FileBasedStore(
                    (String) sslConfig.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                    (String) sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                    (Password) sslConfig.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                    (Password) sslConfig.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG),
                    true
            );
        } else {
            store = new PemStore(
                    (Password) sslConfig.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG),
                    (Password) sslConfig.get(SslConfigs.SSL_KEYSTORE_KEY_CONFIG),
                    (Password) sslConfig.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG)
            );
        }
        return store.get();
    }

    private TestSslUtils.SslConfigsBuilder sslConfigsBuilder(Mode mode) {
        return new TestSslUtils.SslConfigsBuilder(mode).tlsProtocol(tlsProtocol);
    }
}
