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

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.test.TestSslUtils;
import org.junit.Test;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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
    public void testReloadableX509TrustManager() throws Exception {
        Map<String, X509Certificate> certs = new HashMap<>();
        String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Password trustStorePassword = new Password("TrustStorePassword");
        KeyStore ts = createTruststore(1, trustStoreFile, trustStorePassword);

        tmf.init(ts);
        SecurityStore securityStore = new SecurityStore("jks", trustStoreFile.getPath(), trustStorePassword);

        ReloadableX509TrustManager reloadableX509TrustManager = new ReloadableX509TrustManager(securityStore, tmf);
        reloadableX509TrustManager.getAcceptedIssuers();

        // One alias in truststore
        assertEquals(1, reloadableX509TrustManager.getTrustKeyStore().size());

        createTruststore(2, trustStoreFile, trustStorePassword);
        reloadableX509TrustManager.getAcceptedIssuers();
        // Two aliases in truststore, should have reloaded.
        assertEquals(2, reloadableX509TrustManager.getTrustKeyStore().size());
    }

    @Test
    public void testTrustManagerWithIOException() throws Exception {
        Map<String, X509Certificate> certs = new HashMap<>();
        String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Password trustStorePassword = new Password("TrustStorePassword");
        KeyStore ts = createTruststore(1, trustStoreFile, trustStorePassword);

        tmf.init(ts);
        SecurityStore securityStore = new SecurityStore("jks", trustStoreFile.getPath(), trustStorePassword);

        ReloadableX509TrustManager reloadableX509TrustManager = new ReloadableX509TrustManager(securityStore, tmf);

        trustStoreFile.delete();
        // ReloadableX509TrustManager should handle IO exception.
        reloadableX509TrustManager.getAcceptedIssuers();

        trustStoreFile.createNewFile();
        createTruststore(1, trustStoreFile, trustStorePassword);
        reloadableX509TrustManager.getAcceptedIssuers();
        // Two aliases in truststore, should have reloaded.
        assertEquals(1, reloadableX509TrustManager.getTrustKeyStore().size());
    }

    private KeyStore createTruststore(int numberOfKeypairs, File trustStoreFile, Password trustStorePassword) throws Exception {
        Map<String, X509Certificate> certs = new HashMap<>();
        for (int i = 0; i < numberOfKeypairs; i++) {
            KeyPair cKP = TestSslUtils.generateKeyPair("RSA");
            X509Certificate cCert = TestSslUtils.generateCertificate("CN=localhost, O=client " + i, cKP, 30, "SHA1withRSA");
            certs.put("client" + i, cCert);
        }
        KeyStore ts = TestSslUtils.createTrustStore(trustStoreFile.getPath(), trustStorePassword, certs);
        trustStoreFile.deleteOnExit();
        return ts;
    }
}
