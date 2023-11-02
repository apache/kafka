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
package org.apache.kafka.connect.integration;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestSslUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.rest.RestServerConfig.LISTENERS_HTTPS_CONFIGS_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An integration test to ensure that REST SSL is configured correctly.
 */
@Category(IntegrationTest.class)
public class SslRestIntegrationTest {

    private static final Path KEYSTORE_REST = Paths.get("keystore.rest.jks");
    private static final Path KEYSTORE_CLI = Paths.get("keystore.cli.jks");
    private static final Path TRUSTSTORE = Paths.get("truststore.jks");
    private static final Password PASSWORD = new Password("changeIt");
    private static final String TEST_SSL_FACTORY_PARAM_CONFIG = "my.test.ssl.param";
    private static final String TEST_SSL_FACTORY_PARAM_VALUE = "testVal";

    private EmbeddedConnectCluster connect;

    @BeforeClass
    public static void setup() throws Exception {
        createKeystores();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        deleteKeystores();
    }

    @Test
    public void testCustomSslFactory() throws Exception {
        // build a Connect cluster backed by Kafka and Zk
        Map<String, String> config = sslConfig(LISTENERS_HTTPS_CONFIGS_PREFIX, KEYSTORE_REST.toString());
        config.put(LISTENERS_HTTPS_CONFIGS_PREFIX + SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, MyTestSslEngineFactory.class.getName());
        config.put(LISTENERS_HTTPS_CONFIGS_PREFIX + TEST_SSL_FACTORY_PARAM_CONFIG, TEST_SSL_FACTORY_PARAM_VALUE);

        // Set invalid value to unprefixed property to check that listeners SSL  properties values
        // aren't mixed with kafka client SSL properties.
        config.put(SslConfigs.SSL_PROTOCOL_CONFIG, "invalid");

        connect = new EmbeddedConnectCluster.Builder()
            .name("connect-cluster")
            .numWorkers(1)
            .numBrokers(1)
            .ssl()
            .workerProps(config)
            .build();

        // start the clusters
        connect.start();

        try (CloseableHttpClient httpClient = testSslClient()) {
            CloseableHttpResponse response = httpClient.execute(new HttpGet(connect.endpointForResource("")));
            assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode(),
                response + EntityUtils.toString(response.getEntity()));
        }

        assertTrue(MyTestSslEngineFactory.engineCreatedCnt > 0);
        assertEquals(TEST_SSL_FACTORY_PARAM_VALUE, MyTestSslEngineFactory.paramValue);
    }

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    private static Map<String, String> sslConfig(String prefix, String keystore) {
        Map<String, String> config = new HashMap<>();

        config.put(prefix + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
        config.put(prefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD.value());
        config.put(prefix + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TRUSTSTORE.toString());
        config.put(prefix + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD.value());

        return config;
    }

    CloseableHttpClient testSslClient() {
        Map<String, Object> config = new HashMap<>();

        config.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        config.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KEYSTORE_CLI.toString());
        config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD);
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TRUSTSTORE.toString());
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD);

        DefaultSslEngineFactory sslEngineFactory = new DefaultSslEngineFactory();
        sslEngineFactory.configure(config);

        return HttpClients.custom()
            .setSSLContext(sslEngineFactory.sslContext())
            .setSSLHostnameVerifier((hostname, session) -> true)
            .build();
    }

    private static void createKeystores() throws Exception {
        deleteKeystores();

        KeyPair keyPairRest = TestSslUtils.generateKeyPair("RSA");
        X509Certificate certRest = TestSslUtils.generateCertificate(
            "CN=rest-srv,OU=Unknown,O=Test,L=Unknown,ST=Unknown,C=Unknown",
            keyPairRest,
            365,
            "SHA256withRSA"
        );

        KeyPair keyPairCli = TestSslUtils.generateKeyPair("RSA");
        X509Certificate certCli = TestSslUtils.generateCertificate(
            "CN=rest-cli,OU=Unknown,O=Test,L=Unknown,ST=Unknown,C=Unknown",
            keyPairRest,
            365,
            "SHA256withRSA"
        );

        TestSslUtils.createKeyStore(KEYSTORE_REST.toString(), PASSWORD, PASSWORD, "rest", keyPairRest.getPrivate(), certRest);
        TestSslUtils.createKeyStore(KEYSTORE_CLI.toString(), PASSWORD, PASSWORD, "cli", keyPairCli.getPrivate(), certCli);

        Map<String, X509Certificate> certs = new HashMap<>();
        certs.put("rest", certRest);
        certs.put("cli", certCli);

        TRUSTSTORE.toFile().createNewFile();
        TestSslUtils.createTrustStore(TRUSTSTORE.toString(), PASSWORD, certs);
    }

    private static void deleteKeystores() throws IOException {
        Files.deleteIfExists(KEYSTORE_REST);
        Files.deleteIfExists(KEYSTORE_CLI);
        Files.deleteIfExists(TRUSTSTORE);
    }

    public static class MyTestSslEngineFactory extends DefaultSslEngineFactory {

        public static int engineCreatedCnt = 0;
        public static Object paramValue;

        public MyTestSslEngineFactory() {
            engineCreatedCnt = 0;
        }

        @Override public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
            engineCreatedCnt++;
            return super.createServerSslEngine(peerHost, peerPort);
        }

        @Override public void configure(Map<String, ?> configs) {
            paramValue = configs.get(TEST_SSL_FACTORY_PARAM_CONFIG);
            super.configure(configs);
        }
    }
}
