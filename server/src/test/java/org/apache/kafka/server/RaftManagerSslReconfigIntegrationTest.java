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

package org.apache.kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.SslChannelBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.test.api.TestKitDefaults;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration test verifying that dynamic SSL reconfiguration of the Raft controller listener
 * works end-to-end. After a per-broker config update to rotate the controller listener keystore,
 * the KafkaRaftManager channel builder (registered as a Reconfigurable) picks up the new
 * certificate and the live SslEngineFactory inside the running Raft channel reflects the change.
 */
@Tag("integration")
public class RaftManagerSslReconfigIntegrationTest {

    private static final ListenerName CONTROLLER_LISTENER = ListenerName.normalised("CONTROLLER");
    private static final ListenerName BROKER_LISTENER = ListenerName.normalised("EXTERNAL");
    private static final int BROKER_ID = TestKitDefaults.BROKER_ID_OFFSET;
    private static final String STORE_PASSWORD = "changeit";
    private static final Password PASSWORD = new Password(STORE_PASSWORD);
    private static final String SIGNING_ALGORITHM = "SHA256withRSA";

    private String trustStorePath;
    private String controllerKeystorePath;
    private String certAKeystorePath;
    private String certBKeystorePath;
    private BigInteger certASerial;
    private BigInteger certBSerial;

    private static String controllerSslConfig(String key) {
        return CONTROLLER_LISTENER.configPrefix() + key;
    }

    private void generateCertificates() throws Exception {
        KeyPair caKeyPair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate caCert = TestSslUtils.generateSignedCertificate(
            "CN=TestCA", caKeyPair, 0, 3650, null, null, SIGNING_ALGORITHM, true, false, false);

        KeyPair controllerKeyPair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate controllerCert = TestSslUtils.generateSignedCertificate(
            "CN=localhost", controllerKeyPair, 0, 3650, "CN=TestCA", caKeyPair, SIGNING_ALGORITHM,
            false, true, true, new String[]{"localhost"});

        KeyPair certAKeyPair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate certACert = TestSslUtils.generateSignedCertificate(
            "CN=brokerClient", certAKeyPair, 0, 3650, "CN=TestCA", caKeyPair, SIGNING_ALGORITHM,
            false, false, true);
        certASerial = certACert.getSerialNumber();

        KeyPair certBKeyPair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate certBCert = TestSslUtils.generateSignedCertificate(
            "CN=brokerClient", certBKeyPair, 0, 3650, "CN=TestCA", caKeyPair, SIGNING_ALGORITHM,
            false, false, true);
        certBSerial = certBCert.getSerialNumber();

        assertNotEquals(certASerial, certBSerial,
            "Cert A and B must have distinct serials for the test to be meaningful");

        trustStorePath = TestUtils.tempFile("raft-ssl-reconfig-truststore", ".jks").getPath();
        controllerKeystorePath = TestUtils.tempFile("raft-ssl-reconfig-controller-ks", ".jks").getPath();
        certAKeystorePath = TestUtils.tempFile("raft-ssl-reconfig-clientA-ks", ".jks").getPath();
        certBKeystorePath = TestUtils.tempFile("raft-ssl-reconfig-clientB-ks", ".jks").getPath();

        TestSslUtils.createKeyStore(controllerKeystorePath, PASSWORD, PASSWORD, "controller",
            controllerKeyPair.getPrivate(), controllerCert);
        TestSslUtils.createKeyStore(certAKeystorePath, PASSWORD, PASSWORD, "clienta",
            certAKeyPair.getPrivate(), certACert);
        TestSslUtils.createKeyStore(certBKeystorePath, PASSWORD, PASSWORD, "clientb",
            certBKeyPair.getPrivate(), certBCert);

        Map<String, X509Certificate> trustCerts = new HashMap<>();
        trustCerts.put("ca", caCert);
        TestSslUtils.createTrustStore(trustStorePath, PASSWORD, trustCerts);
    }

    private KafkaClusterTestKit buildCluster() throws Exception {
        Map<Integer, Map<String, String>> perServer = Map.of(
            BROKER_ID, Map.of(
                controllerSslConfig(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG), certAKeystorePath));

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(1)
            .setNumControllerNodes(1)
            .setBrokerListenerName(BROKER_LISTENER)
            .setBrokerSecurityProtocol(SecurityProtocol.SSL)
            .setControllerListenerName(CONTROLLER_LISTENER)
            .setControllerSecurityProtocol(SecurityProtocol.SSL)
            .setPerServerProperties(perServer)
            .build();

        return new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, controllerKeystorePath)
            .setConfigProp(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, STORE_PASSWORD)
            .setConfigProp(SslConfigs.SSL_KEY_PASSWORD_CONFIG, STORE_PASSWORD)
            .setConfigProp(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath)
            .setConfigProp(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, STORE_PASSWORD)
            .setConfigProp(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
            .setConfigProp(controllerSslConfig(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG), controllerKeystorePath)
            .setConfigProp(controllerSslConfig(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), STORE_PASSWORD)
            .setConfigProp(controllerSslConfig(SslConfigs.SSL_KEY_PASSWORD_CONFIG), STORE_PASSWORD)
            .setConfigProp(controllerSslConfig(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG), trustStorePath)
            .setConfigProp(controllerSslConfig(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG), STORE_PASSWORD)
            .setConfigProp(controllerSslConfig(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), "")
            .setConfigProp(controllerSslConfig("ssl.client.auth"), "required")
            .build();
    }

    /**
     * Walks the field chain from the broker's raftManager to the SslEngineFactory that is actively
     * used by the Raft channel builder, then extracts the X.509 serial of the first certificate in
     * its keystore. This is the only way to assert that the reconfiguration reached the live SSL
     * engine — the channel identity is not exposed through any public API.
     */
    private BigInteger loadedRaftKeystoreSerial(KafkaClusterTestKit cluster) throws Exception {
        Object raftManager = cluster.brokers().get(BROKER_ID).raftManager();
        Object netChannel = readField(raftManager, "netChannel");
        Object requestThread = readField(netChannel, "requestThread");
        Object networkClient = readField(requestThread, "networkClient");
        Object selector = readField(networkClient, "selector");
        Object channelBuilder = readField(selector, "channelBuilder");
        assertTrue(channelBuilder instanceof SslChannelBuilder,
            "Expected the Raft channel builder to be an SslChannelBuilder but was "
                + channelBuilder.getClass().getName());
        Object sslFactory = readField(channelBuilder, "sslFactory");
        SslEngineFactory engineFactory = (SslEngineFactory) readField(sslFactory, "sslEngineFactory");
        return firstX509Serial(engineFactory.keystore());
    }

    private static Object readField(Object target, String name) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException("Field '" + name + "' not found on " + target.getClass());
    }

    private static BigInteger firstX509Serial(KeyStore keyStore) throws Exception {
        assertNotNull(keyStore, "Raft channel SslFactory keystore was null");
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            Certificate certificate = keyStore.getCertificate(aliases.nextElement());
            if (certificate instanceof X509Certificate) {
                return ((X509Certificate) certificate).getSerialNumber();
            }
        }
        return fail("No X509Certificate found in the Raft channel SslFactory keystore");
    }

    private void rotateToCertB(Admin admin) throws Exception {
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(BROKER_ID));
        List<AlterConfigOp> ops = List.of(
            new AlterConfigOp(new ConfigEntry(
                controllerSslConfig(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG), certBKeystorePath),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(
                controllerSslConfig(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), STORE_PASSWORD),
                AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(
                controllerSslConfig(SslConfigs.SSL_KEY_PASSWORD_CONFIG), STORE_PASSWORD),
                AlterConfigOp.OpType.SET));
        admin.incrementalAlterConfigs(Map.of(broker, ops)).all().get();
    }

    @Timeout(180)
    @Test
    public void testDynamicControllerKeystoreReachesRunningRaftChannel() throws Exception {
        generateCertificates();

        try (KafkaClusterTestKit cluster = buildCluster()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            TestUtils.waitForCondition(
                () -> certASerial.equals(quietlyLoadedSerial(cluster)),
                60_000,
                "Baseline: the Raft channel's SslFactory never loaded the initial keystore (cert A)");

            Map<String, Object> clientSsl = Map.of(
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath,
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, STORE_PASSWORD,
                SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            try (Admin admin = cluster.admin(clientSsl)) {
                rotateToCertB(admin);
            }

            TestUtils.waitForCondition(
                () -> certBSerial.equals(quietlyLoadedSerial(cluster)),
                15_000,
                "Raft channel SslFactory never loaded the rotated keystore (cert B); expected "
                    + certBSerial + ". The dynamic SSL change did not propagate to the running "
                    + "Raft channel's SslFactory.");
        }
    }

    private BigInteger quietlyLoadedSerial(KafkaClusterTestKit cluster) {
        try {
            return loadedRaftKeystoreSerial(cluster);
        } catch (Exception e) {
            return null;
        }
    }
}
