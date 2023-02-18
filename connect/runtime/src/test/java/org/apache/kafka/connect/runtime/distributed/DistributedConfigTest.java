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

package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class DistributedConfigTest {

    public Map<String, String> configs() {
        Map<String, String> result = new HashMap<>();
        result.put(DistributedConfig.GROUP_ID_CONFIG, "connect-cluster");
        result.put(DistributedConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        result.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
        result.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        result.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
        result.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        result.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        return result;
    }

    @Test
    public void shouldCreateKeyGeneratorWithDefaultSettings() {
        DistributedConfig config = new DistributedConfig(configs());
        assertNotNull(config.getInternalRequestKeyGenerator());
    }

    @Test
    public void testDefaultAlgorithmsNotPresent() throws NoSuchAlgorithmException {
        final String fakeKeyGenerationAlgorithm = "FakeKeyGenerationAlgorithm";
        final String fakeMacAlgorithm = "FakeMacAlgorithm";

        final KeyGenerator fakeKeyGenerator = mock(KeyGenerator.class);
        final Mac fakeMac = mock(Mac.class);
        final Crypto crypto = mock(Crypto.class);

        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, fakeKeyGenerationAlgorithm);
        configs.put(DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG, fakeMacAlgorithm);
        configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, fakeMacAlgorithm);

        // Make it seem like the default key generation algorithm isn't available on this worker
        doThrow(new NoSuchAlgorithmException())
                .when(crypto).keyGenerator(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT);
        // But the one specified in the worker config file is
        doReturn(fakeKeyGenerator)
                .when(crypto).keyGenerator(fakeKeyGenerationAlgorithm);

        // And for the signature algorithm
        doThrow(new NoSuchAlgorithmException())
                .when(crypto).mac(DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT);
        // Likewise for key verification algorithms
        for (String verificationAlgorithm : DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_DEFAULT) {
            doThrow(new NoSuchAlgorithmException())
                    .when(crypto).mac(verificationAlgorithm);
        }
        doReturn(fakeMac).when(crypto).mac(fakeMacAlgorithm);

        // Should succeed; even though the defaults aren't present, the manually-specified algorithms are valid
        new DistributedConfig(crypto, configs);

        // Should fail; the default key generation algorithm isn't present, and no override is specified
        String removed = configs.remove(INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG);
        assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
        configs.put(INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, removed);

        // Should fail; the default key generation algorithm isn't present, and no override is specified
        removed = configs.remove(INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG);
        assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
        configs.put(INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG, removed);

        // Should fail; the default key generation algorithm isn't present, and no override is specified
        removed = configs.remove(INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG);
        assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
        configs.put(INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, removed);
    }

    @Test
    public void testSupportedMacAlgorithms() {
        // These algorithms are required to be supported on JVMs ranging from at least Java 8 through Java 17; see
        // https://docs.oracle.com/javase/8/docs/api/javax/crypto/Mac.html
        // and https://docs.oracle.com/en/java/javase/17/docs/api/java.base/javax/crypto/Mac.html
        testSupportedAlgorithms(
                "Mac",
                "HmacSHA1", "HmacSHA256"
        );
    }

    @Test
    public void testSupportedKeyGeneratorAlgorithms() {
        // These algorithms are required to be supported on JVMs ranging from at least Java 8 through Java 17; see
        // https://docs.oracle.com/javase/8/docs/api/javax/crypto/KeyGenerator.html
        // and https://docs.oracle.com/en/java/javase/17/docs/api/java.base/javax/crypto/KeyGenerator.html
        testSupportedAlgorithms(
                "KeyGenerator",
                "AES", "DESede", "HmacSHA1", "HmacSHA256"
        );
    }

    private void testSupportedAlgorithms(String type, String... expectedAlgorithms) {
        Set<String> supportedAlgorithms = DistributedConfig.supportedAlgorithms(type);
        Set<String> unuspportedAlgorithms = new HashSet<>(Arrays.asList(expectedAlgorithms));
        unuspportedAlgorithms.removeAll(supportedAlgorithms);
        assertEquals(type + " algorithms were found that should be supported by this JVM but are not", Collections.emptySet(), unuspportedAlgorithms);
    }

    @Test
    public void shouldCreateKeyGeneratorWithSpecificSettings() {
        final String algorithm = "HmacSHA1";
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, algorithm);
        configs.put(DistributedConfig.INTER_WORKER_KEY_SIZE_CONFIG, "512");
        DistributedConfig config = new DistributedConfig(configs);
        KeyGenerator keyGenerator = config.getInternalRequestKeyGenerator();
        assertNotNull(keyGenerator);
        assertEquals(algorithm, keyGenerator.getAlgorithm());
        assertEquals(512 / 8, keyGenerator.generateKey().getEncoded().length);
    }

    @Test
    public void shouldFailWithEmptyListOfVerificationAlgorithms() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, "");
        assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
    }

    @Test
    public void shouldFailIfSignatureAlgorithmNotInVerificationAlgorithmsList() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG, "HmacSHA1");
        configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, "HmacSHA256");
        assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
    }

    @Test
    public void shouldNotFailIfKeyAlgorithmNotInVerificationAlgorithmsList() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, "HmacSHA1");
        configs.put(DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG, "HmacSHA256");
        configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, "HmacSHA256");
        new DistributedConfig(configs);
    }

    @Test
    public void shouldFailWithInvalidKeyAlgorithm() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, "not-actually-a-key-algorithm");
        assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
    }

    @Test
    public void shouldFailWithInvalidKeySize() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_SIZE_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
    }

    @Test
    public void shouldValidateAllVerificationAlgorithms() {
        List<String> algorithms =
            new ArrayList<>(Arrays.asList("HmacSHA1", "HmacSHA256", "HmacMD5", "bad-algorithm"));
        Map<String, String> configs = configs();
        for (int i = 0; i < algorithms.size(); i++) {
            configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, String.join(",", algorithms));
            assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
            algorithms.add(algorithms.remove(0));
        }
    }

    @Test
    public void shouldAllowNegativeOneAndPositiveForPartitions() {
        Map<String, String> settings = configs();
        settings.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "-1");
        settings.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "-1");
        new DistributedConfig(configs());
        settings.remove(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG);
        settings.remove(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG);

        for (int i = 1; i != 100; ++i) {
            settings.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, Integer.toString(i));
            new DistributedConfig(settings);
            settings.remove(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG);

            settings.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, Integer.toString(i));
            new DistributedConfig(settings);
        }
    }

    @Test
    public void shouldNotAllowZeroPartitions() {
        Map<String, String> settings = configs();
        settings.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
        settings.remove(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG);

        settings.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
    }

    @Test
    public void shouldNotAllowNegativePartitionsLessThanNegativeOne() {
        Map<String, String> settings = configs();
        for (int i = -2; i > -100; --i) {
            settings.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, Integer.toString(i));
            assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
            settings.remove(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG);

            settings.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, Integer.toString(i));
            assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
        }
    }

    @Test
    public void shouldAllowNegativeOneAndPositiveForReplicationFactor() {
        Map<String, String> settings = configs();
        settings.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "-1");
        settings.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "-1");
        settings.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "-1");
        new DistributedConfig(configs());
        settings.remove(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG);
        settings.remove(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG);
        settings.remove(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG);

        for (int i = 1; i != 100; ++i) {
            settings.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, Integer.toString(i));
            new DistributedConfig(settings);
            settings.remove(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG);

            settings.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, Integer.toString(i));
            new DistributedConfig(settings);
            settings.remove(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG);

            settings.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, Integer.toString(i));
            new DistributedConfig(settings);
        }
    }

    @Test
    public void shouldNotAllowZeroReplicationFactor() {
        Map<String, String> settings = configs();
        settings.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
        settings.remove(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG);

        settings.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
        settings.remove(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG);

        settings.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
    }

    @Test
    public void shouldNotAllowNegativeReplicationFactorLessThanNegativeOne() {
        Map<String, String> settings = configs();
        for (int i = -2; i > -100; --i) {
            settings.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, Integer.toString(i));
            assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
            settings.remove(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG);

            settings.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, Integer.toString(i));
            assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
            settings.remove(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG);

            settings.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, Integer.toString(i));
            assertThrows(ConfigException.class, () -> new DistributedConfig(settings));
        }
    }

    @Test
    public void shouldAllowSettingConfigTopicSettings() {
        Map<String, String> topicSettings = new HashMap<>();
        topicSettings.put("foo", "foo value");
        topicSettings.put("bar", "bar value");
        topicSettings.put("baz.bim", "100");
        Map<String, String> settings = configs();
        topicSettings.forEach((k, v) -> settings.put(DistributedConfig.CONFIG_STORAGE_PREFIX + k, v));
        DistributedConfig config = new DistributedConfig(settings);
        assertEquals(topicSettings, config.configStorageTopicSettings());
    }

    @Test
    public void shouldAllowSettingOffsetTopicSettings() {
        Map<String, String> topicSettings = new HashMap<>();
        topicSettings.put("foo", "foo value");
        topicSettings.put("bar", "bar value");
        topicSettings.put("baz.bim", "100");
        Map<String, String> settings = configs();
        topicSettings.forEach((k, v) -> settings.put(DistributedConfig.OFFSET_STORAGE_PREFIX + k, v));
        DistributedConfig config = new DistributedConfig(settings);
        assertEquals(topicSettings, config.offsetStorageTopicSettings());
    }

    @Test
    public void shouldAllowSettingStatusTopicSettings() {
        Map<String, String> topicSettings = new HashMap<>();
        topicSettings.put("foo", "foo value");
        topicSettings.put("bar", "bar value");
        topicSettings.put("baz.bim", "100");
        Map<String, String> settings = configs();
        topicSettings.forEach((k, v) -> settings.put(DistributedConfig.STATUS_STORAGE_PREFIX + k, v));
        DistributedConfig config = new DistributedConfig(settings);
        assertEquals(topicSettings, config.statusStorageTopicSettings());
    }

    @Test
    public void shouldRemoveCompactionFromConfigTopicSettings() {
        Map<String, String> expectedTopicSettings = new HashMap<>();
        expectedTopicSettings.put("foo", "foo value");
        expectedTopicSettings.put("bar", "bar value");
        expectedTopicSettings.put("baz.bim", "100");
        Map<String, String> topicSettings = new HashMap<>(expectedTopicSettings);
        topicSettings.put("cleanup.policy", "something-else");
        topicSettings.put("partitions", "3");

        Map<String, String> settings = configs();
        topicSettings.forEach((k, v) -> settings.put(DistributedConfig.CONFIG_STORAGE_PREFIX + k, v));
        DistributedConfig config = new DistributedConfig(settings);
        Map<String, Object> actual = config.configStorageTopicSettings();
        assertEquals(expectedTopicSettings, actual);
        assertNotEquals(topicSettings, actual);
    }

    @Test
    public void shouldRemoveCompactionFromOffsetTopicSettings() {
        Map<String, String> expectedTopicSettings = new HashMap<>();
        expectedTopicSettings.put("foo", "foo value");
        expectedTopicSettings.put("bar", "bar value");
        expectedTopicSettings.put("baz.bim", "100");
        Map<String, String> topicSettings = new HashMap<>(expectedTopicSettings);
        topicSettings.put("cleanup.policy", "something-else");

        Map<String, String> settings = configs();
        topicSettings.forEach((k, v) -> settings.put(DistributedConfig.OFFSET_STORAGE_PREFIX + k, v));
        DistributedConfig config = new DistributedConfig(settings);
        Map<String, Object> actual = config.offsetStorageTopicSettings();
        assertEquals(expectedTopicSettings, actual);
        assertNotEquals(topicSettings, actual);
    }

    @Test
    public void shouldRemoveCompactionFromStatusTopicSettings() {
        Map<String, String> expectedTopicSettings = new HashMap<>();
        expectedTopicSettings.put("foo", "foo value");
        expectedTopicSettings.put("bar", "bar value");
        expectedTopicSettings.put("baz.bim", "100");
        Map<String, String> topicSettings = new HashMap<>(expectedTopicSettings);
        topicSettings.put("cleanup.policy", "something-else");

        Map<String, String> settings = configs();
        topicSettings.forEach((k, v) -> settings.put(DistributedConfig.STATUS_STORAGE_PREFIX + k, v));
        DistributedConfig config = new DistributedConfig(settings);
        Map<String, Object> actual = config.statusStorageTopicSettings();
        assertEquals(expectedTopicSettings, actual);
        assertNotEquals(topicSettings, actual);
    }

    @Test
    public void testInvalidSecurityProtocol() {
        Map<String, String> configs = configs();

        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "abc");
        ConfigException ce = assertThrows(ConfigException.class,
                () -> new DistributedConfig(configs));
        assertTrue(ce.getMessage().contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void shouldIdentifyNeedForTransactionalLeader() {
        Map<String, String> workerProps = configs();

        workerProps.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "disabled");
        assertFalse(new DistributedConfig(workerProps).transactionalLeaderEnabled());

        workerProps.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "preparing");
        assertTrue(new DistributedConfig(workerProps).transactionalLeaderEnabled());

        workerProps.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        assertTrue(new DistributedConfig(workerProps).transactionalLeaderEnabled());
    }

    @Test
    public void shouldConstructExpectedTransactionalId() {
        Map<String, String> workerProps = configs();

        workerProps.put(GROUP_ID_CONFIG, "why did i stay up all night writing unit tests");
        assertEquals(
                "connect-cluster-why did i stay up all night writing unit tests",
                new DistributedConfig(workerProps).transactionalProducerId()
        );

        workerProps.put(GROUP_ID_CONFIG, "connect-cluster");
        assertEquals(
                "connect-cluster-connect-cluster",
                new DistributedConfig(workerProps).transactionalProducerId()
        );

        workerProps.put(GROUP_ID_CONFIG, "\u2603");
        assertEquals(
                "connect-cluster-\u2603",
                new DistributedConfig(workerProps).transactionalProducerId()
        );
    }

}
