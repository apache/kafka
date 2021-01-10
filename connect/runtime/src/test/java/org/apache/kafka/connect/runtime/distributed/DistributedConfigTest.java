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

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import javax.crypto.KeyGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

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
    public void shouldCreateKeyGeneratorWithSpecificSettings() {
        final String algorithm = "HmacSHA1";
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, algorithm);
        configs.put(DistributedConfig.INTER_WORKER_KEY_SIZE_CONFIG, "512");
        configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, algorithm);
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
    public void shouldFailIfKeyAlgorithmNotInVerificationAlgorithmsList() {
        Map<String, String> configs = configs();
        configs.put(DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG, "HmacSHA1");
        configs.put(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG, "HmacSHA256");
        assertThrows(ConfigException.class, () -> new DistributedConfig(configs));
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
        topicSettings.entrySet().forEach(e -> {
            settings.put(DistributedConfig.CONFIG_STORAGE_PREFIX + e.getKey(), e.getValue());
        });
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
        topicSettings.entrySet().forEach(e -> {
            settings.put(DistributedConfig.OFFSET_STORAGE_PREFIX + e.getKey(), e.getValue());
        });
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
        topicSettings.entrySet().forEach(e -> {
            settings.put(DistributedConfig.STATUS_STORAGE_PREFIX + e.getKey(), e.getValue());
        });
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
        topicSettings.forEach((k, v) -> {
            settings.put(DistributedConfig.CONFIG_STORAGE_PREFIX + k, v);
        });
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
        topicSettings.forEach((k, v) -> {
            settings.put(DistributedConfig.OFFSET_STORAGE_PREFIX + k, v);
        });
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
        topicSettings.forEach((k, v) -> {
            settings.put(DistributedConfig.STATUS_STORAGE_PREFIX + k, v);
        });
        DistributedConfig config = new DistributedConfig(settings);
        Map<String, Object> actual = config.statusStorageTopicSettings();
        assertEquals(expectedTopicSettings, actual);
        assertNotEquals(topicSettings, actual);
    }
}
