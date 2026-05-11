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
package org.apache.kafka.storage.internals.log;

// TODO: Remove Scala KafkaConfig dependency once KAFKA-15853 migrates KafkaConfig to Java
import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogConfigTest {

    @Test
    public void testKafkaConfigToProps() {
        long millisInHour = 60L * 60L * 1000L;
        long millisInDay = 24L * millisInHour;
        long bytesInGB = 1024L * 1024L * 1024L;
        Properties kafkaProps = StorageTestUtils.createDummyBrokerConfig();
        kafkaProps.put(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, "2");
        kafkaProps.put(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG, "2");
        kafkaProps.put(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, "960"); // 40 days
        kafkaProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "2592000000"); // 30 days
        kafkaProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "4294967296"); // 4 GB

        Map<String, Object> logProps = KafkaConfig.fromProps(kafkaProps).extractLogConfigMap();
        assertEquals(2 * millisInHour, logProps.get(TopicConfig.SEGMENT_MS_CONFIG));
        assertEquals(2 * millisInHour, logProps.get(TopicConfig.SEGMENT_JITTER_MS_CONFIG));
        assertEquals(40 * millisInDay, logProps.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals(30 * millisInDay, logProps.get(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG));
        assertEquals(4 * bytesInGB, logProps.get(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG));
    }

    @Test
    public void testFromPropsInvalid() {
        for (String name : LogConfig.configNames()) {
            Object[] invalidValues = invalidValuesForProp(name);
            if (invalidValues != null) {
                assertPropertyInvalid(name, invalidValues);
            }
        }
    }

    private Object[] invalidValuesForProp(String name) {
        return switch (name) {
            case TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG -> new Object[]{"not a boolean"};
            case TopicConfig.RETENTION_BYTES_CONFIG, TopicConfig.RETENTION_MS_CONFIG -> new Object[]{"not_a_number"};
            case TopicConfig.CLEANUP_POLICY_CONFIG -> new Object[]{"true", "foobar"};
            case TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG -> new Object[]{"not_a_number", "-0.1", "1.2"};
            case TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG -> new Object[]{"not_a_number", "0", "-1"};
            case TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG -> new Object[]{"not_a_number", "-3"};
            case TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG -> new Object[]{"not_a_number", "-2"};
            case TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG -> new Object[]{"not_a_number", "-1"};
            case TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG -> new Object[]{"not_a_number", "-0.1"};
            case TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG -> new Object[]{"not_a_number", "remove", "0"};
            case LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG -> null;
            default -> new Object[]{"not_a_number", "-1"};
        };
    }

    @Test
    public void testInvalidCompactionLagConfig() {
        HashMap<String, String> props = new HashMap<>();
        props.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "100");
        props.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "200");
        assertThrows(Exception.class, () -> LogConfig.validate(props));
    }

    @Test
    public void shouldValidateThrottledReplicasConfig() {
        assertTrue(isValid("*"));
        assertTrue(isValid("* "));
        assertTrue(isValid(""));
        assertTrue(isValid(" "));
        assertTrue(isValid("100:10"));
        assertTrue(isValid("100:10,12:10"));
        assertTrue(isValid("100:10,12:10,15:1"));
        assertTrue(isValid("100:10,12:10,15:1  "));
        assertTrue(isValid("100:0,"));

        assertFalse(isValid("100"));
        assertFalse(isValid("100:"));
        assertFalse(isValid("100:0,10"));
        assertFalse(isValid("100:0,10:"));
        assertFalse(isValid("100:0,10:   "));
        assertFalse(isValid("100 :0,10:   "));
        assertFalse(isValid("100: 0,10:   "));
        assertFalse(isValid("100:0,10 :   "));
        assertFalse(isValid("*,100:10"));
        assertFalse(isValid("* ,100:10"));
    }

    /* Sanity check that toHtmlTable produces one of the expected configs */
    @Test
    public void testToHtmlTable() {
        String html = LogConfig.configDefCopy().toHtmlTable();
        String expectedConfig = "<td>file.delete.delay.ms</td>";
        assertTrue(html.contains(expectedConfig), "Could not find `" + expectedConfig + "` in:\n " + html);
    }

    /* Sanity check that toHtml produces one of the expected configs */
    @Test
    public void testToHtml() {
        String html = LogConfig.configDefCopy().toHtml(4, key -> "prefix_" + key, Map.of());
        String expectedConfig = "<h4><a id=\"file.delete.delay.ms\"></a><a id=\"prefix_file.delete.delay.ms\" href=\"#prefix_file.delete.delay.ms\">file.delete.delay.ms</a></h4>";
        assertTrue(html.contains(expectedConfig), "Could not find `" + expectedConfig + "` in:\n " + html);
    }

    /* Sanity check that toEnrichedRst produces one of the expected configs */
    @Test
    public void testToEnrichedRst() {
        String rst = LogConfig.configDefCopy().toEnrichedRst();
        String expectedConfig = "``file.delete.delay.ms``";
        assertTrue(rst.contains(expectedConfig), "Could not find `" + expectedConfig + "` in:\n " + rst);
    }

    /* Sanity check that toRst produces one of the expected configs */
    @Test
    public void testToRst() {
        String rst = LogConfig.configDefCopy().toRst();
        String expectedConfig = "``file.delete.delay.ms``";
        assertTrue(rst.contains(expectedConfig), "Could not find `" + expectedConfig + "` in:\n " + rst);
    }

    @Test
    public void testGetConfigValue() {
        // Add a config that doesn't set the `serverDefaultConfigName`
        LogConfig.LogConfigDef configDef = LogConfig.configDefCopy();
        String configNameWithNoServerMapping = "log.foo";
        configDef.define(configNameWithNoServerMapping, ConfigDef.Type.INT, 1, ConfigDef.Importance.MEDIUM,
                configNameWithNoServerMapping + " doc");

        ConfigDef.ConfigKey deleteDelayKey = configDef.configKeys().get(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG);
        Object deleteDelayServerDefault = configDef.getConfigValue(deleteDelayKey, LogConfig.SERVER_DEFAULT_HEADER_NAME);
        assertEquals(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, deleteDelayServerDefault);

        ConfigDef.ConfigKey keyWithNoServerMapping = configDef.configKeys().get(configNameWithNoServerMapping);
        Object nullServerDefault = configDef.getConfigValue(keyWithNoServerMapping, LogConfig.SERVER_DEFAULT_HEADER_NAME);
        assertNull(nullServerDefault);
    }

    @Test
    public void testOverriddenConfigsAsLoggableString() {
        Properties kafkaProps = StorageTestUtils.createDummyBrokerConfig();
        kafkaProps.put("unknown.broker.password.config", "aaaaa");
        kafkaProps.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "50");
        kafkaProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "somekeypassword");
        KafkaConfig kafkaConfig = KafkaConfig.fromProps(kafkaProps);
        Properties topicOverrides = new Properties();
        // Only set as a topic config
        topicOverrides.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
        // Overrides value from broker config
        topicOverrides.setProperty(TopicConfig.RETENTION_BYTES_CONFIG, "100");
        // Unknown topic config, but known broker config
        topicOverrides.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "sometrustpasswrd");
        // Unknown config
        topicOverrides.setProperty("unknown.topic.password.config", "bbbb");
        // We don't currently have any sensitive topic configs, if we add them, we should set one here
        LogConfig logConfig = LogConfig.fromProps(kafkaConfig.extractLogConfigMap(), topicOverrides);
        assertEquals("{min.insync.replicas=2, retention.bytes=100, ssl.truststore.password=(redacted), unknown.topic.password.config=(redacted)}",
                logConfig.overriddenConfigsAsLoggableString());
    }

    private boolean isValid(String configValue) {
        try {
            ThrottledReplicaListValidator.ensureValidString("", configValue);
            return true;
        } catch (ConfigException e) {
            return false;
        }
    }

    private KafkaConfig createKafkaConfig(boolean remoteStorageEnabled, Properties extra) {
        Properties props = StorageTestUtils.createDummyBrokerConfig();
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, String.valueOf(remoteStorageEnabled));
        props.putAll(extra);
        return KafkaConfig.fromProps(props);
    }

    private KafkaConfig createKafkaConfig(boolean remoteStorageEnabled) {
        return createKafkaConfig(remoteStorageEnabled, new Properties());
    }

    private void assertPropertyInvalid(String name, Object... values) {
        for (Object value : values) {
            Properties props = new Properties();
            props.setProperty(name, value.toString());
            assertThrows(Exception.class, () -> new LogConfig(props),
                () -> "Property " + name + " should not allow " + value);
        }
    }

    @Test
    public void testLocalLogRetentionDerivedProps() {
        Properties props = new Properties();
        int retentionBytes = 1024;
        long retentionMs = 1000L;
        props.put(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(retentionBytes));
        props.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs));
        LogConfig logConfig = new LogConfig(props);

        assertEquals(retentionMs, logConfig.localRetentionMs());
        assertEquals(retentionBytes, logConfig.localRetentionBytes());
    }

    @Test
    public void testLocalLogRetentionDerivedDefaultProps() {
        LogConfig logConfig = new LogConfig(new Properties());

        // Local retention defaults are derived from retention properties which can be default or custom.
        assertEquals(LogConfig.DEFAULT_RETENTION_MS, logConfig.localRetentionMs());
        assertEquals(ServerLogConfigs.LOG_RETENTION_BYTES_DEFAULT, logConfig.localRetentionBytes());
    }

    @Test
    public void testLocalLogRetentionProps() {
        Properties props = new Properties();
        int localRetentionMs = 500;
        int localRetentionBytes = 1000;
        props.put(TopicConfig.RETENTION_BYTES_CONFIG, "2000");
        props.put(TopicConfig.RETENTION_MS_CONFIG, "1000");

        props.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, String.valueOf(localRetentionMs));
        props.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, String.valueOf(localRetentionBytes));
        LogConfig logConfig = new LogConfig(props);

        assertEquals(localRetentionMs, logConfig.localRetentionMs());
        assertEquals(localRetentionBytes, logConfig.localRetentionBytes());
    }

    @Test
    public void testInvalidLocalLogRetentionProps() {
        // Check for invalid localRetentionMs, < -2
        doTestInvalidLocalLogRetentionProps(-3, 10, 2, 500L);

        // Check for invalid localRetentionBytes < -2
        doTestInvalidLocalLogRetentionProps(500L, -3, 2, 1000L);

        // Check for invalid case of localRetentionMs > retentionMs
        doTestInvalidLocalLogRetentionProps(2000L, 2, 100, 1000L);

        // Check for invalid case of localRetentionBytes > retentionBytes
        doTestInvalidLocalLogRetentionProps(500L, 200, 100, 1000L);

        // Check for invalid case of localRetentionMs (-1 viz unlimited) > retentionMs,
        doTestInvalidLocalLogRetentionProps(-1, 200, 100, 1000L);

        // Check for invalid case of localRetentionBytes(-1 viz unlimited) > retentionBytes
        doTestInvalidLocalLogRetentionProps(2000L, -1, 100, 1000L);
    }

    private void doTestInvalidLocalLogRetentionProps(long localRetentionMs,
                                                     int localRetentionBytes,
                                                     int retentionBytes,
                                                     long retentionMs) {
        KafkaConfig kafkaConfig = createKafkaConfig(true);

        HashMap<String, String> props = new HashMap<>();
        props.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
        props.put(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(retentionBytes));
        props.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs));

        props.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, String.valueOf(localRetentionMs));
        props.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, String.valueOf(localRetentionBytes));
        assertThrows(ConfigException.class,
            () -> LogConfig.validate(Map.of(), props, kafkaConfig.extractLogConfigMap(),
                new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled()));
    }

    @Test
    public void testEnableRemoteLogStorageCleanupPolicy() {
        KafkaConfig kafkaConfig = createKafkaConfig(true);
        HashMap<String, String> logProps = new HashMap<>();

        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
        LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());

        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        assertThrows(ConfigException.class, () -> LogConfig.validate(Map.of(), logProps,
                kafkaConfig.extractLogConfigMap(), new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled()));

        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "delete,compact");
        assertThrows(ConfigException.class, () -> LogConfig.validate(Map.of(), logProps,
                kafkaConfig.extractLogConfigMap(), new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled()));

        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete");
        assertThrows(ConfigException.class, () -> LogConfig.validate(Map.of(), logProps,
                kafkaConfig.extractLogConfigMap(), new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled()));

        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "delete,delete,delete");
        LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());

        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "");
        LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());
    }

    @ParameterizedTest(name = "testEnableRemoteLogStorage with sysRemoteStorageEnabled: {0}")
    @ValueSource(booleans = {true, false})
    public void testEnableRemoteLogStorage(boolean sysRemoteStorageEnabled) {
        KafkaConfig kafkaConfig = createKafkaConfig(sysRemoteStorageEnabled);

        Map<String, String> logProps = Map.of(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
        if (sysRemoteStorageEnabled) {
            LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                    new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());
        } else {
            ConfigException message = assertThrows(ConfigException.class,
                    () -> LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                            new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled()));
            assertTrue(message.getMessage().contains("Tiered Storage functionality is disabled in the broker"));
        }
    }

    @ParameterizedTest(name = "testDisableRemoteLogStorage with wasRemoteStorageEnabled: {0}")
    @ValueSource(booleans = {true, false})
    public void testDisableRemoteLogStorage(boolean wasRemoteStorageEnabled) {
        KafkaConfig kafkaConfig = createKafkaConfig(true);

        HashMap<String, String> logProps = new HashMap<>();
        logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
        if (wasRemoteStorageEnabled) {
            InvalidConfigurationException message = assertThrows(InvalidConfigurationException.class,
                    () -> LogConfig.validate(Map.of(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                            logProps, kafkaConfig.extractLogConfigMap(),
                            new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled()));
            assertTrue(message.getMessage().contains("It is invalid to disable remote storage without deleting remote data. " +
                    "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
                    "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`."));

            // It should be able to disable the remote log storage when delete on disable is set to true
            logProps.put(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, "true");
            LogConfig.validate(Map.of(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                    logProps, kafkaConfig.extractLogConfigMap(),
                    new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());
        } else {
            LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                    new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());
            LogConfig.validate(Map.of(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"), logProps,
                    kafkaConfig.extractLogConfigMap(), new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());
        }
    }

    @ParameterizedTest(name = "testTopicCreationWithInvalidRetentionTime with sysRemoteStorageEnabled: {0}")
    @ValueSource(booleans = {true, false})
    public void testTopicCreationWithInvalidRetentionTime(boolean sysRemoteStorageEnabled) {
        Properties extra = new Properties();
        extra.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "1000");
        extra.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "900");
        KafkaConfig kafkaConfig = createKafkaConfig(sysRemoteStorageEnabled, extra);

        // Topic local log retention time inherited from Broker is greater than the topic's complete log retention time
        HashMap<String, String> logProps = new HashMap<>();
        logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, String.valueOf(sysRemoteStorageEnabled));
        logProps.put(TopicConfig.RETENTION_MS_CONFIG, "500");
        if (sysRemoteStorageEnabled) {
            ConfigException message = assertThrows(ConfigException.class,
                    () -> LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                            new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled()));
            assertTrue(message.getMessage().contains(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG));
        } else {
            LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                    new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());
        }
    }

    @ParameterizedTest(name = "testTopicCreationWithInvalidRetentionSize with sysRemoteStorageEnabled: {0}")
    @ValueSource(booleans = {true, false})
    public void testTopicCreationWithInvalidRetentionSize(boolean sysRemoteStorageEnabled) {
        Properties extra = new Properties();
        extra.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "1024");
        extra.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "512");
        KafkaConfig kafkaConfig = createKafkaConfig(sysRemoteStorageEnabled, extra);

        // Topic local retention size inherited from Broker is greater than the topic's complete log retention size
        Map<String, String> logProps = Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, String.valueOf(sysRemoteStorageEnabled),
            TopicConfig.RETENTION_MS_CONFIG, "500"
        );
        if (sysRemoteStorageEnabled) {
            ConfigException message = assertThrows(ConfigException.class,
                    () -> LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                            new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled()));
            assertTrue(message.getMessage().contains(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG));
        } else {
            LogConfig.validate(Map.of(), logProps, kafkaConfig.extractLogConfigMap(),
                    new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());
        }
    }

    @ParameterizedTest(name = "testValidateBrokerLogConfigs with sysRemoteStorageEnabled: {0}")
    @ValueSource(booleans = {true, false})
    public void testValidateBrokerLogConfigs(boolean sysRemoteStorageEnabled) {
        Properties extra = new Properties();
        extra.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "1024");
        extra.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "2048");
        KafkaConfig kafkaConfig = createKafkaConfig(sysRemoteStorageEnabled, extra);

        if (sysRemoteStorageEnabled) {
            ConfigException message = assertThrows(ConfigException.class,
                    () -> LogConfig.validateBrokerLogConfigValues(kafkaConfig.extractLogConfigMap(),
                            new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled()));
            assertTrue(message.getMessage().contains(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG));
        } else {
            LogConfig.validateBrokerLogConfigValues(kafkaConfig.extractLogConfigMap(),
                    new RemoteLogManagerConfig(kafkaConfig).isRemoteStorageSystemEnabled());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testValidRemoteLogCopyDisabled(boolean copyDisabled) {
        Map<String, String> logProps = Map.of(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, String.valueOf(copyDisabled));
        LogConfig.validate(logProps);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testValidRemoteLogDeleteOnDisable(boolean deleteOnDisable) {
        Map<String, String> logProps = Map.of(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, String.valueOf(deleteOnDisable));
        LogConfig.validate(logProps);
    }
}
