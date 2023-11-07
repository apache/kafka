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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerConfigTest {

    private final Deserializer<byte[]> keyDeserializer = new ByteArrayDeserializer();
    private final Deserializer<String> valueDeserializer = new StringDeserializer();
    private final String keyDeserializerClassName = keyDeserializer.getClass().getName();
    private final String valueDeserializerClassName = valueDeserializer.getClass().getName();
    private final Object keyDeserializerClass = keyDeserializer.getClass();
    private final Object valueDeserializerClass = valueDeserializer.getClass();
    private final Properties properties = new Properties();

    @BeforeEach
    public void setUp() {
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
    }

    @Test
    public void testOverrideClientId() {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        ConsumerConfig config = new ConsumerConfig(properties);
        assertFalse(config.getString(ConsumerConfig.CLIENT_ID_CONFIG).isEmpty());
    }

    @Test
    public void testOverrideEnableAutoCommit() {
        // Verify that our default properties (no 'enable.auto.commit' or 'group.id') are valid.
        assertEquals(false, new ConsumerConfig(properties).getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));

        // Verify that explicitly disabling 'enable.auto.commit' still works.
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        assertEquals(false, new ConsumerConfig(properties).getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));

        // Verify that enabling 'enable.auto.commit' but without 'group.id' fails.
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE.toString());
        assertThrows(InvalidConfigurationException.class, () -> new ConsumerConfig(properties));

        // Verify that then adding 'group.id' to the mix allows it to pass OK.
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        assertEquals(true, new ConsumerConfig(properties).getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));

        // Now remove the 'enable.auto.commit' flag and verify that it is set to true (the default).
        properties.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        assertEquals(true, new ConsumerConfig(properties).getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    public void testAppendDeserializerToConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        Map<String, Object> newConfigs = ConsumerConfig.appendDeserializerToConfig(configs, null, null);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);

        configs.clear();
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        newConfigs = ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, null);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);

        configs.clear();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        newConfigs = ConsumerConfig.appendDeserializerToConfig(configs, null, valueDeserializer);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);

        configs.clear();
        newConfigs = ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);
    }

    @Test
    public void testAppendDeserializerToConfigWithException() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, null);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        assertThrows(ConfigException.class, () -> ConsumerConfig.appendDeserializerToConfig(configs, null, valueDeserializer));

        configs.clear();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, null);
        assertThrows(ConfigException.class, () -> ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, null));
    }

    @Test
    public void ensureDefaultThrowOnUnsupportedStableFlagToFalse() {
        assertFalse(new ConsumerConfig(properties).getBoolean(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
    }

    @Test
    public void testDefaultPartitionAssignor() {
        assertEquals(Arrays.asList(RangeAssignor.class, CooperativeStickyAssignor.class),
            new ConsumerConfig(properties).getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));
    }

    @Test
    public void testInvalidGroupInstanceId() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        configs.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "");
        ConfigException ce = assertThrows(ConfigException.class, () -> new ConsumerConfig(configs));
        assertTrue(ce.getMessage().contains(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
    }

    @Test
    public void testInvalidSecurityProtocol() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "abc");
        ConfigException ce = assertThrows(ConfigException.class, () -> new ConsumerConfig(configs));
        assertTrue(ce.getMessage().contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void testCaseInsensitiveSecurityProtocol() {
        final String saslSslLowerCase = SecurityProtocol.SASL_SSL.name.toLowerCase(Locale.ROOT);
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, saslSslLowerCase);
        final ConsumerConfig consumerConfig = new ConsumerConfig(configs);
        assertEquals(saslSslLowerCase, consumerConfig.originals().get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void testDefaultConsumerGroupConfig() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        final ConsumerConfig consumerConfig = new ConsumerConfig(configs);
        assertEquals("generic", consumerConfig.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG));
        assertNull(consumerConfig.getString(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
    }

    @Test
    public void testRemoteAssignorConfig() {
        String remoteAssignorName = "org.apache.kafka.clients.group.someAssignor";
        String protocol = "consumer";
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        configs.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, remoteAssignorName);
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
        final ConsumerConfig consumerConfig = new ConsumerConfig(configs);
        assertEquals(protocol, consumerConfig.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG));
        assertEquals(remoteAssignorName, consumerConfig.getString(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
    }

    @ParameterizedTest
    @CsvSource({"consumer, true", "generic, true", "Consumer, true", "Generic, true", "invalid, false"})
    public void testProtocolConfigValidation(String protocol, boolean isValid) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
        if (isValid) {
            ConsumerConfig config = new ConsumerConfig(configs);
            assertEquals(protocol, config.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG));
        } else {
            assertThrows(ConfigException.class, () -> new ConsumerConfig(configs));
        }
    }
}
