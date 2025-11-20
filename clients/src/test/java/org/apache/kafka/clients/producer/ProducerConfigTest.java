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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ProducerConfigTest {

    private final Serializer<byte[]> keySerializer = new ByteArraySerializer();
    private final Serializer<String> valueSerializer = new StringSerializer();
    private final Object keySerializerClass = keySerializer.getClass();
    private final Object valueSerializerClass = valueSerializer.getClass();

    @Test
    public void testAppendSerializerToConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        Map<String, Object> newConfigs = ProducerConfig.appendSerializerToConfig(configs, null, null);
        assertEquals(newConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClass);
        assertEquals(newConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClass);

        configs.clear();
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        newConfigs = ProducerConfig.appendSerializerToConfig(configs, keySerializer, null);
        assertEquals(newConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClass);
        assertEquals(newConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClass);

        configs.clear();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        newConfigs = ProducerConfig.appendSerializerToConfig(configs, null, valueSerializer);
        assertEquals(newConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClass);
        assertEquals(newConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClass);

        configs.clear();
        newConfigs = ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer);
        assertEquals(newConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClass);
        assertEquals(newConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClass);
    }

    @Test
    public void testAppendSerializerToConfigWithException() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, null);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        assertThrows(ConfigException.class, () -> ProducerConfig.appendSerializerToConfig(configs, null, valueSerializer));

        configs.clear();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, null);
        assertThrows(ConfigException.class, () -> ProducerConfig.appendSerializerToConfig(configs, keySerializer, null));
    }

    @Test
    public void testInvalidCompressionType() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "abc");
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        assertThrows(ConfigException.class, () -> new ProducerConfig(configs));
    }

    @Test
    public void testInvalidSecurityProtocol() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "abc");
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ConfigException ce = assertThrows(ConfigException.class, () -> new ProducerConfig(configs));
        assertTrue(ce.getMessage().contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void testDefaultMetadataRecoveryStrategy() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        final ProducerConfig producerConfig = new ProducerConfig(configs);
        assertEquals(MetadataRecoveryStrategy.REBOOTSTRAP.name, producerConfig.getString(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG));
    }

    @Test
    public void testInvalidMetadataRecoveryStrategy() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        configs.put(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "abc");
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ConfigException ce = assertThrows(ConfigException.class, () -> new ProducerConfig(configs));
        assertTrue(ce.getMessage().contains(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG));
    }

    @Test
    public void testCaseInsensitiveSecurityProtocol() {
        final String saslSslLowerCase = SecurityProtocol.SASL_SSL.name.toLowerCase(Locale.ROOT);
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, saslSslLowerCase);
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        final ProducerConfig producerConfig = new ProducerConfig(configs);
        assertEquals(saslSslLowerCase, producerConfig.originals().get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    void testUpperboundCheckOfEnableIdempotence() {
        String inFlightConnection = "6";
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, inFlightConnection);
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ConfigException configException = assertThrows(ConfigException.class, () -> new ProducerConfig(configs));
        assertEquals("To use the idempotent producer, " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION +
                                " must be set to at most 5. Current value is " + inFlightConnection + ".", configException.getMessage());

        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        assertDoesNotThrow(() -> new ProducerConfig(configs));
    }

    @Test
    void testTwoPhaseCommitIncompatibleWithTransactionTimeout() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-txn-id");
        configs.put(ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG, true);
        configs.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000);
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        ConfigException ce = assertThrows(ConfigException.class, () -> new ProducerConfig(configs));
        assertTrue(ce.getMessage().contains(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG));
        assertTrue(ce.getMessage().contains(ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG));

        // Verify that setting one but not the other is valid
        configs.remove(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        assertDoesNotThrow(() -> new ProducerConfig(configs));

        configs.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000);
        configs.put(ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG, false);
        assertDoesNotThrow(() -> new ProducerConfig(configs));
    }

    /**
     * Validates config/producer.properties file to avoid getting out of sync with ProducerConfig.
     */
    @Test
    public void testValidateConfigPropertiesFile() {
        Properties props = new Properties();

        try (InputStream inputStream = new FileInputStream(System.getProperty("user.dir") + "/../config/producer.properties")) {
            props.load(inputStream);
        } catch (Exception e) {
            fail("Failed to load config/producer.properties file: " + e.getMessage());
        }

        ProducerConfig config = new ProducerConfig(props);

        for (String key : config.originals().keySet()) {
            if (!ProducerConfig.configDef().configKeys().containsKey(key)) {
                fail("Invalid configuration key: " + key);
            }
        }
    }
}
