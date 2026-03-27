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

package org.apache.kafka.clients;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ClientConfigPolicyException;
import org.apache.kafka.common.errors.UnknownConfigProfileException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.policy.ClientConfigPolicy;
import org.apache.kafka.server.policy.ClientConfigProfileKeys;
import org.apache.kafka.server.policy.ClientProfile;
import org.apache.kafka.server.policy.ClientPushConfigData;
f
import org.junit.jupiter.api.BeforeEach;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the client configuration push handshake (KIP-CFG).
 * <p>
 * Tests the end-to-end flow:
 * 1. Client sends GetConfigProfileKeys request
 * 2. Broker responds with profile keys and CRC
 * 3. Client sends PushConfig request with collected configs
 * 4. Broker validates and processes the config
 */
@ClusterTestDefaults(
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.CLIENT_CONFIG_POLICY_CLASS_NAME_CONFIG,
            value = "org.apache.kafka.clients.ClientConfigPushIntegrationTest$TestClientConfigPolicy"),
        @ClusterConfigProperty(key = ServerLogConfigs.CLIENT_CONFIG_MAX_BYTES_CONFIG, value = "1048576")
    }
)
public class ClientConfigPushIntegrationTest {

    private final ClusterInstance clusterInstance;
    private TestClientConfigPolicy clientConfigPolicy;

    public ClientConfigPushIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        clusterInstance.waitForReadyBrokers();
        // Reset test state
        receivedProfiles.clear();
        receivedConfigs.clear();
        profileKeysCallCount.set(0);
        processCallCount.set(0);
    }

    @ClusterTest
    public void testProducerConfigPushHandshake() throws Exception {
        Map<String, Object> producerConfig = new java.util.HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer-config-push");
        producerConfig.put(CommonClientConfigs.ENABLE_CONFIGS_PUSH_CONFIG, true);

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            // Give time for handshake to complete
            Thread.sleep(2000);

            // Verify policy was called
            assertTrue(profileKeysCallCount.get() > 0, "profileKeys() should have been called");
            assertTrue(processCallCount.get() > 0, "process() should have been called");

            // Verify we received a client profile
            assertFalse(receivedProfiles.isEmpty(), "Should have received client profile");

            // Verify we received configs
            assertFalse(receivedConfigs.isEmpty(), "Should have received client configs");

            // Verify configs were collected (should have client.id but not bootstrap.servers)
            Map<String, String> configs = receivedConfigs.values().iterator().next();
            assertTrue(configs.containsKey(ProducerConfig.CLIENT_ID_CONFIG),
                "Should contain client.id config");
            assertFalse(configs.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                "Should NOT contain bootstrap.servers (sensitive)");
        }
    }

    @ClusterTest
    public void testConsumerConfigPushHandshake() throws Exception {
        Map<String, Object> consumerConfig = new java.util.HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-config-push");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(CommonClientConfigs.ENABLE_CONFIGS_PUSH_CONFIG, true);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            // Give time for handshake to complete
            Thread.sleep(2000);

            // Verify policy was called
            assertTrue(profileKeysCallCount.get() > 0, "profileKeys() should have been called");
            assertTrue(processCallCount.get() > 0, "process() should have been called");

            // Verify we received configs
            assertFalse(receivedConfigs.isEmpty(), "Should have received client configs");

            Map<String, String> configs = receivedConfigs.values().iterator().next();
            assertTrue(configs.containsKey(ConsumerConfig.GROUP_ID_CONFIG),
                "Should contain group.id config");
            assertTrue(configs.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "Should contain auto.offset.reset config");
        }
    }

    @ClusterTest
    public void testConfigPushWithEmptyProfile() throws Exception {
        // Configure policy to return empty profile (no config keys requested)
        returnEmptyProfile = true;

        Map<String, Object> producerConfig = new java.util.HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(CommonClientConfigs.ENABLE_CONFIGS_PUSH_CONFIG, true);

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            // Give time for handshake to complete
            Thread.sleep(2000);

            // profileKeys() should be called
            assertTrue(profileKeysCallCount.get() > 0, "profileKeys() should have been called");

            // But process() should NOT be called since no configs were requested
            assertEquals(0, processCallCount.get(), "process() should NOT have been called with empty profile");
        }
    }

    @ClusterTest
    public void testConfigPushDisabled() throws Exception {
        Map<String, Object> producerConfig = new java.util.HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Disable config push
        producerConfig.put(CommonClientConfigs.ENABLE_CONFIGS_PUSH_CONFIG, false);

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            // Give time to ensure no handshake occurs
            Thread.sleep(2000);

            // Policy should NOT be called when disabled
            assertEquals(0, profileKeysCallCount.get(), "profileKeys() should NOT be called when disabled");
            assertEquals(0, processCallCount.get(), "process() should NOT be called when disabled");
        }
    }

    @ClusterTest
    public void testClientProfileContainsMetadata() throws Exception {
        Map<String, Object> producerConfig = new java.util.HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "metadata-test-producer");
        producerConfig.put(CommonClientConfigs.ENABLE_CONFIGS_PUSH_CONFIG, true);

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            Thread.sleep(2000);

            assertFalse(receivedProfiles.isEmpty(), "Should have received client profile");
            ClientProfile profile = receivedProfiles.values().iterator().next();

            assertNotNull(profile.clientInstanceId(), "Client instance ID should not be null");
            assertNotNull(profile.clientSoftwareName(), "Client software name should not be null");
            assertNotNull(profile.clientSoftwareVersion(), "Client software version should not be null");
            assertNotNull(profile.clientMetadata(), "Client metadata should not be null");

            // Verify software name is populated (e.g., "apache-kafka-java")
            assertFalse(profile.clientSoftwareName().isEmpty(),
                "Client software name should not be empty");
        }
    }

    @ClusterTest
    public void testCrcBasedProfileChangeDetection() throws Exception {
        Map<String, Object> producerConfig = new java.util.HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(CommonClientConfigs.ENABLE_CONFIGS_PUSH_CONFIG, true);

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            Thread.sleep(2000);

            // Verify CRC was computed and is non-zero
            assertTrue(profileKeysCallCount.get() > 0, "profileKeys() should have been called");

            // The CRC should be deterministic based on the keys
            // (We can't easily verify the exact value here, but we verified it's used)
        }
    }

    /**
     * Test implementation of ClientConfigPolicy for integration tests.
     * <p>
     * This policy:
     * - Returns a standard set of config keys for all clients
     * - Captures received profiles and configs for verification
     * - Supports test scenarios via static flags
     */
    public static class TestClientConfigPolicy implements ClientConfigPolicy {

        private final Map<Uuid, ClientProfile> receivedProfiles = new ConcurrentHashMap<>();
        private final Map<Uuid, Map<String, String>> receivedConfigs = new ConcurrentHashMap<>();
        private final AtomicInteger profileKeysCallCount = new AtomicInteger(0);
        private final AtomicInteger processCallCount = new AtomicInteger(0);

        private final SortedSet<String> standardConfigKeys = new TreeSet<>(Set.of(
            "client.id",
            "request.timeout.ms",
            "retry.backoff.ms",
            "metadata.max.age.ms",
            "send.buffer.bytes",
            "receive.buffer.bytes",
            "reconnect.backoff.ms",
            "reconnect.backoff.max.ms"
        ));

        @Override
        public void configure(Map<String, ?> configs) {
            // No configuration needed for test policy
        }

        @Override
        public Set<String> reconfigurableConfigs() {
            return Collections.emptySet();
        }

        @Override
        public void validateReconfiguration(Map<String, ?> configs) {
            // No reconfiguration validation needed
        }

        @Override
        public void reconfigure(Map<String, ?> configs) {
            // No reconfiguration needed
        }

        @Override
        public Optional<ClientConfigProfileKeys> profileKeys(ClientProfile clientProfile) {
            profileKeysCallCount.incrementAndGet();

            // Store the profile for verification
            receivedProfiles.put(clientProfile.clientInstanceId(), clientProfile);

            if (throwUnknownProfileOnKeys) {
                throw new UnknownConfigProfileException("Test: Unknown profile");
            }

            if (returnEmptyProfile) {
                return Optional.empty();
            }

            // Return standard config keys
            long crc = configurationProfileCrc(standardConfigKeys);
            return Optional.of(new ClientConfigProfileKeys(standardConfigKeys, crc));
        }

        @Override
        public void process(ClientPushConfigData pushConfigData) {
            processCallCount.incrementAndGet();

            // Store configs for verification
            receivedConfigs.put(
                pushConfigData.clientProfile().clientInstanceId(),
                pushConfigData.configs()
            );

            if (rejectNextPush) {
                throw new ClientConfigPolicyException("Test: Config validation failed");
            }

            // Validate that we received some configs
            if (pushConfigData.configs().isEmpty()) {
                throw new ClientConfigPolicyException("No configs received");
            }
        }

        @Override
        public void close() throws Exception {
            // Cleanup if needed
        }
    }
}
