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
package org.apache.kafka.clients.security;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.function.Executable;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(key = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, value = SaslClientsWithInvalidCredentialsTest.ENABLED_MECHANISMS),
        @ClusterConfigProperty(key = BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, value = "PLAIN"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, value = "true")
    }
)
public class SaslClientsWithInvalidCredentialsTest {
    public static final String SASL_MECHANISM = "SCRAM-SHA-256";
    public static final String ENABLED_MECHANISMS = "PLAIN," + SASL_MECHANISM;

    private static final String CLIENT_USER = "scram-user";
    private static final String CLIENT_PASSWORD = "scram-user-secret";
    private static final String TOPIC = "topic";
    private static final int NUM_PARTITIONS = 1;
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private final ClusterInstance cluster;

    public SaslClientsWithInvalidCredentialsTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setUp() throws Exception {
        try (var admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1))).all().get();
        }
        cluster.waitTopicCreation(TOPIC, NUM_PARTITIONS);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testIdempotentProducerWithAuthenticationFailure() throws InterruptedException {
        try (Producer<byte[], byte[]> producer = cluster.producer(clientConfig(CLIENT_USER, Map.of(
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"
        )))) {
            verifyAuthenticationException(() -> sendOneRecord(producer, 10000));
            verifyAuthenticationException(() -> producer.partitionsFor(TOPIC));
            createScramCredential(CLIENT_USER);
        }

        try (Producer<byte[], byte[]> producer = cluster.producer(clientConfig(CLIENT_USER, Map.of(
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"
        )))) {
            TestUtils.retryOnExceptionWithTimeout(() -> sendOneRecord(producer, 15000));
        }
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testNonIdempotentProducerWithAuthenticationFailure() throws InterruptedException {
        try (Producer<byte[], byte[]> producer = cluster.producer(clientConfig(CLIENT_USER, Map.of(
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false"
        )))) {
            verifyAuthenticationException(() -> sendOneRecord(producer, 10000));
            verifyAuthenticationException(() -> producer.partitionsFor(TOPIC));
            createScramCredential(CLIENT_USER);
            TestUtils.retryOnExceptionWithTimeout(() -> sendOneRecord(producer, 15000));
        }
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testTransactionalProducerWithAuthenticationFailure() {
        try (Producer<byte[], byte[]> producer = cluster.producer(clientConfig(CLIENT_USER, Map.of(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txclient-1",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"
        )))) {
            verifyAuthenticationException(producer::initTransactions);

            createScramCredential(CLIENT_USER);
            assertThrows(KafkaException.class, producer::initTransactions);
        }
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerWithAuthenticationFailure() throws InterruptedException {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String user = CLIENT_USER + "-" + groupProtocol.name().toLowerCase(Locale.ROOT);
            try (Consumer<byte[], byte[]> consumer = createConsumer(user, Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT)
            ))) {
                consumer.subscribe(List.of(TOPIC));
                verifyConsumerWithAuthenticationFailure(consumer, user, true);
            }
        }
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testManualAssignmentConsumerWithAuthenticationFailure() throws InterruptedException {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String user = CLIENT_USER + "-" + groupProtocol.name().toLowerCase(Locale.ROOT);
            try (Consumer<byte[], byte[]> consumer = createConsumer(user, Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT)
            ))) {
                consumer.assign(List.of(TP));
                verifyConsumerWithAuthenticationFailure(consumer, user, false);
            }
        }
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testManualAssignmentConsumerWithAutoCommitDisabledWithAuthenticationFailure() throws InterruptedException {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String user = CLIENT_USER + "-" + groupProtocol.name().toLowerCase(Locale.ROOT);
            try (Consumer<byte[], byte[]> consumer = createConsumer(user, Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT)
            ))) {
                consumer.assign(List.of(TP));
                consumer.seek(TP, 0);
                verifyConsumerWithAuthenticationFailure(consumer, user, false);
            }
        }
    }

    private void verifyConsumerWithAuthenticationFailure(
        Consumer<byte[], byte[]> consumer,
        String user,
        boolean usePollForInitialFailure
    ) throws InterruptedException {
        long startMs = System.currentTimeMillis();
        if (usePollForInitialFailure) {
            assertThrows(Exception.class, () -> consumer.poll(Duration.ofMillis(1000)));
        } else {
            assertThrows(Exception.class, () -> consumer.partitionsFor(TOPIC));
        }
        long elapsedMs = System.currentTimeMillis() - startMs;
        assertTrue(elapsedMs <= 5000, "Poll took too long, elapsed=" + elapsedMs);

        createScramCredential(user);
        try (var producer = cluster.<byte[], byte[]>producer(clientConfig(user, Map.of()))) {
            TestUtils.retryOnExceptionWithTimeout(() -> sendOneRecord(producer, 15000));
        }
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.contains(TP)) {
            consumer.seek(TP, 0);
        }
        TestUtils.waitForCondition(
            () -> {
                try {
                    return consumer.poll(Duration.ofMillis(1000)).count() >= 1;
                } catch (SaslAuthenticationException e) {
                    return false;
                }
            },
            "Consumer.poll() did not read the expected number of records within the timeout"
        );
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testKafkaAdminClientWithAuthenticationFailure() throws InterruptedException {
        try (var admin = cluster.admin(clientConfig(SaslClientsWithInvalidCredentialsTest.CLIENT_USER, Map.of()))) {
            verifyAuthenticationException(() -> assertTopicDescription(admin));

            createScramCredential(CLIENT_USER);
            TestUtils.retryOnExceptionWithTimeout(() -> assertTopicDescription(admin));
        }
    }

    private void assertTopicDescription(Admin admin) throws InterruptedException {
        try {
            Map<String, TopicDescription> response = admin.describeTopics(List.of(TOPIC)).allTopicNames().get();
            assertEquals(1, response.size());
            response.forEach((topic, description) ->
                assertEquals(NUM_PARTITIONS, description.partitions().size()));
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    private void createScramCredential(String userName) {
        try (var admin = cluster.admin()) {
            assertDoesNotThrow(() -> admin.alterUserScramCredentials(List.of(
                new UserScramCredentialUpsertion(
                    userName,
                    new ScramCredentialInfo(org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_256, 4096),
                        SaslClientsWithInvalidCredentialsTest.CLIENT_PASSWORD
                )
            )).all().get());
        }
    }

    private Consumer<byte[], byte[]> createConsumer(String user, Map<String, Object> configOverrides) {
        Map<String, Object> configs = clientConfig(user, configOverrides);
        configs.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return cluster.consumer(configs);
    }

    private Map<String, Object> clientConfig(String user, Map<String, Object> overrides) {
        Map<String, Object> configs = new HashMap<>(overrides);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        configs.put(SaslConfigs.SASL_MECHANISM, SASL_MECHANISM);
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, ScramLoginModule.class.getName()
                + " required username=\"" + user + "\" password=\"" + SaslClientsWithInvalidCredentialsTest.CLIENT_PASSWORD + "\";");
        return configs;
    }

    private void sendOneRecord(Producer<byte[], byte[]> producer, long maxWaitMs) throws InterruptedException, TimeoutException {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
            TP.topic(),
            TP.partition(),
            0L,
            "key".getBytes(),
            "value".getBytes()
        );
        try {
            producer.send(record).get(maxWaitMs, TimeUnit.MILLISECONDS);
            producer.flush();
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    private void verifyAuthenticationException(Executable action) {
        long startMs = System.currentTimeMillis();
        assertThrows(Exception.class, action);
        long elapsedMs = System.currentTimeMillis() - startMs;
        assertTrue(elapsedMs <= 5000, "Authentication failure took too long, elapsed=" + elapsedMs);
    }

}
