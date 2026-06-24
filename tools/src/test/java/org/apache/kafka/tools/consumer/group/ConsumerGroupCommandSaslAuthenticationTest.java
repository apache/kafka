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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.function.Executable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = SASL_ENABLED_MECHANISMS_CONFIG, value = "PLAIN,SCRAM-SHA-256"),
        @ClusterConfigProperty(key = ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, value = "true"),
    }
)
public class ConsumerGroupCommandSaslAuthenticationTest {

    private static final String TOPIC = "topic";
    private static final String KAFKA_CLIENT_SASL_MECHANISM = "SCRAM-SHA-256";
    private static final String SCRAM_USER = "scram-user";
    private static final String SCRAM_PASSWORD = "scram-user-secret";
    private static final String KAFKA_CLIENT_SASL_JAAS_CONFIG =
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + SCRAM_USER + "\" password=\"" + SCRAM_PASSWORD + "\";";

    private final ClusterInstance cluster;

    public ConsumerGroupCommandSaslAuthenticationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerGroupServiceWithAuthenticationFailureWithClassicGroupProtocol() throws Exception {
        testConsumerGroupServiceWithAuthenticationFailure(GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerGroupServiceWithAuthenticationFailureWithConsumerGroupProtocol() throws Exception {
        testConsumerGroupServiceWithAuthenticationFailure(GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerGroupServiceWithAuthenticationSuccessWithClassicGroupProtocol() throws Exception {
        testConsumerGroupServiceWithAuthenticationSuccess(GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerGroupServiceWithAuthenticationSuccessWithConsumerGroupProtocol() throws Exception {
        testConsumerGroupServiceWithAuthenticationSuccess(GroupProtocol.CONSUMER);
    }

    private void testConsumerGroupServiceWithAuthenticationFailure(GroupProtocol groupProtocol) throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        try (
            ConsumerGroupCommand.ConsumerGroupService consumerGroupService = prepareConsumerGroupService();
            KafkaConsumer<byte[], byte[]> consumer = createScramConsumer(groupProtocol)
        ) {
            consumer.subscribe(List.of(TOPIC));
            verifyAuthenticationException(consumerGroupService::listGroups);
        }
    }

    private void testConsumerGroupServiceWithAuthenticationSuccess(GroupProtocol groupProtocol) throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        createScramCredential(SCRAM_USER, SCRAM_PASSWORD);
        try (
            ConsumerGroupCommand.ConsumerGroupService consumerGroupService = prepareConsumerGroupService();
            KafkaConsumer<byte[], byte[]> consumer = createScramConsumer(groupProtocol)
        ) {
            consumer.subscribe(List.of(TOPIC));

            TestUtils.waitForCondition(() -> {
                try {
                    consumer.poll(Duration.ofMillis(1000));
                    return true;
                } catch (SaslAuthenticationException ignored) {
                    return false;
                }
            }, "failed to poll data with authentication");

            TestUtils.waitForCondition(
                () -> consumerGroupService.listConsumerGroups().size() == 1,
                "failed to find consumer group after successful poll"
            );
        }
    }

    private void createScramCredential(String user, String password) throws ExecutionException, InterruptedException {
        try (Admin admin = cluster.admin()) {
            AlterUserScramCredentialsResult result = admin.alterUserScramCredentials(List.of(
                new UserScramCredentialUpsertion(user,
                    new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), password)
            ));
            result.all().get();
        }
    }

    private KafkaConsumer<byte[], byte[]> createScramConsumer(GroupProtocol groupProtocol) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SASL_MECHANISM, KAFKA_CLIENT_SASL_MECHANISM);
        props.put(SASL_JAAS_CONFIG, KAFKA_CLIENT_SASL_JAAS_CONFIG);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private ConsumerGroupCommand.ConsumerGroupService prepareConsumerGroupService() throws IOException {
        File propsFile = TestUtils.tempFile(
            "security.protocol=SASL_PLAINTEXT\n" +
            "sasl.mechanism=" + KAFKA_CLIENT_SASL_MECHANISM + "\n" +
            "sasl.jaas.config=" + KAFKA_CLIENT_SASL_JAAS_CONFIG);

        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(),
            "--describe",
            "--group", "test.group",
            "--command-config", propsFile.getAbsolutePath()};
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(cgcArgs);
        return new ConsumerGroupCommand.ConsumerGroupService(opts, Map.of());
    }

    private void verifyAuthenticationException(Executable action) {
        long startMs = System.currentTimeMillis();
        assertThrows(Exception.class, action);
        long elapsedMs = System.currentTimeMillis() - startMs;
        assertTrue(elapsedMs <= 5000, "Poll took too long, elapsed=" + elapsedMs);
    }
}
