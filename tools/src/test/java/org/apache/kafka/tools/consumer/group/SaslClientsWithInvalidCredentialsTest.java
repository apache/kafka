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

import kafka.api.Both$;
import kafka.api.SaslSetup;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.junit.ClusterTestExtensions;
import kafka.utils.JaasTestUtils;
//import kafka.zk.ConfigEntityChangeNotificationZNode;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
//import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import scala.Option;
import scala.Some$;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.tools.consumer.group.ConsumerGroupCommandTest.seq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InvalidCredentialsSasl implements SaslSetup {
    private final String bootstrapServers;
    private final SecurityProtocol securityProtocol;
    private final String clientSaslMechanism;

    public InvalidCredentialsSasl(String bootstrapServers, SecurityProtocol securityProtocol, String clientSaslMechanism) {
        this.bootstrapServers = bootstrapServers;
        this.securityProtocol = securityProtocol;
        this.clientSaslMechanism = clientSaslMechanism;
    }

    @Override
    public Admin createPrivilegedAdminClient() {
        return createAdminClient(bootstrapServers, securityProtocol, Option.empty(), Option.apply(kafkaClientSaslProperties(clientSaslMechanism, false)),
                clientSaslMechanism, JaasTestUtils.KafkaScramAdmin(), JaasTestUtils.KafkaScramAdminPassword());
    }
}

@Tag("integration")
@ClusterTestDefaults(serverProperties = {
//        @ClusterConfigProperty(key = ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, value = "SASL_PLAINTEXT"),
        @ClusterConfigProperty(key = BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, value = "SCRAM-SHA-256"),
        @ClusterConfigProperty(key = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, value = "SCRAM-SHA-256"),
})
@ExtendWith(value = ClusterTestExtensions.class)
public class SaslClientsWithInvalidCredentialsTest {
    private static final String TOPIC = "topic";
    public static final String KAFKA_CLIENT_SASL_MECHANISM = "SCRAM-SHA-256";
    private static final Seq<String> KAFKA_SERVER_SASL_MECHANISMS = seq(Collections.singletonList(KAFKA_CLIENT_SASL_MECHANISM));
    private final ClusterInstance clusterInstance;
    private InvalidCredentialsSasl sasl;

    SaslClientsWithInvalidCredentialsTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
        this.sasl = new InvalidCredentialsSasl(clusterInstance.bootstrapServers(), SecurityProtocol.SASL_PLAINTEXT, KAFKA_CLIENT_SASL_MECHANISM);
    }

    private Consumer<byte[], byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        props.put(SaslConfigs.SASL_MECHANISM, KAFKA_CLIENT_SASL_MECHANISM);
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    public void tearDown() {
        sasl.closeSasl();
    }

    @ClusterTest(securityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerGroupServiceWithAuthenticationFailure() throws Exception {
        sasl.startSasl(sasl.jaasSections(KAFKA_SERVER_SASL_MECHANISMS, Some$.MODULE$.apply(KAFKA_CLIENT_SASL_MECHANISM), Both$.MODULE$,
                JaasTestUtils.KafkaServerContextName()));
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            Assertions.assertDoesNotThrow(() -> sasl.createScramCredentials(admin, JaasTestUtils.KafkaScramAdmin(), JaasTestUtils.KafkaScramAdminPassword()));
            Assertions.assertDoesNotThrow(() -> admin.createTopics(Collections.singletonList(new NewTopic(TOPIC, 1, (short) 1))).topicId(TOPIC).get());
        }
        ConsumerGroupCommand.ConsumerGroupService consumerGroupService = prepareConsumerGroupService();
        try (Consumer<byte[], byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            verifyAuthenticationException(consumerGroupService::listGroups);
        } finally {
            consumerGroupService.close();
        }
    }

    @ClusterTest(securityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerGroupServiceWithAuthenticationSuccess() throws Exception {
        sasl.createScramCredentialsViaPrivilegedAdminClient(JaasTestUtils.KafkaScramUser2(), JaasTestUtils.KafkaScramPassword2());
        ConsumerGroupCommand.ConsumerGroupService consumerGroupService = prepareConsumerGroupService();
        try (Consumer<byte[], byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            TestUtils.waitForCondition(() -> {
                try {
                    consumer.poll(Duration.ofMillis(1000));
                    return true;
                } catch (SaslAuthenticationException ignored) {
                    return false;
                }
            }, "failed to poll data with authentication");
            assertEquals(1, consumerGroupService.listConsumerGroups().size());
        } finally {
            consumerGroupService.close();
        }
    }

    private ConsumerGroupCommand.ConsumerGroupService prepareConsumerGroupService() throws IOException {
        File propsFile = TestUtils.tempFile(
            "security.protocol=SASL_PLAINTEXT\n" +
            "sasl.mechanism=" + KAFKA_CLIENT_SASL_MECHANISM);

        String[] cgcArgs = new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(),
            "--describe",
            "--group", "test.group",
            "--command-config", propsFile.getAbsolutePath()};
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(cgcArgs);
        return new ConsumerGroupCommand.ConsumerGroupService(opts, Collections.emptyMap());
    }

    private void verifyAuthenticationException(Executable action) {
        long startMs = System.currentTimeMillis();
        assertThrows(Exception.class, action);
        long elapsedMs = System.currentTimeMillis() - startMs;
        assertTrue(elapsedMs <= 5000, "Poll took too long, elapsed=" + elapsedMs);
    }
}
