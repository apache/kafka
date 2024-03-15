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

import kafka.api.AbstractSaslTest;
import kafka.api.Both$;
import kafka.utils.JaasTestUtils;
import kafka.zk.ConfigEntityChangeNotificationZNode;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.function.Executable;
import scala.Option;
import scala.Some$;
import scala.collection.JavaConverters;
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

public class SaslClientsWithInvalidCredentialsTest extends AbstractSaslTest {
    private static final String TOPIC = "topic";
    public static final int NUM_PARTITIONS = 1;
    public static final int BROKER_COUNT = 1;
    public static final String KAFKA_CLIENT_SASL_MECHANISM = "SCRAM-SHA-256";
    private static final Seq<String> KAFKA_SERVER_SASL_MECHANISMS = seq(Collections.singletonList(KAFKA_CLIENT_SASL_MECHANISM));

    @SuppressWarnings({"deprecation"})
    private Consumer<byte[], byte[]> createConsumer() {
        return createConsumer(
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer(),
            new Properties(),
            JavaConverters.asScalaSet(Collections.<String>emptySet()).toList()
        );
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return SecurityProtocol.SASL_PLAINTEXT;
    }

    @Override
    public Option<Properties> serverSaslProperties() {
        return Some$.MODULE$.apply(kafkaServerSaslProperties(KAFKA_SERVER_SASL_MECHANISMS, KAFKA_CLIENT_SASL_MECHANISM));
    }

    @Override
    public Option<Properties> clientSaslProperties() {
        return Some$.MODULE$.apply(kafkaClientSaslProperties(KAFKA_CLIENT_SASL_MECHANISM, false));
    }

    @Override
    public int brokerCount() {
        return 1;
    }

    @Override
    public void configureSecurityBeforeServersStart(TestInfo testInfo) {
        super.configureSecurityBeforeServersStart(testInfo);
        zkClient().makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path());
        // Create broker credentials before starting brokers
        createScramCredentials(zkConnect(), JaasTestUtils.KafkaScramAdmin(), JaasTestUtils.KafkaScramAdminPassword());
    }

    @Override
    public Admin createPrivilegedAdminClient() {
        return createAdminClient(bootstrapServers(listenerName()), securityProtocol(), trustStoreFile(), clientSaslProperties(),
            KAFKA_CLIENT_SASL_MECHANISM, JaasTestUtils.KafkaScramAdmin(), JaasTestUtils.KafkaScramAdminPassword());
    }

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) {
        startSasl(jaasSections(KAFKA_SERVER_SASL_MECHANISMS, Some$.MODULE$.apply(KAFKA_CLIENT_SASL_MECHANISM), Both$.MODULE$,
            JaasTestUtils.KafkaServerContextName()));
        super.setUp(testInfo);
        createTopic(
            TOPIC,
            NUM_PARTITIONS,
            BROKER_COUNT,
            new Properties(),
            listenerName(),
            new Properties());
    }

    @AfterEach
    @Override
    public void tearDown() {
        super.tearDown();
        closeSasl();
    }

    @Test
    public void testConsumerGroupServiceWithAuthenticationFailure() throws Exception {
        ConsumerGroupCommand.ConsumerGroupService consumerGroupService = prepareConsumerGroupService();
        try (Consumer<byte[], byte[]> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            verifyAuthenticationException(consumerGroupService::listGroups);
        } finally {
            consumerGroupService.close();
        }
    }

    @Test
    public void testConsumerGroupServiceWithAuthenticationSuccess() throws Exception {
        createScramCredentialsViaPrivilegedAdminClient(JaasTestUtils.KafkaScramUser2(), JaasTestUtils.KafkaScramPassword2());
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

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()),
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
