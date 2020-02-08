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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkTestUtils;
import org.apache.kafka.common.network.NioEchoServer;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertThrows;

public class ClientAuthenticationFailureTest {
    private static MockTime time = new MockTime(50);

    private NioEchoServer server;
    private Map<String, Object> saslServerConfigs;
    private Map<String, Object> saslClientConfigs;
    private final String topic = "test";
    private TestJaasConfig testJaasConfig;

    @Before
    public void setup() throws Exception {
        LoginManager.closeAll();
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;

        saslServerConfigs = new HashMap<>();
        saslServerConfigs.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, Arrays.asList("PLAIN"));

        saslClientConfigs = new HashMap<>();
        saslClientConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        saslClientConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

        testJaasConfig = TestJaasConfig.createConfiguration("PLAIN", Arrays.asList("PLAIN"));
        testJaasConfig.setClientOptions("PLAIN", TestJaasConfig.USERNAME, "anotherpassword");
        server = createEchoServer(securityProtocol);
    }

    @After
    public void teardown() throws Exception {
        if (server != null)
            server.close();
    }

    @Test
    public void testConsumerWithInvalidCredentials() {
        Map<String, Object> props = new HashMap<>(saslClientConfigs);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + server.port());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "");
        StringDeserializer deserializer = new StringDeserializer();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props, deserializer, deserializer)) {
            assertThrows(SaslAuthenticationException.class, () -> {
                consumer.subscribe(Collections.singleton(topic));
                consumer.poll(Duration.ofSeconds(10));
            });
        }
    }

    @Test
    public void testProducerWithInvalidCredentials() {
        Map<String, Object> props = new HashMap<>(saslClientConfigs);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + server.port());
        StringSerializer serializer = new StringSerializer();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props, serializer, serializer)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message");
            Future<RecordMetadata> future = producer.send(record);
            TestUtils.assertFutureThrows(future, SaslAuthenticationException.class);
        }
    }

    @Test
    public void testAdminClientWithInvalidCredentials() {
        Map<String, Object> props = new HashMap<>(saslClientConfigs);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + server.port());
        try (Admin client = Admin.create(props)) {
            KafkaFuture<Map<String, TopicDescription>> future = client.describeTopics(Collections.singleton("test")).all();
            TestUtils.assertFutureThrows(future, SaslAuthenticationException.class);
        }
    }

    @Test
    public void testTransactionalProducerWithInvalidCredentials() {
        Map<String, Object> props = new HashMap<>(saslClientConfigs);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + server.port());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txclient-1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        StringSerializer serializer = new StringSerializer();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props, serializer, serializer)) {
            assertThrows(SaslAuthenticationException.class, producer::initTransactions);
        }
    }

    private NioEchoServer createEchoServer(SecurityProtocol securityProtocol) throws Exception {
        return createEchoServer(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
    }

    private NioEchoServer createEchoServer(ListenerName listenerName, SecurityProtocol securityProtocol) throws Exception {
        return NetworkTestUtils.createEchoServer(listenerName, securityProtocol,
                new TestSecurityConfig(saslServerConfigs), new CredentialCache(), time);
    }
}
