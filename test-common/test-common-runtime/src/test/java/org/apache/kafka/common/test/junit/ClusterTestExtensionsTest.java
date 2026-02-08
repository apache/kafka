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

package org.apache.kafka.common.test.junit;

import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.JaasUtils;
import org.apache.kafka.common.test.api.AutoStart;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.GroupProtocol.CLASSIC;
import static org.apache.kafka.clients.consumer.GroupProtocol.CONSUMER;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT}, serverProperties = {
    @ClusterConfigProperty(key = "default.key", value = "default.value"),
    @ClusterConfigProperty(id = 0, key = "queued.max.requests", value = "100"),
})  // Set defaults for a few params in @ClusterTest(s)
public class ClusterTestExtensionsTest {

    private final ClusterInstance clusterInstance;

    ClusterTestExtensionsTest(ClusterInstance clusterInstance) {     // Constructor injections
        this.clusterInstance = clusterInstance;
    }

    // Static methods can generate cluster configurations
    static List<ClusterConfig> generate1() {
        Map<String, String> serverProperties = new HashMap<>();
        serverProperties.put("foo", "bar");
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setServerProperties(serverProperties)
                .setTags(List.of("Generated Test"))
                .build());
    }

    // With no params, configuration comes from the annotation defaults as well as @ClusterTestDefaults (if present)
    @ClusterTest
    public void testClusterTest(ClusterInstance clusterInstance) {
        Assertions.assertSame(this.clusterInstance, clusterInstance, "Injected objects should be the same");
        assertEquals(Type.KRAFT, clusterInstance.type()); // From the class level default
        assertEquals("default.value", clusterInstance.config().serverProperties().get("default.key"));
    }

    // generate1 is a template method which generates any number of cluster configs
    @ClusterTemplate("generate1")
    public void testClusterTemplate() {
        assertEquals(Type.KRAFT, clusterInstance.type(),
            "generate1 provided a KRAFT cluster, so we should see that here");
        assertEquals("bar", clusterInstance.config().serverProperties().get("foo"));
        assertEquals(List.of("Generated Test"), clusterInstance.config().tags());
    }

    // Multiple @ClusterTest can be used with @ClusterTests
    @ClusterTests({
        @ClusterTest(types = {Type.KRAFT}, serverProperties = {
            @ClusterConfigProperty(key = "foo", value = "baz"),
            @ClusterConfigProperty(key = "spam", value = "eggz"),
            @ClusterConfigProperty(key = "default.key", value = "overwrite.value"),
            @ClusterConfigProperty(id = 0, key = "queued.max.requests", value = "200"),
            @ClusterConfigProperty(id = 3000, key = "queued.max.requests", value = "300"),
            @ClusterConfigProperty(key = "spam", value = "eggs"),
            @ClusterConfigProperty(key = "default.key", value = "overwrite.value")
        }, tags = {
            "default.display.key1", "default.display.key2"
        }),
        @ClusterTest(types = {Type.CO_KRAFT}, serverProperties = {
            @ClusterConfigProperty(key = "foo", value = "baz"),
            @ClusterConfigProperty(key = "spam", value = "eggz"),
            @ClusterConfigProperty(key = "default.key", value = "overwrite.value"),
            @ClusterConfigProperty(id = 0, key = "queued.max.requests", value = "200"),
            @ClusterConfigProperty(key = "spam", value = "eggs"),
            @ClusterConfigProperty(key = "default.key", value = "overwrite.value")
        }, tags = {
            "default.display.key1", "default.display.key2"
        })
    })
    public void testClusterTests() throws ExecutionException, InterruptedException {
        assertEquals("baz", clusterInstance.config().serverProperties().get("foo"));
        assertEquals("eggs", clusterInstance.config().serverProperties().get("spam"));
        assertEquals("overwrite.value", clusterInstance.config().serverProperties().get("default.key"));
        assertEquals(List.of("default.display.key1", "default.display.key2"), clusterInstance.config().tags());

        // assert broker server 0 contains property queued.max.requests 200 from ClusterTest which overrides
        // the value 100 in server property in ClusterTestDefaults
        try (Admin admin = clusterInstance.admin()) {
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            Map<ConfigResource, Config> configs = admin.describeConfigs(List.of(configResource)).all().get();
            assertEquals(1, configs.size());
            assertEquals("200", configs.get(configResource).get("queued.max.requests").value());
        }
        // In KRaft cluster non-combined mode, assert the controller server 3000 contains the property queued.max.requests 300
        if (clusterInstance.type() == Type.KRAFT) {
            try (Admin admin = Admin.create(Map.of(
                    AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, clusterInstance.bootstrapControllers()))) {
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "3000");
                Map<ConfigResource, Config> configs = admin.describeConfigs(List.of(configResource)).all().get();
                assertEquals(1, configs.size());
                assertEquals("300", configs.get(configResource).get("queued.max.requests").value());
            }
        }
    }

    @ClusterTests({
        @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}),
        @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, disksPerBroker = 2),
    })
    public void testClusterTestWithDisksPerBroker() throws ExecutionException, InterruptedException, IOException {
        try (Admin admin = clusterInstance.admin()) {
            DescribeLogDirsResult result = admin.describeLogDirs(clusterInstance.brokerIds());
            result.allDescriptions().get().forEach((brokerId, logDirDescriptionMap) ->
                assertEquals(clusterInstance.config().numDisksPerBroker(), logDirDescriptionMap.size()));
        }
        for (Map.Entry<Integer, KafkaBroker> entry : clusterInstance.brokers().entrySet()) {
            int brokerId = entry.getKey();
            KafkaBroker broker = entry.getValue();
            List<String> logDirs = broker.config().logDirs();
            for (String logDir : logDirs) {
                Properties props = Utils.loadProps(new File(logDir, MetaPropertiesEnsemble.META_PROPERTIES_NAME).getAbsolutePath());
                MetaProperties metaProps = new MetaProperties.Builder(props).build();

                assertTrue(metaProps.clusterId().isPresent(), "Cluster ID missing in " + logDir);
                assertTrue(metaProps.nodeId().isPresent(), "Node ID missing in " + logDir);
                assertTrue(metaProps.directoryId().isPresent(), "Directory ID missing in " + logDir);

                assertEquals(MetaPropertiesVersion.V1, metaProps.version(), "MetaProperties version mismatch in " + logDir);
                assertEquals(clusterInstance.clusterId(), metaProps.clusterId().get(), "Cluster ID mismatch in " + logDir);
                assertEquals(brokerId, metaProps.nodeId().getAsInt(), "Node ID mismatch in " + logDir);
                assertEquals(metaProps.directoryId().get(), broker.logManager().directoryId(logDir).get(), "Directory ID mismatch in " + logDir);
            }
        }
    }

    @ClusterTest(autoStart = AutoStart.NO)
    public void testNoAutoStart() {
        Assertions.assertThrows(RuntimeException.class, () -> clusterInstance.brokers().values().stream().map(KafkaBroker::socketServer).findFirst());
        clusterInstance.start();
        assertTrue(clusterInstance.brokers().values().stream().map(KafkaBroker::socketServer).findFirst().isPresent());
    }

    @ClusterTest
    public void testDefaults(ClusterInstance clusterInstance) {
        assertEquals(MetadataVersion.latestTesting(), clusterInstance.config().metadataVersion());
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT})
    public void testSupportedNewGroupProtocols(ClusterInstance clusterInstance) {
        Set<GroupProtocol> supportedGroupProtocols = new HashSet<>();
        supportedGroupProtocols.add(CLASSIC);
        supportedGroupProtocols.add(CONSUMER);
        assertEquals(supportedGroupProtocols, clusterInstance.supportedGroupProtocols());
    }

    @ClusterTests({
        @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, serverProperties = {
            @ClusterConfigProperty(key = GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic"),
        })
    })
    public void testNotSupportedNewGroupProtocols(ClusterInstance clusterInstance) {
        assertEquals(Set.of(CLASSIC), clusterInstance.supportedGroupProtocols());
    }



    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, brokers = 3)
    public void testCreateTopic(ClusterInstance clusterInstance) throws Exception {
        String topicName = "test";
        int numPartition = 3;
        short numReplicas = 3;
        clusterInstance.createTopic(topicName, numPartition, numReplicas);

        try (Admin admin = clusterInstance.admin()) {
            Assertions.assertTrue(admin.listTopics().listings().get().stream().anyMatch(s -> s.name().equals(topicName)));
            List<TopicPartitionInfo> partitions = admin.describeTopics(Set.of(topicName)).allTopicNames().get()
                    .get(topicName).partitions();
            assertEquals(numPartition, partitions.size());
            Assertions.assertTrue(partitions.stream().allMatch(partition -> partition.replicas().size() == numReplicas));
        }
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, brokers = 4)
    public void testShutdownAndSyncMetadata(ClusterInstance clusterInstance) throws Exception {
        String topicName = "test";
        int numPartition = 3;
        short numReplicas = 3;
        clusterInstance.createTopic(topicName, numPartition, numReplicas);
        clusterInstance.shutdownBroker(0);
        clusterInstance.waitTopicCreation(topicName, numPartition);
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, brokers = 4)
    public void testClusterAliveBrokers(ClusterInstance clusterInstance) throws Exception {
        clusterInstance.waitForReadyBrokers();

        // Remove broker id 0
        clusterInstance.shutdownBroker(0);
        Assertions.assertFalse(clusterInstance.aliveBrokers().containsKey(0));
        Assertions.assertTrue(clusterInstance.brokers().containsKey(0));

        // add broker id 0 back
        clusterInstance.startBroker(0);
        Assertions.assertTrue(clusterInstance.aliveBrokers().containsKey(0));
        Assertions.assertTrue(clusterInstance.brokers().containsKey(0));
    }


    @ClusterTest(
        types = {Type.CO_KRAFT, Type.KRAFT},
        brokers = 4,
        serverProperties = {
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
        }
    )
    public void testVerifyTopicDeletion(ClusterInstance clusterInstance) throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String testTopic = "testTopic";
            admin.createTopics(List.of(new NewTopic(testTopic, 1, (short) 1)));
            clusterInstance.waitTopicCreation(testTopic, 1);
            admin.deleteTopics(List.of(testTopic));
            clusterInstance.waitTopicDeletion(testTopic);
            Assertions.assertTrue(admin.listTopics().listings().get().stream().noneMatch(
                    topic -> topic.name().equals(testTopic)
            ));
        }
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, brokers = 3)
    public void testCreateProducerAndConsumer(ClusterInstance cluster) throws InterruptedException {
        String topic = "topic";
        String key = "key";
        String value = "value";
        try (Admin adminClient = cluster.admin();
             Producer<String, String> producer = cluster.producer(Map.of(
                 ACKS_CONFIG, "all",
                 KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                 VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
             Consumer<String, String> consumer = cluster.consumer(Map.of(
                 KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                 VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))
        ) {
            adminClient.createTopics(Set.of(new NewTopic(topic, 1, (short) 1)));
            assertNotNull(producer);
            assertNotNull(consumer);
            producer.send(new ProducerRecord<>(topic, key, value));
            producer.flush();
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            RaftClusterInvocationContext.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(records::add);
                return records.size() == 1;
            }, "Failed to receive message");
            assertEquals(key, records.get(0).key());
            assertEquals(value, records.get(0).value());
        }
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    })
    public void testCreateDefaultProducerAndConsumer(ClusterInstance cluster) throws InterruptedException {
        String topic = "topic";
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        try (Admin adminClient = cluster.admin();
             Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer()
        ) {
            adminClient.createTopics(Set.of(new NewTopic(topic, 1, (short) 1)));
            assertNotNull(producer);
            assertNotNull(consumer);
            producer.send(new ProducerRecord<>(topic, key, value));
            producer.flush();
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            RaftClusterInvocationContext.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(records::add);
                return records.size() == 1;
            }, "Failed to receive message");
            assertArrayEquals(key, records.get(0).key());
            assertArrayEquals(value, records.get(0).value());
        }
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, controllerListener = "FOO")
    public void testControllerListenerName(ClusterInstance cluster) throws ExecutionException, InterruptedException {
        assertEquals("FOO", cluster.controllerListenerName().value());
        try (Admin admin = cluster.admin(Map.of(), true)) {
            assertEquals(1, admin.describeMetadataQuorum().quorumInfo().get().nodes().size());
        }
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, brokers = 1)
    public void testBrokerRestart(ClusterInstance cluster) throws ExecutionException, InterruptedException {
        final String topicName = "topic";
        try (Admin admin = cluster.admin();
             Producer<String, String> producer = cluster.producer(Map.of(
                 ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                 ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get();

            cluster.waitTopicCreation(topicName, 1);

            cluster.brokers().values().forEach(broker -> {
                broker.shutdown();
                broker.awaitShutdown();
                broker.startup();
            });

            RecordMetadata recordMetadata0 = producer.send(new ProducerRecord<>(topicName, 0, "key 0", "value 0")).get();
            assertEquals(0, recordMetadata0.offset());
        }
    }

    @ClusterTest(types = {Type.KRAFT})
    public void testControllerRestart(ClusterInstance cluster) throws ExecutionException, InterruptedException {
        try (Admin admin = cluster.admin()) {

            ControllerServer controller = cluster.controllers().values().iterator().next();
            controller.shutdown();
            controller.awaitShutdown();

            controller.startup();

            assertEquals(1, admin.describeMetadataQuorum().quorumInfo().get().nodes().size());
        }
    }

    @ClusterTest(
        types = {Type.KRAFT, Type.CO_KRAFT},
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
        }
    )
    public void testSaslPlaintext(ClusterInstance clusterInstance) throws CancellationException, ExecutionException, InterruptedException {
        assertEquals(SecurityProtocol.SASL_PLAINTEXT, clusterInstance.config().brokerSecurityProtocol());

        // default ClusterInstance#admin helper with admin credentials
        try (Admin admin = clusterInstance.admin()) {
            admin.describeAcls(AclBindingFilter.ANY).values().get();
        }
        String topic = "sasl-plaintext-topic";
        clusterInstance.createTopic(topic, 1, (short) 1);
        try (Producer<byte[], byte[]> producer = clusterInstance.producer()) {
            producer.send(new ProducerRecord<>(topic, Utils.utf8("key"), Utils.utf8("value"))).get();
            producer.flush();
        }
        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer()) {
            consumer.subscribe(List.of(topic));
            RaftClusterInvocationContext.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                return records.count() == 1;
            }, "Failed to receive message");
        }

        // client with non-admin credentials
        Map<String, Object> nonAdminConfig = Map.of(
            SaslConfigs.SASL_JAAS_CONFIG,
            String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD
            )
        );
        try (Admin admin = clusterInstance.admin(nonAdminConfig)) {
            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> admin.describeAcls(AclBindingFilter.ANY).values().get()
            );
            assertInstanceOf(ClusterAuthorizationException.class, exception.getCause());
        }
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(nonAdminConfig)) {
            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> producer.send(new ProducerRecord<>(topic, Utils.utf8("key"), Utils.utf8("value"))).get()
            );
            assertInstanceOf(TopicAuthorizationException.class, exception.getCause());
        }
        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(nonAdminConfig)) {
            consumer.subscribe(List.of(topic));
            AtomicBoolean hasException = new AtomicBoolean(false);
            RaftClusterInvocationContext.waitForCondition(() -> {
                if (hasException.get()) {
                    return true;
                }
                try {
                    consumer.poll(Duration.ofMillis(100));
                } catch (TopicAuthorizationException e) {
                    hasException.set(true);
                }
                return false;
            }, "Failed to get exception");
        }

        // client with unknown credentials
        Map<String, Object> unknownUserConfig = Map.of(
            SaslConfigs.SASL_JAAS_CONFIG,
            String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                "unknown", "unknown"
            )
        );
        try (Admin admin = clusterInstance.admin(unknownUserConfig)) {
            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> admin.describeAcls(AclBindingFilter.ANY).values().get()
            );
            assertInstanceOf(SaslAuthenticationException.class, exception.getCause());
        }
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(unknownUserConfig)) {
            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> producer.send(new ProducerRecord<>(topic, Utils.utf8("key"), Utils.utf8("value"))).get()
            );
            assertInstanceOf(SaslAuthenticationException.class, exception.getCause());
        }
        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(unknownUserConfig)) {
            consumer.subscribe(List.of(topic));
            AtomicBoolean hasException = new AtomicBoolean(false);
            RaftClusterInvocationContext.waitForCondition(() -> {
                if (hasException.get()) {
                    return true;
                }
                try {
                    consumer.poll(Duration.ofMillis(100));
                } catch (SaslAuthenticationException e) {
                    hasException.set(true);
                }
                return false;
            }, "Failed to get exception");
        }
    }

    @ClusterTest(
        types = {Type.KRAFT, Type.CO_KRAFT},
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
        }
    )
    public void testSaslPlaintextWithController(ClusterInstance clusterInstance) throws CancellationException, ExecutionException, InterruptedException {
        // default ClusterInstance#admin helper with admin credentials
        try (Admin admin = clusterInstance.admin(Map.of(), true)) {
            admin.describeAcls(AclBindingFilter.ANY).values().get();
        }

        // client with non-admin credentials
        Map<String, Object> nonAdminConfig = Map.of(
            SaslConfigs.SASL_JAAS_CONFIG,
            String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD
            )
        );
        try (Admin admin = clusterInstance.admin(nonAdminConfig, true)) {
            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> admin.describeAcls(AclBindingFilter.ANY).values().get()
            );
            assertInstanceOf(ClusterAuthorizationException.class, exception.getCause());
        }

        // client with unknown credentials
        Map<String, Object> unknownUserConfig = Map.of(
            SaslConfigs.SASL_JAAS_CONFIG,
            String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                "unknown", "unknown"
            )
        );
        try (Admin admin = clusterInstance.admin(unknownUserConfig)) {
            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> admin.describeAcls(AclBindingFilter.ANY).values().get()
            );
            assertInstanceOf(SaslAuthenticationException.class, exception.getCause());
        }
    }
}
