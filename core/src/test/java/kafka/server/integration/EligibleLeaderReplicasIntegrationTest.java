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
package kafka.server.integration;
import kafka.integration.KafkaServerTestHarness;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.utils.Logging;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.HashMap;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EligibleLeaderReplicasIntegrationTest extends KafkaServerTestHarness implements Logging {
    private String bootstrapServer;
    private String testTopicName;
    private Admin adminClient;
    @Override
    public Seq<KafkaConfig> generateConfigs() {
        List<Properties> brokerConfigs = new ArrayList<>();
        brokerConfigs.addAll(scala.collection.JavaConverters.seqAsJavaList(TestUtils.createBrokerConfigs(
            5,
            zkConnectOrNull(),
            true,
            true,
            scala.Option.empty(),
            scala.Option.empty(),
            scala.Option.empty(),
            true,
            false,
            false,
            false,
            new HashMap<>(),
            1,
            false,
            1,
            (short) 4,
            0,
            false
        )));
        List<KafkaConfig> configs = new ArrayList<>();
        for (Properties props : brokerConfigs) {
            props.put(KafkaConfig.ElrEnabledProp(), "true");
            configs.add(KafkaConfig.fromProps(props));
        }
        return JavaConverters.asScalaBuffer(configs).toSeq();
    }

    @Override
    public Seq<Properties> kraftControllerConfigs() {
        Properties properties = new Properties();
        properties.put(KafkaConfig.ElrEnabledProp(), "true");
        return new scala.collection.mutable.ListBuffer<Properties>().addOne(properties);
    }

    @BeforeEach
    public void setUp(TestInfo info) {
        super.setUp(info);
        // create adminClient
        Properties props = new Properties();
        bootstrapServer = bootstrapServers(listenerName());
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        adminClient = Admin.create(props);
        testTopicName = String.format("%s-%s", info.getTestMethod().get().getName(), TestUtils.randomString(10));
    }

    @AfterEach
    public void close() throws Exception {
        if (adminClient != null) adminClient.close();
        super.tearDown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"kraft"})
    public void testElrMemberCanBeElected(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            Collections.singletonList(new NewTopic(testTopicName, 1, (short) 4))).all().get();
        TestUtils.waitForPartitionMetadata(brokers(), testTopicName, 0, 1000);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        // alter configs on target cluster
        adminClient.incrementalAlterConfigs(configOps).all().get();

        try {
            // check which partition is on broker 0 which we'll kill
            TopicDescription testTopicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            killBroker(initialReplicas.get(0).id());
            killBroker(initialReplicas.get(1).id());
            killBroker(initialReplicas.get(2).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 1 && elrSize == 2;
            });

            topicPartitionInfo = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(2, topicPartitionInfo.elr().size());

            killBroker(initialReplicas.get(3).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 3;
            });

            topicPartitionInfo = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(1, topicPartitionInfo.lastKnownElr().size(), topicPartitionInfo.toString());
            int expectLastKnownLeader = initialReplicas.get(3).id();
            assertEquals(expectLastKnownLeader, topicPartitionInfo.lastKnownElr().getFirst().id(), topicPartitionInfo.toString());

            // At this point, all the replicas are failed and the last know leader is No.3 and 3 members in the ELR.
            // Restart one broker of the ELR and it should be the leader.

            int expectLeader = topicPartitionInfo.elr().stream()
                .filter(node -> node.id() != expectLastKnownLeader).collect(Collectors.toList()).get(0).id();

            startBroker(expectLeader);
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 1 && elrSize == 2;
            });

            topicPartitionInfo = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(0, topicPartitionInfo.lastKnownElr().size(), topicPartitionInfo.toString());
            assertEquals(expectLeader, topicPartitionInfo.leader().id(), topicPartitionInfo.toString());

            // Start another 2 brokers and the ELR fields should be cleaned.
            topicPartitionInfo.replicas().stream().filter(node -> node.id() != expectLeader).limit(2)
                .forEach(node -> startBroker(node.id()));

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 3 && elrSize == 0;
            });

            topicPartitionInfo = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(0, topicPartitionInfo.lastKnownElr().size(), topicPartitionInfo.toString());
            assertEquals(expectLeader, topicPartitionInfo.leader().id(), topicPartitionInfo.toString());
        } finally {
            restartDeadBrokers(false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"kraft"})
    public void testHighWatermarkShouldNotAdvanceIfUnderMinIsr(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
                Collections.singletonList(new NewTopic(testTopicName, 1, (short) 4))).all().get();
        TestUtils.waitForPartitionMetadata(brokers(), testTopicName, 0, 1000);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        // alter configs on target cluster
        adminClient.incrementalAlterConfigs(configOps).all().get();
        Producer producer = null;
        Consumer consumer = null;
        try {
            // check which partition is on broker 0 which we'll kill
            TopicDescription testTopicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName))
                    .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            Properties producerProps = new Properties();
            producerProps.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
            producer = new KafkaProducer(producerProps);

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
            consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singleton(testTopicName));

            producer.send(new ProducerRecord<>(testTopicName, "0", "0")).get();
            Thread.sleep(1000);
            ConsumerRecords records = consumer.poll(Duration.ofSeconds(1L));
            assertEquals(1, records.count());

            killBroker(initialReplicas.get(0).id());
            killBroker(initialReplicas.get(1).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 2 && elrSize == 1;
            });

            // Now the partition is under min ISR. HWM should not advance.
            producer.send(new ProducerRecord<>(testTopicName, "1", "1")).get();
            Thread.sleep(1000);
            records = consumer.poll(Duration.ofSeconds(1L));
            assertEquals(0, records.count());

            // Restore the min ISR and the previous log should be visible.
            startBroker(initialReplicas.get(1).id());
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 3 && elrSize == 0;
            });
            records = consumer.poll(Duration.ofSeconds(1L));
            assertEquals(1, records.count());
        } finally {
            restartDeadBrokers(false);
            if (consumer != null) consumer.close();
            if (producer != null) producer.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"kraft"})
    public void testElrMemberShouldBeKickOutWhenUncleanShutdown(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
                Collections.singletonList(new NewTopic(testTopicName, 1, (short) 4))).all().get();
        TestUtils.waitForPartitionMetadata(brokers(), testTopicName, 0, 1000);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        // alter configs on target cluster
        adminClient.incrementalAlterConfigs(configOps).all().get();

        try {
            // check which partition is on broker 0 which we'll kill
            TopicDescription testTopicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName))
                    .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            killBroker(initialReplicas.get(0).id());
            killBroker(initialReplicas.get(1).id());
            killBroker(initialReplicas.get(2).id());
            killBroker(initialReplicas.get(3).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 3;
            });
            topicPartitionInfo = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);

            int brokerToBeUncleanShutdown = topicPartitionInfo.elr().get(0).id();
            KafkaBroker broker = brokers().find(b -> {
                return b.config().brokerId() == brokerToBeUncleanShutdown;
            }).get();
            Seq<File> dirs = broker.logManager().liveLogDirs();
            assertEquals(1, dirs.size());
            CleanShutdownFileHandler handler = new CleanShutdownFileHandler(dirs.apply(0).toString());
            assertTrue(handler.exists());
            assertDoesNotThrow(() -> handler.delete());

            // After remove the clean shutdown file, the broker should report unclean shutdown during restart.
            startBroker(brokerToBeUncleanShutdown);
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 2;
            });
            topicPartitionInfo = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertTrue(topicPartitionInfo.leader() == null);
            assertEquals(1, topicPartitionInfo.lastKnownElr().size());
        } finally {
            restartDeadBrokers(false);
        }
    }

    /*
        This test is only valid for KIP-966 part 1. When the unclean recovery is implemented, it should be removed.
     */
    @ParameterizedTest
    @ValueSource(strings = {"kraft"})
    public void testLastKnownLeaderShouldBeElectedIfEmptyElr(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
                Collections.singletonList(new NewTopic(testTopicName, 1, (short) 4))).all().get();
        TestUtils.waitForPartitionMetadata(brokers(), testTopicName, 0, 1000);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        // alter configs on target cluster
        adminClient.incrementalAlterConfigs(configOps).all().get();

        try {
            // check which partition is on broker 0 which we'll kill
            TopicDescription testTopicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName))
                    .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            killBroker(initialReplicas.get(0).id());
            killBroker(initialReplicas.get(1).id());
            killBroker(initialReplicas.get(2).id());
            killBroker(initialReplicas.get(3).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 3;
            });
            topicPartitionInfo = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            int lastKnownLeader = topicPartitionInfo.lastKnownElr().get(0).id();

            brokers().foreach(broker -> {
                Seq<File> dirs = broker.logManager().liveLogDirs();
                assertEquals(1, dirs.size());
                CleanShutdownFileHandler handler = new CleanShutdownFileHandler(dirs.apply(0).toString());
                assertDoesNotThrow(() -> handler.delete());
                return true;
            });


            // After remove the clean shutdown file, the broker should report unclean shutdown during restart.
            topicPartitionInfo.replicas().stream().forEach(replica -> {
                if (replica.id() != lastKnownLeader) startBroker(replica.id());
            });
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 1;
            });
            topicPartitionInfo = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertTrue(topicPartitionInfo.leader() == null);
            assertEquals(1, topicPartitionInfo.lastKnownElr().size());

            // Now if the last known leader goes through unclean shutdown, it will still be elected.
            startBroker(lastKnownLeader);
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize > 0 && elrSize == 0;
            });
            topicPartitionInfo = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(lastKnownLeader, topicPartitionInfo.leader().id());
        } finally {
            restartDeadBrokers(false);
        }
    }

    void waitForIsrAndElr(BiFunction<Integer, Integer, Boolean> isIsrAndElrSizeSatisfied) {
        kafka.utils.TestUtils.waitUntilTrue(
            () -> {
                try {
                    TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName))
                            .allTopicNames().get().get(testTopicName);
                    TopicPartitionInfo partition = topicDescription.partitions().get(0);
                    if (!isIsrAndElrSizeSatisfied.apply(partition.isr().size(), partition.elr().size())) return false;
                } catch (Exception e) {
                    return false;
                }
                return true;
            },
            () -> String.format("Partition metadata for %s is not propagated", testTopicName),
            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);
    }
}
