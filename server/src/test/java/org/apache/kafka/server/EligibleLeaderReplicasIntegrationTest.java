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
package org.apache.kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    brokers = 5,
    serverProperties = {
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, value = "4")
    }
)
public class EligibleLeaderReplicasIntegrationTest {
    private final ClusterInstance clusterInstance;

    EligibleLeaderReplicasIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_4_0_IV1)
    public void testHighWatermarkShouldNotAdvanceIfUnderMinIsr() throws ExecutionException, InterruptedException {
        try (var admin = clusterInstance.admin();
            var producer = clusterInstance.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "1"));
            var consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test",
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "10",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            String testTopicName = String.format("%s-%s", "testHighWatermarkShouldNotAdvanceIfUnderMinIsr", "ELR-test");
            admin.updateFeatures(
                Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME,
                    new FeatureUpdate(EligibleLeaderReplicasVersion.ELRV_1.featureLevel(), FeatureUpdate.UpgradeType.UPGRADE))).all().get();

            admin.createTopics(List.of(new NewTopic(testTopicName, 1, (short) 4))).all().get();
            clusterInstance.waitTopicCreation(testTopicName, 1);

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
            Collection<AlterConfigOp> ops = new ArrayList<>();
            ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
            Map<ConfigResource, Collection<AlterConfigOp>> configOps = Map.of(configResource, ops);
            // alter configs on target cluster
            admin.incrementalAlterConfigs(configOps).all().get();

            TopicDescription testTopicDescription = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            consumer.subscribe(Set.of(testTopicName));
            producer.send(new ProducerRecord<>(testTopicName, "0", "0")).get();
            waitUntilOneMessageIsConsumed(consumer);

            clusterInstance.shutdownBroker(initialReplicas.get(0).id());
            clusterInstance.shutdownBroker(initialReplicas.get(1).id());

            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 2 && elrSize == 1, admin, testTopicName);

            TopicPartition partition = new TopicPartition(testTopicName, 0);
            long leoBeforeSend = admin.listOffsets(Map.of(partition, OffsetSpec.latest())).partitionResult(partition).get().offset();
            // Now the partition is under min ISR. HWM should not advance.
            producer.send(new ProducerRecord<>(testTopicName, "1", "1")).get();
            long leoAfterSend = admin.listOffsets(Map.of(partition, OffsetSpec.latest())).partitionResult(partition).get().offset();
            assertEquals(leoBeforeSend, leoAfterSend);

            // Restore the min ISR and the previous log should be visible.
            clusterInstance.startBroker(initialReplicas.get(1).id());
            clusterInstance.startBroker(initialReplicas.get(0).id());
            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 4 && elrSize == 0, admin, testTopicName);

            waitUntilOneMessageIsConsumed(consumer);
        }
    }

    void waitUntilOneMessageIsConsumed(Consumer<?, ?> consumer) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> {
                try {
                    return consumer.poll(Duration.ofMillis(100L)).count() >= 1;
                } catch (Exception e) {
                    return false;
                }
            },
            DEFAULT_MAX_WAIT_MS,
            () -> "fail to consume messages"
        );
    }

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_4_0_IV1)
    public void testElrMemberCanBeElected() throws ExecutionException, InterruptedException {
        try (var admin = clusterInstance.admin()) {
            String testTopicName = String.format("%s-%s", "testElrMemberCanBeElected", "ELR-test");

            admin.updateFeatures(
                Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME,
                    new FeatureUpdate(EligibleLeaderReplicasVersion.ELRV_1.featureLevel(), FeatureUpdate.UpgradeType.UPGRADE))).all().get();
            admin.createTopics(List.of(new NewTopic(testTopicName, 1, (short) 4))).all().get();
            clusterInstance.waitTopicCreation(testTopicName, 1);

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
            Collection<AlterConfigOp> ops = new ArrayList<>();
            ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
            Map<ConfigResource, Collection<AlterConfigOp>> configOps = Map.of(configResource, ops);
            // alter configs on target cluster
            admin.incrementalAlterConfigs(configOps).all().get();

            TopicDescription testTopicDescription = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            clusterInstance.shutdownBroker(initialReplicas.get(0).id());
            clusterInstance.shutdownBroker(initialReplicas.get(1).id());
            clusterInstance.shutdownBroker(initialReplicas.get(2).id());

            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 1 && elrSize == 2, admin, testTopicName);

            clusterInstance.shutdownBroker(initialReplicas.get(3).id());

            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 0 && elrSize == 3, admin, testTopicName);

            topicPartitionInfo = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(1, topicPartitionInfo.lastKnownElr().size(), topicPartitionInfo.toString());
            int expectLastKnownLeader = initialReplicas.get(3).id();
            assertEquals(expectLastKnownLeader, topicPartitionInfo.lastKnownElr().get(0).id(), topicPartitionInfo.toString());

            // At this point, all the replicas are failed and the last know leader is No.3 and 3 members in the ELR.
            // Restart one broker of the ELR and it should be the leader.

            int expectLeader = topicPartitionInfo.elr().stream()
                .filter(node -> node.id() != expectLastKnownLeader).toList().get(0).id();

            clusterInstance.startBroker(expectLeader);
            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 1 && elrSize == 2, admin, testTopicName);

            topicPartitionInfo = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(0, topicPartitionInfo.lastKnownElr().size(), topicPartitionInfo.toString());
            assertEquals(expectLeader, topicPartitionInfo.leader().id(), topicPartitionInfo.toString());

            // Start another 2 brokers and the ELR fields should be cleaned.
            topicPartitionInfo.replicas().stream().filter(node -> node.id() != expectLeader).limit(2)
                .forEach(node -> clusterInstance.startBroker(node.id()));

            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 3 && elrSize == 0, admin, testTopicName);

            topicPartitionInfo = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(0, topicPartitionInfo.lastKnownElr().size(), topicPartitionInfo.toString());
            assertEquals(expectLeader, topicPartitionInfo.leader().id(), topicPartitionInfo.toString());
        }
    }

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_4_0_IV1)
    public void testElrMemberShouldBeKickOutWhenUncleanShutdown() throws ExecutionException, InterruptedException {
        try (var admin = clusterInstance.admin()) {
            String testTopicName = String.format("%s-%s", "testElrMemberShouldBeKickOutWhenUncleanShutdown", "ELR-test");

            admin.updateFeatures(
                Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME,
                    new FeatureUpdate(EligibleLeaderReplicasVersion.ELRV_1.featureLevel(), FeatureUpdate.UpgradeType.UPGRADE))).all().get();
            admin.createTopics(List.of(new NewTopic(testTopicName, 1, (short) 4))).all().get();
            clusterInstance.waitTopicCreation(testTopicName, 1);

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
            Collection<AlterConfigOp> ops = new ArrayList<>();
            ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
            Map<ConfigResource, Collection<AlterConfigOp>> configOps = Map.of(configResource, ops);
            // alter configs on target cluster
            admin.incrementalAlterConfigs(configOps).all().get();

            TopicDescription testTopicDescription = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            clusterInstance.shutdownBroker(initialReplicas.get(0).id());
            clusterInstance.shutdownBroker(initialReplicas.get(1).id());
            clusterInstance.shutdownBroker(initialReplicas.get(2).id());
            clusterInstance.shutdownBroker(initialReplicas.get(3).id());

            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 0 && elrSize == 3, admin, testTopicName);
            topicPartitionInfo = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);

            int brokerToBeUncleanShutdown = topicPartitionInfo.elr().get(0).id();
            var broker = clusterInstance.brokers().values().stream().filter(b -> b.config().brokerId() == brokerToBeUncleanShutdown)
                .findFirst().get();
            List<File> dirs = new ArrayList<>();
            broker.logManager().liveLogDirs().foreach(dirs::add);
            assertEquals(1, dirs.size());
            CleanShutdownFileHandler handler = new CleanShutdownFileHandler(dirs.get(0).toString());
            assertTrue(handler.exists());
            assertDoesNotThrow(handler::delete);

            // After remove the clean shutdown file, the broker should report unclean shutdown during restart.
            clusterInstance.startBroker(brokerToBeUncleanShutdown);
            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 0 && elrSize == 2, admin, testTopicName);
            topicPartitionInfo = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertNull(topicPartitionInfo.leader());
            assertEquals(1, topicPartitionInfo.lastKnownElr().size());
        }
    }

    /*
        This test is only valid for KIP-966 part 1. When the unclean recovery is implemented, it should be removed.
     */
    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_4_0_IV1)
    public void testLastKnownLeaderShouldBeElectedIfEmptyElr() throws ExecutionException, InterruptedException {
        try (var admin = clusterInstance.admin()) {
            String testTopicName = String.format("%s-%s", "testLastKnownLeaderShouldBeElectedIfEmptyElr", "ELR-test");

            admin.updateFeatures(
                Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME,
                    new FeatureUpdate(EligibleLeaderReplicasVersion.ELRV_1.featureLevel(), FeatureUpdate.UpgradeType.UPGRADE))).all().get();
            admin.createTopics(List.of(new NewTopic(testTopicName, 1, (short) 4))).all().get();
            clusterInstance.waitTopicCreation(testTopicName, 1);

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
            Collection<AlterConfigOp> ops = new ArrayList<>();
            ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
            Map<ConfigResource, Collection<AlterConfigOp>> configOps = Map.of(configResource, ops);
            // alter configs on target cluster
            admin.incrementalAlterConfigs(configOps).all().get();


            TopicDescription testTopicDescription = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            clusterInstance.shutdownBroker(initialReplicas.get(0).id());
            clusterInstance.shutdownBroker(initialReplicas.get(1).id());
            clusterInstance.shutdownBroker(initialReplicas.get(2).id());
            clusterInstance.shutdownBroker(initialReplicas.get(3).id());

            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 0 && elrSize == 3, admin, testTopicName);
            topicPartitionInfo = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            int lastKnownLeader = topicPartitionInfo.lastKnownElr().get(0).id();

            Set<Integer> initialReplicaSet = initialReplicas.stream().map(node -> node.id()).collect(Collectors.toSet());
            clusterInstance.brokers().forEach((id, broker) -> {
                if (initialReplicaSet.contains(id)) {
                    List<File> dirs = new ArrayList<>();
                    broker.logManager().liveLogDirs().foreach(dirs::add);
                    assertEquals(1, dirs.size());
                    CleanShutdownFileHandler handler = new CleanShutdownFileHandler(dirs.get(0).toString());
                    assertDoesNotThrow(handler::delete);
                }
            });

            // After remove the clean shutdown file, the broker should report unclean shutdown during restart.
            topicPartitionInfo.replicas().forEach(replica -> {
                if (replica.id() != lastKnownLeader) clusterInstance.startBroker(replica.id());
            });
            waitForIsrAndElr((isrSize, elrSize) -> isrSize == 0 && elrSize == 1, admin, testTopicName);
            topicPartitionInfo = admin.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertNull(topicPartitionInfo.leader());
            assertEquals(1, topicPartitionInfo.lastKnownElr().size());

            // Now if the last known leader goes through unclean shutdown, it will still be elected.
            clusterInstance.startBroker(lastKnownLeader);
            waitForIsrAndElr((isrSize, elrSize) -> isrSize > 0 && elrSize == 0, admin, testTopicName);
            TestUtils.waitForCondition(
                () -> {
                    try {
                        TopicPartitionInfo partition = admin.describeTopics(List.of(testTopicName))
                            .allTopicNames().get().get(testTopicName).partitions().get(0);
                        if (partition.leader() == null) return false;
                        return partition.lastKnownElr().isEmpty() && partition.elr().isEmpty() && partition.leader().id() == lastKnownLeader;
                    } catch (Exception e) {
                        return false;
                    }
                },
                DEFAULT_MAX_WAIT_MS,
                () -> String.format("Partition metadata for %s is not correct", testTopicName)
            );
        }
    }

    void waitForIsrAndElr(BiFunction<Integer, Integer, Boolean> isIsrAndElrSizeSatisfied, Admin admin, String testTopicName) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> {
                try {
                    TopicDescription topicDescription = admin.describeTopics(List.of(testTopicName))
                        .allTopicNames().get().get(testTopicName);
                    TopicPartitionInfo partition = topicDescription.partitions().get(0);
                    return isIsrAndElrSizeSatisfied.apply(partition.isr().size(), partition.elr().size());
                } catch (Exception e) {
                    return false;
                }
            },
            DEFAULT_MAX_WAIT_MS,
            () -> String.format("Partition metadata for %s is not propagated", testTopicName)
        );
    }
}
