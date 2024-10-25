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
package kafka.admin;

import kafka.log.UnifiedLog;
import kafka.server.KafkaBroker;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    })
public class DeleteTopicTestJava {
    private static final String DEFAULT_TOPIC = "topic";
    private final Map<Integer, List<Integer>> expectedReplicaAssignment = Map.of(0, List.of(0, 1, 2));

    @ClusterTest
    public void testDeleteTopicWithAllAliveReplicas(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            kafka.utils.TestUtils.verifyTopicDeletion(null, DEFAULT_TOPIC, 1, CollectionConverters.asScala(cluster.brokers().values()).toSeq());
            // TODO fix
//            verifyTopicDeletion(DEFAULT_TOPIC, 1, cluster.brokers().values());
        }
    }

    @ClusterTest
    public void testResumeDeleteTopicWithRecoveredFollower(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            int leaderId = waitUtilLeaderIsKnown(cluster.brokers(), topicPartition).get();
            KafkaBroker follower = findFollower(cluster.brokers().values(), leaderId);

            follower.shutdown();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            TestUtils.waitForCondition(() -> cluster.brokers().values()
                    .stream()
                    .filter(broker -> broker.config().brokerId() != follower.config().brokerId())
                    .allMatch(b -> b.logManager().getLog(topicPartition, false).isEmpty()),
                "Replicas 0,1 have not deleted log.");

            follower.startup();
            kafka.utils.TestUtils.verifyTopicDeletion(null, DEFAULT_TOPIC, 1, CollectionConverters.asScala(cluster.brokers().values()).toSeq());
            // TODO fix
//            verifyTopicDeletion(DEFAULT_TOPIC, 1, cluster.brokers().values());
        }
    }

    @ClusterTest(brokers = 4)
    public void testPartitionReassignmentDuringDeleteTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            Map<Integer, KafkaBroker> servers = findPartitionHostingBrokers(cluster.brokers());
            int leaderId = waitUtilLeaderIsKnown(cluster.brokers(), topicPartition).get();
            KafkaBroker follower = findFollower(servers.values(), leaderId);
            follower.shutdown();

            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin otherAdmin = Admin.create(properties)) {
                waitUtilTopicGone(otherAdmin);
                assertThrows(ExecutionException.class, () -> otherAdmin.alterPartitionReassignments(
                    Map.of(topicPartition, Optional.of(new NewPartitionReassignment(List.of(1, 2, 3))))
                ).all().get());
            }

            follower.startup();
            kafka.utils.TestUtils.verifyTopicDeletion(null, DEFAULT_TOPIC, 1, CollectionConverters.asScala(cluster.brokers().values()).toSeq());
            // TODO fix
        }
    }

    @ClusterTest(brokers = 4)
    public void testIncreasePartitionCountDuringDeleteTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            Map<Integer, KafkaBroker> partitionHostingBrokers = findPartitionHostingBrokers(cluster.brokers());
            TestUtils.waitForCondition(() -> partitionHostingBrokers.values().stream().allMatch(broker ->
                broker.logManager().getLog(topicPartition, false).isDefined()),
                "Replicas for topic test not created.");
            int leaderId = waitUtilLeaderIsKnown(partitionHostingBrokers, topicPartition).get();
            KafkaBroker follower = findFollower(partitionHostingBrokers.values(), leaderId);
            follower.shutdown();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin otherAdmin = Admin.create(properties)) {
                otherAdmin.createPartitions(Map.of(DEFAULT_TOPIC, NewPartitions.increaseTo(2))).all().get();
            } catch (ExecutionException exception) {

            }

            follower.startup();
            kafka.utils.TestUtils.verifyTopicDeletion(null, DEFAULT_TOPIC, 1, CollectionConverters.asScala(partitionHostingBrokers.values()).toSeq());
            // TODO fix
        }
    }

    @ClusterTest
    public void testDeleteTopicDuringAddPartition(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            int leaderId = waitUtilLeaderIsKnown(cluster.brokers(), new TopicPartition(DEFAULT_TOPIC, 0)).get();
            TopicPartition newTopicPartition = new TopicPartition(DEFAULT_TOPIC, 1);
            KafkaBroker follower = findFollower(cluster.brokers().values(), leaderId);
            follower.shutdown();
            TestUtils.waitForCondition(() -> follower.brokerState().equals(BrokerState.SHUTTING_DOWN),
                "Follower " + follower.config().brokerId() + " was not shutdown");
            increasePartitions(admin, DEFAULT_TOPIC, 3, cluster.brokers().values().stream().filter(broker ->
                broker.config().brokerId() != follower.config().brokerId()).collect(Collectors.toList()));
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            follower.startup();
            kafka.utils.TestUtils.verifyTopicDeletion(null, DEFAULT_TOPIC, 1, CollectionConverters.asScala(cluster.brokers().values()).toSeq());
            // TODO fix
//            verifyTopicDeletion(DEFAULT_TOPIC, 1, cluster.brokers().values());
            TestUtils.waitForCondition(() -> cluster.brokers().values()
                    .stream().allMatch(broker -> broker.logManager().getLog(newTopicPartition, false).isEmpty()),
                "Replica logs not for new partition [" + DEFAULT_TOPIC + ",1] not deleted after delete topic is complete.");
        }
    }

    @ClusterTest
    public void testAddPartitionDuringDeleteTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition newTopicPartition = new TopicPartition(DEFAULT_TOPIC, 1);
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            increasePartitions(admin, DEFAULT_TOPIC, 3, Collections.emptyList());
            // TODO rewrite java
            kafka.utils.TestUtils.verifyTopicDeletion(null, DEFAULT_TOPIC, 1, CollectionConverters.asScala(cluster.brokers().values()).toSeq());
            TestUtils.waitForCondition(() -> cluster.brokers().values().stream().allMatch(broker ->
                    broker.logManager().getLog(newTopicPartition, false).isEmpty()),
                "Replica logs not deleted after delete topic is complete");
        }
    }

    @ClusterTest
    public void testRecreateTopicAfterDeletion(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            // TODO rewrite java
            kafka.utils.TestUtils.verifyTopicDeletion(null, DEFAULT_TOPIC, 1, CollectionConverters.asScala(cluster.brokers().values()).toSeq());
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TestUtils.waitForCondition(() -> cluster.brokers().values().stream().allMatch(broker ->
                    broker.logManager().getLog(topicPartition, false).isDefined()),
                "Replicas for topic test not created.");
        }
    }

    @ClusterTest
    public void testDeleteNonExistingTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            String topic = "test2";
            TestUtils.waitForCondition(() -> {
                try {
                    admin.deleteTopics(List.of(topic)).all().get();
                    return false;
                } catch (Exception exception) {
                    return exception.getCause().getClass().equals(UnknownTopicOrPartitionException.class);
                }
            }, "Topic test2 should not exist.");

            // TODO rewrite java
            kafka.utils.TestUtils.verifyTopicDeletion(null, topic, 1,
                CollectionConverters.asScala(cluster.brokers().values()).toSeq());

            TestUtils.waitForCondition(() -> cluster.brokers().values().stream().allMatch(broker ->
                    broker.logManager().getLog(topicPartition, false).isDefined()),
                "Replicas for topic test not created.");
            kafka.utils.TestUtils.waitUntilLeaderIsElectedOrChangedWithAdmin(admin, DEFAULT_TOPIC, 0, 1000,
                OptionConverters.toScala(Optional.empty()), OptionConverters.toScala(Optional.empty()));
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = "log.cleaner.enable", value = "true"),
        @ClusterConfigProperty(key = "log.cleanup.policy", value = "compact"),
        @ClusterConfigProperty(key = "log.segment.bytes", value = "100"),
        @ClusterConfigProperty(key = "log.cleaner.dedupe.buffer.size", value = "1048577")
    })
    public void testDeleteTopicWithCleaner(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            KafkaBroker server = cluster.brokers().values().stream().findFirst().get();
            TestUtils.waitForCondition(() -> server.logManager().getLog(topicPartition, false).isDefined(), "");
            UnifiedLog log = server.logManager().getLog(topicPartition, false).get();
            writeDups(100, 3, log);
            server.logManager().cleaner().awaitCleaned(topicPartition, 0, 60000);
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            kafka.utils.TestUtils.verifyTopicDeletion(null, DEFAULT_TOPIC, 1,
                CollectionConverters.asScala(cluster.brokers().values()).toSeq());
        }
    }

    @ClusterTest
    public void testDeleteTopicAlreadyMarkedAsDeleted(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            TestUtils.waitForCondition(() -> {
                try {
                    admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
                    return false;
                } catch (Exception exception) {
                    return exception.getCause().getClass().equals(UnknownTopicOrPartitionException.class);
                }
            }, "Topic " + DEFAULT_TOPIC + " should be marked for deletion or already deleted.");

            kafka.utils.TestUtils.verifyTopicDeletion(null, DEFAULT_TOPIC, 1,
                CollectionConverters.asScala(cluster.brokers().values()).toSeq());
        }
    }

    @ClusterTest(controllers = 1,
        serverProperties = {@ClusterConfigProperty(key = ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, value = "false")})
    public void testDisableDeleteTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            TestUtils.waitForCondition(() -> {
                try {
                    admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
                    return false;
                } catch (Exception exception) {
                    return exception.getCause().getClass().equals(TopicDeletionDisabledException.class);
                }
            }, "TopicDeletionDisabledException should be returned when deleting " + DEFAULT_TOPIC);

            TestUtils.waitForCondition(() -> cluster.brokers().values().stream()
                .allMatch(broker -> broker.logManager().getLog(topicPartition, false).isDefined()), "");
            assertDoesNotThrow(() -> admin.describeTopics(List.of(DEFAULT_TOPIC)).allTopicNames().get());
            assertTrue(waitUtilLeaderIsKnown(cluster.brokers(), topicPartition).isPresent());
        }
    }

    private Optional<Integer> waitUtilLeaderIsKnown(Map<Integer, KafkaBroker> idToBroker,
                                                    TopicPartition topicPartition) throws InterruptedException {
        TestUtils.waitForCondition(() -> isLeaderKnown(idToBroker, topicPartition).get().isPresent(),
            "Partition " + topicPartition + " not made yet" + " after 15 seconds");
        return isLeaderKnown(idToBroker, topicPartition).get();
    }

    private Supplier<Optional<Integer>> isLeaderKnown(Map<Integer, KafkaBroker> idToBroker, TopicPartition topicPartition) {
        return () -> idToBroker.values()
            .stream()
            .filter(broker -> OptionConverters.toJava(broker.replicaManager()
                    .onlinePartition(topicPartition))
                .stream().anyMatch(tp -> tp.leaderIdIfLocal().isDefined()))
            .map(broker -> broker.config().brokerId())
            .findFirst();
    }

    private KafkaBroker findFollower(Collection<KafkaBroker> idToBroker, int leaderId) {
        return idToBroker.stream()
            .filter(broker -> broker.config().brokerId() != leaderId)
            .findFirst()
            .orElseGet(() -> fail("Can't find any follower"));
    }

    private void waitUtilTopicGone(Admin admin) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                admin.describeTopics(List.of(DEFAULT_TOPIC)).allTopicNames().get();
                return false;
            } catch (ExecutionException exception) {
                return exception.getCause().getClass().equals(UnknownTopicOrPartitionException.class);
            } catch (InterruptedException e) {
                return false;
            }
        }, "Topic" + DEFAULT_TOPIC + " should be deleted");
    }

    private Map<Integer, KafkaBroker> findPartitionHostingBrokers(Map<Integer, KafkaBroker> brokers) {
        return brokers.entrySet()
                .stream()
                .filter(broker -> expectedReplicaAssignment.get(0).contains(broker.getValue().config().brokerId()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public <B extends KafkaBroker> void increasePartitions(Admin admin,
                                                           String topic,
                                                           int totalPartitionCount,
                                                           List<B> brokersToValidate) throws Exception {
        Map<String, NewPartitions> newPartitionSet = Map.of(topic, NewPartitions.increaseTo(totalPartitionCount));
        admin.createPartitions(newPartitionSet);

        if (!brokersToValidate.isEmpty()) {
            // wait until we've propagated all partitions metadata to all brokers
            Map<TopicPartition, UpdateMetadataPartitionState> allPartitionsMetadata = waitForAllPartitionsMetadata(brokersToValidate, topic, totalPartitionCount);

            IntStream.range(0, totalPartitionCount - 1).forEach(i -> {
                Optional<UpdateMetadataPartitionState> partitionMetadata = Optional.ofNullable(allPartitionsMetadata.get(new TopicPartition(topic, i)));
                partitionMetadata.ifPresent(metadata -> assertEquals(totalPartitionCount, metadata.replicas().size()));
            });
        }
    }

    public <B extends KafkaBroker> Map<TopicPartition, UpdateMetadataPartitionState> waitForAllPartitionsMetadata(
        List<B> brokers, String topic, int expectedNumPartitions) throws Exception {

        // Wait until all brokers have the expected partition metadata
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker -> {
            if (expectedNumPartitions == 0) {
                return broker.metadataCache().numPartitions(topic).isEmpty();
            } else {
                return OptionConverters.toJava(broker.metadataCache().numPartitions(topic))
                    .equals(Optional.of(expectedNumPartitions));
            }
        }), 60000, "Topic [" + topic + "] metadata not propagated after 60000 ms");

        // Since the metadata is propagated, we should get the same metadata from each server
        Map<TopicPartition, UpdateMetadataPartitionState> partitionMetadataMap = new HashMap<>();
        IntStream.range(0, expectedNumPartitions).forEach(i -> {
            TopicPartition topicPartition = new TopicPartition(topic, i);
            UpdateMetadataPartitionState partitionState = OptionConverters.toJava(brokers.get(0).metadataCache()
                .getPartitionInfo(topic, i)).orElseThrow(() ->
                new IllegalStateException("Cannot get topic: " + topic + ", partition: " + i + " in server metadata cache"));
            partitionMetadataMap.put(topicPartition, partitionState);
        });

        return partitionMetadataMap;
    }

    public <B extends KafkaBroker> void verifyTopicDeletion(String topic,
                                                            int numPartitions,
                                                            Collection<B> brokers) throws Exception {
        List<TopicPartition> topicPartitions = IntStream.range(0, numPartitions)
            .mapToObj(partition -> new TopicPartition(topic, partition))
            .collect(Collectors.toList());

        // Ensure that the topic-partition has been deleted from all brokers' replica managers
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                topicPartitions.stream().allMatch(tp -> broker.replicaManager().onlinePartition(tp).isEmpty())),
            "Replica manager's should have deleted all of this topic's partitions");

        // Ensure that logs from all replicas are deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                topicPartitions.stream().allMatch(tp -> broker.logManager().getLog(tp, false).isEmpty())),
            "Replica logs not deleted after delete topic is complete");

        // Ensure that the topic is removed from all cleaner offsets
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                topicPartitions.stream().allMatch(tp -> {
                    List<File> liveLogDirs = CollectionConverters.asJava(broker.logManager().liveLogDirs());
                    return liveLogDirs.stream().allMatch(logDir -> {
                        OffsetCheckpointFile checkpointFile;
                        try {
                            checkpointFile = new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint"), null);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return !checkpointFile.read().containsKey(tp);
                    });
                })),
            "Cleaner offset for deleted partition should have been removed");

        // Ensure that the topic directories are soft-deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                CollectionConverters.asJava(broker.config().logDirs()).stream().allMatch(logDir ->
                    topicPartitions.stream().noneMatch(tp ->
                        new File(logDir, tp.topic() + "-" + tp.partition()).exists()))),
            "Failed to soft-delete the data to a delete directory");

        // Ensure that the topic directories are hard-deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
            CollectionConverters.asJava(broker.config().logDirs()).stream().allMatch(logDir ->
                topicPartitions.stream().allMatch(tp -> {
                    File dir = new File(logDir);
                    String[] files = dir.list();
                    if (files == null) {
                        return true;
                    }
                    return Arrays.stream(files).noneMatch(partitionDirectoryName ->
                        partitionDirectoryName.startsWith(tp.topic() + "-" + tp.partition()) &&
                            partitionDirectoryName.endsWith(UnifiedLog.DeleteDirSuffix()));
                })
            )
        ), "Failed to hard-delete the delete directory");
    }

    private List<int[]> writeDups(int numKeys, int numDups, UnifiedLog log) {
        int counter = 0;
        List<int[]> result = new ArrayList<>();

        for (int i = 0; i < numDups; i++) {
            for (int key = 0; key < numKeys; key++) {
                int count = counter;
                log.appendAsLeader(
                    kafka.utils.TestUtils.singletonRecords(
                        String.valueOf(counter).getBytes(),
                        String.valueOf(key).getBytes(),
                        Compression.NONE,
                        RecordBatch.NO_TIMESTAMP,
                        RecordBatch.CURRENT_MAGIC_VALUE
                    ),
                    0,
                    AppendOrigin.CLIENT,
                    MetadataVersion.LATEST_PRODUCTION,
                    RequestLocal.noCaching(),
                    VerificationGuard.SENTINEL
                );
                counter++;
                result.add(new int[] {key, count});
            }
        }
        return result;
    }
}
