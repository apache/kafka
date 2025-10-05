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
package org.apache.kafka.clients.producer;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerSendWhileDeletionTest.BROKER_COUNT;
import static org.apache.kafka.server.config.ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.NUM_PARTITIONS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = NUM_PARTITIONS_CONFIG, value = "2"),
        @ClusterConfigProperty(key = DEFAULT_REPLICATION_FACTOR_CONFIG, value = "2"),
        @ClusterConfigProperty(key = AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"),
        @ClusterConfigProperty(key = ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, value = "100")
    }
)
public class ProducerSendWhileDeletionTest {

    public static final int BROKER_COUNT = 2;
    private static final int DEFAULT_LINGER_MS = 5;
    private final int numRecords = 10;
    private final String topic = "topic";
    private final ClusterInstance cluster;

    public ProducerSendWhileDeletionTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    /**
     * Tests that Producer gets self-recovered when a topic is deleted mid-way of produce.
     * <p>
     * Producer will attempt to send messages to the partition specified in each record, and should
     * succeed as long as the partition is included in the metadata.
     */
    @ClusterTest
    public void testSendWithTopicDeletionMidWay() throws Exception {
        try (var admin = cluster.admin();
             var producer = createProducer()
        ) {
            // Create topic with leader as 0 for the 2 partitions.
            var topicAssignment = Map.of(
                0, List.of(0, 1),
                1, List.of(0, 1)
            );
            admin.createTopics(List.of(new NewTopic(topic, topicAssignment)));

            // Change leader to 1 for both the partitions to increase leader epoch from 0 -> 1
            var reassignment = Map.of(
                new TopicPartition(topic, 0), Optional.of(new NewPartitionReassignment(List.of(1, 0))),
                new TopicPartition(topic, 1), Optional.of(new NewPartitionReassignment(List.of(1, 0)))
            );
            admin.alterPartitionReassignments(reassignment).all().get();

            for (var i = 1; i <= numRecords; i++) {
                var resp = producer.send(
                    new ProducerRecord<>(topic, null, ("value" + i).getBytes())
                ).get();
                assertEquals(topic, resp.topic());
            }

            // Start topic deletion
            admin.deleteTopics(List.of(topic)).all().get();
            // Verify that the topic is deleted when no metadata request comes in
            verifyTopicDeletion();

            // Producer should be able to send messages even after topic gets deleted and auto-created
            var finalResp = producer.send(new ProducerRecord<>(topic, null, "value".getBytes())).get();
            assertEquals(topic, finalResp.topic());
        }
    }

    /**
     * Tests that Producer produce to new topic id after recreation.
     * <p>
     * Producer will attempt to send messages to the partition specified in each record, and should
     * succeed as long as the metadata has been updated with new topic id.
     */
    @ClusterTest
    public void testSendWithRecreatedTopic() throws Exception {
        try (var admin = cluster.admin();
             var producer = createProducer()
        ) {
            cluster.createTopic(topic, 1, (short) 1);
            var topicId = topicMetadata().topicId();

            for (int i = 1; i <= numRecords; i++) {
                var resp = producer.send(new ProducerRecord<>(topic, null, ("value" + i).getBytes())).get();
                assertEquals(topic, resp.topic());
            }

            // Start topic deletion
            admin.deleteTopics(List.of(topic)).all().get();

            // Verify that the topic is deleted when no metadata request comes in
            verifyTopicDeletion();
            cluster.createTopic(topic, 1, (short) 1);
            assertNotEquals(topicId, topicMetadata().topicId());

            // Producer should be able to send messages even after topic gets recreated
            var recordMetadata = producer.send(new ProducerRecord<>(topic, null, "value".getBytes(StandardCharsets.UTF_8))).get();
            assertEquals(topic, recordMetadata.topic());
            assertEquals(0, recordMetadata.offset());
        }
    }

    @ClusterTest
    public void testSendWhileTopicGetRecreated() {
        int maxNumTopicRecreationAttempts = 5;
        var recreateTopicFuture = CompletableFuture.supplyAsync(() -> {
            var topicIds = new HashSet<Uuid>();
            while (topicIds.size() < maxNumTopicRecreationAttempts) {
                try (var admin = cluster.admin()) {
                    if (admin.listTopics().names().get().contains(topic)) {
                        admin.deleteTopics(List.of(topic)).all().get();
                    }
                    topicIds.add(admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).topicId(topic).get());
                } catch (Exception e) {
                    // ignore
                }
            }
            return topicIds;
        });

        AtomicInteger numAcks = new AtomicInteger(0);
        var producerFuture = CompletableFuture.runAsync(() -> {
            try (var producer = createProducer()) {
                for (int i = 1; i <= numRecords; i++) {
                    producer.send(new ProducerRecord<>(topic, null, ("value" + i).getBytes()),
                            (metadata, exception) -> numAcks.incrementAndGet()
                    );
                }
                producer.flush();
            }
        });
        var topicIds = recreateTopicFuture.join();
        producerFuture.join();
        assertEquals(maxNumTopicRecreationAttempts, topicIds.size());
        assertEquals(numRecords, numAcks.intValue());
    }

    @ClusterTest
    public void testSendWithTopicReassignmentIsMidWay() throws Exception {
        var partition0 = new TopicPartition(topic, 0);

        try (var admin = cluster.admin();
             var producer = createProducer()
        ) {
            // Create topic with leader as 0 for the 1 partition.
            admin.createTopics(List.of(new NewTopic(topic, Map.of(0, List.of(0)))));
            assertLeader(partition0, 0);

            var topicDetails = topicMetadata();
            for (var i = 1; i <= numRecords; i++) {
                var resp = producer.send(new ProducerRecord<>(topic, null, ("value" + i).getBytes())).get();
                assertEquals(topic, resp.topic());
            }

            var reassignment = Map.of(
                partition0, Optional.of(new NewPartitionReassignment(List.of(1)))
            );
            // Change replica assignment from 0 to 1. Leadership moves to 1.
            admin.alterPartitionReassignments(reassignment).all().get();

            assertLeader(partition0, 1);
            assertEquals(topicDetails.topicId(), topicMetadata().topicId());

            // Producer should be able to send messages even after topic gets reassigned
            var recordMetadata = producer.send(new ProducerRecord<>(topic, null, "value".getBytes())).get();
            assertEquals(topic, recordMetadata.topic());
        }
    }

    private Producer<String, byte[]> createProducer() {
        return cluster.producer(Map.of(
            MAX_BLOCK_MS_CONFIG, 5000L,
            REQUEST_TIMEOUT_MS_CONFIG, 10000,
            DELIVERY_TIMEOUT_MS_CONFIG, 10000 + DEFAULT_LINGER_MS,
            KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ));
    }

    private void verifyTopicDeletion() throws InterruptedException {
        var topicPartitions = List.of(
            new TopicPartition(topic, 0),
            new TopicPartition(topic, 1)
        );

        // ensure that the topic-partition has been deleted from all brokers' replica managers
        TestUtils.waitForCondition(() -> 
            cluster.brokers().values().stream()
                .allMatch(broker -> topicPartitions.stream()
                        .allMatch(tp -> broker.replicaManager().onlinePartition(tp).isEmpty())
            ), "Replica manager's should have deleted all of this topic's partitions");

        // ensure that logs from all replicas are deleted
        TestUtils.waitForCondition(() -> cluster.brokers().values().stream()
            .allMatch(broker -> topicPartitions.stream()
                    .allMatch(tp -> broker.logManager().getLog(tp, false).isEmpty())
            ), "Replica logs not deleted after delete topic is complete");

        // ensure that topic is removed from all cleaner offsets
        TestUtils.waitForCondition(() -> cluster.brokers().values().stream()
            .allMatch(broker -> topicPartitions.stream()
                    .allMatch(tp -> partitionNotInCheckpoint(broker, tp))
            ), "Cleaner offset for deleted partition should have been removed");

        TestUtils.waitForCondition(() -> cluster.brokers().values().stream()
            .allMatch(broker -> broker.config().logDirs().stream()
                    .allMatch(logDir -> topicPartitions.stream().noneMatch(tp ->
                            new File(logDir, tp.topic() + "-" + tp.partition()).exists())
                    )
            ), "Failed to soft-delete the data to a delete directory");

        TestUtils.waitForCondition(() -> cluster.brokers().values().stream()
            .allMatch(broker -> broker.config().logDirs().stream()
                    .allMatch(logDir -> deletionDirectoriesAbsent(logDir, topicPartitions))
            ), "Failed to hard-delete the delete directory");
    }

    private boolean partitionNotInCheckpoint(KafkaBroker broker, TopicPartition tp) {
        List<File> liveLogDirs = new ArrayList<>();
        broker.logManager().liveLogDirs().foreach(liveLogDirs::add);
        var checkpoints = liveLogDirs.stream().map(logDir -> {
            try {
                return new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint"), null).read();
            } catch (Exception e) {
                return new HashMap<TopicPartition, Long>();
            }
        }).toList();
        return checkpoints.stream().noneMatch(checkpointsPerLogDir ->
                checkpointsPerLogDir.containsKey(tp));
    }

    private boolean deletionDirectoriesAbsent(String logDir, List<TopicPartition> topicPartitions) {
        var directoryNames = new File(logDir).list();
        if (directoryNames == null) {
            return true;
        }
        return topicPartitions.stream().allMatch(tp ->
                Arrays.stream(directoryNames).noneMatch(directoryName ->
                        directoryName.startsWith(tp.topic() + "-" + tp.partition()) &&
                                directoryName.endsWith(UnifiedLog.DELETE_DIR_SUFFIX)));
    }

    private TopicDescription topicMetadata() throws Exception {
        try (var admin = cluster.admin()) {
            return admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get()
                    .get(topic);
            
        }
    }

    private void assertLeader(TopicPartition topicPartition, Integer expectedLeaderOpt) throws InterruptedException {
        try (var admin = cluster.admin()) {
            TestUtils.waitForCondition(() -> {
                try {
                    Optional<Integer> currentLeader = getCurrentLeader(admin, topicPartition);
                    return currentLeader.equals(Optional.of(expectedLeaderOpt));
                } catch (Exception e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        return false;
                    }
                    throw new RuntimeException(e);
                }
            }, "Waiting for leader to become " + expectedLeaderOpt);
        }
    }

    private Optional<Integer> getCurrentLeader(Admin admin, TopicPartition topicPartition) throws Exception {
        return admin.describeTopics(List.of(topicPartition.topic()))
                .allTopicNames()
                .get()
                .get(topicPartition.topic())
                .partitions()
                .stream()
                .filter(p -> p.partition() == topicPartition.partition())
                .findFirst()
                .map(TopicPartitionInfo::leader)
                .map(Node::id);
    }
}
