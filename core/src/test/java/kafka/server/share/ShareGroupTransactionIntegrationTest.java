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
package kafka.server.share;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.Feature;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = Type.KRAFT, brokers = 3, serverProperties = {
    @ClusterConfigProperty(key = "unstable.feature.versions.enable", value = "true"),
    @ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
    @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "3"),
    @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
    @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
    @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3"),
    @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "2"),
    @ClusterConfigProperty(key = "transaction.state.log.num.partitions", value = "3"),
    @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "3"),
    @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "2")
})
class ShareGroupTransactionIntegrationTest {
    private final ClusterInstance cluster;

    ShareGroupTransactionIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(features = {
        @ClusterFeature(feature = Feature.SHARE_VERSION, version = 3),
        @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)
    })
    void testCommitAndAbortAcrossThreeSourceLeaders() throws Exception {
        runCommitAndAbort(false, false);
    }

    @ClusterTest(features = {
        @ClusterFeature(feature = Feature.SHARE_VERSION, version = 3),
        @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)
    })
    void testAbortAfterSourceLeaderShutdown() throws Exception {
        runCommitAndAbort(true, true);
    }

    @ClusterTest(features = {
        @ClusterFeature(feature = Feature.SHARE_VERSION, version = 3),
        @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)
    })
    void testCommitAfterSourceLeaderShutdown() throws Exception {
        runCommitAndAbort(true, false);
    }

    private void runCommitAndAbort(boolean shutdownSourceLeader, boolean shutdownBeforeAbort) throws Exception {
        String source = "share-txn-source";
        String sink = "share-txn-sink";
        String group = "share-txn-group";
        List<Integer> brokers = cluster.brokerIds().stream().sorted().toList();
        Map<Integer, List<Integer>> assignments = new HashMap<>();
        for (int partition = 0; partition < 3; partition++) {
            assignments.put(partition, List.of(brokers.get(partition), brokers.get((partition + 1) % 3), brokers.get((partition + 2) % 3)));
        }
        try (var admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic(source, assignments), new NewTopic(sink, assignments)))
                .all().get(20, TimeUnit.SECONDS);
            assertEquals(3, admin.describeTopics(List.of(source)).allTopicNames().get().get(source).partitions()
                .stream().map(partition -> partition.leader().id()).collect(Collectors.toSet()).size());
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP, group),
                List.of(new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET))))
                .all().get(20, TimeUnit.SECONDS);
        }
        try (var producer = cluster.<byte[], byte[]>producer()) {
            for (int partition = 0; partition < 3; partition++) {
                for (int offset = 0; offset < 2; offset++) {
                    producer.send(new ProducerRecord<>(source, partition, null,
                        (partition + "-" + offset).getBytes(StandardCharsets.UTF_8))).get(20, TimeUnit.SECONDS);
                }
            }
        }
        Set<String> committed = new HashSet<>();
        String aborted = null;
        boolean redelivered = false;
        try (var consumer = cluster.<byte[], byte[]>shareConsumer(Map.of(
                 ConsumerConfig.GROUP_ID_CONFIG, group,
                 ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit",
                 ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit",
                 ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1));
             var producer = cluster.<byte[], byte[]>producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "share-txn-producer"))) {
            consumer.subscribe(List.of(source));
            producer.initTransactions();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
            while (committed.size() < 6 && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    String identity = new String(record.value(), StandardCharsets.UTF_8);
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(sink, record.partition(), null, record.value()));
                    consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    producer.sendShareAcknowledgementsToTransaction(consumer.acknowledgementsForTransaction(), consumer.shareGroupMetadata());
                    boolean shutdownNow = shutdownSourceLeader && (shutdownBeforeAbort ? aborted == null : aborted != null && committed.isEmpty());
                    int leader = brokers.get(record.partition());
                    if (shutdownNow) cluster.shutdownBroker(leader);
                    if (aborted == null) {
                        aborted = identity;
                        producer.abortTransaction();
                    } else {
                        producer.commitTransaction();
                        assertTrue(committed.add(identity), "Committed input was redelivered: " + identity);
                        redelivered |= identity.equals(aborted);
                    }
                    if (shutdownNow) restartAfterLeaderChange(source, record.partition(), leader);
                }
            }
        }
        assertEquals(6, committed.size());
        assertTrue(redelivered, "Aborted input must become available again");
        List<String> visible = readCommitted(sink);
        assertEquals(6, visible.size());
        assertEquals(committed, new HashSet<>(visible));
    }

    private void restartAfterLeaderChange(String topic, int partition, int previousLeader) throws Exception {
        try (var admin = cluster.admin()) {
            assertTrue(admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic)
                .partitions().get(partition).leader().id() != previousLeader);
        }
        cluster.startBroker(previousLeader);
        cluster.waitForReadyBrokers();
    }

    private List<String> readCommitted(String sink) {
        List<String> visible = new ArrayList<>();
        try (var consumer = cluster.<byte[], byte[]>consumer(Map.of(
            ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed", ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false))) {
            List<TopicPartition> partitions = IntStream.range(0, 3).mapToObj(partition -> new TopicPartition(sink, partition)).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    visible.add(new String(record.value(), StandardCharsets.UTF_8));
                }
                if (partitions.stream().allMatch(partition -> consumer.position(partition) >= ends.get(partition))) break;
            }
        }
        return visible;
    }
}
