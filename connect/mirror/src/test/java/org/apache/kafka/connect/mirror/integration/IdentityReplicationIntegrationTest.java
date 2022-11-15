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
package org.apache.kafka.connect.mirror.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.IdentityReplicationPolicy;
import org.apache.kafka.connect.mirror.MirrorClient;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

/**
 * Tests MM2 replication and failover logic for {@link IdentityReplicationPolicy}.
 *
 * <p>MM2 is configured with active/passive replication between two Kafka clusters with {@link IdentityReplicationPolicy}.
 * Tests validate that records sent to the primary cluster arrive at the backup cluster. Then, a consumer group is
 * migrated from the primary cluster to the backup cluster. Tests validate that consumer offsets
 * are translated and replicated from the primary cluster to the backup cluster during this failover.
 */
@Tag("integration")
public class IdentityReplicationIntegrationTest extends MirrorConnectorsIntegrationBaseTest {
    @BeforeEach
    public void startClusters() throws Exception {
        super.startClusters(new HashMap<String, String>() {{
                put("replication.policy.class", IdentityReplicationPolicy.class.getName());
                put("topics", "test-topic-.*");
                put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
                put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".enabled", "true");
            }});
    }

    @Test
    public void testReplication() throws Exception {
        produceMessages(primary, "test-topic-1");
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = new HashMap<String, Object>() {{
                put("group.id", consumerGroupName);
                put("auto.offset.reset", "latest");
            }};
        // warm up consumers before starting the connectors so we don't need to wait for discovery
        warmUpConsumer(consumerProps);

        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        waitUntilMirrorMakerIsRunning(primary, Collections.singletonList(MirrorHeartbeatConnector.class), mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

        MirrorClient primaryClient = new MirrorClient(mm2Config.clientConfig(PRIMARY_CLUSTER_ALIAS));
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, "test-topic-1");
        waitForTopicCreated(backup, "test-topic-1");
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, getTopicConfig(backup.kafka(), "test-topic-1", TopicConfig.CLEANUP_POLICY_CONFIG),
                "topic config was not synced");
        createAndTestNewTopicWithConfigFilter();

        assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
                "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
                "Records were not replicated to backup cluster.");

        assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
                "Heartbeats were not emitted to primary cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
                "Heartbeats were not emitted to backup cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "primary.heartbeats").count() > 0,
                "Heartbeats were not replicated downstream to backup cluster.");
        assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
                "Heartbeats were not replicated downstream to primary cluster.");

        assertTrue(backupClient.upstreamClusters().contains(PRIMARY_CLUSTER_ALIAS), "Did not find upstream primary cluster.");
        assertEquals(1, backupClient.replicationHops(PRIMARY_CLUSTER_ALIAS), "Did not calculate replication hops correctly.");
        assertTrue(backup.kafka().consume(1, CHECKPOINT_DURATION_MS, "primary.checkpoints.internal").count() > 0,
                "Checkpoints were not emitted downstream to backup cluster.");

        Map<TopicPartition, OffsetAndMetadata> backupOffsets = backupClient.remoteConsumerOffsets(consumerGroupName, PRIMARY_CLUSTER_ALIAS,
                Duration.ofMillis(CHECKPOINT_DURATION_MS));

        assertTrue(backupOffsets.containsKey(
                new TopicPartition("test-topic-1", 0)), "Offsets not translated downstream to backup cluster. Found: " + backupOffsets);

        // Failover consumer group to backup cluster.
        try (Consumer<byte[], byte[]> primaryConsumer = backup.kafka().createConsumer(Collections.singletonMap("group.id", consumerGroupName))) {
            primaryConsumer.assign(backupOffsets.keySet());
            backupOffsets.forEach(primaryConsumer::seek);
            primaryConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            primaryConsumer.commitAsync();

            assertTrue(primaryConsumer.position(new TopicPartition("test-topic-1", 0)) > 0, "Consumer failedover to zero offset.");
            assertTrue(primaryConsumer.position(
                    new TopicPartition("test-topic-1", 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedover beyond expected offset.");
        }

        primaryClient.close();
        backupClient.close();

        // create more matching topics
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(backup, "test-topic-2");

        // only produce messages to the first partition
        produceMessages(primary, "test-topic-2", 1);

        // expect total consumed messages equals to NUM_RECORDS_PER_PARTITION
        assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-2").count(),
                "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, "test-topic-2").count(),
                "New topic was not replicated to backup cluster.");
    }

    @Test
    public void testReplicationWithEmptyPartition() throws Exception {
        String consumerGroupName = "consumer-group-testReplicationWithEmptyPartition";
        Map<String, Object> consumerProps  = Collections.singletonMap("group.id", consumerGroupName);

        // create topic
        String topic = "test-topic-with-empty-partition";
        primary.kafka().createTopic(topic, NUM_PARTITIONS);

        // produce to all test-topic-empty's partitions, except the last partition
        produceMessages(primary, topic, NUM_PARTITIONS - 1);

        // consume before starting the connectors so we don't need to wait for discovery
        int expectedRecords = NUM_RECORDS_PER_PARTITION * (NUM_PARTITIONS - 1);
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, topic)) {
            waitForConsumingAllRecords(primaryConsumer, expectedRecords);
        }

        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // sleep few seconds to have MM2 finish replication so that "end" consumer will consume some record
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        // note that with IdentityReplicationPolicy, topics on the backup are NOT renamed to PRIMARY_CLUSTER_ALIAS + "." + topic
        String backupTopic = topic;

        // consume all records from backup cluster
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(consumerProps,
                backupTopic)) {
            waitForConsumingAllRecords(backupConsumer, expectedRecords);
        }

        try (Admin backupClient = backup.kafka().createAdminClient()) {
            // retrieve the consumer group offset from backup cluster
            Map<TopicPartition, OffsetAndMetadata> remoteOffsets =
                    backupClient.listConsumerGroupOffsets(consumerGroupName).partitionsToOffsetAndMetadata().get();

            // pinpoint the offset of the last partition which does not receive records
            OffsetAndMetadata offset = remoteOffsets.get(new TopicPartition(backupTopic, NUM_PARTITIONS - 1));
            // offset of the last partition should exist, but its value should be 0
            assertNotNull(offset, "Offset of last partition was not replicated");
            assertEquals(0, offset.offset(), "Offset of last partition is not zero");
        }
    }

    @Test
    public void testOneWayReplicationWithAutoOffsetSync() throws InterruptedException {
        produceMessages(primary, "test-topic-1");
        String consumerGroupName = "consumer-group-testOneWayReplicationWithAutoOffsetSync";
        Map<String, Object> consumerProps  = new HashMap<String, Object>() {{
                put("group.id", consumerGroupName);
                put("auto.offset.reset", "earliest");
            }};
        // create consumers before starting the connectors so we don't need to wait for discovery
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps,
                "test-topic-1")) {
            // we need to wait for consuming all the records for MM2 replicating the expected offsets
            waitForConsumingAllRecords(primaryConsumer, NUM_RECORDS_PRODUCED);
        }

        // enable automated consumer group offset sync
        mm2Props.put("sync.group.offsets.enabled", "true");
        mm2Props.put("sync.group.offsets.interval.seconds", "1");
        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");

        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // make sure the topic is created in the backup cluster with same name.
        topicShouldNotBeCreated(primary, "backup.test-topic-1");
        waitForTopicCreated(backup, "test-topic-1");
        // create a consumer at backup cluster with same consumer group Id to consume 1 topic
        Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(
                consumerProps, "test-topic-1");

        waitForConsumerGroupOffsetSync(backup, backupConsumer, Collections.singletonList("test-topic-1"),
                consumerGroupName, NUM_RECORDS_PRODUCED);

        ConsumerRecords<byte[], byte[]> records = backupConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);

        // the size of consumer record should be zero, because the offsets of the same consumer group
        // have been automatically synchronized from primary to backup by the background job, so no
        // more records to consume from the replicated topic by the same consumer group at backup cluster
        assertEquals(0, records.count(), "consumer record size is not zero");

        // now create a new topic in primary cluster
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        // make sure the topic is created in backup cluster
        waitForTopicCreated(backup, "test-topic-2");

        // produce some records to the new topic in primary cluster
        produceMessages(primary, "test-topic-2");

        // create a consumer at primary cluster to consume the new topic
        try (Consumer<byte[], byte[]> consumer1 = primary.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "group.id", "consumer-group-1"), "test-topic-2")) {
            // we need to wait for consuming all the records for MM2 replicating the expected offsets
            waitForConsumingAllRecords(consumer1, NUM_RECORDS_PRODUCED);
        }

        // create a consumer at backup cluster with same consumer group Id to consume old and new topic
        backupConsumer = backup.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "group.id", consumerGroupName), "test-topic-1", "test-topic-2");

        waitForConsumerGroupOffsetSync(backup, backupConsumer, Arrays.asList("test-topic-1", "test-topic-2"),
                consumerGroupName, NUM_RECORDS_PRODUCED);

        records = backupConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
        // similar reasoning as above, no more records to consume by the same consumer group at backup cluster
        assertEquals(0, records.count(), "consumer record size is not zero");
        backupConsumer.close();
    }

    /*
     * Returns expected topic name on target cluster.
     */
    @Override
    String remoteTopicName(String topic, String clusterAlias) {
        return topic;
    }
}
