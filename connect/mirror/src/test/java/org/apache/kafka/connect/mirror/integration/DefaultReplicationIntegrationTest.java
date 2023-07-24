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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.mirror.MirrorClient;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests MM2 replication and failover logic for {@link org.apache.kafka.connect.mirror.DefaultReplicationPolicy}
 * with custom configuration.
 */
@Tag("integration")
public class DefaultReplicationIntegrationTest extends MirrorConnectorsIntegrationBaseTest {

    @Test
    public void testReplicationWithCustomSeparatorAndInternalTopicSeparatorEnabled() throws Exception {
        String separator = "_";
        produceMessages(primaryProducer, "test-topic-1");
        String backupTopic1 = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS, separator);
        if (replicateBackupToPrimary) {
            produceMessages(backupProducer, "test-topic-1");
        }
        String reverseTopic1 = remoteTopicName("test-topic-1", BACKUP_CLUSTER_ALIAS, separator);
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        // warm up consumers before starting the connectors, so we don't need to wait for discovery
        warmUpConsumer(consumerProps);

        mm2Props.put("replication.policy.separator", separator);
        mm2Props.put("replication.policy.internal.topic.separator.enabled", "true");
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        List<Class<? extends Connector>> primaryConnectors = replicateBackupToPrimary ? CONNECTOR_LIST : Collections.singletonList(MirrorHeartbeatConnector.class);
        waitUntilMirrorMakerIsRunning(primary, primaryConnectors, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

        MirrorClient primaryClient = new MirrorClient(mm2Config.clientConfig(PRIMARY_CLUSTER_ALIAS));
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, reverseTopic1);
        waitForTopicCreated(backup, backupTopic1);
        waitForTopicCreated(primary, "mm2-offset-syncs_backup_internal");
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, getTopicConfig(backup.kafka(), backupTopic1, TopicConfig.CLEANUP_POLICY_CONFIG),
            "topic config was not synced");
        createAndTestNewTopicWithConfigFilter(separator);

        assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
            "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, backupTopic1).count(),
            "Records were not replicated to backup cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
            "Records were not produced to backup cluster.");
        if (replicateBackupToPrimary) {
            assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, reverseTopic1).count(),
                "Records were not replicated to primary cluster.");
            assertEquals(NUM_RECORDS_PRODUCED * 2, primary.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, reverseTopic1, "test-topic-1").count(),
                "Primary cluster doesn't have all records from both clusters.");
            assertEquals(NUM_RECORDS_PRODUCED * 2, backup.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, backupTopic1, "test-topic-1").count(),
                "Backup cluster doesn't have all records from both clusters.");
        }

        assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
            "Heartbeats were not emitted to primary cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
            "Heartbeats were not emitted to backup cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "primary_heartbeats").count() > 0,
            "Heartbeats were not replicated downstream to backup cluster.");
        if (replicateBackupToPrimary) {
            assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "backup_heartbeats").count() > 0,
                "Heartbeats were not replicated downstream to primary cluster.");
        }

        assertTrue(backupClient.upstreamClusters().contains(PRIMARY_CLUSTER_ALIAS), "Did not find upstream primary cluster.");
        assertEquals(1, backupClient.replicationHops(PRIMARY_CLUSTER_ALIAS), "Did not calculate replication hops correctly.");
        assertTrue(backup.kafka().consume(1, CHECKPOINT_DURATION_MS, "primary_checkpoints_internal").count() > 0,
            "Checkpoints were not emitted downstream to backup cluster.");
        if (replicateBackupToPrimary) {
            assertTrue(primaryClient.upstreamClusters().contains(BACKUP_CLUSTER_ALIAS), "Did not find upstream backup cluster.");
            assertEquals(1, primaryClient.replicationHops(BACKUP_CLUSTER_ALIAS), "Did not calculate replication hops correctly.");
            assertTrue(primary.kafka().consume(1, CHECKPOINT_DURATION_MS, "backup_checkpoints_internal").count() > 0,
                "Checkpoints were not emitted upstream to primary cluster.");
        }

        Map<TopicPartition, OffsetAndMetadata> backupOffsets = waitForCheckpointOnAllPartitions(
            backupClient, consumerGroupName, PRIMARY_CLUSTER_ALIAS, backupTopic1);

        // Failover consumer group to backup cluster.
        try (Consumer<byte[], byte[]> primaryConsumer = backup.kafka().createConsumer(Collections.singletonMap("group.id", consumerGroupName))) {
            primaryConsumer.assign(backupOffsets.keySet());
            backupOffsets.forEach(primaryConsumer::seek);
            primaryConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            primaryConsumer.commitAsync();

            assertTrue(primaryConsumer.position(new TopicPartition(backupTopic1, 0)) > 0, "Consumer failedover to zero offset.");
            assertTrue(primaryConsumer.position(
                new TopicPartition(backupTopic1, 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedover beyond expected offset.");
        }

        assertMonotonicCheckpoints(backup, "primary_checkpoints_internal");

        primaryClient.close();
        backupClient.close();

        if (replicateBackupToPrimary) {
            Map<TopicPartition, OffsetAndMetadata> primaryOffsets = waitForCheckpointOnAllPartitions(
                primaryClient, consumerGroupName, BACKUP_CLUSTER_ALIAS, reverseTopic1);

            // Failback consumer group to primary cluster
            try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumer(Collections.singletonMap("group.id", consumerGroupName))) {
                primaryConsumer.assign(primaryOffsets.keySet());
                primaryOffsets.forEach(primaryConsumer::seek);
                primaryConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
                primaryConsumer.commitAsync();

                assertTrue(primaryConsumer.position(new TopicPartition(reverseTopic1, 0)) > 0, "Consumer failedback to zero downstream offset.");
                assertTrue(primaryConsumer.position(
                    new TopicPartition(reverseTopic1, 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedback beyond expected downstream offset.");
            }

        }

        // create more matching topics
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        String backupTopic2 = remoteTopicName("test-topic-2", PRIMARY_CLUSTER_ALIAS, separator);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(backup, backupTopic2);

        // only produce messages to the first partition
        produceMessages(primaryProducer, "test-topic-2", 1);

        // expect total consumed messages equals to NUM_RECORDS_PER_PARTITION
        assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-2").count(),
            "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, backupTopic2).count(),
            "New topic was not replicated to backup cluster.");

        if (replicateBackupToPrimary) {
            backup.kafka().createTopic("test-topic-3", NUM_PARTITIONS);
            String reverseTopic3 = remoteTopicName("test-topic-3", BACKUP_CLUSTER_ALIAS, separator);
            waitForTopicCreated(primary, reverseTopic3);
            produceMessages(backupProducer, "test-topic-3", 1);
            assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-3").count(),
                "Records were not produced to backup cluster.");

            assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, reverseTopic3).count(),
                "New topic was not replicated to primary cluster.");
        }
    }

    @Test
    public void testReplicationWithCustomSeparatorAndInternalTopicSeparatorDisable() throws Exception {
        String separator = "_";
        produceMessages(primaryProducer, "test-topic-1");
        String backupTopic1 = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS, separator);
        if (replicateBackupToPrimary) {
            produceMessages(backupProducer, "test-topic-1");
        }
        String reverseTopic1 = remoteTopicName("test-topic-1", BACKUP_CLUSTER_ALIAS, separator);
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        // warm up consumers before starting the connectors, so we don't need to wait for discovery
        warmUpConsumer(consumerProps);

        mm2Props.put("replication.policy.separator", separator);
        mm2Props.put("replication.policy.internal.topic.separator.enabled", "false");
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        List<Class<? extends Connector>> primaryConnectors = replicateBackupToPrimary ? CONNECTOR_LIST : Collections.singletonList(MirrorHeartbeatConnector.class);
        waitUntilMirrorMakerIsRunning(primary, primaryConnectors, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

        MirrorClient primaryClient = new MirrorClient(mm2Config.clientConfig(PRIMARY_CLUSTER_ALIAS));
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, reverseTopic1);
        waitForTopicCreated(backup, backupTopic1);
        waitForTopicCreated(primary, "mm2-offset-syncs.backup.internal");
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, getTopicConfig(backup.kafka(), backupTopic1, TopicConfig.CLEANUP_POLICY_CONFIG),
            "topic config was not synced");
        createAndTestNewTopicWithConfigFilter(separator);

        assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
            "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, backupTopic1).count(),
            "Records were not replicated to backup cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
            "Records were not produced to backup cluster.");
        if (replicateBackupToPrimary) {
            assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, reverseTopic1).count(),
                "Records were not replicated to primary cluster.");
            assertEquals(NUM_RECORDS_PRODUCED * 2, primary.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, reverseTopic1, "test-topic-1").count(),
                "Primary cluster doesn't have all records from both clusters.");
            assertEquals(NUM_RECORDS_PRODUCED * 2, backup.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, backupTopic1, "test-topic-1").count(),
                "Backup cluster doesn't have all records from both clusters.");
        }

        assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
            "Heartbeats were not emitted to primary cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
            "Heartbeats were not emitted to backup cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "primary_heartbeats").count() > 0,
            "Heartbeats were not replicated downstream to backup cluster.");
        if (replicateBackupToPrimary) {
            assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "backup_heartbeats").count() > 0,
                "Heartbeats were not replicated downstream to primary cluster.");
        }

        assertTrue(backupClient.upstreamClusters().contains(PRIMARY_CLUSTER_ALIAS), "Did not find upstream primary cluster.");
        assertEquals(1, backupClient.replicationHops(PRIMARY_CLUSTER_ALIAS), "Did not calculate replication hops correctly.");
        assertTrue(backup.kafka().consume(1, CHECKPOINT_DURATION_MS, "primary.checkpoints.internal").count() > 0,
            "Checkpoints were not emitted downstream to backup cluster.");
        if (replicateBackupToPrimary) {
            assertTrue(primaryClient.upstreamClusters().contains(BACKUP_CLUSTER_ALIAS), "Did not find upstream backup cluster.");
            assertEquals(1, primaryClient.replicationHops(BACKUP_CLUSTER_ALIAS), "Did not calculate replication hops correctly.");
            assertTrue(primary.kafka().consume(1, CHECKPOINT_DURATION_MS, "backup.checkpoints.internal").count() > 0,
                "Checkpoints were not emitted upstream to primary cluster.");
        }

        Map<TopicPartition, OffsetAndMetadata> backupOffsets = waitForCheckpointOnAllPartitions(
            backupClient, consumerGroupName, PRIMARY_CLUSTER_ALIAS, backupTopic1);

        // Failover consumer group to backup cluster.
        try (Consumer<byte[], byte[]> primaryConsumer = backup.kafka().createConsumer(Collections.singletonMap("group.id", consumerGroupName))) {
            primaryConsumer.assign(backupOffsets.keySet());
            backupOffsets.forEach(primaryConsumer::seek);
            primaryConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            primaryConsumer.commitAsync();

            assertTrue(primaryConsumer.position(new TopicPartition(backupTopic1, 0)) > 0, "Consumer failedover to zero offset.");
            assertTrue(primaryConsumer.position(
                new TopicPartition(backupTopic1, 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedover beyond expected offset.");
        }

        assertMonotonicCheckpoints(backup, "primary.checkpoints.internal");

        primaryClient.close();
        backupClient.close();

        if (replicateBackupToPrimary) {
            Map<TopicPartition, OffsetAndMetadata> primaryOffsets = waitForCheckpointOnAllPartitions(
                primaryClient, consumerGroupName, BACKUP_CLUSTER_ALIAS, reverseTopic1);

            // Failback consumer group to primary cluster
            try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumer(Collections.singletonMap("group.id", consumerGroupName))) {
                primaryConsumer.assign(primaryOffsets.keySet());
                primaryOffsets.forEach(primaryConsumer::seek);
                primaryConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
                primaryConsumer.commitAsync();

                assertTrue(primaryConsumer.position(new TopicPartition(reverseTopic1, 0)) > 0, "Consumer failedback to zero downstream offset.");
                assertTrue(primaryConsumer.position(
                    new TopicPartition(reverseTopic1, 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedback beyond expected downstream offset.");
            }

        }

        // create more matching topics
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        String backupTopic2 = remoteTopicName("test-topic-2", PRIMARY_CLUSTER_ALIAS, separator);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(backup, backupTopic2);

        // only produce messages to the first partition
        produceMessages(primaryProducer, "test-topic-2", 1);

        // expect total consumed messages equals to NUM_RECORDS_PER_PARTITION
        assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-2").count(),
            "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, backupTopic2).count(),
            "New topic was not replicated to backup cluster.");

        if (replicateBackupToPrimary) {
            backup.kafka().createTopic("test-topic-3", NUM_PARTITIONS);
            String reverseTopic3 = remoteTopicName("test-topic-3", BACKUP_CLUSTER_ALIAS, separator);
            waitForTopicCreated(primary, reverseTopic3);
            produceMessages(backupProducer, "test-topic-3", 1);
            assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-3").count(),
                "Records were not produced to backup cluster.");

            assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, reverseTopic3).count(),
                "New topic was not replicated to primary cluster.");
        }
    }

    void createAndTestNewTopicWithConfigFilter(String separator) throws Exception {
        final String topic = "test-topic-with-config";
        final String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS, separator);

        createAndTestNewTopicWithConfigFilter(topic, backupTopic);
    }
}
