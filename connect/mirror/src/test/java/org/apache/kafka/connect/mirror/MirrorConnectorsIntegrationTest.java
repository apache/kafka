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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.connect.util.clusters.UngracefulShutdownException;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.common.utils.Exit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests MM2 replication and failover/failback logic.
 *
 * MM2 is configured with active/active replication between two Kafka clusters. Tests validate that
 * records sent to either cluster arrive at the other cluster. Then, a consumer group is migrated from
 * one cluster to the other and back. Tests validate that consumer offsets are translated and replicated
 * between clusters during this failover and failback.
 */
@Category(IntegrationTest.class)
public class MirrorConnectorsIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(MirrorConnectorsIntegrationTest.class);

    private static final int NUM_RECORDS_PER_PARTITION = 10;
    private static final int NUM_PARTITIONS = 10;
    private static final int NUM_RECORDS_PRODUCED = NUM_PARTITIONS * NUM_RECORDS_PER_PARTITION;
    private static final int RECORD_TRANSFER_DURATION_MS = 30_000;
    private static final int CHECKPOINT_DURATION_MS = 20_000;
    private static final int RECORD_CONSUME_DURATION_MS = 20_000;
    private static final int OFFSET_SYNC_DURATION_MS = 30_000;

    private volatile boolean shuttingDown;
    private Map<String, String> mm2Props;
    private MirrorMakerConfig mm2Config;
    private EmbeddedConnectCluster primary;
    private EmbeddedConnectCluster backup;

    private Exit.Procedure exitProcedure;
    private Exit.Procedure haltProcedure;

    @Before
    public void setup() throws InterruptedException {
        shuttingDown = false;
        exitProcedure = (code, message) -> {
            if (shuttingDown) {
                // ignore this since we're shutting down Connect and Kafka and timing isn't always great
                return;
            }
            if (code != 0) {
                String exitMessage = "Abrupt service exit with code " + code + " and message " + message;
                log.warn(exitMessage);
                throw new UngracefulShutdownException(exitMessage);
            }
        };
        haltProcedure = (code, message) -> {
            if (shuttingDown) {
                // ignore this since we're shutting down Connect and Kafka and timing isn't always great
                return;
            }
            if (code != 0) {
                String haltMessage = "Abrupt service halt with code " + code + " and message " + message;
                log.warn(haltMessage);
                throw new UngracefulShutdownException(haltMessage);
            }
        };
        // Override the exit and halt procedure that Connect and Kafka will use. For these integration tests,
        // we don't want to exit the JVM and instead simply want to fail the test
        Exit.setExitProcedure(exitProcedure);
        Exit.setHaltProcedure(haltProcedure);

        Properties brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", "false");

        mm2Props = new HashMap<>();
        mm2Props.put("clusters", "primary, backup");
        mm2Props.put("max.tasks", "10");
        mm2Props.put("topics", "test-topic-.*, primary.test-topic-.*, backup.test-topic-.*");
        mm2Props.put("groups", "consumer-group-.*");
        mm2Props.put("primary->backup.enabled", "true");
        mm2Props.put("backup->primary.enabled", "true");
        mm2Props.put("sync.topic.acls.enabled", "false");
        mm2Props.put("emit.checkpoints.interval.seconds", "1");
        mm2Props.put("emit.heartbeats.interval.seconds", "1");
        mm2Props.put("refresh.topics.interval.seconds", "1");
        mm2Props.put("refresh.groups.interval.seconds", "1");
        mm2Props.put("checkpoints.topic.replication.factor", "1");
        mm2Props.put("heartbeats.topic.replication.factor", "1");
        mm2Props.put("offset-syncs.topic.replication.factor", "1");
        mm2Props.put("config.storage.replication.factor", "1");
        mm2Props.put("offset.storage.replication.factor", "1");
        mm2Props.put("status.storage.replication.factor", "1");
        mm2Props.put("replication.factor", "1");
        
        mm2Config = new MirrorMakerConfig(mm2Props); 
        Map<String, String> primaryWorkerProps = mm2Config.workerConfig(new SourceAndTarget("backup", "primary"));
        Map<String, String> backupWorkerProps = mm2Config.workerConfig(new SourceAndTarget("primary", "backup"));

        primary = new EmbeddedConnectCluster.Builder()
                .name("primary-connect-cluster")
                .numWorkers(3)
                .numBrokers(1)
                .brokerProps(brokerProps)
                .workerProps(primaryWorkerProps)
                .maskExitProcedures(false)
                .build();

        backup = new EmbeddedConnectCluster.Builder()
                .name("backup-connect-cluster")
                .numWorkers(3)
                .numBrokers(1)
                .brokerProps(brokerProps)
                .workerProps(backupWorkerProps)
                .maskExitProcedures(false)
                .build();

        primary.start();
        primary.assertions().assertAtLeastNumWorkersAreUp(3,
                "Workers of primary-connect-cluster did not start in time.");
        backup.start();
        backup.assertions().assertAtLeastNumWorkersAreUp(3,
                "Workers of backup-connect-cluster did not start in time.");

        // create these topics before starting the connectors so we don't need to wait for discovery
        primary.kafka().createTopic("test-topic-1", NUM_PARTITIONS);
        primary.kafka().createTopic("backup.test-topic-1", 1);
        primary.kafka().createTopic("heartbeats", 1);
        backup.kafka().createTopic("test-topic-1", NUM_PARTITIONS);
        backup.kafka().createTopic("primary.test-topic-1", 1);
        backup.kafka().createTopic("heartbeats", 1);

        // produce to all partitions of test-topic-1
        produceMessages(primary, "test-topic-1", "message-1-");
        produceMessages(backup, "test-topic-1", "message-2-");

        // Generate some consumer activity on both clusters to ensure the checkpoint connector always starts promptly
        Map<String, Object> dummyProps = Collections.singletonMap("group.id", "consumer-group-dummy");
        Consumer<byte[], byte[]> dummyConsumer = primary.kafka().createConsumerAndSubscribeTo(dummyProps, "test-topic-1");
        consumeAllMessages(dummyConsumer);
        dummyConsumer.close();
        dummyConsumer = backup.kafka().createConsumerAndSubscribeTo(dummyProps, "test-topic-1");
        consumeAllMessages(dummyConsumer);
        dummyConsumer.close();

        log.info("primary REST service: {}", primary.endpointForResource("connectors"));
        log.info("backup REST service: {}", backup.endpointForResource("connectors"));
 
        log.info("primary brokers: {}", primary.kafka().bootstrapServers());
        log.info("backup brokers: {}", backup.kafka().bootstrapServers());
        
        // now that the brokers are running, we can finish setting up the Connectors
        mm2Props.put("primary.bootstrap.servers", primary.kafka().bootstrapServers());
        mm2Props.put("backup.bootstrap.servers", backup.kafka().bootstrapServers());
        mm2Config = new MirrorMakerConfig(mm2Props);
    }


    private void waitUntilMirrorMakerIsRunning(EmbeddedConnectCluster connectCluster,
        MirrorMakerConfig mm2Config, String primary, String backup) throws InterruptedException {

        connectCluster.configureConnector("MirrorSourceConnector",
                mm2Config.connectorBaseConfig(new SourceAndTarget(primary, backup), MirrorSourceConnector.class));
        connectCluster.configureConnector("MirrorCheckpointConnector",
                mm2Config.connectorBaseConfig(new SourceAndTarget(primary, backup), MirrorCheckpointConnector.class));
        connectCluster.configureConnector("MirrorHeartbeatConnector",
                mm2Config.connectorBaseConfig(new SourceAndTarget(primary, backup), MirrorHeartbeatConnector.class));

        // we wait for the connector and tasks to come up for each connector, so that when we do the
        // actual testing, we are certain that the tasks are up and running; this will prevent
        // flaky tests where the connector and tasks didn't start up in time for the tests to be
        // run
        Set<String> connectorNames = new HashSet<>(Arrays.asList("MirrorSourceConnector",
                                                                 "MirrorCheckpointConnector", "MirrorHeartbeatConnector"));

        for (String connector : connectorNames) {
            connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connector, 1,
                    "Connector " + connector + " tasks did not start in time on cluster: " + connectCluster);
        }
    }

    @After
    public void close() {
        try {
            for (String x : primary.connectors()) {
                primary.deleteConnector(x);
            }
            for (String x : backup.connectors()) {
                backup.deleteConnector(x);
            }
            deleteAllTopics(primary.kafka());
            deleteAllTopics(backup.kafka());
        } finally {
            shuttingDown = true;
            try {
                try {
                    primary.stop();
                } finally {
                    backup.stop();
                }
            } finally {
                Exit.resetExitProcedure();
                Exit.resetHaltProcedure();
            }
        }
    }

    @Test
    public void testReplication() throws InterruptedException {
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = new HashMap<String, Object>() {{
                put("group.id", consumerGroupName);
                put("auto.offset.reset", "latest");
            }};

        // create consumers before starting the connectors so we don't need to wait for discovery
        Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1");
        consumeAllMessages(primaryConsumer, 0);
        primaryConsumer.close();

        Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1");
        consumeAllMessages(backupConsumer, 0);
        backupConsumer.close();

        waitUntilMirrorMakerIsRunning(backup, mm2Config, "primary", "backup");
        waitUntilMirrorMakerIsRunning(primary, mm2Config, "backup", "primary");
        MirrorClient primaryClient = new MirrorClient(mm2Config.clientConfig("primary"));
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig("backup"));

        assertEquals("Records were not produced to primary cluster.", NUM_RECORDS_PRODUCED,
            primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count());
        assertEquals("Records were not replicated to backup cluster.", NUM_RECORDS_PRODUCED,
            backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "primary.test-topic-1").count());
        assertEquals("Records were not produced to backup cluster.", NUM_RECORDS_PRODUCED,
            backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count());
        assertEquals("Records were not replicated to primary cluster.", NUM_RECORDS_PRODUCED,
            primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "backup.test-topic-1").count());

        assertEquals("Primary cluster doesn't have all records from both clusters.", NUM_RECORDS_PRODUCED * 2,
            primary.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, "backup.test-topic-1", "test-topic-1").count());
        assertEquals("Backup cluster doesn't have all records from both clusters.", NUM_RECORDS_PRODUCED * 2,
            backup.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, "primary.test-topic-1", "test-topic-1").count());

        assertTrue("Heartbeats were not emitted to primary cluster.",
            primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0);
        assertTrue("Heartbeats were not emitted to backup cluster.",
            backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0);
        assertTrue("Heartbeats were not replicated downstream to backup cluster.",
            backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "primary.heartbeats").count() > 0);
        assertTrue("Heartbeats were not replicated downstream to primary cluster.",
            primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "backup.heartbeats").count() > 0);

        assertTrue("Did not find upstream primary cluster.", backupClient.upstreamClusters().contains("primary"));
        assertEquals("Did not calculate replication hops correctly.", 1, backupClient.replicationHops("primary"));
        assertTrue("Did not find upstream backup cluster.", primaryClient.upstreamClusters().contains("backup"));
        assertEquals("Did not calculate replication hops correctly.", 1, primaryClient.replicationHops("backup"));

        assertTrue("Checkpoints were not emitted downstream to backup cluster.",
            backup.kafka().consume(1, CHECKPOINT_DURATION_MS, "primary.checkpoints.internal").count() > 0);

        Map<TopicPartition, OffsetAndMetadata> backupOffsets = backupClient.remoteConsumerOffsets(consumerGroupName, "primary",
            Duration.ofMillis(CHECKPOINT_DURATION_MS));

        assertTrue("Offsets not translated downstream to backup cluster. Found: " + backupOffsets, backupOffsets.containsKey(
            new TopicPartition("primary.test-topic-1", 0)));

        // Failover consumer group to backup cluster.
        backupConsumer = backup.kafka().createConsumer(consumerProps);
        backupConsumer.assign(allPartitions("test-topic-1", "primary.test-topic-1"));
        seek(backupConsumer, backupOffsets);
        consumeAllMessages(backupConsumer, 0);

        assertTrue("Consumer failedover to zero offset.", backupConsumer.position(new TopicPartition("primary.test-topic-1", 0)) > 0);
        assertTrue("Consumer failedover beyond expected offset.", backupConsumer.position(
            new TopicPartition("primary.test-topic-1", 0)) <= NUM_RECORDS_PRODUCED);
        assertTrue("Checkpoints were not emitted upstream to primary cluster.", primary.kafka().consume(1,
            CHECKPOINT_DURATION_MS, "backup.checkpoints.internal").count() > 0);

        backupConsumer.close();

        waitForCondition(() -> {
            try {
                return primaryClient.remoteConsumerOffsets(consumerGroupName, "backup",
                    Duration.ofMillis(CHECKPOINT_DURATION_MS)).containsKey(new TopicPartition("backup.test-topic-1", 0));
            } catch (Throwable e) {
                return false;
            }
        }, CHECKPOINT_DURATION_MS, "Offsets not translated downstream to primary cluster.");

        waitForCondition(() -> {
            try {
                return primaryClient.remoteConsumerOffsets(consumerGroupName, "backup",
                    Duration.ofMillis(CHECKPOINT_DURATION_MS)).containsKey(new TopicPartition("test-topic-1", 0));
            } catch (Throwable e) {
                return false;
            }
        }, CHECKPOINT_DURATION_MS, "Offsets not translated upstream to primary cluster.");

        Map<TopicPartition, OffsetAndMetadata> primaryOffsets = primaryClient.remoteConsumerOffsets(consumerGroupName, "backup",
                Duration.ofMillis(CHECKPOINT_DURATION_MS));

        primaryClient.close();
        backupClient.close();

        // Failback consumer group to primary cluster
        primaryConsumer = primary.kafka().createConsumer(consumerProps);
        primaryConsumer.assign(allPartitions("test-topic-1", "backup.test-topic-1"));
        seek(primaryConsumer, primaryOffsets);
        consumeAllMessages(primaryConsumer, 0);

        assertTrue("Consumer failedback to zero upstream offset.", primaryConsumer.position(new TopicPartition("test-topic-1", 0)) > 0);
        assertTrue("Consumer failedback to zero downstream offset.", primaryConsumer.position(new TopicPartition("backup.test-topic-1", 0)) > 0);
        assertTrue("Consumer failedback beyond expected upstream offset.", primaryConsumer.position(
            new TopicPartition("test-topic-1", 0)) <= NUM_RECORDS_PER_PARTITION);
        assertTrue("Consumer failedback beyond expected downstream offset.", primaryConsumer.position(
            new TopicPartition("backup.test-topic-1", 0)) <= NUM_RECORDS_PER_PARTITION);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> messages2 = consumeAllMessages(primaryConsumer, 0);
        // If offset translation was successful we expect no messages to be consumed after failback
        assertEquals("Data was consumed from partitions: " + messages2.keySet() + ".", 0, messages2.size());
        primaryConsumer.close();

        // create more matching topics
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        backup.kafka().createTopic("test-topic-3", NUM_PARTITIONS);

        produceMessages(primary, "test-topic-2", "message-3-", 1);
        produceMessages(backup, "test-topic-3", "message-4-", 1);

        assertEquals("Records were not produced to primary cluster.", NUM_RECORDS_PER_PARTITION,
            primary.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-2").count());
        assertEquals("Records were not produced to backup cluster.", NUM_RECORDS_PER_PARTITION,
            backup.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-3").count());

        assertEquals("New topic was not replicated to primary cluster.", NUM_RECORDS_PER_PARTITION,
            primary.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, "backup.test-topic-3").count());
        assertEquals("New topic was not replicated to backup cluster.", NUM_RECORDS_PER_PARTITION,
            backup.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, "primary.test-topic-2").count());
    }

    @Test
    public void testReplicationWithEmptyPartition() throws Exception {
        String consumerGroupName = "consumer-group-testReplicationWithEmptyPartition";
        Map<String, Object> consumerProps  = Collections.singletonMap("group.id", consumerGroupName);

        // create topics
        String topic = "test-topic-with-empty-partition";
        primary.kafka().createTopic(topic, NUM_PARTITIONS);

        // produce to all test-topic-empty's partitions *but the last one*, on the primary cluster
        produceMessages(primary, topic, "message-1-", NUM_PARTITIONS - 1);
        // Consume, from the primary cluster, before starting the connectors so we don't need to wait for discovery
        int expectedRecords = NUM_RECORDS_PER_PARTITION * (NUM_PARTITIONS - 1);
        try (Consumer<byte[], byte[]> consumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, topic)) {
            consumeAllMessages(consumer, expectedRecords);
        }

        waitUntilMirrorMakerIsRunning(backup, mm2Config, "primary", "backup");

        Map<TopicPartition, OffsetAndMetadata> offsets = waitForConsumerGroupOffsetReplication(
                Collections.singletonList("primary." + topic), consumerGroupName, false);

        // check translated offset for the last partition (empty partition)
        OffsetAndMetadata oam = offsets.get(new TopicPartition("primary." + topic, NUM_PARTITIONS - 1));
        assertNotNull("Offset of last partition was not replicated", oam);
        assertEquals("Offset of last partition is not zero", 0, oam.offset());
    }

    private Map<TopicPartition, OffsetAndMetadata> waitForConsumerGroupOffsetReplication(List<String> topics, String consumerGroupId, boolean waitForAutoSync)
            throws InterruptedException {
        Admin backupClient = backup.kafka().createAdminClient();
        List<TopicPartition> tps = new ArrayList<>(NUM_PARTITIONS * topics.size());
        for (int partitionIndex = 0; partitionIndex < NUM_PARTITIONS; partitionIndex++) {
            for (String topic : topics) {
                tps.add(new TopicPartition(topic, partitionIndex));
            }
        }
        long expectedTotalOffsets = NUM_PARTITIONS * topics.size();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        waitForCondition(() -> {
            offsets.clear();
            Map<TopicPartition, OffsetAndMetadata> translatedOffsets = getTranslatedOffsets(consumerGroupId);
            Map<TopicPartition, OffsetAndMetadata> remoteOffsets =
                    backupClient.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get();
            boolean isConsumerUpdated = true;
            for (TopicPartition tp : tps) {
                OffsetAndMetadata translatedOam = translatedOffsets.get(tp);
                OffsetAndMetadata remoteOam = remoteOffsets.get(tp);
                if (translatedOam != null) {
                    offsets.put(tp, translatedOam);
                    if (waitForAutoSync && remoteOam.offset() != translatedOam.offset())
                        isConsumerUpdated = false;
                }
            }
            return offsets.size() == expectedTotalOffsets && isConsumerUpdated;
        }, OFFSET_SYNC_DURATION_MS, "Consumer group offset sync did not complete in time");
        return offsets;
    }

    private Map<TopicPartition, OffsetAndMetadata> getTranslatedOffsets(String consumerGroupId)
            throws TimeoutException, InterruptedException {
        return RemoteClusterUtils.translateOffsets(
                mm2Config.clientConfig("backup").adminConfig(),
                "primary",
                consumerGroupId,
                Duration.ofMillis(CHECKPOINT_DURATION_MS));
    }

    @Test
    public void testOneWayReplicationWithAutoOffsetSync() throws InterruptedException {
        String topic1 = "test-topic-auto-offset-sync-1";
        String topic2 = "test-topic-auto-offset-sync-2";
        String consumerGroupName = "consumer-group-testOneWayReplicationWithAutoOffsetSync";
        Map<String, Object> consumerProps  = new HashMap<String, Object>() {{
                put("group.id", consumerGroupName);
                put("auto.offset.reset", "earliest");
            }};

        // create new topic
        primary.kafka().createTopic(topic1, NUM_PARTITIONS);
        backup.kafka().createTopic("primary." + topic1, 1);

        // produce some records to the new topic in primary cluster
        produceMessages(primary, topic1, "message-1-");
        // create consumers before starting the connectors so we don't need to wait for discovery
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, topic1)) {
            consumeAllMessages(primaryConsumer);
        }

        // enable automated consumer group offset sync
        mm2Props.put("sync.group.offsets.enabled", "true");
        mm2Props.put("sync.group.offsets.interval.seconds", "1");
        // one way replication from primary to backup
        mm2Props.put("backup->primary.enabled", "false");

        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, mm2Config, "primary", "backup");

        waitForConsumerGroupOffsetReplication(
                Collections.singletonList("primary." + topic1), consumerGroupName, true);

        // create a consumer at backup cluster with same consumer group Id to consume 1 topic
        ConsumerRecords<byte[], byte[]> records = null;
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(
                consumerProps, "primary." + topic1)) {
            records = backupConsumer.poll(Duration.ofMillis(500));
        }

        // the size of consumer record should be zero, because the offsets of the same consumer group
        // have been automatically synchronized from primary to backup by the background job, so no
        // more records to consume from the replicated topic by the same consumer group at backup cluster
        assertEquals("consumer record size is not zero", 0, records.count());

        // create a second topic
        primary.kafka().createTopic(topic2, NUM_PARTITIONS);
        backup.kafka().createTopic("primary." + topic2, 1);

        // produce some records to the new topic in primary cluster
        produceMessages(primary, topic2, "message-1-");

        // create a consumer at primary cluster to consume the new topic
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(
            consumerProps, topic2)) {
            // we need to wait for consuming all the records for MM2 replicating the expected offsets
            consumeAllMessages(primaryConsumer);
        }

        waitForConsumerGroupOffsetReplication(Arrays.asList("primary." + topic1, "primary." + topic2), consumerGroupName, true);

        // create a consumer at backup cluster with same consumer group Id to consume old and new topic
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(
                consumerProps, "primary." + topic1, "primary." + topic2)) {
            records = backupConsumer.poll(Duration.ofMillis(500));
        }

        // similar reasoning as above, no more records to consume by the same consumer group at backup cluster
        assertEquals("consumer record size is not zero", 0, records.count());
    }

    private void deleteAllTopics(EmbeddedKafkaCluster cluster) {
        Admin client = cluster.createAdminClient();
        try {
            client.deleteTopics(client.listTopics().names().get());
        } catch (Throwable e) {
        }
    }

    private void produceMessages(EmbeddedConnectCluster cluster, String topicName, String msgPrefix) {
        produceMessages(cluster, topicName, msgPrefix, NUM_PARTITIONS);
    }

    private void produceMessages(EmbeddedConnectCluster cluster, String topicName, String msgPrefix, int numPartitions) {
        // produce the configured number of records to all specified partitions
        int cnt = 0;
        for (int r = 0; r < NUM_RECORDS_PER_PARTITION; r++)
            for (int p = 0; p < numPartitions; p++)
                cluster.kafka().produce(topicName, p, "key", msgPrefix + cnt++);
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumeAllMessages(Consumer<byte[], byte[]> consumer) throws InterruptedException {
        return consumeAllMessages(consumer, null, null);
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumeAllMessages(Consumer<byte[], byte[]> consumer, Integer expectedRecords) throws InterruptedException {
        return consumeAllMessages(consumer, expectedRecords, null);
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumeAllMessages(Consumer<byte[], byte[]> consumer, Integer numExpectedRecords, Duration timeoutDurationMs)
            throws InterruptedException {
        int expectedRecords = numExpectedRecords != null ? numExpectedRecords : NUM_RECORDS_PRODUCED;
        int timeoutMs = (int) (timeoutDurationMs != null ? timeoutDurationMs.toMillis() : RECORD_CONSUME_DURATION_MS);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        waitForCondition(() -> {
            ConsumerRecords<byte[], byte[]> crs = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<byte[], byte[]> cr : crs)
                records.add(cr);
            assertTrue("Consumer consumed more records than expected: " + records.size() + " (expected " + expectedRecords + ").",
                    records.size() <= expectedRecords);
            return records.size() == expectedRecords;
        }, timeoutMs, "Consumer could not consume all records in time.");

        consumer.commitSync();
        return records.stream().collect(Collectors.groupingBy(c -> new TopicPartition(c.topic(), c.partition())));
    }

    private void seek(Consumer<byte[], byte[]> consumer, Map<TopicPartition, OffsetAndMetadata> offsets) throws InterruptedException {
        // In case offsets are replicated faster than actual records, wait until records are replicated before seeking
        waitForCondition(() -> {
            boolean ready = true;
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(offsets.keySet());
            for (TopicPartition tp : offsets.keySet()) {
                if (offsets.get(tp).offset() > endOffsets.get(tp)) {
                    ready = false;
                    break;
                }
            }
            if (!ready)
                Thread.sleep(1000);
            return ready;
        }, RECORD_TRANSFER_DURATION_MS, "Records were not replicated in time.");
        offsets.forEach(consumer::seek);
    }

    private List<TopicPartition> allPartitions(String... topics) {
        return IntStream.range(0, NUM_PARTITIONS)
                .boxed()
                .flatMap(p -> Arrays.stream(topics).map(t -> new TopicPartition(t, p)))
                .collect(Collectors.toList());
    }
}
