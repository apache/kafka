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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.mirror.DefaultConfigPropertyFilter;
import org.apache.kafka.connect.mirror.MirrorClient;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.mirror.MirrorUtils;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.Checkpoint;
import org.apache.kafka.connect.mirror.MirrorCheckpointConnector;
import org.apache.kafka.connect.mirror.TestUtils;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.connect.util.clusters.UngracefulShutdownException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Tests MM2 replication and failover/failback logic.
 *
 * MM2 is configured with active/active replication between two Kafka clusters. Tests validate that
 * records sent to either cluster arrive at the other cluster. Then, a consumer group is migrated from
 * one cluster to the other and back. Tests validate that consumer offsets are translated and replicated
 * between clusters during this failover and failback.
 */
@Tag("integration")
public class MirrorConnectorsIntegrationBaseTest {
    private static final Logger log = LoggerFactory.getLogger(MirrorConnectorsIntegrationBaseTest.class);
    
    protected static final int NUM_RECORDS_PER_PARTITION = 10;
    protected static final int NUM_PARTITIONS = 10;
    protected static final int NUM_RECORDS_PRODUCED = NUM_PARTITIONS * NUM_RECORDS_PER_PARTITION;
    protected static final int OFFSET_LAG_MAX = 10;
    protected static final int RECORD_PRODUCE_DURATION_MS = 20_000;
    protected static final int RECORD_TRANSFER_DURATION_MS = 30_000;
    protected static final int CHECKPOINT_DURATION_MS = 20_000;
    private static final int RECORD_CONSUME_DURATION_MS = 20_000;
    private static final int OFFSET_SYNC_DURATION_MS = 30_000;
    private static final int TOPIC_SYNC_DURATION_MS = 60_000;
    private static final int REQUEST_TIMEOUT_DURATION_MS = 60_000;
    private static final int CHECKPOINT_INTERVAL_DURATION_MS = 1_000;
    private static final int NUM_WORKERS = 3;
    protected static final Duration CONSUMER_POLL_TIMEOUT_MS = Duration.ofMillis(500L);
    protected static final String PRIMARY_CLUSTER_ALIAS = "primary";
    protected static final String BACKUP_CLUSTER_ALIAS = "backup";
    protected static final List<Class<? extends Connector>> CONNECTOR_LIST = Arrays.asList(
            MirrorSourceConnector.class,
            MirrorCheckpointConnector.class,
            MirrorHeartbeatConnector.class);

    private volatile boolean shuttingDown;
    protected Map<String, String> mm2Props = new HashMap<>();
    protected MirrorMakerConfig mm2Config;
    protected EmbeddedConnectCluster primary;
    protected EmbeddedConnectCluster backup;
    protected Producer<byte[], byte[]> primaryProducer;
    protected Producer<byte[], byte[]> backupProducer;

    protected Map<String, String> additionalPrimaryClusterClientsConfigs = new HashMap<>();
    protected Map<String, String> additionalBackupClusterClientsConfigs = new HashMap<>();
    protected boolean replicateBackupToPrimary = true;
    protected Boolean createReplicatedTopicsUpfront = false; // enable to speed up the test cases
    protected Exit.Procedure exitProcedure;
    private Exit.Procedure haltProcedure;
    
    protected Properties primaryBrokerProps = new Properties();
    protected Properties backupBrokerProps = new Properties();
    protected Map<String, String> primaryWorkerProps = new HashMap<>();
    protected Map<String, String> backupWorkerProps = new HashMap<>();

    @BeforeEach
    public void startClusters() throws Exception {
        startClusters(new HashMap<String, String>() {{
                put("topics", "test-topic-.*, primary.test-topic-.*, backup.test-topic-.*");
            }});
    }

    public void startClusters(Map<String, String> additionalMM2Config) throws Exception {
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
        
        primaryBrokerProps.put("auto.create.topics.enable", "false");
        backupBrokerProps.put("auto.create.topics.enable", "false");

        mm2Props.putAll(basicMM2Config());
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".enabled", "true");
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", Boolean.toString(replicateBackupToPrimary));
        mm2Props.putAll(additionalMM2Config);

        // exclude topic config:
        mm2Props.put(DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG, "delete\\.retention\\..*");
        // set it to shorter sync interval for testing
        mm2Props.put("sync.topic.configs.interval.seconds", "1");

        mm2Config = new MirrorMakerConfig(mm2Props);
        primaryWorkerProps = mm2Config.workerConfig(new SourceAndTarget(BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS));
        backupWorkerProps.putAll(mm2Config.workerConfig(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS)));
        
        primary = new EmbeddedConnectCluster.Builder()
                .name(PRIMARY_CLUSTER_ALIAS + "-connect-cluster")
                .numWorkers(NUM_WORKERS)
                .numBrokers(1)
                .brokerProps(primaryBrokerProps)
                .workerProps(primaryWorkerProps)
                .maskExitProcedures(false)
                .clientProps(additionalPrimaryClusterClientsConfigs)
                .build();

        backup = new EmbeddedConnectCluster.Builder()
                .name(BACKUP_CLUSTER_ALIAS + "-connect-cluster")
                .numWorkers(NUM_WORKERS)
                .numBrokers(1)
                .brokerProps(backupBrokerProps)
                .workerProps(backupWorkerProps)
                .maskExitProcedures(false)
                .clientProps(additionalBackupClusterClientsConfigs)
                .build();
        
        primary.start();
        primary.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
                "Workers of " + PRIMARY_CLUSTER_ALIAS + "-connect-cluster did not start in time.");

        waitForTopicCreated(primary, "mm2-status.backup.internal");
        waitForTopicCreated(primary, "mm2-offsets.backup.internal");
        waitForTopicCreated(primary, "mm2-configs.backup.internal");

        backup.start();
        backup.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS,
            "Workers of " + BACKUP_CLUSTER_ALIAS + "-connect-cluster did not start in time.");

        primaryProducer = initializeProducer(primary);
        backupProducer = initializeProducer(backup);

        createTopics();

        waitForTopicCreated(backup, "mm2-status.primary.internal");
        waitForTopicCreated(backup, "mm2-offsets.primary.internal");
        waitForTopicCreated(backup, "mm2-configs.primary.internal");
        waitForTopicCreated(backup, "test-topic-1");
        waitForTopicCreated(primary, "test-topic-1");
        warmUpConsumer(Collections.singletonMap("group.id", "consumer-group-dummy"));
        
        log.info(PRIMARY_CLUSTER_ALIAS + " REST service: {}", primary.endpointForResource("connectors"));
        log.info(BACKUP_CLUSTER_ALIAS + " REST service: {}", backup.endpointForResource("connectors"));
        log.info(PRIMARY_CLUSTER_ALIAS + " brokers: {}", primary.kafka().bootstrapServers());
        log.info(BACKUP_CLUSTER_ALIAS + " brokers: {}", backup.kafka().bootstrapServers());
        
        // now that the brokers are running, we can finish setting up the Connectors
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + ".bootstrap.servers", primary.kafka().bootstrapServers());
        mm2Props.put(BACKUP_CLUSTER_ALIAS + ".bootstrap.servers", backup.kafka().bootstrapServers());
    }
    
    @AfterEach
    public void shutdownClusters() throws Exception {
        try {
            Utils.closeQuietly(primaryProducer, "primary producer");
            Utils.closeQuietly(backupProducer, "backup producer");
            for (String x : primary.connectors()) {
                primary.deleteConnector(x);
            }
            for (String x : backup.connectors()) {
                backup.deleteConnector(x);
            }
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
    public void testReplication() throws Exception {
        produceMessages(primaryProducer, "test-topic-1");
        String backupTopic1 = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        if (replicateBackupToPrimary) {
            produceMessages(backupProducer, "test-topic-1");
        }
        String reverseTopic1 = remoteTopicName("test-topic-1", BACKUP_CLUSTER_ALIAS);
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        // warm up consumers before starting the connectors, so we don't need to wait for discovery
        warmUpConsumer(consumerProps);
        
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
        createAndTestNewTopicWithConfigFilter();

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
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "primary.heartbeats").count() > 0,
            "Heartbeats were not replicated downstream to backup cluster.");
        if (replicateBackupToPrimary) {
            assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "backup.heartbeats").count() > 0,
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
        String backupTopic2 = remoteTopicName("test-topic-2", PRIMARY_CLUSTER_ALIAS);

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
            String reverseTopic3 = remoteTopicName("test-topic-3", BACKUP_CLUSTER_ALIAS);
            waitForTopicCreated(primary, reverseTopic3);
            produceMessages(backupProducer, "test-topic-3", 1);
            assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-3").count(),
                    "Records were not produced to backup cluster.");

            assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, reverseTopic3).count(),
                    "New topic was not replicated to primary cluster.");
        }
    }

    @Test
    public void testReplicationWithEmptyPartition() throws Exception {
        String consumerGroupName = "consumer-group-testReplicationWithEmptyPartition";
        Map<String, Object> consumerProps  = Collections.singletonMap("group.id", consumerGroupName);

        // create topic
        String topic = "test-topic-with-empty-partition";
        primary.kafka().createTopic(topic, NUM_PARTITIONS);

        // produce to all test-topic-empty's partitions, except the last partition
        produceMessages(primaryProducer, topic, NUM_PARTITIONS - 1);

        // consume before starting the connectors, so we don't need to wait for discovery
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

        String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);

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
        testOneWayReplicationWithOffsetSyncs(OFFSET_LAG_MAX);
    }

    @Test
    public void testOneWayReplicationWithFrequentOffsetSyncs() throws InterruptedException {
        testOneWayReplicationWithOffsetSyncs(0);
    }

    private void testOneWayReplicationWithOffsetSyncs(int offsetLagMax) throws InterruptedException {
        produceMessages(primaryProducer, "test-topic-1");
        String backupTopic1 = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        String consumerGroupName = "consumer-group-testOneWayReplicationWithAutoOffsetSync";
        Map<String, Object> consumerProps  = new HashMap<String, Object>() {{
                put("group.id", consumerGroupName);
                put("auto.offset.reset", "earliest");
            }};
        // create consumers before starting the connectors, so we don't need to wait for discovery
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, 
                "test-topic-1")) {
            // we need to wait for consuming all the records for MM2 replicating the expected offsets
            waitForConsumingAllRecords(primaryConsumer, NUM_RECORDS_PRODUCED);
        }

        // enable automated consumer group offset sync
        mm2Props.put("sync.group.offsets.enabled", "true");
        mm2Props.put("sync.group.offsets.interval.seconds", "1");
        mm2Props.put("offset.lag.max", Integer.toString(offsetLagMax));
        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");


        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // make sure the topic is created in the primary cluster only
        String reverseTopic1 = remoteTopicName("test-topic-1", BACKUP_CLUSTER_ALIAS);
        if (!"test-topic-1".equals(reverseTopic1)) {
            topicShouldNotBeCreated(primary, reverseTopic1);
        }
        waitForTopicCreated(backup, backupTopic1);
        // create a consumer at backup cluster with same consumer group Id to consume 1 topic
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(
            consumerProps, backupTopic1)) {

            waitForConsumerGroupFullSync(backup, Collections.singletonList(backupTopic1),
                    consumerGroupName, NUM_RECORDS_PRODUCED, offsetLagMax);
            assertDownstreamRedeliveriesBoundedByMaxLag(backupConsumer, offsetLagMax);
        }

        // now create a new topic in primary cluster
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        // make sure the topic is created in backup cluster
        String remoteTopic2 = remoteTopicName("test-topic-2", PRIMARY_CLUSTER_ALIAS);
        waitForTopicCreated(backup, remoteTopic2);

        // produce some records to the new topic in primary cluster
        produceMessages(primaryProducer, "test-topic-2");

        // create a consumer at primary cluster to consume the new topic
        try (Consumer<byte[], byte[]> consumer1 = primary.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "group.id", consumerGroupName), "test-topic-2")) {
            // we need to wait for consuming all the records for MM2 replicating the expected offsets
            waitForConsumingAllRecords(consumer1, NUM_RECORDS_PRODUCED);
        }

        // create a consumer at backup cluster with same consumer group ID to consume old and new topic
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
            "group.id", consumerGroupName), backupTopic1, remoteTopic2)) {

            waitForConsumerGroupFullSync(backup, Arrays.asList(backupTopic1, remoteTopic2),
                    consumerGroupName, NUM_RECORDS_PRODUCED, offsetLagMax);
            assertDownstreamRedeliveriesBoundedByMaxLag(backupConsumer, offsetLagMax);
        }

        assertMonotonicCheckpoints(backup, PRIMARY_CLUSTER_ALIAS + ".checkpoints.internal");
    }

    @Test
    public void testOffsetSyncsTopicsOnTarget() throws Exception {
        // move offset-syncs topics to target
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".offset-syncs.topic.location", "target");
        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");

        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // Ensure the offset syncs topic is created in the target cluster
        waitForTopicCreated(backup, "mm2-offset-syncs." + PRIMARY_CLUSTER_ALIAS + ".internal");

        String consumerGroupName = "consumer-group-syncs-on-target";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);

        produceMessages(primaryProducer, "test-topic-1");

        warmUpConsumer(consumerProps);

        String remoteTopic = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);

        // Check offsets are pushed to the checkpoint topic
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "auto.offset.reset", "earliest"), PRIMARY_CLUSTER_ALIAS + ".checkpoints.internal")) {
            waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = backupConsumer.poll(Duration.ofSeconds(1L));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    Checkpoint checkpoint = Checkpoint.deserializeRecord(record);
                    if (remoteTopic.equals(checkpoint.topicPartition().topic())) {
                        return true;
                    }
                }
                return false;
            }, 30_000,
                "Unable to find checkpoints for " + PRIMARY_CLUSTER_ALIAS + ".test-topic-1"
            );
        }

        assertMonotonicCheckpoints(backup, PRIMARY_CLUSTER_ALIAS + ".checkpoints.internal");

        // Ensure no offset-syncs topics have been created on the primary cluster
        try (Admin adminClient = primary.kafka().createAdminClient()) {
            Set<String> primaryTopics = adminClient.listTopics().names().get();
            assertFalse(primaryTopics.contains("mm2-offset-syncs." + PRIMARY_CLUSTER_ALIAS + ".internal"));
            assertFalse(primaryTopics.contains("mm2-offset-syncs." + BACKUP_CLUSTER_ALIAS + ".internal"));
        }
    }

    @Test
    public void testNoCheckpointsIfNoRecordsAreMirrored() throws InterruptedException {
        String consumerGroupName = "consumer-group-no-checkpoints";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);

        // ensure there are some records in the topic on the source cluster
        produceMessages(primaryProducer, "test-topic-1");

        // warm up consumers before starting the connectors, so we don't need to wait for discovery
        warmUpConsumer(consumerProps);

        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // make sure the topics  are created in the backup cluster
        waitForTopicCreated(backup, remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS));
        waitForTopicCreated(backup, remoteTopicName("test-topic-no-checkpoints", PRIMARY_CLUSTER_ALIAS));

        // commit some offsets for both topics in the source cluster
        TopicPartition tp1 = new TopicPartition("test-topic-1", 0);
        TopicPartition tp2 = new TopicPartition("test-topic-no-checkpoints", 0);
        try (Consumer<byte[], byte[]> consumer = primary.kafka().createConsumer(consumerProps)) {
            Collection<TopicPartition> tps = Arrays.asList(tp1, tp2);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = endOffsets.entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> new OffsetAndMetadata(e.getValue())
                            ));
            consumer.commitSync(offsetsToCommit);
        }

        // Only test-topic-1 should have translated offsets because we've not yet mirrored any records for test-topic-no-checkpoints
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));
        waitForCondition(() -> {
            Map<TopicPartition, OffsetAndMetadata> translatedOffsets = backupClient.remoteConsumerOffsets(
                    consumerGroupName, PRIMARY_CLUSTER_ALIAS, Duration.ofSeconds(30L));
            return translatedOffsets.containsKey(remoteTopicPartition(tp1, PRIMARY_CLUSTER_ALIAS)) &&
                   !translatedOffsets.containsKey(remoteTopicPartition(tp2, PRIMARY_CLUSTER_ALIAS));
        }, OFFSET_SYNC_DURATION_MS, "Checkpoints were not emitted correctly to backup cluster");

        // Send some records to test-topic-no-checkpoints in the source cluster
        produceMessages(primaryProducer, "test-topic-no-checkpoints");

        try (Consumer<byte[], byte[]> consumer = primary.kafka().createConsumer(consumerProps)) {
            Collection<TopicPartition> tps = Arrays.asList(tp1, tp2);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = endOffsets.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> new OffsetAndMetadata(e.getValue())
                    ));
            consumer.commitSync(offsetsToCommit);
        }

        waitForCondition(() -> {
            Map<TopicPartition, OffsetAndMetadata> translatedOffsets = backupClient.remoteConsumerOffsets(
                    consumerGroupName, PRIMARY_CLUSTER_ALIAS, Duration.ofSeconds(30L));
            return translatedOffsets.containsKey(remoteTopicPartition(tp1, PRIMARY_CLUSTER_ALIAS)) &&
                   translatedOffsets.containsKey(remoteTopicPartition(tp2, PRIMARY_CLUSTER_ALIAS));
        }, OFFSET_SYNC_DURATION_MS, "Checkpoints were not emitted correctly to backup cluster");

        backupClient.close();
    }

    @Test
    public void testRestartReplication() throws InterruptedException {
        String consumerGroupName = "consumer-group-restart";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        String remoteTopic = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        warmUpConsumer(consumerProps);
        mm2Props.put("sync.group.offsets.enabled", "true");
        mm2Props.put("sync.group.offsets.interval.seconds", "1");
        mm2Props.put("offset.lag.max", Integer.toString(OFFSET_LAG_MAX));
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        produceMessages(primaryProducer, "test-topic-1");
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            waitForConsumingAllRecords(primaryConsumer, NUM_RECORDS_PRODUCED);
        }
        waitForConsumerGroupFullSync(backup, Collections.singletonList(remoteTopic), consumerGroupName, NUM_RECORDS_PRODUCED, OFFSET_LAG_MAX);
        restartMirrorMakerConnectors(backup, CONNECTOR_LIST);
        assertMonotonicCheckpoints(backup, "primary.checkpoints.internal");
        Thread.sleep(5000);
        produceMessages(primaryProducer, "test-topic-1");
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            waitForConsumingAllRecords(primaryConsumer, NUM_RECORDS_PRODUCED);
        }
        waitForConsumerGroupFullSync(backup, Collections.singletonList(remoteTopic), consumerGroupName, 2 * NUM_RECORDS_PRODUCED, OFFSET_LAG_MAX);
        assertMonotonicCheckpoints(backup, "primary.checkpoints.internal");
    }

    @Test
    public void testOffsetTranslationBehindReplicationFlow() throws InterruptedException {
        String consumerGroupName = "consumer-group-lagging-behind";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        String remoteTopic = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        warmUpConsumer(consumerProps);
        mm2Props.put("sync.group.offsets.enabled", "true");
        mm2Props.put("sync.group.offsets.interval.seconds", "1");
        mm2Props.put("offset.lag.max", Integer.toString(OFFSET_LAG_MAX));
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        // Produce and consume an initial batch of records to establish an initial checkpoint
        produceMessages(primaryProducer, "test-topic-1");
        warmUpConsumer(consumerProps);
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));
        Map<TopicPartition, OffsetAndMetadata> initialCheckpoints = waitForCheckpointOnAllPartitions(
                backupClient, consumerGroupName, PRIMARY_CLUSTER_ALIAS, remoteTopic);
        // Produce a large number of records to the topic, all replicated within one MM2 lifetime.
        int iterations = 100;
        for (int i = 1; i < iterations; i++) {
            produceMessages(primaryProducer, "test-topic-1");
        }
        waitForTopicCreated(backup, remoteTopic);
        assertEquals(iterations * NUM_RECORDS_PRODUCED, backup.kafka().consume(iterations * NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, remoteTopic).count(),
                "Records were not replicated to backup cluster.");
        // Once the replication has finished, we spin up the upstream consumer again and start consuming records
        ConsumerRecords<byte[], byte[]> allRecords = primary.kafka().consume(iterations * NUM_RECORDS_PRODUCED, RECORD_CONSUME_DURATION_MS, "test-topic-1");
        Map<TopicPartition, OffsetAndMetadata> partialCheckpoints;
        log.info("Initial checkpoints: {}", initialCheckpoints);
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            primaryConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            primaryConsumer.commitSync(partialOffsets(allRecords, 0.9f));
            partialCheckpoints = waitForNewCheckpointOnAllPartitions(
                    backupClient, consumerGroupName, PRIMARY_CLUSTER_ALIAS, remoteTopic, initialCheckpoints);
            log.info("Partial checkpoints: {}", partialCheckpoints);
        }

        for (TopicPartition tp : initialCheckpoints.keySet()) {
            assertTrue(initialCheckpoints.get(tp).offset() < partialCheckpoints.get(tp).offset(),
                    "Checkpoints should advance when the upstream consumer group advances");
        }

        assertMonotonicCheckpoints(backup, PRIMARY_CLUSTER_ALIAS + ".checkpoints.internal");

        Map<TopicPartition, OffsetAndMetadata> finalCheckpoints;
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            primaryConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            primaryConsumer.commitSync(partialOffsets(allRecords, 0.1f));
            finalCheckpoints = waitForNewCheckpointOnAllPartitions(
                    backupClient, consumerGroupName, PRIMARY_CLUSTER_ALIAS, remoteTopic, partialCheckpoints);
            log.info("Final checkpoints: {}", finalCheckpoints);
        }

        backupClient.close();

        for (TopicPartition tp : partialCheckpoints.keySet()) {
            assertTrue(finalCheckpoints.get(tp).offset() < partialCheckpoints.get(tp).offset(),
                    "Checkpoints should rewind when the upstream consumer group rewinds");
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> partialOffsets(ConsumerRecords<byte[], byte[]> allRecords, double fraction) {
        return allRecords.partitions()
                .stream()
                .collect(Collectors.toMap(Function.identity(), partition -> {
                    List<ConsumerRecord<byte[], byte[]>> records = allRecords.records(partition);
                    int index = (int) (records.size() * fraction);
                    return new OffsetAndMetadata(records.get(index).offset());
                }));
    }

    @Test
    public void testSyncTopicConfigs() throws InterruptedException {
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // create topic with configuration to test:
        final Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("delete.retention.ms", "1000"); // should be excluded (default value is 86400000)
        topicConfig.put("retention.bytes", "1000"); // should be included, default value is -1

        final String topic = "test-topic-with-config";
        final String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);

        primary.kafka().createTopic(topic, NUM_PARTITIONS, 1, topicConfig);
        waitForTopicCreated(backup, backupTopic);

        // alter configs on the target topic
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, backupTopic);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry("delete.retention.ms", "2000"), AlterConfigOp.OpType.SET));
        ops.add(new AlterConfigOp(new ConfigEntry("retention.bytes", "2000"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        // alter configs on target cluster
        backup.kafka().incrementalAlterConfigs(configOps);

        waitForCondition(() -> {
            String primaryConfig, backupConfig;
            primaryConfig = getTopicConfig(primary.kafka(), topic, "delete.retention.ms");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "delete.retention.ms");
            assertNotEquals(primaryConfig, backupConfig,
                    "`delete.retention.ms` should be different, because it's in exclude filter! ");
            assertEquals("2000", backupConfig, "`delete.retention.ms` should be 2000, because it's explicitly defined on the target topic! ");

            // regression test for the config that are still supposed to be replicated
            primaryConfig = getTopicConfig(primary.kafka(), topic, "retention.bytes");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "retention.bytes");
            assertEquals(primaryConfig, backupConfig,
                    "`retention.bytes` should be the same, because it isn't in exclude filter! ");
            return true;
        }, 30000, "Topic configurations were not synced");
    }

    @Test
    public void testReplicateSourceDefault() throws Exception {
        mm2Props.put(DefaultConfigPropertyFilter.USE_DEFAULTS_FROM, "source");
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // create topic with default configurations to test
        final String topic = "test-topic-with-config";
        final String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);

        primary.kafka().createTopic(topic, NUM_PARTITIONS, 1, new HashMap<>());
        waitForTopicCreated(backup, backupTopic);

        // alter target topic configurations
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, backupTopic);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry("delete.retention.ms", "2000"), AlterConfigOp.OpType.SET));
        ops.add(new AlterConfigOp(new ConfigEntry("retention.bytes", "2000"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        backup.kafka().incrementalAlterConfigs(configOps);

        waitForCondition(() -> {
            String primaryConfig, backupConfig;
            // altered configuration of the target topic should be synced with the source cluster's default
            primaryConfig = getTopicConfig(primary.kafka(), topic, "retention.bytes");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "retention.bytes");
            assertEquals(primaryConfig, backupConfig,
                    "`retention.bytes` should be the same, because the source cluster default is being used! ");
            assertEquals("-1", backupConfig,
                    "`retention.bytes` should be synced with default value!");

            // when using the source cluster's default, the excluded configuration should still not be changed
            primaryConfig = getTopicConfig(primary.kafka(), topic, "delete.retention.ms");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "delete.retention.ms");
            assertNotEquals(primaryConfig, backupConfig,
                    "`delete.retention.ms` should be different, because it's in exclude filter! ");
            assertEquals("2000", backupConfig, "`delete.retention.ms` should be 2000, because it's explicitly defined on the target topic! ");
            return true;
        }, 30000, "Topic configurations were not synced");
    }

    @Test
    public void testReplicateTargetDefault() throws Exception {
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // create topic with configuration to test:
        final Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("retention.bytes", "1000");

        final String topic = "test-topic-with-config";
        final String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);

        primary.kafka().createTopic(topic, NUM_PARTITIONS, 1, topicConfig);
        waitForTopicCreated(backup, backupTopic);

        waitForCondition(() -> {
            String primaryConfig, backupConfig;
            primaryConfig = getTopicConfig(primary.kafka(), topic, "retention.bytes");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "retention.bytes");
            assertEquals(primaryConfig, backupConfig,
                    "`retention.bytes` should be the same");
            assertEquals("1000", backupConfig,
                    "`retention.bytes` should be synced with default value!");
            return true;
        }, 30000, "Topic configurations were not synced");

        // delete the previously altered configuration of the source topic
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry("retention.bytes", "1000"), AlterConfigOp.OpType.DELETE));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        primary.kafka().incrementalAlterConfigs(configOps);

        waitForCondition(() -> {
            String backupConfig;
            // the configuration on the target topic should be changed to the target cluster's default
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "retention.bytes");
            assertEquals("-1", backupConfig,
                    "`retention.bytes` should be synced with target's default value!");
            return true;
        }, 30000, "Topic configurations were not synced");
    }

    @Test
    public void testReplicateFromLatest() throws Exception {
        // populate topic with records that should not be replicated
        String topic = "test-topic-1";
        produceMessages(primaryProducer, topic, NUM_PARTITIONS);

        // consume from the ends of topics when no committed offsets are found
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + ".consumer." + AUTO_OFFSET_RESET_CONFIG, "latest");
        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // produce some more messages to the topic, now that MM2 is running and replication should be taking place
        produceMessages(primaryProducer, topic, NUM_PARTITIONS);

        String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);
        // wait for at least the expected number of records to be replicated to the backup cluster
        backup.kafka().consume(NUM_PARTITIONS * NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, backupTopic);
        // consume all records from backup cluster
        ConsumerRecords<byte[], byte[]> replicatedRecords = backup.kafka().consumeAll(RECORD_TRANSFER_DURATION_MS, backupTopic);
        // ensure that we only replicated the records produced after startup
        replicatedRecords.partitions().forEach(topicPartition -> {
            int replicatedCount = replicatedRecords.records(topicPartition).size();
            assertEquals(NUM_RECORDS_PER_PARTITION, replicatedCount);
        });
    }

    private TopicPartition remoteTopicPartition(TopicPartition tp, String alias) {
        return new TopicPartition(remoteTopicName(tp.topic(), alias), tp.partition());
    }

    /*
     * Run tests for Exclude Filter for copying topic configurations
     */
    void createAndTestNewTopicWithConfigFilter() throws Exception {
        // create topic with configuration to test:
        final Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("delete.retention.ms", "1000"); // should be excluded (default value is 86400000)
        topicConfig.put("retention.bytes", "1000"); // should be included, default value is -1

        final String topic = "test-topic-with-config";
        final String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);

        primary.kafka().createTopic(topic, NUM_PARTITIONS, 1, topicConfig);
        waitForTopicCreated(backup, backupTopic);

        String primaryConfig, backupConfig;

        primaryConfig = getTopicConfig(primary.kafka(), topic, "delete.retention.ms");
        backupConfig = getTopicConfig(backup.kafka(), backupTopic, "delete.retention.ms");
        assertNotEquals(primaryConfig, backupConfig,
                "`delete.retention.ms` should be different, because it's in exclude filter! ");

        // regression test for the config that are still supposed to be replicated
        primaryConfig = getTopicConfig(primary.kafka(), topic, "retention.bytes");
        backupConfig = getTopicConfig(backup.kafka(), backupTopic, "retention.bytes");
        assertEquals(primaryConfig, backupConfig,
                "`retention.bytes` should be the same, because it isn't in exclude filter! ");
        assertEquals("1000", backupConfig,
                "`retention.bytes` should be the same, because it's explicitly defined! ");
    }

    /*
     * Returns expected topic name on target cluster.
     */
    String remoteTopicName(String topic, String clusterAlias) {
        return clusterAlias + "." + topic;
    }

    /*
     * launch the connectors on kafka connect cluster and check if they are running
     */
    protected static void waitUntilMirrorMakerIsRunning(EmbeddedConnectCluster connectCluster,
            List<Class<? extends Connector>> connectorClasses, MirrorMakerConfig mm2Config, 
            String primary, String backup) throws InterruptedException {
        for (Class<? extends Connector> connector : connectorClasses) {
            connectCluster.configureConnector(connector.getSimpleName(), mm2Config.connectorBaseConfig(
                new SourceAndTarget(primary, backup), connector));
        }
        
        // we wait for the connector and tasks to come up for each connector, so that when we do the
        // actual testing, we are certain that the tasks are up and running; this will prevent
        // flaky tests where the connector and tasks didn't start up in time for the tests to be run
        for (Class<? extends Connector> connector : connectorClasses) {
            connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connector.getSimpleName(), 1,
                    "Connector " + connector.getSimpleName() + " tasks did not start in time on cluster: " + connectCluster.getName());
        }
    }

    protected static void restartMirrorMakerConnectors(EmbeddedConnectCluster connectCluster, List<Class<? extends Connector>> connectorClasses)  {
        for (Class<? extends Connector> connector : connectorClasses) {
            connectCluster.restartConnectorAndTasks(connector.getSimpleName(), false, true, false);
        }
    }

    @SafeVarargs
    protected static void resumeMirrorMakerConnectors(EmbeddedConnectCluster connectCluster, Class<? extends Connector>... connectorClasses) throws InterruptedException {
        for (Class<? extends Connector> connectorClass : connectorClasses) {
            connectCluster.resumeConnector(connectorClass.getSimpleName());
        }
        for (Class<? extends Connector> connectorClass : connectorClasses) {
            String connectorName = connectorClass.getSimpleName();
            connectCluster.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                    connectorName,
                    1,
                    "Connector '" + connectorName + "' and/or task did not resume in time"
            );
        }
    }

    @SafeVarargs
    protected static void stopMirrorMakerConnectors(EmbeddedConnectCluster connectCluster, Class<? extends Connector>... connectorClasses) throws InterruptedException {
        for (Class<? extends Connector> connectorClass : connectorClasses) {
            connectCluster.stopConnector(connectorClass.getSimpleName());
        }
        for (Class<? extends Connector> connectorClass : connectorClasses) {
            String connectorName = connectorClass.getSimpleName();
            connectCluster.assertions().assertConnectorIsStopped(
                    connectorName,
                    "Connector did not stop in time"
            );
        }
    }

    protected static void alterMirrorMakerSourceConnectorOffsets(EmbeddedConnectCluster connectCluster, LongUnaryOperator alterOffset, String... topics) {
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics));
        String connectorName = MirrorSourceConnector.class.getSimpleName();

        ConnectorOffsets currentOffsets = connectCluster.connectorOffsets(connectorName);
        List<ConnectorOffset> alteredOffsetContents = currentOffsets.offsets().stream()
                .map(connectorOffset -> {
                    TopicPartition topicPartition = MirrorUtils.unwrapPartition(connectorOffset.partition());
                    if (!topicsSet.contains(topicPartition.topic())) {
                        return null;
                    }

                    Object currentOffsetObject = connectorOffset.offset().get(MirrorUtils.OFFSET_KEY);
                    if (!(currentOffsetObject instanceof Integer || currentOffsetObject instanceof Long)) {
                        throw new AssertionError("Unexpected type for offset '" + currentOffsetObject + "'; should be integer or long");
                    }

                    long currentOffset = ((Number) currentOffsetObject).longValue();
                    long alteredOffset = alterOffset.applyAsLong(currentOffset);

                    return new ConnectorOffset(
                            connectorOffset.partition(),
                            MirrorUtils.wrapOffset(alteredOffset)
                    );
                }).filter(Objects::nonNull)
                .collect(Collectors.toList());

        connectCluster.alterConnectorOffsets(connectorName, new ConnectorOffsets(alteredOffsetContents));
    }

    protected static void resetSomeMirrorMakerSourceConnectorOffsets(EmbeddedConnectCluster connectCluster, String... topics) {
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics));
        String connectorName = MirrorSourceConnector.class.getSimpleName();

        ConnectorOffsets currentOffsets = connectCluster.connectorOffsets(connectorName);
        List<ConnectorOffset> alteredOffsetContents = currentOffsets.offsets().stream()
                .map(connectorOffset -> {
                    TopicPartition topicPartition = MirrorUtils.unwrapPartition(connectorOffset.partition());
                    if (!topicsSet.contains(topicPartition.topic())) {
                        return null;
                    }

                    return new ConnectorOffset(connectorOffset.partition(), null);
                }).filter(Objects::nonNull)
                .collect(Collectors.toList());

        connectCluster.alterConnectorOffsets(connectorName, new ConnectorOffsets(alteredOffsetContents));
    }

    @SafeVarargs
    protected static void resetAllMirrorMakerConnectorOffsets(EmbeddedConnectCluster connectCluster, Class<? extends Connector>... connectorClasses) {
        for (Class<? extends Connector> connectorClass : connectorClasses) {
            String connectorName = connectorClass.getSimpleName();
            connectCluster.resetConnectorOffsets(connectorName);
            assertEquals(
                    Collections.emptyList(),
                    connectCluster.connectorOffsets(connectorName).offsets(),
                    "Offsets for connector should be completely empty after full reset"
            );
        }
    }

    /*
     * wait for the topic created on the cluster
     */
    protected static void waitForTopicCreated(EmbeddedConnectCluster cluster, String topicName) throws InterruptedException {
        try (final Admin adminClient = cluster.kafka().createAdminClient()) {
            waitForCondition(() -> {
                    Set<String> topics = adminClient.listTopics().names().get(REQUEST_TIMEOUT_DURATION_MS, TimeUnit.MILLISECONDS);
                    return topics.contains(topicName);
                }, OFFSET_SYNC_DURATION_MS,
                "Topic: " + topicName + " didn't get created in the cluster"
            );
        }
    }

    /*
     * retrieve the config value based on the input cluster, topic and config name
     */
    protected static String getTopicConfig(EmbeddedKafkaCluster cluster, String topic, String configName) throws Exception {
        try (Admin client = cluster.createAdminClient()) {
            Collection<ConfigResource> cr = Collections.singleton(
                new ConfigResource(ConfigResource.Type.TOPIC, topic));

            DescribeConfigsResult configsResult = client.describeConfigs(cr);
            Config allConfigs = (Config) configsResult.all().get().values().toArray()[0];
            return allConfigs.get(configName).value();
        }
    }

    protected void produceMessages(Producer<byte[], byte[]> producer, String topicName) {
        produceMessages(producer, TestUtils.generateRecords(topicName, NUM_RECORDS_PRODUCED));
    }

    protected void produceMessages(Producer<byte[], byte[]> producer, String topicName, int numPartitions) {
        produceMessages(producer, TestUtils.generateRecords(topicName, NUM_RECORDS_PER_PARTITION, numPartitions));
    }


    protected Producer<byte[], byte[]> initializeProducer(EmbeddedConnectCluster cluster) {
        return cluster.kafka().createProducer(Collections.emptyMap());
    }

    /**
     * Produce a batch of records with the specified producer
     * @param producer Producer to use to send records
     * @param records Records to send in one parallel batch
     */
    protected void produceMessages(Producer<byte[], byte[]> producer, List<ProducerRecord<byte[], byte[]>> records) {
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (ProducerRecord<byte[], byte[]> record : records) {
            futures.add(producer.send(record));
        }
        Timer timer = Time.SYSTEM.timer(RECORD_PRODUCE_DURATION_MS);
        try {
            for (Future<RecordMetadata> future : futures) {
                future.get(timer.remainingMs(), TimeUnit.MILLISECONDS);
                timer.update();
            }
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> waitForCheckpointOnAllPartitions(
            MirrorClient client, String consumerGroupName, String remoteClusterAlias, String topicName
    ) throws InterruptedException {
        return waitForNewCheckpointOnAllPartitions(client, consumerGroupName, remoteClusterAlias, topicName, Collections.emptyMap());
    }

    protected static Map<TopicPartition, OffsetAndMetadata> waitForNewCheckpointOnAllPartitions(
                MirrorClient client, String consumerGroupName, String remoteClusterAlias, String topicName,
                Map<TopicPartition, OffsetAndMetadata> lastCheckpoint
    ) throws InterruptedException {
        AtomicReference<Map<TopicPartition, OffsetAndMetadata>> ret = new AtomicReference<>();
        waitForCondition(
                () -> {
                    Map<TopicPartition, OffsetAndMetadata> offsets = client.remoteConsumerOffsets(
                            consumerGroupName, remoteClusterAlias, Duration.ofMillis(3000));
                    for (int i = 0; i < NUM_PARTITIONS; i++) {
                        TopicPartition tp = new TopicPartition(topicName, i);
                        if (!offsets.containsKey(tp)) {
                            log.info("Checkpoint is missing for {}: {}-{}", consumerGroupName, topicName, i);
                            return false;
                        } else if (lastCheckpoint.containsKey(tp) && lastCheckpoint.get(tp).equals(offsets.get(tp))) {
                            log.info("Checkpoint is the same as previous checkpoint");
                            return false;
                        }
                    }
                    ret.set(offsets);
                    return true;
                },
                CHECKPOINT_DURATION_MS,
                String.format(
                        "Offsets for consumer group %s not translated from %s for topic %s",
                        consumerGroupName,
                        remoteClusterAlias,
                        topicName
                )
        );
        return ret.get();
    }

    /*
     * given consumer group, topics and expected number of records, make sure the consumer group
     * offsets are eventually synced to the expected offset numbers
     */
    private static <T> void waitForConsumerGroupFullSync(
            EmbeddedConnectCluster connect,
            List<String> topics,
            String consumerGroupId,
            int numRecords,
            int offsetLagMax
    ) throws InterruptedException {
        int expectedRecords = numRecords * topics.size();
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("isolation.level", "read_committed");
        consumerProps.put("auto.offset.reset", "earliest");
        Map<TopicPartition, Long> lastOffset = new HashMap<>();
        try (Consumer<byte[], byte[]> consumer = connect.kafka().createConsumerAndSubscribeTo(consumerProps, topics.toArray(new String[0]))) {
            final AtomicInteger totalConsumedRecords = new AtomicInteger(0);
            waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_TIMEOUT_MS);
                records.forEach(record -> lastOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset()));
                return expectedRecords == totalConsumedRecords.addAndGet(records.count());
            }, RECORD_CONSUME_DURATION_MS, "Consumer cannot consume all records in time");
        }
        try (Admin adminClient = connect.kafka().createAdminClient()) {
            List<TopicPartition> tps = new ArrayList<>(NUM_PARTITIONS * topics.size());
            for (int partitionIndex = 0; partitionIndex < NUM_PARTITIONS; partitionIndex++) {
                for (String topic : topics) {
                    tps.add(new TopicPartition(topic, partitionIndex));
                }
            }

            waitForCondition(() -> {
                Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets =
                        adminClient.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get();
                Map<TopicPartition, OffsetSpec> endOffsetRequest = tps.stream()
                        .collect(Collectors.toMap(Function.identity(), ignored -> OffsetSpec.latest()));
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                        adminClient.listOffsets(endOffsetRequest).all().get();

                for (TopicPartition tp : tps) {
                    assertTrue(consumerGroupOffsets.containsKey(tp),
                            "TopicPartition " + tp + " does not have translated offsets");
                    long offset = consumerGroupOffsets.get(tp).offset();
                    assertTrue(offset > lastOffset.get(tp) - offsetLagMax,
                            "TopicPartition " + tp + " does not have fully-translated offsets: "
                                    + offset + " is not close enough to " + lastOffset.get(tp)
                                    + " (strictly more than " + (lastOffset.get(tp) - offsetLagMax) + ")");
                    assertTrue(offset <= endOffsets.get(tp).offset(),
                            "TopicPartition " + tp + " has downstream offsets beyond the log end, this would lead to negative lag metrics");
                }
                return true;
            }, OFFSET_SYNC_DURATION_MS, "Consumer group offset sync is not complete in time");
        }
    }

    private static void assertMonotonicCheckpoints(EmbeddedConnectCluster cluster, String checkpointTopic) {
        TopicPartition checkpointTopicPartition = new TopicPartition(checkpointTopic, 0);
        try (Consumer<byte[], byte[]> backupConsumer = cluster.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "auto.offset.reset", "earliest"), checkpointTopic)) {
            Map<String, Map<TopicPartition, Checkpoint>> checkpointsByGroup = new HashMap<>();
            long deadline = System.currentTimeMillis() + CHECKPOINT_DURATION_MS;
            do {
                ConsumerRecords<byte[], byte[]> records = backupConsumer.poll(Duration.ofSeconds(1L));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    Checkpoint checkpoint = Checkpoint.deserializeRecord(record);
                    Map<TopicPartition, Checkpoint> lastCheckpoints = checkpointsByGroup.computeIfAbsent(
                            checkpoint.consumerGroupId(),
                            ignored -> new HashMap<>());
                    Checkpoint lastCheckpoint = lastCheckpoints.getOrDefault(checkpoint.topicPartition(), checkpoint);
                    assertTrue(checkpoint.downstreamOffset() >= lastCheckpoint.downstreamOffset(),
                            "Checkpoint was non-monotonic for "
                                    + checkpoint.consumerGroupId()
                                    + ": "
                                    + checkpoint.topicPartition());
                    lastCheckpoints.put(checkpoint.topicPartition(), checkpoint);
                }
            } while (backupConsumer.currentLag(checkpointTopicPartition).orElse(1) > 0 && System.currentTimeMillis() < deadline);
            assertEquals(0, backupConsumer.currentLag(checkpointTopicPartition).orElse(1), "Unable to read all checkpoints within " + CHECKPOINT_DURATION_MS + "ms");
        }
    }

    private static void assertDownstreamRedeliveriesBoundedByMaxLag(Consumer<byte[], byte[]> targetConsumer, int offsetLagMax) {
        ConsumerRecords<byte[], byte[]> records = targetConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
        // After a full sync, there should be at most offset.lag.max records per partition consumed by both upstream and downstream consumers.
        for (TopicPartition tp : records.partitions()) {
            int count = records.records(tp).size();
            assertTrue(count < offsetLagMax,  "downstream consumer is re-reading more than " + offsetLagMax + " records from" + tp);
        }
    }

    /*
     * make sure the consumer to consume expected number of records
     */
    private static <T> void waitForConsumingAllRecords(Consumer<T, T> consumer, int numExpectedRecords)
            throws InterruptedException {
        final AtomicInteger totalConsumedRecords = new AtomicInteger(0);
        waitForCondition(() -> {
            ConsumerRecords<T, T> records = consumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            return numExpectedRecords == totalConsumedRecords.addAndGet(records.count());
        }, RECORD_CONSUME_DURATION_MS, "Consumer cannot consume all records in time");
        consumer.commitSync();
    }
   
    /*
     * MM2 config to use in integration tests
     */
    private static Map<String, String> basicMM2Config() {
        Map<String, String> mm2Props = new HashMap<>();
        mm2Props.put("clusters", PRIMARY_CLUSTER_ALIAS + ", " + BACKUP_CLUSTER_ALIAS);
        mm2Props.put("max.tasks", "10");
        mm2Props.put("groups", "consumer-group-.*");
        mm2Props.put("sync.topic.acls.enabled", "false");
        mm2Props.put("emit.checkpoints.interval.seconds", String.valueOf(CHECKPOINT_INTERVAL_DURATION_MS / 1000));
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
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + ".offset.flush.interval.ms", "5000");
        mm2Props.put(BACKUP_CLUSTER_ALIAS + ".offset.flush.interval.ms", "5000");
        return mm2Props;
    }
    
    private void createTopics() {
        // to verify topic config will be sync-ed across clusters
        Map<String, String> topicConfig = Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        Map<String, String> emptyMap = Collections.emptyMap();

        // increase admin client request timeout value to make the tests reliable.
        Map<String, Object> adminClientConfig = Collections.singletonMap(
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_DURATION_MS);

        // create these topics before starting the connectors, so we don't need to wait for discovery
        primary.kafka().createTopic("test-topic-no-checkpoints", 1, 1, emptyMap, adminClientConfig);
        primary.kafka().createTopic("test-topic-1", NUM_PARTITIONS, 1, topicConfig, adminClientConfig);
        backup.kafka().createTopic("test-topic-1", NUM_PARTITIONS, 1, emptyMap, adminClientConfig);
        primary.kafka().createTopic("heartbeats", 1, 1, emptyMap, adminClientConfig);
        backup.kafka().createTopic("heartbeats", 1, 1, emptyMap, adminClientConfig);

        // This can speed up some test cases
        if (createReplicatedTopicsUpfront) {
            primary.kafka().createTopic(remoteTopicName("test-topic-1", BACKUP_CLUSTER_ALIAS), 1, 1, emptyMap, adminClientConfig);
            backup.kafka().createTopic(remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS), 1, 1, emptyMap, adminClientConfig);
        }
    }

    /*
     * Generate some consumer activity on both clusters to ensure the checkpoint connector always starts promptly
     */
    protected void warmUpConsumer(Map<String, Object> consumerProps) {
        try (Consumer<byte[], byte[]> dummyConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            dummyConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            dummyConsumer.commitSync();
        }
        try (Consumer<byte[], byte[]> dummyConsumer = backup.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            dummyConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            dummyConsumer.commitSync();
        }
    }

    /*
     * making sure the topic isn't created on the cluster
     */
    private static void topicShouldNotBeCreated(EmbeddedConnectCluster cluster, String topicName) throws InterruptedException {
        try (final Admin adminClient = cluster.kafka().createAdminClient()) {
            waitForCondition(() ->
                    !adminClient.listTopics().names()
                        .get(REQUEST_TIMEOUT_DURATION_MS, TimeUnit.MILLISECONDS)
                        .contains(topicName), TOPIC_SYNC_DURATION_MS,
                "Topic: " + topicName + " get created on cluster: " + cluster.getName()
            );
        }
    }

    /*
     * wait for the topic created on the cluster
     */
    protected static void waitForTopicPartitionCreated(EmbeddedConnectCluster cluster, String topicName, int totalNumPartitions) throws InterruptedException {
        try (final Admin adminClient = cluster.kafka().createAdminClient()) {
            waitForCondition(() -> adminClient.describeTopics(Collections.singleton(topicName)).allTopicNames().get()
                    .get(topicName).partitions().size() == totalNumPartitions, TOPIC_SYNC_DURATION_MS,
                "Topic: " + topicName + "'s partitions didn't get created on cluster: " + cluster.getName()
            );
        }
    }
}
