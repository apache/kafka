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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.mirror.MirrorClient;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.MirrorCheckpointConnector;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.connect.util.clusters.UngracefulShutdownException;
import static org.apache.kafka.test.TestUtils.waitForCondition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.kafka.connect.mirror.TestUtils.generateRecords;

/**
 * Tests MM2 replication and failover/failback logic.
 *
 * MM2 is configured with active/active replication between two Kafka clusters. Tests validate that
 * records sent to either cluster arrive at the other cluster. Then, a consumer group is migrated from
 * one cluster to the other and back. Tests validate that consumer offsets are translated and replicated
 * between clusters during this failover and failback.
 */
@Tag("integration")
public abstract class MirrorConnectorsIntegrationBaseTest {
    private static final Logger log = LoggerFactory.getLogger(MirrorConnectorsIntegrationBaseTest.class);
    
    private static final int NUM_RECORDS_PER_PARTITION = 10;
    private static final int NUM_PARTITIONS = 10;
    private static final int NUM_RECORDS_PRODUCED = NUM_PARTITIONS * NUM_RECORDS_PER_PARTITION;
    private static final int RECORD_TRANSFER_DURATION_MS = 30_000;
    private static final int CHECKPOINT_DURATION_MS = 20_000;
    private static final int RECORD_CONSUME_DURATION_MS = 20_000;
    private static final int OFFSET_SYNC_DURATION_MS = 30_000;
    private static final int TOPIC_SYNC_DURATION_MS = 60_000;
    private static final int REQUEST_TIMEOUT_DURATION_MS = 60_000;
    private static final int NUM_WORKERS = 3;
    private static final Duration CONSUMER_POLL_TIMEOUT_MS = Duration.ofMillis(500);
    protected static final String PRIMARY_CLUSTER_ALIAS = "primary";
    protected static final String BACKUP_CLUSTER_ALIAS = "backup";
    private static final List<Class<? extends Connector>> CONNECTOR_LIST = 
            Arrays.asList(MirrorSourceConnector.class, MirrorCheckpointConnector.class, MirrorHeartbeatConnector.class);

    private volatile boolean shuttingDown;
    protected Map<String, String> mm2Props = new HashMap<>();
    private MirrorMakerConfig mm2Config; 
    private EmbeddedConnectCluster primary;
    private EmbeddedConnectCluster backup;
    
    private Exit.Procedure exitProcedure;
    private Exit.Procedure haltProcedure;
    
    protected Properties primaryBrokerProps = new Properties();
    protected Properties backupBrokerProps = new Properties();
    protected Map<String, String> primaryWorkerProps = new HashMap<>();
    protected Map<String, String> backupWorkerProps = new HashMap<>();
    
    @BeforeEach
    public void startClusters() throws Exception {
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
                .build();

        backup = new EmbeddedConnectCluster.Builder()
                .name(BACKUP_CLUSTER_ALIAS + "-connect-cluster")
                .numWorkers(NUM_WORKERS)
                .numBrokers(1)
                .brokerProps(backupBrokerProps)
                .workerProps(backupWorkerProps)
                .maskExitProcedures(false)
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

        waitForTopicCreated(backup, "mm2-status.primary.internal");
        waitForTopicCreated(backup, "mm2-offsets.primary.internal");
        waitForTopicCreated(backup, "mm2-configs.primary.internal");

        createTopics();
 
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
    public void testReplication() throws Exception {
        produceMessages(primary, "test-topic-1");
        produceMessages(backup, "test-topic-1");
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = new HashMap<String, Object>() {{
                put("group.id", consumerGroupName);
                put("auto.offset.reset", "latest");
            }};
        // warm up consumers before starting the connectors so we don't need to wait for discovery
        warmUpConsumer(consumerProps);
        
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        waitUntilMirrorMakerIsRunning(primary, CONNECTOR_LIST, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS); 

        MirrorClient primaryClient = new MirrorClient(mm2Config.clientConfig(PRIMARY_CLUSTER_ALIAS));
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(primary, "backup.test-topic-1");
        waitForTopicCreated(backup, "primary.test-topic-1");
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, getTopicConfig(backup.kafka(), "primary.test-topic-1", TopicConfig.CLEANUP_POLICY_CONFIG),
                "topic config was not synced");
        
        assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
            "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "primary.test-topic-1").count(),
            "Records were not replicated to backup cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
            "Records were not produced to backup cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "backup.test-topic-1").count(),
            "Records were not replicated to primary cluster.");
        
        assertEquals(NUM_RECORDS_PRODUCED * 2, primary.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, "backup.test-topic-1", "test-topic-1").count(),
            "Primary cluster doesn't have all records from both clusters.");
        assertEquals(NUM_RECORDS_PRODUCED * 2, backup.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, "primary.test-topic-1", "test-topic-1").count(),
            "Backup cluster doesn't have all records from both clusters.");
        
        assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
            "Heartbeats were not emitted to primary cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
            "Heartbeats were not emitted to backup cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "primary.heartbeats").count() > 0,
            "Heartbeats were not replicated downstream to backup cluster.");
        assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "backup.heartbeats").count() > 0,
            "Heartbeats were not replicated downstream to primary cluster.");
        
        assertTrue(backupClient.upstreamClusters().contains(PRIMARY_CLUSTER_ALIAS), "Did not find upstream primary cluster.");
        assertEquals(1, backupClient.replicationHops(PRIMARY_CLUSTER_ALIAS), "Did not calculate replication hops correctly.");
        assertTrue(primaryClient.upstreamClusters().contains(BACKUP_CLUSTER_ALIAS), "Did not find upstream backup cluster.");
        assertEquals(1, primaryClient.replicationHops(BACKUP_CLUSTER_ALIAS), "Did not calculate replication hops correctly.");
        assertTrue(backup.kafka().consume(1, CHECKPOINT_DURATION_MS, "primary.checkpoints.internal").count() > 0,
            "Checkpoints were not emitted downstream to backup cluster.");

        Map<TopicPartition, OffsetAndMetadata> backupOffsets = backupClient.remoteConsumerOffsets(consumerGroupName, PRIMARY_CLUSTER_ALIAS,
            Duration.ofMillis(CHECKPOINT_DURATION_MS));

        assertTrue(backupOffsets.containsKey(
            new TopicPartition("primary.test-topic-1", 0)), "Offsets not translated downstream to backup cluster. Found: " + backupOffsets);

        // Failover consumer group to backup cluster.
        try (Consumer<byte[], byte[]> primaryConsumer = backup.kafka().createConsumer(Collections.singletonMap("group.id", consumerGroupName))) {
            primaryConsumer.assign(backupOffsets.keySet());
            backupOffsets.forEach(primaryConsumer::seek);
            primaryConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            primaryConsumer.commitAsync();

            assertTrue(primaryConsumer.position(new TopicPartition("primary.test-topic-1", 0)) > 0, "Consumer failedover to zero offset.");
            assertTrue(primaryConsumer.position(
                new TopicPartition("primary.test-topic-1", 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedover beyond expected offset.");
            assertTrue(primary.kafka().consume(1, CHECKPOINT_DURATION_MS, "backup.checkpoints.internal").count() > 0,
                "Checkpoints were not emitted upstream to primary cluster.");
        }

        waitForCondition(() -> primaryClient.remoteConsumerOffsets(consumerGroupName, BACKUP_CLUSTER_ALIAS,
            Duration.ofMillis(CHECKPOINT_DURATION_MS)).containsKey(new TopicPartition("backup.test-topic-1", 0)), CHECKPOINT_DURATION_MS, "Offsets not translated downstream to primary cluster.");

        waitForCondition(() -> primaryClient.remoteConsumerOffsets(consumerGroupName, BACKUP_CLUSTER_ALIAS,
            Duration.ofMillis(CHECKPOINT_DURATION_MS)).containsKey(new TopicPartition("test-topic-1", 0)), CHECKPOINT_DURATION_MS, "Offsets not translated upstream to primary cluster.");

        Map<TopicPartition, OffsetAndMetadata> primaryOffsets = primaryClient.remoteConsumerOffsets(consumerGroupName, BACKUP_CLUSTER_ALIAS,
                Duration.ofMillis(CHECKPOINT_DURATION_MS));
 
        primaryClient.close();
        backupClient.close();
        
        // Failback consumer group to primary cluster
        try (Consumer<byte[], byte[]> backupConsumer = primary.kafka().createConsumer(Collections.singletonMap("group.id", consumerGroupName))) {
            backupConsumer.assign(primaryOffsets.keySet());
            primaryOffsets.forEach(backupConsumer::seek);
            backupConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
            backupConsumer.commitAsync();
        
            assertTrue(backupConsumer.position(new TopicPartition("test-topic-1", 0)) > 0, "Consumer failedback to zero upstream offset.");
            assertTrue(backupConsumer.position(new TopicPartition("backup.test-topic-1", 0)) > 0, "Consumer failedback to zero downstream offset.");
            assertTrue(backupConsumer.position(
                new TopicPartition("test-topic-1", 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedback beyond expected upstream offset.");
            assertTrue(backupConsumer.position(
                new TopicPartition("backup.test-topic-1", 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedback beyond expected downstream offset.");
        }
      
        // create more matching topics
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        backup.kafka().createTopic("test-topic-3", NUM_PARTITIONS);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(backup, "primary.test-topic-2");
        waitForTopicCreated(primary, "backup.test-topic-3");

        // only produce messages to the first partition
        produceMessages(primary, "test-topic-2", 1);
        produceMessages(backup, "test-topic-3", 1);
        
        // expect total consumed messages equals to NUM_RECORDS_PER_PARTITION
        assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-2").count(),
            "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-3").count(),
            "Records were not produced to backup cluster.");

        assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, "backup.test-topic-3").count(),
            "New topic was not replicated to primary cluster.");
        assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, "primary.test-topic-2").count(),
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

        // consume all records from backup cluster
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(consumerProps, 
                PRIMARY_CLUSTER_ALIAS + "." + topic)) {
            waitForConsumingAllRecords(backupConsumer, expectedRecords);
        }
        
        try (Admin backupClient = backup.kafka().createAdminClient()) {
            // retrieve the consumer group offset from backup cluster
            Map<TopicPartition, OffsetAndMetadata> remoteOffsets =
                backupClient.listConsumerGroupOffsets(consumerGroupName).partitionsToOffsetAndMetadata().get();

            // pinpoint the offset of the last partition which does not receive records
            OffsetAndMetadata offset = remoteOffsets.get(new TopicPartition(PRIMARY_CLUSTER_ALIAS + "." + topic, NUM_PARTITIONS - 1));
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

        // make sure the topic is created in the other cluster
        waitForTopicCreated(primary, "backup.test-topic-1");
        waitForTopicCreated(backup, "primary.test-topic-1");
        // create a consumer at backup cluster with same consumer group Id to consume 1 topic
        Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(
            consumerProps, "primary.test-topic-1");

        waitForConsumerGroupOffsetSync(backup, backupConsumer, Collections.singletonList("primary.test-topic-1"), 
            consumerGroupName, NUM_RECORDS_PRODUCED);

        ConsumerRecords<byte[], byte[]> records = backupConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);

        // the size of consumer record should be zero, because the offsets of the same consumer group
        // have been automatically synchronized from primary to backup by the background job, so no
        // more records to consume from the replicated topic by the same consumer group at backup cluster
        assertEquals(0, records.count(), "consumer record size is not zero");

        // now create a new topic in primary cluster
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        // make sure the topic is created in backup cluster
        waitForTopicCreated(backup, "primary.test-topic-2");

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
            "group.id", consumerGroupName), "primary.test-topic-1", "primary.test-topic-2");

        waitForConsumerGroupOffsetSync(backup, backupConsumer, Arrays.asList("primary.test-topic-1", "primary.test-topic-2"), 
            consumerGroupName, NUM_RECORDS_PRODUCED);

        records = backupConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
        // similar reasoning as above, no more records to consume by the same consumer group at backup cluster
        assertEquals(0, records.count(), "consumer record size is not zero");
        backupConsumer.close();
    }
    
    /*
     * launch the connectors on kafka connect cluster and check if they are running
     */
    private static void waitUntilMirrorMakerIsRunning(EmbeddedConnectCluster connectCluster, 
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

    /*
     * wait for the topic created on the cluster
     */
    private static void waitForTopicCreated(EmbeddedConnectCluster cluster, String topicName) throws InterruptedException {
        try (final Admin adminClient = cluster.kafka().createAdminClient()) {
            waitForCondition(() -> adminClient.listTopics().names().get().contains(topicName), TOPIC_SYNC_DURATION_MS,
                "Topic: " + topicName + " didn't get created on cluster: " + cluster.getName()
            );
        }
    }

    /*
     * delete all topics of the input kafka cluster
     */
    private static void deleteAllTopics(EmbeddedKafkaCluster cluster) throws Exception {
        try (final Admin adminClient = cluster.createAdminClient()) {
            Set<String> topicsToBeDeleted = adminClient.listTopics().names().get();
            log.debug("Deleting topics: {} ", topicsToBeDeleted);
            adminClient.deleteTopics(topicsToBeDeleted).all().get();
        }
    }
    
    /*
     * retrieve the config value based on the input cluster, topic and config name
     */
    private static String getTopicConfig(EmbeddedKafkaCluster cluster, String topic, String configName) throws Exception {
        try (Admin client = cluster.createAdminClient()) {
            Collection<ConfigResource> cr = Collections.singleton(
                new ConfigResource(ConfigResource.Type.TOPIC, topic));

            DescribeConfigsResult configsResult = client.describeConfigs(cr);
            Config allConfigs = (Config) configsResult.all().get().values().toArray()[0];
            return allConfigs.get(configName).value();
        }
    }
    
    /*
     *  produce messages to the cluster and topic 
     */
    protected void produceMessages(EmbeddedConnectCluster cluster, String topicName) {
        Map<String, String> recordSent = generateRecords(NUM_RECORDS_PRODUCED);
        for (Map.Entry<String, String> entry : recordSent.entrySet()) {
            cluster.kafka().produce(topicName, entry.getKey(), entry.getValue());
        }
    }

    /*
     * produce messages to the cluster and topic partition less than numPartitions 
     */
    protected void produceMessages(EmbeddedConnectCluster cluster, String topicName, int numPartitions) {
        int cnt = 0;
        for (int r = 0; r < NUM_RECORDS_PER_PARTITION; r++)
            for (int p = 0; p < numPartitions; p++)
                cluster.kafka().produce(topicName, p, "key", "value-" + cnt++);
    }
    
    /*
     * given consumer group, topics and expected number of records, make sure the consumer group
     * offsets are eventually synced to the expected offset numbers
     */
    private static <T> void waitForConsumerGroupOffsetSync(EmbeddedConnectCluster connect, 
            Consumer<T, T> consumer, List<String> topics, String consumerGroupId, int numRecords)
            throws InterruptedException {
        try (Admin adminClient = connect.kafka().createAdminClient()) {
            List<TopicPartition> tps = new ArrayList<>(NUM_PARTITIONS * topics.size());
            for (int partitionIndex = 0; partitionIndex < NUM_PARTITIONS; partitionIndex++) {
                for (String topic : topics) {
                    tps.add(new TopicPartition(topic, partitionIndex));
                }
            }
            long expectedTotalOffsets = numRecords * topics.size();

            waitForCondition(() -> {
                Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets =
                    adminClient.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get();
                long consumerGroupOffsetTotal = consumerGroupOffsets.values().stream()
                    .mapToLong(OffsetAndMetadata::offset).sum();

                Map<TopicPartition, Long> offsets = consumer.endOffsets(tps, CONSUMER_POLL_TIMEOUT_MS);
                long totalOffsets = offsets.values().stream().mapToLong(l -> l).sum();

                // make sure the consumer group offsets are synced to expected number
                return totalOffsets == expectedTotalOffsets && consumerGroupOffsetTotal > 0;
            }, OFFSET_SYNC_DURATION_MS, "Consumer group offset sync is not complete in time");
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
        mm2Props.put("topics", "test-topic-.*, primary.test-topic-.*, backup.test-topic-.*");
        mm2Props.put("groups", "consumer-group-.*");
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".enabled", "true");
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "true");
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
        
        return mm2Props;
    }
    
    private void createTopics() {
        // to verify topic config will be sync-ed across clusters
        Map<String, String> topicConfig = Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        Map<String, String> emptyMap = Collections.emptyMap();

        // increase admin client request timeout value to make the tests reliable.
        Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_DURATION_MS);

        // create these topics before starting the connectors so we don't need to wait for discovery
        primary.kafka().createTopic("test-topic-1", NUM_PARTITIONS, 1, topicConfig, adminClientConfig);
        primary.kafka().createTopic("backup.test-topic-1", 1, 1, emptyMap, adminClientConfig);
        primary.kafka().createTopic("heartbeats", 1, 1, emptyMap, adminClientConfig);
        backup.kafka().createTopic("test-topic-1", NUM_PARTITIONS, 1, emptyMap, adminClientConfig);
        backup.kafka().createTopic("primary.test-topic-1", 1, 1, emptyMap, adminClientConfig);
        backup.kafka().createTopic("heartbeats", 1, 1, emptyMap, adminClientConfig);
    }

    /*
     * Generate some consumer activity on both clusters to ensure the checkpoint connector always starts promptly
     */
    private void warmUpConsumer(Map<String, Object> consumerProps) throws InterruptedException {
        Consumer<byte[], byte[]> dummyConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1");
        dummyConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
        dummyConsumer.commitSync();
        dummyConsumer.close();
        dummyConsumer = backup.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1");
        dummyConsumer.poll(CONSUMER_POLL_TIMEOUT_MS);
        dummyConsumer.commitSync();
        dummyConsumer.close();
    }
}
