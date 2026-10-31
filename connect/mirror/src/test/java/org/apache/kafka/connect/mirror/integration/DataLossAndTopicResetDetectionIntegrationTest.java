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
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.mirror.DataLossException;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.TopicResetException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for MirrorSourceTask's opt-in fail-fast detection of data loss (records
 * purged by the source topic's retention policy before they could be replicated) and topic reset
 * (source topic deleted and recreated), gated behind
 * {@link MirrorSourceConfig#DATA_LOSS_AND_TOPIC_RESET_DETECTION_ENABLED}.
 *
 * <p>This suite sets up its own minimal, single-direction, single-connector two-cluster fixture
 * rather than extending {@link MirrorConnectorsIntegrationBaseTest}: that shared fixture's
 * default topics and its battery of unrelated inherited tests aren't compatible with running
 * every replicated topic under strict ({@code auto.offset.reset=none}) semantics.
 */
@Tag("integration")
public class DataLossAndTopicResetDetectionIntegrationTest {

    private static final String PRIMARY_CLUSTER_ALIAS = "primary";
    private static final String BACKUP_CLUSTER_ALIAS = "backup";
    private static final String CONNECTOR_NAME = MirrorSourceConnector.class.getSimpleName();
    private static final long RECORD_TRANSFER_DURATION_MS = 30_000;

    private EmbeddedConnectCluster primary;
    private EmbeddedConnectCluster backup;
    private Producer<byte[], byte[]> primaryProducer;
    private MirrorMakerConfig mm2Config;

    @BeforeEach
    public void startClusters() {
        Map<String, String> mm2Props = new HashMap<>();
        mm2Props.put("clusters", PRIMARY_CLUSTER_ALIAS + ", " + BACKUP_CLUSTER_ALIAS);
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".enabled", "true");
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics", "test-topic-.*");
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + "."
                + MirrorSourceConfig.DATA_LOSS_AND_TOPIC_RESET_DETECTION_ENABLED, "true");
        mm2Props.put("replication.factor", "1");
        mm2Props.put("checkpoints.topic.replication.factor", "1");
        mm2Props.put("heartbeats.topic.replication.factor", "1");
        mm2Props.put("offset-syncs.topic.replication.factor", "1");
        mm2Props.put("offset.storage.replication.factor", "1");
        mm2Props.put("status.storage.replication.factor", "1");
        mm2Props.put("config.storage.replication.factor", "1");

        mm2Config = new MirrorMakerConfig(mm2Props);
        Map<String, String> primaryWorkerProps = mm2Config.workerConfig(new SourceAndTarget(BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS));
        Map<String, String> backupWorkerProps = mm2Config.workerConfig(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS));

        Properties brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", "false");

        primary = new EmbeddedConnectCluster.Builder()
                .name(PRIMARY_CLUSTER_ALIAS + "-connect-cluster")
                .numWorkers(1)
                .numBrokers(1)
                .brokerProps(brokerProps)
                .workerProps(primaryWorkerProps)
                .build();
        backup = new EmbeddedConnectCluster.Builder()
                .name(BACKUP_CLUSTER_ALIAS + "-connect-cluster")
                .numWorkers(1)
                .numBrokers(1)
                .brokerProps(brokerProps)
                .workerProps(backupWorkerProps)
                .build();

        primary.start();
        backup.start();

        primaryProducer = primary.kafka().createProducer(Map.of());

        // The connector's tasks need each cluster's bootstrap.servers, which are only known once
        // the embedded brokers have started; rebuild the config now that they're set.
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + ".bootstrap.servers", primary.kafka().bootstrapServers());
        mm2Props.put(BACKUP_CLUSTER_ALIAS + ".bootstrap.servers", backup.kafka().bootstrapServers());
        mm2Config = new MirrorMakerConfig(mm2Props);
    }

    @AfterEach
    public void shutdownClusters() {
        Utils.closeQuietly(primaryProducer, "primary producer");
        if (primary != null) {
            Utils.closeQuietly(primary::stop, "primary connect cluster");
        }
        if (backup != null) {
            Utils.closeQuietly(backup::stop, "backup connect cluster");
        }
    }

    @Test
    public void testDataLossDetectedOnRetentionTruncation() throws Exception {
        String topic = "test-topic-dataloss";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        primary.kafka().createTopic(topic, 1);

        produce(topic, 10);

        backup.configureConnector(CONNECTOR_NAME, mm2Config.connectorBaseConfig(
                new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS), MirrorSourceConnector.class));
        backup.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, 1,
                "Connector " + CONNECTOR_NAME + " tasks did not start in time");

        String remoteTopic = PRIMARY_CLUSTER_ALIAS + "." + topic;
        backup.kafka().consume(10, RECORD_TRANSFER_DURATION_MS, remoteTopic);

        // Stop the task from polling so that the next few records it produces are purged before
        // it has a chance to replicate them.
        backup.pauseConnector(CONNECTOR_NAME);
        backup.assertions().assertConnectorAndExactlyNumTasksArePaused(CONNECTOR_NAME, 1,
                "Connector did not pause in time");

        produce(topic, 5);

        // Purge past the offset the paused task will resume from, simulating retention deleting
        // records before they were replicated.
        try (Admin admin = primary.kafka().createAdminClient()) {
            admin.deleteRecords(Map.of(topicPartition, RecordsToDelete.beforeOffset(12L))).all().get();
        }

        backup.resumeConnector(CONNECTOR_NAME);

        assertTaskFailedWithException(DataLossException.class);
    }

    @Test
    public void testTopicResetDetectedOnTopicRecreation() throws Exception {
        String topic = "test-topic-reset";
        primary.kafka().createTopic(topic, 1);

        produce(topic, 10);

        backup.configureConnector(CONNECTOR_NAME, mm2Config.connectorBaseConfig(
                new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS), MirrorSourceConnector.class));
        backup.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, 1,
                "Connector " + CONNECTOR_NAME + " tasks did not start in time");

        String remoteTopic = PRIMARY_CLUSTER_ALIAS + "." + topic;
        backup.kafka().consume(10, RECORD_TRANSFER_DURATION_MS, remoteTopic);

        backup.pauseConnector(CONNECTOR_NAME);
        backup.assertions().assertConnectorAndExactlyNumTasksArePaused(CONNECTOR_NAME, 1,
                "Connector did not pause in time");

        // Simulate an irregular system reset: the topic is deleted and recreated, so its log
        // starts fresh at offset 0 while the paused task still expects to resume from offset 10.
        primary.kafka().deleteTopic(topic);
        primary.kafka().createTopic(topic, 1);

        backup.resumeConnector(CONNECTOR_NAME);

        assertTaskFailedWithException(TopicResetException.class);
    }

    private void produce(String topic, int numMessages) throws Exception {
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            primaryProducer.send(new ProducerRecord<>(topic, key.getBytes(), value.getBytes())).get();
        }
    }

    private void assertTaskFailedWithException(Class<? extends Exception> expectedExceptionType) throws InterruptedException {
        backup.assertions().assertConnectorIsRunningAndTasksHaveFailed(CONNECTOR_NAME, 1,
                "Task should have failed with " + expectedExceptionType.getSimpleName());
        ConnectorStateInfo status = backup.connectorStatus(CONNECTOR_NAME);
        String trace = status.tasks().get(0).trace();
        assertTrue(trace != null && trace.contains(expectedExceptionType.getSimpleName()),
                "Expected task failure trace to contain " + expectedExceptionType.getSimpleName() + " but was: " + trace);
    }
}
