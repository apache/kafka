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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.DataLossException;
import org.apache.kafka.connect.mirror.MirrorSourceConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class MirrorConnectorsIntegrationOffsetValidationTest extends MirrorConnectorsIntegrationBaseTest {

    private static final String TOPIC = "test-topic-1";
    private static final int TASK_FAILURE_DURATION_MS = 60_000;
    private static final int ADMIN_REQUEST_TIMEOUT_MS = 60_000;

    @BeforeEach
    @Override
    public void startClusters() throws Exception {
        replicateBackupToPrimary = false;
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put("topics", "test-topic-.*");
        additionalConfig.put(MirrorSourceConfig.OFFSET_VALIDATION_ENABLED, "true");
        super.startClusters(additionalConfig);
    }

    @Test
    public void testDataLossDetectedWhenUnreplicatedRecordsArePurged() throws Exception {
        produceMessages(primaryProducer, TOPIC);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS,
                remoteTopicName(TOPIC, PRIMARY_CLUSTER_ALIAS));

        stopMirrorMakerConnectors(backup, MirrorSourceConnector.class);
        alterMirrorMakerSourceConnectorOffsets(backup, offset -> 0L, TOPIC);
        deleteRecordsBefore();
        backup.resumeConnector(MirrorSourceConnector.class.getSimpleName());

        assertSourceTaskFailedWith(backup, DataLossException.class.getName());
    }

    @Test
    public void testTopicResetRecoversAndReplicationResumesWithoutRestart() throws Exception {
        produceMessages(primaryProducer, TOPIC);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        String remoteTopic = remoteTopicName(TOPIC, PRIMARY_CLUSTER_ALIAS);
        backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, remoteTopic);

        // Keep the connector and task running while the source topic is deleted and recreated.
        recreateSourceTopic();
        produceMessages(primaryProducer, IntStream.range(0, NUM_PARTITIONS)
                .mapToObj(partition -> new ProducerRecord<byte[], byte[]>(
                        TOPIC, partition, null, "recreated".getBytes()))
                .collect(Collectors.toList()));

        backup.assertions().assertConnectorAndAtLeastNumTasksAreRunning(
                MirrorSourceConnector.class.getSimpleName(), 1,
                "MirrorSourceConnector should stay running after automatic topic-reset recovery");

        int expectedTotal = NUM_RECORDS_PRODUCED + NUM_PARTITIONS;
        assertEquals(expectedTotal,
                backup.kafka().consume(expectedTotal, 2 * RECORD_TRANSFER_DURATION_MS, remoteTopic).count(),
                "New records from the recreated source topic should be appended after automatic recovery");
    }

    private void deleteRecordsBefore() throws Exception {
        Map<TopicPartition, RecordsToDelete> toDelete = IntStream.range(0, NUM_PARTITIONS)
                .boxed()
                .collect(Collectors.toMap(
                        partition -> new TopicPartition(TOPIC, partition),
                        partition -> RecordsToDelete.beforeOffset(5)));
        try (Admin admin = primary.kafka().createAdminClient()) {
            admin.deleteRecords(toDelete).all().get(ADMIN_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void recreateSourceTopic() throws Exception {
        try (Admin admin = primary.kafka().createAdminClient()) {
            admin.deleteTopics(Set.of(TOPIC)).all().get(ADMIN_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            waitForCondition(
                    () -> !admin.listTopics().names().get(ADMIN_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).contains(TOPIC),
                    ADMIN_REQUEST_TIMEOUT_MS,
                    "Source topic " + TOPIC + " was not deleted in time");
        }
        primary.kafka().createTopic(TOPIC, NUM_PARTITIONS);
        waitForTopicPartitionCreated(primary, TOPIC, NUM_PARTITIONS);
    }

    private static void assertSourceTaskFailedWith(EmbeddedConnectCluster cluster, String expectedExceptionName)
            throws InterruptedException {
        String connectorName = MirrorSourceConnector.class.getSimpleName();
        AtomicReference<String> lastObservedTrace = new AtomicReference<>("<no failed task observed>");

        waitForCondition(() -> {
            ConnectorStateInfo status = cluster.connectorStatus(connectorName);
            if (status == null) {
                return false;
            }
            List<ConnectorStateInfo.TaskState> failed = status.tasks().stream()
                    .filter(task -> "FAILED".equals(task.state()))
                    .toList();
            if (failed.isEmpty()) {
                return false;
            }
            lastObservedTrace.set(String.valueOf(failed.get(0).trace()));
            return failed.stream()
                    .anyMatch(task -> task.trace() != null && task.trace().contains(expectedExceptionName));
        }, TASK_FAILURE_DURATION_MS, () -> "MirrorSourceConnector task did not fail with "
                + expectedExceptionName + " in time. Last observed trace: " + lastObservedTrace.get());
    }
}
