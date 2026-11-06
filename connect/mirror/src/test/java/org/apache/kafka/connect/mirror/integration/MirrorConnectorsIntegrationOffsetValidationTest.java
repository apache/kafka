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
import org.apache.kafka.connect.mirror.TopicResetException;
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

/**
 * Integration tests for the fail-fast offset validation added to
 * {@link org.apache.kafka.connect.mirror.MirrorSourceTask}.
 *
 * <p>Both scenarios follow the same shape: replicate a topic normally, stop the
 * {@link MirrorSourceConnector}, engineer the failure on the source cluster, then restart the
 * connector and assert the task fails with the expected exception rather than quietly rewinding to
 * the earliest available offset.
 */
@Tag("integration")
public class MirrorConnectorsIntegrationOffsetValidationTest extends MirrorConnectorsIntegrationBaseTest {

    private static final String TOPIC = "test-topic-1";
    private static final int TASK_FAILURE_DURATION_MS = 60_000;
    private static final int ADMIN_REQUEST_TIMEOUT_MS = 60_000;

    @BeforeEach
    @Override
    public void startClusters() throws Exception {
        // One-way replication keeps the failure attribution unambiguous.
        replicateBackupToPrimary = false;
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put("topics", "test-topic-.*");
        additionalConfig.put(MirrorSourceConfig.OFFSET_VALIDATION_ENABLED, "true");
        super.startClusters(additionalConfig);
    }

    /**
     * The source topic's retention policy removes records that MirrorMaker 2 has not replicated yet.
     * The task must fail with a {@link DataLossException} instead of skipping the gap.
     */
    @Test
    public void testDataLossDetectedWhenUnreplicatedRecordsArePurged() throws Exception {
        produceMessages(primaryProducer, TOPIC);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS,
                remoteTopicName(TOPIC, PRIMARY_CLUSTER_ALIAS));

        stopMirrorMakerConnectors(backup, MirrorSourceConnector.class);

        // Rewind the connector to the start of each partition, then purge the first half of every
        // partition on the source cluster. The connector now points below the log start offset,
        // which is exactly the state an aggressive retention policy would leave it in.
        alterMirrorMakerSourceConnectorOffsets(backup, offset -> 0L, TOPIC);
        deleteRecordsBefore();

        backup.resumeConnector(MirrorSourceConnector.class.getSimpleName());

        assertSourceTaskFailedWith(backup, DataLossException.class.getName());
    }

    /**
     * The source topic is deleted and recreated, so the tracked offsets point past the end of a log
     * that now starts at zero. The task must fail with a {@link TopicResetException} instead of
     * re-replicating the new topic on top of the previously mirrored data.
     */
    @Test
    public void testTopicResetDetectedWhenSourceTopicIsRecreated() throws Exception {
        produceMessages(primaryProducer, TOPIC);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS,
                remoteTopicName(TOPIC, PRIMARY_CLUSTER_ALIAS));

        stopMirrorMakerConnectors(backup, MirrorSourceConnector.class);

        recreateSourceTopic();
        // One record per partition, so the new log ends well before the committed offset of
        // NUM_RECORDS_PER_PARTITION - 1 that the connector still holds.
        produceMessages(primaryProducer, IntStream.range(0, NUM_PARTITIONS)
                .mapToObj(partition -> new ProducerRecord<byte[], byte[]>(
                        TOPIC, partition, null, "recreated".getBytes()))
                .collect(Collectors.toList()));

        backup.resumeConnector(MirrorSourceConnector.class.getSimpleName());

        assertSourceTaskFailedWith(backup, TopicResetException.class.getName());
    }

    /**
     * Deletes every record below {@code offset} on all partitions of the source topic, moving the
     * log start offset forward the same way a retention-driven segment deletion would.
     */
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

    /**
     * Deletes and recreates the source topic with the same partition count, waiting for each step so
     * the test does not race the controller.
     */
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

    /**
     * Waits until at least one {@link MirrorSourceConnector} task has failed, and asserts the
     * recorded stack trace names the expected exception.
     */
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
