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
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.junit.After;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

abstract class MirrorConnectorsIntegrationTest {

    protected static final int NUM_RECORDS_PER_PARTITION = 10;
    protected static final int NUM_PARTITIONS = 10;
    protected static final int NUM_RECORDS_PRODUCED = NUM_PARTITIONS * NUM_RECORDS_PER_PARTITION;
    protected static final int RECORD_TRANSFER_DURATION_MS = 30_000;
    protected static final int CHECKPOINT_DURATION_MS = 20_000;
    protected static final int RECORD_CONSUME_DURATION_MS = 20_000;
    protected static final int OFFSET_SYNC_DURATION_MS = 30_000;

    protected final AtomicBoolean exited = new AtomicBoolean(false);

    protected Map<String, String> mm2Props;
    protected MirrorMakerConfig mm2Config;
    protected EmbeddedConnectCluster primary;
    protected EmbeddedConnectCluster backup;

    @After
    public void close() {
        for (String x : primary.connectors()) {
            primary.deleteConnector(x);
        }
        for (String x : backup.connectors()) {
            backup.deleteConnector(x);
        }
        deleteAllTopics(primary.kafka());
        deleteAllTopics(backup.kafka());
        primary.stop();
        backup.stop();
        try {
            assertFalse(exited.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    protected void deleteAllTopics(EmbeddedKafkaCluster cluster) {
        Admin client = cluster.createAdminClient();
        try {
            client.deleteTopics(client.listTopics().names().get());
        } catch (Throwable e) {
        }
    }

    protected void waitUntilMirrorMakerIsRunning(EmbeddedConnectCluster connectCluster,
                                                 MirrorMakerConfig mm2Config, String primary, String backup, boolean onlyHeartbeat) throws InterruptedException {

        if (!onlyHeartbeat) {
            connectCluster.configureConnector("MirrorSourceConnector",
                    mm2Config.connectorBaseConfig(new SourceAndTarget(primary, backup), MirrorSourceConnector.class));
            connectCluster.configureConnector("MirrorCheckpointConnector",
                    mm2Config.connectorBaseConfig(new SourceAndTarget(primary, backup), MirrorCheckpointConnector.class));
        }
        connectCluster.configureConnector("MirrorHeartbeatConnector",
                mm2Config.connectorBaseConfig(new SourceAndTarget(primary, backup), MirrorHeartbeatConnector.class));

        // we wait for the connector and tasks to come up for each connector, so that when we do the
        // actual testing, we are certain that the tasks are up and running; this will prevent
        // flaky tests where the connector and tasks didn't start up in time for the tests to be
        // run
        Set<String> connectorNames = new HashSet<>(Collections.singletonList("MirrorHeartbeatConnector"));
        if (!onlyHeartbeat) {
            connectorNames.add("MirrorSourceConnector");
            connectorNames.add("MirrorCheckpointConnector");
        }

        for (String connector : connectorNames) {
            connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connector, 1,
                    "Connector " + connector + " tasks did not start in time on cluster: " + connectCluster);
        }
    }

    protected Map<TopicPartition, OffsetAndMetadata> waitForConsumerGroupOffsetReplication(List<String> topics, String consumerGroupId, boolean waitForAutoSync)
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

    protected Map<TopicPartition, OffsetAndMetadata> getTranslatedOffsets(String consumerGroupId)
            throws TimeoutException, InterruptedException {
        return RemoteClusterUtils.translateOffsets(
                mm2Config.clientConfig("backup").adminConfig(),
                "primary",
                consumerGroupId,
                Duration.ofMillis(CHECKPOINT_DURATION_MS));
    }

    protected void produceMessages(EmbeddedConnectCluster cluster, String topicName, String msgPrefix) {
        produceMessages(cluster, topicName, msgPrefix, NUM_PARTITIONS);
    }

    protected void produceMessages(EmbeddedConnectCluster cluster, String topicName, String msgPrefix, int numPartitions) {
        // produce the configured number of records to all specified partitions
        int cnt = 0;
        for (int r = 0; r < NUM_RECORDS_PER_PARTITION; r++)
            for (int p = 0; p < numPartitions; p++)
                cluster.kafka().produce(topicName, p, "key", msgPrefix + cnt++);
    }

    protected Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumeAllMessages(Consumer<byte[], byte[]> consumer) throws InterruptedException {
        return consumeAllMessages(consumer, null, null);
    }

    protected Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumeAllMessages(Consumer<byte[], byte[]> consumer, Integer expectedRecords) throws InterruptedException {
        return consumeAllMessages(consumer, expectedRecords, null);
    }

    protected Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> consumeAllMessages(Consumer<byte[], byte[]> consumer, Integer numExpectedRecords, Duration timeoutDurationMs)
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

    protected void seek(Consumer<byte[], byte[]> consumer, Map<TopicPartition, OffsetAndMetadata> offsets) throws InterruptedException {
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

    protected List<TopicPartition> allPartitions(String... topics) {
        return IntStream.range(0, NUM_PARTITIONS)
                .boxed()
                .flatMap(p -> Arrays.stream(topics).map(t -> new TopicPartition(t, p)))
                .collect(Collectors.toList());
    }
}
