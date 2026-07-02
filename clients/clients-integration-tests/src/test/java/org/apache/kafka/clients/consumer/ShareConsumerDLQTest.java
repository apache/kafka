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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Meter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "group.share.min.heartbeat.interval.ms", value = "1500"),
        @ClusterConfigProperty(key = "group.share.heartbeat.interval.ms", value = "1500")
    }
)
public class ShareConsumerDLQTest extends ShareConsumerTestBase {

    // DLQ context headers written onto each DLQ record. These mirror the (package-private) constants in
    // org.apache.kafka.server.share.dlq.ShareGroupDLQStateManager and form the wire contract for DLQ records.
    private static final String HEADER_DLQ_ERRORS_TOPIC = "__dlq.errors.topic";
    private static final String HEADER_DLQ_ERRORS_PARTITION = "__dlq.errors.partition";
    private static final String HEADER_DLQ_ERRORS_OFFSET = "__dlq.errors.offset";
    private static final String HEADER_DLQ_ERRORS_GROUP = "__dlq.errors.group";

    // Yammer metric names registered by org.apache.kafka.server.share.metrics.ShareGroupMetrics.
    private static final String METRIC_DLQ_RECORD_COUNT = "DeadLetterQueueRecordCount";
    private static final String METRIC_DLQ_PRODUCE_TOTAL = "DeadLetterQueueTotalProduceRequestsPerSec";

    public ShareConsumerDLQTest(ClusterInstance cluster) {
        super(cluster);
    }

    /**
     * Produces 5 records, rejects every one of them with a share consumer in EXPLICIT acknowledgement mode,
     * and verifies they are written to the configured DLQ topic. Record copy is disabled, so the DLQ records
     * carry only the context headers (no key/value). Finally asserts the DLQ metrics for records written and
     * produce requests enqueued.
     */
    @ClusterTest
    public void testRejectedRecordsWrittenToDlqWithCopyRecordDisabled() throws Exception {
        String groupId = "dlq-group";
        // The broker's default share-group DLQ topic prefix is "dlq.", so the topic name must start with it.
        String dlqTopic = "dlq.topic";
        int recordCount = 5;

        alterShareAutoOffsetReset(groupId, "earliest");
        // Create the DLQ topic with DLQ enabled, and point the share group at it. Record copy is left
        // disabled (the default), so produced DLQ records contain headers only.
        createDlqTopic(dlqTopic);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);

        // Produce the source records onto "topic" (partition 0), created by the base setup.
        produceMessages(recordCount);

        // Reject every record using an EXPLICIT-mode share consumer.
        rejectRecords(groupId, recordCount);

        // Verify by reading from the DLQ topic, then assert the DLQ metrics.
        verifyDlqTopicRecords(dlqTopic, groupId, expectedSourceOffsets(recordCount), false);
        verifyDlqMetrics(groupId, recordCount);
    }

    /**
     * As {@link #testRejectedRecordsWrittenToDlqWithCopyRecordDisabled()}, but the DLQ topic is not created up
     * front: with DLQ auto topic creation enabled on the broker, the broker should create the configured DLQ
     * topic on the first write. Verifies the topic was created (with DLQ enabled), received the records, and
     * that the DLQ metrics fired.
     */
    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "errors.deadletterqueue.auto.create.topics.enable", value = "true")
        }
    )
    public void testRejectedRecordsWrittenToAutoCreatedDlq() throws Exception {
        String groupId = "dlq-autocreate-group";
        // The broker's default share-group DLQ topic prefix is "dlq.", so the topic name must start with it.
        String dlqTopic = "dlq.autocreate";
        int recordCount = 5;

        alterShareAutoOffsetReset(groupId, "earliest");
        // Point the share group at a DLQ topic that does NOT exist yet; the broker should auto-create it.
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);

        produceMessages(recordCount);
        rejectRecords(groupId, recordCount);

        // Verify the DLQ topic was auto-created (with DLQ enabled), received the records, and metrics fired.
        verifyDlqTopicCreated(dlqTopic);
        verifyDlqTopicRecords(dlqTopic, groupId, expectedSourceOffsets(recordCount), false);
        verifyDlqMetrics(groupId, recordCount);
    }

    /**
     * Produces 5 records and repeatedly releases them with a share consumer. Each release makes the records
     * available again and, on re-acquisition, increments their delivery count; once the delivery count limit
     * is exceeded the broker archives them and writes them to the DLQ (cause: delivery count exceeded). The
     * DLQ topic is created manually up front. Verifies the records reached the DLQ and the DLQ metrics fired.
     */
    @ClusterTest
    public void testReleasedRecordsExceedingDeliveryCountWrittenToDlq() throws Exception {
        String groupId = "dlq-release-group";
        // The broker's default share-group DLQ topic prefix is "dlq.", so the topic name must start with it.
        String dlqTopic = "dlq.release";
        int recordCount = 5;

        alterShareAutoOffsetReset(groupId, "earliest");
        // Keep the delivery count limit low so a couple of releases exhaust it and trigger the DLQ.
        alterShareDeliveryCountLimit(groupId, "2");
        // Create the DLQ topic with DLQ enabled, and point the share group at it.
        createDlqTopic(dlqTopic);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);

        produceMessages(recordCount);

        // Repeatedly release the records until their delivery count is exceeded and they are written to the DLQ.
        releaseRecordsUntilDlq(groupId, recordCount);

        verifyDlqTopicRecords(dlqTopic, groupId, expectedSourceOffsets(recordCount), false);
        verifyDlqMetrics(groupId, recordCount);
    }

    /**
     * Produces records and applies a mix of acknowledgement types so the DLQ is reached via more than one path:
     * some records are rejected (DLQ via client reject), some are released on every delivery so they reach the
     * DLQ once their delivery count is exceeded (release-based DLQ), and the rest are accepted (never DLQ'd).
     * The actions are interleaved by offset so the DLQ'd offsets are non-contiguous and produce multiple
     * separate DLQ writes to the same DLQ partition. The DLQ topic is created manually up front.
     */
    @ClusterTest
    public void testMixedAcknowledgementTypesWrittenToDlq() throws Exception {
        String groupId = "dlq-mixed-group";
        // The broker's default share-group DLQ topic prefix is "dlq.", so the topic name must start with it.
        String dlqTopic = "dlq.mixed";
        int recordCount = 6;

        alterShareAutoOffsetReset(groupId, "earliest");
        // Low delivery count limit so the released records reach the DLQ (delivery count exceeded) quickly.
        alterShareDeliveryCountLimit(groupId, "2");
        createDlqTopic(dlqTopic);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);

        produceMessages(recordCount);

        // Assign each source offset an action, interleaved so the DLQ'd offsets are non-contiguous:
        //   offset % 3 == 0 -> reject  (DLQ via client reject)
        //   offset % 3 == 1 -> release (DLQ via delivery count exceeded - released on every delivery)
        //   offset % 3 == 2 -> accept  (never DLQ'd)
        Set<Long> rejectOffsets = new HashSet<>();
        Set<Long> releaseOffsets = new HashSet<>();
        for (long offset = 0; offset < recordCount; offset++) {
            if (offset % 3 == 0) {
                rejectOffsets.add(offset);
            } else if (offset % 3 == 1) {
                releaseOffsets.add(offset);
            }
        }
        Set<Long> expectedDlqOffsets = new HashSet<>();
        expectedDlqOffsets.addAll(rejectOffsets);
        expectedDlqOffsets.addAll(releaseOffsets);

        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
            groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            // Keep polling/acknowledging until every rejected and released record has reached the DLQ. Released
            // records are released on every delivery, so they are redelivered until their delivery count is
            // exceeded and they are archived to the DLQ.
            long deadlineMs = System.currentTimeMillis() + DEFAULT_MAX_WAIT_MS;
            while (dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId) < expectedDlqOffsets.size()
                && System.currentTimeMillis() < deadlineMs) {
                ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    long offset = record.offset();
                    if (rejectOffsets.contains(offset)) {
                        shareConsumer.acknowledge(record, AcknowledgeType.REJECT);
                    } else if (releaseOffsets.contains(offset)) {
                        shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                    } else {
                        shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    }
                }
                if (records.count() > 0) {
                    shareConsumer.commitSync(Duration.ofMillis(10000));
                }
            }
        }

        // Exactly the rejected and released records should be on the DLQ; accepted records must not be.
        verifyDlqTopicRecords(dlqTopic, groupId, expectedDlqOffsets, false);
        verifyDlqMetrics(groupId, expectedDlqOffsets.size());
    }

    /**
     * As {@link #testRejectedRecordsWrittenToDlqWithCopyRecordDisabled()}, but with record copy enabled, so
     * each DLQ record must carry the original key and value (in addition to the context headers).
     */
    @ClusterTest
    public void testRejectedRecordsWrittenToDlqWithCopyRecordEnabled() throws Exception {
        String groupId = "dlq-copy-group";
        String dlqTopic = "dlq.copy";
        int recordCount = 5;

        alterShareAutoOffsetReset(groupId, "earliest");
        createDlqTopic(dlqTopic);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);
        // Enable record copy so the original key/value are written onto the DLQ record.
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true");

        produceMessages(recordCount);
        rejectRecords(groupId, recordCount);

        verifyDlqTopicRecords(dlqTopic, groupId, expectedSourceOffsets(recordCount), true);
        verifyDlqMetrics(groupId, recordCount);
    }

    /**
     * Rejects records from a multi-partition source topic and verifies they are routed to the correct DLQ
     * partition. The destination partition is {@code sourcePartition % numDlqPartitions}; with a DLQ topic that
     * has as many partitions as the source, each source partition maps to the DLQ partition of the same index.
     */
    @ClusterTest
    public void testDlqRecordsRoutedToCorrectDlqPartition() throws Exception {
        String groupId = "dlq-routing-group";
        String sourceTopic = "dlq-routing-source";
        String dlqTopic = "dlq.routing";
        int partitions = 2;
        int recordsPerPartition = 3;

        createTopic(sourceTopic, partitions, 1);
        alterShareAutoOffsetReset(groupId, "earliest");
        createDlqTopic(dlqTopic, partitions);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);

        // Produce the same number of records to each source partition.
        for (int sourcePartition = 0; sourcePartition < partitions; sourcePartition++) {
            produceTo(sourceTopic, sourcePartition, recordsPerPartition);
        }

        int total = partitions * recordsPerPartition;
        rejectRecords(groupId, sourceTopic, total);
        verifyDlqMetrics(groupId, total);

        // Source partition p maps to DLQ partition (p % partitions) == p. Each DLQ partition should hold the
        // records from the matching source partition (offsets 0..recordsPerPartition-1).
        for (int sourcePartition = 0; sourcePartition < partitions; sourcePartition++) {
            List<ConsumerRecord<byte[], byte[]>> dlqRecords = readDlqPartition(dlqTopic, sourcePartition, recordsPerPartition);
            assertEquals(recordsPerPartition, dlqRecords.size(),
                "DLQ partition " + sourcePartition + " has an unexpected number of records");
            Set<Long> offsets = new HashSet<>();
            for (ConsumerRecord<byte[], byte[]> record : dlqRecords) {
                assertEquals(groupId, headerValue(record, HEADER_DLQ_ERRORS_GROUP));
                assertEquals(sourceTopic, headerValue(record, HEADER_DLQ_ERRORS_TOPIC));
                assertEquals(Integer.toString(sourcePartition), headerValue(record, HEADER_DLQ_ERRORS_PARTITION),
                    "Records on DLQ partition " + sourcePartition + " must originate from source partition " + sourcePartition);
                offsets.add(Long.parseLong(Objects.requireNonNull(headerValue(record, HEADER_DLQ_ERRORS_OFFSET))));
            }
            assertEquals(expectedSourceOffsets(recordsPerPartition), offsets);
        }
    }

    /**
     * Rejects a larger batch of records and verifies they all reach the DLQ. Exercises the produce coalescing /
     * record-merge path at volume (many records DLQ'd to the same DLQ partition).
     */
    @ClusterTest
    public void testManyRejectedRecordsAllWrittenToDlq() throws Exception {
        String groupId = "dlq-scale-group";
        String dlqTopic = "dlq.scale";
        int recordCount = 500;

        alterShareAutoOffsetReset(groupId, "earliest");
        createDlqTopic(dlqTopic);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);

        produceMessages(recordCount);
        rejectRecords(groupId, recordCount);

        verifyDlqTopicRecords(dlqTopic, groupId, expectedSourceOffsets(recordCount), false);
        verifyDlqMetrics(groupId, recordCount);
    }

    /**
     * Verifies the DLQ is NOT written when it is gated off, across the gating conditions:
     *   (a) no DLQ topic configured for the group;
     *   (b) the configured DLQ topic exists but is not DLQ-enabled;
     *   (c) the configured DLQ topic does not exist and auto creation is disabled (the cluster default);
     *   (d) the configured DLQ topic name does not match the broker's DLQ topic name prefix ("dlq.").
     * In every case the rejected records are archived (the start offset advances) but no DLQ record is written.
     */
    @ClusterTest
    public void testDlqNotTriggeredWhenGatedOff() throws Exception {
        int recordCount = 3;

        // (a) No DLQ topic configured for the group.
        assertNoDlqWritten("dlq-gate-noname", "dlq-gate-source-a", recordCount, group -> { });

        // (b) DLQ topic exists but is not DLQ-enabled.
        assertNoDlqWritten("dlq-gate-disabled", "dlq-gate-source-b", recordCount, group -> {
            createTopic("dlq.disabled");
            alterShareGroupConfig(group, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, "dlq.disabled");
        });

        // (c) DLQ topic missing and auto creation disabled (broker default).
        assertNoDlqWritten("dlq-gate-missing", "dlq-gate-source-c", recordCount, group ->
            alterShareGroupConfig(group, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, "dlq.missing"));

        // (d) DLQ topic is DLQ-enabled but its name violates the "dlq." prefix.
        assertNoDlqWritten("dlq-gate-prefix", "dlq-gate-source-d", recordCount, group -> {
            createDlqTopic("wrong.prefix");
            alterShareGroupConfig(group, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, "wrong.prefix");
        });
    }

    /**
     * A single consumer rejects records while the DLQ is enabled (they are written to the DLQ), then the DLQ is
     * turned off for the group by clearing its DLQ topic name, and the same consumer keeps rejecting more
     * records. Verifies via the DLQ record-count metric that no further DLQ records are written after the DLQ
     * is disabled.
     */
    @ClusterTest
    public void testDlqStopsAfterDisablingForGroup() throws Exception {
        String groupId = "dlq-toggle-group";
        String dlqTopic = "dlq.toggle";
        int firstBatch = 5;
        int secondBatch = 5;

        alterShareAutoOffsetReset(groupId, "earliest");
        createDlqTopic(dlqTopic);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);

        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
            groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));

            // Phase 1: DLQ enabled - reject the first batch; they are written to the DLQ.
            produceMessages(firstBatch);
            rejectRecords(shareConsumer, firstBatch);
            waitForCondition(() -> dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId) == firstBatch,
                DEFAULT_MAX_WAIT_MS, 200L,
                () -> "First batch not written to DLQ, count was " + dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId));

            // Turn DLQ off for the group by clearing its DLQ topic name.
            alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, "");

            // Phase 2: DLQ disabled - the same consumer rejects more records; none should be DLQ'd.
            produceMessages(secondBatch);
            rejectRecords(shareConsumer, secondBatch);

            // The second-batch records are archived (terminal) and not redelivered; confirming this also gives
            // any (erroneous) DLQ write a chance to land before we assert.
            for (int i = 0; i < 3; i++) {
                assertEquals(0, shareConsumer.poll(Duration.ofMillis(2000)).count(),
                    "Archived records must not be redelivered");
            }
        }

        // Only the first batch (written while DLQ was enabled) should be on the DLQ.
        assertEquals(firstBatch, dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId),
            "No DLQ records should be written after the DLQ is disabled for the group");
    }

    // Consumes from the base source topic in EXPLICIT acknowledgement mode and rejects `recordCount` records.
    private void rejectRecords(String groupId, int recordCount) {
        rejectRecords(groupId, tp.topic(), recordCount);
    }

    // Consumes from the given source topic in EXPLICIT acknowledgement mode and rejects `recordCount` records.
    private void rejectRecords(String groupId, String topic, int recordCount) {
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
            groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(topic));
            rejectRecords(shareConsumer, recordCount);
        }
    }

    // Polls the given (already subscribed) consumer and rejects records until `count` have been rejected.
    private void rejectRecords(ShareConsumer<byte[], byte[]> shareConsumer, int count) {
        int rejected = 0;
        long deadlineMs = System.currentTimeMillis() + DEFAULT_MAX_WAIT_MS;
        while (rejected < count && System.currentTimeMillis() < deadlineMs) {
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.REJECT);
                rejected++;
            }
            if (records.count() > 0) {
                shareConsumer.commitSync(Duration.ofMillis(10000));
            }
        }
        assertEquals(count, rejected, "Expected to reject the requested number of records");
    }

    // Repeatedly polls and releases every record until the delivery count limit is exceeded for all of them
    // and they have been written to the DLQ (tracked via the DLQ record-count metric).
    private void releaseRecordsUntilDlq(String groupId, int recordCount) {
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
            groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            long deadlineMs = System.currentTimeMillis() + DEFAULT_MAX_WAIT_MS;
            while (dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId) < recordCount
                && System.currentTimeMillis() < deadlineMs) {
                ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                }
                if (records.count() > 0) {
                    shareConsumer.commitSync(Duration.ofMillis(10000));
                }
            }
        }
    }

    // Sets up a gating condition for a fresh group/source topic, rejects records, and asserts that although the
    // records are archived (start offset advances) no DLQ record is ever written for the group. The setup
    // callback receives the group id (java.util.function.Consumer is fully qualified to avoid clashing with the
    // Kafka Consumer type in this package).
    private void assertNoDlqWritten(String groupId, String sourceTopic, int recordCount,
                                    java.util.function.Consumer<String> gateSetup) throws Exception {
        createTopic(sourceTopic);
        alterShareAutoOffsetReset(groupId, "earliest");
        gateSetup.accept(groupId);

        produceTo(sourceTopic, 0, recordCount);
        rejectRecords(groupId, sourceTopic, recordCount);

        // The rejected records are archived (terminal) so they are never redelivered. Confirming this also
        // gives any DLQ attempt time to run (and, for the gated cases, to fail/short-circuit).
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
            groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(sourceTopic));
            for (int i = 0; i < 3; i++) {
                assertEquals(0, shareConsumer.poll(Duration.ofMillis(2000)).count(),
                    "Archived records must not be redelivered for group: " + groupId);
            }
        }
        // No DLQ record was written for this group (the per-group meter is never even registered).
        assertEquals(-1L, dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId),
            "DLQ should not have been written for a gated-off group: " + groupId);
    }

    private static Set<Long> expectedSourceOffsets(int recordCount) {
        Set<Long> offsets = new HashSet<>();
        for (long offset = 0; offset < recordCount; offset++) {
            offsets.add(offset);
        }
        return offsets;
    }

    // Asserts that every record was written to the DLQ and that at least one DLQ produce request was enqueued.
    private void verifyDlqMetrics(String groupId, int expectedRecordCount) throws InterruptedException {
        waitForCondition(() -> dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId) == expectedRecordCount,
            DEFAULT_MAX_WAIT_MS, 200L,
            () -> "DeadLetterQueueRecordCount did not reach " + expectedRecordCount
                + ", was " + dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId));
        assertEquals(expectedRecordCount, dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId));
        assertTrue(dlqMeterCount(METRIC_DLQ_PRODUCE_TOTAL, groupId) >= 1,
            "Expected at least one DLQ produce request to have been enqueued");
    }

    // Verifies the DLQ topic was auto-created by the broker and has the DLQ-enable topic config set.
    private void verifyDlqTopicCreated(String dlqTopic) throws Exception {
        try (Admin admin = createAdminClient()) {
            waitForCondition(() -> admin.listTopics().names().get().contains(dlqTopic),
                DEFAULT_MAX_WAIT_MS, 500L, () -> "DLQ topic " + dlqTopic + " was not auto-created");

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, dlqTopic);
            Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            ConfigEntry entry = config.get(TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG);
            assertNotNull(entry, "Auto-created DLQ topic is missing the DLQ-enable config");
            assertEquals("true", entry.value(), "Auto-created DLQ topic should have DLQ enabled");
        }
    }

    /**
     * Reads the DLQ topic (single partition) and asserts it received exactly one record per expected source
     * offset, each carrying the context headers for the base source topic-partition. When {@code copyEnabled}
     * is true the DLQ records must also carry the original key/value; otherwise they carry headers only.
     *
     * @param dlqTopic              the DLQ topic to read from (single partition)
     * @param groupId               the share group the rejected records belonged to
     * @param expectedSourceOffsets the source offsets expected to have been written to the DLQ
     * @param copyEnabled           whether record copy is enabled for the group
     */
    private void verifyDlqTopicRecords(String dlqTopic, String groupId, Set<Long> expectedSourceOffsets,
                                       boolean copyEnabled) throws InterruptedException {
        List<ConsumerRecord<byte[], byte[]>> dlqRecords = readDlqPartition(dlqTopic, 0, expectedSourceOffsets.size());

        assertEquals(expectedSourceOffsets.size(), dlqRecords.size(), "Unexpected number of records on the DLQ topic");
        Set<Long> actualSourceOffsets = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> record : dlqRecords) {
            if (copyEnabled) {
                // produceMessages() produces records with key "key" and value "value".
                assertEquals("key", new String(Objects.requireNonNull(record.key()), StandardCharsets.UTF_8),
                    "DLQ record key should be copied when record copy is enabled");
                assertEquals("value", new String(Objects.requireNonNull(record.value()), StandardCharsets.UTF_8),
                    "DLQ record value should be copied when record copy is enabled");
            } else {
                // Record copy is disabled, so only headers are written - the value (and key) are null.
                assertNull(record.value(), "DLQ record value should be null when record copy is disabled");
            }
            assertEquals(groupId, headerValue(record, HEADER_DLQ_ERRORS_GROUP));
            assertEquals(tp.topic(), headerValue(record, HEADER_DLQ_ERRORS_TOPIC));
            assertEquals(Integer.toString(tp.partition()), headerValue(record, HEADER_DLQ_ERRORS_PARTITION));
            actualSourceOffsets.add(Long.parseLong(Objects.requireNonNull(headerValue(record, HEADER_DLQ_ERRORS_OFFSET))));
        }
        assertEquals(expectedSourceOffsets, actualSourceOffsets, "DLQ records should cover every expected source offset");
    }

    // Reads at least `expectedCount` records from a single DLQ topic-partition.
    private List<ConsumerRecord<byte[], byte[]>> readDlqPartition(String dlqTopic, int partition, int expectedCount)
            throws InterruptedException {
        TopicPartition dlqTp = new TopicPartition(dlqTopic, partition);
        List<ConsumerRecord<byte[], byte[]>> dlqRecords = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = cluster.consumer()) {
            consumer.assign(List.of(dlqTp));
            consumer.seekToBeginning(List.of(dlqTp));
            waitForCondition(() -> {
                dlqRecords.addAll(consumer.poll(Duration.ofMillis(1000)).records(dlqTp));
                return dlqRecords.size() >= expectedCount;
            }, DEFAULT_MAX_WAIT_MS, 500L,
                () -> dlqTp + " did not receive " + expectedCount + " records, got " + dlqRecords.size());
        }
        return dlqRecords;
    }

    private void createDlqTopic(String topicName) {
        createDlqTopic(topicName, 1);
    }

    private void createDlqTopic(String topicName, int numPartitions) {
        assertDoesNotThrow(() -> {
            try (Admin admin = createAdminClient()) {
                NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1)
                    .configs(Map.of(TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true"));
                admin.createTopics(Set.of(newTopic)).all().get();
            }
        }, "Failed to create DLQ topic");
    }

    // Produces `count` records (key "key", value "value") to a specific topic-partition.
    private void produceTo(String topic, int partition, int count) {
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, partition, "key".getBytes(), "value".getBytes()));
            }
            producer.flush();
        }
    }

    private static String headerValue(ConsumerRecord<byte[], byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    // Returns the count of the (per-group) ShareGroupMetrics meter with the given name, or 0 if it has not
    // been registered yet (the meters are created lazily on the first DLQ write for a group).
    private static long dlqMeterCount(String metricName, String groupId) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(entry -> {
                String mBeanName = entry.getKey().toString();
                return mBeanName.contains("name=" + metricName) && mBeanName.contains("group=" + groupId);
            })
            .map(Map.Entry::getValue)
            .mapToLong(metric -> ((Meter) metric).count())
            .findFirst()
            .orElse(-1L);
    }
}
