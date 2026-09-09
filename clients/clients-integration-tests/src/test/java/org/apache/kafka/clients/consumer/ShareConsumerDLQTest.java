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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
     * End-to-end DLQ copy when the source records have been tiered to remote storage. The source topic enables
     * tiered storage and rolls a segment per record, with a 45s total retention and a 5s local retention so the
     * early offsets are offloaded to remote storage and then deleted locally (well before the remote segments
     * expire). Once the local segments are gone (verified via the earliest-local offset advancing past them), the
     * records are rejected with record copy enabled - so the DLQ record fetcher must read the original records
     * back from remote storage. The resulting DLQ records carrying the original key/value confirm the fetcher
     * successfully pulled them from remote storage.
     *
     * <p>Tiered storage is backed by the local-filesystem {@code LocalTieredStorage} RSM and the default
     * {@code TopicBasedRemoteLogMetadataManager}; short task/cleanup intervals keep the offload + local-delete
     * cycle quick.
     */
    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "remote.log.storage.system.enable", value = "true"),
            @ClusterConfigProperty(key = "remote.log.storage.manager.class.name",
                value = "org.apache.kafka.server.log.remote.storage.LocalTieredStorage"),
            @ClusterConfigProperty(key = "remote.log.manager.task.interval.ms", value = "500"),
            @ClusterConfigProperty(key = "remote.log.metadata.manager.listener.name", value = "EXTERNAL"),
            @ClusterConfigProperty(key = "rlmm.config.remote.log.metadata.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "rlmm.config.remote.log.metadata.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "log.retention.check.interval.ms", value = "500"),
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100")
        }
    )
    public void testDlqCopiesRecordsReadFromRemoteStorage() throws Exception {
        String groupId = "dlq-remote-group";
        // The broker's default share-group DLQ topic prefix is "dlq.", so the topic name must start with it.
        String dlqTopic = "dlq.remote";
        String sourceTopic = "dlq-remote-source";
        int recordCount = 5;

        alterShareAutoOffsetReset(groupId, "earliest");
        createDlqTopic(dlqTopic);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);
        // Record copy enabled: the DLQ fetcher must read the original records back to copy their key/value.
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true");

        // Tiered source topic: one segment per record, 45s total retention and 5s local retention so inactive
        // segments are offloaded to remote storage and then deleted locally shortly after, while the remote
        // segments comfortably survive the rest of the test.
        createRemoteStorageSourceTopic(sourceTopic, 45_000L, 5_000L);

        produceTo(sourceTopic, 0, recordCount);

        // Wait until the early offsets have been offloaded to remote storage AND removed locally - i.e. the
        // earliest *local* offset has advanced past them, so reading those offsets must now hit remote storage.
        // The last record stays in the active (never-offloaded) segment, so the earliest local offset should
        // reach recordCount - 1. A generous timeout (well inside the 45s remote retention) absorbs remote-log
        // metadata-manager startup; it normally resolves a few seconds after the 5s local retention elapses.
        waitForCondition(() -> earliestLocalOffset(sourceTopic, 0) >= recordCount - 1,
            30_000L, 500L,
            () -> "Source records were not tiered to remote storage and removed locally in time");

        // Reject every record. Both the share fetch (to deliver them) and the DLQ record fetcher (to copy them)
        // must read the tiered offsets back from remote storage.
        rejectRecords(groupId, sourceTopic, recordCount);

        // Record copy is enabled, so every DLQ record must carry the original key/value. For the tiered offsets
        // (no longer present locally) that is only possible if the DLQ fetcher pulled them from remote storage.
        verifyDlqTopicRecords(dlqTopic, groupId, sourceTopic, 0, expectedSourceOffsets(recordCount), true);
        verifyDlqMetrics(groupId, recordCount);
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "remote.log.storage.system.enable", value = "true"),
            @ClusterConfigProperty(key = "remote.log.storage.manager.class.name",
                value = "org.apache.kafka.server.log.remote.storage.LocalTieredStorage"),
            @ClusterConfigProperty(key = "remote.log.manager.task.interval.ms", value = "500"),
            @ClusterConfigProperty(key = "remote.log.metadata.manager.listener.name", value = "EXTERNAL"),
            @ClusterConfigProperty(key = "rlmm.config.remote.log.metadata.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "rlmm.config.remote.log.metadata.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "log.retention.check.interval.ms", value = "500"),
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "group.share.min.heartbeat.interval.ms", value = "1500"),
            @ClusterConfigProperty(key = "group.share.heartbeat.interval.ms", value = "1500")
        }
    )
    public void testDlqCopiesRecordsReadFromRemoteAndLocalStorage() throws Exception {
        String groupId = "dlq-remote-and-local-group";
        // The broker's default share-group DLQ topic prefix is "dlq.", so the topic name must start with it.
        String dlqTopic = "dlq.remote-and-local-topic";
        String sourceTopic = "dlq-remote-and-local-source";
        int recordCount = 5;

        alterShareAutoOffsetReset(groupId, "earliest");
        createDlqTopic(dlqTopic);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);
        // Record copy enabled: the DLQ fetcher must read the original records back to copy their key/value.
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true");

        // Tiered source topic: one segment per record, 45s total retention and 5s local retention so inactive
        // segments are offloaded to remote storage and then deleted locally shortly after, while the remote
        // segments comfortably survive the rest of the test.
        createRemoteStorageSourceTopic(sourceTopic, 45_000L, 10_000L);

        produceTo(sourceTopic, 0, recordCount);

        // Wait until the early offsets have been offloaded to remote storage AND removed locally - i.e. the
        // earliest *local* offset has advanced past them, so reading those offsets must now hit remote storage.
        // The last record stays in the active (never-offloaded) segment, so the earliest local offset should
        // reach recordCount - 1. A generous timeout (well inside the 45s remote retention) absorbs remote-log
        // metadata-manager startup; it normally resolves a few seconds after the 5s local retention elapses.
        waitForCondition(() -> earliestLocalOffset(sourceTopic, 0) >= recordCount - 1,
            30_000L, 500L,
            () -> "Source records were not tiered to remote storage and removed locally in time");

        // Produce some more which stay in local.
        produceTo(sourceTopic, 0, recordCount);

        // Reject every record. Both the share fetch (to deliver them) and the DLQ record fetcher (to copy them)
        // must read the tiered offsets back from remote storage.
        rejectRecords(groupId, sourceTopic, recordCount * 2);

        // Record copy is enabled, so every DLQ record must carry the original key/value. For the tiered offsets
        // (no longer present locally) that is only possible if the DLQ fetcher pulled them from remote storage.
        verifyDlqTopicRecords(dlqTopic, groupId, sourceTopic, 0, expectedSourceOffsets(recordCount * 2), true);

        // Make sure not all offsets from second produce are tiered.
        waitForCondition(() -> earliestLocalOffset(sourceTopic, 0) < recordCount * 2 - 1,
            30_000L, 500L,
            () -> "Offsets from second produce were tiered");

        verifyDlqMetrics(groupId, recordCount * 2);
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
     * Verifies that the DLQ topic's own {@code max.message.bytes} - not the source topic's - bounds each DLQ
     * produce request, and that this is honored dynamically when the config changes.
     *
     * <p>Phase 1: the source topic allows records up to {@code sourceMaxMessageBytes}, but the DLQ topic is
     * configured with a third of that ({@code dlqMaxMessageBytes}). Three records, each sized so that any two
     * of their (record-copy-enabled) DLQ copies together would exceed dlqMaxMessageBytes, are rejected together
     * in a single commit - so SharePartition issues one DLQ call spanning the whole offset range (see
     * ShareGroupDLQStateManagerTest for the underlying chunking logic). The DLQ produce path must then split
     * that single call into multiple sequential produce requests (one record each) rather than failing or
     * dropping any record; this confirms all 3 records still land on the DLQ topic, and that the DLQ
     * produce-request count increased by at least 3 (one per chunk) - concrete proof splitting occurred, since
     * without it the single oversized request would be rejected once (non-retriable) and no records would ever
     * arrive.
     *
     * <p>Phase 2: the DLQ topic's {@code max.message.bytes} is then raised via {@code IncrementalAlterConfigs}
     * to comfortably exceed what a fresh batch of 3 more (same-sized) records needs combined, and the reject
     * scenario is repeated with that new batch. This confirms two more things: the broker picks up the raised
     * limit dynamically (not a value cached at startup), and the chunking logic does not split unnecessarily
     * once the budget is actually sufficient - the produce-request count must increase by exactly 1 for the
     * second batch, not 3.
     */
    @ClusterTest
    public void testDlqRespectsDlqTopicMaxMessageBytesNotEqToSourceTopic() throws Exception {
        String groupId = "dlq-maxbytes-group";
        String sourceTopic = "dlq-maxbytes-source";
        String dlqTopic = "dlq.maxbytes";
        int recordCount = 3;
        int sourceMaxMessageBytes = 300_000;
        int dlqMaxMessageBytes = sourceMaxMessageBytes / 3;
        // Leave headroom below dlqMaxMessageBytes for the DLQ context headers/record-batch framing overhead,
        // so a single record's DLQ copy cleanly fits under the limit but two together clearly don't.
        int payloadSize = dlqMaxMessageBytes - 2_000;

        try (Admin admin = createAdminClient()) {
            admin.createTopics(Set.of(
                new NewTopic(sourceTopic, 1, (short) 1)
                    .configs(Map.of(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(sourceMaxMessageBytes))),
                new NewTopic(dlqTopic, 1, (short) 1)
                    .configs(Map.of(
                        TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true",
                        TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(dlqMaxMessageBytes)))
            )).all().get();
        }

        alterShareAutoOffsetReset(groupId, "earliest");
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);
        // Record copy must be enabled - otherwise DLQ records carry headers only (tiny) and would never
        // approach dlqMaxMessageBytes regardless of the source record size.
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true");

        byte[] payload = new byte[payloadSize];
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < recordCount; i++) {
                producer.send(new ProducerRecord<>(sourceTopic, 0, "key".getBytes(StandardCharsets.UTF_8), payload));
            }
            producer.flush();
        }

        // Reject all 3 records together in one commit, so SharePartition issues a single DLQ call spanning
        // the whole offset range (a fresh, contiguous, single-fetch acquisition with no prior redeliveries
        // maps to one cached in-flight batch, so one client-side reject commit produces one DLQ call).
        rejectRecords(groupId, sourceTopic, recordCount);

        // All 3 records must still reach the DLQ, split across multiple produce requests since no pairing
        // of their DLQ copies fits within dlqMaxMessageBytes. Verified inline (rather than via
        // verifyDlqTopicRecords()) since that helper hardcodes checking the copied value against the fixed
        // "value" content produced by produceMessages()/produceTo(), not this test's large payload.
        List<ConsumerRecord<byte[], byte[]>> dlqRecords = readDlqPartition(dlqTopic, 0, recordCount);
        assertEquals(recordCount, dlqRecords.size(), "Unexpected number of records on the DLQ topic");
        Set<Long> actualSourceOffsets = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> record : dlqRecords) {
            assertArrayEquals(payload, record.value(), "DLQ record value should be the copied payload");
            assertEquals(groupId, headerValue(record, HEADER_DLQ_ERRORS_GROUP));
            assertEquals(sourceTopic, headerValue(record, HEADER_DLQ_ERRORS_TOPIC));
            assertEquals("0", headerValue(record, HEADER_DLQ_ERRORS_PARTITION));
            actualSourceOffsets.add(Long.parseLong(Objects.requireNonNull(headerValue(record, HEADER_DLQ_ERRORS_OFFSET))));
        }
        assertEquals(expectedSourceOffsets(recordCount), actualSourceOffsets,
            "DLQ records should cover every expected source offset");
        verifyDlqMetrics(groupId, recordCount);

        // Concrete proof that splitting - not some other mechanism - is why all 3 records arrived: there is
        // exactly one logical DLQ call here (one contiguous reject batch, offsets 0-2), so any produce-request
        // count above 1 for this group can only come from the resumable-cursor chunking logic splitting that
        // one call into multiple sequential produce requests to stay within dlqMaxMessageBytes. Without it,
        // the single oversized request would be rejected once (MESSAGE_TOO_LARGE is not retriable) and no
        // records would ever reach the DLQ - contradicting the assertions above. >= rather than == tolerates
        // an occasional extra retry (e.g. a transient network blip) without being flaky.
        assertTrue(dlqMeterCount(METRIC_DLQ_PRODUCE_TOTAL, groupId) >= recordCount,
            "Expected at least " + recordCount + " separate DLQ produce requests (one per chunk), was "
                + dlqMeterCount(METRIC_DLQ_PRODUCE_TOTAL, groupId));
        long produceCountBeforeRaise = dlqMeterCount(METRIC_DLQ_PRODUCE_TOTAL, groupId);

        // Now raise the DLQ topic's max.message.bytes well above what all 3 (record-copy-enabled) DLQ copies
        // need together, and repeat the same reject scenario with a fresh batch of 3 records. This confirms
        // two things at once: dlqTopicMaxMessageBytes() picks up the change dynamically (it wraps a live
        // topic-config lookup, not a value captured once at startup - see ShareCoordinatorMetadataCacheHelperImpl),
        // and the chunking logic doesn't split unnecessarily when the budget is actually sufficient - the
        // produce-request count must increase by exactly 1 (one request for the whole new batch), not 3.
        int raisedDlqMaxMessageBytes = sourceMaxMessageBytes * 2;
        ConfigResource dlqTopicResource = new ConfigResource(ConfigResource.Type.TOPIC, dlqTopic);
        try (Admin admin = createAdminClient()) {
            admin.incrementalAlterConfigs(
                Map.of(dlqTopicResource, List.of(new AlterConfigOp(
                    new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(raisedDlqMaxMessageBytes)),
                    AlterConfigOp.OpType.SET))),
                new AlterConfigsOptions()
            ).all().get();
            waitForCondition(() -> {
                Config config = admin.describeConfigs(List.of(dlqTopicResource)).all().get().get(dlqTopicResource);
                ConfigEntry entry = config.get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
                return entry != null && entry.value().equals(Integer.toString(raisedDlqMaxMessageBytes));
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Raised max.message.bytes did not propagate on the DLQ topic");
        }

        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < recordCount; i++) {
                producer.send(new ProducerRecord<>(sourceTopic, 0, "key".getBytes(StandardCharsets.UTF_8), payload));
            }
            producer.flush();
        }
        rejectRecords(groupId, sourceTopic, recordCount);

        List<ConsumerRecord<byte[], byte[]>> secondBatchDlqRecords = readDlqPartition(dlqTopic, 0, recordCount * 2);
        assertEquals(recordCount * 2, secondBatchDlqRecords.size(),
            "Unexpected number of records on the DLQ topic after the second batch");

        waitForCondition(() -> dlqMeterCount(METRIC_DLQ_PRODUCE_TOTAL, groupId) == produceCountBeforeRaise + 1,
            DEFAULT_MAX_WAIT_MS, 200L,
            () -> "Expected exactly 1 additional DLQ produce request for the second batch (budget no longer forces "
                + "chunking), count went from " + produceCountBeforeRaise + " to " + dlqMeterCount(METRIC_DLQ_PRODUCE_TOTAL, groupId));
    }

    /**
     * Guards against a decompression-bomb-shaped source record: a highly compressible record that is tiny
     * on the wire but expands to a much larger size once decompressed. The DLQ record fetcher bounds the
     * decompression budget it will spend copying a record by the DLQ topic's own {@code max.message.bytes}
     * (there is no point retaining more decompressed data than the DLQ topic could ever accept anyway).
     *
     * <p>Phase 1: the DLQ topic is configured with a deliberately low {@code max.message.bytes}. Two records
     * with a highly compressible ~200KB payload (tiny once gzip-compressed) are rejected with record copy
     * enabled. The decompression budget (derived from the low limit) is exhausted well before the payload can
     * be fully decompressed, so the copy is skipped - but the DLQ write itself still succeeds, headers-only
     * (same degraded outcome as record-copy-disabled): the DLQ metrics still fire and the records still land
     * on the DLQ topic, just without a key/value.
     *
     * <p>Phase 2: the DLQ topic's {@code max.message.bytes} is raised comfortably above the decompressed
     * payload size, and a fresh batch of the same records is rejected. This time the decompression budget is
     * sufficient, so the copy succeeds and the new DLQ records carry the original key/value.
     */
    @ClusterTest
    public void testDlqCopyRecordSkippedWhenDecompressedSizeExceedsDlqMaxMessageBytes() throws Exception {
        String groupId = "dlq-decompress-cap-group";
        String sourceTopic = "dlq-decompress-cap-source";
        String dlqTopic = "dlq.decompress-cap";
        int recordCount = 2;
        int payloadSize = 200_000;
        int lowDlqMaxMessageBytes = 2_000;
        // Comfortably above the two records' combined decompressed payload (2 * payloadSize = 400,000 bytes)
        // plus batch/record framing overhead.
        int highDlqMaxMessageBytes = 500_000;

        try (Admin admin = createAdminClient()) {
            admin.createTopics(Set.of(
                new NewTopic(sourceTopic, 1, (short) 1),
                new NewTopic(dlqTopic, 1, (short) 1)
                    .configs(Map.of(
                        TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true",
                        TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(lowDlqMaxMessageBytes)))
            )).all().get();
        }

        alterShareAutoOffsetReset(groupId, "earliest");
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic);
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG, "true");

        // Highly compressible payload: tiny on the wire (comfortably under any max.message.bytes involved),
        // but decompresses to `payloadSize` bytes - large enough to blow past lowDlqMaxMessageBytes once the
        // DLQ record fetcher tries to decompress it for copying.
        byte[] payload = new byte[payloadSize];
        Arrays.fill(payload, (byte) 'a');
        try (Producer<byte[], byte[]> producer = createProducer(Map.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"))) {
            for (int i = 0; i < recordCount; i++) {
                producer.send(new ProducerRecord<>(sourceTopic, 0, "key".getBytes(StandardCharsets.UTF_8), payload));
            }
            producer.flush();
        }
        rejectRecords(groupId, sourceTopic, recordCount);

        // Phase 1: DLQ write succeeds (headers-only) despite the low limit - copy is skipped, not the write.
        waitForCondition(() -> dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId) == recordCount,
            DEFAULT_MAX_WAIT_MS, 200L,
            () -> "Expected " + recordCount + " DLQ records, was " + dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId));
        List<ConsumerRecord<byte[], byte[]>> dlqRecords = readDlqPartition(dlqTopic, 0, recordCount);
        assertEquals(recordCount, dlqRecords.size(), "Unexpected number of records on the DLQ topic");
        for (ConsumerRecord<byte[], byte[]> record : dlqRecords) {
            assertNull(record.key(), "DLQ record key should be absent - record copy must have been skipped");
            assertNull(record.value(), "DLQ record value should be absent - record copy must have been skipped");
            assertEquals(groupId, headerValue(record, HEADER_DLQ_ERRORS_GROUP));
            assertEquals(sourceTopic, headerValue(record, HEADER_DLQ_ERRORS_TOPIC));
        }

        // Phase 2: point the group at a second, freshly-created DLQ topic whose max.message.bytes is set
        // comfortably above the decompressed payload size up front - simpler than altering the first
        // topic's config and waiting for it to propagate.
        String dlqTopic2 = "dlq.decompress-cap-2";
        try (Admin admin = createAdminClient()) {
            admin.createTopics(Set.of(
                new NewTopic(dlqTopic2, 1, (short) 1)
                    .configs(Map.of(
                        TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true",
                        TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(highDlqMaxMessageBytes)))
            )).all().get();
        }
        alterShareGroupConfig(groupId, GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic2);

        try (Producer<byte[], byte[]> producer = createProducer(Map.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"))) {
            for (int i = 0; i < recordCount; i++) {
                producer.send(new ProducerRecord<>(sourceTopic, 0, "key".getBytes(StandardCharsets.UTF_8), payload));
            }
            producer.flush();
        }
        rejectRecords(groupId, sourceTopic, recordCount);

        // The budget is now sufficient, so the second batch's DLQ records (on the new topic) must carry the
        // copied payload. The per-group metric accumulates across both DLQ topics used by this group.
        waitForCondition(() -> dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId) == recordCount * 2,
            DEFAULT_MAX_WAIT_MS, 200L,
            () -> "Expected " + (recordCount * 2) + " DLQ records, was " + dlqMeterCount(METRIC_DLQ_RECORD_COUNT, groupId));
        List<ConsumerRecord<byte[], byte[]>> secondBatchDlqRecords = readDlqPartition(dlqTopic2, 0, recordCount);
        assertEquals(recordCount, secondBatchDlqRecords.size(),
            "Unexpected number of records on the second DLQ topic");
        for (ConsumerRecord<byte[], byte[]> record : secondBatchDlqRecords) {
            assertArrayEquals(payload, record.value(), "DLQ record value should be the copied payload");
            assertEquals(groupId, headerValue(record, HEADER_DLQ_ERRORS_GROUP));
            assertEquals(sourceTopic, headerValue(record, HEADER_DLQ_ERRORS_TOPIC));
        }
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
        verifyDlqTopicRecords(dlqTopic, groupId, tp.topic(), tp.partition(), expectedSourceOffsets, copyEnabled);
    }

    /**
     * As {@link #verifyDlqTopicRecords(String, String, Set, boolean)}, but for an explicit source topic-partition
     * (rather than the base {@code tp}). The DLQ records' context headers must reference {@code sourceTopic} /
     * {@code sourcePartition}.
     */
    private void verifyDlqTopicRecords(String dlqTopic, String groupId, String sourceTopic, int sourcePartition,
                                       Set<Long> expectedSourceOffsets, boolean copyEnabled) throws InterruptedException {
        List<ConsumerRecord<byte[], byte[]>> dlqRecords = readDlqPartition(dlqTopic, 0, expectedSourceOffsets.size());

        assertEquals(expectedSourceOffsets.size(), dlqRecords.size(), "Unexpected number of records on the DLQ topic");
        Set<Long> actualSourceOffsets = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> record : dlqRecords) {
            if (copyEnabled) {
                // produceMessages()/produceTo() produce records with key "key" and value "value".
                assertEquals("key", new String(Objects.requireNonNull(record.key()), StandardCharsets.UTF_8),
                    "DLQ record key should be copied when record copy is enabled");
                assertEquals("value", new String(Objects.requireNonNull(record.value()), StandardCharsets.UTF_8),
                    "DLQ record value should be copied when record copy is enabled");
            } else {
                // Record copy is disabled, so only headers are written - the value (and key) are null.
                assertNull(record.value(), "DLQ record value should be null when record copy is disabled");
            }
            assertEquals(groupId, headerValue(record, HEADER_DLQ_ERRORS_GROUP));
            assertEquals(sourceTopic, headerValue(record, HEADER_DLQ_ERRORS_TOPIC));
            assertEquals(Integer.toString(sourcePartition), headerValue(record, HEADER_DLQ_ERRORS_PARTITION));
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

    // Creates a single-partition source topic with tiered storage enabled and one log segment per record (via
    // per-record index entries). A short local retention (`localRetentionMs`) deletes inactive segments from
    // local storage soon after they are offloaded to remote storage, so reading those offsets must hit remote
    // storage; the total retention (`retentionMs`) is kept generous so the remote segments are not deleted while
    // the test is still running.
    private void createRemoteStorageSourceTopic(String topic, long retentionMs, long localRetentionMs) {
        assertDoesNotThrow(() -> {
            try (Admin admin = createAdminClient()) {
                Map<String, String> configs = Map.of(
                    TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
                    TopicConfig.RETENTION_MS_CONFIG, Long.toString(retentionMs),
                    TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, Long.toString(localRetentionMs),
                    // Roll a segment for every record so each inactive segment can be offloaded then deleted locally.
                    TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "1",
                    TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, "12");
                admin.createTopics(Set.of(new NewTopic(topic, 1, (short) 1).configs(configs))).all().get();
            }
        }, "Failed to create remote-storage source topic");
    }

    // The earliest offset still held in local storage. Offsets below this have been removed locally (e.g. after
    // being offloaded to remote storage), so reading them must hit remote storage. Used to confirm tiering.
    private long earliestLocalOffset(String topic, int partition) throws Exception {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        try (Admin admin = createAdminClient()) {
            return admin.listOffsets(Map.of(topicPartition, OffsetSpec.earliestLocal()))
                .partitionResult(topicPartition).get().offset();
        }
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
