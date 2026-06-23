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
        @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1")
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
        rejectAllRecords(groupId, recordCount);

        // Verify by reading from the DLQ topic, then assert the DLQ metrics.
        verifyDlqTopicRecords(dlqTopic, groupId, expectedSourceOffsets(recordCount));
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
        rejectAllRecords(groupId, recordCount);

        // Verify the DLQ topic was auto-created (with DLQ enabled), received the records, and metrics fired.
        verifyDlqTopicCreated(dlqTopic);
        verifyDlqTopicRecords(dlqTopic, groupId, expectedSourceOffsets(recordCount));
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

        verifyDlqTopicRecords(dlqTopic, groupId, expectedSourceOffsets(recordCount));
        verifyDlqMetrics(groupId, recordCount);
    }

    // Consumes from the source topic in EXPLICIT acknowledgement mode and rejects every record.
    private void rejectAllRecords(String groupId, int recordCount) {
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
            groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            int rejected = 0;
            long deadlineMs = System.currentTimeMillis() + DEFAULT_MAX_WAIT_MS;
            while (rejected < recordCount && System.currentTimeMillis() < deadlineMs) {
                ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    shareConsumer.acknowledge(record, AcknowledgeType.REJECT);
                    rejected++;
                }
                if (records.count() > 0) {
                    shareConsumer.commitSync(Duration.ofMillis(10000));
                }
            }
            assertEquals(recordCount, rejected, "Expected to reject all produced records");
        }
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
     * Reads the DLQ topic and asserts it received exactly one record per expected source offset. Record copy
     * is disabled, so each DLQ record carries only the context headers (no key/value).
     *
     * @param dlqTopic              the DLQ topic to read from (single partition)
     * @param groupId               the share group the rejected records belonged to
     * @param expectedSourceOffsets the source offsets expected to have been written to the DLQ
     */
    private void verifyDlqTopicRecords(String dlqTopic, String groupId, Set<Long> expectedSourceOffsets) throws InterruptedException {
        TopicPartition dlqTp = new TopicPartition(dlqTopic, 0);
        List<ConsumerRecord<byte[], byte[]>> dlqRecords = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = cluster.consumer()) {
            consumer.assign(List.of(dlqTp));
            consumer.seekToBeginning(List.of(dlqTp));
            waitForCondition(() -> {
                dlqRecords.addAll(consumer.poll(Duration.ofMillis(1000)).records(dlqTp));
                return dlqRecords.size() >= expectedSourceOffsets.size();
            }, DEFAULT_MAX_WAIT_MS, 500L, () -> "DLQ topic did not receive " + expectedSourceOffsets.size() + " records, got " + dlqRecords.size());
        }

        assertEquals(expectedSourceOffsets.size(), dlqRecords.size(), "Unexpected number of records on the DLQ topic");
        Set<Long> actualSourceOffsets = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> record : dlqRecords) {
            // Record copy is disabled, so only headers are written - the value (and key) are null.
            assertNull(record.value(), "DLQ record value should be null when record copy is disabled");
            assertEquals(groupId, headerValue(record, HEADER_DLQ_ERRORS_GROUP));
            assertEquals(tp.topic(), headerValue(record, HEADER_DLQ_ERRORS_TOPIC));
            assertEquals(Integer.toString(tp.partition()), headerValue(record, HEADER_DLQ_ERRORS_PARTITION));
            actualSourceOffsets.add(Long.parseLong(Objects.requireNonNull(headerValue(record, HEADER_DLQ_ERRORS_OFFSET))));
        }
        assertEquals(expectedSourceOffsets, actualSourceOffsets, "DLQ records should cover every expected source offset");
    }

    private void createDlqTopic(String topicName) {
        assertDoesNotThrow(() -> {
            try (Admin admin = createAdminClient()) {
                NewTopic newTopic = new NewTopic(topicName, 1, (short) 1)
                    .configs(Map.of(TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true"));
                admin.createTopics(Set.of(newTopic)).all().get();
            }
        }, "Failed to create DLQ topic");
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
