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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataManagerTestUtils;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;

import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for verifying tombstoning behavior of remote log metadata topic.
 * This test validates that:
 * 1. Metadata records are written with correct keys (topicId:partition:endOffset:brokerLeaderEpoch)
 * 2. Tombstones are published for all historical records when segment is deleted
 * 3. Topic compaction eventually removes tombstoned records
 */
@ClusterTestDefaults(brokers = 3)
public class RemoteLogMetadataTombstoneTest {
    private static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataTombstoneTest.class);
    private static final int SEG_SIZE = 1048576;
    private static final String METADATA_TOPIC = "__remote_log_metadata";

    private final ClusterInstance clusterInstance;
    private final Time time = Time.SYSTEM;
    private TopicBasedRemoteLogMetadataManager remoteLogMetadataManager;

    RemoteLogMetadataTombstoneTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private TopicBasedRemoteLogMetadataManager createManager() {
        if (remoteLogMetadataManager == null) {
            remoteLogMetadataManager = RemoteLogMetadataManagerTestUtils.builder()
                    .bootstrapServers(clusterInstance.bootstrapServers())
                    .build();
        }
        return remoteLogMetadataManager;
    }

    @AfterEach
    public void teardown() throws IOException {
        if (remoteLogMetadataManager != null) {
            remoteLogMetadataManager.close();
        }
    }

    /**
     * Test the full lifecycle of remote log segment metadata with tombstoning.
     *
     * This test creates a single segment and uploads it. Then it deletes the segment
     * and verifies that the metadata record is tombstoned in the __remote_log_metadata topic.
     */
    @ClusterTest
    public void testRemoteLogSegmentLifecycleWithTombstoning() throws Exception {
        TopicBasedRemoteLogMetadataManager rlmm = createManager();

        // Create test topic partition
        TopicIdPartition topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-topic", 0)
        );

        // Register partition as leader
        rlmm.onPartitionLeadershipChanges(
                Collections.singleton(topicIdPartition),
                Collections.emptySet()
        );

        // Wait for initialization
        waitForInitialization(rlmm, topicIdPartition);

        // Create segment ID
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long endOffset = 1000L;
        int brokerLeaderEpoch = 1;

        // ===== Phase 1: Upload segment =====
        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(
                segmentId,
                0L,             // startOffset
                endOffset,      // endOffset
                -1L,            // maxTimestampMs
                0,              // brokerId
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 0L),  // segmentLeaderEpochs
                brokerLeaderEpoch
        );

        // Add metadata (COPY_SEGMENT_STARTED state)
        rlmm.addRemoteLogSegmentMetadata(metadata).get();

        // Update to COPY_SEGMENT_FINISHED
        RemoteLogSegmentMetadataUpdate update = new RemoteLogSegmentMetadataUpdate(
                segmentId,
                time.milliseconds(),
                java.util.Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                0,  // brokerId
                brokerLeaderEpoch,
                endOffset
        );
        rlmm.updateRemoteLogSegmentMetadata(update).get();

        // ===== Phase 2: Verify metadata topic has the record =====
        Map<String, byte[]> recordsBeforeDeletion = consumeMetadataTopicRecords(topicIdPartition);

        log.info("Records before deletion:");
        for (String key : recordsBeforeDeletion.keySet()) {
            log.info("  Key: {}, Value: {}", key,
                    (recordsBeforeDeletion.get(key) != null ? "present" : "null"));
        }

        String baseKey = buildExpectedKey(topicIdPartition, endOffset, brokerLeaderEpoch);
        String updateKey = baseKey + ":UPDATE";

        // Should have both base key (from COPY_SEGMENT_STARTED) and update key (from COPY_SEGMENT_FINISHED)
        assertTrue(recordsBeforeDeletion.containsKey(baseKey),
                "Should have base key for brokerLeaderEpoch=" + brokerLeaderEpoch);
        assertTrue(recordsBeforeDeletion.containsKey(updateKey),
                "Should have update key for brokerLeaderEpoch=" + brokerLeaderEpoch);
        assertNotNull(recordsBeforeDeletion.get(baseKey),
                "Base key should not be tombstone");
        assertNotNull(recordsBeforeDeletion.get(updateKey),
                "Update key should not be tombstone");

        // ===== Phase 3: Delete the segment =====
        RemoteLogSegmentMetadataUpdate deleteStart = new RemoteLogSegmentMetadataUpdate(
                segmentId,
                time.milliseconds(),
                java.util.Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED,
                0,  // brokerId
                brokerLeaderEpoch,
                endOffset
        );
        rlmm.updateRemoteLogSegmentMetadata(deleteStart).get();

        // Mark deletion as finished - this should trigger tombstoning
        RemoteLogSegmentMetadataUpdate deleteFinish = new RemoteLogSegmentMetadataUpdate(
                segmentId,
                time.milliseconds(),
                java.util.Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_FINISHED,
                0,  // brokerId
                brokerLeaderEpoch,
                endOffset
        );
        rlmm.updateRemoteLogSegmentMetadata(deleteFinish).get();

        // ===== Phase 4: Wait for tombstones to appear =====
        // Both base key and update key should be tombstoned
        waitForTombstone(topicIdPartition, baseKey, 10000);
        waitForTombstone(topicIdPartition, updateKey, 10000);

        // Verify both tombstones were published
        Map<String, byte[]> recordsAfterDeletion = consumeMetadataTopicRecords(topicIdPartition);

        log.info("Records after deletion:");
        for (String key : recordsAfterDeletion.keySet()) {
            log.info("  Key: {}, Value: {}", key,
                    (recordsAfterDeletion.get(key) != null ? "present" : "TOMBSTONE"));
        }

        // Both keys should still exist but with null values (tombstones)
        assertTrue(recordsAfterDeletion.containsKey(baseKey),
                "Should still have base key for brokerLeaderEpoch=" + brokerLeaderEpoch);
        assertTrue(recordsAfterDeletion.containsKey(updateKey),
                "Should still have update key for brokerLeaderEpoch=" + brokerLeaderEpoch);
        assertNull(recordsAfterDeletion.get(baseKey),
                "Base key should be tombstoned");
        assertNull(recordsAfterDeletion.get(updateKey),
                "Update key should be tombstoned");
    }

    /**
     * Test leadership change during segment upload and subsequent tombstoning.
     *
     * Scenario:
     * 1. Leader 1 starts uploading segment (COPY_SEGMENT_STARTED with brokerLeaderEpoch=1)
     * 2. Leadership changes before upload finishes
     * 3. Leader 2 retries and finishes upload (COPY_SEGMENT_FINISHED with brokerLeaderEpoch=2)
     * 4. Leader 2 deletes the segment
     * 5. Verify tombstones are published for both brokerLeaderEpoch=1 and brokerLeaderEpoch=2
     */
    @ClusterTest
    public void testLeadershipChangeDuringUploadWithTombstoning() throws Exception {
        TopicBasedRemoteLogMetadataManager rlmm = createManager();

        // Create test topic partition
        TopicIdPartition topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-topic-leadership", 0)
        );

        // Register partition as leader
        rlmm.onPartitionLeadershipChanges(
                Collections.singleton(topicIdPartition),
                Collections.emptySet()
        );

        // Wait for initialization
        waitForInitialization(rlmm, topicIdPartition);

        // Create segment ID
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long endOffset = 2000L;

        // ===== Phase 1: Leader 1 starts uploading segment =====
        int brokerLeaderEpoch1 = 1;
        RemoteLogSegmentMetadata metadata1 = new RemoteLogSegmentMetadata(
                segmentId,
                0L,
                endOffset,
                -1L,
                0,  // broker 0
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 0L),
                brokerLeaderEpoch1
        );

        // Leader 1 adds metadata (COPY_SEGMENT_STARTED state)
        rlmm.addRemoteLogSegmentMetadata(metadata1).get();

        // Simulate leadership change - Leader 1 never finishes the upload
        // In real scenario, the segment stays in COPY_SEGMENT_STARTED state
        // ===== Phase 2: Leadership changes to Leader 2 =====
        // Leader 2 sees segment in COPY_SEGMENT_STARTED, retries and finishes upload
        int brokerLeaderEpoch2 = 2;
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());

        RemoteLogSegmentMetadata metadata2 = new RemoteLogSegmentMetadata(
                segmentId2,
                0L,
                endOffset,
                -1L,
                1,  // broker 1
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 0L),
                brokerLeaderEpoch2
        );

        // Leader 1 adds metadata (COPY_SEGMENT_STARTED state)
        rlmm.addRemoteLogSegmentMetadata(metadata2).get();

        // Leader 2 may re-add the metadata or just update it
        // In practice, it will update the existing segment with new brokerLeaderEpoch
        RemoteLogSegmentMetadataUpdate update2Finish = new RemoteLogSegmentMetadataUpdate(
                segmentId2,
                time.milliseconds(),
                java.util.Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                1,  // broker 1 (new leader)
                brokerLeaderEpoch2,
                endOffset
        );
        rlmm.updateRemoteLogSegmentMetadata(update2Finish).get();

        // ===== Phase 3: Verify metadata topic has records for both epochs =====
        Map<String, byte[]> recordsBeforeDeletion = consumeMetadataTopicRecords(topicIdPartition);

        log.info("Records before deletion (leadership change scenario):");
        for (String key : recordsBeforeDeletion.keySet()) {
            log.info("  Key: {}, Value: {}", key,
                    (recordsBeforeDeletion.get(key) != null ? "present" : "null"));
        }

        // We should have records for both brokerLeaderEpoch=1 and brokerLeaderEpoch=2
        // Each has a base key and an update key
        String baseKey1 = buildExpectedKey(topicIdPartition, endOffset, brokerLeaderEpoch1);
        String baseKey2 = buildExpectedKey(topicIdPartition, endOffset, brokerLeaderEpoch2);
        String updateKey2 = baseKey2 + ":UPDATE";

        assertTrue(recordsBeforeDeletion.containsKey(updateKey2),
                "Should have update record for brokerLeaderEpoch=" + brokerLeaderEpoch2);

        // ===== Phase 4: Leader 2 deletes the segment =====
        RemoteLogSegmentMetadataUpdate deleteStart = new RemoteLogSegmentMetadataUpdate(
                segmentId2,
                time.milliseconds(),
                java.util.Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED,
                1,  // broker 1
                brokerLeaderEpoch2,
                endOffset
        );
        rlmm.updateRemoteLogSegmentMetadata(deleteStart).get();

        RemoteLogSegmentMetadataUpdate deleteFinish = new RemoteLogSegmentMetadataUpdate(
                segmentId2,
                time.milliseconds(),
                java.util.Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_FINISHED,
                1,  // broker 1
                brokerLeaderEpoch2,
                endOffset
        );
        rlmm.updateRemoteLogSegmentMetadata(deleteFinish).get();

        // ===== Phase 5: Wait for tombstones to appear =====
        // All 4 keys should be tombstoned (base and update for both epochs)
        waitForTombstone(topicIdPartition, baseKey1, 10000);
        waitForTombstone(topicIdPartition, baseKey2, 10000);
        waitForTombstone(topicIdPartition, updateKey2, 10000);

        // Verify tombstones for both epochs
        Map<String, byte[]> recordsAfterDeletion = consumeMetadataTopicRecords(topicIdPartition);

        log.info("Records after deletion (leadership change scenario):");
        for (String key : recordsAfterDeletion.keySet()) {
            log.info("  Key: {}, Value: {}", key,
                    (recordsAfterDeletion.get(key) != null ? "present" : "TOMBSTONE"));
        }

        // All keys should be tombstoned because brokerLeaderEpoch1 <= brokerLeaderEpoch2
        assertTrue(recordsAfterDeletion.containsKey(baseKey1),
                "Should still have base key for brokerLeaderEpoch=" + brokerLeaderEpoch1);
        assertTrue(recordsAfterDeletion.containsKey(baseKey2),
                "Should still have base key for brokerLeaderEpoch=" + brokerLeaderEpoch2);
        assertTrue(recordsAfterDeletion.containsKey(updateKey2),
                "Should still have update key for brokerLeaderEpoch=" + brokerLeaderEpoch2);

        assertNull(recordsAfterDeletion.get(baseKey1),
                "Base key with brokerLeaderEpoch=" + brokerLeaderEpoch1 + " should be tombstoned");
        assertNull(recordsAfterDeletion.get(baseKey2),
                "Base key with brokerLeaderEpoch=" + brokerLeaderEpoch2 + " should be tombstoned");
        assertNull(recordsAfterDeletion.get(updateKey2),
                "Update key with brokerLeaderEpoch=" + brokerLeaderEpoch2 + " should be tombstoned");

        assertEquals(4, recordsAfterDeletion.size(),
                "Should have exactly 4 tombstone records (base and update for each broker leader epoch)");
    }

    /**
     * Test that only records with brokerLeaderEpoch <= current are tombstoned
     */
    @ClusterTest
    public void testTombstoningOnlyAffectsLowerOrEqualEpochs() throws Exception {
        TopicBasedRemoteLogMetadataManager rlmm = createManager();

        TopicIdPartition topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-topic-2", 0)
        );

        rlmm.onPartitionLeadershipChanges(
                Collections.singleton(topicIdPartition),
                Collections.emptySet()
        );

        waitForInitialization(rlmm, topicIdPartition);

        long endOffset = 2000L;

        // Create two segments with same endOffset but different broker leader epochs
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());

        // Segment 1: broker leader epoch = 1
        RemoteLogSegmentMetadata metadata1 = new RemoteLogSegmentMetadata(
                segmentId1, 0L, endOffset, -1L, 0, time.milliseconds(), SEG_SIZE,
                Collections.singletonMap(0, 0L), 1
        );
        rlmm.addRemoteLogSegmentMetadata(metadata1).get();
        rlmm.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(
                segmentId1, time.milliseconds(), java.util.Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0, 1, endOffset
        )).get();

        // Segment 2: broker leader epoch = 3 (higher)
        RemoteLogSegmentMetadata metadata2 = new RemoteLogSegmentMetadata(
                segmentId2, 0L, endOffset, -1L, 0, time.milliseconds(), SEG_SIZE,
                Collections.singletonMap(0, 0L), 3
        );
        rlmm.addRemoteLogSegmentMetadata(metadata2).get();
        rlmm.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(
                segmentId2, time.milliseconds(), java.util.Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0, 3, endOffset
        )).get();

        // Delete segment 1 (broker leader epoch = 1)
        rlmm.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(
                segmentId1, time.milliseconds(), java.util.Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED, 0, 1, endOffset
        )).get();
        rlmm.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(
                segmentId1, time.milliseconds(), java.util.Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, 0, 1, endOffset
        )).get();

        String baseKey1 = buildExpectedKey(topicIdPartition, endOffset, 1);
        String updateKey1 = baseKey1 + ":UPDATE";
        String baseKey3 = buildExpectedKey(topicIdPartition, endOffset, 3);
        String updateKey3 = baseKey3 + ":UPDATE";

        // Wait for tombstones to appear for segment 1 (both base and update keys)
        waitForTombstone(topicIdPartition, baseKey1, 10000);
        waitForTombstone(topicIdPartition, updateKey1, 10000);

        // Verify: only segment 1's keys should be tombstoned, segment 3 should remain
        Map<String, byte[]> records = consumeMetadataTopicRecords(topicIdPartition);

        assertNull(records.get(baseKey1), "Base key with brokerLeaderEpoch=1 should be tombstoned");
        assertNull(records.get(updateKey1), "Update key with brokerLeaderEpoch=1 should be tombstoned");
        assertNotNull(records.get(baseKey3), "Base key with brokerLeaderEpoch=3 should NOT be tombstoned");
        assertNotNull(records.get(updateKey3), "Update key with brokerLeaderEpoch=3 should NOT be tombstoned");
    }

    // ===== Helper Methods =====

    private void waitForInitialization(TopicBasedRemoteLogMetadataManager rlmm,
                                       TopicIdPartition topicIdPartition) throws InterruptedException {
        int maxWaitMs = 30000;
        int waitedMs = 0;
        while (!rlmm.isReady(topicIdPartition) && waitedMs < maxWaitMs) {
            Thread.sleep(100);
            waitedMs += 100;
        }
        assertTrue(rlmm.isReady(topicIdPartition),
                "RLMM should be initialized within " + maxWaitMs + "ms");
    }

    /**
     * Consumes all records from the metadata topic partition and returns them as a map.
     * Key -> Value (null for tombstones)
     */
    private Map<String, byte[]> consumeMetadataTopicRecords(TopicIdPartition topicIdPartition) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");  // Allow consuming internal topics

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            // Calculate metadata partition using the partitioner
            // The metadata topic has 3 partitions by default (see RemoteLogMetadataManagerTestUtils.METADATA_TOPIC_PARTITIONS_COUNT)
            // Use the same hashing logic as RemoteLogMetadataTopicPartitioner

            List<TopicPartition> allPartitions = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                allPartitions.add(new TopicPartition(METADATA_TOPIC, i));
            }

            consumer.assign(allPartitions);
            consumer.seekToBeginning(allPartitions);

            Map<String, byte[]> records = new HashMap<>();

            // Poll until no more records
            int consecutiveEmptyPolls = 0;
            int maxEmptyPolls = 2;

            while (consecutiveEmptyPolls < maxEmptyPolls) {
                ConsumerRecords<String, byte[]> polled = consumer.poll(Duration.ofMillis(500));

                if (polled.isEmpty()) {
                    consecutiveEmptyPolls++;
                } else {
                    consecutiveEmptyPolls = 0;
                    for (ConsumerRecord<String, byte[]> record : polled) {
                        records.put(record.key(), record.value());
                    }
                }
            }

            return records;
        }
    }

    /**
     * Wait for a tombstone to appear for the given key in the metadata topic.
     * Polls the topic periodically until the tombstone is found or timeout is reached.
     */
    private void waitForTombstone(TopicIdPartition topicIdPartition, String expectedKey, long timeoutMs)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        boolean tombstoneFound = false;

        while (!tombstoneFound && (System.currentTimeMillis() - startTime) < timeoutMs) {
            Map<String, byte[]> records = consumeMetadataTopicRecords(topicIdPartition);
            if (records.containsKey(expectedKey) && records.get(expectedKey) == null) {
                tombstoneFound = true;
                log.debug("Tombstone found for key: {}", expectedKey);
            } else {
                Thread.sleep(500);
            }
        }

        if (!tombstoneFound) {
            log.warn("Tombstone not found within timeout for key: {}", expectedKey);
        }
    }

    /**
     * Build the base metadata key based on the key format:
     * topicId:partition:endOffset:brokerLeaderEpoch
     * Note: Update keys have an additional ":UPDATE" suffix
     */
    private String buildExpectedKey(TopicIdPartition topicIdPartition,
                                    long endOffset,
                                    int brokerLeaderEpoch) {
        return topicIdPartition.topicId() + ":"
                + topicIdPartition.partition() + ":"
                + endOffset + ":"
                + brokerLeaderEpoch;
    }
}