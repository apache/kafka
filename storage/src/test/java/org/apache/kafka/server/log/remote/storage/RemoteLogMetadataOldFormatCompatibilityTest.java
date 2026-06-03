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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataManagerTestUtils;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataTopicPartitioner;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;

import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify backward compatibility: new code can handle old message format (messages without keys).
 *
 * Background:
 * - Old code (before commit b92795741b) wrote messages with null keys
 * - New code writes messages with keys in format: "topicId:partition:endOffset:brokerLeaderEpoch"
 * - Consumer (ConsumerTask) only reads the value field, not the key
 * - Therefore, new code can read old messages without keys
 *
 * Upgrade strategy:
 * - Existing clusters: topic starts with delete policy, old messages (null keys) exist
 * - After upgrade: new messages written with keys
 * - Old messages expire via time-based retention (24h default)
 * - After retention period: topic can be safely changed to compacted policy
 */
@ClusterTestDefaults(brokers = 3)
public class RemoteLogMetadataOldFormatCompatibilityTest {
    private static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataOldFormatCompatibilityTest.class);
    private static final String METADATA_TOPIC = "__remote_log_metadata";
    private static final int SEG_SIZE = 1048576;

    private final ClusterInstance clusterInstance;
    private final Time time = Time.SYSTEM;
    private TopicBasedRemoteLogMetadataManager remoteLogMetadataManager;

    RemoteLogMetadataOldFormatCompatibilityTest(ClusterInstance clusterInstance) {
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
     * Test upgrade scenario: topic starts with delete policy (old messages with null keys),
     * then new messages with keys are written, and finally topic is changed to compacted.
     */
    @ClusterTest
    public void testUpgradeScenarioWithMixedMessageFormats() throws Exception {
        TopicIdPartition topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-upgrade-scenario", 0)
        );
        int brokerLeaderEpoch = 1;

        initializeOldCluster(topicIdPartition);
        SegmentIds segmentIds = writeOldFormatMessages(topicIdPartition, brokerLeaderEpoch);
        TopicBasedRemoteLogMetadataManager rlmm2 = upgradeToNewCode(topicIdPartition);
        reEmitStatesWithKeys(topicIdPartition, rlmm2, segmentIds, brokerLeaderEpoch);
        writeNewFormatMessages(topicIdPartition, rlmm2, brokerLeaderEpoch);
        changeTopicToCompactedAndVerify(topicIdPartition, rlmm2, brokerLeaderEpoch);
        log.info("✅ Test passed! Upgrade scenario with mixed message formats works correctly.");
    }

    private TopicBasedRemoteLogMetadataManager initializeOldCluster(TopicIdPartition topicIdPartition) throws Exception {
        log.info("Step 1: Initializing RLMM with delete policy...");
        TopicBasedRemoteLogMetadataManager rlmm = createManager();
        rlmm.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());
        waitForInitialization(rlmm, topicIdPartition);
        changeTopicToDeletePolicy();
        rlmm.close();
        remoteLogMetadataManager = null;
        Thread.sleep(2000);
        return rlmm;
    }

    private SegmentIds writeOldFormatMessages(TopicIdPartition topicIdPartition, int brokerLeaderEpoch) throws Exception {
        log.info("Step 2: Writing old format messages (null key)...");
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long endOffset1 = 500L;
        long endOffset2 = 1000L;

        writeMessageWithNullKey(topicIdPartition, new RemoteLogSegmentMetadata(
                segmentId1, 0L, endOffset1, -1L, 0, time.milliseconds(), SEG_SIZE,
                Collections.singletonMap(0, 0L), brokerLeaderEpoch));

        writeMessageWithNullKey(topicIdPartition, new RemoteLogSegmentMetadata(
                segmentId2, 501L, endOffset2, -1L, 0, time.milliseconds(), SEG_SIZE,
                Collections.singletonMap(0, 501L), brokerLeaderEpoch));

        writeUpdateWithNullKey(topicIdPartition, new RemoteLogSegmentMetadataUpdate(
                segmentId2, time.milliseconds(), Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0, brokerLeaderEpoch, endOffset2));

        writeUpdateWithNullKey(topicIdPartition, new RemoteLogSegmentMetadataUpdate(
                segmentId2, time.milliseconds(), Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED, 0, brokerLeaderEpoch, endOffset2));

        Thread.sleep(2000);
        return new SegmentIds(segmentId1, segmentId2, endOffset1, endOffset2);
    }

    private TopicBasedRemoteLogMetadataManager upgradeToNewCode(TopicIdPartition topicIdPartition) throws Exception {
        log.info("Step 3: Upgrading to new code...");
        TopicBasedRemoteLogMetadataManager rlmm2 = createManager();
        rlmm2.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());
        waitForInitialization(rlmm2, topicIdPartition);
        Thread.sleep(3000);
        return rlmm2;
    }

    private void reEmitStatesWithKeys(TopicIdPartition topicIdPartition, TopicBasedRemoteLogMetadataManager rlmm2,
                                      SegmentIds segmentIds, int brokerLeaderEpoch) throws Exception {
        log.info("Step 4: New broker re-emitting states with keys...");
        assertDoesNotThrow(() -> rlmm2.addRemoteLogSegmentMetadata(new RemoteLogSegmentMetadata(
                segmentIds.segmentId1, 0L, segmentIds.endOffset1, -1L, 0, time.milliseconds(), SEG_SIZE,
                Collections.singletonMap(0, 0L), brokerLeaderEpoch)).get());

        Thread.sleep(1000);
        assertDoesNotThrow(() -> rlmm2.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(
                segmentIds.segmentId1, time.milliseconds(), Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0, brokerLeaderEpoch, segmentIds.endOffset1)).get());

        Thread.sleep(1000);
        Optional<RemoteLogSegmentMetadata> retrieved = rlmm2.remoteLogSegmentMetadata(topicIdPartition, 0, 250);
        assertTrue(retrieved.isPresent());
        assertEquals(segmentIds.segmentId1, retrieved.get().remoteLogSegmentId());

        assertDoesNotThrow(() -> rlmm2.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(
                segmentIds.segmentId2, time.milliseconds(), Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED, 0, brokerLeaderEpoch, segmentIds.endOffset2)).get());

        Thread.sleep(1000);
        assertDoesNotThrow(() -> rlmm2.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(
                segmentIds.segmentId2, time.milliseconds(), Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, 0, brokerLeaderEpoch, segmentIds.endOffset2)).get());
    }

    private void writeNewFormatMessages(TopicIdPartition topicIdPartition, TopicBasedRemoteLogMetadataManager rlmm2,
                                       int brokerLeaderEpoch) throws Exception {
        log.info("Step 5: Writing new format messages...");
        RemoteLogSegmentId newSegmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long newEndOffset = 1500L;

        assertDoesNotThrow(() -> rlmm2.addRemoteLogSegmentMetadata(new RemoteLogSegmentMetadata(
                newSegmentId, 1001L, newEndOffset, -1L, 0, time.milliseconds(), SEG_SIZE,
                Collections.singletonMap(0, 1001L), brokerLeaderEpoch)).get());

        assertDoesNotThrow(() -> rlmm2.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(
                newSegmentId, time.milliseconds(), Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0, brokerLeaderEpoch, newEndOffset)).get());

        Thread.sleep(1000);
    }

    private void changeTopicToCompactedAndVerify(TopicIdPartition topicIdPartition,
                                                 TopicBasedRemoteLogMetadataManager rlmm2,
                                                 int brokerLeaderEpoch) throws Exception {
        log.info("Step 6: Changing to compacted policy and verifying...");
        changeTopicToCompactedPolicy();

        RemoteLogSegmentId thirdSegmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        assertDoesNotThrow(() -> rlmm2.addRemoteLogSegmentMetadata(new RemoteLogSegmentMetadata(
                thirdSegmentId, 1501L, 2500L, -1L, 0, time.milliseconds(), SEG_SIZE,
                Collections.singletonMap(0, 1501L), brokerLeaderEpoch)).get());
    }

    private static class SegmentIds {
        final RemoteLogSegmentId segmentId1;
        final RemoteLogSegmentId segmentId2;
        final long endOffset1;
        final long endOffset2;

        SegmentIds(RemoteLogSegmentId segmentId1, RemoteLogSegmentId segmentId2, long endOffset1, long endOffset2) {
            this.segmentId1 = segmentId1;
            this.segmentId2 = segmentId2;
            this.endOffset1 = endOffset1;
            this.endOffset2 = endOffset2;
        }
    }

    private void writeMessageWithNullKey(TopicIdPartition topicIdPartition,
                                         RemoteLogSegmentMetadata metadata) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
            byte[] value = serde.serialize(metadata);

            RemoteLogMetadataTopicPartitioner partitioner = new RemoteLogMetadataTopicPartitioner(3);
            int metadataPartition = partitioner.metadataPartition(topicIdPartition);

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    METADATA_TOPIC,
                    metadataPartition,
                    null,  // Old format: null key
                    value
            );

            producer.send(record).get();
            producer.flush();
        }
    }

    private void writeUpdateWithNullKey(TopicIdPartition topicIdPartition,
                                        RemoteLogSegmentMetadataUpdate update) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
            byte[] value = serde.serialize(update);

            RemoteLogMetadataTopicPartitioner partitioner = new RemoteLogMetadataTopicPartitioner(3);
            int metadataPartition = partitioner.metadataPartition(topicIdPartition);

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    METADATA_TOPIC,
                    metadataPartition,
                    null,  // Old format: null key
                    value
            );

            producer.send(record).get();
            producer.flush();
        }
    }

    private void changeTopicToDeletePolicy() throws Exception {
        try (Admin admin = Admin.create(Collections.singletonMap(
                "bootstrap.servers", clusterInstance.bootstrapServers()))) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, METADATA_TOPIC);

            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(resource, Collections.singletonList(
                    new AlterConfigOp(
                            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
                            AlterConfigOp.OpType.SET
                    )
            ));

            admin.incrementalAlterConfigs(configs).all().get();
            Thread.sleep(2000); // Wait for config change to propagate
        }
    }

    private void changeTopicToCompactedPolicy() throws Exception {
        try (Admin admin = Admin.create(Collections.singletonMap(
                "bootstrap.servers", clusterInstance.bootstrapServers()))) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, METADATA_TOPIC);

            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(resource, Collections.singletonList(
                    new AlterConfigOp(
                            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT),
                            AlterConfigOp.OpType.SET
                    )
            ));

            admin.incrementalAlterConfigs(configs).all().get();
            Thread.sleep(2000); // Wait for config change to propagate
        }
    }

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
}
