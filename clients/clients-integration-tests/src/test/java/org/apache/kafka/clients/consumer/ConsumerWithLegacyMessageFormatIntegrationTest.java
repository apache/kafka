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

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.BeforeEach;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


@ClusterTestDefaults(
    brokers = 3
)
public class ConsumerWithLegacyMessageFormatIntegrationTest {

    private final ClusterInstance cluster;

    private final String topic1 = "part-test-topic-1";
    private final String topic2 = "part-test-topic-2";
    private final String topic3 = "part-test-topic-3";

    private final TopicPartition t1p0 = new TopicPartition(topic1, 0);
    private final TopicPartition t1p1 = new TopicPartition(topic1, 1);
    private final TopicPartition t2p0 = new TopicPartition(topic2, 0);
    private final TopicPartition t2p1 = new TopicPartition(topic2, 1);
    private final TopicPartition t3p0 = new TopicPartition(topic3, 0);
    private final TopicPartition t3p1 = new TopicPartition(topic3, 1);

    public ConsumerWithLegacyMessageFormatIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    private void appendLegacyRecords(int numRecords, TopicPartition tp, int brokerId, byte magicValue) {
        List<SimpleRecord> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            records.add(new SimpleRecord(i, ("key " + i).getBytes(), ("value " + i).getBytes()));
        }

        ByteBuffer buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue,
                CompressionType.NONE, records));
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer,
                magicValue,
                Compression.of(CompressionType.NONE).build(),
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                0,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH
        );

        records.forEach(builder::append);

        cluster.brokers().values().stream()
                .filter(b -> b.config().brokerId() == brokerId)
                .forEach(b -> {
                    UnifiedLog unifiedLog = b.replicaManager().logManager().getLog(tp, false).get();
                    unifiedLog.appendAsLeaderWithRecordVersion(builder.build(), 0, RecordVersion.lookup(magicValue));
                    // Default isolation.level is read_uncommitted. It makes Partition#fetchOffsetForTimestamp to return UnifiedLog#highWatermark,
                    // so increasing high watermark to make it return the correct offset.
                    assertDoesNotThrow(() -> unifiedLog.maybeIncrementHighWatermark(unifiedLog.logEndOffsetMetadata()));
                });
    }

    private void createTopicWithAssignment(String topic, Map<Integer, List<Integer>> assignment) throws InterruptedException {
        try (Admin admin = cluster.admin()) {
            NewTopic newTopic = new NewTopic(topic, assignment);
            admin.createTopics(List.of(newTopic));
            cluster.waitTopicCreation(topic, assignment.size());
        }
    }

    @BeforeEach
    public void setupTopics() throws InterruptedException {
        cluster.createTopic(topic1, 2, (short) 1);
        createTopicWithAssignment(topic2, Map.of(0, List.of(0), 1, List.of(1)));
        createTopicWithAssignment(topic3, Map.of(0, List.of(0), 1, List.of(1)));

        // v2 message format for topic1
        ClientsTestUtils.sendRecords(cluster, t1p0, 100, 0);
        ClientsTestUtils.sendRecords(cluster, t1p1, 100, 0);
        // v0 message format for topic2
        appendLegacyRecords(100, t2p0, 0, RecordBatch.MAGIC_VALUE_V0);
        appendLegacyRecords(100, t2p1, 1, RecordBatch.MAGIC_VALUE_V0);
        // v1 message format for topic3
        appendLegacyRecords(100, t3p0, 0, RecordBatch.MAGIC_VALUE_V1);
        appendLegacyRecords(100, t3p1, 1, RecordBatch.MAGIC_VALUE_V1);
    }

    @ClusterTest
    public void testOffsetsForTimesWithClassicConsumer() {
        testOffsetsForTimes(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testOffsetsForTimesWithAsyncConsumer() {
        testOffsetsForTimes(GroupProtocol.CONSUMER);
    }

    public void testOffsetsForTimes(GroupProtocol groupProtocol) {
        try (Consumer<Object, Object> consumer = cluster.consumer(Map.of(
                GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT)))
        ) {
            // Test negative target time
            assertThrows(IllegalArgumentException.class, () ->
                    consumer.offsetsForTimes(Map.of(t1p0, -1L)));

            Map<TopicPartition, Long> timestampsToSearch = Map.of(
                    t1p0, 0L,
                    t1p1, 20L,
                    t2p0, 40L,
                    t2p1, 60L,
                    t3p0, 80L,
                    t3p1, 100L
            );

            Map<TopicPartition, OffsetAndTimestamp> timestampOffsets = consumer.offsetsForTimes(timestampsToSearch);

            OffsetAndTimestamp timestampTopic1P0 = timestampOffsets.get(t1p0);
            assertEquals(0, timestampTopic1P0.offset());
            assertEquals(0, timestampTopic1P0.timestamp());
            assertEquals(Optional.of(0), timestampTopic1P0.leaderEpoch());

            OffsetAndTimestamp timestampTopic1P1 = timestampOffsets.get(t1p1);
            assertEquals(20, timestampTopic1P1.offset());
            assertEquals(20, timestampTopic1P1.timestamp());
            assertEquals(Optional.of(0), timestampTopic1P1.leaderEpoch());

            OffsetAndTimestamp timestampTopic2P0 = timestampOffsets.get(t2p0);
            assertNull(timestampTopic2P0, "v0 message format shouldn't have timestamp");

            OffsetAndTimestamp timestampTopic2P1 = timestampOffsets.get(t2p1);
            assertNull(timestampTopic2P1);

            OffsetAndTimestamp timestampTopic3P0 = timestampOffsets.get(t3p0);
            assertEquals(80, timestampTopic3P0.offset());
            assertEquals(80, timestampTopic3P0.timestamp());
            assertEquals(Optional.empty(), timestampTopic3P0.leaderEpoch());

            assertNull(timestampOffsets.get(t3p1), "v1 message format doesn't have leader epoch");
        }

    }

    @ClusterTest
    public void testEarliestOrLatestOffsetsWithClassicConsumer() {
        testEarliestOrLatestOffsets(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testEarliestOrLatestOffsetsWithAsyncConsumer() {
        testEarliestOrLatestOffsets(GroupProtocol.CONSUMER);
    }

    public void testEarliestOrLatestOffsets(GroupProtocol groupProtocol) {
        Set<TopicPartition> partitions = Set.of(t1p0, t1p1, t2p0, t2p1, t3p0, t3p1);

        try (Consumer<Object, Object> consumer = cluster.consumer(Map.of(
                GROUP_PROTOCOL_CONFIG, groupProtocol.name.toLowerCase(Locale.ROOT)))
        ) {
            Map<TopicPartition, Long> earliests = consumer.beginningOffsets(partitions);
            assertEquals(0L, earliests.get(t1p0));
            assertEquals(0L, earliests.get(t1p1));
            assertEquals(0L, earliests.get(t2p0));
            assertEquals(0L, earliests.get(t2p1));
            assertEquals(0L, earliests.get(t3p0));
            assertEquals(0L, earliests.get(t3p1));

            Map<TopicPartition, Long> latests = consumer.endOffsets(partitions);
            assertEquals(100L, latests.get(t1p0));
            assertEquals(100L, latests.get(t1p1));
            assertEquals(100L, latests.get(t2p0));
            assertEquals(100L, latests.get(t2p1));
            assertEquals(100L, latests.get(t3p0));
            assertEquals(100L, latests.get(t3p1));
        }
    }
}
