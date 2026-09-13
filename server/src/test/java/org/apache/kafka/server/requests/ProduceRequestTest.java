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
package org.apache.kafka.server.requests;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.server.IntegrationTestUtils;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Meter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the produce request protocol. These tests require a running cluster and
 * complement the unit tests in clients/src/test/java/org/apache/kafka/common/requests/ProduceRequestTest.java,
 * which test request construction and serialization in isolation.
 */
@ClusterTestDefaults(brokers = 3)
public class ProduceRequestTest {

    private static final String TOPIC = "topic";
    private static final long FIVE_HOURS_IN_MS = Duration.ofHours(5).toMillis();
    private static final String INVALID_MESSAGE_CRC_RECORDS_PER_SEC = "InvalidMessageCrcRecordsPerSec";

    private final ClusterInstance cluster;

    ProduceRequestTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testSimpleProduceRequest() throws Exception {
        cluster.createTopic(TOPIC, 3, (short) 2);
        TopicPartitionInfo partitionAndLeader = findPartitionWithLeader();
        int partition = partitionAndLeader.partition();
        int leaderId = partitionAndLeader.leader().id();
        sendAndCheckProduceResponse(leaderId, partition,
            MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), "value".getBytes())),
            0L);

        sendAndCheckProduceResponse(leaderId, partition,
            MemoryRecords.withRecords(Compression.gzip().build(),
                new SimpleRecord(System.currentTimeMillis(), "key1".getBytes(), "value1".getBytes()),
                new SimpleRecord(System.currentTimeMillis(), "key2".getBytes(), "value2".getBytes())),
            1L);
    }

    @ClusterTest
    public void testProduceWithTimestampTooOld() throws Exception {
        doTestProduceWithInvalidTimestamp(
            TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG,
            System.currentTimeMillis() - FIVE_HOURS_IN_MS
        );
    }

    @ClusterTest
    public void testProduceWithTimestampTooNew() throws Exception {
        doTestProduceWithInvalidTimestamp(
            TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG,
            System.currentTimeMillis() + FIVE_HOURS_IN_MS
        );
    }

    @ClusterTest
    public void testProduceToNonReplica() throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        int leaderId = cluster.getLeaderBrokerId(new TopicPartition(TOPIC, 0));
        Uuid topicId = getTopicId();

        int nonReplicaId = cluster.brokers().keySet().stream()
            .filter(id -> id != leaderId)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No non-replica broker found"));

        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("key".getBytes(), "value".getBytes()));
        ProduceRequest request = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setTopicId(topicId)
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(0)
                            .setRecords(records))))
                .iterator()))
            .setAcks((short) -1)
            .setTimeoutMs(3000)
            .setTransactionalId(null)).build();

        ProduceResponse response = sendProduceRequest(nonReplicaId, request);
        assertEquals(1, response.data().responses().size());
        var topicResponse = response.data().responses().iterator().next();
        assertEquals(1, topicResponse.partitionResponses().size());
        var partitionResponse = topicResponse.partitionResponses().get(0);
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionResponse.errorCode()));
    }

    @ClusterTest
    public void testCorruptLz4ProduceRequest() throws Exception {
        cluster.createTopic(TOPIC, 3, (short) 2);
        TopicPartitionInfo partitionAndLeader = findPartitionWithLeader();
        int partition = partitionAndLeader.partition();
        int leaderId = partitionAndLeader.leader().id();
        Uuid topicId = getTopicId();

        MemoryRecords memoryRecords = MemoryRecords.withRecords(Compression.lz4().build(),
            new SimpleRecord(1000000L, "key".getBytes(), "value".getBytes()));
        // Corrupt the lz4 frame checksum (not the kafka record CRC) to trigger CORRUPT_MESSAGE
        int lz4ChecksumOffset = 6;
        memoryRecords.buffer().array()[DefaultRecordBatch.RECORD_BATCH_OVERHEAD + lz4ChecksumOffset] = 0;

        ProduceRequest request = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setTopicId(topicId)
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(partition)
                            .setRecords(memoryRecords))))
                .iterator()))
            .setAcks((short) -1)
            .setTimeoutMs(3000)
            .setTransactionalId(null)).build();

        ProduceResponse response = sendProduceRequest(leaderId, request);
        assertEquals(1, response.data().responses().size());
        var topicResponse = response.data().responses().iterator().next();
        assertEquals(1, topicResponse.partitionResponses().size());
        var partitionResponse = topicResponse.partitionResponses().get(0);
        assertEquals(topicId, topicResponse.topicId());
        assertEquals(partition, partitionResponse.index());
        assertEquals(Errors.CORRUPT_MESSAGE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(-1L, partitionResponse.baseOffset());
        assertEquals(-1L, partitionResponse.logAppendTimeMs());

        long matchingMetricsCount = KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
            .filter(k -> k.getName().endsWith(INVALID_MESSAGE_CRC_RECORDS_PER_SEC))
            .count();
        assertEquals(1, matchingMetricsCount);
        Meter meter = (Meter) KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(e -> e.getKey().getName().endsWith(INVALID_MESSAGE_CRC_RECORDS_PER_SEC))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Metric not found: " + INVALID_MESSAGE_CRC_RECORDS_PER_SEC));
        assertTrue(meter.count() > 0);
    }

    @ClusterTest
    public void testZSTDProduceRequest() throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1,
            Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd"));
        int leaderId = cluster.getLeaderBrokerId(new TopicPartition(TOPIC, 0));

        MemoryRecords memoryRecords = MemoryRecords.withRecords(Compression.zstd().build(),
            new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), "value".getBytes()));

        // v7 uses topic name rather than topic ID
        ProduceRequestData data = new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setName(TOPIC)
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(0)
                            .setRecords(memoryRecords))))
                .iterator()))
            .setAcks((short) -1)
            .setTimeoutMs(3000)
            .setTransactionalId(null);

        ProduceResponse response = sendProduceRequest(leaderId, new ProduceRequest.Builder((short) 7, (short) 7, data).build());
        var topicResponse = response.data().responses().iterator().next();
        var partitionResponse = topicResponse.partitionResponses().get(0);
        assertEquals(TOPIC, topicResponse.name());
        assertEquals(0, partitionResponse.index());
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(0L, partitionResponse.baseOffset());
        assertEquals(-1L, partitionResponse.logAppendTimeMs());
    }

    // String so it can double as an @ClusterConfigProperty value (must be a compile-time constant).
    private static final String SMALL_MAX_DECOMPRESSED_MESSAGE_BYTES = "512";
    // Above the limit but tiny gzip-compressed and below max.message.bytes, so the decompressed
    // per-record limit -- not the wire bound -- is what rejects it.
    private static final int OVERSIZED_VALUE_BYTES = 4096;
    private static final int UNDERSIZED_VALUE_BYTES = 64;

    /**
     * A topic-level limit rejects a compressed record whose declared decompressed body exceeds it
     * (INVALID_RECORD, before allocating the body). A small compressed record and an equally-large
     * uncompressed record (bounded instead by max.message.bytes) are accepted.
     */
    @ClusterTest
    public void testProduceRejectsCompressedRecordExceedingMaxDecompressedMessageBytes() throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1,
            Map.of(TopicConfig.MAX_DECOMPRESSED_MESSAGE_BYTES_CONFIG, SMALL_MAX_DECOMPRESSED_MESSAGE_BYTES));
        int leaderId = cluster.getLeaderBrokerId(new TopicPartition(TOPIC, 0));
        Uuid topicId = getTopicId();

        var rejected = onlyPartitionResponse(sendProduceRequest(leaderId,
            produceRequest(topicId, singleRecord(Compression.gzip().build(), OVERSIZED_VALUE_BYTES))));
        assertEquals(Errors.INVALID_RECORD.code(), rejected.errorCode(),
            "a compressed record exceeding the configured per-record limit must be rejected as invalid");
        assertEquals(-1L, rejected.baseOffset(), "a rejected record must not be appended");

        var acceptedSmall = onlyPartitionResponse(sendProduceRequest(leaderId,
            produceRequest(topicId, singleRecord(Compression.gzip().build(), UNDERSIZED_VALUE_BYTES))));
        assertEquals(Errors.NONE.code(), acceptedSmall.errorCode(),
            "a compressed record under the configured per-record limit must be accepted");

        var acceptedUncompressed = onlyPartitionResponse(sendProduceRequest(leaderId,
            produceRequest(topicId, singleRecord(Compression.NONE, OVERSIZED_VALUE_BYTES))));
        assertEquals(Errors.NONE.code(), acceptedUncompressed.errorCode(),
            "an uncompressed record must not be subject to the decompressed per-record limit");
    }

    /**
     * The topic-level limit is dynamically reconfigurable: a large compressed record is accepted at
     * the default, then rejected after lowering it via incrementalAlterConfigs -- no restart.
     */
    @ClusterTest
    public void testMaxDecompressedMessageBytesIsDynamicallyReconfigurable() throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        int leaderId = cluster.getLeaderBrokerId(new TopicPartition(TOPIC, 0));
        Uuid topicId = getTopicId();

        var before = onlyPartitionResponse(sendProduceRequest(leaderId,
            produceRequest(topicId, singleRecord(Compression.gzip().build(), OVERSIZED_VALUE_BYTES))));
        assertEquals(Errors.NONE.code(), before.errorCode(),
            "with the default per-record limit the record must be accepted");

        try (Admin admin = cluster.admin()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
            admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                new ConfigEntry(TopicConfig.MAX_DECOMPRESSED_MESSAGE_BYTES_CONFIG, SMALL_MAX_DECOMPRESSED_MESSAGE_BYTES),
                AlterConfigOp.OpType.SET)))).all().get();
        }

        // The topic-config change reaches the produce path asynchronously; poll until it takes effect.
        TestUtils.waitForCondition(
            () -> onlyPartitionResponse(sendProduceRequest(leaderId,
                produceRequest(topicId, singleRecord(Compression.gzip().build(), OVERSIZED_VALUE_BYTES))))
                .errorCode() == Errors.INVALID_RECORD.code(),
            15000L,
            "the lowered topic-level " + TopicConfig.MAX_DECOMPRESSED_MESSAGE_BYTES_CONFIG
                + " was not applied to the produce path");
    }

    /**
     * A broker-level default is inherited by a topic without an override: an oversized compressed
     * record is rejected.
     */
    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = TopicConfig.MAX_DECOMPRESSED_MESSAGE_BYTES_CONFIG, value = SMALL_MAX_DECOMPRESSED_MESSAGE_BYTES)
    })
    public void testBrokerDefaultMaxDecompressedMessageBytesAppliesToTopicWithoutOverride() throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        int leaderId = cluster.getLeaderBrokerId(new TopicPartition(TOPIC, 0));
        Uuid topicId = getTopicId();

        var rejected = onlyPartitionResponse(sendProduceRequest(leaderId,
            produceRequest(topicId, singleRecord(Compression.gzip().build(), OVERSIZED_VALUE_BYTES))));
        assertEquals(Errors.INVALID_RECORD.code(), rejected.errorCode(),
            "the broker-level default per-record limit must apply to a topic without an override");
    }

    /**
     * The broker-level default is dynamically reconfigurable and flows to a topic without an
     * override: accepted at the default, rejected after lowering the cluster-wide default via a
     * BROKER-resource alter (asserted dynamic in KafkaConfigTest#testDynamicLogConfigs) -- no restart.
     */
    @ClusterTest
    public void testBrokerDefaultMaxDecompressedMessageBytesIsDynamicallyReconfigurable() throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        int leaderId = cluster.getLeaderBrokerId(new TopicPartition(TOPIC, 0));
        Uuid topicId = getTopicId();

        var before = onlyPartitionResponse(sendProduceRequest(leaderId,
            produceRequest(topicId, singleRecord(Compression.gzip().build(), OVERSIZED_VALUE_BYTES))));
        assertEquals(Errors.NONE.code(), before.errorCode(),
            "with the default broker per-record limit and no topic override the record must be accepted");

        try (Admin admin = cluster.admin()) {
            // Empty resource name = cluster-wide broker default.
            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "");
            admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                new ConfigEntry(TopicConfig.MAX_DECOMPRESSED_MESSAGE_BYTES_CONFIG, SMALL_MAX_DECOMPRESSED_MESSAGE_BYTES),
                AlterConfigOp.OpType.SET)))).all().get();
        }

        // The broker-default change reaches the produce path asynchronously; poll until it takes effect.
        TestUtils.waitForCondition(
            () -> onlyPartitionResponse(sendProduceRequest(leaderId,
                produceRequest(topicId, singleRecord(Compression.gzip().build(), OVERSIZED_VALUE_BYTES))))
                .errorCode() == Errors.INVALID_RECORD.code(),
            15000L,
            "the lowered cluster-wide broker-default " + TopicConfig.MAX_DECOMPRESSED_MESSAGE_BYTES_CONFIG
                + " was not applied to the produce path");
    }

    private ProduceRequest produceRequest(Uuid topicId, MemoryRecords records) {
        return ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setTopicId(topicId)
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(0)
                            .setRecords(records))))
                .iterator()))
            .setAcks((short) -1)
            .setTimeoutMs(3000)
            .setTransactionalId(null)).build();
    }

    private static MemoryRecords singleRecord(Compression compression, int valueSize) {
        return MemoryRecords.withRecords(compression,
            new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), new byte[valueSize]));
    }

    private ProduceResponseData.PartitionProduceResponse onlyPartitionResponse(ProduceResponse response) {
        assertEquals(1, response.data().responses().size());
        var topicResponse = response.data().responses().iterator().next();
        assertEquals(1, topicResponse.partitionResponses().size());
        return topicResponse.partitionResponses().get(0);
    }

    private ProduceResponse sendProduceRequest(int brokerId, ProduceRequest request) throws IOException {
        KafkaBroker broker = cluster.brokers().get(brokerId);
        int port = broker.socketServer().boundPort(cluster.clientListener());
        return IntegrationTestUtils.connectAndReceive(request, port);
    }

    private void sendAndCheckProduceResponse(int leaderId, int partition,
                                             MemoryRecords records, long expectedOffset) throws IOException, ExecutionException, InterruptedException {
        Uuid topicId = getTopicId();
        ProduceRequest request = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setTopicId(topicId)
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(partition)
                            .setRecords(records))))
                .iterator()))
            .setAcks((short) -1)
            .setTimeoutMs(3000)
            .setTransactionalId(null)).build();

        assertEquals(ApiKeys.PRODUCE.latestVersion(), request.version());
        ProduceResponse response = sendProduceRequest(leaderId, request);
        assertEquals(1, response.data().responses().size());
        var topicResponse = response.data().responses().iterator().next();
        assertEquals(1, topicResponse.partitionResponses().size());
        var partitionResponse = topicResponse.partitionResponses().get(0);
        assertEquals(topicId, topicResponse.topicId());
        assertEquals(partition, partitionResponse.index());
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(expectedOffset, partitionResponse.baseOffset());
        assertEquals(-1L, partitionResponse.logAppendTimeMs());
        assertTrue(partitionResponse.recordErrors().isEmpty());
    }

    private void doTestProduceWithInvalidTimestamp(String timestampConfig, long recordTimestamp) throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1, Map.of(timestampConfig, "1000"));
        int leaderId = cluster.getLeaderBrokerId(new TopicPartition(TOPIC, 0));
        Uuid topicId = getTopicId();

        ByteBuffer buf = ByteBuffer.allocate(512);
        var builder = MemoryRecords.builder(buf, RecordBatch.MAGIC_VALUE_V2,
            Compression.gzip().build(), TimestampType.CREATE_TIME, 0L);
        builder.appendWithOffset(0, recordTimestamp, null, "hello".getBytes());
        builder.appendWithOffset(1, recordTimestamp, null, "there".getBytes());
        builder.appendWithOffset(2, recordTimestamp, null, "beautiful".getBytes());
        MemoryRecords records = builder.build();

        ProduceResponse response = sendProduceRequest(leaderId, ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setTopicId(topicId)
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(0)
                            .setRecords(records))))
                .iterator()))
            .setAcks((short) -1)
            .setTimeoutMs(3000)
            .setTransactionalId(null)).build());

        assertEquals(1, response.data().responses().size());
        var topicResponse = response.data().responses().iterator().next();
        assertEquals(1, topicResponse.partitionResponses().size());
        var partitionResponse = topicResponse.partitionResponses().get(0);
        assertEquals(topicId, topicResponse.topicId());
        assertEquals(0, partitionResponse.index());
        assertEquals(Errors.INVALID_TIMESTAMP, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(3, partitionResponse.recordErrors().size());
        for (int i = 0; i < partitionResponse.recordErrors().size(); i++) {
            assertEquals(i, partitionResponse.recordErrors().get(i).batchIndex());
            assertNotNull(partitionResponse.recordErrors().get(i).batchIndexErrorMessage());
        }
        assertEquals("One or more records have been rejected due to invalid timestamp",
            partitionResponse.errorMessage());
    }

    private Uuid getTopicId() throws ExecutionException, InterruptedException {
        try (Admin admin = cluster.admin()) {
            return admin.describeTopics(List.of(TOPIC))
                .topicNameValues().get(TOPIC).get().topicId();
        }
    }

    private TopicPartitionInfo findPartitionWithLeader() throws ExecutionException, InterruptedException {
        try (Admin admin = cluster.admin()) {
            TopicDescription desc = admin.describeTopics(List.of(TOPIC))
                .topicNameValues().get(TOPIC).get();
            return desc.partitions().stream()
                .filter(p -> p.leader() != null && p.leader().id() != -1)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No partition with leader found for topic " + TOPIC));
        }
    }
}
