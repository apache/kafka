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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class FetcherTest {
    private static final double EPSILON = 0.0001;

    private ConsumerRebalanceListener listener = new NoOpConsumerRebalanceListener();
    private String topicName = "test";
    private String groupId = "test-group";
    private final String metricGroup = "consumer" + groupId + "-fetch-manager-metrics";
    private TopicPartition tp0 = new TopicPartition(topicName, 0);
    private TopicPartition tp1 = new TopicPartition(topicName, 1);
    private TopicPartition tp2 = new TopicPartition(topicName, 2);
    private TopicPartition tp3 = new TopicPartition(topicName, 3);
    private int validLeaderEpoch = 0;
    private MetadataResponse initialUpdateResponse =
        RequestTestUtils.metadataUpdateWith(1, singletonMap(topicName, 4));

    private int minBytes = 1;
    private int maxBytes = Integer.MAX_VALUE;
    private int maxWaitMs = 0;
    private int fetchSize = 1000;
    private long retryBackoffMs = 100;
    private long requestTimeoutMs = 30000;
    private MockTime time = new MockTime(1);
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private FetcherMetricsRegistry metricsRegistry;
    private MockClient client;
    private Metrics metrics;
    private ApiVersions apiVersions = new ApiVersions();
    private ConsumerNetworkClient consumerClient;
    private Fetcher<?, ?> fetcher;

    private MemoryRecords records;
    private MemoryRecords nextRecords;
    private MemoryRecords emptyRecords;
    private MemoryRecords partialRecords;
    private ExecutorService executorService;

    @BeforeEach
    public void setup() {
        records = buildRecords(1L, 3, 1);
        nextRecords = buildRecords(4L, 2, 4);
        emptyRecords = buildRecords(0L, 0, 0);
        partialRecords = buildRecords(4L, 1, 0);
        partialRecords.buffer().putInt(Records.SIZE_OFFSET, 10000);
    }

    private void assignFromUser(Set<TopicPartition> partitions) {
        subscriptions.assignFromUser(partitions);
        client.updateMetadata(initialUpdateResponse);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
            Collections.emptyMap(), singletonMap(topicName, 4),
            tp -> validLeaderEpoch), false, 0L);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (metrics != null)
            this.metrics.close();
        if (fetcher != null)
            this.fetcher.close();
        if (executorService != null) {
            executorService.shutdownNow();
            assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testFetchNormal() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(3, records.size());
        assertEquals(4L, subscriptions.position(tp0).offset); // this is the next fetching position
        long offset = 1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertEquals(offset, record.offset());
            offset += 1;
        }
    }

    @Test
    public void testMissingLeaderEpochInRecords() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0,
                CompressionType.NONE, TimestampType.CREATE_TIME, 0L, System.currentTimeMillis(),
                RecordBatch.NO_PARTITION_LEADER_EPOCH);
        builder.append(0L, "key".getBytes(), "1".getBytes());
        builder.append(0L, "key".getBytes(), "2".getBytes());
        MemoryRecords records = builder.build();

        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertEquals(2, partitionRecords.get(tp0).size());

        for (ConsumerRecord<byte[], byte[]> record : partitionRecords.get(tp0)) {
            assertEquals(Optional.empty(), record.leaderEpoch());
        }
    }

    @Test
    public void testLeaderEpochInConsumerRecord() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        Integer partitionLeaderEpoch = 1;

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE, TimestampType.CREATE_TIME, 0L, System.currentTimeMillis(),
                partitionLeaderEpoch);
        builder.append(0L, "key".getBytes(), partitionLeaderEpoch.toString().getBytes());
        builder.append(0L, "key".getBytes(), partitionLeaderEpoch.toString().getBytes());
        builder.close();

        partitionLeaderEpoch += 7;

        builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
                TimestampType.CREATE_TIME, 2L, System.currentTimeMillis(), partitionLeaderEpoch);
        builder.append(0L, "key".getBytes(), partitionLeaderEpoch.toString().getBytes());
        builder.close();

        partitionLeaderEpoch += 5;
        builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
                TimestampType.CREATE_TIME, 3L, System.currentTimeMillis(), partitionLeaderEpoch);
        builder.append(0L, "key".getBytes(), partitionLeaderEpoch.toString().getBytes());
        builder.append(0L, "key".getBytes(), partitionLeaderEpoch.toString().getBytes());
        builder.append(0L, "key".getBytes(), partitionLeaderEpoch.toString().getBytes());
        builder.close();

        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertEquals(6, partitionRecords.get(tp0).size());

        for (ConsumerRecord<byte[], byte[]> record : partitionRecords.get(tp0)) {
            int expectedLeaderEpoch = Integer.parseInt(Utils.utf8(record.value()));
            assertEquals(Optional.of(expectedLeaderEpoch), record.leaderEpoch());
        }
    }

    @Test
    public void testClearBufferedDataForTopicPartitions() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        Set<TopicPartition> newAssignedTopicPartitions = new HashSet<>();
        newAssignedTopicPartitions.add(tp1);

        fetcher.clearBufferedDataForUnassignedPartitions(newAssignedTopicPartitions);
        assertFalse(fetcher.hasCompletedFetches());
    }

    @Test
    public void testFetchSkipsBlackedOutNodes() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        Node node = initialUpdateResponse.brokers().iterator().next();

        client.backoff(node, 500);
        assertEquals(0, fetcher.sendFetches());

        time.sleep(500);
        assertEquals(1, fetcher.sendFetches());
    }

    @Test
    public void testFetcherIgnoresControlRecords() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        long producerId = 1;
        short producerEpoch = 0;
        int baseSequence = 0;
        int partitionLeaderEpoch = 0;

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.idempotentBuilder(buffer, CompressionType.NONE, 0L, producerId,
                producerEpoch, baseSequence);
        builder.append(0L, "key".getBytes(), null);
        builder.close();

        MemoryRecords.writeEndTransactionalMarker(buffer, 1L, time.milliseconds(), partitionLeaderEpoch, producerId, producerEpoch,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));

        buffer.flip();

        client.prepareResponse(fullFetchResponse(tp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(1, records.size());
        assertEquals(2L, subscriptions.position(tp0).offset);

        ConsumerRecord<byte[], byte[]> record = records.get(0);
        assertArrayEquals("key".getBytes(), record.key());
    }

    @Test
    public void testFetchError() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NOT_LEADER_OR_FOLLOWER, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertFalse(partitionRecords.containsKey(tp0));
    }

    private MockClient.RequestMatcher matchesOffset(final TopicPartition tp, final long offset) {
        return body -> {
            FetchRequest fetch = (FetchRequest) body;
            return fetch.fetchData().containsKey(tp) &&
                    fetch.fetchData().get(tp).fetchOffset == offset;
        };
    }

    @Test
    public void testFetchedRecordsRaisesOnSerializationErrors() {
        // raise an exception from somewhere in the middle of the fetch response
        // so that we can verify that our position does not advance after raising
        ByteArrayDeserializer deserializer = new ByteArrayDeserializer() {
            int i = 0;
            @Override
            public byte[] deserialize(String topic, byte[] data) {
                if (i++ % 2 == 1) {
                    // Should be blocked on the value deserialization of the first record.
                    assertEquals("value-1", new String(data, StandardCharsets.UTF_8));
                    throw new SerializationException();
                }
                return data;
            }
        };

        buildFetcher(deserializer, deserializer);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        client.prepareResponse(matchesOffset(tp0, 1), fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(time.timer(0));
        // The fetcher should block on Deserialization error
        for (int i = 0; i < 2; i++) {
            try {
                fetcher.fetchedRecords();
                fail("fetchedRecords should have raised");
            } catch (SerializationException e) {
                // the position should not advance since no data has been returned
                assertEquals(1, subscriptions.position(tp0).offset);
            }
        }
    }

    @Test
    public void testParseCorruptedRecord() throws Exception {
        buildFetcher();
        assignFromUser(singleton(tp0));

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));

        byte magic = RecordBatch.MAGIC_VALUE_V1;
        byte[] key = "foo".getBytes();
        byte[] value = "baz".getBytes();
        long offset = 0;
        long timestamp = 500L;

        int size = LegacyRecord.recordSize(magic, key.length, value.length);
        byte attributes = LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME);
        long crc = LegacyRecord.computeChecksum(magic, attributes, timestamp, key, value);

        // write one valid record
        out.writeLong(offset);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        // and one invalid record (note the crc)
        out.writeLong(offset + 1);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc + 1, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        // write one valid record
        out.writeLong(offset + 2);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        // Write a record whose size field is invalid.
        out.writeLong(offset + 3);
        out.writeInt(1);

        // write one valid record
        out.writeLong(offset + 4);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        buffer.flip();

        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0, Optional.empty(), metadata.currentLeader(tp0)));

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // the first fetchedRecords() should return the first valid message
        assertEquals(1, fetcher.fetchedRecords().get(tp0).size());
        assertEquals(1, subscriptions.position(tp0).offset);

        ensureBlockOnRecord(1L);
        seekAndConsumeRecord(buffer, 2L);
        ensureBlockOnRecord(3L);
        try {
            // For a record that cannot be retrieved from the iterator, we cannot seek over it within the batch.
            seekAndConsumeRecord(buffer, 4L);
            fail("Should have thrown exception when fail to retrieve a record from iterator.");
        } catch (KafkaException ke) {
           // let it go
        }
        ensureBlockOnRecord(4L);
    }

    private void ensureBlockOnRecord(long blockedOffset) {
        // the fetchedRecords() should always throw exception due to the invalid message at the starting offset.
        for (int i = 0; i < 2; i++) {
            try {
                fetcher.fetchedRecords();
                fail("fetchedRecords should have raised KafkaException");
            } catch (KafkaException e) {
                assertEquals(blockedOffset, subscriptions.position(tp0).offset);
            }
        }
    }

    private void seekAndConsumeRecord(ByteBuffer responseBuffer, long toOffset) {
        // Seek to skip the bad record and fetch again.
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(toOffset, Optional.empty(), metadata.currentLeader(tp0)));
        // Should not throw exception after the seek.
        fetcher.fetchedRecords();
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, MemoryRecords.readableRecords(responseBuffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchedRecords();
        List<ConsumerRecord<byte[], byte[]>> records = recordsByPartition.get(tp0);
        assertEquals(1, records.size());
        assertEquals(toOffset, records.get(0).offset());
        assertEquals(toOffset + 1, subscriptions.position(tp0).offset);
    }

    @Test
    public void testInvalidDefaultRecordBatch() {
        buildFetcher();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteBufferOutputStream out = new ByteBufferOutputStream(buffer);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(out,
                                                                DefaultRecordBatch.CURRENT_MAGIC_VALUE,
                                                                CompressionType.NONE,
                                                                TimestampType.CREATE_TIME,
                                                                0L, 10L, 0L, (short) 0, 0, false, false, 0, 1024);
        builder.append(10L, "key".getBytes(), "value".getBytes());
        builder.close();
        buffer.flip();

        // Garble the CRC
        buffer.position(17);
        buffer.put("beef".getBytes());
        buffer.position(0);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // the fetchedRecords() should always throw exception due to the bad batch.
        for (int i = 0; i < 2; i++) {
            try {
                fetcher.fetchedRecords();
                fail("fetchedRecords should have raised KafkaException");
            } catch (KafkaException e) {
                assertEquals(0, subscriptions.position(tp0).offset);
            }
        }
    }

    @Test
    public void testParseInvalidRecordBatch() {
        buildFetcher();
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        ByteBuffer buffer = records.buffer();

        // flip some bits to fail the crc
        buffer.putInt(32, buffer.get(32) ^ 87238423);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        try {
            fetcher.fetchedRecords();
            fail("fetchedRecords should have raised");
        } catch (KafkaException e) {
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp0).offset);
        }
    }

    @Test
    public void testHeaders() {
        buildFetcher();

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, 1L);
        builder.append(0L, "key".getBytes(), "value-1".getBytes());

        Header[] headersArray = new Header[1];
        headersArray[0] = new RecordHeader("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, "key".getBytes(), "value-2".getBytes(), headersArray);

        Header[] headersArray2 = new Header[2];
        headersArray2[0] = new RecordHeader("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        headersArray2[1] = new RecordHeader("headerKey", "headerValue2".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, "key".getBytes(), "value-3".getBytes(), headersArray2);

        MemoryRecords memoryRecords = builder.build();

        List<ConsumerRecord<byte[], byte[]>> records;
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        client.prepareResponse(matchesOffset(tp0, 1), fullFetchResponse(tp0, memoryRecords, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchedRecords();
        records = recordsByPartition.get(tp0);

        assertEquals(3, records.size());

        Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = records.iterator();

        ConsumerRecord<byte[], byte[]> record = recordIterator.next();
        assertNull(record.headers().lastHeader("headerKey"));

        record = recordIterator.next();
        assertEquals("headerValue", new String(record.headers().lastHeader("headerKey").value(), StandardCharsets.UTF_8));
        assertEquals("headerKey", record.headers().lastHeader("headerKey").key());

        record = recordIterator.next();
        assertEquals("headerValue2", new String(record.headers().lastHeader("headerKey").value(), StandardCharsets.UTF_8));
        assertEquals("headerKey", record.headers().lastHeader("headerKey").key());
    }

    @Test
    public void testFetchMaxPollRecords() {
        buildFetcher(2);

        List<ConsumerRecord<byte[], byte[]>> records;
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        client.prepareResponse(matchesOffset(tp0, 1), fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        client.prepareResponse(matchesOffset(tp0, 4), fullFetchResponse(tp0, this.nextRecords, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchedRecords();
        records = recordsByPartition.get(tp0);
        assertEquals(2, records.size());
        assertEquals(3L, subscriptions.position(tp0).offset);
        assertEquals(1, records.get(0).offset());
        assertEquals(2, records.get(1).offset());

        assertEquals(0, fetcher.sendFetches());
        consumerClient.poll(time.timer(0));
        recordsByPartition = fetchedRecords();
        records = recordsByPartition.get(tp0);
        assertEquals(1, records.size());
        assertEquals(4L, subscriptions.position(tp0).offset);
        assertEquals(3, records.get(0).offset());

        assertTrue(fetcher.sendFetches() > 0);
        consumerClient.poll(time.timer(0));
        recordsByPartition = fetchedRecords();
        records = recordsByPartition.get(tp0);
        assertEquals(2, records.size());
        assertEquals(6L, subscriptions.position(tp0).offset);
        assertEquals(4, records.get(0).offset());
        assertEquals(5, records.get(1).offset());
    }

    /**
     * Test the scenario where a partition with fetched but not consumed records (i.e. max.poll.records is
     * less than the number of fetched records) is unassigned and a different partition is assigned. This is a
     * pattern used by Streams state restoration and KAFKA-5097 would have been caught by this test.
     */
    @Test
    public void testFetchAfterPartitionWithFetchedRecordsIsUnassigned() {
        buildFetcher(2);

        List<ConsumerRecord<byte[], byte[]>> records;
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        // Returns 3 records while `max.poll.records` is configured to 2
        client.prepareResponse(matchesOffset(tp0, 1), fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchedRecords();
        records = recordsByPartition.get(tp0);
        assertEquals(2, records.size());
        assertEquals(3L, subscriptions.position(tp0).offset);
        assertEquals(1, records.get(0).offset());
        assertEquals(2, records.get(1).offset());

        assignFromUser(singleton(tp1));
        client.prepareResponse(matchesOffset(tp1, 4), fullFetchResponse(tp1, this.nextRecords, Errors.NONE, 100L, 0));
        subscriptions.seek(tp1, 4);

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertNull(fetchedRecords.get(tp0));
        records = fetchedRecords.get(tp1);
        assertEquals(2, records.size());
        assertEquals(6L, subscriptions.position(tp1).offset);
        assertEquals(4, records.get(0).offset());
        assertEquals(5, records.get(1).offset());
    }

    @Test
    public void testFetchNonContinuousRecords() {
        // if we are fetching from a compacted topic, there may be gaps in the returned records
        // this test verifies the fetcher updates the current fetched/consumed positions correctly for this case
        buildFetcher();

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        builder.appendWithOffset(15L, 0L, "key".getBytes(), "value-1".getBytes());
        builder.appendWithOffset(20L, 0L, "key".getBytes(), "value-2".getBytes());
        builder.appendWithOffset(30L, 0L, "key".getBytes(), "value-3".getBytes());
        MemoryRecords records = builder.build();

        List<ConsumerRecord<byte[], byte[]>> consumerRecords;
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchedRecords();
        consumerRecords = recordsByPartition.get(tp0);
        assertEquals(3, consumerRecords.size());
        assertEquals(31L, subscriptions.position(tp0).offset); // this is the next fetching position

        assertEquals(15L, consumerRecords.get(0).offset());
        assertEquals(20L, consumerRecords.get(1).offset());
        assertEquals(30L, consumerRecords.get(2).offset());
    }

    /**
     * Test the case where the client makes a pre-v3 FetchRequest, but the server replies with only a partial
     * request. This happens when a single message is larger than the per-partition limit.
     */
    @Test
    public void testFetchRequestWhenRecordTooLarge() {
        try {
            buildFetcher();

            client.setNodeApiVersions(NodeApiVersions.create(ApiKeys.FETCH.id, (short) 2, (short) 2));
            makeFetchRequestWithIncompleteRecord();
            try {
                fetcher.fetchedRecords();
                fail("RecordTooLargeException should have been raised");
            } catch (RecordTooLargeException e) {
                assertTrue(e.getMessage().startsWith("There are some messages at [Partition=Offset]: "));
                // the position should not advance since no data has been returned
                assertEquals(0, subscriptions.position(tp0).offset);
            }
        } finally {
            client.setNodeApiVersions(NodeApiVersions.create());
        }
    }

    /**
     * Test the case where the client makes a post KIP-74 FetchRequest, but the server replies with only a
     * partial request. For v3 and later FetchRequests, the implementation of KIP-74 changed the behavior
     * so that at least one message is always returned. Therefore, this case should not happen, and it indicates
     * that an internal error has taken place.
     */
    @Test
    public void testFetchRequestInternalError() {
        buildFetcher();
        makeFetchRequestWithIncompleteRecord();
        try {
            fetcher.fetchedRecords();
            fail("RecordTooLargeException should have been raised");
        } catch (KafkaException e) {
            assertTrue(e.getMessage().startsWith("Failed to make progress reading messages"));
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp0).offset);
        }
    }

    private void makeFetchRequestWithIncompleteRecord() {
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        MemoryRecords partialRecord = MemoryRecords.readableRecords(
            ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}));
        client.prepareResponse(fullFetchResponse(tp0, partialRecord, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
    }

    @Test
    public void testUnauthorizedTopic() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // resize the limit of the buffer to pretend it is only fetch-size large
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.TOPIC_AUTHORIZATION_FAILED, 100L, 0));
        consumerClient.poll(time.timer(0));
        try {
            fetcher.fetchedRecords();
            fail("fetchedRecords should have thrown");
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(topicName), e.unauthorizedTopics());
        }
    }

    @Test
    public void testFetchDuringEagerRebalance() {
        buildFetcher();

        subscriptions.subscribe(singleton(topicName), listener);
        subscriptions.assignFromSubscribed(singleton(tp0));
        subscriptions.seek(tp0, 0);

        client.updateMetadata(RequestTestUtils.metadataUpdateWith(
            1, singletonMap(topicName, 4), tp -> validLeaderEpoch));

        assertEquals(1, fetcher.sendFetches());

        // Now the eager rebalance happens and fetch positions are cleared
        subscriptions.assignFromSubscribed(Collections.emptyList());

        subscriptions.assignFromSubscribed(singleton(tp0));
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // The active fetch should be ignored since its position is no longer valid
        assertTrue(fetcher.fetchedRecords().isEmpty());
    }

    @Test
    public void testFetchDuringCooperativeRebalance() {
        buildFetcher();

        subscriptions.subscribe(singleton(topicName), listener);
        subscriptions.assignFromSubscribed(singleton(tp0));
        subscriptions.seek(tp0, 0);

        client.updateMetadata(RequestTestUtils.metadataUpdateWith(
            1, singletonMap(topicName, 4), tp -> validLeaderEpoch));

        assertEquals(1, fetcher.sendFetches());

        // Now the cooperative rebalance happens and fetch positions are NOT cleared for unrevoked partitions
        subscriptions.assignFromSubscribed(singleton(tp0));

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();

        // The active fetch should NOT be ignored since the position for tp0 is still valid
        assertEquals(1, fetchedRecords.size());
        assertEquals(3, fetchedRecords.get(tp0).size());
    }

    @Test
    public void testInFlightFetchOnPausedPartition() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        subscriptions.pause(tp0);

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertNull(fetcher.fetchedRecords().get(tp0));
    }

    @Test
    public void testFetchOnPausedPartition() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        subscriptions.pause(tp0);
        assertFalse(fetcher.sendFetches() > 0);
        assertTrue(client.requests().isEmpty());
    }

    @Test
    public void testFetchOnCompletedFetchesForPausedAndResumedPartitions() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());

        subscriptions.pause(tp0);

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertEquals(0, fetchedRecords.size(), "Should not return any records when partition is paused");
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches");
        assertFalse(fetcher.hasAvailableFetches(), "Should not have any available (non-paused) completed fetches");
        assertNull(fetchedRecords.get(tp0));
        assertEquals(0, fetcher.sendFetches());

        subscriptions.resume(tp0);

        assertTrue(fetcher.hasAvailableFetches(), "Should have available (non-paused) completed fetches");

        consumerClient.poll(time.timer(0));
        fetchedRecords = fetchedRecords();
        assertEquals(1, fetchedRecords.size(), "Should return records when partition is resumed");
        assertNotNull(fetchedRecords.get(tp0));
        assertEquals(3, fetchedRecords.get(tp0).size());

        consumerClient.poll(time.timer(0));
        fetchedRecords = fetchedRecords();
        assertEquals(0, fetchedRecords.size(), "Should not return records after previously paused partitions are fetched");
        assertFalse(fetcher.hasCompletedFetches(), "Should no longer contain completed fetches");
    }

    @Test
    public void testFetchOnCompletedFetchesForSomePausedPartitions() {
        buildFetcher();

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords;

        assignFromUser(Utils.mkSet(tp0, tp1));

        // seek to tp0 and tp1 in two polls to generate 2 complete requests and responses

        // #1 seek, request, poll, response
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp0)));
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // #2 seek, request, poll, response
        subscriptions.seekUnvalidated(tp1, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp1)));
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp1, this.nextRecords, Errors.NONE, 100L, 0));

        subscriptions.pause(tp0);
        consumerClient.poll(time.timer(0));

        fetchedRecords = fetchedRecords();
        assertEquals(1, fetchedRecords.size(), "Should return completed fetch for unpaused partitions");
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches");
        assertNotNull(fetchedRecords.get(tp1));
        assertNull(fetchedRecords.get(tp0));

        fetchedRecords = fetchedRecords();
        assertEquals(0, fetchedRecords.size(), "Should return no records for remaining paused partition");
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches");
    }

    @Test
    public void testFetchOnCompletedFetchesForAllPausedPartitions() {
        buildFetcher();

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords;

        assignFromUser(Utils.mkSet(tp0, tp1));

        // seek to tp0 and tp1 in two polls to generate 2 complete requests and responses

        // #1 seek, request, poll, response
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp0)));
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // #2 seek, request, poll, response
        subscriptions.seekUnvalidated(tp1, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp1)));
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp1, this.nextRecords, Errors.NONE, 100L, 0));

        subscriptions.pause(tp0);
        subscriptions.pause(tp1);

        consumerClient.poll(time.timer(0));

        fetchedRecords = fetchedRecords();
        assertEquals(0, fetchedRecords.size(), "Should return no records for all paused partitions");
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches");
        assertFalse(fetcher.hasAvailableFetches(), "Should not have any available (non-paused) completed fetches");
    }

    @Test
    public void testPartialFetchWithPausedPartitions() {
        // this test sends creates a completed fetch with 3 records and a max poll of 2 records to assert
        // that a fetch that must be returned over at least 2 polls can be cached successfully when its partition is
        // paused, then returned successfully after its been resumed again later
        buildFetcher(2);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords;

        assignFromUser(Utils.mkSet(tp0, tp1));

        subscriptions.seek(tp0, 1);
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        fetchedRecords = fetchedRecords();

        assertEquals(2, fetchedRecords.get(tp0).size(), "Should return 2 records from fetch with 3 records");
        assertFalse(fetcher.hasCompletedFetches(), "Should have no completed fetches");

        subscriptions.pause(tp0);
        consumerClient.poll(time.timer(0));

        fetchedRecords = fetchedRecords();

        assertEquals(0, fetchedRecords.size(), "Should return no records for paused partitions");
        assertTrue(fetcher.hasCompletedFetches(), "Should have 1 entry in completed fetches");
        assertFalse(fetcher.hasAvailableFetches(), "Should not have any available (non-paused) completed fetches");

        subscriptions.resume(tp0);

        consumerClient.poll(time.timer(0));

        fetchedRecords = fetchedRecords();

        assertEquals(1, fetchedRecords.get(tp0).size(), "Should return last remaining record");
        assertFalse(fetcher.hasCompletedFetches(), "Should have no completed fetches");
    }

    @Test
    public void testFetchDiscardedAfterPausedPartitionResumedAndSeekedToNewOffset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        subscriptions.pause(tp0);
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));

        subscriptions.seek(tp0, 3);
        subscriptions.resume(tp0);
        consumerClient.poll(time.timer(0));

        assertTrue(fetcher.hasCompletedFetches(), "Should have 1 entry in completed fetches");
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertEquals(0, fetchedRecords.size(), "Should not return any records because we seeked to a new offset");
        assertNull(fetchedRecords.get(tp0));
        assertFalse(fetcher.hasCompletedFetches(), "Should have no completed fetches");
    }

    @Test
    public void testFetchNotLeaderOrFollower() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NOT_LEADER_OR_FOLLOWER, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testFetchUnknownTopicOrPartition() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.UNKNOWN_TOPIC_OR_PARTITION, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testFetchFencedLeaderEpoch() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.FENCED_LEADER_EPOCH, 100L, 0));
        consumerClient.poll(time.timer(0));

        assertEquals(0, fetcher.fetchedRecords().size(), "Should not return any records");
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()), "Should have requested metadata update");
    }

    @Test
    public void testFetchUnknownLeaderEpoch() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.UNKNOWN_LEADER_EPOCH, 100L, 0));
        consumerClient.poll(time.timer(0));

        assertEquals(0, fetcher.fetchedRecords().size(), "Should not return any records");
        assertNotEquals(0L, metadata.timeToNextUpdate(time.milliseconds()), "Should not have requested metadata update");
    }

    @Test
    public void testEpochSetInFetchRequest() {
        buildFetcher();
        subscriptions.assignFromUser(singleton(tp0));
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), Collections.singletonMap(topicName, 4), tp -> 99);
        client.updateMetadata(metadataResponse);

        subscriptions.seek(tp0, 10);
        assertEquals(1, fetcher.sendFetches());

        // Check for epoch in outgoing request
        MockClient.RequestMatcher matcher = body -> {
            if (body instanceof FetchRequest) {
                FetchRequest fetchRequest = (FetchRequest) body;
                fetchRequest.fetchData().values().forEach(partitionData -> {
                    assertTrue(partitionData.currentLeaderEpoch.isPresent(), "Expected Fetcher to set leader epoch in request");
                    assertEquals(99, partitionData.currentLeaderEpoch.get().longValue(), "Expected leader epoch to match epoch from metadata update");
                });
                return true;
            } else {
                fail("Should have seen FetchRequest");
                return false;
            }
        };
        client.prepareResponse(matcher, fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.pollNoWakeup();
    }

    @Test
    public void testFetchOffsetOutOfRange() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertEquals(0, fetcher.fetchedRecords().size());
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertNull(subscriptions.validPosition(tp0));
        assertNull(subscriptions.position(tp0));
    }

    @Test
    public void testStaleOutOfRangeError() {
        // verify that an out of range error which arrives after a seek
        // does not cause us to reset our position or throw an exception
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        subscriptions.seek(tp0, 1);
        consumerClient.poll(time.timer(0));
        assertEquals(0, fetcher.fetchedRecords().size());
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(1, subscriptions.position(tp0).offset);
    }

    @Test
    public void testFetchedRecordsAfterSeek() {
        buildFetcher(OffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), 2, IsolationLevel.READ_UNCOMMITTED);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertTrue(fetcher.sendFetches() > 0);
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        subscriptions.seek(tp0, 2);
        assertEquals(0, fetcher.fetchedRecords().size());
    }

    @Test
    public void testFetchOffsetOutOfRangeException() {
        buildFetcher(OffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), 2, IsolationLevel.READ_UNCOMMITTED);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        fetcher.sendFetches();
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(time.timer(0));

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        for (int i = 0; i < 2; i++) {
            OffsetOutOfRangeException e = assertThrows(OffsetOutOfRangeException.class, () ->
                    fetcher.fetchedRecords());
            assertEquals(singleton(tp0), e.offsetOutOfRangePartitions().keySet());
            assertEquals(0L, e.offsetOutOfRangePartitions().get(tp0).longValue());
        }
    }

    @Test
    public void testFetchPositionAfterException() {
        // verify the advancement in the next fetch offset equals to the number of fetched records when
        // some fetched partitions cause Exception. This ensures that consumer won't lose record upon exception
        buildFetcher(OffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
        assignFromUser(Utils.mkSet(tp0, tp1));
        subscriptions.seek(tp0, 1);
        subscriptions.seek(tp1, 1);

        assertEquals(1, fetcher.sendFetches());

        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = new LinkedHashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData<>(Errors.NONE, 100,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, records));
        partitions.put(tp0, new FetchResponse.PartitionData<>(Errors.OFFSET_OUT_OF_RANGE, 100,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY));
        client.prepareResponse(new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions),
            0, INVALID_SESSION_ID));
        consumerClient.poll(time.timer(0));

        List<ConsumerRecord<byte[], byte[]>> allFetchedRecords = new ArrayList<>();
        fetchRecordsInto(allFetchedRecords);

        assertEquals(1, subscriptions.position(tp0).offset);
        assertEquals(4, subscriptions.position(tp1).offset);
        assertEquals(3, allFetchedRecords.size());

        OffsetOutOfRangeException e = assertThrows(OffsetOutOfRangeException.class, () ->
                fetchRecordsInto(allFetchedRecords));

        assertEquals(singleton(tp0), e.offsetOutOfRangePartitions().keySet());
        assertEquals(1L, e.offsetOutOfRangePartitions().get(tp0).longValue());

        assertEquals(1, subscriptions.position(tp0).offset);
        assertEquals(4, subscriptions.position(tp1).offset);
        assertEquals(3, allFetchedRecords.size());
    }

    private void fetchRecordsInto(List<ConsumerRecord<byte[], byte[]>> allFetchedRecords) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        fetchedRecords.values().forEach(allFetchedRecords::addAll);
    }

    @Test
    public void testCompletedFetchRemoval() {
        // Ensure the removal of completed fetches that cause an Exception if and only if they contain empty records.
        buildFetcher(OffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
        assignFromUser(Utils.mkSet(tp0, tp1, tp2, tp3));

        subscriptions.seek(tp0, 1);
        subscriptions.seek(tp1, 1);
        subscriptions.seek(tp2, 1);
        subscriptions.seek(tp3, 1);

        assertEquals(1, fetcher.sendFetches());

        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = new LinkedHashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData<>(Errors.NONE, 100, FetchResponse.INVALID_LAST_STABLE_OFFSET,
                FetchResponse.INVALID_LOG_START_OFFSET, null, records));
        partitions.put(tp0, new FetchResponse.PartitionData<>(Errors.OFFSET_OUT_OF_RANGE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY));
        partitions.put(tp2, new FetchResponse.PartitionData<>(Errors.NONE, 100L, 4,
                0L, null, nextRecords));
        partitions.put(tp3, new FetchResponse.PartitionData<>(Errors.NONE, 100L, 4,
                0L, null, partialRecords));
        client.prepareResponse(new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions),
                0, INVALID_SESSION_ID));
        consumerClient.poll(time.timer(0));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = new ArrayList<>();
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchedRecords();
        for (List<ConsumerRecord<byte[], byte[]>> records : recordsByPartition.values())
            fetchedRecords.addAll(records);

        assertEquals(fetchedRecords.size(), subscriptions.position(tp1).offset - 1);
        assertEquals(4, subscriptions.position(tp1).offset);
        assertEquals(3, fetchedRecords.size());

        List<OffsetOutOfRangeException> oorExceptions = new ArrayList<>();
        try {
            recordsByPartition = fetchedRecords();
            for (List<ConsumerRecord<byte[], byte[]>> records : recordsByPartition.values())
                fetchedRecords.addAll(records);
        } catch (OffsetOutOfRangeException oor) {
            oorExceptions.add(oor);
        }

        // Should have received one OffsetOutOfRangeException for partition tp1
        assertEquals(1, oorExceptions.size());
        OffsetOutOfRangeException oor = oorExceptions.get(0);
        assertTrue(oor.offsetOutOfRangePartitions().containsKey(tp0));
        assertEquals(oor.offsetOutOfRangePartitions().size(), 1);

        recordsByPartition = fetchedRecords();
        for (List<ConsumerRecord<byte[], byte[]>> records : recordsByPartition.values())
            fetchedRecords.addAll(records);

        // Should not have received an Exception for tp2.
        assertEquals(6, subscriptions.position(tp2).offset);
        assertEquals(5, fetchedRecords.size());

        int numExceptionsExpected = 3;
        List<KafkaException> kafkaExceptions = new ArrayList<>();
        for (int i = 1; i <= numExceptionsExpected; i++) {
            try {
                recordsByPartition = fetchedRecords();
                for (List<ConsumerRecord<byte[], byte[]>> records : recordsByPartition.values())
                    fetchedRecords.addAll(records);
            } catch (KafkaException e) {
                kafkaExceptions.add(e);
            }
        }
        // Should have received as much as numExceptionsExpected Kafka exceptions for tp3.
        assertEquals(numExceptionsExpected, kafkaExceptions.size());
    }

    @Test
    public void testSeekBeforeException() {
        buildFetcher(OffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), 2, IsolationLevel.READ_UNCOMMITTED);

        assignFromUser(Utils.mkSet(tp0));
        subscriptions.seek(tp0, 1);
        assertEquals(1, fetcher.sendFetches());
        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = new HashMap<>();
        partitions.put(tp0, new FetchResponse.PartitionData<>(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), null, records));
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        assertEquals(2, fetcher.fetchedRecords().get(tp0).size());

        subscriptions.assignFromUser(Utils.mkSet(tp0, tp1));
        subscriptions.seekUnvalidated(tp1, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp1)));

        assertEquals(1, fetcher.sendFetches());
        partitions = new HashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData<>(Errors.OFFSET_OUT_OF_RANGE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), null, MemoryRecords.EMPTY));
        client.prepareResponse(new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions), 0, INVALID_SESSION_ID));
        consumerClient.poll(time.timer(0));
        assertEquals(1, fetcher.fetchedRecords().get(tp0).size());

        subscriptions.seek(tp1, 10);
        // Should not throw OffsetOutOfRangeException after the seek
        assertEquals(0, fetcher.fetchedRecords().size());
    }

    @Test
    public void testFetchDisconnected() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0), true);
        consumerClient.poll(time.timer(0));
        assertEquals(0, fetcher.fetchedRecords().size());

        // disconnects should have no affect on subscription state
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(0, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionNoOpWithPositionSet() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 5L);

        fetcher.resetOffsetsIfNeeded();
        assertFalse(client.hasInFlightRequests());
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionResetToDefaultOffset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.EARLIEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionResetToLatestOffset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.updateMetadata(initialUpdateResponse);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    /**
     * Make sure the client behaves appropriately when receiving an exception for unavailable offsets
     */
    @Test
    public void testFetchOffsetErrors() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Fail with OFFSET_NOT_AVAILABLE
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.OFFSET_NOT_AVAILABLE, 1L, 5L), false);
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0));

        // Fail with LEADER_NOT_AVAILABLE
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.LEADER_NOT_AVAILABLE, 1L, 5L), false);
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0));

        // Back to normal
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L), false);
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertTrue(subscriptions.hasValidPosition(tp0));
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(subscriptions.position(tp0).offset, 5L);
    }

    @Test
    public void testListOffsetSendsReadUncommitted() {
        testListOffsetsSendsIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    }

    @Test
    public void testListOffsetSendsReadCommitted() {
        testListOffsetsSendsIsolationLevel(IsolationLevel.READ_COMMITTED);
    }

    private void testListOffsetsSendsIsolationLevel(IsolationLevel isolationLevel) {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                Integer.MAX_VALUE, isolationLevel);

        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(body -> {
            ListOffsetsRequest request = (ListOffsetsRequest) body;
            return request.isolationLevel() == isolationLevel;
        }, listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testResetOffsetsSkipsBlackedOutConnections() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);

        // Check that we skip sending the ListOffset request when the node is blacked out
        client.updateMetadata(initialUpdateResponse);
        Node node = initialUpdateResponse.brokers().iterator().next();
        client.backoff(node, 500);
        fetcher.resetOffsetsIfNeeded();
        assertEquals(0, consumerClient.pendingRequestCount());
        consumerClient.pollNoWakeup();
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(tp0));

        time.sleep(500);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.EARLIEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionResetToEarliestOffset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.EARLIEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testResetOffsetsMetadataRefresh() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // First fetch fails with stale metadata
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NOT_LEADER_OR_FOLLOWER, 1L, 5L), false);
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Expect a metadata refresh
        client.prepareMetadataUpdate(initialUpdateResponse);
        consumerClient.pollNoWakeup();
        assertFalse(client.hasPendingMetadataUpdates());

        // Next fetch succeeds
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testListOffsetNoUpdateMissingEpoch() {
        buildFetcher();

        // Set up metadata with no leader epoch
        subscriptions.assignFromUser(singleton(tp0));
        MetadataResponse metadataWithNoLeaderEpochs = RequestTestUtils.metadataUpdateWith(
                "kafka-cluster", 1, Collections.emptyMap(), singletonMap(topicName, 4), tp -> null);
        client.updateMetadata(metadataWithNoLeaderEpochs);

        // Return a ListOffsets response with leaderEpoch=1, we should ignore it
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(tp0, Errors.NONE, 1L, 5L, 1));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        // Reset should be satisfied and no metadata update requested
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(metadata.updateRequested());
        assertFalse(metadata.lastSeenLeaderEpoch(tp0).isPresent());
    }

    @Test
    public void testListOffsetUpdateEpoch() {
        buildFetcher();

        // Set up metadata with leaderEpoch=1
        subscriptions.assignFromUser(singleton(tp0));
        MetadataResponse metadataWithLeaderEpochs = RequestTestUtils.metadataUpdateWith(
                "kafka-cluster", 1, Collections.emptyMap(), singletonMap(topicName, 4), tp -> 1);
        client.updateMetadata(metadataWithLeaderEpochs);

        // Reset offsets to trigger ListOffsets call
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Now we see a ListOffsets with leaderEpoch=2 epoch, we trigger a metadata update
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP, 1),
                listOffsetResponse(tp0, Errors.NONE, 1L, 5L, 2));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(metadata.updateRequested());
        assertOptional(metadata.lastSeenLeaderEpoch(tp0), epoch -> assertEquals((long) epoch, 2));
    }

    @Test
    public void testUpdateFetchPositionDisconnect() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // First request gets a disconnect
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NONE, 1L, 5L), true);
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Expect a metadata refresh
        client.prepareMetadataUpdate(initialUpdateResponse);
        consumerClient.pollNoWakeup();
        assertFalse(client.hasPendingMetadataUpdates());

        // No retry until the backoff passes
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Next one succeeds
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testAssignmentChangeWithInFlightReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Send the ListOffsets request to reset the position
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(client.hasInFlightRequests());

        // Now we have an assignment change
        assignFromUser(singleton(tp1));

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();

        assertFalse(client.hasPendingResponses());
        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.isAssigned(tp0));
    }

    @Test
    public void testSeekWithInFlightReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Send the ListOffsets request to reset the position
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(client.hasInFlightRequests());

        // Now we get a seek from the user
        subscriptions.seek(tp0, 237);

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();

        assertFalse(client.hasPendingResponses());
        assertFalse(client.hasInFlightRequests());
        assertEquals(237L, subscriptions.position(tp0).offset);
    }

    @Timeout(10)
    @Test
    public void testEarlierOffsetResetArrivesLate() throws InterruptedException {
        LogContext lc = new LogContext();
        buildFetcher(spy(new SubscriptionState(lc, OffsetResetStrategy.EARLIEST)), lc);
        assignFromUser(singleton(tp0));

        ExecutorService es = Executors.newSingleThreadExecutor();
        CountDownLatch latchLatestStart = new CountDownLatch(1);
        CountDownLatch latchEarliestStart = new CountDownLatch(1);
        CountDownLatch latchEarliestDone = new CountDownLatch(1);
        CountDownLatch latchEarliestFinish = new CountDownLatch(1);
        try {
            doAnswer(invocation -> {
                latchLatestStart.countDown();
                latchEarliestStart.await();
                Object result = invocation.callRealMethod();
                latchEarliestDone.countDown();
                return result;
            }).when(subscriptions).maybeSeekUnvalidated(tp0, new SubscriptionState.FetchPosition(0L,
                Optional.empty(), metadata.currentLeader(tp0)), OffsetResetStrategy.EARLIEST);

            es.submit(() -> {
                subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);
                fetcher.resetOffsetsIfNeeded();
                consumerClient.pollNoWakeup();
                client.respond(listOffsetResponse(Errors.NONE, 1L, 0L));
                consumerClient.pollNoWakeup();
                latchEarliestFinish.countDown();
            }, Void.class);

            latchLatestStart.await();
            subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);
            fetcher.resetOffsetsIfNeeded();
            consumerClient.pollNoWakeup();
            client.respond(listOffsetResponse(Errors.NONE, 1L, 10L));
            latchEarliestStart.countDown();
            latchEarliestDone.await();
            consumerClient.pollNoWakeup();
            latchEarliestFinish.await();
            assertEquals(10, subscriptions.position(tp0).offset);
        } finally {
            es.shutdown();
            es.awaitTermination(10000, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testChangeResetWithInFlightReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Send the ListOffsets request to reset the position
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(client.hasInFlightRequests());

        // Now we get a seek from the user
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();

        assertFalse(client.hasPendingResponses());
        assertFalse(client.hasInFlightRequests());
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(tp0));
    }

    @Test
    public void testIdempotentResetWithInFlightReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Send the ListOffsets request to reset the position
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(client.hasInFlightRequests());

        // Now we get a seek from the user
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();

        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(5L, subscriptions.position(tp0).offset);
    }

    @Test
    public void testRestOffsetsAuthorizationFailure() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // First request gets a disconnect
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.TOPIC_AUTHORIZATION_FAILED, -1, -1), false);
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));

        try {
            fetcher.resetOffsetsIfNeeded();
            fail("Expected authorization error to be raised");
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(tp0.topic()), e.unauthorizedTopics());
        }

        // The exception should clear after being raised, but no retry until the backoff
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Next one succeeds
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsRequiringOffsetReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.pause(tp0); // paused partition does not have a valid position
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NONE, 1L, 10L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp0));
        assertEquals(10, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsWithoutAValidPosition() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0);
        subscriptions.pause(tp0); // paused partition does not have a valid position

        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0)); // because tp is paused
        assertFalse(subscriptions.hasValidPosition(tp0));
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsWithAValidPosition() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 10);
        subscriptions.pause(tp0); // paused partition already has a valid position

        fetcher.resetOffsetsIfNeeded();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp0));
        assertEquals(10, subscriptions.position(tp0).offset);
    }

    @Test
    public void testGetAllTopics() {
        // sending response before request, as getTopicMetadata is a blocking call
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(topicName, Errors.NONE));

        Map<String, List<PartitionInfo>> allTopics = fetcher.getAllTopicMetadata(time.timer(5000L));

        assertEquals(initialUpdateResponse.topicMetadata().size(), allTopics.size());
    }

    @Test
    public void testGetAllTopicsDisconnect() {
        // first try gets a disconnect, next succeeds
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(null, true);
        client.prepareResponse(newMetadataResponse(topicName, Errors.NONE));
        Map<String, List<PartitionInfo>> allTopics = fetcher.getAllTopicMetadata(time.timer(5000L));
        assertEquals(initialUpdateResponse.topicMetadata().size(), allTopics.size());
    }

    @Test
    public void testGetAllTopicsTimeout() {
        // since no response is prepared, the request should timeout
        buildFetcher();
        assignFromUser(singleton(tp0));
        assertThrows(TimeoutException.class, () -> fetcher.getAllTopicMetadata(time.timer(50L)));
    }

    @Test
    public void testGetAllTopicsUnauthorized() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(topicName, Errors.TOPIC_AUTHORIZATION_FAILED));
        try {
            fetcher.getAllTopicMetadata(time.timer(10L));
            fail();
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(topicName), e.unauthorizedTopics());
        }
    }

    @Test
    public void testGetTopicMetadataInvalidTopic() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(topicName, Errors.INVALID_TOPIC_EXCEPTION));
        assertThrows(InvalidTopicException.class, () -> fetcher.getTopicMetadata(
                new MetadataRequest.Builder(Collections.singletonList(topicName), true), time.timer(5000L)));
    }

    @Test
    public void testGetTopicMetadataUnknownTopic() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(topicName, Errors.UNKNOWN_TOPIC_OR_PARTITION));

        Map<String, List<PartitionInfo>> topicMetadata = fetcher.getTopicMetadata(
                new MetadataRequest.Builder(Collections.singletonList(topicName), true), time.timer(5000L));
        assertNull(topicMetadata.get(topicName));
    }

    @Test
    public void testGetTopicMetadataLeaderNotAvailable() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(topicName, Errors.LEADER_NOT_AVAILABLE));
        client.prepareResponse(newMetadataResponse(topicName, Errors.NONE));

        Map<String, List<PartitionInfo>> topicMetadata = fetcher.getTopicMetadata(
                new MetadataRequest.Builder(Collections.singletonList(topicName), true), time.timer(5000L));
        assertTrue(topicMetadata.containsKey(topicName));
    }

    @Test
    public void testGetTopicMetadataOfflinePartitions() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        MetadataResponse originalResponse = newMetadataResponse(topicName, Errors.NONE); //baseline ok response

        //create a response based on the above one with all partitions being leaderless
        List<MetadataResponse.TopicMetadata> altTopics = new ArrayList<>();
        for (MetadataResponse.TopicMetadata item : originalResponse.topicMetadata()) {
            List<MetadataResponse.PartitionMetadata> partitions = item.partitionMetadata();
            List<MetadataResponse.PartitionMetadata> altPartitions = new ArrayList<>();
            for (MetadataResponse.PartitionMetadata p : partitions) {
                altPartitions.add(new MetadataResponse.PartitionMetadata(
                    p.error,
                    p.topicPartition,
                    Optional.empty(), //no leader
                    Optional.empty(),
                    p.replicaIds,
                    p.inSyncReplicaIds,
                    p.offlineReplicaIds
                ));
            }
            MetadataResponse.TopicMetadata alteredTopic = new MetadataResponse.TopicMetadata(
                item.error(),
                item.topic(),
                item.isInternal(),
                altPartitions
            );
            altTopics.add(alteredTopic);
        }
        Node controller = originalResponse.controller();
        MetadataResponse altered = RequestTestUtils.metadataResponse(
            originalResponse.brokers(),
            originalResponse.clusterId(),
            controller != null ? controller.id() : MetadataResponse.NO_CONTROLLER_ID,
            altTopics);

        client.prepareResponse(altered);

        Map<String, List<PartitionInfo>> topicMetadata =
            fetcher.getTopicMetadata(new MetadataRequest.Builder(Collections.singletonList(topicName), false),
                    time.timer(5000L));

        assertNotNull(topicMetadata);
        assertNotNull(topicMetadata.get(topicName));
        //noinspection ConstantConditions
        assertEquals(metadata.fetch().partitionCountForTopic(topicName).longValue(), topicMetadata.get(topicName).size());
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testQuotaMetrics() {
        buildFetcher();

        MockSelector selector = new MockSelector(time);
        Sensor throttleTimeSensor = Fetcher.throttleTimeSensor(metrics, metricsRegistry);
        Cluster cluster = TestUtils.singletonCluster("test", 1);
        Node node = cluster.nodes().get(0);
        NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                1000, 1000, 64 * 1024, 64 * 1024, 1000, 10 * 1000, 127 * 1000, ClientDnsLookup.USE_ALL_DNS_IPS,
                time, true, new ApiVersions(), throttleTimeSensor, new LogContext());

        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(ApiVersionsResponse.createApiVersionsResponse(
            400, RecordBatch.CURRENT_MAGIC_VALUE), ApiKeys.API_VERSIONS.latestVersion(), 0);

        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        while (!client.ready(node, time.milliseconds())) {
            client.poll(1, time.milliseconds());
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()));
        }
        selector.clear();

        for (int i = 1; i <= 3; i++) {
            int throttleTimeMs = 100 * i;
            FetchRequest.Builder builder = FetchRequest.Builder.forConsumer(100, 100, new LinkedHashMap<>());
            builder.rackId("");
            ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true);
            client.send(request, time.milliseconds());
            client.poll(1, time.milliseconds());
            FetchResponse<MemoryRecords> response = fullFetchResponse(tp0, nextRecords, Errors.NONE, i, throttleTimeMs);
            buffer = RequestTestUtils.serializeResponseWithHeader(response, ApiKeys.FETCH.latestVersion(), request.correlationId());
            selector.completeReceive(new NetworkReceive(node.idString(), buffer));
            client.poll(1, time.milliseconds());
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()));
            selector.clear();
        }
        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric avgMetric = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchThrottleTimeAvg));
        KafkaMetric maxMetric = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchThrottleTimeMax));
        // Throttle times are ApiVersions=400, Fetch=(100, 200, 300)
        assertEquals(250, (Double) avgMetric.metricValue(), EPSILON);
        assertEquals(400, (Double) maxMetric.metricValue(), EPSILON);
        client.close();
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testFetcherMetrics() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        MetricName maxLagMetric = metrics.metricInstance(metricsRegistry.recordsLagMax);
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", tp0.topic());
        tags.put("partition", String.valueOf(tp0.partition()));
        MetricName partitionLagMetric = metrics.metricName("records-lag", metricGroup, tags);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLagMax = allMetrics.get(maxLagMetric);

        // recordsFetchLagMax should be initialized to NaN
        assertEquals(Double.NaN, (Double) recordsFetchLagMax.metricValue(), EPSILON);

        // recordsFetchLagMax should be hw - fetchOffset after receiving an empty FetchResponse
        fetchRecords(tp0, MemoryRecords.EMPTY, Errors.NONE, 100L, 0);
        assertEquals(100, (Double) recordsFetchLagMax.metricValue(), EPSILON);

        KafkaMetric partitionLag = allMetrics.get(partitionLagMetric);
        assertEquals(100, (Double) partitionLag.metricValue(), EPSILON);

        // recordsFetchLagMax should be hw - offset of the last message after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        fetchRecords(tp0, builder.build(), Errors.NONE, 200L, 0);
        assertEquals(197, (Double) recordsFetchLagMax.metricValue(), EPSILON);
        assertEquals(197, (Double) partitionLag.metricValue(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        fetcher.sendFetches();
        assertFalse(allMetrics.containsKey(partitionLagMetric));
    }

    @Test
    public void testFetcherLeadMetric() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        MetricName minLeadMetric = metrics.metricInstance(metricsRegistry.recordsLeadMin);
        Map<String, String> tags = new HashMap<>(2);
        tags.put("topic", tp0.topic());
        tags.put("partition", String.valueOf(tp0.partition()));
        MetricName partitionLeadMetric = metrics.metricName("records-lead", metricGroup, "", tags);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLeadMin = allMetrics.get(minLeadMetric);

        // recordsFetchLeadMin should be initialized to NaN
        assertEquals(Double.NaN, (Double) recordsFetchLeadMin.metricValue(), EPSILON);

        // recordsFetchLeadMin should be position - logStartOffset after receiving an empty FetchResponse
        fetchRecords(tp0, MemoryRecords.EMPTY, Errors.NONE, 100L, -1L, 0L, 0);
        assertEquals(0L, (Double) recordsFetchLeadMin.metricValue(), EPSILON);

        KafkaMetric partitionLead = allMetrics.get(partitionLeadMetric);
        assertEquals(0L, (Double) partitionLead.metricValue(), EPSILON);

        // recordsFetchLeadMin should be position - logStartOffset after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++) {
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        }
        fetchRecords(tp0, builder.build(), Errors.NONE, 200L, -1L, 0L, 0);
        assertEquals(0L, (Double) recordsFetchLeadMin.metricValue(), EPSILON);
        assertEquals(3L, (Double) partitionLead.metricValue(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        fetcher.sendFetches();
        assertFalse(allMetrics.containsKey(partitionLeadMetric));
    }

    @Test
    public void testReadCommittedLagMetric() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        MetricName maxLagMetric = metrics.metricInstance(metricsRegistry.recordsLagMax);

        Map<String, String> tags = new HashMap<>();
        tags.put("topic", tp0.topic());
        tags.put("partition", String.valueOf(tp0.partition()));
        MetricName partitionLagMetric = metrics.metricName("records-lag", metricGroup, tags);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLagMax = allMetrics.get(maxLagMetric);

        // recordsFetchLagMax should be initialized to NaN
        assertEquals(Double.NaN, (Double) recordsFetchLagMax.metricValue(), EPSILON);

        // recordsFetchLagMax should be lso - fetchOffset after receiving an empty FetchResponse
        fetchRecords(tp0, MemoryRecords.EMPTY, Errors.NONE, 100L, 50L, 0);
        assertEquals(50, (Double) recordsFetchLagMax.metricValue(), EPSILON);

        KafkaMetric partitionLag = allMetrics.get(partitionLagMetric);
        assertEquals(50, (Double) partitionLag.metricValue(), EPSILON);

        // recordsFetchLagMax should be lso - offset of the last message after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        fetchRecords(tp0, builder.build(), Errors.NONE, 200L, 150L, 0);
        assertEquals(147, (Double) recordsFetchLagMax.metricValue(), EPSILON);
        assertEquals(147, (Double) partitionLag.metricValue(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        fetcher.sendFetches();
        assertFalse(allMetrics.containsKey(partitionLagMetric));
    }

    @Test
    public void testFetchResponseMetrics() {
        buildFetcher();

        String topic1 = "foo";
        String topic2 = "bar";
        TopicPartition tp1 = new TopicPartition(topic1, 0);
        TopicPartition tp2 = new TopicPartition(topic2, 0);

        subscriptions.assignFromUser(Utils.mkSet(tp1, tp2));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(topic1, 1);
        partitionCounts.put(topic2, 1);
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, partitionCounts, tp -> validLeaderEpoch));

        int expectedBytes = 0;
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> fetchPartitionData = new LinkedHashMap<>();

        for (TopicPartition tp : Utils.mkSet(tp1, tp2)) {
            subscriptions.seek(tp, 0);

            MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                    TimestampType.CREATE_TIME, 0L);
            for (int v = 0; v < 3; v++)
                builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
            MemoryRecords records = builder.build();
            for (Record record : records.records())
                expectedBytes += record.sizeInBytes();

            fetchPartitionData.put(tp, new FetchResponse.PartitionData<>(Errors.NONE, 15L,
                    FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, records));
        }

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(new FetchResponse<>(Errors.NONE, fetchPartitionData, 0, INVALID_SESSION_ID));
        consumerClient.poll(time.timer(0));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertEquals(3, fetchedRecords.get(tp1).size());
        assertEquals(3, fetchedRecords.get(tp2).size());

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchSizeAvg));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg));
        assertEquals(expectedBytes, (Double) fetchSizeAverage.metricValue(), EPSILON);
        assertEquals(6, (Double) recordsCountAverage.metricValue(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsPartialResponse() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchSizeAvg));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg));

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        MemoryRecords records = builder.build();

        int expectedBytes = 0;
        for (Record record : records.records()) {
            if (record.offset() >= 1)
                expectedBytes += record.sizeInBytes();
        }

        fetchRecords(tp0, records, Errors.NONE, 100L, 0);
        assertEquals(expectedBytes, (Double) fetchSizeAverage.metricValue(), EPSILON);
        assertEquals(2, (Double) recordsCountAverage.metricValue(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsWithOnePartitionError() {
        buildFetcher();
        assignFromUser(Utils.mkSet(tp0, tp1));
        subscriptions.seek(tp0, 0);
        subscriptions.seek(tp1, 0);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchSizeAvg));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg));

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        MemoryRecords records = builder.build();

        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = new HashMap<>();
        partitions.put(tp0, new FetchResponse.PartitionData<>(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, records));
        partitions.put(tp1, new FetchResponse.PartitionData<>(Errors.OFFSET_OUT_OF_RANGE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, MemoryRecords.EMPTY));

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions),
                0, INVALID_SESSION_ID));
        consumerClient.poll(time.timer(0));
        fetcher.fetchedRecords();

        int expectedBytes = 0;
        for (Record record : records.records())
            expectedBytes += record.sizeInBytes();

        assertEquals(expectedBytes, (Double) fetchSizeAverage.metricValue(), EPSILON);
        assertEquals(3, (Double) recordsCountAverage.metricValue(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsWithOnePartitionAtTheWrongOffset() {
        buildFetcher();

        assignFromUser(Utils.mkSet(tp0, tp1));
        subscriptions.seek(tp0, 0);
        subscriptions.seek(tp1, 0);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchSizeAvg));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg));

        // send the fetch and then seek to a new offset
        assertEquals(1, fetcher.sendFetches());
        subscriptions.seek(tp1, 5);

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        MemoryRecords records = builder.build();

        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = new HashMap<>();
        partitions.put(tp0, new FetchResponse.PartitionData<>(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, records));
        partitions.put(tp1, new FetchResponse.PartitionData<>(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null,
                MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("val".getBytes()))));

        client.prepareResponse(new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions),
                0, INVALID_SESSION_ID));
        consumerClient.poll(time.timer(0));
        fetcher.fetchedRecords();

        // we should have ignored the record at the wrong offset
        int expectedBytes = 0;
        for (Record record : records.records())
            expectedBytes += record.sizeInBytes();

        assertEquals(expectedBytes, (Double) fetchSizeAverage.metricValue(), EPSILON);
        assertEquals(3, (Double) recordsCountAverage.metricValue(), EPSILON);
    }

    @Test
    public void testFetcherMetricsTemplates() {
        Map<String, String> clientTags = Collections.singletonMap("client-id", "clientA");
        buildFetcher(new MetricConfig().tags(clientTags), OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);

        // Fetch from topic to generate topic metrics
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        // Create throttle metrics
        Fetcher.throttleTimeSensor(metrics, metricsRegistry);

        // Verify that all metrics except metrics-count have registered templates
        Set<MetricNameTemplate> allMetrics = new HashSet<>();
        for (MetricName n : metrics.metrics().keySet()) {
            String name = n.name().replaceAll(tp0.toString(), "{topic}-{partition}");
            if (!n.group().equals("kafka-metrics-count"))
                allMetrics.add(new MetricNameTemplate(name, n.group(), "", n.tags().keySet()));
        }
        TestUtils.checkEquals(allMetrics, new HashSet<>(metricsRegistry.getAllTemplates()), "metrics", "templates");
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchRecords(
            TopicPartition tp, MemoryRecords records, Errors error, long hw, int throttleTime) {
        return fetchRecords(tp, records, error, hw, FetchResponse.INVALID_LAST_STABLE_OFFSET, throttleTime);
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchRecords(
            TopicPartition tp, MemoryRecords records, Errors error, long hw, long lastStableOffset, int throttleTime) {
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp, records, error, hw, lastStableOffset, throttleTime));
        consumerClient.poll(time.timer(0));
        return fetchedRecords();
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchRecords(
            TopicPartition tp, MemoryRecords records, Errors error, long hw, long lastStableOffset, long logStartOffset, int throttleTime) {
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(tp, records, error, hw, lastStableOffset, logStartOffset, throttleTime));
        consumerClient.poll(time.timer(0));
        return fetchedRecords();
    }

    @Test
    public void testGetOffsetsForTimesTimeout() {
        buildFetcher();
        assertThrows(TimeoutException.class, () -> fetcher.offsetsForTimes(
            Collections.singletonMap(new TopicPartition(topicName, 2), 1000L), time.timer(100L)));
    }

    @Test
    public void testGetOffsetsForTimes() {
        buildFetcher();

        // Empty map
        assertTrue(fetcher.offsetsForTimes(new HashMap<>(), time.timer(100L)).isEmpty());
        // Unknown Offset
        testGetOffsetsForTimesWithUnknownOffset();
        // Error code none with unknown offset
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NONE, -1L, 100L, null, 100L);
        // Error code none with known offset
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NONE, 10L, 100L, 10L, 100L);
        // Test both of partition has error.
        testGetOffsetsForTimesWithError(Errors.NOT_LEADER_OR_FOLLOWER, Errors.INVALID_REQUEST, 10L, 100L, 10L, 100L);
        // Test the second partition has error.
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NOT_LEADER_OR_FOLLOWER, 10L, 100L, 10L, 100L);
        // Test different errors.
        testGetOffsetsForTimesWithError(Errors.NOT_LEADER_OR_FOLLOWER, Errors.NONE, 10L, 100L, 10L, 100L);
        testGetOffsetsForTimesWithError(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE, 10L, 100L, 10L, 100L);
        testGetOffsetsForTimesWithError(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, Errors.NONE, 10L, 100L, null, 100L);
        testGetOffsetsForTimesWithError(Errors.BROKER_NOT_AVAILABLE, Errors.NONE, 10L, 100L, 10L, 100L);
    }

    @Test
    public void testGetOffsetsFencedLeaderEpoch() {
        buildFetcher();
        subscriptions.assignFromUser(singleton(tp0));
        client.updateMetadata(initialUpdateResponse);

        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetResponse(Errors.FENCED_LEADER_EPOCH, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0));
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testGetOffsetByTimeWithPartitionsRetryCouldTriggerMetadataUpdate() {
        List<Errors> retriableErrors = Arrays.asList(Errors.NOT_LEADER_OR_FOLLOWER,
            Errors.REPLICA_NOT_AVAILABLE, Errors.KAFKA_STORAGE_ERROR, Errors.OFFSET_NOT_AVAILABLE,
            Errors.LEADER_NOT_AVAILABLE, Errors.FENCED_LEADER_EPOCH, Errors.UNKNOWN_LEADER_EPOCH);

        final int newLeaderEpoch = 3;
        MetadataResponse updatedMetadata = RequestTestUtils.metadataUpdateWith("dummy", 3,
            singletonMap(topicName, Errors.NONE), singletonMap(topicName, 4), tp -> newLeaderEpoch);

        Node originalLeader = initialUpdateResponse.cluster().leaderFor(tp1);
        Node newLeader = updatedMetadata.cluster().leaderFor(tp1);
        assertNotEquals(originalLeader, newLeader);

        for (Errors retriableError : retriableErrors) {
            buildFetcher();

            subscriptions.assignFromUser(Utils.mkSet(tp0, tp1));
            client.updateMetadata(initialUpdateResponse);

            final long fetchTimestamp = 10L;
            ListOffsetsPartitionResponse tp0NoError = new ListOffsetsPartitionResponse()
                .setPartitionIndex(tp0.partition())
                .setErrorCode(Errors.NONE.code())
                .setTimestamp(fetchTimestamp)
                .setOffset(4L);
            List<ListOffsetsTopicResponse> topics = Collections.singletonList(
                    new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Arrays.asList(
                                tp0NoError,
                                new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp1.partition())
                                    .setErrorCode(retriableError.code())
                                    .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                                    .setOffset(-1L))));
            ListOffsetsResponseData data = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(topics);

            client.prepareResponseFrom(body -> {
                boolean isListOffsetRequest = body instanceof ListOffsetsRequest;
                if (isListOffsetRequest) {
                    ListOffsetsRequest request = (ListOffsetsRequest) body;
                    List<ListOffsetsTopic> expectedTopics = Collections.singletonList(
                            new ListOffsetsTopic()
                                .setName(tp0.topic())
                                .setPartitions(Arrays.asList(
                                        new ListOffsetsPartition()
                                            .setPartitionIndex(tp1.partition())
                                            .setTimestamp(fetchTimestamp)
                                            .setCurrentLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH),
                                        new ListOffsetsPartition()
                                            .setPartitionIndex(tp0.partition())
                                            .setTimestamp(fetchTimestamp)
                                            .setCurrentLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH))));
                    return request.topics().equals(expectedTopics);
                } else {
                    return false;
                }
            }, new ListOffsetsResponse(data), originalLeader);

            client.prepareMetadataUpdate(updatedMetadata);

            // If the metadata wasn't updated before retrying, the fetcher would consult the original leader and hit a NOT_LEADER exception.
            // We will count the answered future response in the end to verify if this is the case.
            List<ListOffsetsTopicResponse> topicsWithFatalError = Collections.singletonList(
                    new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Arrays.asList(
                                tp0NoError,
                                new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp1.partition())
                                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                                    .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                                    .setOffset(-1L))));
            ListOffsetsResponseData dataWithFatalError = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(topicsWithFatalError);
            client.prepareResponseFrom(new ListOffsetsResponse(dataWithFatalError), originalLeader);

            // The request to new leader must only contain one partition tp1 with error.
            client.prepareResponseFrom(body -> {
                boolean isListOffsetRequest = body instanceof ListOffsetsRequest;
                if (isListOffsetRequest) {
                    ListOffsetsRequest request = (ListOffsetsRequest) body;

                    ListOffsetsTopic requestTopic = request.topics().get(0);
                    ListOffsetsPartition expectedPartition = new ListOffsetsPartition()
                            .setPartitionIndex(tp1.partition())
                            .setTimestamp(fetchTimestamp)
                            .setCurrentLeaderEpoch(newLeaderEpoch);
                    return expectedPartition.equals(requestTopic.partitions().get(0));
                } else {
                    return false;
                }
            }, listOffsetResponse(tp1, Errors.NONE, fetchTimestamp, 5L), newLeader);

            Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                fetcher.offsetsForTimes(
                    Utils.mkMap(Utils.mkEntry(tp0, fetchTimestamp),
                    Utils.mkEntry(tp1, fetchTimestamp)), time.timer(Integer.MAX_VALUE));

            assertEquals(Utils.mkMap(
                Utils.mkEntry(tp0, new OffsetAndTimestamp(4L, fetchTimestamp)),
                Utils.mkEntry(tp1, new OffsetAndTimestamp(5L, fetchTimestamp))), offsetAndTimestampMap);

            // The NOT_LEADER exception future should not be cleared as we already refreshed the metadata before
            // first retry, thus never hitting.
            assertEquals(1, client.numAwaitingResponses());

            fetcher.close();
        }
    }

    @Test
    public void testGetOffsetsUnknownLeaderEpoch() {
        buildFetcher();
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetResponse(Errors.UNKNOWN_LEADER_EPOCH, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0));
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testGetOffsetsIncludesLeaderEpoch() {
        buildFetcher();
        subscriptions.assignFromUser(singleton(tp0));

        client.updateMetadata(initialUpdateResponse);

        // Metadata update with leader epochs
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), Collections.singletonMap(topicName, 4), tp -> 99);
        client.updateMetadata(metadataResponse);

        // Request latest offset
        subscriptions.requestOffsetReset(tp0);
        fetcher.resetOffsetsIfNeeded();

        // Check for epoch in outgoing request
        MockClient.RequestMatcher matcher = body -> {
            if (body instanceof ListOffsetsRequest) {
                ListOffsetsRequest offsetRequest = (ListOffsetsRequest) body;
                int epoch = offsetRequest.topics().get(0).partitions().get(0).currentLeaderEpoch();
                assertTrue(epoch != ListOffsetsResponse.UNKNOWN_EPOCH, "Expected Fetcher to set leader epoch in request");
                assertEquals(epoch, 99, "Expected leader epoch to match epoch from metadata update");
                return true;
            } else {
                fail("Should have seen ListOffsetRequest");
                return false;
            }
        };

        client.prepareResponse(matcher, listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();
    }

    @Test
    public void testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially() {
        buildFetcher();

        subscriptions.assignFromUser(Utils.mkSet(tp0, tp1));
        final String anotherTopic = "another-topic";
        final TopicPartition t2p0 = new TopicPartition(anotherTopic, 0);

        client.reset();

        // Metadata initially has one topic
        MetadataResponse initialMetadata = RequestTestUtils.metadataUpdateWith(3, singletonMap(topicName, 2));
        client.updateMetadata(initialMetadata);

        // The first metadata refresh should contain one topic
        client.prepareMetadataUpdate(initialMetadata);
        client.prepareResponseFrom(listOffsetResponse(tp0, Errors.NONE, 1000L, 11L),
                metadata.fetch().leaderFor(tp0));
        client.prepareResponseFrom(listOffsetResponse(tp1, Errors.NONE, 1000L, 32L),
                metadata.fetch().leaderFor(tp1));

        // Second metadata refresh should contain two topics
        Map<String, Integer> partitionNumByTopic = new HashMap<>();
        partitionNumByTopic.put(topicName, 2);
        partitionNumByTopic.put(anotherTopic, 1);
        MetadataResponse updatedMetadata = RequestTestUtils.metadataUpdateWith(3, partitionNumByTopic);
        client.prepareMetadataUpdate(updatedMetadata);
        client.prepareResponseFrom(listOffsetResponse(t2p0, Errors.NONE, 1000L, 54L),
                metadata.fetch().leaderFor(t2p0));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, ListOffsetsRequest.LATEST_TIMESTAMP);
        timestampToSearch.put(tp1, ListOffsetsRequest.LATEST_TIMESTAMP);
        timestampToSearch.put(t2p0, ListOffsetsRequest.LATEST_TIMESTAMP);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
            fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        assertNotNull(offsetAndTimestampMap.get(tp0), "Expect Fetcher.offsetsForTimes() to return non-null result for " + tp0);
        assertNotNull(offsetAndTimestampMap.get(tp1), "Expect Fetcher.offsetsForTimes() to return non-null result for " + tp1);
        assertNotNull(offsetAndTimestampMap.get(t2p0), "Expect Fetcher.offsetsForTimes() to return non-null result for " + t2p0);
        assertEquals(11L, offsetAndTimestampMap.get(tp0).offset());
        assertEquals(32L, offsetAndTimestampMap.get(tp1).offset());
        assertEquals(54L, offsetAndTimestampMap.get(t2p0).offset());
    }

    @Test
    public void testGetOffsetsForTimesWhenSomeTopicPartitionLeadersDisconnectException() {
        buildFetcher();
        final String anotherTopic = "another-topic";
        final TopicPartition t2p0 = new TopicPartition(anotherTopic, 0);
        subscriptions.assignFromUser(Utils.mkSet(tp0, t2p0));

        client.reset();

        MetadataResponse initialMetadata = RequestTestUtils.metadataUpdateWith(1, singletonMap(topicName, 1));
        client.updateMetadata(initialMetadata);

        Map<String, Integer> partitionNumByTopic = new HashMap<>();
        partitionNumByTopic.put(topicName, 1);
        partitionNumByTopic.put(anotherTopic, 1);
        MetadataResponse updatedMetadata = RequestTestUtils.metadataUpdateWith(1, partitionNumByTopic);
        client.prepareMetadataUpdate(updatedMetadata);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(tp0, Errors.NONE, 1000L, 11L), true);
        client.prepareResponseFrom(listOffsetResponse(tp0, Errors.NONE, 1000L, 11L), metadata.fetch().leaderFor(tp0));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, ListOffsetsRequest.LATEST_TIMESTAMP);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        assertNotNull(offsetAndTimestampMap.get(tp0), "Expect Fetcher.offsetsForTimes() to return non-null result for " + tp0);
        assertEquals(11L, offsetAndTimestampMap.get(tp0).offset());
        assertNotNull(metadata.fetch().partitionCountForTopic(anotherTopic));
    }

    @Test
    public void testBatchedListOffsetsMetadataErrors() {
        buildFetcher();

        ListOffsetsResponseData data = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(Collections.singletonList(new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Arrays.asList(
                                new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp0.partition())
                                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                    .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET),
                                new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp1.partition())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                    .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)))));
        client.prepareResponse(new ListOffsetsResponse(data));

        Map<TopicPartition, Long> offsetsToSearch = new HashMap<>();
        offsetsToSearch.put(tp0, ListOffsetsRequest.EARLIEST_TIMESTAMP);
        offsetsToSearch.put(tp1, ListOffsetsRequest.EARLIEST_TIMESTAMP);

        assertThrows(TimeoutException.class, () -> fetcher.offsetsForTimes(offsetsToSearch, time.timer(0)));
    }

    @Test
    public void testSkippingAbortedTransactions() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

        abortTransaction(buffer, 1L, currentOffset);

        buffer.flip();

        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        abortedTransactions.add(new FetchResponse.AbortedTransaction(1, 0));
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertFalse(fetchedRecords.containsKey(tp0));
    }

    @Test
    public void testReturnCommittedTransactions() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

        commitTransaction(buffer, 1L, currentOffset);
        buffer.flip();

        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        client.prepareResponse(body -> {
            FetchRequest request = (FetchRequest) body;
            assertEquals(IsolationLevel.READ_COMMITTED, request.isolationLevel());
            return true;
        }, fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));

        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        assertEquals(fetchedRecords.get(tp0).size(), 2);
    }

    @Test
    public void testReadCommittedWithCommittedAndAbortedTransactions() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();

        long pid1 = 1L;
        long pid2 = 2L;

        // Appends for producer 1 (eventually committed)
        appendTransactionalRecords(buffer, pid1, 0L,
                new SimpleRecord("commit1-1".getBytes(), "value".getBytes()),
                new SimpleRecord("commit1-2".getBytes(), "value".getBytes()));

        // Appends for producer 2 (eventually aborted)
        appendTransactionalRecords(buffer, pid2, 2L,
                new SimpleRecord("abort2-1".getBytes(), "value".getBytes()));

        // commit producer 1
        commitTransaction(buffer, pid1, 3L);

        // append more for producer 2 (eventually aborted)
        appendTransactionalRecords(buffer, pid2, 4L,
                new SimpleRecord("abort2-2".getBytes(), "value".getBytes()));

        // abort producer 2
        abortTransaction(buffer, pid2, 5L);
        abortedTransactions.add(new FetchResponse.AbortedTransaction(pid2, 2L));

        // New transaction for producer 1 (eventually aborted)
        appendTransactionalRecords(buffer, pid1, 6L,
                new SimpleRecord("abort1-1".getBytes(), "value".getBytes()));

        // New transaction for producer 2 (eventually committed)
        appendTransactionalRecords(buffer, pid2, 7L,
                new SimpleRecord("commit2-1".getBytes(), "value".getBytes()));

        // Add messages for producer 1 (eventually aborted)
        appendTransactionalRecords(buffer, pid1, 8L,
                new SimpleRecord("abort1-2".getBytes(), "value".getBytes()));

        // abort producer 1
        abortTransaction(buffer, pid1, 9L);
        abortedTransactions.add(new FetchResponse.AbortedTransaction(1, 6));

        // commit producer 2
        commitTransaction(buffer, pid2, 10L);

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        // There are only 3 committed records
        List<ConsumerRecord<byte[], byte[]>> fetchedConsumerRecords = fetchedRecords.get(tp0);
        Set<String> fetchedKeys = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> consumerRecord : fetchedConsumerRecords) {
            fetchedKeys.add(new String(consumerRecord.key(), StandardCharsets.UTF_8));
        }
        assertEquals(Utils.mkSet("commit1-1", "commit1-2", "commit2-1"), fetchedKeys);
    }

    @Test
    public void testMultipleAbortMarkers() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "abort1-1".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "abort1-2".getBytes(), "value".getBytes()));

        currentOffset += abortTransaction(buffer, 1L, currentOffset);
        // Duplicate abort -- should be ignored.
        currentOffset += abortTransaction(buffer, 1L, currentOffset);
        // Now commit a transaction.
        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "commit1-1".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "commit1-2".getBytes(), "value".getBytes()));
        commitTransaction(buffer, 1L, currentOffset);
        buffer.flip();

        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        abortedTransactions.add(new FetchResponse.AbortedTransaction(1, 0));
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        assertEquals(fetchedRecords.get(tp0).size(), 2);
        List<ConsumerRecord<byte[], byte[]>> fetchedConsumerRecords = fetchedRecords.get(tp0);
        Set<String> committedKeys = new HashSet<>(Arrays.asList("commit1-1", "commit1-2"));
        Set<String> actuallyCommittedKeys = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> consumerRecord : fetchedConsumerRecords) {
            actuallyCommittedKeys.add(new String(consumerRecord.key(), StandardCharsets.UTF_8));
        }
        assertEquals(actuallyCommittedKeys, committedKeys);
    }

    @Test
    public void testReadCommittedAbortMarkerWithNoData() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new StringDeserializer(),
                new StringDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        long producerId = 1L;

        abortTransaction(buffer, producerId, 5L);

        appendTransactionalRecords(buffer, producerId, 6L,
                new SimpleRecord("6".getBytes(), null),
                new SimpleRecord("7".getBytes(), null),
                new SimpleRecord("8".getBytes(), null));

        commitTransaction(buffer, producerId, 9L);

        buffer.flip();

        // send the fetch
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());

        // prepare the response. the aborted transactions begin at offsets which are no longer in the log
        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        abortedTransactions.add(new FetchResponse.AbortedTransaction(producerId, 0L));

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(MemoryRecords.readableRecords(buffer),
                abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> allFetchedRecords = fetchedRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<String, String>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(3, fetchedRecords.size());
        assertEquals(Arrays.asList(6L, 7L, 8L), collectRecordOffsets(fetchedRecords));
    }

    @Test
    public void testUpdatePositionWithLastRecordMissingFromBatch() {
        buildFetcher();

        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        // Remove the last record to simulate compaction
        MemoryRecords.FilterResult result = records.filterTo(tp0, new MemoryRecords.RecordFilter() {
            @Override
            protected BatchRetention checkBatchRetention(RecordBatch batch) {
                return BatchRetention.DELETE_EMPTY;
            }

            @Override
            protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                return record.key() != null;
            }
        }, ByteBuffer.allocate(1024), Integer.MAX_VALUE, BufferSupplier.NO_CACHING);
        result.outputBuffer().flip();
        MemoryRecords compactedRecords = MemoryRecords.readableRecords(result.outputBuffer());

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, compactedRecords, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> allFetchedRecords = fetchedRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(3, fetchedRecords.size());

        for (int i = 0; i < 3; i++) {
            assertEquals(Integer.toString(i), new String(fetchedRecords.get(i).key()));
        }

        // The next offset should point to the next batch
        assertEquals(4L, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdatePositionOnEmptyBatch() {
        buildFetcher();

        long producerId = 1;
        short producerEpoch = 0;
        int sequence = 1;
        long baseOffset = 37;
        long lastOffset = 54;
        int partitionLeaderEpoch = 7;
        ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.CURRENT_MAGIC_VALUE, producerId, producerEpoch,
                sequence, baseOffset, lastOffset, partitionLeaderEpoch, TimestampType.CREATE_TIME,
                System.currentTimeMillis(), false, false);
        buffer.flip();
        MemoryRecords recordsWithEmptyBatch = MemoryRecords.readableRecords(buffer);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, recordsWithEmptyBatch, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> allFetchedRecords = fetchedRecords();
        assertTrue(allFetchedRecords.isEmpty());

        // The next offset should point to the next batch
        assertEquals(lastOffset + 1, subscriptions.position(tp0).offset);
    }

    @Test
    public void testReadCommittedWithCompactedTopic() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new StringDeserializer(),
                new StringDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        long pid1 = 1L;
        long pid2 = 2L;
        long pid3 = 3L;

        appendTransactionalRecords(buffer, pid3, 3L,
                new SimpleRecord("3".getBytes(), "value".getBytes()),
                new SimpleRecord("4".getBytes(), "value".getBytes()));

        appendTransactionalRecords(buffer, pid2, 15L,
                new SimpleRecord("15".getBytes(), "value".getBytes()),
                new SimpleRecord("16".getBytes(), "value".getBytes()),
                new SimpleRecord("17".getBytes(), "value".getBytes()));

        appendTransactionalRecords(buffer, pid1, 22L,
                new SimpleRecord("22".getBytes(), "value".getBytes()),
                new SimpleRecord("23".getBytes(), "value".getBytes()));

        abortTransaction(buffer, pid2, 28L);

        appendTransactionalRecords(buffer, pid3, 30L,
                new SimpleRecord("30".getBytes(), "value".getBytes()),
                new SimpleRecord("31".getBytes(), "value".getBytes()),
                new SimpleRecord("32".getBytes(), "value".getBytes()));

        commitTransaction(buffer, pid3, 35L);

        appendTransactionalRecords(buffer, pid1, 39L,
                new SimpleRecord("39".getBytes(), "value".getBytes()),
                new SimpleRecord("40".getBytes(), "value".getBytes()));

        // transaction from pid1 is aborted, but the marker is not included in the fetch

        buffer.flip();

        // send the fetch
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());

        // prepare the response. the aborted transactions begin at offsets which are no longer in the log
        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        abortedTransactions.add(new FetchResponse.AbortedTransaction(pid2, 6L));
        abortedTransactions.add(new FetchResponse.AbortedTransaction(pid1, 0L));

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(MemoryRecords.readableRecords(buffer),
                abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> allFetchedRecords = fetchedRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<String, String>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(5, fetchedRecords.size());
        assertEquals(Arrays.asList(3L, 4L, 30L, 31L, 32L), collectRecordOffsets(fetchedRecords));
    }

    @Test
    public void testReturnAbortedTransactionsinUncommittedMode() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

        abortTransaction(buffer, 1L, currentOffset);

        buffer.flip();

        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        abortedTransactions.add(new FetchResponse.AbortedTransaction(1, 0));
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
    }

    @Test
    public void testConsumerPositionUpdatedWhenSkippingAbortedTransactions() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        long currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "abort1-1".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "abort1-2".getBytes(), "value".getBytes()));

        currentOffset += abortTransaction(buffer, 1L, currentOffset);
        buffer.flip();

        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        abortedTransactions.add(new FetchResponse.AbortedTransaction(1, 0));
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();

        // Ensure that we don't return any of the aborted records, but yet advance the consumer position.
        assertFalse(fetchedRecords.containsKey(tp0));
        assertEquals(currentOffset, subscriptions.position(tp0).offset);
    }

    @Test
    public void testConsumingViaIncrementalFetchRequests() {
        buildFetcher(2);

        List<ConsumerRecord<byte[], byte[]>> records;
        assignFromUser(new HashSet<>(Arrays.asList(tp0, tp1)));
        subscriptions.seekValidated(tp0, new SubscriptionState.FetchPosition(0, Optional.empty(), metadata.currentLeader(tp0)));
        subscriptions.seekValidated(tp1, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp1)));

        // Fetch some records and establish an incremental fetch session.
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions1 = new LinkedHashMap<>();
        partitions1.put(tp0, new FetchResponse.PartitionData<>(Errors.NONE, 2L,
                2, 0L, null, this.records));
        partitions1.put(tp1, new FetchResponse.PartitionData<>(Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, emptyRecords));
        FetchResponse<MemoryRecords> resp1 = new FetchResponse<>(Errors.NONE, partitions1, 0, 123);
        client.prepareResponse(resp1);
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertFalse(fetchedRecords.containsKey(tp1));
        records = fetchedRecords.get(tp0);
        assertEquals(2, records.size());
        assertEquals(3L, subscriptions.position(tp0).offset);
        assertEquals(1L, subscriptions.position(tp1).offset);
        assertEquals(1, records.get(0).offset());
        assertEquals(2, records.get(1).offset());

        // There is still a buffered record.
        assertEquals(0, fetcher.sendFetches());
        fetchedRecords = fetchedRecords();
        assertFalse(fetchedRecords.containsKey(tp1));
        records = fetchedRecords.get(tp0);
        assertEquals(1, records.size());
        assertEquals(3, records.get(0).offset());
        assertEquals(4L, subscriptions.position(tp0).offset);

        // The second response contains no new records.
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions2 = new LinkedHashMap<>();
        FetchResponse<MemoryRecords> resp2 = new FetchResponse<>(Errors.NONE, partitions2, 0, 123);
        client.prepareResponse(resp2);
        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(time.timer(0));
        fetchedRecords = fetchedRecords();
        assertTrue(fetchedRecords.isEmpty());
        assertEquals(4L, subscriptions.position(tp0).offset);
        assertEquals(1L, subscriptions.position(tp1).offset);

        // The third response contains some new records for tp0.
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions3 = new LinkedHashMap<>();
        partitions3.put(tp0, new FetchResponse.PartitionData<>(Errors.NONE, 100L,
                4, 0L, null, this.nextRecords));
        FetchResponse<MemoryRecords> resp3 = new FetchResponse<>(Errors.NONE, partitions3, 0, 123);
        client.prepareResponse(resp3);
        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(time.timer(0));
        fetchedRecords = fetchedRecords();
        assertFalse(fetchedRecords.containsKey(tp1));
        records = fetchedRecords.get(tp0);
        assertEquals(2, records.size());
        assertEquals(6L, subscriptions.position(tp0).offset);
        assertEquals(1L, subscriptions.position(tp1).offset);
        assertEquals(4, records.get(0).offset());
        assertEquals(5, records.get(1).offset());
    }

    @Test
    public void testFetcherConcurrency() throws Exception {
        int numPartitions = 20;
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int i = 0; i < numPartitions; i++)
            topicPartitions.add(new TopicPartition(topicName, i));

        LogContext logContext = new LogContext();
        buildDependencies(new MetricConfig(), Long.MAX_VALUE, new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST), logContext);

        fetcher = new Fetcher<byte[], byte[]>(
                new LogContext(),
                consumerClient,
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                2 * numPartitions,
                true,
                "",
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer(),
                metadata,
                subscriptions,
                metrics,
                metricsRegistry,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                IsolationLevel.READ_UNCOMMITTED,
                apiVersions) {
            @Override
            protected FetchSessionHandler sessionHandler(int id) {
                final FetchSessionHandler handler = super.sessionHandler(id);
                if (handler == null)
                    return null;
                else {
                    return new FetchSessionHandler(new LogContext(), id) {
                        @Override
                        public Builder newBuilder() {
                            verifySessionPartitions();
                            return handler.newBuilder();
                        }

                        @Override
                        public boolean handleResponse(FetchResponse<?> response) {
                            verifySessionPartitions();
                            return handler.handleResponse(response);
                        }

                        @Override
                        public void handleError(Throwable t) {
                            verifySessionPartitions();
                            handler.handleError(t);
                        }

                        // Verify that session partitions can be traversed safely.
                        private void verifySessionPartitions() {
                            try {
                                Field field = FetchSessionHandler.class.getDeclaredField("sessionPartitions");
                                field.setAccessible(true);
                                LinkedHashMap<?, ?> sessionPartitions =
                                        (LinkedHashMap<?, ?>) field.get(handler);
                                for (Map.Entry<?, ?> entry : sessionPartitions.entrySet()) {
                                    // If `sessionPartitions` are modified on another thread, Thread.yield will increase the
                                    // possibility of ConcurrentModificationException if appropriate synchronization is not used.
                                    Thread.yield();
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
                }
            }
        };

        MetadataResponse initialMetadataResponse = RequestTestUtils.metadataUpdateWith(1,
                singletonMap(topicName, numPartitions), tp -> validLeaderEpoch);
        client.updateMetadata(initialMetadataResponse);
        fetchSize = 10000;

        assignFromUser(topicPartitions);
        topicPartitions.forEach(tp -> subscriptions.seek(tp, 0L));

        AtomicInteger fetchesRemaining = new AtomicInteger(1000);
        executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit(() -> {
            while (fetchesRemaining.get() > 0) {
                synchronized (consumerClient) {
                    if (!client.requests().isEmpty()) {
                        ClientRequest request = client.requests().peek();
                        FetchRequest fetchRequest = (FetchRequest) request.requestBuilder().build();
                        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseMap = new LinkedHashMap<>();
                        for (Map.Entry<TopicPartition, FetchRequest.PartitionData> entry : fetchRequest.fetchData().entrySet()) {
                            TopicPartition tp = entry.getKey();
                            long offset = entry.getValue().fetchOffset;
                            responseMap.put(tp, new FetchResponse.PartitionData<>(Errors.NONE, offset + 2L, offset + 2,
                                    0L, null, buildRecords(offset, 2, offset)));
                        }
                        client.respondToRequest(request, new FetchResponse<>(Errors.NONE, responseMap, 0, 123));
                        consumerClient.poll(time.timer(0));
                    }
                }
            }
            return fetchesRemaining.get();
        });
        Map<TopicPartition, Long> nextFetchOffsets = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), t -> 0L));
        while (fetchesRemaining.get() > 0 && !future.isDone()) {
            if (fetcher.sendFetches() == 1) {
                synchronized (consumerClient) {
                    consumerClient.poll(time.timer(0));
                }
            }
            if (fetcher.hasCompletedFetches()) {
                Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
                if (!fetchedRecords.isEmpty()) {
                    fetchesRemaining.decrementAndGet();
                    fetchedRecords.forEach((tp, records) -> {
                        assertEquals(2, records.size());
                        long nextOffset = nextFetchOffsets.get(tp);
                        assertEquals(nextOffset, records.get(0).offset());
                        assertEquals(nextOffset + 1, records.get(1).offset());
                        nextFetchOffsets.put(tp, nextOffset + 2);
                    });
                }
            }
        }
        assertEquals(0, future.get());
    }

    @Test
    public void testFetcherSessionEpochUpdate() throws Exception {
        buildFetcher(2);

        MetadataResponse initialMetadataResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap(topicName, 1));
        client.updateMetadata(initialMetadataResponse);
        assignFromUser(Collections.singleton(tp0));
        subscriptions.seek(tp0, 0L);

        AtomicInteger fetchesRemaining = new AtomicInteger(1000);
        executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit(() -> {
            long nextOffset = 0;
            long nextEpoch = 0;
            while (fetchesRemaining.get() > 0) {
                synchronized (consumerClient) {
                    if (!client.requests().isEmpty()) {
                        ClientRequest request = client.requests().peek();
                        FetchRequest fetchRequest = (FetchRequest) request.requestBuilder().build();
                        int epoch = fetchRequest.metadata().epoch();
                        assertTrue(epoch == 0 || epoch == nextEpoch,
                            String.format("Unexpected epoch expected %d got %d", nextEpoch, epoch));
                        nextEpoch++;
                        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseMap = new LinkedHashMap<>();
                        responseMap.put(tp0, new FetchResponse.PartitionData<>(Errors.NONE, nextOffset + 2L, nextOffset + 2,
                                0L, null, buildRecords(nextOffset, 2, nextOffset)));
                        nextOffset += 2;
                        client.respondToRequest(request, new FetchResponse<>(Errors.NONE, responseMap, 0, 123));
                        consumerClient.poll(time.timer(0));
                    }
                }
            }
            return fetchesRemaining.get();
        });
        long nextFetchOffset = 0;
        while (fetchesRemaining.get() > 0 && !future.isDone()) {
            if (fetcher.sendFetches() == 1) {
                synchronized (consumerClient) {
                    consumerClient.poll(time.timer(0));
                }
            }
            if (fetcher.hasCompletedFetches()) {
                Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
                if (!fetchedRecords.isEmpty()) {
                    fetchesRemaining.decrementAndGet();
                    List<ConsumerRecord<byte[], byte[]>> records = fetchedRecords.get(tp0);
                    assertEquals(2, records.size());
                    assertEquals(nextFetchOffset, records.get(0).offset());
                    assertEquals(nextFetchOffset + 1, records.get(1).offset());
                    nextFetchOffset += 2;
                }
                assertTrue(fetchedRecords().isEmpty());
            }
        }
        assertEquals(0, future.get());
    }

    @Test
    public void testEmptyControlBatch() {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 1;

        // Empty control batch should not cause an exception
        DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.MAGIC_VALUE_V2, 1L,
                (short) 0, -1, 0, 0,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, TimestampType.CREATE_TIME, time.milliseconds(),
                true, true);

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

        commitTransaction(buffer, 1L, currentOffset);
        buffer.flip();

        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        client.prepareResponse(body -> {
            FetchRequest request = (FetchRequest) body;
            assertEquals(IsolationLevel.READ_COMMITTED, request.isolationLevel());
            return true;
        }, fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));

        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchedRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        assertEquals(fetchedRecords.get(tp0).size(), 2);
    }

    private MemoryRecords buildRecords(long baseOffset, int count, long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
    }

    private int appendTransactionalRecords(ByteBuffer buffer, long pid, long baseOffset, int baseSequence, SimpleRecord... records) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
                TimestampType.CREATE_TIME, baseOffset, time.milliseconds(), pid, (short) 0, baseSequence, true,
                RecordBatch.NO_PARTITION_LEADER_EPOCH);

        for (SimpleRecord record : records) {
            builder.append(record);
        }
        builder.build();
        return records.length;
    }

    private int appendTransactionalRecords(ByteBuffer buffer, long pid, long baseOffset, SimpleRecord... records) {
        return appendTransactionalRecords(buffer, pid, baseOffset, (int) baseOffset, records);
    }

    private void commitTransaction(ByteBuffer buffer, long producerId, long baseOffset) {
        short producerEpoch = 0;
        int partitionLeaderEpoch = 0;
        MemoryRecords.writeEndTransactionalMarker(buffer, baseOffset, time.milliseconds(), partitionLeaderEpoch, producerId, producerEpoch,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));
    }

    private int abortTransaction(ByteBuffer buffer, long producerId, long baseOffset) {
        short producerEpoch = 0;
        int partitionLeaderEpoch = 0;
        MemoryRecords.writeEndTransactionalMarker(buffer, baseOffset, time.milliseconds(), partitionLeaderEpoch, producerId, producerEpoch,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));
        return 1;
    }

    private void testGetOffsetsForTimesWithError(Errors errorForP0,
                                                 Errors errorForP1,
                                                 long offsetForP0,
                                                 long offsetForP1,
                                                 Long expectedOffsetForP0,
                                                 Long expectedOffsetForP1) {
        client.reset();
        String topicName2 = "topic2";
        TopicPartition t2p0 = new TopicPartition(topicName2, 0);
        // Expect a metadata refresh.
        metadata.bootstrap(ClientUtils.parseAndValidateAddresses(Collections.singletonList("1.1.1.1:1111"),
                ClientDnsLookup.USE_ALL_DNS_IPS));

        Map<String, Integer> partitionNumByTopic = new HashMap<>();
        partitionNumByTopic.put(topicName, 2);
        partitionNumByTopic.put(topicName2, 1);
        MetadataResponse updateMetadataResponse = RequestTestUtils.metadataUpdateWith(2, partitionNumByTopic);
        Cluster updatedCluster = updateMetadataResponse.cluster();

        // The metadata refresh should contain all the topics.
        client.prepareMetadataUpdate(updateMetadataResponse, true);

        // First try should fail due to metadata error.
        client.prepareResponseFrom(listOffsetResponse(t2p0, errorForP0, offsetForP0, offsetForP0),
                updatedCluster.leaderFor(t2p0));
        client.prepareResponseFrom(listOffsetResponse(tp1, errorForP1, offsetForP1, offsetForP1),
                updatedCluster.leaderFor(tp1));
        // Second try should succeed.
        client.prepareResponseFrom(listOffsetResponse(t2p0, Errors.NONE, offsetForP0, offsetForP0),
                updatedCluster.leaderFor(t2p0));
        client.prepareResponseFrom(listOffsetResponse(tp1, Errors.NONE, offsetForP1, offsetForP1),
                updatedCluster.leaderFor(tp1));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(t2p0, 0L);
        timestampToSearch.put(tp1, 0L);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        if (expectedOffsetForP0 == null)
            assertNull(offsetAndTimestampMap.get(t2p0));
        else {
            assertEquals(expectedOffsetForP0.longValue(), offsetAndTimestampMap.get(t2p0).timestamp());
            assertEquals(expectedOffsetForP0.longValue(), offsetAndTimestampMap.get(t2p0).offset());
        }

        if (expectedOffsetForP1 == null)
            assertNull(offsetAndTimestampMap.get(tp1));
        else {
            assertEquals(expectedOffsetForP1.longValue(), offsetAndTimestampMap.get(tp1).timestamp());
            assertEquals(expectedOffsetForP1.longValue(), offsetAndTimestampMap.get(tp1).offset());
        }
    }

    private void testGetOffsetsForTimesWithUnknownOffset() {
        client.reset();
        // Ensure metadata has both partitions.
        MetadataResponse initialMetadataUpdate = RequestTestUtils.metadataUpdateWith(1, singletonMap(topicName, 1));
        client.updateMetadata(initialMetadataUpdate);

        ListOffsetsResponseData data = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(Collections.singletonList(new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Collections.singletonList(new ListOffsetsPartitionResponse()
                                .setPartitionIndex(tp0.partition())
                                .setErrorCode(Errors.NONE.code())
                                .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)))));

        client.prepareResponseFrom(new ListOffsetsResponse(data),
                metadata.fetch().leaderFor(tp0));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, 0L);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        assertTrue(offsetAndTimestampMap.containsKey(tp0));
        assertNull(offsetAndTimestampMap.get(tp0));
    }

    @Test
    public void testGetOffsetsForTimesWithUnknownOffsetV0() {
        buildFetcher();
        // Empty map
        assertTrue(fetcher.offsetsForTimes(new HashMap<>(), time.timer(100L)).isEmpty());
        // Unknown Offset
        client.reset();
        // Ensure metadata has both partition.
        MetadataResponse initialMetadataUpdate = RequestTestUtils.metadataUpdateWith(1, singletonMap(topicName, 1));
        client.updateMetadata(initialMetadataUpdate);
        // Force LIST_OFFSETS version 0
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create(
            ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 0));

        ListOffsetsResponseData data = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(Collections.singletonList(new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Collections.singletonList(new ListOffsetsPartitionResponse()
                                .setPartitionIndex(tp0.partition())
                                .setErrorCode(Errors.NONE.code())
                                .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                .setOldStyleOffsets(Collections.emptyList())))));

        client.prepareResponseFrom(new ListOffsetsResponse(data),
                metadata.fetch().leaderFor(tp0));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, 0L);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                fetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        assertTrue(offsetAndTimestampMap.containsKey(tp0));
        assertNull(offsetAndTimestampMap.get(tp0));
    }

    @Test
    public void testSubscriptionPositionUpdatedWithEpoch() {
        // Create some records that include a leader epoch (1)
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                1
        );
        builder.appendWithOffset(0L, 0L, "key".getBytes(), "value-1".getBytes());
        builder.appendWithOffset(1L, 0L, "key".getBytes(), "value-2".getBytes());
        builder.appendWithOffset(2L, 0L, "key".getBytes(), "value-3".getBytes());
        MemoryRecords records = builder.build();

        buildFetcher();
        assignFromUser(singleton(tp0));

        // Initialize the epoch=1
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, tp -> 1);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // Seek
        subscriptions.seek(tp0, 0);

        // Do a normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, records, Errors.NONE, 100L, 0));
        consumerClient.pollNoWakeup();
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        assertEquals(subscriptions.position(tp0).offset, 3L);
        assertOptional(subscriptions.position(tp0).offsetEpoch, value -> assertEquals(value.intValue(), 1));
    }

    @Test
    public void testOffsetValidationRequestGrouping() {
        buildFetcher();
        assignFromUser(Utils.mkSet(tp0, tp1, tp2, tp3));

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 3,
            Collections.emptyMap(), singletonMap(topicName, 4),
            tp -> 5), false, 0L);

        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                metadata.currentLeader(tp).leader, Optional.of(4));
            subscriptions.seekUnvalidated(tp,
                new SubscriptionState.FetchPosition(0, Optional.of(4), leaderAndEpoch));
        }

        Set<TopicPartition> allRequestedPartitions = new HashSet<>();

        for (Node node : metadata.fetch().nodes()) {
            apiVersions.update(node.idString(), NodeApiVersions.create());

            Set<TopicPartition> expectedPartitions = subscriptions.assignedPartitions().stream()
                .filter(tp ->
                    metadata.currentLeader(tp).leader.equals(Optional.of(node)))
                .collect(Collectors.toSet());

            assertTrue(expectedPartitions.stream().noneMatch(allRequestedPartitions::contains));
            assertTrue(expectedPartitions.size() > 0);
            allRequestedPartitions.addAll(expectedPartitions);

            OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData();
            expectedPartitions.forEach(tp -> {
                OffsetForLeaderTopicResult topic = data.topics().find(tp.topic());
                if (topic == null) {
                    topic = new OffsetForLeaderTopicResult().setTopic(tp.topic());
                    data.topics().add(topic);
                }
                topic.partitions().add(new EpochEndOffset()
                    .setPartition(tp.partition())
                    .setErrorCode(Errors.NONE.code())
                    .setLeaderEpoch(4)
                    .setEndOffset(0));
            });

            OffsetsForLeaderEpochResponse response = new OffsetsForLeaderEpochResponse(data);
            client.prepareResponseFrom(body -> {
                OffsetsForLeaderEpochRequest request = (OffsetsForLeaderEpochRequest) body;
                return expectedPartitions.equals(offsetForLeaderPartitionMap(request.data()).keySet());
            }, response, node);
        }

        assertEquals(subscriptions.assignedPartitions(), allRequestedPartitions);

        fetcher.validateOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertTrue(subscriptions.assignedPartitions()
            .stream().noneMatch(subscriptions::awaitingValidation));
    }

    @Test
    public void testOffsetValidationAwaitsNodeApiVersion() {
        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        Node node = metadata.fetch().nodes().get(0);
        assertFalse(client.isConnected(node.idString()));

        // Seek with a position and leader+epoch
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(20L, Optional.of(epochOne), leaderAndEpoch));
        assertFalse(client.isConnected(node.idString()));
        assertTrue(subscriptions.awaitingValidation(tp0));

        // No version information is initially available, but the node is now connected
        fetcher.validateOffsetsIfNeeded();
        assertTrue(subscriptions.awaitingValidation(tp0));
        assertTrue(client.isConnected(node.idString()));
        apiVersions.update(node.idString(), NodeApiVersions.create());

        // On the next call, the OffsetForLeaderEpoch request is sent and validation completes
        client.prepareResponseFrom(
            prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, epochOne, 30L),
            node);

        fetcher.validateOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.awaitingValidation(tp0));
        assertEquals(20L, subscriptions.position(tp0).offset);
    }

    @Test
    public void testOffsetValidationSkippedForOldBroker() {
        // Old brokers may require CLUSTER permission to use the OffsetForLeaderEpoch API,
        // so we should skip offset validation and not send the request.

        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;
        final int epochTwo = 2;

        // Start with metadata, epoch=1
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create(
            ApiKeys.OFFSET_FOR_LEADER_EPOCH.id, (short) 0, (short) 2));

        {
            // Seek with a position and leader+epoch
            Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                    metadata.currentLeader(tp0).leader, Optional.of(epochOne));
            subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(epochOne), leaderAndEpoch));

            // Update metadata to epoch=2, enter validation
            metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                    Collections.emptyMap(), partitionCounts, tp -> epochTwo), false, 0L);
            fetcher.validateOffsetsIfNeeded();

            // Offset validation is skipped
            assertFalse(subscriptions.awaitingValidation(tp0));
        }

        {
            // Seek with a position and leader+epoch
            Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                    metadata.currentLeader(tp0).leader, Optional.of(epochOne));
            subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(epochOne), leaderAndEpoch));

            // Update metadata to epoch=2, enter validation
            metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                    Collections.emptyMap(), partitionCounts, tp -> epochTwo), false, 0L);

            // Subscription should not stay in AWAITING_VALIDATION in prepareFetchRequest
            assertEquals(1, fetcher.sendFetches());
            assertFalse(subscriptions.awaitingValidation(tp0));
        }
    }

    @Test
    public void testOffsetValidationSkippedForOldResponse() {
        // Old responses may provide unreliable leader epoch,
        // so we should skip offset validation and not send the request.
        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
            Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        Node node = metadata.fetch().nodes().get(0);
        assertFalse(client.isConnected(node.idString()));

        // Seek with a position and leader+epoch
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
            metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(20L, Optional.of(epochOne), leaderAndEpoch));
        assertFalse(client.isConnected(node.idString()));
        assertTrue(subscriptions.awaitingValidation(tp0));

        // Inject an older version of the metadata response
        final short responseVersion = 8;
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
            Collections.emptyMap(), partitionCounts, responseVersion), false, 0L);
        fetcher.validateOffsetsIfNeeded();
        // Offset validation is skipped
        assertFalse(subscriptions.awaitingValidation(tp0));
    }

    @Test
    public void testOffsetValidationResetOffsetForUndefinedEpochWithDefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            UNDEFINED_EPOCH, 0L, OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void testOffsetValidationResetOffsetForUndefinedOffsetWithDefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            2, UNDEFINED_EPOCH_OFFSET, OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void testOffsetValidationResetOffsetForUndefinedEpochWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            UNDEFINED_EPOCH, 0L, OffsetResetStrategy.NONE);
    }

    @Test
    public void testOffsetValidationResetOffsetForUndefinedOffsetWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            2, UNDEFINED_EPOCH_OFFSET, OffsetResetStrategy.NONE);
    }

    @Test
    public void testOffsetValidationTriggerLogTruncationForBadOffsetWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            1, 1L, OffsetResetStrategy.NONE);
    }

    private void testOffsetValidationWithGivenEpochOffset(int leaderEpoch,
                                                          long endOffset,
                                                          OffsetResetStrategy offsetResetStrategy) {
        buildFetcher(offsetResetStrategy);
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;
        final long initialOffset = 5;

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
            Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create());

        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(initialOffset, Optional.of(epochOne), leaderAndEpoch));

        fetcher.validateOffsetsIfNeeded();

        consumerClient.poll(time.timer(Duration.ZERO));
        assertTrue(subscriptions.awaitingValidation(tp0));
        assertTrue(client.hasInFlightRequests());

        client.respond(
            offsetsForLeaderEpochRequestMatcher(tp0, epochOne, epochOne),
            prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, leaderEpoch, endOffset));
        consumerClient.poll(time.timer(Duration.ZERO));

        if (offsetResetStrategy == OffsetResetStrategy.NONE) {
            LogTruncationException thrown =
                assertThrows(LogTruncationException.class, () -> fetcher.validateOffsetsIfNeeded());
            assertEquals(singletonMap(tp0, initialOffset), thrown.offsetOutOfRangePartitions());

            if (endOffset == UNDEFINED_EPOCH_OFFSET || leaderEpoch == UNDEFINED_EPOCH) {
                assertEquals(Collections.emptyMap(), thrown.divergentOffsets());
            } else {
                OffsetAndMetadata expectedDivergentOffset = new OffsetAndMetadata(
                    endOffset, Optional.of(leaderEpoch), "");
                assertEquals(singletonMap(tp0, expectedDivergentOffset), thrown.divergentOffsets());
            }
            assertTrue(subscriptions.awaitingValidation(tp0));
        } else {
            fetcher.validateOffsetsIfNeeded();
            assertFalse(subscriptions.awaitingValidation(tp0));
        }
    }

    @Test
    public void testOffsetValidationHandlesSeekWithInflightOffsetForLeaderRequest() {
        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create());

        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(epochOne), leaderAndEpoch));

        fetcher.validateOffsetsIfNeeded();
        consumerClient.poll(time.timer(Duration.ZERO));
        assertTrue(subscriptions.awaitingValidation(tp0));
        assertTrue(client.hasInFlightRequests());

        // While the OffsetForLeaderEpoch request is in-flight, we seek to a different offset.
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(5, Optional.of(epochOne), leaderAndEpoch));
        assertTrue(subscriptions.awaitingValidation(tp0));

        client.respond(
            offsetsForLeaderEpochRequestMatcher(tp0, epochOne, epochOne),
            prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, 0, 0L));
        consumerClient.poll(time.timer(Duration.ZERO));

        // The response should be ignored since we were validating a different position.
        assertTrue(subscriptions.awaitingValidation(tp0));
    }

    @Test
    public void testOffsetValidationFencing() {
        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;
        final int epochTwo = 2;
        final int epochThree = 3;

        // Start with metadata, epoch=1
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create());

        // Seek with a position and leader+epoch
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekValidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(epochOne), leaderAndEpoch));

        // Update metadata to epoch=2, enter validation
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochTwo), false, 0L);
        fetcher.validateOffsetsIfNeeded();
        assertTrue(subscriptions.awaitingValidation(tp0));

        // Update the position to epoch=3, as we would from a fetch
        subscriptions.completeValidation(tp0);
        SubscriptionState.FetchPosition nextPosition = new SubscriptionState.FetchPosition(
                10,
                Optional.of(epochTwo),
                new Metadata.LeaderAndEpoch(leaderAndEpoch.leader, Optional.of(epochTwo)));
        subscriptions.position(tp0, nextPosition);
        subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, tp0, new Metadata.LeaderAndEpoch(leaderAndEpoch.leader, Optional.of(epochThree)));

        // Prepare offset list response from async validation with epoch=2
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, epochTwo, 10L));
        consumerClient.pollNoWakeup();
        assertTrue(subscriptions.awaitingValidation(tp0), "Expected validation to fail since leader epoch changed");

        // Next round of validation, should succeed in validating the position
        fetcher.validateOffsetsIfNeeded();
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, epochThree, 10L));
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.awaitingValidation(tp0), "Expected validation to succeed with latest epoch");
    }

    @Test
    public void testSkipValidationForOlderApiVersion() {
        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        apiVersions.update("0", NodeApiVersions.create(ApiKeys.OFFSET_FOR_LEADER_EPOCH.id, (short) 0, (short) 2));

        // Start with metadata, epoch=1
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> 1), false, 0L);

        // Request offset reset
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Since we have no position due to reset, no fetch is sent
        assertEquals(0, fetcher.sendFetches());

        // Still no position, ensure offset validation logic did not transition us to FETCHING state
        assertEquals(0, fetcher.sendFetches());

        // Complete reset and now we can fetch
        fetcher.resetOffsetIfNeeded(tp0, OffsetResetStrategy.LATEST,
                new Fetcher.ListOffsetData(100, 1L, Optional.empty()));
        assertEquals(1, fetcher.sendFetches());
    }

    @Test
    public void testTruncationDetected() {
        // Create some records that include a leader epoch (1)
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                1 // record epoch is earlier than the leader epoch on the client
        );
        builder.appendWithOffset(0L, 0L, "key".getBytes(), "value-1".getBytes());
        builder.appendWithOffset(1L, 0L, "key".getBytes(), "value-2".getBytes());
        builder.appendWithOffset(2L, 0L, "key".getBytes(), "value-3".getBytes());
        MemoryRecords records = builder.build();

        buildFetcher();
        assignFromUser(singleton(tp0));

        // Initialize the epoch=2
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, tp -> 2);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create());

        // Seek
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(1));
        subscriptions.seekValidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(1), leaderAndEpoch));

        // Check for truncation, this should cause tp0 to go into validation
        fetcher.validateOffsetsIfNeeded();

        // No fetches sent since we entered validation
        assertEquals(0, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        assertTrue(subscriptions.awaitingValidation(tp0));

        // Prepare OffsetForEpoch response then check that we update the subscription position correctly.
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, 1, 10L));
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.awaitingValidation(tp0));

        // Fetch again, now it works
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, records, Errors.NONE, 100L, 0));
        consumerClient.pollNoWakeup();
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        assertEquals(subscriptions.position(tp0).offset, 3L);
        assertOptional(subscriptions.position(tp0).offsetEpoch, value -> assertEquals(value.intValue(), 1));
    }

    @Test
    public void testPreferredReadReplica() {
        buildFetcher(new MetricConfig(), OffsetResetStrategy.EARLIEST, new BytesDeserializer(), new BytesDeserializer(),
                Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED, Duration.ofMinutes(5).toMillis());

        subscriptions.assignFromUser(singleton(tp0));
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(2, singletonMap(topicName, 4), tp -> validLeaderEpoch));
        subscriptions.seek(tp0, 0);

        // Node preferred replica before first fetch response
        Node selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(selected.id(), -1);

        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Set preferred read replica to node=1
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(1)));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        // verify
        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(selected.id(), 1);


        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Set preferred read replica to node=2, which isn't in our metadata, should revert to leader
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(2)));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchedRecords();
        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(selected.id(), -1);
    }

    @Test
    public void testPreferredReadReplicaOffsetError() {
        buildFetcher(new MetricConfig(), OffsetResetStrategy.EARLIEST, new BytesDeserializer(), new BytesDeserializer(),
                Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED, Duration.ofMinutes(5).toMillis());

        subscriptions.assignFromUser(singleton(tp0));
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(2, singletonMap(topicName, 4), tp -> validLeaderEpoch));

        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(1)));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        fetchedRecords();

        Node selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(selected.id(), 1);

        // Return an error, should unset the preferred read replica
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.OFFSET_OUT_OF_RANGE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.empty()));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        fetchedRecords();

        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(selected.id(), -1);
    }

    @Test
    public void testFetchCompletedBeforeHandlerAdded() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        fetcher.sendFetches();
        client.prepareResponse(fullFetchResponse(tp0, buildRecords(1L, 1, 1), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        fetchedRecords();

        Metadata.LeaderAndEpoch leaderAndEpoch = subscriptions.position(tp0).currentLeader;
        assertTrue(leaderAndEpoch.leader.isPresent());
        Node readReplica = fetcher.selectReadReplica(tp0, leaderAndEpoch.leader.get(), time.milliseconds());

        AtomicBoolean wokenUp = new AtomicBoolean(false);
        client.setWakeupHook(() -> {
            if (!wokenUp.getAndSet(true)) {
                consumerClient.disconnectAsync(readReplica);
                consumerClient.poll(time.timer(0));
            }
        });

        assertEquals(1, fetcher.sendFetches());

        consumerClient.disconnectAsync(readReplica);
        consumerClient.poll(time.timer(0));

        assertEquals(1, fetcher.sendFetches());
    }

    @Test
    public void testCorruptMessageError() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Prepare a response with the CORRUPT_MESSAGE error.
        client.prepareResponse(fullFetchResponse(
                tp0,
                buildRecords(1L, 1, 1),
                Errors.CORRUPT_MESSAGE,
                100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        // Trigger the exception.
        assertThrows(KafkaException.class, this::fetchedRecords);
    }

    @Test
    public void testBeginningOffsets() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(listOffsetResponse(tp0, Errors.NONE, ListOffsetsRequest.EARLIEST_TIMESTAMP, 2L));
        assertEquals(singletonMap(tp0, 2L), fetcher.beginningOffsets(singleton(tp0), time.timer(5000L)));
    }

    @Test
    public void testBeginningOffsetsDuplicateTopicPartition() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(listOffsetResponse(tp0, Errors.NONE, ListOffsetsRequest.EARLIEST_TIMESTAMP, 2L));
        assertEquals(singletonMap(tp0, 2L), fetcher.beginningOffsets(asList(tp0, tp0), time.timer(5000L)));
    }

    @Test
    public void testBeginningOffsetsMultipleTopicPartitions() {
        buildFetcher();
        Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(tp0, 2L);
        expectedOffsets.put(tp1, 4L);
        expectedOffsets.put(tp2, 6L);
        assignFromUser(expectedOffsets.keySet());
        client.prepareResponse(listOffsetResponse(expectedOffsets, Errors.NONE, ListOffsetsRequest.EARLIEST_TIMESTAMP, ListOffsetsResponse.UNKNOWN_EPOCH));
        assertEquals(expectedOffsets, fetcher.beginningOffsets(asList(tp0, tp1, tp2), time.timer(5000L)));
    }

    @Test
    public void testBeginningOffsetsEmpty() {
        buildFetcher();
        assertEquals(emptyMap(), fetcher.beginningOffsets(emptyList(), time.timer(5000L)));
    }

    @Test
    public void testEndOffsets() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(listOffsetResponse(tp0, Errors.NONE, ListOffsetsRequest.LATEST_TIMESTAMP, 5L));
        assertEquals(singletonMap(tp0, 5L), fetcher.endOffsets(singleton(tp0), time.timer(5000L)));
    }

    @Test
    public void testEndOffsetsDuplicateTopicPartition() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(listOffsetResponse(tp0, Errors.NONE, ListOffsetsRequest.LATEST_TIMESTAMP, 5L));
        assertEquals(singletonMap(tp0, 5L), fetcher.endOffsets(asList(tp0, tp0), time.timer(5000L)));
    }

    @Test
    public void testEndOffsetsMultipleTopicPartitions() {
        buildFetcher();
        Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(tp0, 5L);
        expectedOffsets.put(tp1, 7L);
        expectedOffsets.put(tp2, 9L);
        assignFromUser(expectedOffsets.keySet());
        client.prepareResponse(listOffsetResponse(expectedOffsets, Errors.NONE, ListOffsetsRequest.LATEST_TIMESTAMP, ListOffsetsResponse.UNKNOWN_EPOCH));
        assertEquals(expectedOffsets, fetcher.endOffsets(asList(tp0, tp1, tp2), time.timer(5000L)));
    }

    @Test
    public void testEndOffsetsEmpty() {
        buildFetcher();
        assertEquals(emptyMap(), fetcher.endOffsets(emptyList(), time.timer(5000L)));
    }

    private MockClient.RequestMatcher offsetsForLeaderEpochRequestMatcher(
        TopicPartition topicPartition,
        int currentLeaderEpoch,
        int leaderEpoch
    ) {
        return request -> {
            OffsetsForLeaderEpochRequest epochRequest = (OffsetsForLeaderEpochRequest) request;
            OffsetForLeaderPartition partition = offsetForLeaderPartitionMap(epochRequest.data())
                .get(topicPartition);
            return partition != null
                && partition.currentLeaderEpoch() == currentLeaderEpoch
                && partition.leaderEpoch() == leaderEpoch;
        };
    }

    private OffsetsForLeaderEpochResponse prepareOffsetsForLeaderEpochResponse(
        TopicPartition topicPartition,
        Errors error,
        int leaderEpoch,
        long endOffset
    ) {
        OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData();
        data.topics().add(new OffsetForLeaderTopicResult()
            .setTopic(topicPartition.topic())
            .setPartitions(Collections.singletonList(new EpochEndOffset()
                .setPartition(topicPartition.partition())
                .setErrorCode(error.code())
                .setLeaderEpoch(leaderEpoch)
                .setEndOffset(endOffset))));
        return new OffsetsForLeaderEpochResponse(data);
    }

    private Map<TopicPartition, OffsetForLeaderPartition> offsetForLeaderPartitionMap(
        OffsetForLeaderEpochRequestData data
    ) {
        Map<TopicPartition, OffsetForLeaderPartition> result = new HashMap<>();
        data.topics().forEach(topic ->
            topic.partitions().forEach(partition ->
                result.put(new TopicPartition(topic.topic(), partition.partition()), partition)));
        return result;
    }

    private MockClient.RequestMatcher listOffsetRequestMatcher(final long timestamp) {
        return listOffsetRequestMatcher(timestamp, ListOffsetsResponse.UNKNOWN_EPOCH);
    }

    private MockClient.RequestMatcher listOffsetRequestMatcher(final long timestamp, final int leaderEpoch) {
        // matches any list offset request with the provided timestamp
        return body -> {
            ListOffsetsRequest req = (ListOffsetsRequest) body;
            ListOffsetsTopic topic = req.topics().get(0);
            ListOffsetsPartition partition = topic.partitions().get(0);
            return tp0.topic().equals(topic.name())
                    && tp0.partition() == partition.partitionIndex()
                    && timestamp == partition.timestamp()
                    && leaderEpoch == partition.currentLeaderEpoch();
        };
    }

    private ListOffsetsResponse listOffsetResponse(Errors error, long timestamp, long offset) {
        return listOffsetResponse(tp0, error, timestamp, offset);
    }

    private ListOffsetsResponse listOffsetResponse(TopicPartition tp, Errors error, long timestamp, long offset) {
        return listOffsetResponse(tp, error, timestamp, offset, ListOffsetsResponse.UNKNOWN_EPOCH);
    }

    private ListOffsetsResponse listOffsetResponse(TopicPartition tp, Errors error, long timestamp, long offset, int leaderEpoch) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp, offset);
        return listOffsetResponse(offsets, error, timestamp, leaderEpoch);
    }

    private ListOffsetsResponse listOffsetResponse(Map<TopicPartition, Long> offsets, Errors error, long timestamp, int leaderEpoch) {
        Map<String, List<ListOffsetsPartitionResponse>> responses = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            responses.putIfAbsent(tp.topic(), new ArrayList<>());
            responses.get(tp.topic()).add(new ListOffsetsPartitionResponse()
                                .setPartitionIndex(tp.partition())
                                .setErrorCode(error.code())
                                .setOffset(entry.getValue())
                                .setTimestamp(timestamp)
                                .setLeaderEpoch(leaderEpoch));
        }
        List<ListOffsetsTopicResponse> topics = new ArrayList<>();
        for (Map.Entry<String, List<ListOffsetsPartitionResponse>> response : responses.entrySet()) {
            topics.add(new ListOffsetsTopicResponse()
                    .setName(response.getKey())
                    .setPartitions(response.getValue()));
        }
        ListOffsetsResponseData data = new ListOffsetsResponseData().setTopics(topics);
        return new ListOffsetsResponse(data);
    }

    private FetchResponse<MemoryRecords> fullFetchResponseWithAbortedTransactions(MemoryRecords records,
                                                                                  List<FetchResponse.AbortedTransaction> abortedTransactions,
                                                                                  Errors error,
                                                                                  long lastStableOffset,
                                                                                  long hw,
                                                                                  int throttleTime) {
        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = Collections.singletonMap(tp0,
                new FetchResponse.PartitionData<>(error, hw, lastStableOffset, 0L, abortedTransactions, records));
        return new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions), throttleTime, INVALID_SESSION_ID);
    }

    private FetchResponse<MemoryRecords> fullFetchResponse(TopicPartition tp, MemoryRecords records, Errors error, long hw, int throttleTime) {
        return fullFetchResponse(tp, records, error, hw, FetchResponse.INVALID_LAST_STABLE_OFFSET, throttleTime);
    }

    private FetchResponse<MemoryRecords> fullFetchResponse(TopicPartition tp, MemoryRecords records, Errors error, long hw,
                                            long lastStableOffset, int throttleTime) {
        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = Collections.singletonMap(tp,
                new FetchResponse.PartitionData<>(error, hw, lastStableOffset, 0L, null, records));
        return new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions), throttleTime, INVALID_SESSION_ID);
    }

    private FetchResponse<MemoryRecords> fullFetchResponse(TopicPartition tp, MemoryRecords records, Errors error, long hw,
                                                           long lastStableOffset, int throttleTime, Optional<Integer> preferredReplicaId) {
        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = Collections.singletonMap(tp,
                new FetchResponse.PartitionData<>(error, hw, lastStableOffset, 0L,
                        preferredReplicaId, null, records));
        return new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions), throttleTime, INVALID_SESSION_ID);
    }

    private FetchResponse<MemoryRecords> fetchResponse(TopicPartition tp, MemoryRecords records, Errors error, long hw,
                                        long lastStableOffset, long logStartOffset, int throttleTime) {
        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = Collections.singletonMap(tp,
                new FetchResponse.PartitionData<>(error, hw, lastStableOffset, logStartOffset, null, records));
        return new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions), throttleTime, INVALID_SESSION_ID);
    }

    private MetadataResponse newMetadataResponse(String topic, Errors error) {
        List<MetadataResponse.PartitionMetadata> partitionsMetadata = new ArrayList<>();
        if (error == Errors.NONE) {
            Optional<MetadataResponse.TopicMetadata> foundMetadata = initialUpdateResponse.topicMetadata()
                    .stream()
                    .filter(topicMetadata -> topicMetadata.topic().equals(topic))
                    .findFirst();
            foundMetadata.ifPresent(topicMetadata -> {
                partitionsMetadata.addAll(topicMetadata.partitionMetadata());
            });
        }

        MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(error, topic, false,
                partitionsMetadata);
        List<Node> brokers = new ArrayList<>(initialUpdateResponse.brokers());
        return RequestTestUtils.metadataResponse(brokers, initialUpdateResponse.clusterId(),
                initialUpdateResponse.controller().id(), Collections.singletonList(topicMetadata));
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        return (Map) fetcher.fetchedRecords();
    }

    private void buildFetcher(int maxPollRecords) {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                maxPollRecords, IsolationLevel.READ_UNCOMMITTED);
    }

    private void buildFetcher() {
        buildFetcher(Integer.MAX_VALUE);
    }

    private void buildFetcher(Deserializer<?> keyDeserializer,
                              Deserializer<?> valueDeserializer) {
        buildFetcher(OffsetResetStrategy.EARLIEST, keyDeserializer, valueDeserializer,
                Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
    }

    private void buildFetcher(OffsetResetStrategy offsetResetStrategy) {
        buildFetcher(new MetricConfig(), offsetResetStrategy,
            new ByteArrayDeserializer(), new ByteArrayDeserializer(),
            Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
    }

    private <K, V> void buildFetcher(OffsetResetStrategy offsetResetStrategy,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel) {
        buildFetcher(new MetricConfig(), offsetResetStrategy, keyDeserializer, valueDeserializer,
                maxPollRecords, isolationLevel);
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     OffsetResetStrategy offsetResetStrategy,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel) {
        buildFetcher(metricConfig, offsetResetStrategy, keyDeserializer, valueDeserializer, maxPollRecords, isolationLevel, Long.MAX_VALUE);
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     OffsetResetStrategy offsetResetStrategy,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel,
                                     long metadataExpireMs) {
        LogContext logContext = new LogContext();
        SubscriptionState subscriptionState = new SubscriptionState(logContext, offsetResetStrategy);
        buildFetcher(metricConfig, keyDeserializer, valueDeserializer, maxPollRecords, isolationLevel, metadataExpireMs,
                subscriptionState, logContext);
    }

    private void buildFetcher(SubscriptionState subscriptionState, LogContext logContext) {
        buildFetcher(new MetricConfig(), new ByteArrayDeserializer(), new ByteArrayDeserializer(), Integer.MAX_VALUE,
                IsolationLevel.READ_UNCOMMITTED, Long.MAX_VALUE, subscriptionState, logContext);
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel,
                                     long metadataExpireMs,
                                     SubscriptionState subscriptionState,
                                     LogContext logContext) {
        buildDependencies(metricConfig, metadataExpireMs, subscriptionState, logContext);
        fetcher = new Fetcher<>(
                new LogContext(),
                consumerClient,
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                true, // check crc
                "",
                keyDeserializer,
                valueDeserializer,
                metadata,
                subscriptions,
                metrics,
                metricsRegistry,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                isolationLevel,
                apiVersions);
    }

    private void buildDependencies(MetricConfig metricConfig,
                                   long metadataExpireMs,
                                   SubscriptionState subscriptionState,
                                   LogContext logContext) {
        time = new MockTime(1);
        subscriptions = subscriptionState;
        metadata = new ConsumerMetadata(0, metadataExpireMs, false, false,
                subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = new Metrics(metricConfig, time);
        consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time,
                100, 1000, Integer.MAX_VALUE);
        metricsRegistry = new FetcherMetricsRegistry(metricConfig.tags().keySet(), "consumer" + groupId);
    }

    private <T> List<Long> collectRecordOffsets(List<ConsumerRecord<T, T>> records) {
        return records.stream().map(ConsumerRecord::offset).collect(Collectors.toList());
    }
}
