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
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
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
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class FetcherTest {
    private ConsumerRebalanceListener listener = new NoOpConsumerRebalanceListener();
    private String topicName = "test";
    private String groupId = "test-group";
    private final String metricGroup = "consumer" + groupId + "-fetch-manager-metrics";
    private TopicPartition tp0 = new TopicPartition(topicName, 0);
    private TopicPartition tp1 = new TopicPartition(topicName, 1);
    private TopicPartition tp2 = new TopicPartition(topicName, 2);
    private TopicPartition tp3 = new TopicPartition(topicName, 3);
    private int minBytes = 1;
    private int maxBytes = Integer.MAX_VALUE;
    private int maxWaitMs = 0;
    private int fetchSize = 1000;
    private long retryBackoffMs = 100;
    private long requestTimeoutMs = 30000;
    private MockTime time = new MockTime(1);
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE, true);
    private MockClient client = new MockClient(time, metadata);
    private Cluster cluster = TestUtils.singletonCluster(topicName, 4);
    private Node node = cluster.nodes().get(0);
    private Metrics metrics = new Metrics(time);
    FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry("consumer" + groupId);

    private SubscriptionState subscriptions = new SubscriptionState(OffsetResetStrategy.EARLIEST);
    private SubscriptionState subscriptionsNoAutoReset = new SubscriptionState(OffsetResetStrategy.NONE);
    private static final double EPSILON = 0.0001;
    private ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(new LogContext(),
            client, metadata, time, 100, 1000, Integer.MAX_VALUE);

    private MemoryRecords records;
    private MemoryRecords nextRecords;
    private MemoryRecords emptyRecords;
    private MemoryRecords partialRecords;
    private Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, metrics);
    private Metrics fetcherMetrics = new Metrics(time);
    private Fetcher<byte[], byte[]> fetcherNoAutoReset = createFetcher(subscriptionsNoAutoReset, fetcherMetrics);

    @Before
    public void setup() throws Exception {
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());
        client.setNode(node);

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, 1L);
        builder.append(0L, "key".getBytes(), "value-1".getBytes());
        builder.append(0L, "key".getBytes(), "value-2".getBytes());
        builder.append(0L, "key".getBytes(), "value-3".getBytes());
        records = builder.build();

        builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, 4L);
        builder.append(0L, "key".getBytes(), "value-4".getBytes());
        builder.append(0L, "key".getBytes(), "value-5".getBytes());
        nextRecords = builder.build();

        builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        emptyRecords = builder.build();

        builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, 4L);
        builder.append(0L, "key".getBytes(), "value-0".getBytes());
        partialRecords = builder.build();
        partialRecords.buffer().putInt(Records.SIZE_OFFSET, 10000);
    }

    @After
    public void teardown() {
        this.metrics.close();
        this.fetcherMetrics.close();
        this.fetcher.close();
        this.fetcherMetrics.close();
    }

    @Test
    public void testFetchNormal() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetcher.fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(3, records.size());
        assertEquals(4L, subscriptions.position(tp0).longValue()); // this is the next fetching position
        long offset = 1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertEquals(offset, record.offset());
            offset += 1;
        }
    }

    @Test
    public void testFetchSkipsBlackedOutNodes() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        client.blackout(node, 500);
        assertEquals(0, fetcher.sendFetches());

        time.sleep(500);
        assertEquals(1, fetcher.sendFetches());
    }

    @Test
    public void testFetcherIgnoresControlRecords() {
        subscriptions.assignFromUser(singleton(tp0));
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
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetcher.fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(1, records.size());
        assertEquals(2L, subscriptions.position(tp0).longValue());

        ConsumerRecord<byte[], byte[]> record = records.get(0);
        assertArrayEquals("key".getBytes(), record.key());
    }

    @Test
    public void testFetchError() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NOT_LEADER_FOR_PARTITION, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetcher.fetchedRecords();
        assertFalse(partitionRecords.containsKey(tp0));
    }

    private MockClient.RequestMatcher matchesOffset(final TopicPartition tp, final long offset) {
        return new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                FetchRequest fetch = (FetchRequest) body;
                return fetch.fetchData().containsKey(tp) &&
                        fetch.fetchData().get(tp).fetchOffset == offset;
            }
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

        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(time), deserializer, deserializer);

        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        client.prepareResponse(matchesOffset(tp0, 1), fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(0);
        // The fetcher should block on Deserialization error
        for (int i = 0; i < 2; i++) {
            try {
                fetcher.fetchedRecords();
                fail("fetchedRecords should have raised");
            } catch (SerializationException e) {
                // the position should not advance since no data has been returned
                assertEquals(1, subscriptions.position(tp0).longValue());
            }
        }
    }

    @Test
    public void testParseCorruptedRecord() throws Exception {
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

        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(0);

        // the first fetchedRecords() should return the first valid message
        assertEquals(1, fetcher.fetchedRecords().get(tp0).size());
        assertEquals(1, subscriptions.position(tp0).longValue());

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
                assertEquals(blockedOffset, subscriptions.position(tp0).longValue());
            }
        }
    }

    private void seekAndConsumeRecord(ByteBuffer responseBuffer, long toOffset) {
        // Seek to skip the bad record and fetch again.
        subscriptions.seek(tp0, toOffset);
        // Should not throw exception after the seek.
        fetcher.fetchedRecords();
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, MemoryRecords.readableRecords(responseBuffer), Errors.NONE, 100L, 0));
        consumerClient.poll(0);

        List<ConsumerRecord<byte[], byte[]>> records = fetcher.fetchedRecords().get(tp0);
        assertEquals(1, records.size());
        assertEquals(toOffset, records.get(0).offset());
        assertEquals(toOffset + 1, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testInvalidDefaultRecordBatch() {
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

        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(0);

        // the fetchedRecords() should always throw exception due to the bad batch.
        for (int i = 0; i < 2; i++) {
            try {
                fetcher.fetchedRecords();
                fail("fetchedRecords should have raised KafkaException");
            } catch (KafkaException e) {
                assertEquals(0, subscriptions.position(tp0).longValue());
            }
        }
    }

    @Test
    public void testParseInvalidRecordBatch() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        ByteBuffer buffer = records.buffer();

        // flip some bits to fail the crc
        buffer.putInt(32, buffer.get(32) ^ 87238423);

        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        try {
            fetcher.fetchedRecords();
            fail("fetchedRecords should have raised");
        } catch (KafkaException e) {
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp0).longValue());
        }
    }

    @Test
    public void testHeaders() {
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(time));

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
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        client.prepareResponse(matchesOffset(tp0, 1), fullFetchResponse(tp0, memoryRecords, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(0);
        records = fetcher.fetchedRecords().get(tp0);

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
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(time), 2);

        List<ConsumerRecord<byte[], byte[]>> records;
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        client.prepareResponse(matchesOffset(tp0, 1), fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        client.prepareResponse(matchesOffset(tp0, 4), fullFetchResponse(tp0, this.nextRecords, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(0);
        records = fetcher.fetchedRecords().get(tp0);
        assertEquals(2, records.size());
        assertEquals(3L, subscriptions.position(tp0).longValue());
        assertEquals(1, records.get(0).offset());
        assertEquals(2, records.get(1).offset());

        assertEquals(0, fetcher.sendFetches());
        consumerClient.poll(0);
        records = fetcher.fetchedRecords().get(tp0);
        assertEquals(1, records.size());
        assertEquals(4L, subscriptions.position(tp0).longValue());
        assertEquals(3, records.get(0).offset());

        assertTrue(fetcher.sendFetches() > 0);
        consumerClient.poll(0);
        records = fetcher.fetchedRecords().get(tp0);
        assertEquals(2, records.size());
        assertEquals(6L, subscriptions.position(tp0).longValue());
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
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(time), 2);

        List<ConsumerRecord<byte[], byte[]>> records;
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        // Returns 3 records while `max.poll.records` is configured to 2
        client.prepareResponse(matchesOffset(tp0, 1), fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(0);
        records = fetcher.fetchedRecords().get(tp0);
        assertEquals(2, records.size());
        assertEquals(3L, subscriptions.position(tp0).longValue());
        assertEquals(1, records.get(0).offset());
        assertEquals(2, records.get(1).offset());

        subscriptions.assignFromUser(singleton(tp1));
        client.prepareResponse(matchesOffset(tp1, 4), fullFetchResponse(tp1, this.nextRecords, Errors.NONE, 100L, 0));
        subscriptions.seek(tp1, 4);

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(0);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetcher.fetchedRecords();
        assertNull(fetchedRecords.get(tp0));
        records = fetchedRecords.get(tp1);
        assertEquals(2, records.size());
        assertEquals(6L, subscriptions.position(tp1).longValue());
        assertEquals(4, records.get(0).offset());
        assertEquals(5, records.get(1).offset());
    }

    @Test
    public void testFetchNonContinuousRecords() {
        // if we are fetching from a compacted topic, there may be gaps in the returned records
        // this test verifies the fetcher updates the current fetched/consumed positions correctly for this case

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        builder.appendWithOffset(15L, 0L, "key".getBytes(), "value-1".getBytes());
        builder.appendWithOffset(20L, 0L, "key".getBytes(), "value-2".getBytes());
        builder.appendWithOffset(30L, 0L, "key".getBytes(), "value-3".getBytes());
        MemoryRecords records = builder.build();

        List<ConsumerRecord<byte[], byte[]>> consumerRecords;
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        consumerRecords = fetcher.fetchedRecords().get(tp0);
        assertEquals(3, consumerRecords.size());
        assertEquals(31L, subscriptions.position(tp0).longValue()); // this is the next fetching position

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
            client.setNodeApiVersions(NodeApiVersions.create(Collections.singletonList(
                new ApiVersionsResponse.ApiVersion(ApiKeys.FETCH.id, (short) 2, (short) 2))));
            makeFetchRequestWithIncompleteRecord();
            try {
                fetcher.fetchedRecords();
                fail("RecordTooLargeException should have been raised");
            } catch (RecordTooLargeException e) {
                assertTrue(e.getMessage().startsWith("There are some messages at [Partition=Offset]: "));
                // the position should not advance since no data has been returned
                assertEquals(0, subscriptions.position(tp0).longValue());
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
        makeFetchRequestWithIncompleteRecord();
        try {
            fetcher.fetchedRecords();
            fail("RecordTooLargeException should have been raised");
        } catch (KafkaException e) {
            assertTrue(e.getMessage().startsWith("Failed to make progress reading messages"));
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp0).longValue());
        }
    }

    private void makeFetchRequestWithIncompleteRecord() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        MemoryRecords partialRecord = MemoryRecords.readableRecords(
            ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}));
        client.prepareResponse(fullFetchResponse(tp0, partialRecord, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());
    }

    @Test
    public void testUnauthorizedTopic() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // resize the limit of the buffer to pretend it is only fetch-size large
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.TOPIC_AUTHORIZATION_FAILED, 100L, 0));
        consumerClient.poll(0);
        try {
            fetcher.fetchedRecords();
            fail("fetchedRecords should have thrown");
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(topicName), e.unauthorizedTopics());
        }
    }

    @Test
    public void testFetchDuringRebalance() {
        subscriptions.subscribe(singleton(topicName), listener);
        subscriptions.assignFromSubscribed(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());

        // Now the rebalance happens and fetch positions are cleared
        subscriptions.assignFromSubscribed(singleton(tp0));
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);

        // The active fetch should be ignored since its position is no longer valid
        assertTrue(fetcher.fetchedRecords().isEmpty());
    }

    @Test
    public void testInFlightFetchOnPausedPartition() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        subscriptions.pause(tp0);

        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertNull(fetcher.fetchedRecords().get(tp0));
    }

    @Test
    public void testFetchOnPausedPartition() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        subscriptions.pause(tp0);
        assertFalse(fetcher.sendFetches() > 0);
        assertTrue(client.requests().isEmpty());
    }

    @Test
    public void testFetchNotLeaderForPartition() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NOT_LEADER_FOR_PARTITION, 100L, 0));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testFetchUnknownTopicOrPartition() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.UNKNOWN_TOPIC_OR_PARTITION, 100L, 0));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testFetchOffsetOutOfRange() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(null, subscriptions.position(tp0));
    }

    @Test
    public void testStaleOutOfRangeError() {
        // verify that an out of range error which arrives after a seek
        // does not cause us to reset our position or throw an exception
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        subscriptions.seek(tp0, 1);
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(1, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testFetchedRecordsAfterSeek() {
        subscriptionsNoAutoReset.assignFromUser(singleton(tp0));
        subscriptionsNoAutoReset.seek(tp0, 0);

        assertTrue(fetcherNoAutoReset.sendFetches() > 0);
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(0);
        assertFalse(subscriptionsNoAutoReset.isOffsetResetNeeded(tp0));
        subscriptionsNoAutoReset.seek(tp0, 2);
        assertEquals(0, fetcherNoAutoReset.fetchedRecords().size());
    }

    @Test
    public void testFetchOffsetOutOfRangeException() {
        subscriptionsNoAutoReset.assignFromUser(singleton(tp0));
        subscriptionsNoAutoReset.seek(tp0, 0);

        fetcherNoAutoReset.sendFetches();
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(0);

        assertFalse(subscriptionsNoAutoReset.isOffsetResetNeeded(tp0));
        for (int i = 0; i < 2; i++) {
            try {
                fetcherNoAutoReset.fetchedRecords();
                fail("Should have thrown OffsetOutOfRangeException");
            } catch (OffsetOutOfRangeException e) {
                assertTrue(e.offsetOutOfRangePartitions().containsKey(tp0));
                assertEquals(e.offsetOutOfRangePartitions().size(), 1);
            }
        }
    }

    @Test
    public void testFetchPositionAfterException() {
        // verify the advancement in the next fetch offset equals to the number of fetched records when
        // some fetched partitions cause Exception. This ensures that consumer won't lose record upon exception
        subscriptionsNoAutoReset.assignFromUser(Utils.mkSet(tp0, tp1));
        subscriptionsNoAutoReset.seek(tp0, 1);
        subscriptionsNoAutoReset.seek(tp1, 1);

        assertEquals(1, fetcherNoAutoReset.sendFetches());

        Map<TopicPartition, FetchResponse.PartitionData> partitions = new LinkedHashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData(Errors.NONE, 100,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, records));
        partitions.put(tp0, new FetchResponse.PartitionData(Errors.OFFSET_OUT_OF_RANGE, 100,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY));
        client.prepareResponse(new FetchResponse(Errors.NONE, new LinkedHashMap<>(partitions),
            0, INVALID_SESSION_ID));
        consumerClient.poll(0);

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = new ArrayList<>();
        List<OffsetOutOfRangeException> exceptions = new ArrayList<>();

        for (List<ConsumerRecord<byte[], byte[]>> records: fetcherNoAutoReset.fetchedRecords().values())
            fetchedRecords.addAll(records);

        assertEquals(fetchedRecords.size(), subscriptionsNoAutoReset.position(tp1) - 1);

        try {
            for (List<ConsumerRecord<byte[], byte[]>> records: fetcherNoAutoReset.fetchedRecords().values())
                fetchedRecords.addAll(records);
        } catch (OffsetOutOfRangeException e) {
            exceptions.add(e);
        }

        assertEquals(4, subscriptionsNoAutoReset.position(tp1).longValue());
        assertEquals(3, fetchedRecords.size());

        // Should have received one OffsetOutOfRangeException for partition tp1
        assertEquals(1, exceptions.size());
        OffsetOutOfRangeException e = exceptions.get(0);
        assertTrue(e.offsetOutOfRangePartitions().containsKey(tp0));
        assertEquals(e.offsetOutOfRangePartitions().size(), 1);
    }

    @Test
    public void testCompletedFetchRemoval() {
        // Ensure the removal of completed fetches that cause an Exception if and only if they contain empty records.
        subscriptionsNoAutoReset.assignFromUser(Utils.mkSet(tp0, tp1, tp2, tp3));
        subscriptionsNoAutoReset.seek(tp0, 1);
        subscriptionsNoAutoReset.seek(tp1, 1);
        subscriptionsNoAutoReset.seek(tp2, 1);
        subscriptionsNoAutoReset.seek(tp3, 1);

        assertEquals(1, fetcherNoAutoReset.sendFetches());

        Map<TopicPartition, FetchResponse.PartitionData> partitions = new LinkedHashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData(Errors.NONE, 100, FetchResponse.INVALID_LAST_STABLE_OFFSET,
                FetchResponse.INVALID_LOG_START_OFFSET, null, records));
        partitions.put(tp0, new FetchResponse.PartitionData(Errors.OFFSET_OUT_OF_RANGE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY));
        partitions.put(tp2, new FetchResponse.PartitionData(Errors.NONE, 100L, 4,
                0L, null, nextRecords));
        partitions.put(tp3, new FetchResponse.PartitionData(Errors.NONE, 100L, 4,
                0L, null, partialRecords));
        client.prepareResponse(new FetchResponse(Errors.NONE, new LinkedHashMap<>(partitions),
                0, INVALID_SESSION_ID));
        consumerClient.poll(0);

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = new ArrayList<>();
        for (List<ConsumerRecord<byte[], byte[]>> records: fetcherNoAutoReset.fetchedRecords().values())
            fetchedRecords.addAll(records);

        assertEquals(fetchedRecords.size(), subscriptionsNoAutoReset.position(tp1) - 1);
        assertEquals(4, subscriptionsNoAutoReset.position(tp1).longValue());
        assertEquals(3, fetchedRecords.size());

        List<OffsetOutOfRangeException> oorExceptions = new ArrayList<>();
        try {
            for (List<ConsumerRecord<byte[], byte[]>> records: fetcherNoAutoReset.fetchedRecords().values())
                fetchedRecords.addAll(records);
        } catch (OffsetOutOfRangeException oor) {
            oorExceptions.add(oor);
        }

        // Should have received one OffsetOutOfRangeException for partition tp1
        assertEquals(1, oorExceptions.size());
        OffsetOutOfRangeException oor = oorExceptions.get(0);
        assertTrue(oor.offsetOutOfRangePartitions().containsKey(tp0));
        assertEquals(oor.offsetOutOfRangePartitions().size(), 1);

        for (List<ConsumerRecord<byte[], byte[]>> records: fetcherNoAutoReset.fetchedRecords().values())
            fetchedRecords.addAll(records);

        // Should not have received an Exception for tp2.
        assertEquals(6, subscriptionsNoAutoReset.position(tp2).longValue());
        assertEquals(5, fetchedRecords.size());

        int numExceptionsExpected = 3;
        List<KafkaException> kafkaExceptions = new ArrayList<>();
        for (int i = 1; i <= numExceptionsExpected; i++) {
            try {
                for (List<ConsumerRecord<byte[], byte[]>> records: fetcherNoAutoReset.fetchedRecords().values())
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
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptionsNoAutoReset, new Metrics(time), 2);

        subscriptionsNoAutoReset.assignFromUser(Utils.mkSet(tp0));
        subscriptionsNoAutoReset.seek(tp0, 1);
        assertEquals(1, fetcher.sendFetches());
        Map<TopicPartition, FetchResponse.PartitionData> partitions = new HashMap<>();
        partitions.put(tp0, new FetchResponse.PartitionData(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, records));
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);

        assertEquals(2, fetcher.fetchedRecords().get(tp0).size());

        subscriptionsNoAutoReset.assignFromUser(Utils.mkSet(tp0, tp1));
        subscriptionsNoAutoReset.seek(tp1, 1);
        assertEquals(1, fetcher.sendFetches());
        partitions = new HashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData(Errors.OFFSET_OUT_OF_RANGE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY));
        client.prepareResponse(new FetchResponse(Errors.NONE, new LinkedHashMap<>(partitions), 0, INVALID_SESSION_ID));
        consumerClient.poll(0);
        assertEquals(1, fetcher.fetchedRecords().get(tp0).size());

        subscriptionsNoAutoReset.seek(tp1, 10);
        // Should not throw OffsetOutOfRangeException after the seek
        assertEquals(0, fetcher.fetchedRecords().size());
    }

    @Test
    public void testFetchDisconnected() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0), true);
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());

        // disconnects should have no affect on subscription state
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(0, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testUpdateFetchPositionNoOpWithPositionSet() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 5L);

        fetcher.resetOffsetsIfNeeded();
        assertFalse(client.hasInFlightRequests());
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testUpdateFetchPositionResetToDefaultOffset() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.EARLIEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testUpdateFetchPositionResetToLatestOffset() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testListOffsetsSendsIsolationLevel() {
        for (final IsolationLevel isolationLevel : IsolationLevel.values()) {
            Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(), new ByteArrayDeserializer(),
                    new ByteArrayDeserializer(), Integer.MAX_VALUE, isolationLevel);

            subscriptions.assignFromUser(singleton(tp0));
            subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

            client.prepareResponse(new MockClient.RequestMatcher() {
                @Override
                public boolean matches(AbstractRequest body) {
                    ListOffsetRequest request = (ListOffsetRequest) body;
                    return request.isolationLevel() == isolationLevel;
                }
            }, listOffsetResponse(Errors.NONE, 1L, 5L));
            fetcher.resetOffsetsIfNeeded();
            consumerClient.pollNoWakeup();

            assertFalse(subscriptions.isOffsetResetNeeded(tp0));
            assertTrue(subscriptions.isFetchable(tp0));
            assertEquals(5, subscriptions.position(tp0).longValue());
        }
    }

    @Test
    public void testResetOffsetsSkipsBlackedOutConnections() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);

        // Check that we skip sending the ListOffset request when the node is blacked out
        client.blackout(node, 500);
        fetcher.resetOffsetsIfNeeded();
        assertEquals(0, consumerClient.pendingRequestCount());
        consumerClient.pollNoWakeup();
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(tp0));

        time.sleep(500);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.EARLIEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testUpdateFetchPositionResetToEarliestOffset() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.EARLIEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testResetOffsetsMetadataRefresh() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // First fetch fails with stale metadata
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NOT_LEADER_FOR_PARTITION, 1L, 5L), false);
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Expect a metadata refresh
        client.prepareMetadataUpdate(cluster, Collections.<String>emptySet());
        consumerClient.pollNoWakeup();
        assertFalse(client.hasPendingMetadataUpdates());

        // Next fetch succeeds
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testUpdateFetchPositionDisconnect() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // First request gets a disconnect
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L), true);
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Expect a metadata refresh
        client.prepareMetadataUpdate(cluster, Collections.<String>emptySet());
        consumerClient.pollNoWakeup();
        assertFalse(client.hasPendingMetadataUpdates());

        // No retry until the backoff passes
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Next one succeeds
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testAssignmentChangeWithInFlightReset() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Send the ListOffsets request to reset the position
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(client.hasInFlightRequests());

        // Now we have an assignment change
        subscriptions.assignFromUser(singleton(tp1));

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();

        assertFalse(client.hasPendingResponses());
        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.isAssigned(tp0));
    }

    @Test
    public void testSeekWithInFlightReset() {
        subscriptions.assignFromUser(singleton(tp0));
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
        assertEquals(237L, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testChangeResetWithInFlightReset() {
        subscriptions.assignFromUser(singleton(tp0));
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
        subscriptions.assignFromUser(singleton(tp0));
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
        assertEquals(5L, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testRestOffsetsAuthorizationFailure() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // First request gets a disconnect
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.TOPIC_AUTHORIZATION_FAILED, -1, -1), false);
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
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsRequiringOffsetReset() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.pause(tp0); // paused partition does not have a valid position
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 10L));
        fetcher.resetOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp0));
        assertEquals(10, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsWithoutAValidPosition() {
        subscriptions.assignFromUser(singleton(tp0));
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
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 10);
        subscriptions.pause(tp0); // paused partition already has a valid position

        fetcher.resetOffsetsIfNeeded();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp0));
        assertEquals(10, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testGetAllTopics() {
        // sending response before request, as getTopicMetadata is a blocking call
        client.prepareResponse(newMetadataResponse(topicName, Errors.NONE));

        Map<String, List<PartitionInfo>> allTopics = fetcher.getAllTopicMetadata(5000L);

        assertEquals(cluster.topics().size(), allTopics.size());
    }

    @Test
    public void testGetAllTopicsDisconnect() {
        // first try gets a disconnect, next succeeds
        client.prepareResponse(null, true);
        client.prepareResponse(newMetadataResponse(topicName, Errors.NONE));
        Map<String, List<PartitionInfo>> allTopics = fetcher.getAllTopicMetadata(5000L);
        assertEquals(cluster.topics().size(), allTopics.size());
    }

    @Test(expected = TimeoutException.class)
    public void testGetAllTopicsTimeout() {
        // since no response is prepared, the request should timeout
        fetcher.getAllTopicMetadata(50L);
    }

    @Test
    public void testGetAllTopicsUnauthorized() {
        client.prepareResponse(newMetadataResponse(topicName, Errors.TOPIC_AUTHORIZATION_FAILED));
        try {
            fetcher.getAllTopicMetadata(10L);
            fail();
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(topicName), e.unauthorizedTopics());
        }
    }

    @Test(expected = InvalidTopicException.class)
    public void testGetTopicMetadataInvalidTopic() {
        client.prepareResponse(newMetadataResponse(topicName, Errors.INVALID_TOPIC_EXCEPTION));
        fetcher.getTopicMetadata(
                new MetadataRequest.Builder(Collections.singletonList(topicName), true), 5000L);
    }

    @Test
    public void testGetTopicMetadataUnknownTopic() {
        client.prepareResponse(newMetadataResponse(topicName, Errors.UNKNOWN_TOPIC_OR_PARTITION));

        Map<String, List<PartitionInfo>> topicMetadata = fetcher.getTopicMetadata(
                new MetadataRequest.Builder(Collections.singletonList(topicName), true), 5000L);
        assertNull(topicMetadata.get(topicName));
    }

    @Test
    public void testGetTopicMetadataLeaderNotAvailable() {
        client.prepareResponse(newMetadataResponse(topicName, Errors.LEADER_NOT_AVAILABLE));
        client.prepareResponse(newMetadataResponse(topicName, Errors.NONE));

        Map<String, List<PartitionInfo>> topicMetadata = fetcher.getTopicMetadata(
                new MetadataRequest.Builder(Collections.singletonList(topicName), true), 5000L);
        assertTrue(topicMetadata.containsKey(topicName));
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testQuotaMetrics() throws Exception {
        MockSelector selector = new MockSelector(time);
        Sensor throttleTimeSensor = Fetcher.throttleTimeSensor(metrics, metricsRegistry);
        Cluster cluster = TestUtils.singletonCluster("test", 1);
        Node node = cluster.nodes().get(0);
        NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                1000, 1000, 64 * 1024, 64 * 1024, 1000,
                time, true, new ApiVersions(), throttleTimeSensor, new LogContext());

        short apiVersionsResponseVersion = ApiKeys.API_VERSIONS.latestVersion();
        ByteBuffer buffer = ApiVersionsResponse.createApiVersionsResponse(400, RecordBatch.CURRENT_MAGIC_VALUE).serialize(apiVersionsResponseVersion, new ResponseHeader(0));
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        while (!client.ready(node, time.milliseconds())) {
            client.poll(1, time.milliseconds());
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()));
        }
        selector.clear();

        for (int i = 1; i <= 3; i++) {
            int throttleTimeMs = 100 * i;
            FetchRequest.Builder builder = FetchRequest.Builder.forConsumer(100, 100, new LinkedHashMap<TopicPartition, PartitionData>());
            ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true, null);
            client.send(request, time.milliseconds());
            client.poll(1, time.milliseconds());
            FetchResponse response = fullFetchResponse(tp0, nextRecords, Errors.NONE, i, throttleTimeMs);
            buffer = response.serialize(ApiKeys.FETCH.latestVersion(), new ResponseHeader(request.correlationId()));
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
        assertEquals(250, avgMetric.value(), EPSILON);
        assertEquals(400, maxMetric.value(), EPSILON);
        client.close();
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testFetcherMetrics() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        MetricName maxLagMetric = metrics.metricInstance(metricsRegistry.recordsLagMax);
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", tp0.topic());
        tags.put("partition", String.valueOf(tp0.partition()));
        MetricName partitionLagMetric = metrics.metricName("records-lag", metricGroup, tags);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLagMax = allMetrics.get(maxLagMetric);

        // recordsFetchLagMax should be initialized to negative infinity
        assertEquals(Double.NEGATIVE_INFINITY, recordsFetchLagMax.value(), EPSILON);

        // recordsFetchLagMax should be hw - fetchOffset after receiving an empty FetchResponse
        fetchRecords(tp0, MemoryRecords.EMPTY, Errors.NONE, 100L, 0);
        assertEquals(100, recordsFetchLagMax.value(), EPSILON);

        KafkaMetric partitionLag = allMetrics.get(partitionLagMetric);
        assertEquals(100, partitionLag.value(), EPSILON);

        // recordsFetchLagMax should be hw - offset of the last message after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        fetchRecords(tp0, builder.build(), Errors.NONE, 200L, 0);
        assertEquals(197, recordsFetchLagMax.value(), EPSILON);
        assertEquals(197, partitionLag.value(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        assertFalse(allMetrics.containsKey(partitionLagMetric));
    }

    @Test
    public void testFetcherLeadMetric() {
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        MetricName minLeadMetric = metrics.metricInstance(metricsRegistry.recordsLeadMin);
        Map<String, String> tags = new HashMap<>(2);
        tags.put("topic", tp0.topic());
        tags.put("partition", String.valueOf(tp0.partition()));
        MetricName partitionLeadMetric = metrics.metricName("records-lead", metricGroup, "", tags);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLeadMin = allMetrics.get(minLeadMetric);

        // recordsFetchLeadMin should be initialized to MAX_VALUE
        assertEquals(Double.MAX_VALUE, recordsFetchLeadMin.value(), EPSILON);

        // recordsFetchLeadMin should be position - logStartOffset after receiving an empty FetchResponse
        fetchRecords(tp0, MemoryRecords.EMPTY, Errors.NONE, 100L, -1L, 0L, 0);
        assertEquals(0L, recordsFetchLeadMin.value(), EPSILON);

        KafkaMetric partitionLead = allMetrics.get(partitionLeadMetric);
        assertEquals(0L, partitionLead.value(), EPSILON);

        // recordsFetchLeadMin should be position - logStartOffset after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++) {
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        }
        fetchRecords(tp0, builder.build(), Errors.NONE, 200L, -1L, 0L, 0);
        assertEquals(0L, recordsFetchLeadMin.value(), EPSILON);
        assertEquals(3L, partitionLead.value(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        assertFalse(allMetrics.containsKey(partitionLeadMetric));
    }

    @Test
    public void testReadCommittedLagMetric() {
        Metrics metrics = new Metrics();
        fetcher = createFetcher(subscriptions, metrics, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);

        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        MetricName maxLagMetric = metrics.metricInstance(metricsRegistry.recordsLagMax);

        Map<String, String> tags = new HashMap<>();
        tags.put("topic", tp0.topic());
        tags.put("partition", String.valueOf(tp0.partition()));
        MetricName partitionLagMetric = metrics.metricName("records-lag", metricGroup, tags);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLagMax = allMetrics.get(maxLagMetric);

        // recordsFetchLagMax should be initialized to negative infinity
        assertEquals(Double.NEGATIVE_INFINITY, recordsFetchLagMax.value(), EPSILON);

        // recordsFetchLagMax should be lso - fetchOffset after receiving an empty FetchResponse
        fetchRecords(tp0, MemoryRecords.EMPTY, Errors.NONE, 100L, 50L, 0);
        assertEquals(50, recordsFetchLagMax.value(), EPSILON);

        KafkaMetric partitionLag = allMetrics.get(partitionLagMetric);
        assertEquals(50, partitionLag.value(), EPSILON);

        // recordsFetchLagMax should be lso - offset of the last message after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        fetchRecords(tp0, builder.build(), Errors.NONE, 200L, 150L, 0);
        assertEquals(147, recordsFetchLagMax.value(), EPSILON);
        assertEquals(147, partitionLag.value(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        assertFalse(allMetrics.containsKey(partitionLagMetric));
    }

    @Test
    public void testFetchResponseMetrics() {
        String topic1 = "foo";
        String topic2 = "bar";
        TopicPartition tp1 = new TopicPartition(topic1, 0);
        TopicPartition tp2 = new TopicPartition(topic2, 0);

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(topic1, 1);
        partitionCounts.put(topic2, 1);
        Cluster cluster = TestUtils.clusterWith(1, partitionCounts);
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());

        subscriptions.assignFromUser(Utils.mkSet(tp1, tp2));

        int expectedBytes = 0;
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> fetchPartitionData = new LinkedHashMap<>();

        for (TopicPartition tp : Utils.mkSet(tp1, tp2)) {
            subscriptions.seek(tp, 0);

            MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                    TimestampType.CREATE_TIME, 0L);
            for (int v = 0; v < 3; v++)
                builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
            MemoryRecords records = builder.build();
            for (Record record : records.records())
                expectedBytes += record.sizeInBytes();

            fetchPartitionData.put(tp, new FetchResponse.PartitionData(Errors.NONE, 15L,
                    FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, records));
        }

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(new FetchResponse(Errors.NONE, fetchPartitionData, 0, INVALID_SESSION_ID));
        consumerClient.poll(0);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetcher.fetchedRecords();
        assertEquals(3, fetchedRecords.get(tp1).size());
        assertEquals(3, fetchedRecords.get(tp2).size());

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchSizeAvg));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg));
        assertEquals(expectedBytes, fetchSizeAverage.value(), EPSILON);
        assertEquals(6, recordsCountAverage.value(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsPartialResponse() {
        subscriptions.assignFromUser(singleton(tp0));
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
        assertEquals(expectedBytes, fetchSizeAverage.value(), EPSILON);
        assertEquals(2, recordsCountAverage.value(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsWithOnePartitionError() {
        subscriptions.assignFromUser(Utils.mkSet(tp0, tp1));
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

        Map<TopicPartition, FetchResponse.PartitionData> partitions = new HashMap<>();
        partitions.put(tp0, new FetchResponse.PartitionData(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, records));
        partitions.put(tp1, new FetchResponse.PartitionData(Errors.OFFSET_OUT_OF_RANGE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, MemoryRecords.EMPTY));

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(new FetchResponse(Errors.NONE, new LinkedHashMap<>(partitions),
                0, INVALID_SESSION_ID));
        consumerClient.poll(0);
        fetcher.fetchedRecords();

        int expectedBytes = 0;
        for (Record record : records.records())
            expectedBytes += record.sizeInBytes();

        assertEquals(expectedBytes, fetchSizeAverage.value(), EPSILON);
        assertEquals(3, recordsCountAverage.value(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsWithOnePartitionAtTheWrongOffset() {
        subscriptions.assignFromUser(Utils.mkSet(tp0, tp1));
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

        Map<TopicPartition, FetchResponse.PartitionData> partitions = new HashMap<>();
        partitions.put(tp0, new FetchResponse.PartitionData(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, records));
        partitions.put(tp1, new FetchResponse.PartitionData(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null,
                MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("val".getBytes()))));

        client.prepareResponse(new FetchResponse(Errors.NONE, new LinkedHashMap<>(partitions),
                0, INVALID_SESSION_ID));
        consumerClient.poll(0);
        fetcher.fetchedRecords();

        // we should have ignored the record at the wrong offset
        int expectedBytes = 0;
        for (Record record : records.records())
            expectedBytes += record.sizeInBytes();

        assertEquals(expectedBytes, fetchSizeAverage.value(), EPSILON);
        assertEquals(3, recordsCountAverage.value(), EPSILON);
    }

    @Test
    public void testFetcherMetricsTemplates() throws Exception {
        metrics.close();
        Map<String, String> clientTags = Collections.singletonMap("client-id", "clientA");
        metrics = new Metrics(new MetricConfig().tags(clientTags));
        metricsRegistry = new FetcherMetricsRegistry(clientTags.keySet(), "consumer" + groupId);
        fetcher.close();
        fetcher = createFetcher(subscriptions, metrics);

        // Fetch from topic to generate topic metrics
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetcher.fetchedRecords();
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
        consumerClient.poll(0);
        return fetcher.fetchedRecords();
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchRecords(
            TopicPartition tp, MemoryRecords records, Errors error, long hw, long lastStableOffset, long logStartOffset, int throttleTime) {
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(tp, records, error, hw, lastStableOffset, logStartOffset, throttleTime));
        consumerClient.poll(0);
        return fetcher.fetchedRecords();
    }

    @Test
    public void testGetOffsetsForTimesTimeout() {
        try {
            fetcher.offsetsByTimes(Collections.singletonMap(new TopicPartition(topicName, 2), 1000L), 100L);
            fail("Should throw timeout exception.");
        } catch (TimeoutException e) {
            // let it go.
        }
    }

    @Test
    public void testGetOffsetsForTimes() {
        // Empty map
        assertTrue(fetcher.offsetsByTimes(new HashMap<TopicPartition, Long>(), 100L).isEmpty());
        // Unknown Offset
        testGetOffsetsForTimesWithUnknownOffset();
        // Error code none with unknown offset
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NONE, -1L, 100L, null, 100L);
        // Error code none with known offset
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NONE, 10L, 100L, 10L, 100L);
        // Test both of partition has error.
        testGetOffsetsForTimesWithError(Errors.NOT_LEADER_FOR_PARTITION, Errors.INVALID_REQUEST, 10L, 100L, 10L, 100L);
        // Test the second partition has error.
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NOT_LEADER_FOR_PARTITION, 10L, 100L, 10L, 100L);
        // Test different errors.
        testGetOffsetsForTimesWithError(Errors.NOT_LEADER_FOR_PARTITION, Errors.NONE, 10L, 100L, 10L, 100L);
        testGetOffsetsForTimesWithError(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE, 10L, 100L, 10L, 100L);
        testGetOffsetsForTimesWithError(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, Errors.NONE, 10L, 100L, null, 100L);
        testGetOffsetsForTimesWithError(Errors.BROKER_NOT_AVAILABLE, Errors.NONE, 10L, 100L, 10L, 100L);
    }

    @Test(expected = TimeoutException.class)
    public void testBatchedListOffsetsMetadataErrors() {
        Map<TopicPartition, ListOffsetResponse.PartitionData> partitionData = new HashMap<>();
        partitionData.put(tp0, new ListOffsetResponse.PartitionData(Errors.NOT_LEADER_FOR_PARTITION,
                ListOffsetResponse.UNKNOWN_TIMESTAMP, ListOffsetResponse.UNKNOWN_OFFSET));
        partitionData.put(tp1, new ListOffsetResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION,
                ListOffsetResponse.UNKNOWN_TIMESTAMP, ListOffsetResponse.UNKNOWN_OFFSET));
        client.prepareResponse(new ListOffsetResponse(0, partitionData));

        Map<TopicPartition, Long> offsetsToSearch = new HashMap<>();
        offsetsToSearch.put(tp0, ListOffsetRequest.EARLIEST_TIMESTAMP);
        offsetsToSearch.put(tp1, ListOffsetRequest.EARLIEST_TIMESTAMP);

        fetcher.offsetsByTimes(offsetsToSearch, 0);
    }

    @Test
    public void testSkippingAbortedTransactions() {
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(), new ByteArrayDeserializer(),
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
        subscriptions.assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetcher.fetchedRecords();
        assertFalse(fetchedRecords.containsKey(tp0));
    }

    @Test
    public void testReturnCommittedTransactions() {
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(), new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

        currentOffset += commitTransaction(buffer, 1L, currentOffset);
        buffer.flip();

        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        subscriptions.assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                FetchRequest request = (FetchRequest) body;
                assertEquals(IsolationLevel.READ_COMMITTED, request.isolationLevel());
                return true;
            }
        }, fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));

        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetcher.fetchedRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        assertEquals(fetchedRecords.get(tp0).size(), 2);
    }

    @Test
    public void testReadCommittedWithCommittedAndAbortedTransactions() {
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(), new ByteArrayDeserializer(),
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
        subscriptions.assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetcher.fetchedRecords();
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
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(), new ByteArrayDeserializer(),
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
        subscriptions.assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetcher.fetchedRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        assertEquals(fetchedRecords.get(tp0).size(), 2);
        List<ConsumerRecord<byte[], byte[]>> fetchedConsumerRecords = fetchedRecords.get(tp0);
        Set<String> committedKeys = new HashSet<>(Arrays.asList("commit1-1", "commit1-2"));
        Set<String> actuallyCommittedKeys = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> consumerRecord : fetchedConsumerRecords) {
            actuallyCommittedKeys.add(new String(consumerRecord.key(), StandardCharsets.UTF_8));
        }
        assertTrue(actuallyCommittedKeys.equals(committedKeys));
    }

    @Test
    public void testReadCommittedAbortMarkerWithNoData() {
        Fetcher<String, String> fetcher = createFetcher(subscriptions, new Metrics(), new StringDeserializer(),
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
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());

        // prepare the response. the aborted transactions begin at offsets which are no longer in the log
        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        abortedTransactions.add(new FetchResponse.AbortedTransaction(producerId, 0L));

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(MemoryRecords.readableRecords(buffer),
                abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> allFetchedRecords = fetcher.fetchedRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<String, String>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(3, fetchedRecords.size());
        assertEquals(Arrays.asList(6L, 7L, 8L), collectRecordOffsets(fetchedRecords));
    }

    @Test
    public void testUpdatePositionWithLastRecordMissingFromBatch() {
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
        result.output.flip();
        MemoryRecords compactedRecords = MemoryRecords.readableRecords(result.output);

        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, compactedRecords, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> allFetchedRecords = fetcher.fetchedRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(3, fetchedRecords.size());

        for (int i = 0; i < 3; i++) {
            assertEquals(Integer.toString(i), new String(fetchedRecords.get(i).key()));
        }

        // The next offset should point to the next batch
        assertEquals(4L, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testUpdatePositionOnEmptyBatch() {
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

        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fullFetchResponse(tp0, recordsWithEmptyBatch, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> allFetchedRecords = fetcher.fetchedRecords();
        assertTrue(allFetchedRecords.isEmpty());

        // The next offset should point to the next batch
        assertEquals(lastOffset + 1, subscriptions.position(tp0).longValue());
    }

    @Test
    public void testReadCommittedWithCompactedTopic() {
        Fetcher<String, String> fetcher = createFetcher(subscriptions, new Metrics(), new StringDeserializer(),
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
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, fetcher.sendFetches());

        // prepare the response. the aborted transactions begin at offsets which are no longer in the log
        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        abortedTransactions.add(new FetchResponse.AbortedTransaction(pid2, 6L));
        abortedTransactions.add(new FetchResponse.AbortedTransaction(pid1, 0L));

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(MemoryRecords.readableRecords(buffer),
                abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> allFetchedRecords = fetcher.fetchedRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<String, String>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(5, fetchedRecords.size());
        assertEquals(Arrays.asList(3L, 4L, 30L, 31L, 32L), collectRecordOffsets(fetchedRecords));
    }

    @Test
    public void testReturnAbortedTransactionsinUncommittedMode() {
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(), new ByteArrayDeserializer(),
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
        subscriptions.assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetcher.fetchedRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
    }

    @Test
    public void testConsumerPositionUpdatedWhenSkippingAbortedTransactions() {
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(), new ByteArrayDeserializer(),
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
        subscriptions.assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetcher.fetchedRecords();

        // Ensure that we don't return any of the aborted records, but yet advance the consumer position.
        assertFalse(fetchedRecords.containsKey(tp0));
        assertEquals(currentOffset, (long) subscriptions.position(tp0));
    }

    @Test
    public void testConsumingViaIncrementalFetchRequests() {
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(time), 2);

        List<ConsumerRecord<byte[], byte[]>> records;
        subscriptions.assignFromUser(new HashSet<>(Arrays.asList(tp0, tp1)));
        subscriptions.seek(tp0, 0);
        subscriptions.seek(tp1, 1);

        // Fetch some records and establish an incremental fetch session.
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> partitions1 = new LinkedHashMap<>();
        partitions1.put(tp0, new FetchResponse.PartitionData(Errors.NONE, 2L,
                2, 0L, null, this.records));
        partitions1.put(tp1, new FetchResponse.PartitionData(Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, emptyRecords));
        FetchResponse resp1 = new FetchResponse(Errors.NONE, partitions1, 0, 123);
        client.prepareResponse(resp1);
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetcher.fetchedRecords();
        assertFalse(fetchedRecords.containsKey(tp1));
        records = fetchedRecords.get(tp0);
        assertEquals(2, records.size());
        assertEquals(3L, subscriptions.position(tp0).longValue());
        assertEquals(1L, subscriptions.position(tp1).longValue());
        assertEquals(1, records.get(0).offset());
        assertEquals(2, records.get(1).offset());

        // There is still a buffered record.
        assertEquals(0, fetcher.sendFetches());
        fetchedRecords = fetcher.fetchedRecords();
        assertFalse(fetchedRecords.containsKey(tp1));
        records = fetchedRecords.get(tp0);
        assertEquals(1, records.size());
        assertEquals(3, records.get(0).offset());
        assertEquals(4L, subscriptions.position(tp0).longValue());

        // The second response contains no new records.
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> partitions2 = new LinkedHashMap<>();
        FetchResponse resp2 = new FetchResponse(Errors.NONE, partitions2, 0, 123);
        client.prepareResponse(resp2);
        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(0);
        fetchedRecords = fetcher.fetchedRecords();
        assertTrue(fetchedRecords.isEmpty());
        assertEquals(4L, subscriptions.position(tp0).longValue());
        assertEquals(1L, subscriptions.position(tp1).longValue());

        // The third response contains some new records for tp0.
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> partitions3 = new LinkedHashMap<>();
        partitions3.put(tp0, new FetchResponse.PartitionData(Errors.NONE, 100L,
                4, 0L, null, this.nextRecords));
        new FetchResponse(Errors.NONE, new LinkedHashMap<>(partitions1), 0, INVALID_SESSION_ID);
        FetchResponse resp3 = new FetchResponse(Errors.NONE, partitions3, 0, 123);
        client.prepareResponse(resp3);
        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(0);
        fetchedRecords = fetcher.fetchedRecords();
        assertFalse(fetchedRecords.containsKey(tp1));
        records = fetchedRecords.get(tp0);
        assertEquals(2, records.size());
        assertEquals(6L, subscriptions.position(tp0).longValue());
        assertEquals(1L, subscriptions.position(tp1).longValue());
        assertEquals(4, records.get(0).offset());
        assertEquals(5, records.get(1).offset());
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

    private int commitTransaction(ByteBuffer buffer, long producerId, long baseOffset) {
        short producerEpoch = 0;
        int partitionLeaderEpoch = 0;
        MemoryRecords.writeEndTransactionalMarker(buffer, baseOffset, time.milliseconds(), partitionLeaderEpoch, producerId, producerEpoch,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));
        return 1;
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
        metadata.update(Cluster.bootstrap(ClientUtils.parseAndValidateAddresses(Collections.singletonList("1.1.1.1:1111"))),
                        Collections.<String>emptySet(),
                        time.milliseconds());

        Map<String, Integer> partitionNumByTopic = new HashMap<>();
        partitionNumByTopic.put(topicName, 2);
        partitionNumByTopic.put(topicName2, 1);
        cluster = TestUtils.clusterWith(2, partitionNumByTopic);
        // The metadata refresh should contain all the topics.
        client.prepareMetadataUpdate(cluster, Collections.<String>emptySet(), true);

        // First try should fail due to metadata error.
        client.prepareResponseFrom(listOffsetResponse(t2p0, errorForP0, offsetForP0, offsetForP0), cluster.leaderFor(t2p0));
        client.prepareResponseFrom(listOffsetResponse(tp1, errorForP1, offsetForP1, offsetForP1), cluster.leaderFor(tp1));
        // Second try should succeed.
        client.prepareResponseFrom(listOffsetResponse(t2p0, Errors.NONE, offsetForP0, offsetForP0), cluster.leaderFor(t2p0));
        client.prepareResponseFrom(listOffsetResponse(tp1, Errors.NONE, offsetForP1, offsetForP1), cluster.leaderFor(tp1));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(t2p0, 0L);
        timestampToSearch.put(tp1, 0L);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = fetcher.offsetsByTimes(timestampToSearch, Long.MAX_VALUE);

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
        // Ensure metadata has both partition.
        Cluster cluster = TestUtils.clusterWith(1, topicName, 1);
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());

        Map<TopicPartition, ListOffsetResponse.PartitionData> partitionData = new HashMap<>();
        partitionData.put(tp0, new ListOffsetResponse.PartitionData(Errors.NONE,
                ListOffsetResponse.UNKNOWN_TIMESTAMP, ListOffsetResponse.UNKNOWN_OFFSET));

        client.prepareResponseFrom(new ListOffsetResponse(0, partitionData), cluster.leaderFor(tp0));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, 0L);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = fetcher.offsetsByTimes(timestampToSearch, Long.MAX_VALUE);

        assertTrue(offsetAndTimestampMap.containsKey(tp0));
        assertNull(offsetAndTimestampMap.get(tp0));
    }

    private MockClient.RequestMatcher listOffsetRequestMatcher(final long timestamp) {
        // matches any list offset request with the provided timestamp
        return new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ListOffsetRequest req = (ListOffsetRequest) body;
                return timestamp == req.partitionTimestamps().get(tp0);
            }
        };
    }

    private ListOffsetResponse listOffsetResponse(Errors error, long timestamp, long offset) {
        return listOffsetResponse(tp0, error, timestamp, offset);
    }

    private ListOffsetResponse listOffsetResponse(TopicPartition tp, Errors error, long timestamp, long offset) {
        ListOffsetResponse.PartitionData partitionData = new ListOffsetResponse.PartitionData(error, timestamp, offset);
        Map<TopicPartition, ListOffsetResponse.PartitionData> allPartitionData = new HashMap<>();
        allPartitionData.put(tp, partitionData);
        return new ListOffsetResponse(allPartitionData);
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

    private FetchResponse<MemoryRecords> fetchResponse(TopicPartition tp, MemoryRecords records, Errors error, long hw,
                                        long lastStableOffset, long logStartOffset, int throttleTime) {
        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = Collections.singletonMap(tp,
                new FetchResponse.PartitionData<>(error, hw, lastStableOffset, logStartOffset, null, records));
        return new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions), throttleTime, INVALID_SESSION_ID);
    }

    private MetadataResponse newMetadataResponse(String topic, Errors error) {
        List<MetadataResponse.PartitionMetadata> partitionsMetadata = new ArrayList<>();
        if (error == Errors.NONE) {
            for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
                partitionsMetadata.add(new MetadataResponse.PartitionMetadata(
                        Errors.NONE,
                        partitionInfo.partition(),
                        partitionInfo.leader(),
                        Arrays.asList(partitionInfo.replicas()),
                        Arrays.asList(partitionInfo.inSyncReplicas()),
                        Arrays.asList(partitionInfo.offlineReplicas())));
            }
        }

        MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(error, topic, false, partitionsMetadata);
        return new MetadataResponse(cluster.nodes(), null, MetadataResponse.NO_CONTROLLER_ID, Arrays.asList(topicMetadata));
    }

    private Fetcher<byte[], byte[]> createFetcher(SubscriptionState subscriptions,
                                                  Metrics metrics,
                                                  int maxPollRecords) {
        return createFetcher(subscriptions, metrics, new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                maxPollRecords, IsolationLevel.READ_UNCOMMITTED);
    }

    private Fetcher<byte[], byte[]> createFetcher(SubscriptionState subscriptions, Metrics metrics) {
        return createFetcher(subscriptions, metrics, Integer.MAX_VALUE);
    }

    private <K, V> Fetcher<K, V> createFetcher(SubscriptionState subscriptions,
                                               Metrics metrics,
                                               Deserializer<K> keyDeserializer,
                                               Deserializer<V> valueDeserializer) {
        return createFetcher(subscriptions, metrics, keyDeserializer, valueDeserializer, Integer.MAX_VALUE,
                IsolationLevel.READ_UNCOMMITTED);
    }

    private <K, V> Fetcher<K, V> createFetcher(SubscriptionState subscriptions,
                                               Metrics metrics,
                                               Deserializer<K> keyDeserializer,
                                               Deserializer<V> valueDeserializer,
                                               int maxPollRecords,
                                               IsolationLevel isolationLevel) {
        return new Fetcher<>(
                new LogContext(),
                consumerClient,
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                true, // check crc
                keyDeserializer,
                valueDeserializer,
                metadata,
                subscriptions,
                metrics,
                metricsRegistry,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                isolationLevel);
    }

    private <T> List<Long> collectRecordOffsets(List<ConsumerRecord<T, T>> records) {
        List<Long> res = new ArrayList<>(records.size());
        for (ConsumerRecord<?, ?> record : records)
            res.add(record.offset());
        return res;
    }
}
