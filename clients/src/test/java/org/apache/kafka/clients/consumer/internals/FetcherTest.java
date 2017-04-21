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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FetcherTest {
    private ConsumerRebalanceListener listener = new NoOpConsumerRebalanceListener();
    private String topicName = "test";
    private String groupId = "test-group";
    private final String metricGroup = "consumer" + groupId + "-fetch-manager-metrics";
    private TopicPartition tp1 = new TopicPartition(topicName, 0);
    private TopicPartition tp2 = new TopicPartition(topicName, 1);
    private int minBytes = 1;
    private int maxBytes = Integer.MAX_VALUE;
    private int maxWaitMs = 0;
    private int fetchSize = 1000;
    private long retryBackoffMs = 100;
    private MockTime time = new MockTime(1);
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private MockClient client = new MockClient(time, metadata);
    private Cluster cluster = TestUtils.singletonCluster(topicName, 2);
    private Node node = cluster.nodes().get(0);
    private Metrics metrics = new Metrics(time);
    private SubscriptionState subscriptions = new SubscriptionState(OffsetResetStrategy.EARLIEST);
    private SubscriptionState subscriptionsNoAutoReset = new SubscriptionState(OffsetResetStrategy.NONE);
    private static final double EPSILON = 0.0001;
    private ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(client, metadata, time, 100, 1000);

    private MemoryRecords records;
    private MemoryRecords nextRecords;
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
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fetchResponse(this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetcher.fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp1);
        assertEquals(3, records.size());
        assertEquals(4L, subscriptions.position(tp1).longValue()); // this is the next fetching position
        long offset = 1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertEquals(offset, record.offset());
            offset += 1;
        }
    }

    @Test
    public void testFetcherIgnoresControlRecords() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(0L, "key".getBytes(), null);
        builder.appendControlRecord(0L, ControlRecordType.COMMIT, null);
        builder.append(0L, "key".getBytes(), null);
        builder.close();

        builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 3L);
        builder.appendControlRecord(0L, ControlRecordType.ABORT, null);
        builder.close();

        buffer.flip();

        client.prepareResponse(fetchResponse(MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetcher.fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp1);
        assertEquals(2, records.size());
        assertEquals(4L, subscriptions.position(tp1).longValue());
        for (ConsumerRecord<byte[], byte[]> record : records)
            assertArrayEquals("key".getBytes(), record.key());
    }

    @Test
    public void testFetchError() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fetchResponse(this.records, Errors.NOT_LEADER_FOR_PARTITION, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetcher.fetchedRecords();
        assertFalse(partitionRecords.containsKey(tp1));
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
                if (i++ == 1)
                    throw new SerializationException();
                return data;
            }
        };

        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(time), deserializer, deserializer);

        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 1);

        client.prepareResponse(matchesOffset(tp1, 1), fetchResponse(this.records, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(0);
        try {
            fetcher.fetchedRecords();
            fail("fetchedRecords should have raised");
        } catch (SerializationException e) {
            // the position should not advance since no data has been returned
            assertEquals(1, subscriptions.position(tp1).longValue());
        }
    }

    @Test
    public void testParseInvalidRecord() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));

        byte magic = RecordBatch.MAGIC_VALUE_V1;
        byte[] key = "foo".getBytes();
        byte[] value = "baz".getBytes();
        long offset = 0;
        long timestamp = 500L;

        int size = LegacyRecord.recordSize(key, value);
        byte attributes = LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME);
        long crc = LegacyRecord.computeChecksum(magic, attributes, timestamp, key, value);

        // write one valid record
        out.writeLong(offset);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        // and one invalid record (note the crc)
        out.writeLong(offset);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc + 1, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        buffer.flip();

        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        try {
            fetcher.fetchedRecords();
            fail("fetchedRecords should have raised");
        } catch (KafkaException e) {
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp1).longValue());
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

        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        try {
            fetcher.fetchedRecords();
            fail("fetchedRecords should have raised");
        } catch (KafkaException e) {
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp1).longValue());
        }
    }

    @Test
    public void testFetchMaxPollRecords() {
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptions, new Metrics(time), 2);

        List<ConsumerRecord<byte[], byte[]>> records;
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 1);

        client.prepareResponse(matchesOffset(tp1, 1), fetchResponse(this.records, Errors.NONE, 100L, 0));
        client.prepareResponse(matchesOffset(tp1, 4), fetchResponse(this.nextRecords, Errors.NONE, 100L, 0));

        assertEquals(1, fetcher.sendFetches());
        consumerClient.poll(0);
        records = fetcher.fetchedRecords().get(tp1);
        assertEquals(2, records.size());
        assertEquals(3L, subscriptions.position(tp1).longValue());
        assertEquals(1, records.get(0).offset());
        assertEquals(2, records.get(1).offset());

        assertEquals(0, fetcher.sendFetches());
        consumerClient.poll(0);
        records = fetcher.fetchedRecords().get(tp1);
        assertEquals(1, records.size());
        assertEquals(4L, subscriptions.position(tp1).longValue());
        assertEquals(3, records.get(0).offset());

        assertTrue(fetcher.sendFetches() > 0);
        consumerClient.poll(0);
        records = fetcher.fetchedRecords().get(tp1);
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
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        // normal fetch
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        consumerRecords = fetcher.fetchedRecords().get(tp1);
        assertEquals(3, consumerRecords.size());
        assertEquals(31L, subscriptions.position(tp1).longValue()); // this is the next fetching position

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
                assertEquals(0, subscriptions.position(tp1).longValue());
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
            assertEquals(0, subscriptions.position(tp1).longValue());
        }
    }

    private void makeFetchRequestWithIncompleteRecord() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        MemoryRecords partialRecord = MemoryRecords.readableRecords(
            ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}));
        client.prepareResponse(fetchResponse(partialRecord, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertTrue(fetcher.hasCompletedFetches());
    }

    @Test
    public void testUnauthorizedTopic() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        // resize the limit of the buffer to pretend it is only fetch-size large
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(this.records, Errors.TOPIC_AUTHORIZATION_FAILED, 100L, 0));
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
        subscriptions.assignFromSubscribed(singleton(tp1));
        subscriptions.seek(tp1, 0);

        assertEquals(1, fetcher.sendFetches());

        // Now the rebalance happens and fetch positions are cleared
        subscriptions.assignFromSubscribed(singleton(tp1));
        client.prepareResponse(fetchResponse(this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);

        // The active fetch should be ignored since its position is no longer valid
        assertTrue(fetcher.fetchedRecords().isEmpty());
    }

    @Test
    public void testInFlightFetchOnPausedPartition() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        assertEquals(1, fetcher.sendFetches());
        subscriptions.pause(tp1);

        client.prepareResponse(fetchResponse(this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);
        assertNull(fetcher.fetchedRecords().get(tp1));
    }

    @Test
    public void testFetchOnPausedPartition() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        subscriptions.pause(tp1);
        assertFalse(fetcher.sendFetches() > 0);
        assertTrue(client.requests().isEmpty());
    }

    @Test
    public void testFetchNotLeaderForPartition() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(this.records, Errors.NOT_LEADER_FOR_PARTITION, 100L, 0));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testFetchUnknownTopicOrPartition() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(this.records, Errors.UNKNOWN_TOPIC_OR_PARTITION, 100L, 0));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testFetchOffsetOutOfRange() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertTrue(subscriptions.isOffsetResetNeeded(tp1));
        assertEquals(null, subscriptions.position(tp1));
    }

    @Test
    public void testStaleOutOfRangeError() {
        // verify that an out of range error which arrives after a seek
        // does not cause us to reset our position or throw an exception
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        subscriptions.seek(tp1, 1);
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertEquals(1, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testFetchedRecordsAfterSeek() {
        subscriptionsNoAutoReset.assignFromUser(singleton(tp1));
        subscriptionsNoAutoReset.seek(tp1, 0);

        assertTrue(fetcherNoAutoReset.sendFetches() > 0);
        client.prepareResponse(fetchResponse(this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(0);
        assertFalse(subscriptionsNoAutoReset.isOffsetResetNeeded(tp1));
        subscriptionsNoAutoReset.seek(tp1, 2);
        assertEquals(0, fetcherNoAutoReset.fetchedRecords().size());
    }

    @Test
    public void testFetchOffsetOutOfRangeException() {
        subscriptionsNoAutoReset.assignFromUser(singleton(tp1));
        subscriptionsNoAutoReset.seek(tp1, 0);

        fetcherNoAutoReset.sendFetches();
        client.prepareResponse(fetchResponse(this.records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(0);

        assertFalse(subscriptionsNoAutoReset.isOffsetResetNeeded(tp1));
        try {
            fetcherNoAutoReset.fetchedRecords();
            fail("Should have thrown OffsetOutOfRangeException");
        } catch (OffsetOutOfRangeException e) {
            assertTrue(e.offsetOutOfRangePartitions().containsKey(tp1));
            assertEquals(e.offsetOutOfRangePartitions().size(), 1);
        }
        assertEquals(0, fetcherNoAutoReset.fetchedRecords().size());
    }

    @Test
    public void testFetchPositionAfterException() {
        // verify the advancement in the next fetch offset equals the number of fetched records when
        // some fetched partitions cause Exception. This ensures that consumer won't lose record upon exception
        subscriptionsNoAutoReset.assignFromUser(Utils.mkSet(tp1, tp2));
        subscriptionsNoAutoReset.seek(tp1, 1);
        subscriptionsNoAutoReset.seek(tp2, 1);

        assertEquals(1, fetcherNoAutoReset.sendFetches());

        Map<TopicPartition, FetchResponse.PartitionData> partitions = new HashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData(Errors.OFFSET_OUT_OF_RANGE, 100,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY));
        partitions.put(tp2, new FetchResponse.PartitionData(Errors.NONE, 100,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, records));
        client.prepareResponse(new FetchResponse(new LinkedHashMap<>(partitions), 0));
        consumerClient.poll(0);

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = new ArrayList<>();
        List<OffsetOutOfRangeException> exceptions = new ArrayList<>();

        try {
            for (List<ConsumerRecord<byte[], byte[]>> records: fetcherNoAutoReset.fetchedRecords().values())
                fetchedRecords.addAll(records);
        } catch (OffsetOutOfRangeException e) {
            exceptions.add(e);
        }

        assertEquals(fetchedRecords.size(), subscriptionsNoAutoReset.position(tp2).longValue() - 1);

        try {
            for (List<ConsumerRecord<byte[], byte[]>> records: fetcherNoAutoReset.fetchedRecords().values())
                fetchedRecords.addAll(records);
        } catch (OffsetOutOfRangeException e) {
            exceptions.add(e);
        }

        assertEquals(4, subscriptionsNoAutoReset.position(tp2).longValue());
        assertEquals(3, fetchedRecords.size());

        // Should have received one OffsetOutOfRangeException for partition tp1
        assertEquals(1, exceptions.size());
        OffsetOutOfRangeException e = exceptions.get(0);
        assertTrue(e.offsetOutOfRangePartitions().containsKey(tp1));
        assertEquals(e.offsetOutOfRangePartitions().size(), 1);
    }

    @Test
    public void testSeekBeforeException() {
        Fetcher<byte[], byte[]> fetcher = createFetcher(subscriptionsNoAutoReset, new Metrics(time), 2);

        subscriptionsNoAutoReset.assignFromUser(Utils.mkSet(tp1));
        subscriptionsNoAutoReset.seek(tp1, 1);
        assertEquals(1, fetcher.sendFetches());
        Map<TopicPartition, FetchResponse.PartitionData> partitions = new HashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData(Errors.NONE, 100,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, records));
        client.prepareResponse(fetchResponse(this.records, Errors.NONE, 100L, 0));
        consumerClient.poll(0);

        assertEquals(2, fetcher.fetchedRecords().get(tp1).size());

        subscriptionsNoAutoReset.assignFromUser(Utils.mkSet(tp1, tp2));
        subscriptionsNoAutoReset.seek(tp2, 1);
        assertEquals(1, fetcher.sendFetches());
        partitions = new HashMap<>();
        partitions.put(tp2, new FetchResponse.PartitionData(Errors.OFFSET_OUT_OF_RANGE, 100,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY));
        client.prepareResponse(new FetchResponse(new LinkedHashMap<>(partitions), 0));
        consumerClient.poll(0);
        assertEquals(1, fetcher.fetchedRecords().get(tp1).size());

        subscriptionsNoAutoReset.seek(tp2, 10);
        // Should not throw OffsetOutOfRangeException after the seek
        assertEquals(0, fetcher.fetchedRecords().size());
    }

    @Test
    public void testFetchDisconnected() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(this.records, Errors.NONE, 100L, 0), true);
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());

        // disconnects should have no affect on subscription state
        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertTrue(subscriptions.isFetchable(tp1));
        assertEquals(0, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testUpdateFetchPositionToCommitted() {
        // unless a specific reset is expected, the default behavior is to reset to the committed
        // position if one is present
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.committed(tp1, new OffsetAndMetadata(5));

        fetcher.updateFetchPositions(singleton(tp1));
        assertTrue(subscriptions.isFetchable(tp1));
        assertEquals(5, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testUpdateFetchPositionResetToDefaultOffset() {
        subscriptions.assignFromUser(singleton(tp1));
        // with no commit position, we should reset using the default strategy defined above (EARLIEST)

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.EARLIEST_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.updateFetchPositions(singleton(tp1));
        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertTrue(subscriptions.isFetchable(tp1));
        assertEquals(5, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testUpdateFetchPositionResetToLatestOffset() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.needOffsetReset(tp1, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.updateFetchPositions(singleton(tp1));
        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertTrue(subscriptions.isFetchable(tp1));
        assertEquals(5, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testUpdateFetchPositionResetToEarliestOffset() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.needOffsetReset(tp1, OffsetResetStrategy.EARLIEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.EARLIEST_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.updateFetchPositions(singleton(tp1));
        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertTrue(subscriptions.isFetchable(tp1));
        assertEquals(5, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testUpdateFetchPositionDisconnect() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.needOffsetReset(tp1, OffsetResetStrategy.LATEST);

        // First request gets a disconnect
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, 1L, 5L), true);

        // Next one succeeds
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, 1L, 5L));
        fetcher.updateFetchPositions(singleton(tp1));
        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertTrue(subscriptions.isFetchable(tp1));
        assertEquals(5, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsRequiringOffsetReset() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.committed(tp1, new OffsetAndMetadata(0));
        subscriptions.pause(tp1); // paused partition does not have a valid position
        subscriptions.needOffsetReset(tp1, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.LATEST_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, 1L, 10L));
        fetcher.updateFetchPositions(singleton(tp1));

        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertFalse(subscriptions.isFetchable(tp1)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp1));
        assertEquals(10, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsWithoutACommittedOffset() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.pause(tp1); // paused partition does not have a valid position

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetRequest.EARLIEST_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, 1L, 0L));
        fetcher.updateFetchPositions(singleton(tp1));

        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertFalse(subscriptions.isFetchable(tp1)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp1));
        assertEquals(0, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsWithoutAValidPosition() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.committed(tp1, new OffsetAndMetadata(0));
        subscriptions.pause(tp1); // paused partition does not have a valid position
        subscriptions.seek(tp1, 10);

        fetcher.updateFetchPositions(singleton(tp1));

        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertFalse(subscriptions.isFetchable(tp1)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp1));
        assertEquals(10, subscriptions.position(tp1).longValue());
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsWithAValidPosition() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.committed(tp1, new OffsetAndMetadata(0));
        subscriptions.seek(tp1, 10);
        subscriptions.pause(tp1); // paused partition already has a valid position

        fetcher.updateFetchPositions(singleton(tp1));

        assertFalse(subscriptions.isOffsetResetNeeded(tp1));
        assertFalse(subscriptions.isFetchable(tp1)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp1));
        assertEquals(10, subscriptions.position(tp1).longValue());
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
            new MetadataRequest.Builder(Collections.singletonList(topicName)), 5000L);
    }

    @Test
    public void testGetTopicMetadataUnknownTopic() {
        client.prepareResponse(newMetadataResponse(topicName, Errors.UNKNOWN_TOPIC_OR_PARTITION));

        Map<String, List<PartitionInfo>> topicMetadata =
                fetcher.getTopicMetadata(
                        new MetadataRequest.Builder(Collections.singletonList(topicName)), 5000L);
        assertNull(topicMetadata.get(topicName));
    }

    @Test
    public void testGetTopicMetadataLeaderNotAvailable() {
        client.prepareResponse(newMetadataResponse(topicName, Errors.LEADER_NOT_AVAILABLE));
        client.prepareResponse(newMetadataResponse(topicName, Errors.NONE));

        Map<String, List<PartitionInfo>> topicMetadata =
                fetcher.getTopicMetadata(
                        new MetadataRequest.Builder(Collections.singletonList(topicName)), 5000L);
        assertTrue(topicMetadata.containsKey(topicName));
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testQuotaMetrics() throws Exception {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        // normal fetch
        for (int i = 1; i < 4; i++) {
            // We need to make sure the message offset grows. Otherwise they will be considered as already consumed
            // and filtered out by consumer.
            MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                    TimestampType.CREATE_TIME, 0L);
            for (int v = 0; v < 3; v++)
                builder.appendWithOffset(i * 3 + v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
            List<ConsumerRecord<byte[], byte[]>> records = fetchRecords(builder.build(), Errors.NONE, 100L, 100 * i).get(tp1);
            assertEquals(3, records.size());
        }

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric avgMetric = allMetrics.get(metrics.metricName("fetch-throttle-time-avg", metricGroup));
        KafkaMetric maxMetric = allMetrics.get(metrics.metricName("fetch-throttle-time-max", metricGroup));
        assertEquals(200, avgMetric.value(), EPSILON);
        assertEquals(300, maxMetric.value(), EPSILON);
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testFetcherMetrics() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        MetricName maxLagMetric = metrics.metricName("records-lag-max", metricGroup);
        MetricName partitionLagMetric = metrics.metricName(tp1 + ".records-lag", metricGroup);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLagMax = allMetrics.get(maxLagMetric);

        // recordsFetchLagMax should be initialized to negative infinity
        assertEquals(Double.NEGATIVE_INFINITY, recordsFetchLagMax.value(), EPSILON);

        // recordsFetchLagMax should be hw - fetchOffset after receiving an empty FetchResponse
        fetchRecords(MemoryRecords.EMPTY, Errors.NONE, 100L, 0);
        assertEquals(100, recordsFetchLagMax.value(), EPSILON);

        KafkaMetric partitionLag = allMetrics.get(partitionLagMetric);
        assertEquals(100, partitionLag.value(), EPSILON);

        // recordsFetchLagMax should be hw - offset of the last message after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        fetchRecords(builder.build(), Errors.NONE, 200L, 0);
        assertEquals(197, recordsFetchLagMax.value(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        assertFalse(allMetrics.containsKey(partitionLagMetric));
    }

    @Test
    public void testFetchResponseMetrics() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 0);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricName("fetch-size-avg", metricGroup));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricName("records-per-request-avg", metricGroup));

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        MemoryRecords records = builder.build();

        int expectedBytes = 0;
        for (Record record : records.records())
            expectedBytes += record.sizeInBytes();

        fetchRecords(records, Errors.NONE, 100L, 0);
        assertEquals(expectedBytes, fetchSizeAverage.value(), EPSILON);
        assertEquals(3, recordsCountAverage.value(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsPartialResponse() {
        subscriptions.assignFromUser(singleton(tp1));
        subscriptions.seek(tp1, 1);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricName("fetch-size-avg", metricGroup));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricName("records-per-request-avg", metricGroup));

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

        fetchRecords(records, Errors.NONE, 100L, 0);
        assertEquals(expectedBytes, fetchSizeAverage.value(), EPSILON);
        assertEquals(2, recordsCountAverage.value(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsWithOnePartitionError() {
        subscriptions.assignFromUser(Utils.mkSet(tp1, tp2));
        subscriptions.seek(tp1, 0);
        subscriptions.seek(tp2, 0);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricName("fetch-size-avg", metricGroup));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricName("records-per-request-avg", metricGroup));

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        MemoryRecords records = builder.build();

        Map<TopicPartition, FetchResponse.PartitionData> partitions = new HashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, records));
        partitions.put(tp2, new FetchResponse.PartitionData(Errors.OFFSET_OUT_OF_RANGE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, MemoryRecords.EMPTY));

        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(new FetchResponse(new LinkedHashMap<>(partitions), 0));
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
        subscriptions.assignFromUser(Utils.mkSet(tp1, tp2));
        subscriptions.seek(tp1, 0);
        subscriptions.seek(tp2, 0);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricName("fetch-size-avg", metricGroup));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricName("records-per-request-avg", metricGroup));

        // send the fetch and then seek to a new offset
        assertEquals(1, fetcher.sendFetches());
        subscriptions.seek(tp2, 5);

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        MemoryRecords records = builder.build();

        Map<TopicPartition, FetchResponse.PartitionData> partitions = new HashMap<>();
        partitions.put(tp1, new FetchResponse.PartitionData(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, records));
        partitions.put(tp2, new FetchResponse.PartitionData(Errors.NONE, 100,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null,
                MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("val".getBytes()))));

        client.prepareResponse(new FetchResponse(new LinkedHashMap<>(partitions), 0));
        consumerClient.poll(0);
        fetcher.fetchedRecords();

        // we should have ignored the record at the wrong offset
        int expectedBytes = 0;
        for (Record record : records.records())
            expectedBytes += record.sizeInBytes();

        assertEquals(expectedBytes, fetchSizeAverage.value(), EPSILON);
        assertEquals(3, recordsCountAverage.value(), EPSILON);
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchRecords(MemoryRecords records, Errors error, long hw, int throttleTime) {
        assertEquals(1, fetcher.sendFetches());
        client.prepareResponse(fetchResponse(records, error, hw, throttleTime));
        consumerClient.poll(0);
        return fetcher.fetchedRecords();
    }

    @Test
    public void testGetOffsetsForTimesTimeout() {
        try {
            fetcher.getOffsetsByTimes(Collections.singletonMap(new TopicPartition(topicName, 2), 1000L), 100L);
            fail("Should throw timeout exception.");
        } catch (TimeoutException e) {
            // let it go.
        }
    }

    @Test
    public void testGetOffsetsForTimes() {
        // Empty map
        assertTrue(fetcher.getOffsetsByTimes(new HashMap<TopicPartition, Long>(), 100L).isEmpty());
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

    private void testGetOffsetsForTimesWithError(Errors errorForTp0,
                                                 Errors errorForTp1,
                                                 long offsetForTp0,
                                                 long offsetForTp1,
                                                 Long expectedOffsetForTp0,
                                                 Long expectedOffsetForTp1) {
        client.reset();
        TopicPartition tp0 = tp1;
        TopicPartition tp1 = new TopicPartition(topicName, 1);
        // Ensure metadata has both partition.
        Cluster cluster = TestUtils.clusterWith(2, topicName, 2);
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());

        // First try should fail due to metadata error.
        client.prepareResponseFrom(listOffsetResponse(tp0, errorForTp0, offsetForTp0, offsetForTp0), cluster.leaderFor(tp0));
        client.prepareResponseFrom(listOffsetResponse(tp1, errorForTp1, offsetForTp1, offsetForTp1), cluster.leaderFor(tp1));
        // Second try should succeed.
        client.prepareResponseFrom(listOffsetResponse(tp0, Errors.NONE, offsetForTp0, offsetForTp0), cluster.leaderFor(tp0));
        client.prepareResponseFrom(listOffsetResponse(tp1, Errors.NONE, offsetForTp1, offsetForTp1), cluster.leaderFor(tp1));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, 0L);
        timestampToSearch.put(tp1, 0L);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = fetcher.getOffsetsByTimes(timestampToSearch, Long.MAX_VALUE);

        if (expectedOffsetForTp0 == null)
            assertNull(offsetAndTimestampMap.get(tp0));
        else {
            assertEquals(expectedOffsetForTp0.longValue(), offsetAndTimestampMap.get(tp0).timestamp());
            assertEquals(expectedOffsetForTp0.longValue(), offsetAndTimestampMap.get(tp0).offset());
        }

        if (expectedOffsetForTp1 == null)
            assertNull(offsetAndTimestampMap.get(tp1));
        else {
            assertEquals(expectedOffsetForTp1.longValue(), offsetAndTimestampMap.get(tp1).timestamp());
            assertEquals(expectedOffsetForTp1.longValue(), offsetAndTimestampMap.get(tp1).offset());
        }
    }

    private MockClient.RequestMatcher listOffsetRequestMatcher(final long timestamp) {
        // matches any list offset request with the provided timestamp
        return new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ListOffsetRequest req = (ListOffsetRequest) body;
                return timestamp == req.partitionTimestamps().get(tp1);
            }
        };
    }

    private ListOffsetResponse listOffsetResponse(Errors error, long timestamp, long offset) {
        return listOffsetResponse(tp1, error, timestamp, offset);
    }

    private ListOffsetResponse listOffsetResponse(TopicPartition tp, Errors error, long timestamp, long offset) {
        ListOffsetResponse.PartitionData partitionData = new ListOffsetResponse.PartitionData(error, timestamp, offset);
        Map<TopicPartition, ListOffsetResponse.PartitionData> allPartitionData = new HashMap<>();
        allPartitionData.put(tp, partitionData);
        return new ListOffsetResponse(allPartitionData);
    }

    private FetchResponse fetchResponse(MemoryRecords records, Errors error, long hw, int throttleTime) {
        Map<TopicPartition, FetchResponse.PartitionData> partitions = Collections.singletonMap(tp1,
                new FetchResponse.PartitionData(error, hw, FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, null, records));
        return new FetchResponse(new LinkedHashMap<>(partitions), throttleTime);
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
                        Arrays.asList(partitionInfo.inSyncReplicas())));
            }
        }

        MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(error, topic, false, partitionsMetadata);
        return new MetadataResponse(cluster.nodes(), null, MetadataResponse.NO_CONTROLLER_ID, Arrays.asList(topicMetadata));
    }

    private Fetcher<byte[], byte[]> createFetcher(SubscriptionState subscriptions,
                                                  Metrics metrics,
                                                  int maxPollRecords) {
        return createFetcher(subscriptions, metrics, new ByteArrayDeserializer(), new ByteArrayDeserializer(), maxPollRecords);
    }

    private Fetcher<byte[], byte[]> createFetcher(SubscriptionState subscriptions, Metrics metrics) {
        return createFetcher(subscriptions, metrics, Integer.MAX_VALUE);
    }

    private <K, V> Fetcher<K, V> createFetcher(SubscriptionState subscriptions,
                                               Metrics metrics,
                                               Deserializer<K> keyDeserializer,
                                               Deserializer<V> valueDeserializer) {
        return createFetcher(subscriptions, metrics, keyDeserializer, valueDeserializer, Integer.MAX_VALUE);
    }

    private <K, V> Fetcher<K, V> createFetcher(SubscriptionState subscriptions,
                                               Metrics metrics,
                                               Deserializer<K> keyDeserializer,
                                               Deserializer<V> valueDeserializer,
                                               int maxPollRecords) {
        return new Fetcher<>(consumerClient,
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
                "consumer" + groupId,
                time,
                retryBackoffMs);
    }

}
