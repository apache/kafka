/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FetcherTest {
    private SubscriptionState.RebalanceListener listener = new SubscriptionState.RebalanceListener();
    private String topicName = "test";
    private String groupId = "test-group";
    private final String metricGroup = "consumer" + groupId + "-fetch-manager-metrics";
    private TopicPartition tp = new TopicPartition(topicName, 0);
    private int minBytes = 1;
    private int maxWaitMs = 0;
    private int fetchSize = 1000;
    private long retryBackoffMs = 100;
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private Cluster cluster = TestUtils.singletonCluster(topicName, 1);
    private Node node = cluster.nodes().get(0);
    private SubscriptionState subscriptions = new SubscriptionState(OffsetResetStrategy.EARLIEST);
    private Metrics metrics = new Metrics(time);
    private Map<String, String> metricTags = new LinkedHashMap<String, String>();
    private static final double EPSILON = 0.0001;
    private ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(client, metadata, time, 100);

    private MemoryRecords records = MemoryRecords.emptyRecords(ByteBuffer.allocate(1024), CompressionType.NONE);

    private Fetcher<byte[], byte[]> fetcher = new Fetcher<byte[], byte[]>(consumerClient,
                                                                          minBytes,
                                                                          maxWaitMs,
                                                                          fetchSize,
                                                                          true, // check crc
                                                                          new ByteArrayDeserializer(),
                                                                          new ByteArrayDeserializer(),
                                                                          metadata,
                                                                          subscriptions,
                                                                          metrics,
                                                                          "consumer" + groupId,
                                                                          metricTags,
                                                                          time,
                                                                          retryBackoffMs);

    @Before
    public void setup() throws Exception {
        metadata.update(cluster, time.milliseconds());
        client.setNode(node);

        records.append(1L, "key".getBytes(), "value-1".getBytes());
        records.append(2L, "key".getBytes(), "value-2".getBytes());
        records.append(3L, "key".getBytes(), "value-3".getBytes());
        records.close();
        records.flip();
    }

    @Test
    public void testFetchNormal() {
        List<ConsumerRecord<byte[], byte[]>> records;
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        // normal fetch
        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.NONE.code(), 100L, 0));
        consumerClient.poll(0);
        records = fetcher.fetchedRecords().get(tp);
        assertEquals(3, records.size());
        assertEquals(4L, (long) subscriptions.fetched(tp)); // this is the next fetching position
        assertEquals(4L, (long) subscriptions.consumed(tp));
        long offset = 1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertEquals(offset, record.offset());
            offset += 1;
        }
    }

    @Test
    public void testFetchDuringRebalance() {
        subscriptions.subscribe(Arrays.asList(topicName), listener);
        subscriptions.changePartitionAssignment(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        fetcher.initFetches(cluster);

        // Now the rebalance happens and fetch positions are cleared
        subscriptions.changePartitionAssignment(Arrays.asList(tp));
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.NONE.code(), 100L, 0));
        consumerClient.poll(0);

        // The active fetch should be ignored since its position is no longer valid
        assertTrue(fetcher.fetchedRecords().isEmpty());
    }

    @Test
    public void testInFlightFetchOnPausedPartition() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        fetcher.initFetches(cluster);
        subscriptions.pause(tp);

        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.NONE.code(), 100L, 0));
        consumerClient.poll(0);
        assertNull(fetcher.fetchedRecords().get(tp));
    }

    @Test
    public void testFetchOnPausedPartition() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        subscriptions.pause(tp);
        fetcher.initFetches(cluster);
        assertTrue(client.requests().isEmpty());
    }

    @Test
    public void testFetchNotLeaderForPartition() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.NOT_LEADER_FOR_PARTITION.code(), 100L, 0));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testFetchUnknownTopicOrPartition() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), 100L, 0));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testFetchOffsetOutOfRange() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.OFFSET_OUT_OF_RANGE.code(), 100L, 0));
        consumerClient.poll(0);
        assertTrue(subscriptions.isOffsetResetNeeded(tp));
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(null, subscriptions.fetched(tp));
        assertEquals(null, subscriptions.consumed(tp));
    }

    @Test
    public void testFetchDisconnected() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.NONE.code(), 100L, 0), true);
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());

        // disconnects should have no affect on subscription state
        assertFalse(subscriptions.isOffsetResetNeeded(tp));
        assertTrue(subscriptions.isFetchable(tp));
        assertEquals(0, (long) subscriptions.fetched(tp));
        assertEquals(0, (long) subscriptions.consumed(tp));
    }

    @Test
    public void testUpdateFetchPositionToCommitted() {
        // unless a specific reset is expected, the default behavior is to reset to the committed
        // position if one is present
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.committed(tp, 5);

        fetcher.updateFetchPositions(Collections.singleton(tp));
        assertTrue(subscriptions.isFetchable(tp));
        assertEquals(5, (long) subscriptions.fetched(tp));
        assertEquals(5, (long) subscriptions.consumed(tp));
    }

    @Test
    public void testUpdateFetchPositionResetToDefaultOffset() {
        subscriptions.assign(Arrays.asList(tp));
        // with no commit position, we should reset using the default strategy defined above (EARLIEST)

        client.prepareResponse(listOffsetRequestMatcher(Fetcher.EARLIEST_OFFSET_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, Arrays.asList(5L)));
        fetcher.updateFetchPositions(Collections.singleton(tp));
        assertFalse(subscriptions.isOffsetResetNeeded(tp));
        assertTrue(subscriptions.isFetchable(tp));
        assertEquals(5, (long) subscriptions.fetched(tp));
        assertEquals(5, (long) subscriptions.consumed(tp));
    }

    @Test
    public void testUpdateFetchPositionResetToLatestOffset() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetRequestMatcher(Fetcher.LATEST_OFFSET_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, Arrays.asList(5L)));
        fetcher.updateFetchPositions(Collections.singleton(tp));
        assertFalse(subscriptions.isOffsetResetNeeded(tp));
        assertTrue(subscriptions.isFetchable(tp));
        assertEquals(5, (long) subscriptions.fetched(tp));
        assertEquals(5, (long) subscriptions.consumed(tp));
    }

    @Test
    public void testUpdateFetchPositionResetToEarliestOffset() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.needOffsetReset(tp, OffsetResetStrategy.EARLIEST);

        client.prepareResponse(listOffsetRequestMatcher(Fetcher.EARLIEST_OFFSET_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, Arrays.asList(5L)));
        fetcher.updateFetchPositions(Collections.singleton(tp));
        assertFalse(subscriptions.isOffsetResetNeeded(tp));
        assertTrue(subscriptions.isFetchable(tp));
        assertEquals(5, (long) subscriptions.fetched(tp));
        assertEquals(5, (long) subscriptions.consumed(tp));
    }

    @Test
    public void testUpdateFetchPositionDisconnect() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);

        // First request gets a disconnect
        client.prepareResponse(listOffsetRequestMatcher(Fetcher.LATEST_OFFSET_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, Arrays.asList(5L)), true);

        // Next one succeeds
        client.prepareResponse(listOffsetRequestMatcher(Fetcher.LATEST_OFFSET_TIMESTAMP),
                               listOffsetResponse(Errors.NONE, Arrays.asList(5L)));
        fetcher.updateFetchPositions(Collections.singleton(tp));
        assertFalse(subscriptions.isOffsetResetNeeded(tp));
        assertTrue(subscriptions.isFetchable(tp));
        assertEquals(5, (long) subscriptions.fetched(tp));
        assertEquals(5, (long) subscriptions.consumed(tp));
    }

    @Test
    public void testGetAllTopics() throws InterruptedException {
        // sending response before request, as getAllTopics is a blocking call
        client.prepareResponse(
            new MetadataResponse(cluster, Collections.<String, Errors>emptyMap()).toStruct());

        Map<String, List<PartitionInfo>> allTopics = fetcher.getAllTopics(5000L);

        assertEquals(cluster.topics().size(), allTopics.size());
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testQuotaMetrics() throws Exception {
        List<ConsumerRecord<byte[], byte[]>> records;
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        // normal fetch
        for (int i = 1; i < 4; i++) {
            fetcher.initFetches(cluster);

            client.prepareResponse(fetchResponse(this.records.buffer(), Errors.NONE.code(), 100L, 100 * i));
            consumerClient.poll(0);
            records = fetcher.fetchedRecords().get(tp);
            assertEquals(3, records.size());
        }

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric avgMetric = allMetrics.get(new MetricName("fetch-throttle-time-avg", metricGroup, "", metricTags));
        KafkaMetric maxMetric = allMetrics.get(new MetricName("fetch-throttle-time-max", metricGroup, "", metricTags));
        assertEquals(200, avgMetric.value(), EPSILON);
        assertEquals(300, maxMetric.value(), EPSILON);
    }

    private MockClient.RequestMatcher listOffsetRequestMatcher(final long timestamp) {
        // matches any list offset request with the provided timestamp
        return new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                ListOffsetRequest req = new ListOffsetRequest(request.request().body());
                ListOffsetRequest.PartitionData partitionData = req.offsetData().get(tp);
                return partitionData != null && partitionData.timestamp == timestamp;
            }
        };
    }

    private Struct listOffsetResponse(Errors error, List<Long> offsets) {
        ListOffsetResponse.PartitionData partitionData = new ListOffsetResponse.PartitionData(error.code(), offsets);
        Map<TopicPartition, ListOffsetResponse.PartitionData> allPartitionData = new HashMap<>();
        allPartitionData.put(tp, partitionData);
        ListOffsetResponse response = new ListOffsetResponse(allPartitionData);
        return response.toStruct();
    }

    private Struct fetchResponse(ByteBuffer buffer, short error, long hw, int throttleTime) {
        FetchResponse response = new FetchResponse(Collections.singletonMap(tp, new FetchResponse.PartitionData(error, hw, buffer)), throttleTime);
        return response.toStruct();
    }
}
