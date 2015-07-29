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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FetcherTest {

    private String topicName = "test";
    private String groupId = "test-group";
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
        subscriptions.subscribe(tp);
        subscriptions.fetched(tp, 0);
        subscriptions.consumed(tp, 0);

        // normal fetch
        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.NONE.code(), 100L));
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
        subscriptions.subscribe(topicName);
        subscriptions.changePartitionAssignment(Arrays.asList(tp));
        subscriptions.fetched(tp, 0);
        subscriptions.consumed(tp, 0);

        fetcher.initFetches(cluster);

        // Now the rebalance happens and fetch positions are cleared
        subscriptions.changePartitionAssignment(Arrays.asList(tp));
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.NONE.code(), 100L));
        consumerClient.poll(0);

        // The active fetch should be ignored since its position is no longer valid
        assertTrue(fetcher.fetchedRecords().isEmpty());
    }

    @Test
    public void testFetchFailed() {
        subscriptions.subscribe(tp);
        subscriptions.fetched(tp, 0);
        subscriptions.consumed(tp, 0);

        // fetch with not leader
        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.NOT_LEADER_FOR_PARTITION.code(), 100L));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));

        // fetch with unknown topic partition
        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), 100L));
        consumerClient.poll(0);
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));

        // fetch with out of range
        subscriptions.fetched(tp, 5);
        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.OFFSET_OUT_OF_RANGE.code(), 100L));
        consumerClient.poll(0);
        assertTrue(subscriptions.isOffsetResetNeeded(tp));
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(null, subscriptions.fetched(tp));
        assertEquals(null, subscriptions.consumed(tp));
    }

    @Test
    public void testFetchOutOfRange() {
        subscriptions.subscribe(tp);
        subscriptions.fetched(tp, 5);
        subscriptions.consumed(tp, 5);

        // fetch with out of range
        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.OFFSET_OUT_OF_RANGE.code(), 100L));
        consumerClient.poll(0);
        assertTrue(subscriptions.isOffsetResetNeeded(tp));
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(null, subscriptions.fetched(tp));
        assertEquals(null, subscriptions.consumed(tp));
    }

    @Test
    public void testGetAllTopics() throws InterruptedException {
        // sending response before request, as getAllTopics is a blocking call
        client.prepareResponse(
            new MetadataResponse(cluster, Collections.<String, Errors>emptyMap()).toStruct());

        Map<String, List<PartitionInfo>> allTopics = fetcher.getAllTopics(5000L);

        assertEquals(cluster.topics().size(), allTopics.size());
    }

    private Struct fetchResponse(ByteBuffer buffer, short error, long hw) {
        FetchResponse response = new FetchResponse(Collections.singletonMap(tp, new FetchResponse.PartitionData(error, hw, buffer)));
        return response.toStruct();
    }


}
