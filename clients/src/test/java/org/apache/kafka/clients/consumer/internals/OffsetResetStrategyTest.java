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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class OffsetResetStrategyTest {
    private ConsumerRebalanceListener listener = new NoOpConsumerRebalanceListener();
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
    private SubscriptionState subscriptions = new SubscriptionState(OffsetResetStrategy.NONE);
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
    public void testFetchOffsetOutOfRange() {
        subscriptions.assign(Arrays.asList(tp));
        subscriptions.seek(tp, 0);

        fetcher.initFetches(cluster);
        client.prepareResponse(fetchResponse(this.records.buffer(), Errors.OFFSET_OUT_OF_RANGE.code(), 100L, 0));
        consumerClient.poll(0);
        assertFalse(subscriptions.isOffsetResetNeeded(tp));
        try {
            fetcher.fetchedRecords();
            fail("Should have thrown OffsetOutOfRangeException");
        } catch (OffsetOutOfRangeException e) {
            assertTrue(e.offsetOutOfRangePartitions().containsKey(tp));
            assertEquals(e.offsetOutOfRangePartitions().size(), 1);
        }
        assertEquals(0, fetcher.fetchedRecords().size());
    }

    private Struct fetchResponse(ByteBuffer buffer, short error, long hw, int throttleTime) {
        FetchResponse response = new FetchResponse(Collections.singletonMap(tp, new FetchResponse.PartitionData(error, hw, buffer)), throttleTime);
        return response.toStruct();
    }
}
