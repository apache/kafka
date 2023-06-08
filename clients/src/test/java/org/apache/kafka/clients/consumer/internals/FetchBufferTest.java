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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.Utils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.Utils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This tests the {@link FetchBuffer} functionality in addition to what {@link FetcherTest} tests during the course
 * of its tests. One of the main concerns of these tests are that we correctly handle both places that data is held
 * internally:
 *
 * <ol>
 *     <li>A special "next in line" buffer</li>
 *     <li>The remainder of the buffers in a queue</li>
 * </ol>
 */
public class FetchBufferTest {

    private final Time time = new MockTime(0, 0, 0);
    private final TopicPartition topicAPartition0 = new TopicPartition("topic-a", 0);
    private final TopicPartition topicAPartition1 = new TopicPartition("topic-a", 1);
    private final TopicPartition topicAPartition2 = new TopicPartition("topic-a", 2);
    private final Set<TopicPartition> allPartitions = partitions(topicAPartition0, topicAPartition1, topicAPartition2);
    private LogContext logContext;

    private SubscriptionState subscriptions;

    private FetchConfig<String, String> fetchConfig;

    private FetchMetricsManager metricsManager;

    @BeforeEach
    public void setup() {
        logContext = new LogContext();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        ConsumerConfig config = new ConsumerConfig(p);

        Deserializers<String, String> deserializers = new Deserializers<>(new StringDeserializer(), new StringDeserializer());

        subscriptions = createSubscriptionState(config, logContext);
        fetchConfig = createFetchConfig(config, deserializers);

        Metrics metrics = createMetrics(config, time);
        metricsManager = createFetchMetricsManager(metrics);
    }

    /**
     * Verifies the basics: we can add buffered data to the queue, peek to view them, and poll to remove them.
     */
    @Test
    public void testBasicPeekAndPoll() {
        try (FetchBuffer<String, String> fetchBuffer = new FetchBuffer<>(logContext)) {
            CompletedFetch<String, String> completedFetch = completedFetch(topicAPartition0);
            assertTrue(fetchBuffer.isEmpty());
            fetchBuffer.add(completedFetch);
            assertTrue(fetchBuffer.hasCompletedFetches(p -> true));
            assertFalse(fetchBuffer.isEmpty());
            assertNotNull(fetchBuffer.peek());
            assertSame(completedFetch, fetchBuffer.peek());
            assertSame(completedFetch, fetchBuffer.poll());
            assertNull(fetchBuffer.peek());
        }
    }

    /**
     * Verifies {@link FetchBuffer#close()}} closes the buffered data for both the queue and the next-in-line buffer.
     */
    @Test
    public void testCloseClearsData() {
        FetchBuffer<String, String> fetchBuffer = null;

        try {
            fetchBuffer = new FetchBuffer<>(logContext);
            assertNull(fetchBuffer.nextInLineFetch());
            assertTrue(fetchBuffer.isEmpty());

            fetchBuffer.add(completedFetch(topicAPartition0));
            assertFalse(fetchBuffer.isEmpty());

            fetchBuffer.setNextInLineFetch(completedFetch(topicAPartition0));
            assertNotNull(fetchBuffer.nextInLineFetch());
        } finally {
            if (fetchBuffer != null)
                fetchBuffer.close();
        }

        assertNull(fetchBuffer.nextInLineFetch());
        assertTrue(fetchBuffer.isEmpty());
    }

    /**
     * Tests that the buffer returns partitions for both the queue and the next-in-line buffer.
     */
    @Test
    public void testPartitions() {
        try (FetchBuffer<String, String> fetchBuffer = new FetchBuffer<>(logContext)) {
            fetchBuffer.setNextInLineFetch(completedFetch(topicAPartition0));
            fetchBuffer.add(completedFetch(topicAPartition1));
            fetchBuffer.add(completedFetch(topicAPartition2));
            assertEquals(allPartitions, fetchBuffer.partitions());

            fetchBuffer.setNextInLineFetch(null);
            assertEquals(partitions(topicAPartition1, topicAPartition2), fetchBuffer.partitions());

            fetchBuffer.poll();
            assertEquals(partitions(topicAPartition2), fetchBuffer.partitions());

            fetchBuffer.poll();
            assertEquals(partitions(), fetchBuffer.partitions());
        }
    }

    /**
     * Tests that the buffer manipulates partitions for both the queue and the next-in-line buffer.
     */
    @Test
    public void testAddAllAndRetainAll() {
        try (FetchBuffer<String, String> fetchBuffer = new FetchBuffer<>(logContext)) {
            fetchBuffer.setNextInLineFetch(completedFetch(topicAPartition0));
            fetchBuffer.addAll(Arrays.asList(completedFetch(topicAPartition1), completedFetch(topicAPartition2)));
            assertEquals(allPartitions, fetchBuffer.partitions());

            fetchBuffer.retainAll(partitions(topicAPartition1, topicAPartition2));
            assertEquals(partitions(topicAPartition1, topicAPartition2), fetchBuffer.partitions());

            fetchBuffer.retainAll(partitions(topicAPartition2));
            assertEquals(partitions(topicAPartition2), fetchBuffer.partitions());

            fetchBuffer.retainAll(partitions());
            assertEquals(partitions(), fetchBuffer.partitions());
        }
    }

    private CompletedFetch<String, String> completedFetch(TopicPartition tp) {
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData();
        FetchMetricsAggregator metricsAggregator = new FetchMetricsAggregator(metricsManager, allPartitions);
        return new CompletedFetch<>(
                logContext,
                subscriptions,
                fetchConfig,
                BufferSupplier.create(),
                tp,
                partitionData,
                metricsAggregator,
                0L,
                ApiKeys.FETCH.latestVersion());
    }

    /**
     * This is a handy utility method for returning a set from a varargs array.
     */
    private static Set<TopicPartition> partitions(TopicPartition... partitions) {
        return new HashSet<>(Arrays.asList(partitions));
    }
}
