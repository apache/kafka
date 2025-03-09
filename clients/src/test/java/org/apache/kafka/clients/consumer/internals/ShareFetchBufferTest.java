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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createShareFetchMetricsManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This tests the {@link ShareFetchBuffer} functionality.
 * One of the main concerns of these tests are that we correctly handle both places that data is held internally:
 *
 * <ol>
 *     <li>A special "next in line" buffer</li>
 *     <li>The remainder of the buffers in a queue</li>
 * </ol>
 */
public class ShareFetchBufferTest {

    private final Time time = new MockTime(0, 0, 0);
    private final TopicIdPartition topicAPartition0 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic-a");
    private final TopicIdPartition topicAPartition1 = new TopicIdPartition(Uuid.randomUuid(), 1, "topic-a");
    private final TopicIdPartition topicAPartition2 = new TopicIdPartition(Uuid.randomUuid(), 2, "topic-a");
    private final Set<TopicIdPartition> allPartitions = partitions(topicAPartition0, topicAPartition1, topicAPartition2);
    private LogContext logContext;
    private ShareFetchMetricsManager shareFetchMetricsManager;

    @BeforeEach
    public void setup() {
        logContext = new LogContext();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        ConsumerConfig config = new ConsumerConfig(p);

        Metrics metrics = createMetrics(config, time);
        shareFetchMetricsManager = createShareFetchMetricsManager(metrics);
    }

    /**
     * Verifies the basics: we can add buffered data to the queue, peek to view them, and poll to remove them.
     */
    @Test
    public void testBasicPeekAndPoll() {
        try (ShareFetchBuffer fetchBuffer = new ShareFetchBuffer(logContext)) {
            ShareCompletedFetch completedFetch = completedFetch(topicAPartition0);
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
     * Verifies {@link ShareFetchBuffer#close()}} closes the buffered data for both the queue and the next-in-line buffer.
     */
    @Test
    public void testCloseClearsData() {
        // We don't use the try-with-resources approach because we want to have access to the FetchBuffer after
        // the try block so that we can run our asserts on the object.
        ShareFetchBuffer fetchBuffer = null;

        try {
            fetchBuffer = new ShareFetchBuffer(logContext);
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
    public void testBufferedPartitions() {
        try (ShareFetchBuffer fetchBuffer = new ShareFetchBuffer(logContext)) {
            fetchBuffer.setNextInLineFetch(completedFetch(topicAPartition0));
            fetchBuffer.add(completedFetch(topicAPartition1));
            fetchBuffer.add(completedFetch(topicAPartition2));
            assertEquals(allPartitions, fetchBuffer.bufferedPartitions());

            fetchBuffer.setNextInLineFetch(null);
            assertEquals(partitions(topicAPartition1, topicAPartition2), fetchBuffer.bufferedPartitions());

            fetchBuffer.poll();
            assertEquals(partitions(topicAPartition2), fetchBuffer.bufferedPartitions());

            fetchBuffer.poll();
            assertEquals(partitions(), fetchBuffer.bufferedPartitions());
        }
    }

    @Test
    public void testWakeup() throws Exception {
        try (ShareFetchBuffer fetchBuffer = new ShareFetchBuffer(logContext)) {
            final Thread waitingThread = new Thread(() -> {
                final Timer timer = time.timer(Duration.ofMinutes(1));
                fetchBuffer.awaitNotEmpty(timer);
            });
            waitingThread.start();
            fetchBuffer.wakeup();
            waitingThread.join(Duration.ofSeconds(30).toMillis());
            assertFalse(waitingThread.isAlive());
        }
    }

    private ShareCompletedFetch completedFetch(TopicIdPartition tp) {
        ShareFetchMetricsAggregator shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(shareFetchMetricsManager,
                allPartitions.stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet()));
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData();
        return new ShareCompletedFetch(
                logContext,
                BufferSupplier.create(),
                0,
                tp,
                partitionData,
                shareFetchMetricsAggregator,
                ApiKeys.SHARE_FETCH.latestVersion());
    }

    /**
     * This is a handy utility method for returning a set from a varargs array.
     */
    private static Set<TopicIdPartition> partitions(TopicIdPartition... partitions) {
        return new HashSet<>(Arrays.asList(partitions));
    }
}
