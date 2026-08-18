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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
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

    private static final Optional<Integer> DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS = Optional.of(30000);
    private final Time time = new MockTime(0, 0, 0);
    private final TopicIdPartition topicAPartition0 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic-a");
    private final TopicIdPartition topicAPartition1 = new TopicIdPartition(Uuid.randomUuid(), 1, "topic-a");
    private final TopicIdPartition topicAPartition2 = new TopicIdPartition(Uuid.randomUuid(), 2, "topic-a");
    private final Set<TopicIdPartition> allPartitions = partitions(topicAPartition0, topicAPartition1, topicAPartition2);
    private final Deserializers<String, String> deserializers = new Deserializers<>(new StringDeserializer(), new StringDeserializer(), null);
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
            fetchBuffer.add(List.of(completedFetch));
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

            fetchBuffer.add(List.of(completedFetch(topicAPartition0)));
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
            fetchBuffer.add(List.of(completedFetch(topicAPartition1), completedFetch(topicAPartition2)));
            assertEquals(allPartitions, fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(0), fetchBuffer.bufferedNodes());

            fetchBuffer.setNextInLineFetch(null);
            assertEquals(partitions(topicAPartition1, topicAPartition2), fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(0), fetchBuffer.bufferedNodes());

            fetchBuffer.poll();
            assertEquals(partitions(topicAPartition2), fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(0), fetchBuffer.bufferedNodes());

            fetchBuffer.poll();
            assertEquals(partitions(), fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(), fetchBuffer.bufferedNodes());
        }
    }

    /**
     * Tests that a fetch which has been consumed but whose acknowledgements are still outstanding continues to be
     * reported as buffered, thus preventing the share session for its node being closed prematurely.
     */
    @Test
    public void testBufferedIncludesConsumedFetchWithPendingAcknowledgements() {
        try (ShareFetchBuffer fetchBuffer = new ShareFetchBuffer(logContext)) {
            ShareCompletedFetch completedFetch = completedFetchWithAcquiredRecords(topicAPartition0, 0, 5);
            ShareInFlightBatch<String, String> batch = consumeFetchWithAcquiredRecords(completedFetch);
            fetchBuffer.setNextInLineFetch(completedFetch);

            // Even though the fetch has been consumed, its acknowledgements are still outstanding, so it is still buffered.
            assertEquals(partitions(topicAPartition0), fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(0), fetchBuffer.bufferedNodes());
            assertTrue(completedFetch.hasPendingAcknowledgements());

            // Acknowledging and taking the records clears the outstanding acknowledgements, so it is no longer buffered.
            batch.acknowledgeAll(AcknowledgeType.ACCEPT);
            batch.takeAcknowledgedRecords();
            assertFalse(completedFetch.hasPendingAcknowledgements());
            assertEquals(partitions(), fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(), fetchBuffer.bufferedNodes());
        }
    }

    @Test
    public void testRetainsEvictedFetchWithPendingAcknowledgements() {
        try (ShareFetchBuffer fetchBuffer = new ShareFetchBuffer(logContext)) {
            ShareCompletedFetch fetchForNode0 = completedFetchWithAcquiredRecords(topicAPartition0, 0, 5);
            ShareInFlightBatch<String, String> batch = consumeFetchWithAcquiredRecords(fetchForNode0);
            fetchBuffer.setNextInLineFetch(fetchForNode0);

            // Replacing the next-in-line fetch with one for a different node still retains the node 0 fetch because
            // its acknowledgements are outstanding.
            fetchBuffer.setNextInLineFetch(completedFetchWithAcquiredRecords(topicAPartition1, 1, 5));
            assertEquals(partitions(topicAPartition0, topicAPartition1), fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(0, 1), fetchBuffer.bufferedNodes());

            // Once node 0's acknowledgements are taken, the retained fetch is pruned.
            batch.acknowledgeAll(AcknowledgeType.ACCEPT);
            batch.takeAcknowledgedRecords();
            assertEquals(partitions(topicAPartition1), fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(1), fetchBuffer.bufferedNodes());
        }
    }

    @Test
    public void testRetainsEvictedFetchWithPendingRenewAcknowledgements() {
        try (ShareFetchBuffer fetchBuffer = new ShareFetchBuffer(logContext)) {
            ShareCompletedFetch completedFetch = completedFetchWithAcquiredRecords(topicAPartition0, 0, 5);
            ShareInFlightBatch<String, String> batch = consumeFetchWithAcquiredRecords(completedFetch);
            fetchBuffer.setNextInLineFetch(completedFetch);

            // Renewing the records moves them out of the in-flight set, but they are still held and the fetch remains buffered.
            batch.acknowledgeAll(AcknowledgeType.RENEW);
            Acknowledgements renewAcknowledgements = batch.takeAcknowledgedRecords();
            assertTrue(completedFetch.hasPendingAcknowledgements());
            assertEquals(partitions(topicAPartition0), fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(0), fetchBuffer.bufferedNodes());

            // The renewal completed and the records move to the renewed state, and they are still held.
            renewAcknowledgements.complete(null);
            batch.renew(renewAcknowledgements);
            assertTrue(completedFetch.hasPendingAcknowledgements());
            assertEquals(Set.of(0), fetchBuffer.bufferedNodes());

            // The renewed records return to the in-flight set, so still buffered.
            batch.takeRenewals();
            assertTrue(completedFetch.hasPendingAcknowledgements());
            assertEquals(Set.of(0), fetchBuffer.bufferedNodes());

            // Finally the records accepted and the fetch is no longer buffered.
            batch.acknowledgeAll(AcknowledgeType.ACCEPT);
            batch.takeAcknowledgedRecords();
            assertFalse(completedFetch.hasPendingAcknowledgements());
            assertEquals(partitions(), fetchBuffer.bufferedPartitions());
            assertEquals(Set.of(), fetchBuffer.bufferedNodes());
        }
    }

    @Test
    public void testCloseClearsPendingAcknowledgementFetches() {
        ShareFetchBuffer fetchBuffer = new ShareFetchBuffer(logContext);
        try {
            ShareCompletedFetch fetchForNode0 = completedFetchWithAcquiredRecords(topicAPartition0, 0, 5);
            consumeFetchWithAcquiredRecords(fetchForNode0);
            fetchBuffer.setNextInLineFetch(fetchForNode0);

            // Replace the next-in-line fetch with one for a different node
            fetchBuffer.setNextInLineFetch(completedFetchWithAcquiredRecords(topicAPartition1, 1, 5));
            assertEquals(Set.of(0, 1), fetchBuffer.bufferedNodes());
        } finally {
            fetchBuffer.close();
        }

        assertEquals(partitions(), fetchBuffer.bufferedPartitions());
        assertEquals(Set.of(), fetchBuffer.bufferedNodes());
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

    private ShareInFlightBatch<String, String> consumeFetchWithAcquiredRecords(ShareCompletedFetch completedFetch) {
        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 100, false);
        assertTrue(completedFetch.isConsumed());
        assertTrue(completedFetch.hasPendingAcknowledgements());
        return batch;
    }

    private ShareCompletedFetch completedFetch(TopicIdPartition tp) {
        return completedFetch(tp, 0, new ShareFetchResponseData.PartitionData());
    }

    private ShareCompletedFetch completedFetchWithAcquiredRecords(TopicIdPartition tp, int nodeId, int numRecords) {
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(newRecords(0, numRecords))
                .setAcquiredRecords(acquiredRecords(0, numRecords));
        return completedFetch(tp, nodeId, partitionData);
    }

    private ShareCompletedFetch completedFetch(TopicIdPartition tp, int nodeId, ShareFetchResponseData.PartitionData partitionData) {
        ShareFetchMetricsAggregator shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(shareFetchMetricsManager,
                allPartitions.stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet()));
        return new ShareCompletedFetch(
                logContext,
                BufferSupplier.create(),
                nodeId,
                tp,
                partitionData,
                DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS,
                shareFetchMetricsAggregator,
                ApiKeys.SHARE_FETCH.latestVersion());
    }

    private static Records newRecords(long baseOffset, int numRecords) {
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, baseOffset)) {
            for (int i = 0; i < numRecords; i++) {
                builder.append(0L, "key".getBytes(), "value".getBytes());
            }
            return builder.build();
        }
    }

    private static List<ShareFetchResponseData.AcquiredRecords> acquiredRecords(long firstOffset, int numRecords) {
        return List.of(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(firstOffset)
                .setLastOffset(firstOffset + numRecords - 1)
                .setDeliveryCount((short) 1));
    }

    /**
     * This is a handy utility method for returning a set from a varargs array.
     */
    private static Set<TopicIdPartition> partitions(TopicIdPartition... partitions) {
        return Set.of(partitions);
    }
}
