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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionConfig;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RecordAccumulatorTest {

    private String topic = "test";
    private int partition1 = 0;
    private int partition2 = 1;
    private int partition3 = 2;
    private Node node1 = new Node(0, "localhost", 1111);
    private Node node2 = new Node(1, "localhost", 1112);
    private TopicPartition tp1 = new TopicPartition(topic, partition1);
    private TopicPartition tp2 = new TopicPartition(topic, partition2);
    private TopicPartition tp3 = new TopicPartition(topic, partition3);
    private PartitionInfo part1 = new PartitionInfo(topic, partition1, node1, null, null);
    private PartitionInfo part2 = new PartitionInfo(topic, partition2, node1, null, null);
    private PartitionInfo part3 = new PartitionInfo(topic, partition3, node2, null, null);
    private MockTime time = new MockTime();
    private byte[] key = "key".getBytes();
    private byte[] value = "value".getBytes();
    private int msgSize = DefaultRecord.sizeInBytes(0, 0, key.length, value.length, Record.EMPTY_HEADERS);
    private Cluster cluster = new Cluster(null, Arrays.asList(node1, node2), Arrays.asList(part1, part2, part3),
        Collections.emptySet(), Collections.emptySet());
    private Metrics metrics = new Metrics(time);
    private final long maxBlockTimeMs = 1000;
    private final LogContext logContext = new LogContext();

    @AfterEach
    public void teardown() {
        this.metrics.close();
    }

    @Test
    public void testFull() throws Exception {
        long now = time.milliseconds();

        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;

        RecordAccumulator accum = createTestRecordAccumulator(
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10L * batchSize, CompressionConfig.NONE, 10);
        int appends = expectedNumAppends(batchSize);
        for (int i = 0; i < appends; i++) {
            // append to the first batch
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            Deque<ProducerBatch> partitionBatches = accum.batches().get(tp1);
            assertEquals(1, partitionBatches.size());

            ProducerBatch batch = partitionBatches.peekFirst();
            assertTrue(batch.isWritable());
            assertEquals(0, accum.ready(cluster, now).readyNodes.size(), "No partitions should be ready.");
        }

        // this append doesn't fit in the first batch, so a new batch is created and the first batch is closed

        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        Deque<ProducerBatch> partitionBatches = accum.batches().get(tp1);
        assertEquals(2, partitionBatches.size());
        Iterator<ProducerBatch> partitionBatchesIterator = partitionBatches.iterator();
        assertTrue(partitionBatchesIterator.next().isWritable());
        assertEquals(Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes, "Our partition's leader should be ready");

        List<ProducerBatch> batches = accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
        assertEquals(1, batches.size());
        ProducerBatch batch = batches.get(0);

        Iterator<Record> iter = batch.records().records().iterator();
        for (int i = 0; i < appends; i++) {
            Record record = iter.next();
            assertEquals(ByteBuffer.wrap(key), record.key(), "Keys should match");
            assertEquals(ByteBuffer.wrap(value), record.value(), "Values should match");
        }
        assertFalse(iter.hasNext(), "No more records");
    }

    @Test
    public void testAppendLargeCompressed() throws Exception {
        testAppendLarge(CompressionConfig.gzip().build());
    }

    @Test
    public void testAppendLargeNonCompressed() throws Exception {
        testAppendLarge(CompressionConfig.NONE);
    }

    private void testAppendLarge(CompressionConfig compressionConfig) throws Exception {
        int batchSize = 512;
        byte[] value = new byte[2 * batchSize];
        RecordAccumulator accum = createTestRecordAccumulator(
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, compressionConfig, 0);
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        assertEquals(Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes, "Our partition's leader should be ready");

        Deque<ProducerBatch> batches = accum.batches().get(tp1);
        assertEquals(1, batches.size());
        ProducerBatch producerBatch = batches.peek();
        List<MutableRecordBatch> recordBatches = TestUtils.toList(producerBatch.records().batches());
        assertEquals(1, recordBatches.size());
        MutableRecordBatch recordBatch = recordBatches.get(0);
        assertEquals(0L, recordBatch.baseOffset());
        List<Record> records = TestUtils.toList(recordBatch);
        assertEquals(1, records.size());
        Record record = records.get(0);
        assertEquals(0L, record.offset());
        assertEquals(ByteBuffer.wrap(key), record.key());
        assertEquals(ByteBuffer.wrap(value), record.value());
        assertEquals(0L, record.timestamp());
    }

    @Test
    public void testAppendLargeOldMessageFormatCompressed() throws Exception {
        testAppendLargeOldMessageFormat(CompressionConfig.gzip().build());
    }

    @Test
    public void testAppendLargeOldMessageFormatNonCompressed() throws Exception {
        testAppendLargeOldMessageFormat(CompressionConfig.gzip().build());
    }

    private void testAppendLargeOldMessageFormat(CompressionConfig compressionConfig) throws Exception {
        int batchSize = 512;
        byte[] value = new byte[2 * batchSize];

        ApiVersions apiVersions = new ApiVersions();
        apiVersions.update(node1.idString(), NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 0, (short) 2));

        RecordAccumulator accum = createTestRecordAccumulator(
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, compressionConfig, 0);
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        assertEquals(Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes, "Our partition's leader should be ready");

        Deque<ProducerBatch> batches = accum.batches().get(tp1);
        assertEquals(1, batches.size());
        ProducerBatch producerBatch = batches.peek();
        List<MutableRecordBatch> recordBatches = TestUtils.toList(producerBatch.records().batches());
        assertEquals(1, recordBatches.size());
        MutableRecordBatch recordBatch = recordBatches.get(0);
        assertEquals(0L, recordBatch.baseOffset());
        List<Record> records = TestUtils.toList(recordBatch);
        assertEquals(1, records.size());
        Record record = records.get(0);
        assertEquals(0L, record.offset());
        assertEquals(ByteBuffer.wrap(key), record.key());
        assertEquals(ByteBuffer.wrap(value), record.value());
        assertEquals(0L, record.timestamp());
    }

    @Test
    public void testLinger() throws Exception {
        int lingerMs = 10;
        RecordAccumulator accum = createTestRecordAccumulator(
                1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, CompressionConfig.NONE, lingerMs);
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        assertEquals(0, accum.ready(cluster, time.milliseconds()).readyNodes.size(), "No partitions should be ready");
        time.sleep(10);
        assertEquals(Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes, "Our partition's leader should be ready");
        List<ProducerBatch> batches = accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
        assertEquals(1, batches.size());
        ProducerBatch batch = batches.get(0);

        Iterator<Record> iter = batch.records().records().iterator();
        Record record = iter.next();
        assertEquals(ByteBuffer.wrap(key), record.key(), "Keys should match");
        assertEquals(ByteBuffer.wrap(value), record.value(), "Values should match");
        assertFalse(iter.hasNext(), "No more records");
    }

    @Test
    public void testPartialDrain() throws Exception {
        RecordAccumulator accum = createTestRecordAccumulator(
                1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, CompressionConfig.NONE, 10);
        int appends = 1024 / msgSize + 1;
        List<TopicPartition> partitions = asList(tp1, tp2);
        for (TopicPartition tp : partitions) {
            for (int i = 0; i < appends; i++)
                accum.append(tp, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        }
        assertEquals(Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes, "Partition's leader should be ready");

        List<ProducerBatch> batches = accum.drain(cluster, Collections.singleton(node1), 1024, 0).get(node1.id());
        assertEquals(1, batches.size(), "But due to size bound only one partition should have been retrieved");
    }

    @SuppressWarnings("unused")
    @Test
    public void testStressfulSituation() throws Exception {
        final int numThreads = 5;
        final int msgs = 10000;
        final int numParts = 2;
        final RecordAccumulator accum = createTestRecordAccumulator(
            1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, CompressionConfig.NONE, 0);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread() {
                public void run() {
                    for (int i = 0; i < msgs; i++) {
                        try {
                            accum.append(new TopicPartition(topic, i % numParts), 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        for (Thread t : threads)
            t.start();
        int read = 0;
        long now = time.milliseconds();
        while (read < numThreads * msgs) {
            Set<Node> nodes = accum.ready(cluster, now).readyNodes;
            List<ProducerBatch> batches = accum.drain(cluster, nodes, 5 * 1024, 0).get(node1.id());
            if (batches != null) {
                for (ProducerBatch batch : batches) {
                    for (Record record : batch.records().records())
                        read++;
                    accum.deallocate(batch);
                }
            }
        }

        for (Thread t : threads)
            t.join();
    }


    @Test
    public void testNextReadyCheckDelay() throws Exception {
        // Next check time will use lingerMs since this test won't trigger any retries/backoff
        int lingerMs = 10;

        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;

        RecordAccumulator accum = createTestRecordAccumulator(batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
                10 * batchSize, CompressionConfig.NONE, lingerMs);
        // Just short of going over the limit so we trigger linger time
        int appends = expectedNumAppends(batchSize);

        // Partition on node1 only
        for (int i = 0; i < appends; i++)
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals(0, result.readyNodes.size(), "No nodes should be ready.");
        assertEquals(lingerMs, result.nextReadyCheckDelayMs, "Next check time should be the linger time");

        time.sleep(lingerMs / 2);

        // Add partition on node2 only
        for (int i = 0; i < appends; i++)
            accum.append(tp3, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        result = accum.ready(cluster, time.milliseconds());
        assertEquals(0, result.readyNodes.size(), "No nodes should be ready.");
        assertEquals(lingerMs / 2, result.nextReadyCheckDelayMs, "Next check time should be defined by node1, half remaining linger time");

        // Add data for another partition on node1, enough to make data sendable immediately
        for (int i = 0; i < appends + 1; i++)
            accum.append(tp2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        result = accum.ready(cluster, time.milliseconds());
        assertEquals(Collections.singleton(node1), result.readyNodes, "Node1 should be ready");
        // Note this can actually be < linger time because it may use delays from partitions that aren't sendable
        // but have leaders with other sendable data.
        assertTrue(result.nextReadyCheckDelayMs <= lingerMs, "Next check time should be defined by node2, at most linger time");
    }

    @Test
    public void testRetryBackoff() throws Exception {
        int lingerMs = Integer.MAX_VALUE / 16;
        long retryBackoffMs = Integer.MAX_VALUE / 8;
        int deliveryTimeoutMs = Integer.MAX_VALUE;
        long totalSize = 10 * 1024;
        int batchSize = 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        String metricGrpName = "producer-metrics";

        final RecordAccumulator accum = new RecordAccumulator(logContext, batchSize,
            CompressionConfig.NONE, lingerMs, retryBackoffMs, deliveryTimeoutMs, metrics, metricGrpName, time, new ApiVersions(), null,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));

        long now = time.milliseconds();
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, now + lingerMs + 1);
        assertEquals(Collections.singleton(node1), result.readyNodes, "Node1 should be ready");
        Map<Integer, List<ProducerBatch>> batches = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, now + lingerMs + 1);
        assertEquals(1, batches.size(), "Node1 should be the only ready node.");
        assertEquals(1, batches.get(0).size(), "Partition 0 should only have one batch drained.");

        // Reenqueue the batch
        now = time.milliseconds();
        accum.reenqueue(batches.get(0).get(0), now);

        // Put message for partition 1 into accumulator
        accum.append(tp2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        result = accum.ready(cluster, now + lingerMs + 1);
        assertEquals(Collections.singleton(node1), result.readyNodes, "Node1 should be ready");

        // tp1 should backoff while tp2 should not
        batches = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, now + lingerMs + 1);
        assertEquals(1, batches.size(), "Node1 should be the only ready node.");
        assertEquals(1, batches.get(0).size(), "Node1 should only have one batch drained.");
        assertEquals(tp2, batches.get(0).get(0).topicPartition, "Node1 should only have one batch for partition 1.");

        // Partition 0 can be drained after retry backoff
        result = accum.ready(cluster, now + retryBackoffMs + 1);
        assertEquals(Collections.singleton(node1), result.readyNodes, "Node1 should be ready");
        batches = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, now + retryBackoffMs + 1);
        assertEquals(1, batches.size(), "Node1 should be the only ready node.");
        assertEquals(1, batches.get(0).size(), "Node1 should only have one batch drained.");
        assertEquals(tp1, batches.get(0).get(0).topicPartition, "Node1 should only have one batch for partition 0.");
    }

    @Test
    public void testFlush() throws Exception {
        int lingerMs = Integer.MAX_VALUE;
        final RecordAccumulator accum = createTestRecordAccumulator(
                4 * 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionConfig.NONE, lingerMs);

        for (int i = 0; i < 100; i++) {
            accum.append(new TopicPartition(topic, i % 3), 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            assertTrue(accum.hasIncomplete());
        }
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals(0, result.readyNodes.size(), "No nodes should be ready.");

        accum.beginFlush();
        result = accum.ready(cluster, time.milliseconds());

        // drain and deallocate all batches
        Map<Integer, List<ProducerBatch>> results = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertTrue(accum.hasIncomplete());

        for (List<ProducerBatch> batches: results.values())
            for (ProducerBatch batch: batches)
                accum.deallocate(batch);

        // should be complete with no unsent records.
        accum.awaitFlushCompletion();
        assertFalse(accum.hasUndrained());
        assertFalse(accum.hasIncomplete());
    }


    private void delayedInterrupt(final Thread thread, final long delayMs) {
        Thread t = new Thread() {
            public void run() {
                Time.SYSTEM.sleep(delayMs);
                thread.interrupt();
            }
        };
        t.start();
    }

    @Test
    public void testAwaitFlushComplete() throws Exception {
        RecordAccumulator accum = createTestRecordAccumulator(
            4 * 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionConfig.NONE, Integer.MAX_VALUE);
        accum.append(new TopicPartition(topic, 0), 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());

        accum.beginFlush();
        assertTrue(accum.flushInProgress());
        delayedInterrupt(Thread.currentThread(), 1000L);
        try {
            accum.awaitFlushCompletion();
            fail("awaitFlushCompletion should throw InterruptException");
        } catch (InterruptedException e) {
            assertFalse(accum.flushInProgress(), "flushInProgress count should be decremented even if thread is interrupted");
        }
    }

    @Test
    public void testAbortIncompleteBatches() throws Exception {
        int lingerMs = Integer.MAX_VALUE;
        int numRecords = 100;

        final AtomicInteger numExceptionReceivedInCallback = new AtomicInteger(0);
        final RecordAccumulator accum = createTestRecordAccumulator(
            128 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionConfig.NONE, lingerMs);
        class TestCallback implements Callback {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                assertTrue(exception.getMessage().equals("Producer is closed forcefully."));
                numExceptionReceivedInCallback.incrementAndGet();
            }
        }
        for (int i = 0; i < numRecords; i++)
            accum.append(new TopicPartition(topic, i % 3), 0L, key, value, null, new TestCallback(), maxBlockTimeMs, false, time.milliseconds());
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertFalse(result.readyNodes.isEmpty());
        Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertTrue(accum.hasUndrained());
        assertTrue(accum.hasIncomplete());

        int numDrainedRecords = 0;
        for (Map.Entry<Integer, List<ProducerBatch>> drainedEntry : drained.entrySet()) {
            for (ProducerBatch batch : drainedEntry.getValue()) {
                assertTrue(batch.isClosed());
                assertFalse(batch.produceFuture.completed());
                numDrainedRecords += batch.recordCount;
            }
        }

        assertTrue(numDrainedRecords > 0 && numDrainedRecords < numRecords);
        accum.abortIncompleteBatches();
        assertEquals(numRecords, numExceptionReceivedInCallback.get());
        assertFalse(accum.hasUndrained());
        assertFalse(accum.hasIncomplete());
    }

    @Test
    public void testAbortUnsentBatches() throws Exception {
        int lingerMs = Integer.MAX_VALUE;
        int numRecords = 100;

        final AtomicInteger numExceptionReceivedInCallback = new AtomicInteger(0);
        final RecordAccumulator accum = createTestRecordAccumulator(
                128 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionConfig.NONE, lingerMs);
        final KafkaException cause = new KafkaException();

        class TestCallback implements Callback {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                assertEquals(cause, exception);
                numExceptionReceivedInCallback.incrementAndGet();
            }
        }
        for (int i = 0; i < numRecords; i++)
            accum.append(new TopicPartition(topic, i % 3), 0L, key, value, null, new TestCallback(), maxBlockTimeMs, false, time.milliseconds());
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertFalse(result.readyNodes.isEmpty());
        Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE,
                time.milliseconds());
        assertTrue(accum.hasUndrained());
        assertTrue(accum.hasIncomplete());

        accum.abortUndrainedBatches(cause);
        int numDrainedRecords = 0;
        for (Map.Entry<Integer, List<ProducerBatch>> drainedEntry : drained.entrySet()) {
            for (ProducerBatch batch : drainedEntry.getValue()) {
                assertTrue(batch.isClosed());
                assertFalse(batch.produceFuture.completed());
                numDrainedRecords += batch.recordCount;
            }
        }

        assertTrue(numDrainedRecords > 0);
        assertTrue(numExceptionReceivedInCallback.get() > 0);
        assertEquals(numRecords, numExceptionReceivedInCallback.get() + numDrainedRecords);
        assertFalse(accum.hasUndrained());
        assertTrue(accum.hasIncomplete());
    }

    private void doExpireBatchSingle(int deliveryTimeoutMs) throws InterruptedException {
        int lingerMs = 300;
        List<Boolean> muteStates = Arrays.asList(false, true);
        Set<Node> readyNodes = null;
        List<ProducerBatch> expiredBatches = new ArrayList<>();
        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;
        RecordAccumulator accum = createTestRecordAccumulator(deliveryTimeoutMs,
            batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionConfig.NONE, lingerMs);

        // Make the batches ready due to linger. These batches are not in retry
        for (Boolean mute: muteStates) {
            if (time.milliseconds() < System.currentTimeMillis())
                time.setCurrentTimeMs(System.currentTimeMillis());
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            assertEquals(0, accum.ready(cluster, time.milliseconds()).readyNodes.size(), "No partition should be ready.");

            time.sleep(lingerMs);
            readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
            assertEquals(Collections.singleton(node1), readyNodes, "Our partition's leader should be ready");

            expiredBatches = accum.expiredBatches(time.milliseconds());
            assertEquals(0, expiredBatches.size(), "The batch should not expire when just linger has passed");

            if (mute)
                accum.mutePartition(tp1);
            else
                accum.unmutePartition(tp1);

            // Advance the clock to expire the batch.
            time.sleep(deliveryTimeoutMs - lingerMs);
            expiredBatches = accum.expiredBatches(time.milliseconds());
            assertEquals(1, expiredBatches.size(), "The batch may expire when the partition is muted");
            assertEquals(0, accum.ready(cluster, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");
        }
    }

    @Test
    public void testExpiredBatchSingle() throws InterruptedException {
        doExpireBatchSingle(3200);
    }

    @Test
    public void testExpiredBatchSingleMaxValue() throws InterruptedException {
        doExpireBatchSingle(Integer.MAX_VALUE);
    }

    @Test
    public void testExpiredBatches() throws InterruptedException {
        long retryBackoffMs = 100L;
        int lingerMs = 30;
        int requestTimeout = 60;
        int deliveryTimeoutMs = 3200;

        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;

        RecordAccumulator accum = createTestRecordAccumulator(
            deliveryTimeoutMs, batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionConfig.NONE, lingerMs);
        int appends = expectedNumAppends(batchSize);

        // Test batches not in retry
        for (int i = 0; i < appends; i++) {
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            assertEquals(0, accum.ready(cluster, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");
        }
        // Make the batches ready due to batch full
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
        Set<Node> readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        assertEquals(Collections.singleton(node1), readyNodes, "Our partition's leader should be ready");
        // Advance the clock to expire the batch.
        time.sleep(deliveryTimeoutMs + 1);
        accum.mutePartition(tp1);
        List<ProducerBatch> expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(2, expiredBatches.size(), "The batches will be muted no matter if the partition is muted or not");

        accum.unmutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "All batches should have been expired earlier");
        assertEquals(0, accum.ready(cluster, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");

        // Advance the clock to make the next batch ready due to linger.ms
        time.sleep(lingerMs);
        assertEquals(Collections.singleton(node1), readyNodes, "Our partition's leader should be ready");
        time.sleep(requestTimeout + 1);

        accum.mutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "The batch should not be expired when metadata is still available and partition is muted");

        accum.unmutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "All batches should have been expired");
        assertEquals(0, accum.ready(cluster, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");

        // Test batches in retry.
        // Create a retried batch
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
        time.sleep(lingerMs);
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        assertEquals(Collections.singleton(node1), readyNodes, "Our partition's leader should be ready");
        Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals(drained.get(node1.id()).size(), 1, "There should be only one batch.");
        time.sleep(1000L);
        accum.reenqueue(drained.get(node1.id()).get(0), time.milliseconds());

        // test expiration.
        time.sleep(requestTimeout + retryBackoffMs);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "The batch should not be expired.");
        time.sleep(1L);

        accum.mutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "The batch should not be expired when the partition is muted");

        accum.unmutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "All batches should have been expired.");

        // Test that when being throttled muted batches are expired before the throttle time is over.
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
        time.sleep(lingerMs);
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        assertEquals(Collections.singleton(node1), readyNodes, "Our partition's leader should be ready");
        // Advance the clock to expire the batch.
        time.sleep(requestTimeout + 1);
        accum.mutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "The batch should not be expired when the partition is muted");

        long throttleTimeMs = 100L;
        accum.unmutePartition(tp1);
        // The batch shouldn't be expired yet.
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "The batch should not be expired when the partition is muted");

        // Once the throttle time is over, the batch can be expired.
        time.sleep(throttleTimeMs);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "All batches should have been expired earlier");
        assertEquals(1, accum.ready(cluster, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");
    }

    @Test
    public void testMutedPartitions() throws InterruptedException {
        long now = time.milliseconds();
        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;

        RecordAccumulator accum = createTestRecordAccumulator(
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionConfig.NONE, 10);
        int appends = expectedNumAppends(batchSize);
        for (int i = 0; i < appends; i++) {
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            assertEquals(0, accum.ready(cluster, now).readyNodes.size(), "No partitions should be ready.");
        }
        time.sleep(2000);

        // Test ready with muted partition
        accum.mutePartition(tp1);
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals(0, result.readyNodes.size(), "No node should be ready");

        // Test ready without muted partition
        accum.unmutePartition(tp1);
        result = accum.ready(cluster, time.milliseconds());
        assertTrue(result.readyNodes.size() > 0, "The batch should be ready");

        // Test drain with muted partition
        accum.mutePartition(tp1);
        Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals(0, drained.get(node1.id()).size(), "No batch should have been drained");

        // Test drain without muted partition.
        accum.unmutePartition(tp1);
        drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertTrue(drained.get(node1.id()).size() > 0, "The batch should have been drained.");
    }

    @Test
    public void testIdempotenceWithOldMagic() {
        // Simulate talking to an older broker, ie. one which supports a lower magic.
        ApiVersions apiVersions = new ApiVersions();
        int batchSize = 1025;
        int deliveryTimeoutMs = 3200;
        int lingerMs = 10;
        long retryBackoffMs = 100L;
        long totalSize = 10 * batchSize;
        String metricGrpName = "producer-metrics";

        apiVersions.update("foobar", NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 0, (short) 2));
        TransactionManager transactionManager = new TransactionManager(new LogContext(), null, 0, retryBackoffMs, apiVersions);
        RecordAccumulator accum = new RecordAccumulator(logContext, batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            CompressionConfig.NONE, lingerMs, retryBackoffMs, deliveryTimeoutMs, metrics, metricGrpName, time, apiVersions, transactionManager,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));
        assertThrows(UnsupportedVersionException.class,
            () -> accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds()));
    }

    @Test
    public void testRecordsDrainedWhenTransactionCompleting() throws Exception {
        int batchSize = 1025;
        int deliveryTimeoutMs = 3200;
        int lingerMs = 10;
        long totalSize = 10 * batchSize;

        TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
        RecordAccumulator accumulator = createTestRecordAccumulator(transactionManager, deliveryTimeoutMs,
            batchSize, totalSize, CompressionConfig.NONE, lingerMs);

        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(12345L, (short) 5);
        Mockito.when(transactionManager.producerIdAndEpoch()).thenReturn(producerIdAndEpoch);
        Mockito.when(transactionManager.isSendToPartitionAllowed(tp1)).thenReturn(true);
        Mockito.when(transactionManager.isPartitionAdded(tp1)).thenReturn(true);
        Mockito.when(transactionManager.firstInFlightSequence(tp1)).thenReturn(0);

        // Initially, the transaction is still in progress, so we should respect the linger.
        Mockito.when(transactionManager.isCompleting()).thenReturn(false);

        accumulator.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs,
            false, time.milliseconds());
        accumulator.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs,
            false, time.milliseconds());
        assertTrue(accumulator.hasUndrained());

        RecordAccumulator.ReadyCheckResult firstResult = accumulator.ready(cluster, time.milliseconds());
        assertEquals(0, firstResult.readyNodes.size());
        Map<Integer, List<ProducerBatch>> firstDrained = accumulator.drain(cluster, firstResult.readyNodes,
            Integer.MAX_VALUE, time.milliseconds());
        assertEquals(0, firstDrained.size());

        // Once the transaction begins completion, then the batch should be drained immediately.
        Mockito.when(transactionManager.isCompleting()).thenReturn(true);

        RecordAccumulator.ReadyCheckResult secondResult = accumulator.ready(cluster, time.milliseconds());
        assertEquals(1, secondResult.readyNodes.size());
        Node readyNode = secondResult.readyNodes.iterator().next();

        Map<Integer, List<ProducerBatch>> secondDrained = accumulator.drain(cluster, secondResult.readyNodes,
            Integer.MAX_VALUE, time.milliseconds());
        assertEquals(Collections.singleton(readyNode.id()), secondDrained.keySet());
        List<ProducerBatch> batches = secondDrained.get(readyNode.id());
        assertEquals(1, batches.size());
    }

    @Test
    public void testSplitAndReenqueue() throws ExecutionException, InterruptedException {
        long now = time.milliseconds();
        RecordAccumulator accum = createTestRecordAccumulator(1024, 10 * 1024, CompressionConfig.gzip().build(), 10);

        // Create a big batch
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, CompressionConfig.NONE, TimestampType.CREATE_TIME, 0L);
        ProducerBatch batch = new ProducerBatch(tp1, builder, now, true);

        byte[] value = new byte[1024];
        final AtomicInteger acked = new AtomicInteger(0);
        Callback cb = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                acked.incrementAndGet();
            }
        };
        // Append two messages so the batch is too big.
        Future<RecordMetadata> future1 = batch.tryAppend(now, null, value, Record.EMPTY_HEADERS, cb, now);
        Future<RecordMetadata> future2 = batch.tryAppend(now, null, value, Record.EMPTY_HEADERS, cb, now);
        assertNotNull(future1);
        assertNotNull(future2);
        batch.close();
        // Enqueue the batch to the accumulator as if the batch was created by the accumulator.
        accum.reenqueue(batch, now);
        time.sleep(101L);
        // Drain the batch.
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertTrue(result.readyNodes.size() > 0, "The batch should be ready");
        Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals(1, drained.size(), "Only node1 should be drained");
        assertEquals(1, drained.get(node1.id()).size(), "Only one batch should be drained");
        // Split and reenqueue the batch.
        accum.splitAndReenqueue(drained.get(node1.id()).get(0));
        time.sleep(101L);

        drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertFalse(drained.isEmpty());
        assertFalse(drained.get(node1.id()).isEmpty());
        drained.get(node1.id()).get(0).complete(acked.get(), 100L);
        assertEquals(1, acked.get(), "The first message should have been acked.");
        assertTrue(future1.isDone());
        assertEquals(0, future1.get().offset());

        drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertFalse(drained.isEmpty());
        assertFalse(drained.get(node1.id()).isEmpty());
        drained.get(node1.id()).get(0).complete(acked.get(), 100L);
        assertEquals(2, acked.get(), "Both message should have been acked.");
        assertTrue(future2.isDone());
        assertEquals(1, future2.get().offset());
    }

    @Test
    public void testSplitBatchOffAccumulator() throws InterruptedException {
        long seed = System.currentTimeMillis();
        final int batchSize = 1024;
        final int bufferCapacity = 3 * 1024;

        // First set the compression ratio estimation to be good.
        CompressionRatioEstimator.setEstimation(tp1.topic(), CompressionType.GZIP, 0.1f);
        RecordAccumulator accum = createTestRecordAccumulator(batchSize, bufferCapacity, CompressionConfig.gzip().build(), 0);
        int numSplitBatches = prepareSplitBatches(accum, seed, 100, 20);
        assertTrue(numSplitBatches > 0, "There should be some split batches");
        // Drain all the split batches.
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        for (int i = 0; i < numSplitBatches; i++) {
            Map<Integer, List<ProducerBatch>> drained =
                accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
            assertFalse(drained.isEmpty());
            assertFalse(drained.get(node1.id()).isEmpty());
        }
        assertTrue(accum.ready(cluster, time.milliseconds()).readyNodes.isEmpty(), "All the batches should have been drained.");
        assertEquals(bufferCapacity, accum.bufferPoolAvailableMemory(),
            "The split batches should be allocated off the accumulator");
    }

    @Test
    public void testSplitFrequency() throws InterruptedException {
        long seed = System.currentTimeMillis();
        Random random = new Random();
        random.setSeed(seed);
        final int batchSize = 1024;
        final int numMessages = 1000;

        RecordAccumulator accum = createTestRecordAccumulator(batchSize, 3 * 1024, CompressionConfig.gzip().build(), 10);
        // Adjust the high and low compression ratio message percentage
        for (int goodCompRatioPercentage = 1; goodCompRatioPercentage < 100; goodCompRatioPercentage++) {
            int numSplit = 0;
            int numBatches = 0;
            CompressionRatioEstimator.resetEstimation(topic);
            for (int i = 0; i < numMessages; i++) {
                int dice = random.nextInt(100);
                byte[] value = (dice < goodCompRatioPercentage) ?
                        bytesWithGoodCompression(random) : bytesWithPoorCompression(random, 100);
                accum.append(tp1, 0L, null, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
                BatchDrainedResult result = completeOrSplitBatches(accum, batchSize);
                numSplit += result.numSplit;
                numBatches += result.numBatches;
            }
            time.sleep(10);
            BatchDrainedResult result = completeOrSplitBatches(accum, batchSize);
            numSplit += result.numSplit;
            numBatches += result.numBatches;
            assertTrue((double) numSplit / numBatches < 0.1f, String.format("Total num batches = %d, split batches = %d, more than 10%% of the batch splits. "
                    + "Random seed is " + seed,
                numBatches, numSplit));
        }
    }

    @Test
    public void testSoonToExpireBatchesArePickedUpForExpiry() throws InterruptedException {
        int lingerMs = 500;
        int batchSize = 1025;

        RecordAccumulator accum = createTestRecordAccumulator(
            batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionConfig.NONE, lingerMs);

        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        Set<Node> readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertTrue(drained.isEmpty());
        //assertTrue(accum.soonToExpireInFlightBatches().isEmpty());

        // advanced clock and send one batch out but it should not be included in soon to expire inflight
        // batches because batch's expiry is quite far.
        time.sleep(lingerMs + 1);
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        drained = accum.drain(cluster, readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals(1, drained.size(), "A batch did not drain after linger");
        //assertTrue(accum.soonToExpireInFlightBatches().isEmpty());

        // Queue another batch and advance clock such that batch expiry time is earlier than request timeout.
        accum.append(tp2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        time.sleep(lingerMs * 4);

        // Now drain and check that accumulator picked up the drained batch because its expiry is soon.
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        drained = accum.drain(cluster, readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals(1, drained.size(), "A batch did not drain after linger");
    }

    @Test
    public void testExpiredBatchesRetry() throws InterruptedException {
        int lingerMs = 3000;
        int rtt = 1000;
        int deliveryTimeoutMs = 3200;
        Set<Node> readyNodes;
        List<ProducerBatch> expiredBatches;
        List<Boolean> muteStates = Arrays.asList(false, true);

        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;
        RecordAccumulator accum = createTestRecordAccumulator(
            batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionConfig.NONE, lingerMs);

        // Test batches in retry.
        for (Boolean mute : muteStates) {
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
            time.sleep(lingerMs);
            readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
            assertEquals(Collections.singleton(node1), readyNodes, "Our partition's leader should be ready");
            Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, readyNodes, Integer.MAX_VALUE, time.milliseconds());
            assertEquals(1, drained.get(node1.id()).size(), "There should be only one batch.");
            time.sleep(rtt);
            accum.reenqueue(drained.get(node1.id()).get(0), time.milliseconds());

            if (mute)
                accum.mutePartition(tp1);
            else
                accum.unmutePartition(tp1);

            // test expiration
            time.sleep(deliveryTimeoutMs - rtt);
            accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE, time.milliseconds());
            expiredBatches = accum.expiredBatches(time.milliseconds());
            assertEquals(mute ? 1 : 0, expiredBatches.size(), "RecordAccumulator has expired batches if the partition is not muted");
        }
    }

    @Test
    public void testStickyBatches() throws Exception {
        long now = time.milliseconds();

        // Test case assumes that the records do not fill the batch completely
        int batchSize = 1025;

        Partitioner partitioner = new DefaultPartitioner();
        RecordAccumulator accum = createTestRecordAccumulator(3200,
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10L * batchSize, CompressionConfig.NONE, 10);
        int expectedAppends = expectedNumAppendsNoKey(batchSize);

        // Create first batch
        int partition = partitioner.partition(topic, null, null, "value", value, cluster);
        TopicPartition tp = new TopicPartition(topic, partition);
        accum.append(tp, 0L, null, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        int appends = 1;

        boolean switchPartition = false;
        while (!switchPartition) {
            // Append to the first batch
            partition = partitioner.partition(topic, null, null, "value", value, cluster);
            tp = new TopicPartition(topic, partition);
            RecordAccumulator.RecordAppendResult result = accum.append(tp, 0L, null, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, true, time.milliseconds());
            Deque<ProducerBatch> partitionBatches1 = accum.batches().get(tp1);
            Deque<ProducerBatch> partitionBatches2 = accum.batches().get(tp2);
            Deque<ProducerBatch> partitionBatches3 = accum.batches().get(tp3);
            int numBatches = (partitionBatches1 == null ? 0 : partitionBatches1.size()) + (partitionBatches2 == null ? 0 : partitionBatches2.size()) + (partitionBatches3 == null ? 0 : partitionBatches3.size());
            // Only one batch is created because the partition is sticky.
            assertEquals(1, numBatches);

            switchPartition = result.abortForNewBatch;
            // We only appended if we do not retry.
            if (!switchPartition) {
                appends++;
                assertEquals(0, accum.ready(cluster, now).readyNodes.size(), "No partitions should be ready.");
            }
        }

        // Batch should be full.
        assertEquals(1, accum.ready(cluster, time.milliseconds()).readyNodes.size());
        assertEquals(appends, expectedAppends);
        switchPartition = false;

        // KafkaProducer would call this method in this case, make second batch
        partitioner.onNewBatch(topic, cluster, partition);
        partition = partitioner.partition(topic, null, null, "value", value, cluster);
        tp = new TopicPartition(topic, partition);
        accum.append(tp, 0L, null, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        appends++;

        // These appends all go into the second batch
        while (!switchPartition) {
            partition = partitioner.partition(topic, null, null, "value", value, cluster);
            tp = new TopicPartition(topic, partition);
            RecordAccumulator.RecordAppendResult result = accum.append(tp, 0L, null, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, true, time.milliseconds());
            Deque<ProducerBatch> partitionBatches1 = accum.batches().get(tp1);
            Deque<ProducerBatch> partitionBatches2 = accum.batches().get(tp2);
            Deque<ProducerBatch> partitionBatches3 = accum.batches().get(tp3);
            int numBatches = (partitionBatches1 == null ? 0 : partitionBatches1.size()) + (partitionBatches2 == null ? 0 : partitionBatches2.size()) + (partitionBatches3 == null ? 0 : partitionBatches3.size());
            // Only two batches because the new partition is also sticky.
            assertEquals(2, numBatches);

            switchPartition = result.abortForNewBatch;
            // We only appended if we do not retry.
            if (!switchPartition) {
                appends++;
            }
        }

        // There should be two full batches now.
        assertEquals(appends, 2 * expectedAppends);
    }

    private int prepareSplitBatches(RecordAccumulator accum, long seed, int recordSize, int numRecords)
        throws InterruptedException {
        Random random = new Random();
        random.setSeed(seed);

        // First set the compression ratio estimation to be good.
        CompressionRatioEstimator.setEstimation(tp1.topic(), CompressionType.GZIP, 0.1f);
        // Append 20 records of 100 bytes size with poor compression ratio should make the batch too big.
        for (int i = 0; i < numRecords; i++) {
            accum.append(tp1, 0L, null, bytesWithPoorCompression(random, recordSize), Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
        }

        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertFalse(result.readyNodes.isEmpty());
        Map<Integer, List<ProducerBatch>> batches = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals(1, batches.size());
        assertEquals(1, batches.values().iterator().next().size());
        ProducerBatch batch = batches.values().iterator().next().get(0);
        int numSplitBatches = accum.splitAndReenqueue(batch);
        accum.deallocate(batch);

        return numSplitBatches;
    }

    private BatchDrainedResult completeOrSplitBatches(RecordAccumulator accum, int batchSize) {
        int numSplit = 0;
        int numBatches = 0;
        boolean batchDrained;
        do {
            batchDrained = false;
            RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
            Map<Integer, List<ProducerBatch>> batches = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
            for (List<ProducerBatch> batchList : batches.values()) {
                for (ProducerBatch batch : batchList) {
                    batchDrained = true;
                    numBatches++;
                    if (batch.estimatedSizeInBytes() > batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD) {
                        accum.splitAndReenqueue(batch);
                        // release the resource of the original big batch.
                        numSplit++;
                    } else {
                        batch.complete(0L, 0L);
                    }
                    accum.deallocate(batch);
                }
            }
        } while (batchDrained);
        return new BatchDrainedResult(numSplit, numBatches);
    }

    /**
     * Generates the compression ratio at about 0.6
     */
    private byte[] bytesWithGoodCompression(Random random) {
        byte[] value = new byte[100];
        ByteBuffer buffer = ByteBuffer.wrap(value);
        while (buffer.remaining() > 0)
            buffer.putInt(random.nextInt(1000));
        return value;
    }

    /**
     * Generates the compression ratio at about 0.9
     */
    private byte[] bytesWithPoorCompression(Random random, int size) {
        byte[] value = new byte[size];
        random.nextBytes(value);
        return value;
    }

    private class BatchDrainedResult {
        final int numSplit;
        final int numBatches;
        BatchDrainedResult(int numSplit, int numBatches) {
            this.numBatches = numBatches;
            this.numSplit = numSplit;
        }
    }

    /**
     * Return the offset delta.
     */
    private int expectedNumAppends(int batchSize) {
        int size = 0;
        int offsetDelta = 0;
        while (true) {
            int recordSize = DefaultRecord.sizeInBytes(offsetDelta, 0, key.length, value.length,
                Record.EMPTY_HEADERS);
            if (size + recordSize > batchSize)
                return offsetDelta;
            offsetDelta += 1;
            size += recordSize;
        }
    }

     /**
     * Return the offset delta when there is no key.
     */
    private int expectedNumAppendsNoKey(int batchSize) {
        int size = 0;
        int offsetDelta = 0;
        while (true) {
            int recordSize = DefaultRecord.sizeInBytes(offsetDelta, 0, 0, value.length,
                Record.EMPTY_HEADERS);
            if (size + recordSize > batchSize)
                return offsetDelta;
            offsetDelta += 1;
            size += recordSize;
        }
    }

    private RecordAccumulator createTestRecordAccumulator(int batchSize, long totalSize, CompressionConfig compressionConfig, int lingerMs) {
        int deliveryTimeoutMs = 3200;
        return createTestRecordAccumulator(deliveryTimeoutMs, batchSize, totalSize, compressionConfig, lingerMs);
    }

    private RecordAccumulator createTestRecordAccumulator(int deliveryTimeoutMs, int batchSize, long totalSize, CompressionConfig compressionConfig, int lingerMs) {
        return createTestRecordAccumulator(null, deliveryTimeoutMs, batchSize, totalSize, compressionConfig, lingerMs);
    }

    /**
     * Return a test RecordAccumulator instance
     */
    private RecordAccumulator createTestRecordAccumulator(TransactionManager txnManager, int deliveryTimeoutMs, int batchSize, long totalSize, CompressionConfig compressionConfig, int lingerMs) {
        long retryBackoffMs = 100L;
        String metricGrpName = "producer-metrics";

        return new RecordAccumulator(
            logContext,
            batchSize,
            compressionConfig,
            lingerMs,
            retryBackoffMs,
            deliveryTimeoutMs,
            metrics,
            metricGrpName,
            time,
            new ApiVersions(),
            txnManager,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));
    }
}
