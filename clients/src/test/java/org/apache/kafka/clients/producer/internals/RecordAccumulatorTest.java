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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    @After
    public void teardown() {
        this.metrics.close();
    }

    @Test
    public void testFull() throws Exception {
        long now = time.milliseconds();

        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;

        RecordAccumulator accum = createTestRecordAccumulator(
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10L * batchSize, CompressionType.NONE, 10);
        int appends = expectedNumAppends(batchSize);
        for (int i = 0; i < appends; i++) {
            // append to the first batch
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            Deque<ProducerBatch> partitionBatches = accum.batches().get(tp1);
            assertEquals(1, partitionBatches.size());

            ProducerBatch batch = partitionBatches.peekFirst();
            assertTrue(batch.isWritable());
            assertEquals("No partitions should be ready.", 0, accum.ready(cluster, now).readyNodes.size());
        }

        // this append doesn't fit in the first batch, so a new batch is created and the first batch is closed

        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        Deque<ProducerBatch> partitionBatches = accum.batches().get(tp1);
        assertEquals(2, partitionBatches.size());
        Iterator<ProducerBatch> partitionBatchesIterator = partitionBatches.iterator();
        assertTrue(partitionBatchesIterator.next().isWritable());
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);

        List<ProducerBatch> batches = accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
        assertEquals(1, batches.size());
        ProducerBatch batch = batches.get(0);

        Iterator<Record> iter = batch.records().records().iterator();
        for (int i = 0; i < appends; i++) {
            Record record = iter.next();
            assertEquals("Keys should match", ByteBuffer.wrap(key), record.key());
            assertEquals("Values should match", ByteBuffer.wrap(value), record.value());
        }
        assertFalse("No more records", iter.hasNext());
    }

    @Test
    public void testAppendLargeCompressed() throws Exception {
        testAppendLarge(CompressionType.GZIP);
    }

    @Test
    public void testAppendLargeNonCompressed() throws Exception {
        testAppendLarge(CompressionType.NONE);
    }

    private void testAppendLarge(CompressionType compressionType) throws Exception {
        int batchSize = 512;
        byte[] value = new byte[2 * batchSize];
        RecordAccumulator accum = createTestRecordAccumulator(
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, compressionType, 0);
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);

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
        testAppendLargeOldMessageFormat(CompressionType.GZIP);
    }

    @Test
    public void testAppendLargeOldMessageFormatNonCompressed() throws Exception {
        testAppendLargeOldMessageFormat(CompressionType.NONE);
    }

    private void testAppendLargeOldMessageFormat(CompressionType compressionType) throws Exception {
        int batchSize = 512;
        byte[] value = new byte[2 * batchSize];

        ApiVersions apiVersions = new ApiVersions();
        apiVersions.update(node1.idString(), NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 0, (short) 2));

        RecordAccumulator accum = createTestRecordAccumulator(
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, compressionType, 0);
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);

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
                1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, CompressionType.NONE, lingerMs);
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        assertEquals("No partitions should be ready", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());
        time.sleep(10);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);
        List<ProducerBatch> batches = accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
        assertEquals(1, batches.size());
        ProducerBatch batch = batches.get(0);

        Iterator<Record> iter = batch.records().records().iterator();
        Record record = iter.next();
        assertEquals("Keys should match", ByteBuffer.wrap(key), record.key());
        assertEquals("Values should match", ByteBuffer.wrap(value), record.value());
        assertFalse("No more records", iter.hasNext());
    }

    @Test
    public void testPartialDrain() throws Exception {
        RecordAccumulator accum = createTestRecordAccumulator(
                1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, CompressionType.NONE, 10);
        int appends = 1024 / msgSize + 1;
        List<TopicPartition> partitions = asList(tp1, tp2);
        for (TopicPartition tp : partitions) {
            for (int i = 0; i < appends; i++)
                accum.append(tp, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        }
        assertEquals("Partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);

        List<ProducerBatch> batches = accum.drain(cluster, Collections.singleton(node1), 1024, 0).get(node1.id());
        assertEquals("But due to size bound only one partition should have been retrieved", 1, batches.size());
    }

    @SuppressWarnings("unused")
    @Test
    public void testStressfulSituation() throws Exception {
        final int numThreads = 5;
        final int msgs = 10000;
        final int numParts = 2;
        final RecordAccumulator accum = createTestRecordAccumulator(
            1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, CompressionType.NONE, 0);
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
                10 * batchSize, CompressionType.NONE, lingerMs);
        // Just short of going over the limit so we trigger linger time
        int appends = expectedNumAppends(batchSize);

        // Partition on node1 only
        for (int i = 0; i < appends; i++)
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals("No nodes should be ready.", 0, result.readyNodes.size());
        assertEquals("Next check time should be the linger time", lingerMs, result.nextReadyCheckDelayMs);

        time.sleep(lingerMs / 2);

        // Add partition on node2 only
        for (int i = 0; i < appends; i++)
            accum.append(tp3, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        result = accum.ready(cluster, time.milliseconds());
        assertEquals("No nodes should be ready.", 0, result.readyNodes.size());
        assertEquals("Next check time should be defined by node1, half remaining linger time", lingerMs / 2, result.nextReadyCheckDelayMs);

        // Add data for another partition on node1, enough to make data sendable immediately
        for (int i = 0; i < appends + 1; i++)
            accum.append(tp2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        result = accum.ready(cluster, time.milliseconds());
        assertEquals("Node1 should be ready", Collections.singleton(node1), result.readyNodes);
        // Note this can actually be < linger time because it may use delays from partitions that aren't sendable
        // but have leaders with other sendable data.
        assertTrue("Next check time should be defined by node2, at most linger time", result.nextReadyCheckDelayMs <= lingerMs);
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
            CompressionType.NONE, lingerMs, retryBackoffMs, deliveryTimeoutMs, metrics, metricGrpName, time, new ApiVersions(), null,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));

        long now = time.milliseconds();
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, now + lingerMs + 1);
        assertEquals("Node1 should be ready", Collections.singleton(node1), result.readyNodes);
        Map<Integer, List<ProducerBatch>> batches = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, now + lingerMs + 1);
        assertEquals("Node1 should be the only ready node.", 1, batches.size());
        assertEquals("Partition 0 should only have one batch drained.", 1, batches.get(0).size());

        // Reenqueue the batch
        now = time.milliseconds();
        accum.reenqueue(batches.get(0).get(0), now);

        // Put message for partition 1 into accumulator
        accum.append(tp2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        result = accum.ready(cluster, now + lingerMs + 1);
        assertEquals("Node1 should be ready", Collections.singleton(node1), result.readyNodes);

        // tp1 should backoff while tp2 should not
        batches = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, now + lingerMs + 1);
        assertEquals("Node1 should be the only ready node.", 1, batches.size());
        assertEquals("Node1 should only have one batch drained.", 1, batches.get(0).size());
        assertEquals("Node1 should only have one batch for partition 1.", tp2, batches.get(0).get(0).topicPartition);

        // Partition 0 can be drained after retry backoff
        result = accum.ready(cluster, now + retryBackoffMs + 1);
        assertEquals("Node1 should be ready", Collections.singleton(node1), result.readyNodes);
        batches = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, now + retryBackoffMs + 1);
        assertEquals("Node1 should be the only ready node.", 1, batches.size());
        assertEquals("Node1 should only have one batch drained.", 1, batches.get(0).size());
        assertEquals("Node1 should only have one batch for partition 0.", tp1, batches.get(0).get(0).topicPartition);
    }

    @Test
    public void testFlush() throws Exception {
        int lingerMs = Integer.MAX_VALUE;
        final RecordAccumulator accum = createTestRecordAccumulator(
                4 * 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionType.NONE, lingerMs);

        for (int i = 0; i < 100; i++) {
            accum.append(new TopicPartition(topic, i % 3), 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            assertTrue(accum.hasIncomplete());
        }
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals("No nodes should be ready.", 0, result.readyNodes.size());

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
            4 * 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionType.NONE, Integer.MAX_VALUE);
        accum.append(new TopicPartition(topic, 0), 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());

        accum.beginFlush();
        assertTrue(accum.flushInProgress());
        delayedInterrupt(Thread.currentThread(), 1000L);
        try {
            accum.awaitFlushCompletion();
            fail("awaitFlushCompletion should throw InterruptException");
        } catch (InterruptedException e) {
            assertFalse("flushInProgress count should be decremented even if thread is interrupted", accum.flushInProgress());
        }
    }

    @Test
    public void testAbortIncompleteBatches() throws Exception {
        int lingerMs = Integer.MAX_VALUE;
        int numRecords = 100;

        final AtomicInteger numExceptionReceivedInCallback = new AtomicInteger(0);
        final RecordAccumulator accum = createTestRecordAccumulator(
            128 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionType.NONE, lingerMs);
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
                128 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionType.NONE, lingerMs);
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
            batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionType.NONE, lingerMs);

        // Make the batches ready due to linger. These batches are not in retry
        for (Boolean mute: muteStates) {
            if (time.milliseconds() < System.currentTimeMillis())
                time.setCurrentTimeMs(System.currentTimeMillis());
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            assertEquals("No partition should be ready.", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());

            time.sleep(lingerMs);
            readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
            assertEquals("Our partition's leader should be ready", Collections.singleton(node1), readyNodes);

            expiredBatches = accum.expiredBatches(time.milliseconds());
            assertEquals("The batch should not expire when just linger has passed", 0, expiredBatches.size());

            if (mute)
                accum.mutePartition(tp1);
            else
                accum.unmutePartition(tp1);

            // Advance the clock to expire the batch.
            time.sleep(deliveryTimeoutMs - lingerMs);
            expiredBatches = accum.expiredBatches(time.milliseconds());
            assertEquals("The batch may expire when the partition is muted", 1, expiredBatches.size());
            assertEquals("No partitions should be ready.", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());
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
            deliveryTimeoutMs, batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionType.NONE, lingerMs);
        int appends = expectedNumAppends(batchSize);

        // Test batches not in retry
        for (int i = 0; i < appends; i++) {
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            assertEquals("No partitions should be ready.", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());
        }
        // Make the batches ready due to batch full
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
        Set<Node> readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), readyNodes);
        // Advance the clock to expire the batch.
        time.sleep(deliveryTimeoutMs + 1);
        accum.mutePartition(tp1);
        List<ProducerBatch> expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("The batches will be muted no matter if the partition is muted or not", 2, expiredBatches.size());

        accum.unmutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("All batches should have been expired earlier", 0, expiredBatches.size());
        assertEquals("No partitions should be ready.", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());

        // Advance the clock to make the next batch ready due to linger.ms
        time.sleep(lingerMs);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), readyNodes);
        time.sleep(requestTimeout + 1);

        accum.mutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("The batch should not be expired when metadata is still available and partition is muted", 0, expiredBatches.size());

        accum.unmutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("All batches should have been expired", 0, expiredBatches.size());
        assertEquals("No partitions should be ready.", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());

        // Test batches in retry.
        // Create a retried batch
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
        time.sleep(lingerMs);
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), readyNodes);
        Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals("There should be only one batch.", drained.get(node1.id()).size(), 1);
        time.sleep(1000L);
        accum.reenqueue(drained.get(node1.id()).get(0), time.milliseconds());

        // test expiration.
        time.sleep(requestTimeout + retryBackoffMs);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("The batch should not be expired.", 0, expiredBatches.size());
        time.sleep(1L);

        accum.mutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("The batch should not be expired when the partition is muted", 0, expiredBatches.size());

        accum.unmutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("All batches should have been expired.", 0, expiredBatches.size());

        // Test that when being throttled muted batches are expired before the throttle time is over.
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
        time.sleep(lingerMs);
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), readyNodes);
        // Advance the clock to expire the batch.
        time.sleep(requestTimeout + 1);
        accum.mutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("The batch should not be expired when the partition is muted", 0, expiredBatches.size());

        long throttleTimeMs = 100L;
        accum.unmutePartition(tp1);
        // The batch shouldn't be expired yet.
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("The batch should not be expired when the partition is muted", 0, expiredBatches.size());

        // Once the throttle time is over, the batch can be expired.
        time.sleep(throttleTimeMs);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals("All batches should have been expired earlier", 0, expiredBatches.size());
        assertEquals("No partitions should be ready.", 1, accum.ready(cluster, time.milliseconds()).readyNodes.size());
    }

    @Test
    public void testMutedPartitions() throws InterruptedException {
        long now = time.milliseconds();
        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;

        RecordAccumulator accum = createTestRecordAccumulator(
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionType.NONE, 10);
        int appends = expectedNumAppends(batchSize);
        for (int i = 0; i < appends; i++) {
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
            assertEquals("No partitions should be ready.", 0, accum.ready(cluster, now).readyNodes.size());
        }
        time.sleep(2000);

        // Test ready with muted partition
        accum.mutePartition(tp1);
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals("No node should be ready", 0, result.readyNodes.size());

        // Test ready without muted partition
        accum.unmutePartition(tp1);
        result = accum.ready(cluster, time.milliseconds());
        assertTrue("The batch should be ready", result.readyNodes.size() > 0);

        // Test drain with muted partition
        accum.mutePartition(tp1);
        Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals("No batch should have been drained", 0, drained.get(node1.id()).size());

        // Test drain without muted partition.
        accum.unmutePartition(tp1);
        drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertTrue("The batch should have been drained.", drained.get(node1.id()).size() > 0);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testIdempotenceWithOldMagic() throws InterruptedException {
        // Simulate talking to an older broker, ie. one which supports a lower magic.
        ApiVersions apiVersions = new ApiVersions();
        int batchSize = 1025;
        int deliveryTimeoutMs = 3200;
        int lingerMs = 10;
        long retryBackoffMs = 100L;
        long totalSize = 10 * batchSize;
        String metricGrpName = "producer-metrics";

        apiVersions.update("foobar", NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 0, (short) 2));
        TransactionManager transactionManager = new TransactionManager(new LogContext(), null, 0, 100L, new ApiVersions(), false);
        RecordAccumulator accum = new RecordAccumulator(logContext, batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            CompressionType.NONE, lingerMs, retryBackoffMs, deliveryTimeoutMs, metrics, metricGrpName, time, apiVersions, transactionManager,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));
        accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
    }

    @Test
    public void testSplitAndReenqueue() throws ExecutionException, InterruptedException {
        long now = time.milliseconds();
        RecordAccumulator accum = createTestRecordAccumulator(1024, 10 * 1024, CompressionType.GZIP, 10);

        // Create a big batch
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
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
        assertTrue("The batch should be ready", result.readyNodes.size() > 0);
        Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals("Only node1 should be drained", 1, drained.size());
        assertEquals("Only one batch should be drained", 1, drained.get(node1.id()).size());
        // Split and reenqueue the batch.
        accum.splitAndReenqueue(drained.get(node1.id()).get(0));
        time.sleep(101L);

        drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertFalse(drained.isEmpty());
        assertFalse(drained.get(node1.id()).isEmpty());
        drained.get(node1.id()).get(0).done(acked.get(), 100L, null);
        assertEquals("The first message should have been acked.", 1, acked.get());
        assertTrue(future1.isDone());
        assertEquals(0, future1.get().offset());

        drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertFalse(drained.isEmpty());
        assertFalse(drained.get(node1.id()).isEmpty());
        drained.get(node1.id()).get(0).done(acked.get(), 100L, null);
        assertEquals("Both message should have been acked.", 2, acked.get());
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
        RecordAccumulator accum = createTestRecordAccumulator(batchSize, bufferCapacity, CompressionType.GZIP, 0);
        int numSplitBatches = prepareSplitBatches(accum, seed, 100, 20);
        assertTrue("There should be some split batches", numSplitBatches > 0);
        // Drain all the split batches.
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        for (int i = 0; i < numSplitBatches; i++) {
            Map<Integer, List<ProducerBatch>> drained =
                accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
            assertFalse(drained.isEmpty());
            assertFalse(drained.get(node1.id()).isEmpty());
        }
        assertTrue("All the batches should have been drained.",
                accum.ready(cluster, time.milliseconds()).readyNodes.isEmpty());
        assertEquals("The split batches should be allocated off the accumulator",
                bufferCapacity, accum.bufferPoolAvailableMemory());
    }

    @Test
    public void testSplitFrequency() throws InterruptedException {
        long seed = System.currentTimeMillis();
        Random random = new Random();
        random.setSeed(seed);
        final int batchSize = 1024;
        final int numMessages = 1000;

        RecordAccumulator accum = createTestRecordAccumulator(batchSize, 3 * 1024, CompressionType.GZIP, 10);
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
            assertTrue(String.format("Total num batches = %d, split batches = %d, more than 10%% of the batch splits. "
                    + "Random seed is " + seed,
                numBatches, numSplit), (double) numSplit / numBatches < 0.1f);
        }
    }

    @Test
    public void testSoonToExpireBatchesArePickedUpForExpiry() throws InterruptedException {
        int lingerMs = 500;
        int batchSize = 1025;

        RecordAccumulator accum = createTestRecordAccumulator(
            batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionType.NONE, lingerMs);

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
        assertEquals("A batch did not drain after linger", 1, drained.size());
        //assertTrue(accum.soonToExpireInFlightBatches().isEmpty());

        // Queue another batch and advance clock such that batch expiry time is earlier than request timeout.
        accum.append(tp2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds());
        time.sleep(lingerMs * 4);

        // Now drain and check that accumulator picked up the drained batch because its expiry is soon.
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        drained = accum.drain(cluster, readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals("A batch did not drain after linger", 1, drained.size());
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
            batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionType.NONE, lingerMs);

        // Test batches in retry.
        for (Boolean mute : muteStates) {
            accum.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
            time.sleep(lingerMs);
            readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
            assertEquals("Our partition's leader should be ready", Collections.singleton(node1), readyNodes);
            Map<Integer, List<ProducerBatch>> drained = accum.drain(cluster, readyNodes, Integer.MAX_VALUE, time.milliseconds());
            assertEquals("There should be only one batch.", 1, drained.get(node1.id()).size());
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
            assertEquals("RecordAccumulator has expired batches if the partition is not muted", mute ? 1 : 0, expiredBatches.size());
        }
    }

    @Test
    public void testStickyBatches() throws Exception {
        long now = time.milliseconds();

        // Test case assumes that the records do not fill the batch completely
        int batchSize = 1025;

        Partitioner partitioner = new DefaultPartitioner();
        RecordAccumulator accum = createTestRecordAccumulator(3200,
                batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10L * batchSize, CompressionType.NONE, 10);
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
                assertEquals("No partitions should be ready.", 0, accum.ready(cluster, now).readyNodes.size());
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
                        batch.done(0L, 0L, null);
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


    private RecordAccumulator createTestRecordAccumulator(int batchSize, long totalSize, CompressionType type, int lingerMs) {
        int deliveryTimeoutMs = 3200;
        return createTestRecordAccumulator(deliveryTimeoutMs, batchSize, totalSize, type, lingerMs);
    }

    /**
     * Return a test RecordAccumulator instance
     */
    private RecordAccumulator createTestRecordAccumulator(int deliveryTimeoutMs, int batchSize, long totalSize, CompressionType type, int lingerMs) {
        long retryBackoffMs = 100L;
        String metricGrpName = "producer-metrics";

        return new RecordAccumulator(
            logContext,
            batchSize,
            type,
            lingerMs,
            retryBackoffMs,
            deliveryTimeoutMs,
            metrics,
            metricGrpName,
            time,
            new ApiVersions(),
            null,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));
    }
}
