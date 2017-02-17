/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
    private int msgSize = Records.LOG_OVERHEAD + Record.recordSize(key, value);
    private Cluster cluster = new Cluster(null, Arrays.asList(node1, node2), Arrays.asList(part1, part2, part3),
            Collections.<String>emptySet(), Collections.<String>emptySet());
    private Metrics metrics = new Metrics(time);
    private final long maxBlockTimeMs = 1000;

    @After
    public void teardown() {
        this.metrics.close();
    }

    @Test
    public void testFull() throws Exception {
        long now = time.milliseconds();
        int batchSize = 1024;
        RecordAccumulator accum = new RecordAccumulator(batchSize, 10 * batchSize, CompressionType.NONE, 10L, 100L, metrics, time);
        int appends = batchSize / msgSize;
        for (int i = 0; i < appends; i++) {
            // append to the first batch
            accum.append(tp1, 0L, key, value, null, maxBlockTimeMs);
            Deque<RecordBatch> partitionBatches = accum.batches().get(tp1);
            assertEquals(1, partitionBatches.size());
            assertTrue(partitionBatches.peekFirst().isWritable());
            assertEquals("No partitions should be ready.", 0, accum.ready(cluster, now).readyNodes.size());
        }

        // this append doesn't fit in the first batch, so a new batch is created and the first batch is closed
        accum.append(tp1, 0L, key, value, null, maxBlockTimeMs);
        Deque<RecordBatch> partitionBatches = accum.batches().get(tp1);
        assertEquals(2, partitionBatches.size());
        Iterator<RecordBatch> partitionBatchesIterator = partitionBatches.iterator();
        assertFalse(partitionBatchesIterator.next().isWritable());
        assertTrue(partitionBatchesIterator.next().isWritable());
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);

        List<RecordBatch> batches = accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
        assertEquals(1, batches.size());
        RecordBatch batch = batches.get(0);

        Iterator<LogEntry> iter = batch.records().deepEntries().iterator();
        for (int i = 0; i < appends; i++) {
            LogEntry entry = iter.next();
            assertEquals("Keys should match", ByteBuffer.wrap(key), entry.record().key());
            assertEquals("Values should match", ByteBuffer.wrap(value), entry.record().value());
        }
        assertFalse("No more records", iter.hasNext());
    }

    @Test
    public void testAppendLarge() throws Exception {
        int batchSize = 512;
        RecordAccumulator accum = new RecordAccumulator(batchSize, 10 * 1024, CompressionType.NONE, 0L, 100L, metrics, time);
        accum.append(tp1, 0L, key, new byte[2 * batchSize], null, maxBlockTimeMs);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);
    }

    @Test
    public void testLinger() throws Exception {
        long lingerMs = 10L;
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, CompressionType.NONE, lingerMs, 100L, metrics, time);
        accum.append(tp1, 0L, key, value, null, maxBlockTimeMs);
        assertEquals("No partitions should be ready", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());
        time.sleep(10);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);
        List<RecordBatch> batches = accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
        assertEquals(1, batches.size());
        RecordBatch batch = batches.get(0);

        Iterator<LogEntry> iter = batch.records().deepEntries().iterator();
        LogEntry entry = iter.next();
        assertEquals("Keys should match", ByteBuffer.wrap(key), entry.record().key());
        assertEquals("Values should match", ByteBuffer.wrap(value), entry.record().value());
        assertFalse("No more records", iter.hasNext());
    }

    @Test
    public void testPartialDrain() throws Exception {
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, CompressionType.NONE, 10L, 100L, metrics, time);
        int appends = 1024 / msgSize + 1;
        List<TopicPartition> partitions = asList(tp1, tp2);
        for (TopicPartition tp : partitions) {
            for (int i = 0; i < appends; i++)
                accum.append(tp, 0L, key, value, null, maxBlockTimeMs);
        }
        assertEquals("Partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);

        List<RecordBatch> batches = accum.drain(cluster, Collections.singleton(node1), 1024, 0).get(node1.id());
        assertEquals("But due to size bound only one partition should have been retrieved", 1, batches.size());
    }

    @SuppressWarnings("unused")
    @Test
    public void testStressfulSituation() throws Exception {
        final int numThreads = 5;
        final int msgs = 10000;
        final int numParts = 2;
        final RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, CompressionType.NONE, 0L, 100L, metrics, time);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread() {
                public void run() {
                    for (int i = 0; i < msgs; i++) {
                        try {
                            accum.append(new TopicPartition(topic, i % numParts), 0L, key, value, null, maxBlockTimeMs);
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
            List<RecordBatch> batches = accum.drain(cluster, nodes, 5 * 1024, 0).get(node1.id());
            if (batches != null) {
                for (RecordBatch batch : batches) {
                    for (LogEntry entry : batch.records().deepEntries())
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
        long lingerMs = 10L;
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024,  CompressionType.NONE, lingerMs, 100L, metrics, time);
        // Just short of going over the limit so we trigger linger time
        int appends = 1024 / msgSize;

        // Partition on node1 only
        for (int i = 0; i < appends; i++)
            accum.append(tp1, 0L, key, value, null, maxBlockTimeMs);
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals("No nodes should be ready.", 0, result.readyNodes.size());
        assertEquals("Next check time should be the linger time", lingerMs, result.nextReadyCheckDelayMs);

        time.sleep(lingerMs / 2);

        // Add partition on node2 only
        for (int i = 0; i < appends; i++)
            accum.append(tp3, 0L, key, value, null, maxBlockTimeMs);
        result = accum.ready(cluster, time.milliseconds());
        assertEquals("No nodes should be ready.", 0, result.readyNodes.size());
        assertEquals("Next check time should be defined by node1, half remaining linger time", lingerMs / 2, result.nextReadyCheckDelayMs);

        // Add data for another partition on node1, enough to make data sendable immediately
        for (int i = 0; i < appends + 1; i++)
            accum.append(tp2, 0L, key, value, null, maxBlockTimeMs);
        result = accum.ready(cluster, time.milliseconds());
        assertEquals("Node1 should be ready", Collections.singleton(node1), result.readyNodes);
        // Note this can actually be < linger time because it may use delays from partitions that aren't sendable
        // but have leaders with other sendable data.
        assertTrue("Next check time should be defined by node2, at most linger time", result.nextReadyCheckDelayMs <= lingerMs);
    }

    @Test
    public void testRetryBackoff() throws Exception {
        long lingerMs = Long.MAX_VALUE / 4;
        long retryBackoffMs = Long.MAX_VALUE / 2;
        final RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, CompressionType.NONE, lingerMs, retryBackoffMs, metrics, time);

        long now = time.milliseconds();
        accum.append(tp1, 0L, key, value, null, maxBlockTimeMs);
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, now + lingerMs + 1);
        assertEquals("Node1 should be ready", Collections.singleton(node1), result.readyNodes);
        Map<Integer, List<RecordBatch>> batches = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, now + lingerMs + 1);
        assertEquals("Node1 should be the only ready node.", 1, batches.size());
        assertEquals("Partition 0 should only have one batch drained.", 1, batches.get(0).size());

        // Reenqueue the batch
        now = time.milliseconds();
        accum.reenqueue(batches.get(0).get(0), now);

        // Put message for partition 1 into accumulator
        accum.append(tp2, 0L, key, value, null, maxBlockTimeMs);
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
        long lingerMs = Long.MAX_VALUE;
        final RecordAccumulator accum = new RecordAccumulator(4 * 1024, 64 * 1024, CompressionType.NONE, lingerMs, 100L, metrics, time);
        for (int i = 0; i < 100; i++)
            accum.append(new TopicPartition(topic, i % 3), 0L, key, value, null, maxBlockTimeMs);
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals("No nodes should be ready.", 0, result.readyNodes.size());
        
        accum.beginFlush();
        result = accum.ready(cluster, time.milliseconds());
        
        // drain and deallocate all batches
        Map<Integer, List<RecordBatch>> results = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        for (List<RecordBatch> batches: results.values())
            for (RecordBatch batch: batches)
                accum.deallocate(batch);
        
        // should be complete with no unsent records.
        accum.awaitFlushCompletion();
        assertFalse(accum.hasUnsent());
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
        RecordAccumulator accum = new RecordAccumulator(4 * 1024, 64 * 1024, CompressionType.NONE, Long.MAX_VALUE, 100L, metrics, time);
        accum.append(new TopicPartition(topic, 0), 0L, key, value, null, maxBlockTimeMs);

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
        long lingerMs = Long.MAX_VALUE;
        final AtomicInteger numExceptionReceivedInCallback = new AtomicInteger(0);
        final RecordAccumulator accum = new RecordAccumulator(4 * 1024, 64 * 1024, CompressionType.NONE, lingerMs, 100L, metrics, time);
        class TestCallback implements Callback {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                assertTrue(exception.getMessage().equals("Producer is closed forcefully."));
                numExceptionReceivedInCallback.incrementAndGet();
            }
        }
        for (int i = 0; i < 100; i++)
            accum.append(new TopicPartition(topic, i % 3), 0L, key, value, new TestCallback(), maxBlockTimeMs);
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals("No nodes should be ready.", 0, result.readyNodes.size());

        accum.abortIncompleteBatches();
        assertEquals(numExceptionReceivedInCallback.get(), 100);
        assertFalse(accum.hasUnsent());

    }

    @Test
    public void testExpiredBatches() throws InterruptedException {
        long retryBackoffMs = 100L;
        long lingerMs = 3000L;
        int requestTimeout = 60;

        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, CompressionType.NONE, lingerMs, retryBackoffMs, metrics, time);
        int appends = 1024 / msgSize;

        // Test batches not in retry
        for (int i = 0; i < appends; i++) {
            accum.append(tp1, 0L, key, value, null, maxBlockTimeMs);
            assertEquals("No partitions should be ready.", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());
        }
        // Make the batches ready due to batch full
        accum.append(tp1, 0L, key, value, null, 0);
        Set<Node> readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), readyNodes);
        // Advance the clock to expire the batch.
        time.sleep(requestTimeout + 1);
        accum.mutePartition(tp1);
        List<RecordBatch> expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should not be expired when the partition is muted", 0, expiredBatches.size());

        accum.unmutePartition(tp1);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should be expired", 1, expiredBatches.size());
        assertEquals("No partitions should be ready.", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());

        // Advance the clock to make the next batch ready due to linger.ms
        time.sleep(lingerMs);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), readyNodes);
        time.sleep(requestTimeout + 1);

        accum.mutePartition(tp1);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should not be expired when metadata is still available and partition is muted", 0, expiredBatches.size());

        accum.unmutePartition(tp1);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should be expired when the partition is not muted", 1, expiredBatches.size());
        assertEquals("No partitions should be ready.", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());

        // Test batches in retry.
        // Create a retried batch
        accum.append(tp1, 0L, key, value, null, 0);
        time.sleep(lingerMs);
        readyNodes = accum.ready(cluster, time.milliseconds()).readyNodes;
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), readyNodes);
        Map<Integer, List<RecordBatch>> drained = accum.drain(cluster, readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals("There should be only one batch.", drained.get(node1.id()).size(), 1);
        time.sleep(1000L);
        accum.reenqueue(drained.get(node1.id()).get(0), time.milliseconds());

        // test expiration.
        time.sleep(requestTimeout + retryBackoffMs);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should not be expired.", 0, expiredBatches.size());
        time.sleep(1L);

        accum.mutePartition(tp1);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should not be expired when the partition is muted", 0, expiredBatches.size());

        accum.unmutePartition(tp1);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should be expired when the partition is not muted.", 1, expiredBatches.size());
    }

    @Test
    public void testAppendInExpiryCallback() throws InterruptedException {
        long retryBackoffMs = 100L;
        long lingerMs = 3000L;
        int requestTimeout = 60;
        int messagesPerBatch = 1024 / msgSize;

        final RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, CompressionType.NONE, lingerMs,
                retryBackoffMs, metrics, time);
        final AtomicInteger expiryCallbackCount = new AtomicInteger();
        final AtomicReference<Exception> unexpectedException = new AtomicReference<Exception>();
        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception instanceof TimeoutException) {
                    expiryCallbackCount.incrementAndGet();
                    try {
                        accum.append(tp1, 0L, key, value, null, maxBlockTimeMs);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Unexpected interruption", e);
                    }
                } else if (exception != null)
                    unexpectedException.compareAndSet(null, exception);
            }
        };

        for (int i = 0; i < messagesPerBatch + 1; i++)
            accum.append(tp1, 0L, key, value, callback, maxBlockTimeMs);

        assertEquals(2, accum.batches().get(tp1).size());
        assertTrue("First batch not full", accum.batches().get(tp1).peekFirst().isFull());

        // Advance the clock to expire the first batch.
        time.sleep(requestTimeout + 1);
        List<RecordBatch> expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch was not expired", 1, expiredBatches.size());
        assertEquals("Callbacks not invoked for expiry", messagesPerBatch, expiryCallbackCount.get());
        assertNull("Unexpected exception", unexpectedException.get());
        assertEquals("Some messages not appended from expiry callbacks", 2, accum.batches().get(tp1).size());
        assertTrue("First batch not full after expiry callbacks with appends", accum.batches().get(tp1).peekFirst().isFull());
    }

    @Test
    public void testMutedPartitions() throws InterruptedException {
        long now = time.milliseconds();
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, CompressionType.NONE, 10, 100L, metrics, time);
        int appends = 1024 / msgSize;
        for (int i = 0; i < appends; i++) {
            accum.append(tp1, 0L, key, value, null, maxBlockTimeMs);
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
        Map<Integer, List<RecordBatch>> drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals("No batch should have been drained", 0, drained.get(node1.id()).size());

        // Test drain without muted partition.
        accum.unmutePartition(tp1);
        drained = accum.drain(cluster, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertTrue("The batch should have been drained.", drained.get(node1.id()).size() > 0);
    }
}
