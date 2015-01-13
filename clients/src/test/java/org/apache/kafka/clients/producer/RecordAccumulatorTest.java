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
package org.apache.kafka.clients.producer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.RecordBatch;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

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
    private Cluster cluster = new Cluster(Arrays.asList(node1, node2), Arrays.asList(part1, part2, part3));
    private Metrics metrics = new Metrics(time);
    String metricGroup = "TestMetrics";
    Map<String, String> metricTags = new LinkedHashMap<String, String>();

    @Test
    public void testFull() throws Exception {
        long now = time.milliseconds();
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, 10L, 100L, false, metrics, time,  metricTags);
        int appends = 1024 / msgSize;
        for (int i = 0; i < appends; i++) {
            accum.append(tp1, key, value, CompressionType.NONE, null);
            assertEquals("No partitions should be ready.", 0, accum.ready(cluster, now).readyNodes.size());
        }
        accum.append(tp1, key, value, CompressionType.NONE, null);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);
        List<RecordBatch> batches = accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
        assertEquals(1, batches.size());
        RecordBatch batch = batches.get(0);
        Iterator<LogEntry> iter = batch.records.iterator();
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
        RecordAccumulator accum = new RecordAccumulator(batchSize, 10 * 1024, 0L, 100L, false, metrics, time, metricTags);
        accum.append(tp1, key, new byte[2 * batchSize], CompressionType.NONE, null);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);
    }

    @Test
    public void testLinger() throws Exception {
        long lingerMs = 10L;
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, lingerMs, 100L, false, metrics, time, metricTags);
        accum.append(tp1, key, value, CompressionType.NONE, null);
        assertEquals("No partitions should be ready", 0, accum.ready(cluster, time.milliseconds()).readyNodes.size());
        time.sleep(10);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);
        List<RecordBatch> batches = accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
        assertEquals(1, batches.size());
        RecordBatch batch = batches.get(0);
        Iterator<LogEntry> iter = batch.records.iterator();
        LogEntry entry = iter.next();
        assertEquals("Keys should match", ByteBuffer.wrap(key), entry.record().key());
        assertEquals("Values should match", ByteBuffer.wrap(value), entry.record().value());
        assertFalse("No more records", iter.hasNext());
    }

    @Test
    public void testPartialDrain() throws Exception {
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, 10L, 100L, false, metrics, time, metricTags);
        int appends = 1024 / msgSize + 1;
        List<TopicPartition> partitions = asList(tp1, tp2);
        for (TopicPartition tp : partitions) {
            for (int i = 0; i < appends; i++)
                accum.append(tp, key, value, CompressionType.NONE, null);
        }
        assertEquals("Partition's leader should be ready", Collections.singleton(node1), accum.ready(cluster, time.milliseconds()).readyNodes);

        List<RecordBatch> batches = accum.drain(cluster, Collections.singleton(node1), 1024, 0).get(node1.id());
        assertEquals("But due to size bound only one partition should have been retrieved", 1, batches.size());
    }

    @Test
    public void testStressfulSituation() throws Exception {
        final int numThreads = 5;
        final int msgs = 10000;
        final int numParts = 2;
        final RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, 0L, 100L, true, metrics, time, metricTags);
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread() {
                public void run() {
                    for (int i = 0; i < msgs; i++) {
                        try {
                            accum.append(new TopicPartition(topic, i % numParts), key, value, CompressionType.NONE, null);
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
                    for (LogEntry entry : batch.records)
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
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, lingerMs, 100L, false, metrics, time, metricTags);
        // Just short of going over the limit so we trigger linger time
        int appends = 1024 / msgSize;

        // Partition on node1 only
        for (int i = 0; i < appends; i++)
            accum.append(tp1, key, value, CompressionType.NONE, null);
        RecordAccumulator.ReadyCheckResult result = accum.ready(cluster, time.milliseconds());
        assertEquals("No nodes should be ready.", 0, result.readyNodes.size());
        assertEquals("Next check time should be the linger time", lingerMs, result.nextReadyCheckDelayMs);

        time.sleep(lingerMs / 2);

        // Add partition on node2 only
        for (int i = 0; i < appends; i++)
            accum.append(tp3, key, value, CompressionType.NONE, null);
        result = accum.ready(cluster, time.milliseconds());
        assertEquals("No nodes should be ready.", 0, result.readyNodes.size());
        assertEquals("Next check time should be defined by node1, half remaining linger time", lingerMs / 2, result.nextReadyCheckDelayMs);

        // Add data for another partition on node1, enough to make data sendable immediately
        for (int i = 0; i < appends+1; i++)
            accum.append(tp2, key, value, CompressionType.NONE, null);
        result = accum.ready(cluster, time.milliseconds());
        assertEquals("Node1 should be ready", Collections.singleton(node1), result.readyNodes);
        // Note this can actually be < linger time because it may use delays from partitions that aren't sendable
        // but have leaders with other sendable data.
        assertTrue("Next check time should be defined by node2, at most linger time", result.nextReadyCheckDelayMs <= lingerMs);
    }

}
