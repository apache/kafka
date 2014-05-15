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
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.util.*;

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
    private Node node = new Node(0, "localhost", 1111);
    private TopicPartition tp1 = new TopicPartition(topic, partition1);
    private TopicPartition tp2 = new TopicPartition(topic, partition2);
    private PartitionInfo part1 = new PartitionInfo(topic, partition1, node, null, null);
    private PartitionInfo part2 = new PartitionInfo(topic, partition2, node, null, null);
    private MockTime time = new MockTime();
    private byte[] key = "key".getBytes();
    private byte[] value = "value".getBytes();
    private int msgSize = Records.LOG_OVERHEAD + Record.recordSize(key, value);
    private Cluster cluster = new Cluster(Collections.singleton(node), Arrays.asList(part1, part2));
    private Metrics metrics = new Metrics(time);

    @Test
    public void testFull() throws Exception {
        long now = time.milliseconds();
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, 10L, 100L, false, metrics, time);
        int appends = 1024 / msgSize;
        for (int i = 0; i < appends; i++) {
            accum.append(tp1, key, value, CompressionType.NONE, null);
            assertEquals("No partitions should be ready.", 0, accum.ready(cluster, now).size());
        }
        accum.append(tp1, key, value, CompressionType.NONE, null);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node), accum.ready(cluster, time.milliseconds()));
        List<RecordBatch> batches = accum.drain(cluster, Collections.singleton(node), Integer.MAX_VALUE, 0).get(node.id());
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
        RecordAccumulator accum = new RecordAccumulator(batchSize, 10 * 1024, 0L, 100L, false, metrics, time);
        accum.append(tp1, key, new byte[2 * batchSize], CompressionType.NONE, null);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node), accum.ready(cluster, time.milliseconds()));
    }

    @Test
    public void testLinger() throws Exception {
        long lingerMs = 10L;
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, lingerMs, 100L, false, metrics, time);
        accum.append(tp1, key, value, CompressionType.NONE, null);
        assertEquals("No partitions should be ready", 0, accum.ready(cluster, time.milliseconds()).size());
        time.sleep(10);
        assertEquals("Our partition's leader should be ready", Collections.singleton(node), accum.ready(cluster, time.milliseconds()));
        List<RecordBatch> batches = accum.drain(cluster, Collections.singleton(node), Integer.MAX_VALUE, 0).get(node.id());
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
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, 10L, 100L, false, metrics, time);
        int appends = 1024 / msgSize + 1;
        List<TopicPartition> partitions = asList(tp1, tp2);
        for (TopicPartition tp : partitions) {
            for (int i = 0; i < appends; i++)
                accum.append(tp, key, value, CompressionType.NONE, null);
        }
        assertEquals("Partition's leader should be ready", Collections.singleton(node), accum.ready(cluster, time.milliseconds()));

        List<RecordBatch> batches = accum.drain(cluster, Collections.singleton(node), 1024, 0).get(node.id());
        assertEquals("But due to size bound only one partition should have been retrieved", 1, batches.size());
    }

    @Test
    public void testStressfulSituation() throws Exception {
        final int numThreads = 5;
        final int msgs = 10000;
        final int numParts = 2;
        final RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, 0L, 100L, true, metrics, time);
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
            Set<Node> nodes = accum.ready(cluster, now);
            List<RecordBatch> batches = accum.drain(cluster, nodes, 5 * 1024, 0).get(node.id());
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

}
