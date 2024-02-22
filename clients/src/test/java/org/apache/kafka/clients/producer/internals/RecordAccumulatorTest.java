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

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataSnapshot;
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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

    private PartitionMetadata partMetadata1 = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()), Optional.empty(), null, null, null);
    private PartitionMetadata partMetadata2 = new PartitionMetadata(Errors.NONE, tp2, Optional.of(node1.id()), Optional.empty(), null, null, null);
    private PartitionMetadata partMetadata3 = new PartitionMetadata(Errors.NONE, tp3, Optional.of(node2.id()), Optional.empty(), null, null, null);
    private List<PartitionMetadata> partMetadatas = new ArrayList<>(Arrays.asList(partMetadata1, partMetadata2, partMetadata3));

    private Map<Integer, Node> nodes = Arrays.asList(node1, node2).stream().collect(Collectors.toMap(Node::id, Function.identity()));
    private MetadataSnapshot metadataCache = new MetadataSnapshot(null,
        nodes,
        partMetadatas,
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet(),
        null,
        Collections.emptyMap());

    private Cluster cluster = metadataCache.cluster();

    private MockTime time = new MockTime();
    private byte[] key = "key".getBytes();
    private byte[] value = "value".getBytes();
    private int msgSize = DefaultRecord.sizeInBytes(0, 0, key.length, value.length, Record.EMPTY_HEADERS);

    private Metrics metrics = new Metrics(time);
    private final long maxBlockTimeMs = 1000;
    private final LogContext logContext = new LogContext();

    @BeforeEach void setup() {}

    @AfterEach
    public void teardown() {
        this.metrics.close();
    }

    @Test
    public void testDrainBatches() throws Exception {
        // test case: node1(tp1,tp2) , node2(tp3,tp4)
        // add tp-4
        int partition4 = 3;
        TopicPartition tp4 = new TopicPartition(topic, partition4);
        PartitionMetadata partMetadata4 = new PartitionMetadata(Errors.NONE, tp4, Optional.of(node2.id()), Optional.empty(), null, null, null);
        partMetadatas.add(partMetadata4);

        // This test requires that partitions to be drained in order for each node i.e.
        // node1 -> tp1, tp3, and node2 -> tp2, tp4.
        // So setup cluster with this order, and pass this cluster to MetadataCache to preserve this order.
        PartitionInfo part1 = MetadataResponse.toPartitionInfo(partMetadata1, nodes);
        PartitionInfo part2 = MetadataResponse.toPartitionInfo(partMetadata2, nodes);
        PartitionInfo part3 = MetadataResponse.toPartitionInfo(partMetadata3, nodes);
        PartitionInfo part4 = MetadataResponse.toPartitionInfo(partMetadata4, nodes);
        Cluster cluster = new Cluster(null, Arrays.asList(node1, node2), Arrays.asList(part1, part2, part3, part4),
            Collections.emptySet(), Collections.emptySet());

        metadataCache = new MetadataSnapshot(null,
            nodes,
            partMetadatas,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            null,
            Collections.emptyMap(),
            cluster);
        long batchSize = value.length + DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        RecordAccumulator accum = createTestRecordAccumulator((int) batchSize, Integer.MAX_VALUE, CompressionType.NONE, 10);


        //  initial data
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.append(topic, partition2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.append(topic, partition3, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.append(topic, partition4, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);

        // drain batches from 2 nodes: node1 => tp1, node2 => tp3, because the max request size is full after the first batch drained
        Map<Integer, List<ProducerBatch>> batches1 = accum.drain(metadataCache, new HashSet<>(Arrays.asList(node1, node2)), (int) batchSize, 0);
        verifyTopicPartitionInBatches(batches1, tp1, tp3);

        // add record for tp1, tp3
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.append(topic, partition3, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);

        // drain batches from 2 nodes: node1 => tp2, node2 => tp4, because the max request size is full after the first batch drained
        // The drain index should start from next topic partition, that is, node1 => tp2, node2 => tp4
        Map<Integer, List<ProducerBatch>> batches2 = accum.drain(metadataCache, new HashSet<>(Arrays.asList(node1, node2)), (int) batchSize, 0);
        verifyTopicPartitionInBatches(batches2, tp2, tp4);

        // make sure in next run, the drain index will start from the beginning
        Map<Integer, List<ProducerBatch>> batches3 = accum.drain(metadataCache, new HashSet<>(Arrays.asList(node1, node2)), (int) batchSize, 0);
        verifyTopicPartitionInBatches(batches3, tp1, tp3);

        // add record for tp2, tp3, tp4 and mute the tp4
        accum.append(topic, partition2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.append(topic, partition3, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.append(topic, partition4, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.mutePartition(tp4);
        // drain batches from 2 nodes: node1 => tp2, node2 => tp3 (because tp4 is muted)
        Map<Integer, List<ProducerBatch>> batches4 = accum.drain(metadataCache, new HashSet<>(Arrays.asList(node1, node2)), (int) batchSize, 0);
        verifyTopicPartitionInBatches(batches4, tp2, tp3);

        // add record for tp1, tp2, tp3, and unmute tp4
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.append(topic, partition2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.append(topic, partition3, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        accum.unmutePartition(tp4);
        // set maxSize as a max value, so that the all partitions in 2 nodes should be drained: node1 => [tp1, tp2], node2 => [tp3, tp4]
        Map<Integer, List<ProducerBatch>> batches5 = accum.drain(metadataCache, new HashSet<>(Arrays.asList(node1, node2)), Integer.MAX_VALUE, 0);
        verifyTopicPartitionInBatches(batches5, tp1, tp2, tp3, tp4);
    }

    private void verifyTopicPartitionInBatches(Map<Integer, List<ProducerBatch>> nodeBatches, TopicPartition... tp) {
        int allTpBatchCount = (int) nodeBatches.values().stream().flatMap(Collection::stream).count();
        assertEquals(tp.length, allTpBatchCount);
        List<TopicPartition> topicPartitionsInBatch = new ArrayList<>();
        for (Map.Entry<Integer, List<ProducerBatch>> entry : nodeBatches.entrySet()) {
            List<ProducerBatch> tpBatchList = entry.getValue();
            List<TopicPartition> tpList = tpBatchList.stream().map(producerBatch -> producerBatch.topicPartition).collect(Collectors.toList());
            topicPartitionsInBatch.addAll(tpList);
        }

        for (int i = 0; i < tp.length; i++) {
            assertEquals(tp[i], topicPartitionsInBatch.get(i));
        }
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
            accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), metadataCache.cluster());
            Deque<ProducerBatch> partitionBatches = accum.getDeque(tp1);
            assertEquals(1, partitionBatches.size());

            ProducerBatch batch = partitionBatches.peekFirst();
            assertTrue(batch.isWritable());
            assertEquals(0, accum.ready(metadataCache, now).readyNodes.size(), "No partitions should be ready.");
        }

        // this append doesn't fit in the first batch, so a new batch is created and the first batch is closed

        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), metadataCache.cluster());
        Deque<ProducerBatch> partitionBatches = accum.getDeque(tp1);
        assertEquals(2, partitionBatches.size());
        Iterator<ProducerBatch> partitionBatchesIterator = partitionBatches.iterator();
        assertTrue(partitionBatchesIterator.next().isWritable());
        assertEquals(Collections.singleton(node1), accum.ready(metadataCache, time.milliseconds()).readyNodes, "Our partition's leader should be ready");

        List<ProducerBatch> batches = accum.drain(metadataCache, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
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
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), metadataCache.cluster());
        assertEquals(Collections.singleton(node1), accum.ready(metadataCache, time.milliseconds()).readyNodes, "Our partition's leader should be ready");

        Deque<ProducerBatch> batches = accum.getDeque(tp1);
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
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), metadataCache.cluster());
        assertEquals(Collections.singleton(node1), accum.ready(metadataCache, time.milliseconds()).readyNodes, "Our partition's leader should be ready");

        Deque<ProducerBatch> batches = accum.getDeque(tp1);
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
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        assertEquals(0, accum.ready(metadataCache, time.milliseconds()).readyNodes.size(), "No partitions should be ready");
        time.sleep(10);
        assertEquals(Collections.singleton(node1), accum.ready(metadataCache, time.milliseconds()).readyNodes, "Our partition's leader should be ready");
        List<ProducerBatch> batches = accum.drain(metadataCache, Collections.singleton(node1), Integer.MAX_VALUE, 0).get(node1.id());
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
                1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * 1024, CompressionType.NONE, 10);
        int appends = 1024 / msgSize + 1;
        List<TopicPartition> partitions = asList(tp1, tp2);
        for (TopicPartition tp : partitions) {
            for (int i = 0; i < appends; i++)
                accum.append(tp.topic(), tp.partition(), 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        }
        assertEquals(Collections.singleton(node1), accum.ready(metadataCache, time.milliseconds()).readyNodes, "Partition's leader should be ready");

        List<ProducerBatch> batches = accum.drain(metadataCache, Collections.singleton(node1), 1024, 0).get(node1.id());
        assertEquals(1, batches.size(), "But due to size bound only one partition should have been retrieved");
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
            threads.add(new Thread(() -> {
                for (int j = 0; j < msgs; j++) {
                    try {
                        accum.append(topic, j % numParts, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }));
        }
        for (Thread t : threads)
            t.start();
        int read = 0;
        long now = time.milliseconds();
        while (read < numThreads * msgs) {
            Set<Node> nodes = accum.ready(metadataCache, now).readyNodes;
            List<ProducerBatch> batches = accum.drain(metadataCache, nodes, 5 * 1024, 0).get(node1.id());
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
            accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, time.milliseconds());
        assertEquals(0, result.readyNodes.size(), "No nodes should be ready.");
        assertEquals(lingerMs, result.nextReadyCheckDelayMs, "Next check time should be the linger time");

        time.sleep(lingerMs / 2);

        // Add partition on node2 only
        for (int i = 0; i < appends; i++)
            accum.append(topic, partition3, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        result = accum.ready(metadataCache, time.milliseconds());
        assertEquals(0, result.readyNodes.size(), "No nodes should be ready.");
        assertEquals(lingerMs / 2, result.nextReadyCheckDelayMs, "Next check time should be defined by node1, half remaining linger time");

        // Add data for another partition on node1, enough to make data sendable immediately
        for (int i = 0; i < appends + 1; i++)
            accum.append(topic, partition2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        result = accum.ready(metadataCache, time.milliseconds());
        assertEquals(Collections.singleton(node1), result.readyNodes, "Node1 should be ready");
        // Note this can actually be < linger time because it may use delays from partitions that aren't sendable
        // but have leaders with other sendable data.
        assertTrue(result.nextReadyCheckDelayMs <= lingerMs, "Next check time should be defined by node2, at most linger time");
    }

    @Test
    public void testRetryBackoff() throws Exception {
        int lingerMs = Integer.MAX_VALUE / 16;
        long retryBackoffMs = Integer.MAX_VALUE / 8;
        long retryBackoffMaxMs = retryBackoffMs * 10;
        int deliveryTimeoutMs = Integer.MAX_VALUE;
        long totalSize = 10 * 1024;
        int batchSize = 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        String metricGrpName = "producer-metrics";

        final RecordAccumulator accum = new RecordAccumulator(logContext, batchSize,
            CompressionType.NONE, lingerMs, retryBackoffMs, retryBackoffMaxMs,
            deliveryTimeoutMs, metrics, metricGrpName, time, new ApiVersions(), null,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));

        long now = time.milliseconds();
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, now + lingerMs + 1);
        assertEquals(Collections.singleton(node1), result.readyNodes, "Node1 should be ready");
        Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, now + lingerMs + 1);
        assertEquals(1, batches.size(), "Node1 should be the only ready node.");
        assertEquals(1, batches.get(0).size(), "Partition 0 should only have one batch drained.");

        // Reenqueue the batch
        now = time.milliseconds();
        accum.reenqueue(batches.get(0).get(0), now);

        // Put message for partition 1 into accumulator
        accum.append(topic, partition2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        result = accum.ready(metadataCache, now + lingerMs + 1);
        assertEquals(Collections.singleton(node1), result.readyNodes, "Node1 should be ready");

        // tp1 should backoff while tp2 should not
        batches = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, now + lingerMs + 1);
        assertEquals(1, batches.size(), "Node1 should be the only ready node.");
        assertEquals(1, batches.get(0).size(), "Node1 should only have one batch drained.");
        assertEquals(tp2, batches.get(0).get(0).topicPartition, "Node1 should only have one batch for partition 1.");

        // Partition 0 can be drained after retry backoff
        long upperBoundBackoffMs = (long) (retryBackoffMs * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
        result = accum.ready(metadataCache, now + upperBoundBackoffMs + 1);
        assertEquals(Collections.singleton(node1), result.readyNodes, "Node1 should be ready");
        batches = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, now + upperBoundBackoffMs + 1);
        assertEquals(1, batches.size(), "Node1 should be the only ready node.");
        assertEquals(1, batches.get(0).size(), "Node1 should only have one batch drained.");
        assertEquals(tp1, batches.get(0).get(0).topicPartition, "Node1 should only have one batch for partition 0.");
    }

    private Map<Integer, List<ProducerBatch>> drainAndCheckBatchAmount(
        MetadataSnapshot metadataCache, Node leader, RecordAccumulator accum, long now, int expected) {
        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, now);
        if (expected > 0) {
            assertEquals(Collections.singleton(leader), result.readyNodes, "Leader should be ready");
            Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, now);
            assertEquals(expected, batches.size(), "Leader should be the only ready node.");
            assertEquals(expected, batches.get(leader.id()).size(), "Partition should only have " + expected + " batch drained.");
            return batches;
        } else {
            assertEquals(0, result.readyNodes.size(), "Leader should not be ready");
            Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, now);
            assertEquals(0, batches.size(), "Leader should not be drained.");
            return null;
        }
    }

    @Test
    public void testExponentialRetryBackoff() throws Exception {
        int lingerMs = Integer.MAX_VALUE / 16;
        long retryBackoffMs = 100;
        long retryBackoffMaxMs = 1000;
        int deliveryTimeoutMs = Integer.MAX_VALUE;
        long totalSize = 10 * 1024;
        int batchSize = 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        String metricGrpName = "producer-metrics";

        final RecordAccumulator accum = new RecordAccumulator(logContext, batchSize,
                CompressionType.NONE, lingerMs, retryBackoffMs, retryBackoffMaxMs,
                deliveryTimeoutMs, metrics, metricGrpName, time, new ApiVersions(), null,
                new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));

        long now = time.milliseconds();
        long initial = now;
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);

        // No backoff for initial attempt
        Map<Integer, List<ProducerBatch>> batches = drainAndCheckBatchAmount(metadataCache, node1, accum, now + lingerMs + 1, 1);
        ProducerBatch batch = batches.get(0).get(0);
        long currentRetryBackoffMs = 0;

        for (int i = 0; currentRetryBackoffMs < retryBackoffMaxMs * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER); i++) {
            // Re-enqueue the batch
            now = time.milliseconds();
            accum.reenqueue(batch, now);
            long lowerBoundBackoffMs = (long) (retryBackoffMs * Math.pow(CommonClientConfigs.RETRY_BACKOFF_EXP_BASE, i) * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoffMs * Math.pow(CommonClientConfigs.RETRY_BACKOFF_EXP_BASE, i) * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            currentRetryBackoffMs = upperBoundBackoffMs;
            // Should back off
            drainAndCheckBatchAmount(metadataCache, node1, accum, initial + lowerBoundBackoffMs - 1, 0);
            // Should not back off
            drainAndCheckBatchAmount(metadataCache, node1, accum, initial + upperBoundBackoffMs + 1, 1);
        }
    }

    @Test
    public void testExponentialRetryBackoffLeaderChange() throws Exception {
        int lingerMs = Integer.MAX_VALUE / 16;
        long retryBackoffMs = 100;
        long retryBackoffMaxMs = 1000;
        int deliveryTimeoutMs = Integer.MAX_VALUE;
        long totalSize = 10 * 1024;
        int batchSize = 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        String metricGrpName = "producer-metrics";

        PartitionMetadata part1Metadata = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()), Optional.empty(), null, null, null);
        PartitionMetadata part1MetadataChange = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node2.id()), Optional.empty(), null, null, null);
        PartitionMetadata part2Metadata = new PartitionMetadata(Errors.NONE, tp2, Optional.of(node1.id()), Optional.empty(), null, null, null);
        PartitionMetadata part3Metadata = new PartitionMetadata(Errors.NONE, tp3, Optional.of(node2.id()), Optional.empty(), null, null, null);

        MetadataSnapshot metadataCache = new MetadataSnapshot(null,
            nodes,
            Arrays.asList(part1Metadata, part2Metadata, part3Metadata),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            null,
            Collections.emptyMap());

        MetadataSnapshot metadataCacheChange = new MetadataSnapshot(null,
            nodes,
            Arrays.asList(part1MetadataChange, part2Metadata, part3Metadata),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            null,
            Collections.emptyMap());

        final RecordAccumulator accum = new RecordAccumulator(logContext, batchSize,
                CompressionType.NONE, lingerMs, retryBackoffMs, retryBackoffMaxMs,
                deliveryTimeoutMs, metrics, metricGrpName, time, new ApiVersions(), null,
                new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));

        long now = time.milliseconds();
        long initial = now;
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);

        // No backoff for initial attempt
        Map<Integer, List<ProducerBatch>> batches = drainAndCheckBatchAmount(metadataCache, node1, accum, now + lingerMs + 1, 1);
        ProducerBatch batch = batches.get(0).get(0);

        long lowerBoundBackoffMs;
        long upperBoundBackoffMs;

        // Retry 1 - delay by retryBackoffMs +/- jitter
        now = time.milliseconds();
        accum.reenqueue(batch, now);
        lowerBoundBackoffMs = (long) (retryBackoffMs * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
        upperBoundBackoffMs = (long) (retryBackoffMs * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
        // Should back off
        drainAndCheckBatchAmount(metadataCache, node1, accum, initial + lowerBoundBackoffMs - 1, 0);
        // Should not back off
        drainAndCheckBatchAmount(metadataCache, node1, accum, initial + upperBoundBackoffMs + 1, 1);

        // Retry 2 - delay by retryBackoffMs * 2 +/- jitter
        now = time.milliseconds();
        accum.reenqueue(batch, now);
        lowerBoundBackoffMs = (long) (retryBackoffMs * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
        upperBoundBackoffMs = (long) (retryBackoffMs * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
        // Should back off
        drainAndCheckBatchAmount(metadataCache, node1, accum, initial + lowerBoundBackoffMs - 1, 0);
        // Should not back off
        drainAndCheckBatchAmount(metadataCache, node1, accum, initial + upperBoundBackoffMs + 1, 1);

        // Retry 3 - after a leader change, delay by retryBackoffMs * 2^2 +/- jitter (could optimise to do not delay at all)
        now = time.milliseconds();
        accum.reenqueue(batch, now);
        lowerBoundBackoffMs = (long) (retryBackoffMs * Math.pow(CommonClientConfigs.RETRY_BACKOFF_EXP_BASE, 2) * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
        upperBoundBackoffMs = (long) (retryBackoffMs * Math.pow(CommonClientConfigs.RETRY_BACKOFF_EXP_BASE, 2) * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
        // Should back off
        drainAndCheckBatchAmount(metadataCacheChange, node2, accum, initial + lowerBoundBackoffMs - 1, 0);
        // Should not back off
        drainAndCheckBatchAmount(metadataCacheChange, node2, accum, initial + upperBoundBackoffMs + 1, 1);

        // Retry 4 - delay by retryBackoffMs * 2^3 +/- jitter (capped to retryBackoffMaxMs)
        now = time.milliseconds();
        accum.reenqueue(batch, now);
        lowerBoundBackoffMs = (long) (retryBackoffMs * Math.pow(CommonClientConfigs.RETRY_BACKOFF_EXP_BASE, 3) * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
        upperBoundBackoffMs = retryBackoffMaxMs;
        // Should back off
        drainAndCheckBatchAmount(metadataCacheChange, node2, accum, initial + lowerBoundBackoffMs - 1, 0);
        // Should not back off
        drainAndCheckBatchAmount(metadataCacheChange, node2, accum, initial + upperBoundBackoffMs + 1, 1);
    }

    @Test
    public void testFlush() throws Exception {
        int lingerMs = Integer.MAX_VALUE;
        final RecordAccumulator accum = createTestRecordAccumulator(
                4 * 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionType.NONE, lingerMs);

        for (int i = 0; i < 100; i++) {
            accum.append(topic, i % 3, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
            assertTrue(accum.hasIncomplete());
        }
        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, time.milliseconds());
        assertEquals(0, result.readyNodes.size(), "No nodes should be ready.");

        accum.beginFlush();
        result = accum.ready(metadataCache, time.milliseconds());

        // drain and deallocate all batches
        Map<Integer, List<ProducerBatch>> results = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
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
        Thread t = new Thread(() -> {
            Time.SYSTEM.sleep(delayMs);
            thread.interrupt();
        });
        t.start();
    }

    @Test
    public void testAwaitFlushComplete() throws Exception {
        RecordAccumulator accum = createTestRecordAccumulator(
            4 * 1024 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionType.NONE, Integer.MAX_VALUE);
        accum.append(topic, 0, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);

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
            128 + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 64 * 1024, CompressionType.NONE, lingerMs);
        class TestCallback implements RecordAccumulator.AppendCallbacks {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                assertEquals("Producer is closed forcefully.", exception.getMessage());
                numExceptionReceivedInCallback.incrementAndGet();
            }

            @Override
            public void setPartition(int partition) {
            }
        }
        for (int i = 0; i < numRecords; i++)
            accum.append(topic, i % 3, 0L, key, value, null, new TestCallback(), maxBlockTimeMs, false, time.milliseconds(), cluster);
        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, time.milliseconds());
        assertFalse(result.readyNodes.isEmpty());
        Map<Integer, List<ProducerBatch>> drained = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
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

        class TestCallback implements RecordAccumulator.AppendCallbacks {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                assertEquals(cause, exception);
                numExceptionReceivedInCallback.incrementAndGet();
            }

            @Override
            public void setPartition(int partition) {
            }
        }
        for (int i = 0; i < numRecords; i++)
            accum.append(topic, i % 3, 0L, key, value, null, new TestCallback(), maxBlockTimeMs, false, time.milliseconds(), cluster);
        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, time.milliseconds());
        assertFalse(result.readyNodes.isEmpty());
        Map<Integer, List<ProducerBatch>> drained = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE,
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
        Set<Node> readyNodes;
        List<ProducerBatch> expiredBatches = new ArrayList<>();
        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;
        RecordAccumulator accum = createTestRecordAccumulator(deliveryTimeoutMs,
            batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionType.NONE, lingerMs);

        // Make the batches ready due to linger. These batches are not in retry
        for (Boolean mute: muteStates) {
            if (time.milliseconds() < System.currentTimeMillis())
                time.setCurrentTimeMs(System.currentTimeMillis());
            accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
            assertEquals(0, accum.ready(metadataCache, time.milliseconds()).readyNodes.size(), "No partition should be ready.");

            time.sleep(lingerMs);
            readyNodes = accum.ready(metadataCache, time.milliseconds()).readyNodes;
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
            assertEquals(0, accum.ready(metadataCache, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");
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
            accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
            assertEquals(0, accum.ready(metadataCache, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");
        }
        // Make the batches ready due to batch full
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds(), cluster);
        Set<Node> readyNodes = accum.ready(metadataCache, time.milliseconds()).readyNodes;
        assertEquals(Collections.singleton(node1), readyNodes, "Our partition's leader should be ready");
        // Advance the clock to expire the batch.
        time.sleep(deliveryTimeoutMs + 1);
        accum.mutePartition(tp1);
        List<ProducerBatch> expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(2, expiredBatches.size(), "The batches will be muted no matter if the partition is muted or not");

        accum.unmutePartition(tp1);
        expiredBatches = accum.expiredBatches(time.milliseconds());
        assertEquals(0, expiredBatches.size(), "All batches should have been expired earlier");
        assertEquals(0, accum.ready(metadataCache, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");

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
        assertEquals(0, accum.ready(metadataCache, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");

        // Test batches in retry.
        // Create a retried batch
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds(), cluster);
        time.sleep(lingerMs);
        readyNodes = accum.ready(metadataCache, time.milliseconds()).readyNodes;
        assertEquals(Collections.singleton(node1), readyNodes, "Our partition's leader should be ready");
        Map<Integer, List<ProducerBatch>> drained = accum.drain(metadataCache, readyNodes, Integer.MAX_VALUE, time.milliseconds());
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
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds(), cluster);
        time.sleep(lingerMs);
        readyNodes = accum.ready(metadataCache, time.milliseconds()).readyNodes;
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
        assertEquals(1, accum.ready(metadataCache, time.milliseconds()).readyNodes.size(), "No partitions should be ready.");
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
            accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
            assertEquals(0, accum.ready(metadataCache, now).readyNodes.size(), "No partitions should be ready.");
        }
        time.sleep(2000);

        // Test ready with muted partition
        accum.mutePartition(tp1);
        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, time.milliseconds());
        assertEquals(0, result.readyNodes.size(), "No node should be ready");

        // Test ready without muted partition
        accum.unmutePartition(tp1);
        result = accum.ready(metadataCache, time.milliseconds());
        assertTrue(result.readyNodes.size() > 0, "The batch should be ready");

        // Test drain with muted partition
        accum.mutePartition(tp1);
        Map<Integer, List<ProducerBatch>> drained = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals(0, drained.get(node1.id()).size(), "No batch should have been drained");

        // Test drain without muted partition.
        accum.unmutePartition(tp1);
        drained = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
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
            CompressionType.NONE, lingerMs, retryBackoffMs, retryBackoffMs, deliveryTimeoutMs, metrics, metricGrpName, time, apiVersions, transactionManager,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));
        assertThrows(UnsupportedVersionException.class,
            () -> accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds(), cluster));
    }

    @Test
    public void testRecordsDrainedWhenTransactionCompleting() throws Exception {
        int batchSize = 1025;
        int deliveryTimeoutMs = 3200;
        int lingerMs = 10;
        long totalSize = 10 * batchSize;

        TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
        RecordAccumulator accumulator = createTestRecordAccumulator(transactionManager, deliveryTimeoutMs,
            batchSize, totalSize, CompressionType.NONE, lingerMs);

        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(12345L, (short) 5);
        Mockito.when(transactionManager.producerIdAndEpoch()).thenReturn(producerIdAndEpoch);
        Mockito.when(transactionManager.isSendToPartitionAllowed(tp1)).thenReturn(true);
        Mockito.when(transactionManager.isPartitionAdded(tp1)).thenReturn(true);
        Mockito.when(transactionManager.firstInFlightSequence(tp1)).thenReturn(0);

        // Initially, the transaction is still in progress, so we should respect the linger.
        Mockito.when(transactionManager.isCompleting()).thenReturn(false);

        accumulator.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs,
            false, time.milliseconds(), cluster);
        accumulator.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs,
            false, time.milliseconds(), cluster);
        assertTrue(accumulator.hasUndrained());

        RecordAccumulator.ReadyCheckResult firstResult = accumulator.ready(metadataCache, time.milliseconds());
        assertEquals(0, firstResult.readyNodes.size());
        Map<Integer, List<ProducerBatch>> firstDrained = accumulator.drain(metadataCache, firstResult.readyNodes,
            Integer.MAX_VALUE, time.milliseconds());
        assertEquals(0, firstDrained.size());

        // Once the transaction begins completion, then the batch should be drained immediately.
        Mockito.when(transactionManager.isCompleting()).thenReturn(true);

        RecordAccumulator.ReadyCheckResult secondResult = accumulator.ready(metadataCache, time.milliseconds());
        assertEquals(1, secondResult.readyNodes.size());
        Node readyNode = secondResult.readyNodes.iterator().next();

        Map<Integer, List<ProducerBatch>> secondDrained = accumulator.drain(metadataCache, secondResult.readyNodes,
            Integer.MAX_VALUE, time.milliseconds());
        assertEquals(Collections.singleton(readyNode.id()), secondDrained.keySet());
        List<ProducerBatch> batches = secondDrained.get(readyNode.id());
        assertEquals(1, batches.size());
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
        Callback cb = (metadata, exception) -> acked.incrementAndGet();
        // Append two messages so the batch is too big.
        Future<RecordMetadata> future1 = batch.tryAppend(now, null, value, Record.EMPTY_HEADERS, cb, now);
        Future<RecordMetadata> future2 = batch.tryAppend(now, null, value, Record.EMPTY_HEADERS, cb, now);
        assertNotNull(future1);
        assertNotNull(future2);
        batch.close();
        // Enqueue the batch to the accumulator as if the batch was created by the accumulator.
        accum.reenqueue(batch, now);
        // Re-enqueuing counts as a second attempt, so the delay with jitter is 100 * (1 + 0.2) + 1
        time.sleep(121L);
        // Drain the batch.
        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, time.milliseconds());
        assertTrue(result.readyNodes.size() > 0, "The batch should be ready");
        Map<Integer, List<ProducerBatch>> drained = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals(1, drained.size(), "Only node1 should be drained");
        assertEquals(1, drained.get(node1.id()).size(), "Only one batch should be drained");
        // Split and reenqueue the batch.
        accum.splitAndReenqueue(drained.get(node1.id()).get(0));
        time.sleep(101L);

        drained = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertFalse(drained.isEmpty());
        assertFalse(drained.get(node1.id()).isEmpty());
        drained.get(node1.id()).get(0).complete(acked.get(), 100L);
        assertEquals(1, acked.get(), "The first message should have been acked.");
        assertTrue(future1.isDone());
        assertEquals(0, future1.get().offset());

        drained = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
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
        RecordAccumulator accum = createTestRecordAccumulator(batchSize, bufferCapacity, CompressionType.GZIP, 0);
        int numSplitBatches = prepareSplitBatches(accum, seed, 100, 20);
        assertTrue(numSplitBatches > 0, "There should be some split batches");
        // Drain all the split batches.
        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, time.milliseconds());
        for (int i = 0; i < numSplitBatches; i++) {
            Map<Integer, List<ProducerBatch>> drained =
                accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
            assertFalse(drained.isEmpty());
            assertFalse(drained.get(node1.id()).isEmpty());
        }
        assertTrue(accum.ready(metadataCache, time.milliseconds()).readyNodes.isEmpty(), "All the batches should have been drained.");
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
                accum.append(topic, partition1, 0L, null, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds(), cluster);
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
            batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionType.NONE, lingerMs);

        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        Set<Node> readyNodes = accum.ready(metadataCache, time.milliseconds()).readyNodes;
        Map<Integer, List<ProducerBatch>> drained = accum.drain(metadataCache, readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertTrue(drained.isEmpty());
        //assertTrue(accum.soonToExpireInFlightBatches().isEmpty());

        // advanced clock and send one batch out but it should not be included in soon to expire inflight
        // batches because batch's expiry is quite far.
        time.sleep(lingerMs + 1);
        readyNodes = accum.ready(metadataCache, time.milliseconds()).readyNodes;
        drained = accum.drain(metadataCache, readyNodes, Integer.MAX_VALUE, time.milliseconds());
        assertEquals(1, drained.size(), "A batch did not drain after linger");
        //assertTrue(accum.soonToExpireInFlightBatches().isEmpty());

        // Queue another batch and advance clock such that batch expiry time is earlier than request timeout.
        accum.append(topic, partition2, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        time.sleep(lingerMs * 4);

        // Now drain and check that accumulator picked up the drained batch because its expiry is soon.
        readyNodes = accum.ready(metadataCache, time.milliseconds()).readyNodes;
        drained = accum.drain(metadataCache, readyNodes, Integer.MAX_VALUE, time.milliseconds());
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
            batchSize + DefaultRecordBatch.RECORD_BATCH_OVERHEAD, 10 * batchSize, CompressionType.NONE, lingerMs);

        // Test batches in retry.
        for (Boolean mute : muteStates) {
            accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, 0, false, time.milliseconds(), cluster);
            time.sleep(lingerMs);
            readyNodes = accum.ready(metadataCache, time.milliseconds()).readyNodes;
            assertEquals(Collections.singleton(node1), readyNodes, "Our partition's leader should be ready");
            Map<Integer, List<ProducerBatch>> drained = accum.drain(metadataCache, readyNodes, Integer.MAX_VALUE, time.milliseconds());
            assertEquals(1, drained.get(node1.id()).size(), "There should be only one batch.");
            time.sleep(rtt);
            accum.reenqueue(drained.get(node1.id()).get(0), time.milliseconds());

            if (mute)
                accum.mutePartition(tp1);
            else
                accum.unmutePartition(tp1);

            // test expiration
            time.sleep(deliveryTimeoutMs - rtt);
            accum.drain(metadataCache, Collections.singleton(node1), Integer.MAX_VALUE, time.milliseconds());
            expiredBatches = accum.expiredBatches(time.milliseconds());
            assertEquals(mute ? 1 : 0, expiredBatches.size(), "RecordAccumulator has expired batches if the partition is not muted");
        }
    }

    @SuppressWarnings("deprecation")
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
        accum.append(topic, partition, 0L, null, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        int appends = 1;

        boolean switchPartition = false;
        while (!switchPartition) {
            // Append to the first batch
            partition = partitioner.partition(topic, null, null, "value", value, cluster);
            RecordAccumulator.RecordAppendResult result = accum.append(topic, partition, 0L, null,
                value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, true, time.milliseconds(), cluster);
            Deque<ProducerBatch> partitionBatches1 = accum.getDeque(tp1);
            Deque<ProducerBatch> partitionBatches2 = accum.getDeque(tp2);
            Deque<ProducerBatch> partitionBatches3 = accum.getDeque(tp3);
            int numBatches = (partitionBatches1 == null ? 0 : partitionBatches1.size()) + (partitionBatches2 == null ? 0 : partitionBatches2.size()) + (partitionBatches3 == null ? 0 : partitionBatches3.size());
            // Only one batch is created because the partition is sticky.
            assertEquals(1, numBatches);

            switchPartition = result.abortForNewBatch;
            // We only appended if we do not retry.
            if (!switchPartition) {
                appends++;
                assertEquals(0, accum.ready(metadataCache, now).readyNodes.size(), "No partitions should be ready.");
            }
        }

        // Batch should be full.
        assertEquals(1, accum.ready(metadataCache, time.milliseconds()).readyNodes.size());
        assertEquals(appends, expectedAppends);
        switchPartition = false;

        // KafkaProducer would call this method in this case, make second batch
        partitioner.onNewBatch(topic, cluster, partition);
        partition = partitioner.partition(topic, null, null, "value", value, cluster);
        accum.append(topic, partition, 0L, null, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, time.milliseconds(), cluster);
        appends++;

        // These appends all go into the second batch
        while (!switchPartition) {
            partition = partitioner.partition(topic, null, null, "value", value, cluster);
            RecordAccumulator.RecordAppendResult result = accum.append(topic, partition, 0L, null, value,
                Record.EMPTY_HEADERS, null, maxBlockTimeMs, true, time.milliseconds(), cluster);
            Deque<ProducerBatch> partitionBatches1 = accum.getDeque(tp1);
            Deque<ProducerBatch> partitionBatches2 = accum.getDeque(tp2);
            Deque<ProducerBatch> partitionBatches3 = accum.getDeque(tp3);
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

    @Test
    public void testUniformBuiltInPartitioner() throws Exception {

        try {
            // Mock random number generator with just sequential integer.
            AtomicInteger mockRandom = new AtomicInteger();
            BuiltInPartitioner.mockRandom = () -> mockRandom.getAndAdd(1);

            long totalSize = 1024 * 1024;
            int batchSize = 1024;  // note that this is also a "sticky" limit for the partitioner
            RecordAccumulator accum = createTestRecordAccumulator(batchSize, totalSize, CompressionType.NONE, 0);

            // Set up callbacks so that we know what partition is chosen.
            final AtomicInteger partition = new AtomicInteger(RecordMetadata.UNKNOWN_PARTITION);
            RecordAccumulator.AppendCallbacks callbacks = new RecordAccumulator.AppendCallbacks() {
                @Override
                public void setPartition(int p) {
                    partition.set(p);
                }

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                }
            };

            PartitionInfo part1 = MetadataResponse.toPartitionInfo(partMetadata1, nodes);
            PartitionInfo part2 = MetadataResponse.toPartitionInfo(partMetadata2, nodes);
            PartitionInfo part3 = MetadataResponse.toPartitionInfo(partMetadata3, nodes);
            Cluster cluster = new Cluster(null, asList(node1, node2), asList(part1, part2, part3),
                Collections.emptySet(), Collections.emptySet());

            // Produce small record, we should switch to first partition.
            accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, null, value, Record.EMPTY_HEADERS,
                callbacks, maxBlockTimeMs, false, time.milliseconds(), cluster);
            assertEquals(partition1, partition.get());
            assertEquals(1, mockRandom.get());

            // Produce large record, we should exceed "sticky" limit, but produce to this partition
            // as we try to switch after the "sticky" limit is exceeded.  The switch is disabled
            // because of incomplete batch.
            byte[] largeValue = new byte[batchSize];
            accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, null, largeValue, Record.EMPTY_HEADERS,
                callbacks, maxBlockTimeMs, false, time.milliseconds(), cluster);
            assertEquals(partition1, partition.get());
            assertEquals(1, mockRandom.get());

            // Produce large record, we should switch to next partition as we complete
            // previous batch and exceeded sticky limit.
            accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, null, largeValue, Record.EMPTY_HEADERS,
                callbacks, maxBlockTimeMs, false, time.milliseconds(), cluster);
            assertEquals(partition2, partition.get());
            assertEquals(2, mockRandom.get());

            // Produce large record, we should switch to next partition as we complete
            // previous batch and exceeded sticky limit.
            accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, null, largeValue, Record.EMPTY_HEADERS,
                callbacks, maxBlockTimeMs, false, time.milliseconds(), cluster);
            assertEquals(partition3, partition.get());
            assertEquals(3, mockRandom.get());

            // Produce large record, we should switch to next partition as we complete
            // previous batch and exceeded sticky limit.
            accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, null, largeValue, Record.EMPTY_HEADERS,
                callbacks, maxBlockTimeMs, false, time.milliseconds(), cluster);
            assertEquals(partition1, partition.get());
            assertEquals(4, mockRandom.get());
        } finally {
            BuiltInPartitioner.mockRandom = null;
        }
    }

    @Test
    public void testAdaptiveBuiltInPartitioner() throws Exception {
        try {
            // Mock random number generator with just sequential integer.
            AtomicInteger mockRandom = new AtomicInteger();
            BuiltInPartitioner.mockRandom = () -> mockRandom.getAndAdd(1);

            // Create accumulator with partitioner config to enable adaptive partitioning.
            RecordAccumulator.PartitionerConfig config = new RecordAccumulator.PartitionerConfig(true, 100);
            long totalSize = 1024 * 1024;
            int batchSize = 128;
            RecordAccumulator accum = new RecordAccumulator(logContext, batchSize, CompressionType.NONE, 0, 0L, 0L,
                3200, config, metrics, "producer-metrics", time, new ApiVersions(), null,
                new BufferPool(totalSize, batchSize, metrics, time, "producer-internal-metrics"));

            byte[] largeValue = new byte[batchSize];
            int[] queueSizes = {1, 7, 2};
            int[] expectedFrequencies = new int[queueSizes.length];
            for (int i = 0; i < queueSizes.length; i++) {
                expectedFrequencies[i] = 8 - queueSizes[i];  // 8 is max(queueSizes) + 1
                for (int c = queueSizes[i]; c-- > 0; ) {
                    // Add large records to each partition, so that each record creates a batch.
                    accum.append(topic, i, 0L, null, largeValue, Record.EMPTY_HEADERS,
                        null, maxBlockTimeMs, false, time.milliseconds(), cluster);
                }
                assertEquals(queueSizes[i], accum.getDeque(new TopicPartition(topic, i)).size());
            }

            // Let the accumulator generate the probability tables.
            accum.ready(metadataCache, time.milliseconds());

            // Set up callbacks so that we know what partition is chosen.
            final AtomicInteger partition = new AtomicInteger(RecordMetadata.UNKNOWN_PARTITION);
            RecordAccumulator.AppendCallbacks callbacks = new RecordAccumulator.AppendCallbacks() {
                @Override
                public void setPartition(int p) {
                    partition.set(p);
                }

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                }
            };

            // Prime built-in partitioner so that it'd switch on every record, as switching only
            // happens after the "sticky" limit is exceeded.
            accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, null, largeValue, Record.EMPTY_HEADERS,
                callbacks, maxBlockTimeMs, false, time.milliseconds(), cluster);

            // Issue a certain number of partition calls to validate that the partitions would be
            // distributed with frequencies that are reciprocal to the queue sizes.  The number of
            // iterations is defined by the last element of the cumulative frequency table which is
            // the sum of all frequencies.  We do 2 cycles, just so it's more than 1.
            final int numberOfCycles = 2;
            int numberOfIterations = accum.getBuiltInPartitioner(topic).loadStatsRangeEnd() * numberOfCycles;
            int[] frequencies = new int[queueSizes.length];

            for (int i = 0; i < numberOfIterations; i++) {
                accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, null, largeValue, Record.EMPTY_HEADERS,
                    callbacks, maxBlockTimeMs, false, time.milliseconds(), cluster);
                ++frequencies[partition.get()];
            }

            // Verify that frequencies are reciprocal of queue sizes.
            for (int i = 0; i < frequencies.length; i++) {
                assertEquals(expectedFrequencies[i] * numberOfCycles, frequencies[i],
                    "Partition " + i + " was chosen " + frequencies[i] + " times");
            }

            // Test that partitions residing on high-latency nodes don't get switched to.
            accum.updateNodeLatencyStats(0, time.milliseconds() - 200, true);
            accum.updateNodeLatencyStats(0, time.milliseconds(), false);
            accum.ready(metadataCache, time.milliseconds());

            // Do one append, because partition gets switched after append.
            accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, null, largeValue, Record.EMPTY_HEADERS,
                    callbacks, maxBlockTimeMs, false, time.milliseconds(), cluster);

            for (int c = 10; c-- > 0; ) {
                accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, null, largeValue, Record.EMPTY_HEADERS,
                    callbacks, maxBlockTimeMs, false, time.milliseconds(), cluster);
                assertEquals(partition3, partition.get());
            }
        } finally {
            BuiltInPartitioner.mockRandom = null;
        }
    }

    @Test
    public void testBuiltInPartitionerFractionalBatches() throws Exception {
        // Test how we avoid creating fractional batches with high linger.ms (see
        // BuiltInPartitioner.updatePartitionInfo).
        long totalSize = 1024 * 1024;
        int batchSize = 512;  // note that this is also a "sticky" limit for the partitioner
        int valSize = 32;
        RecordAccumulator accum = createTestRecordAccumulator(batchSize, totalSize, CompressionType.NONE, 10);
        byte[] value = new byte[valSize];

        for (int c = 10; c-- > 0; ) {
            // Produce about 2/3 of the batch size.
            for (int recCount = batchSize * 2 / 3 / valSize; recCount-- > 0; ) {
                accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0, null, value, Record.EMPTY_HEADERS,
                    null, maxBlockTimeMs, false, time.milliseconds(), cluster);
            }

            // Advance the time to make the batch ready.
            time.sleep(10);

            // We should have one batch ready.
            Set<Node> nodes = accum.ready(metadataCache, time.milliseconds()).readyNodes;
            assertEquals(1, nodes.size(), "Should have 1 leader ready");
            List<ProducerBatch> batches = accum.drain(metadataCache, nodes, Integer.MAX_VALUE, 0).entrySet().iterator().next().getValue();
            assertEquals(1, batches.size(), "Should have 1 batch ready");
            int actualBatchSize = batches.get(0).records().sizeInBytes();
            assertTrue(actualBatchSize > batchSize / 2, "Batch must be greater than half batch.size");
            assertTrue(actualBatchSize < batchSize, "Batch must be less than batch.size");
        }
    }

    /**
     * For a batch being retried, this validates ready() and drain() whether a batch should skip-backoff(retries-immediately), or backoff, based on -
     * 1. how long it has waited between retry attempts.
     * 2. change in leader hosting the partition.
     */
    @Test
    public void testReadyAndDrainWhenABatchIsBeingRetried() throws InterruptedException {
        int part1LeaderEpoch = 100;
        // Create cluster metadata, partition1 being hosted by node1
        PartitionMetadata part1Metadata = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()),  Optional.of(part1LeaderEpoch), null, null, null);
        MetadataSnapshot metadataCache = new MetadataSnapshot(null, nodes, Arrays.asList(part1Metadata), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());

        int batchSize = 10;
        int lingerMs = 10;
        int retryBackoffMs = 100;
        int retryBackoffMaxMs = 1000;
        int deliveryTimeoutMs = Integer.MAX_VALUE;
        long totalSize = 10 * 1024;
        String metricGrpName = "producer-metrics";
        final RecordAccumulator accum = new RecordAccumulator(logContext, batchSize,
            CompressionType.NONE, lingerMs, retryBackoffMs, retryBackoffMaxMs,
            deliveryTimeoutMs, metrics, metricGrpName, time, new ApiVersions(), null,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));

        // Create 1 batch(batchA) to be produced to partition1.
        long now = time.milliseconds();
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs, false, now, cluster);

        // 1st attempt(not a retry) to produce batchA, it should be ready & drained to be produced.
        {
            now += lingerMs + 1;
            RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, now);
            assertTrue(result.readyNodes.contains(node1), "Node1 is ready");

            Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache,
                result.readyNodes, 999999 /* maxSize */, now);
            assertTrue(batches.containsKey(node1.id()) && batches.get(node1.id()).size() == 1, "Node1 has 1 batch ready & drained");
            ProducerBatch batch = batches.get(node1.id()).get(0);
            assertEquals(OptionalInt.of(part1LeaderEpoch), batch.currentLeaderEpoch());
            assertEquals(0, batch.attemptsWhenLeaderLastChanged());
            // Re-enqueue batch for subsequent retries & test-cases
            accum.reenqueue(batch, now);
        }

        // In this retry of batchA, wait-time between retries is less than configured and no leader change, so should backoff.
        {
            now += 1;
            RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, now);
            assertFalse(result.readyNodes.contains(node1), "Node1 is not ready");

            // Try to drain from node1, it should return no batches.
            Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache,
                new HashSet<>(Arrays.asList(node1)), 999999 /* maxSize */, now);
            assertTrue(batches.containsKey(node1.id()) && batches.get(node1.id()).isEmpty(),
                "No batches ready to be drained on Node1");
        }

        // In this retry of batchA, wait-time between retries is less than configured and leader has changed, so should not backoff.
        {
            now += 1;
            part1LeaderEpoch++;
            // Create cluster metadata, with new leader epoch.
            part1Metadata = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()),  Optional.of(part1LeaderEpoch), null, null, null);
            metadataCache = new MetadataSnapshot(null, nodes, Arrays.asList(part1Metadata), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());
            RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, now);
            assertTrue(result.readyNodes.contains(node1), "Node1 is ready");

            Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache,
                result.readyNodes, 999999 /* maxSize */, now);
            assertTrue(batches.containsKey(node1.id()) && batches.get(node1.id()).size() == 1, "Node1 has 1 batch ready & drained");
            ProducerBatch batch = batches.get(node1.id()).get(0);
            assertEquals(OptionalInt.of(part1LeaderEpoch), batch.currentLeaderEpoch());
            assertEquals(1, batch.attemptsWhenLeaderLastChanged());

            // Re-enqueue batch for subsequent retries/test-cases.
            accum.reenqueue(batch, now);
        }

        // In this retry of batchA, wait-time between retries is more than configured and no leader change, so should not backoff.
        {
            now += 2 * retryBackoffMaxMs;
            // Create cluster metadata, with new leader epoch.
            part1Metadata = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()),  Optional.of(part1LeaderEpoch), null, null, null);
            metadataCache = new MetadataSnapshot(null, nodes, Arrays.asList(part1Metadata), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());
            RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, now);
            assertTrue(result.readyNodes.contains(node1), "Node1 is ready");

            Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache,
                result.readyNodes, 999999 /* maxSize */, now);
            assertTrue(batches.containsKey(node1.id()) && batches.get(node1.id()).size() == 1, "Node1 has 1 batch ready & drained");
            ProducerBatch batch = batches.get(node1.id()).get(0);
            assertEquals(OptionalInt.of(part1LeaderEpoch), batch.currentLeaderEpoch());
            assertEquals(1, batch.attemptsWhenLeaderLastChanged());

            // Re-enqueue batch for subsequent retries/test-cases.
            accum.reenqueue(batch, now);
        }

        // In this retry of batchA, wait-time between retries is more than configured and leader has changed, so should not backoff.
        {
            now += 2 * retryBackoffMaxMs;
            part1LeaderEpoch++;
            // Create cluster metadata, with new leader epoch.
            part1Metadata = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()),  Optional.of(part1LeaderEpoch), null, null, null);
            metadataCache = new MetadataSnapshot(null, nodes, Arrays.asList(part1Metadata), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());
            RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, now);
            assertTrue(result.readyNodes.contains(node1), "Node1 is ready");

            Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache,
                result.readyNodes, 999999 /* maxSize */, now);
            assertTrue(batches.containsKey(node1.id()) && batches.get(node1.id()).size() == 1, "Node1 has 1 batch ready & drained");
            ProducerBatch batch = batches.get(node1.id()).get(0);
            assertEquals(OptionalInt.of(part1LeaderEpoch), batch.currentLeaderEpoch());
            assertEquals(3, batch.attemptsWhenLeaderLastChanged());

            // Re-enqueue batch for subsequent retries/test-cases.
            accum.reenqueue(batch, now);
        }
    }

    @Test
    public void testDrainWithANodeThatDoesntHostAnyPartitions() {
        int batchSize = 10;
        int lingerMs = 10;
        long totalSize = 10 * 1024;
        RecordAccumulator accum = createTestRecordAccumulator(batchSize, totalSize,
            CompressionType.NONE, lingerMs);

        // Create cluster metadata, node2 doesn't host any partitions.
        PartitionMetadata part1Metadata = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()), Optional.empty(), null, null, null);
        MetadataSnapshot metadataCache = new MetadataSnapshot(null, nodes, Arrays.asList(part1Metadata), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());

        // Drain for node2, it should return 0 batches,
        Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache,
            new HashSet<>(Arrays.asList(node2)), 999999 /* maxSize */, time.milliseconds());
        assertTrue(batches.get(node2.id()).isEmpty());
    }

    private int prepareSplitBatches(RecordAccumulator accum, long seed, int recordSize, int numRecords)
        throws InterruptedException {
        Random random = new Random();
        random.setSeed(seed);

        // First set the compression ratio estimation to be good.
        CompressionRatioEstimator.setEstimation(tp1.topic(), CompressionType.GZIP, 0.1f);
        // Append 20 records of 100 bytes size with poor compression ratio should make the batch too big.
        for (int i = 0; i < numRecords; i++) {
            accum.append(topic, partition1, 0L, null, bytesWithPoorCompression(random, recordSize), Record.EMPTY_HEADERS, null, 0, false, time.milliseconds(), cluster);
        }

        RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, time.milliseconds());
        assertFalse(result.readyNodes.isEmpty());
        Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
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
            RecordAccumulator.ReadyCheckResult result = accum.ready(metadataCache, time.milliseconds());
            Map<Integer, List<ProducerBatch>> batches = accum.drain(metadataCache, result.readyNodes, Integer.MAX_VALUE, time.milliseconds());
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

    private RecordAccumulator createTestRecordAccumulator(int batchSize, long totalSize, CompressionType type, int lingerMs) {
        int deliveryTimeoutMs = 3200;
        return createTestRecordAccumulator(deliveryTimeoutMs, batchSize, totalSize, type, lingerMs);
    }

    private RecordAccumulator createTestRecordAccumulator(int deliveryTimeoutMs, int batchSize, long totalSize, CompressionType type, int lingerMs) {
        return createTestRecordAccumulator(null, deliveryTimeoutMs, batchSize, totalSize, type, lingerMs);
    }

    /**
     * Return a test RecordAccumulator instance
     */
    private RecordAccumulator createTestRecordAccumulator(
        TransactionManager txnManager,
        int deliveryTimeoutMs,
        int batchSize,
        long totalSize,
        CompressionType type,
        int lingerMs
    ) {
        long retryBackoffMs = 100L;
        long retryBackoffMaxMs = 1000L;
        String metricGrpName = "producer-metrics";

        return new RecordAccumulator(
            logContext,
            batchSize,
            type,
            lingerMs,
            retryBackoffMs,
            retryBackoffMaxMs,
            deliveryTimeoutMs,
            metrics,
            metricGrpName,
            time,
            new ApiVersions(),
            txnManager,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));
    }
}
