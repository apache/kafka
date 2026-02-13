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

package org.apache.kafka.jmh.producer;

import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ProducerBatch;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RecordAccumulatorFlushBenchmark {

    private static final String TOPIC = "test";
    private static final int PARTITION = 0;
    private static final int BATCH_SIZE = 1024;
    private static final long TOTAL_SIZE = 10 * 1024 * 1024;

    @Param({"5000", "10000"})
    private int numRecords;

    private RecordAccumulator accum;
    private Metrics metrics;
    private TopicPartition tp;
    private Time time;

    @Setup(Level.Invocation)
    public void setup() throws InterruptedException {
        tp = new TopicPartition(TOPIC, PARTITION);
        time = new MockTime();
        metrics = new Metrics(time);

        Cluster cluster = createTestCluster();
        accum = createRecordAccumulator();

        appendRecords(cluster);
        prepareFlush();
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        deallocateBatches();
        if (metrics != null) {
            metrics.close();
        }
    }

    @Benchmark
    public void measureFlushCompletion() throws InterruptedException {
        accum.awaitFlushCompletion();
    }

    private Cluster createTestCluster() {
        Node node = new Node(0, "localhost", 1111);
        MetadataResponse.PartitionMetadata partMetadata = new MetadataResponse.PartitionMetadata(
            Errors.NONE,
            tp,
            Optional.of(node.id()),
            Optional.empty(),
            null,
            null,
            null
        );

        Map<Integer, Node> nodes = Stream.of(node).collect(Collectors.toMap(Node::id, Function.identity()));
        MetadataSnapshot metadataCache = new MetadataSnapshot(
            null,
            nodes,
            Collections.singletonList(partMetadata),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            null,
            Collections.emptyMap()
        );
        return metadataCache.cluster();
    }

    private RecordAccumulator createRecordAccumulator() {
        return new RecordAccumulator(
            new LogContext(),
            BATCH_SIZE + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            Compression.NONE,
            Integer.MAX_VALUE,  // lingerMs
            100L,               // retryBackoffMs
            1000L,              // retryBackoffMaxMs
            3200,               // deliveryTimeoutMs
            metrics,
            "producer-metrics",
            time,
            null,
            new BufferPool(TOTAL_SIZE, BATCH_SIZE, metrics, time, "producer-metrics")
        );
    }

    private void appendRecords(Cluster cluster) throws InterruptedException {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        for (int i = 0; i < numRecords; i++) {
            accum.append(
                TOPIC,
                PARTITION,
                0L,
                key,
                value,
                Record.EMPTY_HEADERS,
                null,
                1000L,
                time.milliseconds(),
                cluster
            );
        }
    }

    private void prepareFlush() {
        accum.beginFlush();

        // Complete all batches to mimic successful sends
        List<ProducerBatch> batches = new ArrayList<>(accum.getDeque(tp));
        for (ProducerBatch batch : batches) {
            batch.complete(0L, time.milliseconds());
        }
    }

    private void deallocateBatches() {
        if (accum != null && tp != null) {
            List<ProducerBatch> batches = new ArrayList<>(accum.getDeque(tp));
            for (ProducerBatch batch : batches) {
                accum.deallocate(batch);
            }
        }
    }
}
