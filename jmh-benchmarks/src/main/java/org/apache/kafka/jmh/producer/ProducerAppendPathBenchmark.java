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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator;
import org.apache.kafka.clients.producer.internals.ProducerBatch;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Measures the producer's append path: {@link RecordAccumulator#append} and the
 * {@link ChunkedRecordAccumulator} override of it. Run the same source on two revisions to detect
 * regressions.
 * <p>
 * Results are nanoseconds per record appended and, under {@code -prof gc}, bytes allocated per record.
 * The accumulator is built once per trial and drained between invocations, so appends are measured
 * against a warm producer: partition map populated, buffers on the pool's free list. {@code nowMs} is
 * read once per invocation.
 * <p>
 * Modes, each loading a different part of the path:
 * <ul>
 * <li>{@code steadyStateAppend} — one explicit partition, so nearly every record lands in the open
 *     batch. Per-append work.</li>
 * <li>{@code newBatchAppend} — one record per partition, so every append creates a batch. Per-batch
 *     work: buffer acquisition and builder construction.</li>
 * <li>{@code builtInPartitionerAppend} — no explicit partition, as a default producer sends. The only
 *     mode reaching the built-in partitioner.</li>
 * </ul>
 * A slower append should show in {@code steadyStateAppend} and {@code builtInPartitionerAppend} but hardly
 * in {@code newBatchAppend}; slower batch creation should show in {@code newBatchAppend} alone.
 * <p>
 * Strategies: {@code full} reserves a whole {@code batch.size} buffer per batch; {@code full-lz4} is
 * the same compressed; {@code incremental} is {@link ChunkedRecordAccumulator} over an
 * {@link BufferPool.AllocationMode#INCREMENTAL} pool, taking chunks on demand. Under {@code incremental}
 * a batch extends when a record does not fit its attached chunks; larger batches and values load that
 * path more heavily.
 * <p>
 * {@code full-lz4} appends a zero-filled value, so it measures the compressed path on a highly
 * compressible payload.
 * <p>
 * Some examples of how to run it:
 * <pre>
 * # everything
 * jmh-benchmarks/jmh.sh ProducerAppendPathBenchmark
 * # both strategies, per-append cost only
 * jmh-benchmarks/jmh.sh -p strategy=full,incremental ProducerAppendPathBenchmark.steadyStateAppend
 * # load the incremental extension path
 * jmh-benchmarks/jmh.sh -p strategy=incremental -p batchSize=262144 -p valueSize=8192 ProducerAppendPathBenchmark
 * # allocation per record
 * jmh-benchmarks/jmh.sh -prof gc ProducerAppendPathBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@Fork(value = 3, jvmArgs = {"-Xmx3g"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ProducerAppendPathBenchmark {

    private static final String TOPIC = "test";
    private static final long TOTAL_MEMORY = 512 * 1024 * 1024L;
    private static final int STEADY_STATE_RECORDS = 10_000;
    /** One append per partition in {@code newBatchAppend}, so this is also that mode's record count. */
    private static final int NUM_PARTITIONS = 500;
    /** The single partition {@code steadyStateAppend} writes to. */
    private static final int STEADY_STATE_PARTITION = 0;
    private static final String FULL = "full";
    private static final String FULL_LZ4 = "full-lz4";
    private static final String INCREMENTAL = "incremental";

    // TODO: extend to support compression + incremental.
    @Param({FULL, FULL_LZ4, INCREMENTAL})
    private String strategy;

    /**
     * 16384 is the producer default. The larger size shifts the ratio of per-append to per-batch work
     * and, under {@code incremental}, makes batches span several chunks so they extend mid-batch.
     */
    @Param({"16384", "262144"})
    private int batchSize;

    /** Value bytes, not total record size. Separates fixed per-append cost from per-byte cost. */
    @Param({"100", "1024"})
    private int valueSize;

    private Time time;
    private byte[] key;
    private byte[] value;
    private Set<Node> nodes;
    private MetadataSnapshot metadataSnapshot;
    /** Covers only {@link #STEADY_STATE_PARTITION}. See {@link #resetAccumulator()}. */
    private MetadataSnapshot singlePartitionSnapshot;
    private Cluster cluster;

    private Metrics metrics;
    private RecordAccumulator accum;

    /** Built once per trial, so the per-invocation reset neither rebuilds nor re-allocates any of it. */
    @Setup(Level.Trial)
    public void setupTrial() {
        time = Time.SYSTEM;
        key = "key".getBytes(StandardCharsets.UTF_8);
        value = new byte[valueSize];

        Node node = new Node(0, "localhost", 1111);
        nodes = Set.of(node);
        metadataSnapshot = createMetadataSnapshot(node, NUM_PARTITIONS);
        singlePartitionSnapshot = createMetadataSnapshot(node, STEADY_STATE_PARTITION + 1);
        cluster = metadataSnapshot.cluster();

        metrics = new Metrics(time);
        accum = createAccumulator();
    }

    /**
     * Empties the accumulator between invocations by draining every batch and returning its memory to
     * the pool. Nothing on the measured path drains, so batches would otherwise pile up and exhaust the
     * pool. Draining rather than rebuilding leaves the partition map and the pool's free list warm, as
     * in a running producer.
     * <p>
     * Two passes for speed: {@code drain} takes one batch per partition per call but scans every
     * partition in the snapshot, so {@code steadyStateAppend}'s single partition is drained through a
     * one-partition snapshot first; the full snapshot collects what the other modes leave.
     * <p>
     * The reset's allocation is charged to {@code gc.alloc.rate.norm}, though its time is not.
     */
    @Setup(Level.Invocation)
    public void resetAccumulator() {
        long now = time.milliseconds();
        drainAll(singlePartitionSnapshot, now);
        drainAll(metadataSnapshot, now);
    }

    private void drainAll(MetadataSnapshot snapshot, long now) {
        boolean drainedAny = true;
        while (drainedAny) {
            drainedAny = false;
            Map<Integer, List<ProducerBatch>> drained = accum.drain(snapshot, nodes, Integer.MAX_VALUE, now);
            for (List<ProducerBatch> batches : drained.values()) {
                for (ProducerBatch batch : batches) {
                    drainedAny = true;
                    accum.completeBatch(batch);
                    accum.deallocate(batch);
                }
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        accum.close();
        metrics.close();
    }

    /**
     * Per-append work: all records go to one explicit partition, so all but roughly one append per
     * batch lands in the already-open batch.
     */
    @Benchmark
    @OperationsPerInvocation(STEADY_STATE_RECORDS)
    public void steadyStateAppend(Blackhole blackhole) throws InterruptedException {
        long nowMs = time.milliseconds();
        for (int i = 0; i < STEADY_STATE_RECORDS; i++) {
            blackhole.consume(accum.append(TOPIC, STEADY_STATE_PARTITION, 0L, key, value, Record.EMPTY_HEADERS,
                    null, 1000L, nowMs, cluster));
        }
    }

    /** Per-batch work: each record goes to a fresh partition, so every append creates a batch. */
    @Benchmark
    @OperationsPerInvocation(NUM_PARTITIONS)
    public void newBatchAppend(Blackhole blackhole) throws InterruptedException {
        long nowMs = time.milliseconds();
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            blackhole.consume(accum.append(TOPIC, partition, 0L, key, value, Record.EMPTY_HEADERS,
                    null, 1000L, nowMs, cluster));
        }
    }

    /**
     * The default producer path: no explicit partition, so the built-in partitioner picks the partition
     * and switches it as batches fill.
     */
    @Benchmark
    @OperationsPerInvocation(STEADY_STATE_RECORDS)
    public void builtInPartitionerAppend(Blackhole blackhole) throws InterruptedException {
        long nowMs = time.milliseconds();
        for (int i = 0; i < STEADY_STATE_RECORDS; i++) {
            blackhole.consume(accum.append(TOPIC, RecordMetadata.UNKNOWN_PARTITION, 0L, key, value,
                    Record.EMPTY_HEADERS, null, 1000L, nowMs, cluster));
        }
    }

    private RecordAccumulator createAccumulator() {
        // Matches what KafkaProducer builds by default. Adaptive partitioning only takes effect once
        // ready() has computed load stats on the drain side; ready() is never called here, so the
        // partitioner picks uniformly at random and the adaptive branch of nextPartition is not exercised.
        RecordAccumulator.PartitionerConfig partitionerConfig =
                new RecordAccumulator.PartitionerConfig(true, 0, false, "");
        if (INCREMENTAL.equals(strategy)) {
            BufferPool pool = new BufferPool(TOTAL_MEMORY, ChunkedRecordAccumulator.CHUNK_SIZE, metrics, time,
                    "producer-metrics", BufferPool.AllocationMode.INCREMENTAL);
            return new ChunkedRecordAccumulator(
                new LogContext(),
                batchSize,
                Compression.NONE,
                Integer.MAX_VALUE,  // lingerMs, so nothing ever becomes ready
                100L,               // retryBackoffMs
                1000L,              // retryBackoffMaxMs
                3200,               // deliveryTimeoutMs
                partitionerConfig,
                metrics,
                "producer-metrics",
                time,
                null,               // transactionManager
                pool
            );
        }
        // TODO: extend to support compression + incremental.
        Compression compression = FULL_LZ4.equals(strategy) ? Compression.lz4().build() : Compression.NONE;
        BufferPool pool = new BufferPool(TOTAL_MEMORY, batchSize, metrics, time, "producer-metrics",
                BufferPool.AllocationMode.FULL);
        return new RecordAccumulator(
            new LogContext(),
            batchSize,
            compression,
            Integer.MAX_VALUE,
            100L,
            1000L,
            3200,
            partitionerConfig,
            metrics,
            "producer-metrics",
            time,
            null,
            pool
        );
    }

    private MetadataSnapshot createMetadataSnapshot(Node node, int partitionCount) {
        Map<Integer, Node> nodesById = Stream.of(node).collect(Collectors.toMap(Node::id, Function.identity()));
        List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>(partitionCount);
        for (int partition = 0; partition < partitionCount; partition++) {
            partitions.add(new MetadataResponse.PartitionMetadata(
                Errors.NONE,
                new TopicPartition(TOPIC, partition),
                Optional.of(node.id()),
                Optional.empty(),
                null,
                null,
                null
            ));
        }
        return new MetadataSnapshot(
            null,
            nodesById,
            partitions,
            Set.of(),
            Set.of(),
            Set.of(),
            null,
            Map.of()
        );
    }
}
