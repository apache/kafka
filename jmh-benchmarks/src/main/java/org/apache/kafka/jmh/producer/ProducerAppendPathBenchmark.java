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
 * Measures the producer's append path — {@link RecordAccumulator#append} and the
 * {@link ChunkedRecordAccumulator} override of it — so the same source can be run on two revisions
 * to check for regressions. Results are reported per record appended.
 * <p>
 * <b>Modes.</b> Each covers a different part of the path, because a change can be cheap on one and
 * expensive on another:
 * <ul>
 * <li>{@code steadyStateAppend} — explicit partition, all records to partition 0. Almost every
 *     append lands in the already-open batch, so this isolates per-append work. About one append in
 *     {@code batch.size / record size} still creates a batch (1 in 145 at {@code valueSize=100} and
 *     16KB batches, 1 in 15 at 1024; roughly 16x rarer at 256KB).</li>
 * <li>{@code newBatchAppend} — explicit partition, one record per partition, so every append also
 *     creates a batch. Isolates per-batch work: pool acquisition, {@link org.apache.kafka.common.record.internal.MemoryRecordsBuilder}
 *     construction, and — under {@code incremental} — {@code allocateChunks} plus the
 *     {@code ChunkedByteBufferOutputStream} wrapper.</li>
 * <li>{@code builtInPartitionerAppend} — {@link RecordMetadata#UNKNOWN_PARTITION}, which is what a
 *     producer sending without an explicit partition does. This is the only mode that reaches
 *     {@code peekCurrentPartitionInfo}, {@code partitionChanged} and the byte accounting in
 *     {@code updatePartitionInfo}, including the periodic partition switch. A regression confined to
 *     the sticky-partitioner path shows up only here.</li>
 * </ul>
 * <b>Strategy.</b> The {@code strategy} parameter selects the accumulator and its
 * {@code buffer.memory} allocation mode. It is a single parameter rather than a
 * strategy-by-compression grid because {@link ChunkedRecordAccumulator} rejects compression today,
 * so a grid would contain a cell that cannot run:
 * <ul>
 * <li>{@code full} — {@link RecordAccumulator} with {@link Compression#NONE}: one whole
 *     {@code batch.size} buffer per batch.</li>
 * <li>{@code full-lz4} — the same with lz4. Compression is a separate branch in
 *     {@code MemoryRecordsBuilder.estimatedBytesWritten}, which is on the per-append path, so it
 *     needs its own cell. Read a null result here as weaker evidence than one under {@code full}:
 *     the compressor does real work per record and dilutes per-append effects.</li>
 * <li>{@code incremental} — {@link ChunkedRecordAccumulator} over an
 *     {@link BufferPool.AllocationMode#INCREMENTAL} pool: {@value ChunkedRecordAccumulator#CHUNK_SIZE}-byte
 *     chunks attached on demand. Add an {@code incremental-lz4} value once that combination is
 *     supported.</li>
 * </ul>
 * <b>Warm state.</b> The accumulator, pool and metrics are built once per trial and the batches are
 * drained and returned to the pool before each invocation (see {@link #resetAccumulator()}), so the
 * measured appends run against a producer that is warm in the two ways a real one is: the
 * per-partition deque map is already populated, and the pool's free list already holds buffers. That
 * matters most for {@code newBatchAppend}: the deque map is a
 * {@link org.apache.kafka.common.utils.internals.CopyOnWriteMap}, so the <em>first</em> touch of each
 * of 500 partitions copies the whole map — about 125k entry copies, which swamps the batch creation
 * the mode exists to measure. A real producer pays that once in its lifetime, not once per batch.
 * <p>
 * <b>What this does not cover.</b>
 * <ul>
 * <li>Contention. Every mode here is single-threaded, so nothing exercises the {@code synchronized (dq)}
 *     hold time, the {@code appendsInProgress} counter, or {@link BufferPool}'s lock. See
 *     {@code ProducerAppendContentionBenchmark}.</li>
 * <li>Memory pressure. The pool is sized so no append ever blocks, so neither
 *     {@code max.block.ms}, {@code BufferExhaustedException}, nor — for {@code incremental} — the
 *     non-blocking extension path's pool-exhausted fallback is reached.</li>
 * <li>Records larger than {@code batch.size}, which take the non-poolable branch of
 *     {@code BufferPool.allocate} under {@code full} and a multi-chunk initial acquisition under
 *     {@code incremental}.</li>
 * <li>Record headers: every record is appended with {@link Record#EMPTY_HEADERS}, so the header
 *     loops in {@code estimateSizeInBytesUpperBound} and {@code DefaultRecord.writeTo} stay empty.</li>
 * <li>Draining. Nothing here calls {@code ready()} or {@code drain()} on the measured path, so
 *     {@code lingerMs} and {@code enableAdaptivePartitioning} never come into play.</li>
 * </ul>
 * {@link Time#SYSTEM} is used rather than {@code MockTime} so clock calls cost what they cost in
 * production; the {@code nowMs} passed to {@code append} is read once per invocation rather than per
 * record, so the benchmark measures {@code append} and not {@code System.currentTimeMillis}.
 * <p>
 * The full parameter matrix is 3 strategies x 2 batch sizes x 2 value sizes x 3 modes. Run a subset
 * with, for example,
 * {@code jmh.sh -p strategy=full,incremental -p batchSize=16384 ProducerAppendPathBenchmark.steadyStateAppend},
 * and add {@code -prof gc} to compare allocation.
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

    @Param({FULL, FULL_LZ4, INCREMENTAL})
    private String strategy;

    /**
     * 16384 is the producer default and equals {@link ChunkedRecordAccumulator#CHUNK_SIZE}, so under
     * {@code incremental} a batch fits in one chunk and the mid-batch extension path never runs.
     * 262144 spans 16 chunks, so extension is exercised — on the appends that cross a chunk boundary,
     * about 1 in 145 at {@code valueSize=100} and 1 in 15 at 1024. Under {@code full} the second size
     * checks that a regression does not depend on batch size, which shifts the ratio of per-append to
     * per-batch work.
     */
    @Param({"16384", "262144"})
    private int batchSize;

    /**
     * Size in bytes of the record's value. Not the total record size: the key is a fixed 3 bytes and
     * V2 record overhead adds about another 9 bytes of varints (length, attributes, timestamp and
     * offset deltas, key and value lengths, header count), so the total is ~112 bytes at
     * {@code valueSize=100} and ~1036 at 1024. Parameterized to separate fixed per-append cost from
     * per-byte cost — a fixed cost is a much larger share of a small record.
     */
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

    /**
     * Everything below is built once per trial and reused, so no per-invocation fixture allocates it.
     * JMH excludes invocation-level fixture <em>time</em> from the result but not its allocation, so
     * anything built here rather than per invocation also keeps {@code gc.alloc.rate.norm} readable.
     */
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
     * Drain every buffered batch and return its memory to the pool, so each invocation starts from an
     * empty accumulator without rebuilding one.
     * <p>
     * A reset is needed because nothing on the measured path drains: batches would otherwise pile up
     * across the thousands of invocations in an iteration and exhaust the pool. Draining rather than
     * rebuilding keeps the deque map and the pool's free list warm across invocations, which is both
     * cheaper and closer to a running producer — see the class javadoc.
     * <p>
     * JMH excludes an invocation-level fixture's <em>time</em> from the result (it times the benchmark
     * method itself once a fixture is present) but not its allocation, so the reset has to stay cheap
     * to keep {@code gc.alloc.rate.norm} readable. Two passes are used for that reason, not for
     * correctness: {@code drain} takes at most one batch per partition per call but walks every
     * partition the snapshot places on the node, allocating a {@code TopicPartition} for each. All of
     * {@code steadyStateAppend}'s batches sit on one partition, so draining them through the
     * {@value #NUM_PARTITIONS}-partition snapshot would cost a full scan per batch — measured at 25 kB
     * per batch, several times what the appends themselves allocate. The single-partition snapshot
     * clears that case cheaply and the full one then collects whatever the other modes left. What
     * remains, measured by running the reset twice and taking the difference, is 2-3 B/op: about 2% of
     * the reported allocation, and identical across revisions.
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
     * The default producer path: no explicit partition, so the built-in partitioner picks one, the
     * post-lock {@code partitionChanged} check runs, and {@code updatePartitionInfo} accumulates bytes
     * and switches partition every {@code batch.size} bytes.
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
        // Matches what KafkaProducer builds: adaptive partitioning on, which is what
        // builtInPartitionerAppend needs to exercise the real partition-switch accounting.
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
