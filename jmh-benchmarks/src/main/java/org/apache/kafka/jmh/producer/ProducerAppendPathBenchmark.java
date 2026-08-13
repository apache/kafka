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
 * <b>Results are nanoseconds per record appended</b>, and under {@code -prof gc}, bytes allocated per
 * record. The accumulator is built once per trial and drained between invocations, so appends are
 * measured against a warm producer: partition map already populated, buffers already on the pool's
 * free list. {@link Time#SYSTEM} is used, but {@code nowMs} is read once per invocation, so a clock
 * read is not part of the per-record cost.
 * <p>
 * <b>Modes</b>, each loading a different part of the path:
 * <ul>
 * <li>{@code steadyStateAppend} — one explicit partition. Per-append work: nearly every record lands
 *     in the open batch.</li>
 * <li>{@code newBatchAppend} — one record per partition, so every append also creates a batch.
 *     Per-batch work: pool acquisition, builder construction, and under {@code incremental}
 *     {@code allocateChunks}.</li>
 * <li>{@code builtInPartitionerAppend} — no explicit partition, as a default producer sends. The only
 *     mode reaching {@code peekCurrentPartitionInfo}, {@code partitionChanged} and
 *     {@code updatePartitionInfo}.</li>
 * </ul>
 * <b>Strategies:</b> {@code full} is today's {@code buffer.memory} behaviour, a whole
 * {@code batch.size} buffer per batch; {@code full-lz4} the same compressed, which is a separate
 * branch of the per-append size estimate; {@code incremental} is {@link ChunkedRecordAccumulator} over
 * an {@link BufferPool.AllocationMode#INCREMENTAL} pool, taking
 * {@value ChunkedRecordAccumulator#CHUNK_SIZE}-byte chunks on demand. One parameter rather than a
 * strategy-by-compression grid, since the chunked accumulator rejects compression; add
 * {@code incremental-lz4} when it stops doing so.
 * <p>
 * <b>Reading allocation.</b> {@code gc.alloc.rate.norm} also counts the per-invocation reset
 * ({@link #resetAccumulator()}): 2-16% of the figure under {@code full}, but 45-90% under
 * {@code incremental}, whose batches are flattened as they drain. That pedestal is identical on both
 * sides of a revision comparison and cancels there; it does not cancel between strategies. Timing is
 * clean — JMH does not count fixture time.
 * <p>
 * <b>Not covered:</b> contention (see {@code ProducerAppendContentionBenchmark}); memory pressure, as
 * the pool never blocks, leaving {@code max.block.ms} and the incremental pool-exhausted fallback
 * unreached; records above {@code batch.size}; non-empty headers; and chunk extension at
 * {@code batchSize=16384}, where a batch is a single chunk — only 262144 extends, and only on 5.9% of
 * appends at {@code valueSize=1024} and 0.6% at 100.
 * <p>
 * Run a subset with, for example,
 * {@code jmh.sh -p strategy=full,incremental -p batchSize=16384 ProducerAppendPathBenchmark.steadyStateAppend}.
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
     * {@code incremental} a batch is one chunk and never extends; 262144 spans 16 and does. Under
     * {@code full} the larger size shifts the ratio of per-append to per-batch work.
     */
    @Param({"16384", "262144"})
    private int batchSize;

    /**
     * Value bytes, not total record size — the 3-byte key and V2 varint overhead add about 12 more,
     * giving ~112 and ~1036 byte records. Separates fixed per-append cost from per-byte cost.
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
     * the pool. Needed because nothing on the measured path drains, so batches would otherwise pile up
     * across an iteration's invocations and exhaust the pool. Draining rather than rebuilding leaves the
     * partition map and the pool's free list warm, as in a running producer.
     * <p>
     * Two passes for speed: {@code drain} takes one batch per partition per call but scans every
     * partition the snapshot puts on the node, so draining {@code steadyStateAppend}'s single partition
     * through the {@value #NUM_PARTITIONS}-partition snapshot costs 25 kB per batch. The small snapshot
     * clears that case first, the full one collects what the other modes leave.
     * <p>
     * The reset's allocation is charged to {@code gc.alloc.rate.norm}, though its time is not: 2.7-29.5 B
     * per record under {@code full}, but 116-1078 B under {@code incremental}, where draining closes each
     * batch and closing a chunked batch flattens its chunks into one buffer — roughly a record's worth
     * per record (see {@code ChunkedByteBufferOutputStream.buffer()}, KAFKA-20580). Measured by sampling
     * {@code getCurrentThreadAllocatedBytes} here; running the reset twice and differencing does not
     * measure it, since the second pass finds the deques already empty.
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
