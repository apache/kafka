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
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
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
 * Measures {@link RecordAccumulator#append} under the default ({@code full}) buffer.memory
 * allocation strategy, so the same source can be run on two revisions to check the producer's
 * default append path for regressions.
 * <p>
 * Uses only APIs that predate the incremental allocation strategy, and references nothing from it,
 * so this compiles and runs unchanged on a revision that does not have it.
 * <p>
 * Two modes:
 * <ul>
 * <li>{@code steadyStateAppend} — all records to one partition. Dominated by per-append work, but
 *     not purely per-append: batches still fill and get replaced, so about 1 append in 145 creates a
 *     batch at {@code valueSize=100}, and 1 in 15 at {@code valueSize=1024}.</li>
 * <li>{@code newBatchAppend} — one record per partition, so every append also creates a batch.
 *     Maximizes per-batch work, but read its <em>allocation</em> numbers with care: the per-partition
 *     deque map is a {@link org.apache.kafka.common.utils.internals.CopyOnWriteMap}, which copies the
 *     whole map on every insert, so filling 500 partitions costs on the order of 125k entry copies
 *     per invocation — several MB, dwarfing anything the append path itself allocates.</li>
 * </ul>
 * {@link Time#SYSTEM} is used rather than {@code MockTime} so clock calls cost what they cost in
 * production. Nothing drains, so no memory is ever returned to the pool, and the pool is sized so no
 * append ever blocks. ({@code lingerMs} is {@code Integer.MAX_VALUE} for good measure, but
 * {@code append} never reads it — readiness is only evaluated in {@code ready()} and {@code drain()},
 * which this benchmark does not call.)
 * <p>
 * Limitations:
 * <ul>
 * <li>It passes an <em>explicit</em> partition, so {@code partitionInfo} is always null:
 *     {@code peekCurrentPartitionInfo} is never called and {@code updatePartitionInfo} returns
 *     immediately. A default producer sending without an explicit partition goes through both, so a
 *     regression confined to the sticky-partitioner path would not show up here.</li>
 * <li>It uses the {@link RecordAccumulator} constructor which defaults
 *     {@code PartitionerConfig} to {@code (false, 0, false, "")}, where a default producer builds
 *     {@code enableAdaptivePartitioning = true}. That field is read only in {@code partitionReady},
 *     on the drain path this benchmark never invokes, so it cannot affect these measurements. The
 *     13-argument constructor exists on both revisions, so switching to it would cost no
 *     cross-revision compatibility.</li>
 * </ul>
 */
@State(Scope.Benchmark)
@Fork(value = 3, jvmArgs = {"-Xmx3g"})
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ProducerAppendPathBenchmark {

    private static final String TOPIC = "test";
    private static final long TOTAL_MEMORY = 512 * 1024 * 1024L;
    private static final int STEADY_STATE_RECORDS = 10_000;
    /** One append per partition in newBatchAppend, so this is also that mode's record count. */
    private static final int NUM_PARTITIONS = 500;
    /**
     * Above the worst-case batch count of either mode (~670, for 10k records of 1024 bytes into
     * 16KB batches), so no batch creation misses the free list.
     */
    private static final int POOL_WARM_BUFFERS = 800;

    /**
     * The producer default. 262144 was dropped once compression was added, to keep the run time
     * flat: it is not a default, and the question here is specifically about the default path.
     */
    @Param({"16384"})
    private int batchSize;

    /**
     * Uncompressed and compressed are separate code paths in
     * {@code MemoryRecordsBuilder.estimatedBytesWritten}, which is on the per-append path, so both
     * need covering. Note the compressor does real work as each record is written, which reduces
     * sensitivity to per-append effects — a null result under lz4 is weaker evidence than one under
     * none. Measured here, lz4 allocates 1.1-8.9 kB per record against none's 149-181 bytes.
     */
    @Param({"none", "lz4"})
    private String compressionType;

    /**
     * Size in bytes of the record's value. Not the total record size: the key is a fixed 3 bytes and
     * V2 record overhead adds about another 9 bytes of varints (length, attributes, timestamp and
     * offset deltas, key and value lengths, header count), so the total is ~112 bytes at
     * {@code valueSize=100} and ~1036 at 1024. Parameterized to separate fixed per-append cost from
     * per-byte cost — a fixed cost is a much larger share of a small record.
     */
    @Param({"100", "1024"})
    private int valueSize;

    private RecordAccumulator accum;
    private Metrics metrics;
    private Cluster cluster;
    private Time time;
    private byte[] key;
    private byte[] value;
    private List<ByteBuffer> warmBuffers;

    /**
     * Allocated once per fork and reused, so seeding each invocation's pool costs no allocation.
     * Note this inflates the pool's {@code availableMemory()} bookkeeping, since these buffers were
     * never handed out by it — harmless here because the pool is sized so nothing ever blocks.
     */
    @Setup(Level.Trial)
    public void setupTrial() {
        warmBuffers = new ArrayList<>(POOL_WARM_BUFFERS);
        for (int i = 0; i < POOL_WARM_BUFFERS; i++)
            warmBuffers.add(ByteBuffer.allocate(batchSize));
    }

    @Setup(Level.Invocation)
    public void setup() {
        time = Time.SYSTEM;
        metrics = new Metrics(time);
        cluster = createTestCluster();
        key = "key".getBytes(StandardCharsets.UTF_8);
        value = new byte[valueSize];
        Compression compression = "lz4".equals(compressionType)
                ? Compression.lz4().build()
                : Compression.NONE;
        BufferPool pool = new BufferPool(TOTAL_MEMORY, batchSize, metrics, time, "producer-metrics");
        // Seed the free list so batch creation hits free.pollFirst() rather than a raw
        // ByteBuffer.allocate plus zeroing, as it would in a warm producer. Nothing in this
        // benchmark drains, so without this no buffer is ever returned and every batch pays an
        // allocation production does not — enough to swamp the per-batch effects this benchmark
        // exists to detect. The buffers themselves come from setupTrial, so reseeding adds no
        // per-invocation *buffer* allocation, though it does regrow the pool's free deque.
        // Everything else built below (Metrics, the 500-partition Cluster, the pool, the
        // accumulator) is rebuilt per invocation, and JMH's gc profiler counts that in
        // gc.alloc.rate.norm — so read the absolute B/op as an upper bound on what append itself
        // allocates. It cancels when comparing two revisions, which is what this benchmark is for.
        for (ByteBuffer warm : warmBuffers)
            pool.deallocate(warm);
        accum = new RecordAccumulator(
            new LogContext(),
            batchSize,
            compression,
            Integer.MAX_VALUE,  // lingerMs, so nothing ever becomes ready
            100L,               // retryBackoffMs
            1000L,              // retryBackoffMaxMs
            3200,               // deliveryTimeoutMs
            metrics,
            "producer-metrics",
            time,
            null,               // transactionManager
            pool
        );
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        if (accum != null)
            accum.close();
        if (metrics != null)
            metrics.close();
    }

    /**
     * Mostly per-append work: records go to one partition, so an append lands in the already-open
     * batch unless it has filled (about 1 in 145 at {@code valueSize=100}, 1 in 15 at 1024).
     */
    @Benchmark
    public void steadyStateAppend(Blackhole blackhole) throws InterruptedException {
        for (int i = 0; i < STEADY_STATE_RECORDS; i++) {
            blackhole.consume(accum.append(TOPIC, 0, 0L, key, value, Record.EMPTY_HEADERS,
                    null, 1000L, time.milliseconds(), cluster));
        }
    }

    /**
     * Per-batch work: each record goes to a fresh partition, so every append creates a batch. See the
     * class javadoc before reading this mode's allocation numbers — the CopyOnWriteMap insert cost
     * dominates them.
     */
    @Benchmark
    public void newBatchAppend(Blackhole blackhole) throws InterruptedException {
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            blackhole.consume(accum.append(TOPIC, partition, 0L, key, value, Record.EMPTY_HEADERS,
                    null, 1000L, time.milliseconds(), cluster));
        }
    }

    private Cluster createTestCluster() {
        Node node = new Node(0, "localhost", 1111);
        Map<Integer, Node> nodes = Stream.of(node).collect(Collectors.toMap(Node::id, Function.identity()));
        List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>(NUM_PARTITIONS);
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
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
        MetadataSnapshot metadataCache = new MetadataSnapshot(
            null,
            nodes,
            partitions,
            Set.of(),
            Set.of(),
            Set.of(),
            null,
            Map.of()
        );
        return metadataCache.cluster();
    }
}
