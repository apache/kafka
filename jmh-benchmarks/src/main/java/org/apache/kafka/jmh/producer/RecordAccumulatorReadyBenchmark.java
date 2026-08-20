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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.utils.internals.LogContext;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link RecordAccumulator#ready} (which drives {@code partitionReady()} internally),
 * the method the Sender background thread calls on every {@code runOnce()} cycle to decide which
 * nodes have data ready to send. This loop runs once per partition per cycle regardless of
 * traffic, so its per-partition cost matters most for producers with many partitions.
 *
 * <p>Only a small fraction of partitions have a pending batch, matching the common case where
 * most deques are empty on any given {@code runOnce()} cycle. Adaptive partitioning is
 * parameterized: when enabled, every partition requires some bookkeeping even with an empty
 * deque; when disabled, partitions with an empty deque can be skipped almost entirely.
 *
 * <p>Partitions are spread across many topics ({@code test-topic-<n>}, 10 partitions each)
 * instead of one giant topic, so per-partition lookups keyed by {@link TopicPartition} (e.g.
 * {@code Cluster.leaderFor()}, {@code MetadataSnapshot.leaderEpochFor()}) exercise a realistic
 * multi-topic key distribution instead of a single-topic one.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RecordAccumulatorReadyBenchmark {

    @Param({"100", "1000"})
    private int numOfTopics;

    @Param({"false", "true"})
    private boolean adaptivePartitioning;

    private static final int PARTITIONS_PER_TOPIC = 10;

    private RecordAccumulator accumulator;
    private MetadataSnapshot metadataSnapshot;
    private Metrics metrics;

    @Setup(Level.Trial)
    public void setup() throws InterruptedException {
        int partitionCount = numOfTopics * PARTITIONS_PER_TOPIC;

        // Only 1% of partitions (at least 1) have a pending batch; the rest have an empty deque,
        // matching a producer that's only actively sending to a small subset of its partitions
        // at any moment.
        int partitionsWithPendingBatch = Math.max(1, partitionCount / 100);

        List<String> topics = new ArrayList<>(numOfTopics);
        for (int t = 0; t < numOfTopics; t++) {
            topics.add("test-topic-" + t);
        }

        Node node = new Node(0, "localhost", 9092);
        List<Node> nodes = Collections.singletonList(node);

        List<PartitionInfo> partitionInfos = new ArrayList<>(partitionCount);
        List<PartitionMetadata> partitionMetadatas = new ArrayList<>(partitionCount);
        for (String topic : topics) {
            for (int p = 0; p < PARTITIONS_PER_TOPIC; p++) {
                Node[] replicas = new Node[] {node};
                partitionInfos.add(new PartitionInfo(topic, p, node, replicas, replicas));
                List<Integer> replicaIds = Collections.singletonList(node.id());
                partitionMetadatas.add(new PartitionMetadata(Errors.NONE, new TopicPartition(topic, p),
                    Optional.of(node.id()), Optional.empty(), replicaIds, replicaIds,
                    Collections.emptyList()));
            }
        }
        Cluster cluster = new Cluster("cluster-id", nodes, partitionInfos,
            Collections.emptySet(), Collections.emptySet());

        Map<Integer, Node> nodesById = new HashMap<>();
        nodesById.put(node.id(), node);
        metadataSnapshot = new MetadataSnapshot(null, nodesById, partitionMetadatas,
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null,
            Collections.emptyMap(), cluster);

        LogContext logContext = new LogContext();
        metrics = new Metrics(Time.SYSTEM);
        int batchSize = 16 * 1024;
        long totalMemory = (long) (partitionsWithPendingBatch + 16) * batchSize;
        RecordAccumulator.PartitionerConfig partitionerConfig =
            new RecordAccumulator.PartitionerConfig(adaptivePartitioning, 0, false, null);
        accumulator = new RecordAccumulator(
            logContext,
            batchSize,
            Compression.NONE,
            0,
            100L,
            1000L,
            120_000,
            partitionerConfig,
            metrics,
            "producer-metrics",
            Time.SYSTEM,
            null,
            new BufferPool(totalMemory, batchSize, metrics, Time.SYSTEM, "producer-metrics"));

        // Only give a subset of partitions a pending batch; the rest keep an empty deque so
        // ready() takes its early-exit path for them, matching the common case.
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[100];
        long nowMs = Time.SYSTEM.milliseconds();
        for (int i = 0; i < partitionsWithPendingBatch; i++) {
            String topic = topics.get(i / PARTITIONS_PER_TOPIC);
            int partition = i % PARTITIONS_PER_TOPIC;
            accumulator.append(topic, partition, 0L, key, value, null, null, 0L, nowMs, cluster);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        metrics.close();
    }

    @Benchmark
    public RecordAccumulator.ReadyCheckResult ready() {
        return accumulator.ready(metadataSnapshot, Time.SYSTEM.milliseconds());
    }
}
