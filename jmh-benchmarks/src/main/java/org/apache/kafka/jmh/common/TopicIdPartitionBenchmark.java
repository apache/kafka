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
package org.apache.kafka.jmh.common;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.common.TopicIdPartition;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class TopicIdPartitionBenchmark {
    private static final int UUID_SIZE = 100;
    private static final int PARTITION_SIZE = 100000;
    private final List<Uuid> uuids = new ArrayList<>(UUID_SIZE);
    private final HashSet<TopicIdPartition> topicIdPartitions = new HashSet<>();
    private final HashSet<TopicIdPartitionOldImplementation> oldTopicIdPartitions = new HashSet<>();

    @Setup(Level.Trial)
    public void setUp() {
        IntStream.range(0, UUID_SIZE).forEach(i -> {
            Uuid uuid = Uuid.randomUuid();
            uuids.add(uuid);
            IntStream.range(0, PARTITION_SIZE).forEach(j -> {
                topicIdPartitions.add(new TopicIdPartition(uuid, j));
                oldTopicIdPartitions.add(new TopicIdPartitionOldImplementation(uuid, j));
            });
        });
    }

    @Benchmark
    public void recordBenchmark(Blackhole blackhole) {
        for (int i = 0; i < PARTITION_SIZE; i++) {
            int uuidIndex = ThreadLocalRandom.current().nextInt(UUID_SIZE);
            int partitionId = ThreadLocalRandom.current().nextInt(PARTITION_SIZE);
            Uuid uuid = uuids.get(uuidIndex);
            TopicIdPartition tp = new TopicIdPartition(uuid, partitionId);
            blackhole.consume(topicIdPartitions.contains(tp));
        }
    }

    @Benchmark
    public void oldImplBenchmark(Blackhole blackhole) {
        for (int i = 0; i < PARTITION_SIZE; i++) {
            int uuidIndex = ThreadLocalRandom.current().nextInt(UUID_SIZE);
            int partitionId = ThreadLocalRandom.current().nextInt(PARTITION_SIZE);
            Uuid uuid = uuids.get(uuidIndex);
            TopicIdPartitionOldImplementation oldTp = new TopicIdPartitionOldImplementation(uuid, partitionId);
            blackhole.consume(oldTopicIdPartitions.contains(oldTp));
        }
    }
}

class TopicIdPartitionOldImplementation {
    private final Uuid topicId;
    private final int partitionId;

    public TopicIdPartitionOldImplementation(Uuid topicId, int partitionId) {
        this.topicId = topicId;
        this.partitionId = partitionId;
    }

    public Uuid topicId() {
        return topicId;
    }

    public int partitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TopicIdPartitionOldImplementation)) return false;
        TopicIdPartitionOldImplementation other = (TopicIdPartitionOldImplementation) o;
        return other.topicId.equals(topicId) && other.partitionId == partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicId, partitionId);
    }

    @Override
    public String toString() {
        return topicId + ":" + partitionId;
    }
}