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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class TopicIdPartitionBenchmark {
    private TopicIdPartition topicIdPartition1;
    private TopicIdPartition topicIdPartition2;
    private TopicIdPartitionOldImplementation oldTopicIdPartition1;
    private TopicIdPartitionOldImplementation oldTopicIdPartition2;


    @Setup(Level.Trial)
    public void setUp() {
        Uuid topicId = Uuid.randomUuid();
        topicIdPartition1 = new TopicIdPartition(topicId, 1);
        topicIdPartition2 = new TopicIdPartition(topicId, 1);
        oldTopicIdPartition1 = new TopicIdPartitionOldImplementation(topicId, 1);
        oldTopicIdPartition2 = new TopicIdPartitionOldImplementation(topicId, 1);
    }

    @Benchmark
    public void recordEqualsBenchmark(Blackhole blackhole) {
        blackhole.consume(topicIdPartition1.equals(topicIdPartition2));
    }

    @Benchmark
    public void oldImplEqualsBenchmark(Blackhole blackhole) {
        blackhole.consume(oldTopicIdPartition1.equals(oldTopicIdPartition2));
    }

    @Benchmark
    public void recordHashCodeBenchmark(Blackhole blackhole) {
        blackhole.consume(topicIdPartition1.hashCode());
    }

    @Benchmark
    public void oldImplHashCodeBenchmark(Blackhole blackhole) {
        blackhole.consume(oldTopicIdPartition1.hashCode());
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