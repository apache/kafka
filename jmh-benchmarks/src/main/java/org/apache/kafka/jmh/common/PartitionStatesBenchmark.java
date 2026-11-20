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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PartitionStatesBenchmark {
    @Param({"22", "125", "1000"})
    private int partitionCount;

    private PartitionStates<Long> states;
    private TopicPartition[] partitions;
    private Long[] values;
    private int next;

    @Setup(Level.Trial)
    public void setUp() {
        states = new PartitionStates<>();
        partitions = new TopicPartition[partitionCount];
        values = new Long[1024];
        for (int i = 0; i < values.length; i++)
            values[i] = (long) i + 1_000_000L;
        for (int i = 0; i < partitionCount; i++) {
            partitions[i] = new TopicPartition("topic-" + (i % 10), i);
            states.updateAndMoveToEnd(partitions[i], values[0]);
        }
    }

    private int nextIndex() {
        next = next + 1 == partitionCount ? 0 : next + 1;
        return next;
    }

    // What the fetcher does for each partition that returned data.
    @Benchmark
    public void updateAndMoveToEnd() {
        states.updateAndMoveToEnd(partitions[nextIndex()], values[next & 1023]);
    }

    // What the fetcher does before processing each partition of a response.
    @Benchmark
    public Long stateValue() {
        return states.stateValue(partitions[nextIndex()]);
    }

    // What buildFetch does once per fetch request.
    @Benchmark
    public void forEach(Blackhole bh) {
        states.partitionStateMap().forEach((tp, state) -> bh.consume(state));
    }
}
