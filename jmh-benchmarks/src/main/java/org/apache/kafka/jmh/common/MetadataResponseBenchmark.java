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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MetadataResponseBenchmark {

    @Param({"10", "500", "1000"})
    private int nodes;

    private MetadataResponse.PartitionMetadata metadata;
    private Map<Integer, Node> nodesById;

    @Setup
    public void setup() {
        metadata = new MetadataResponse.PartitionMetadata(Errors.UNKNOWN_SERVER_ERROR,
                new TopicPartition("benchmark", 42),
                Optional.of(4),
                Optional.of(42),
                IntStream.range(0, nodes).boxed().toList(),
                IntStream.range(0, nodes).filter(i1 -> i1 % 3 != 0).boxed().toList(),
                IntStream.range(0, nodes).filter(i2 -> i2 % 3 == 0).boxed().toList());
        nodesById = new HashMap<>(nodes);
        for (int i = 0; i < nodes; i++) {
            nodesById.put(i, new Node(i, "localhost", 1234));
        }
        nodesById = Collections.unmodifiableMap(nodesById);
    }

    @Benchmark
    public PartitionInfo toPartitionInfo() {
        return MetadataResponse.toPartitionInfo(metadata, nodesById);
    }

    public static void main(String[] args) throws RunnerException {
        new Runner(new OptionsBuilder().include(MetadataResponseBenchmark.class.getSimpleName()).build()).run();
    }

}
