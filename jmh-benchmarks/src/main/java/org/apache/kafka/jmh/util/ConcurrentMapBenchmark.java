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

package org.apache.kafka.jmh.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.utils.CopyOnWriteMap;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(2)
public class ConcurrentMapBenchmark {
    @Param({"1000000"})
    private int times;

    @Param({"100"})
    private int mapSize;

    // execute 1 computeIfAbsent per 10000 loops
    @Param({"10000"})
    private int writePerLoops;

    private int counter;
    private Map<Integer, Integer> concurrentHashMap;
    private Map<Integer, Integer> copyOnWriteMap;

    @Setup
    public void setup() {
        counter = 0;
        Map<Integer, Integer> mapTemplate = IntStream.range(0, mapSize).boxed()
                .collect(Collectors.toMap(i -> i, i -> i));
        concurrentHashMap = new ConcurrentHashMap<>(mapTemplate);
        copyOnWriteMap = new CopyOnWriteMap<>(mapTemplate);
    }

    @Benchmark
    public void testConcurrentHashMap(Blackhole blackhole) {
        for (int i = 0; i < times; i++) {
            counter++;
            if (counter % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                concurrentHashMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                blackhole.consume(concurrentHashMap.get(i % mapSize));
            }
        }
    }

    @Benchmark
    public void testCopyOnWriteMap(Blackhole blackhole) {
        for (int i = 0; i < times; i++) {
            counter++;
            if (counter % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                copyOnWriteMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                blackhole.consume(copyOnWriteMap.get(i % mapSize));
            }
        }
    }
}
