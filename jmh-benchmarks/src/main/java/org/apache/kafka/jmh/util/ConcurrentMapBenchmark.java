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

import org.apache.kafka.common.utils.CopyOnWriteMap;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This benchmark compares the performance of CopyOnWriteMap and ConcurrentHashMap
 * under a low-write scenario. We use computeIfAbsent as the write operation,
 * since it is the only CopyOnWriteMap write operation used in the code base and
 * constitutes 10% of the operations. The benchmark tests the performance of
 * get, values, and entrySet methods, as these methods are also used in the code.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(2)
public class ConcurrentMapBenchmark {
    private static final int TIMES = 100_000;

    @Param({"100"})
    private int mapSize;

    @Param({"0.1"})
    private double writePercentage;

    private Map<Integer, Integer> concurrentHashMap;
    private Map<Integer, Integer> copyOnWriteMap;
    private int writePerLoops;

    @Setup(Level.Invocation)
    public void setup() {
        Map<Integer, Integer> mapTemplate = IntStream.range(0, mapSize).boxed()
                .collect(Collectors.toMap(i -> i, i -> i));
        concurrentHashMap = new ConcurrentHashMap<>(mapTemplate);
        copyOnWriteMap = new CopyOnWriteMap<>(mapTemplate);
        writePerLoops = TIMES / (int) Math.round(writePercentage * TIMES);
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testConcurrentHashMapGet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            if (i % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                concurrentHashMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                blackhole.consume(concurrentHashMap.get(i % mapSize));
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testConcurrentHashMapGetRandom(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            if (i % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                concurrentHashMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                blackhole.consume(concurrentHashMap.get(ThreadLocalRandom.current().nextInt(0, mapSize + 1)));
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testCopyOnWriteMapGet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            if (i % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                copyOnWriteMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                blackhole.consume(copyOnWriteMap.get(i % mapSize));
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testCopyOnWriteMapGetRandom(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            if (i % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                copyOnWriteMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                blackhole.consume(copyOnWriteMap.get(ThreadLocalRandom.current().nextInt(0, mapSize + 1)));
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testConcurrentHashMapValues(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            if (i % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                concurrentHashMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                for (int value : concurrentHashMap.values()) {
                    blackhole.consume(value);
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testCopyOnWriteMapValues(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            if (i % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                copyOnWriteMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                for (int value : copyOnWriteMap.values()) {
                    blackhole.consume(value);
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testConcurrentHashMapEntrySet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            if (i % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                concurrentHashMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                for (Map.Entry<Integer, Integer> entry : concurrentHashMap.entrySet()) {
                    blackhole.consume(entry);
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testCopyOnWriteMapEntrySet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            if (i % writePerLoops == 0) {
                // add offset mapSize to ensure computeIfAbsent do add new entry
                copyOnWriteMap.computeIfAbsent(i + mapSize, key -> key);
            } else {
                for (Map.Entry<Integer, Integer> entry : copyOnWriteMap.entrySet()) {
                    blackhole.consume(entry);
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testConcurrentHashMapComputeIfAbsentReadOnly(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            blackhole.consume(concurrentHashMap.computeIfAbsent(
                    ThreadLocalRandom.current().nextInt(0, mapSize),
                    newValue -> Integer.MAX_VALUE
            ));
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    public void testConcurrentHashMapGetReadOnly(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            blackhole.consume(concurrentHashMap.get(ThreadLocalRandom.current().nextInt(0, mapSize)));
        }
    }
}
