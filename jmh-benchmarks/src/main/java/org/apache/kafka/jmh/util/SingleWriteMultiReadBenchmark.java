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
import org.apache.kafka.server.immutable.ImmutableMap;
import org.apache.kafka.server.immutable.pcollections.PCollectionsImmutableMap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Group)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SingleWriteMultiReadBenchmark {
    private static final int TIMES = 100_000;

    @Param({"100"})
    private int mapSize;

    @Param({"0.1"})
    private double writePercentage;

    private Map<Integer, Integer> concurrentHashMap;
    private Map<Integer, Integer> copyOnWriteMap;

    private int writeTimes;
    private volatile ImmutableMap<Integer, Integer> pcollectionsImmutableMap;

    static final String PCOLLECTIONSIMMUTABLEMAPGROUP = "PcollectionsImmutableMap";
    static final String CONCURRENTHASHMAPGROUP = "ConcurrentHashMap";
    static final String COPYONWRITEGROUP = "ConcurrentHashMap";

    @Setup(Level.Invocation)
    public void setup() {
        Map<Integer, Integer> mapTemplate = IntStream.range(0, mapSize).boxed()
                .collect(Collectors.toMap(i -> i, i -> i));
        concurrentHashMap = new ConcurrentHashMap<>(mapTemplate);
        copyOnWriteMap = new CopyOnWriteMap<>(mapTemplate);
        pcollectionsImmutableMap = PCollectionsImmutableMap.empty();
        mapTemplate.forEach((key, value) ->
                pcollectionsImmutableMap = pcollectionsImmutableMap.updated(key, value)
        );
        writeTimes = (int) Math.round(writePercentage * TIMES);
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(PCOLLECTIONSIMMUTABLEMAPGROUP)
    @GroupThreads(10)
    public void testPcollectionsImmutableMapGet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            blackhole.consume(pcollectionsImmutableMap.get(i % mapSize));
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(PCOLLECTIONSIMMUTABLEMAPGROUP)
    @GroupThreads(10)
    public void testPcollectionsImmutableMapRandomGet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            blackhole.consume(pcollectionsImmutableMap.get(ThreadLocalRandom.current().nextInt(0, mapSize + 1)));
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(PCOLLECTIONSIMMUTABLEMAPGROUP)
    @GroupThreads(10)
    public void testPcollectionsImmutableMapValues(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            for (int value : pcollectionsImmutableMap.values()) {
                blackhole.consume(value);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(PCOLLECTIONSIMMUTABLEMAPGROUP)
    @GroupThreads(10)
    public void testPcollectionsImmutableMapEntry(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            for (Map.Entry<Integer, Integer> entry : pcollectionsImmutableMap.entrySet()) {
                blackhole.consume(entry);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(PCOLLECTIONSIMMUTABLEMAPGROUP)
    @GroupThreads(1)
    public void testPcollectionsImmutableMapWrite() {
        for (int i = 0; i < writeTimes; i++) {
            pcollectionsImmutableMap = pcollectionsImmutableMap.updated(i + mapSize, 0);
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(CONCURRENTHASHMAPGROUP)
    @GroupThreads(10)
    public void testConcurrentHashMapGet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            blackhole.consume(concurrentHashMap.get(i % mapSize));
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(CONCURRENTHASHMAPGROUP)
    @GroupThreads(10)
    public void testConcurrentHashMapRandomGet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            blackhole.consume(concurrentHashMap.get(ThreadLocalRandom.current().nextInt(0, mapSize + 1)));
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(CONCURRENTHASHMAPGROUP)
    @GroupThreads(10)
    public void testConcurrentHashMapValues(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            for (int value : concurrentHashMap.values()) {
                blackhole.consume(value);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(CONCURRENTHASHMAPGROUP)
    @GroupThreads(10)
    public void testConcurrentHashMapEntry(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            for (Map.Entry<Integer, Integer> entry : concurrentHashMap.entrySet()) {
                blackhole.consume(entry);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(CONCURRENTHASHMAPGROUP)
    @GroupThreads(1)
    public void testConcurrentHashMapWrite() {
        for (int i = 0; i < writeTimes; i++) {
            concurrentHashMap.put(i + mapSize, 0);
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(COPYONWRITEGROUP)
    @GroupThreads(10)
    public void testCopyOnWriteMapGet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            blackhole.consume(copyOnWriteMap.get(i % mapSize));
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(COPYONWRITEGROUP)
    @GroupThreads(10)
    public void testCopyOnWriteMapRandomGet(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            blackhole.consume(copyOnWriteMap.get(ThreadLocalRandom.current().nextInt(0, mapSize + 1)));
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(COPYONWRITEGROUP)
    @GroupThreads(10)
    public void testCopyOnWriteMapValues(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            for (int value : copyOnWriteMap.values()) {
                blackhole.consume(value);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(COPYONWRITEGROUP)
    @GroupThreads(10)
    public void testCopyOnWriteMapEntry(Blackhole blackhole) {
        for (int i = 0; i < TIMES; i++) {
            for (Map.Entry<Integer, Integer> entry : copyOnWriteMap.entrySet()) {
                blackhole.consume(entry);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(TIMES)
    @Group(COPYONWRITEGROUP)
    @GroupThreads(1)
    public void testCopyOnWriteMapWrite() {
        for (int i = 0; i < writeTimes; i++) {
            copyOnWriteMap.put(i + mapSize, 0);
        }
    }
}
