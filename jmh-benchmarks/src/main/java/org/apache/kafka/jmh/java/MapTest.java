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

package org.apache.kafka.jmh.java;

import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(2)
public class MapTest {
    public static final int TIMES = 1_000_000;
    private static final int MAP_LENGTH = 100;
    private static final Map<String, Integer> MAP_TEMPLATE = IntStream.range(0, MAP_LENGTH).boxed()
            .collect(Collectors.toMap(i -> Integer.toString(i), i -> i));
    private static final List<String> KEYS = new ArrayList<>(MAP_TEMPLATE.keySet());

    Map<String, Integer> hashMap = new HashMap<>(MAP_TEMPLATE);
    Map<String, Integer> concurrentHashMap = new ConcurrentHashMap<>(MAP_TEMPLATE);
    Map<String, Integer> copyOnWriteMap = new CopyOnWriteMap<>(MAP_TEMPLATE);

    @Benchmark
    public void testHashMap(Blackhole blackhole) {
        IntStream.range(0, TIMES).forEach(x ->
                blackhole.consume(hashMap.get(KEYS.get(x % MAP_LENGTH))));
    }

    @Benchmark
    public void testConcurrentHashMap(Blackhole blackhole) {
        IntStream.range(0, TIMES).forEach(x ->
                blackhole.consume(concurrentHashMap.get(KEYS.get(x % MAP_LENGTH))));
    }

    @Benchmark
    public void testCopyOnWriteMap(Blackhole blackhole) {
        IntStream.range(0, TIMES).forEach(x ->
                blackhole.consume(copyOnWriteMap.get(KEYS.get(x % MAP_LENGTH))));
    }
}
