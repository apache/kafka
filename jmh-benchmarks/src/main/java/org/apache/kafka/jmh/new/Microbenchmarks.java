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
package org.apache.kafka.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

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

import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class Microbenchmarks {

    @Param({"10", "50", "100", "500", "1000", "5000", "10000"})
    private int threadCount;
    
    @Param({"1000000"})
    private int iters;

    @Benchmark
    public void main() throws Exception {

        Map<String, Integer> values = new HashMap<>();
        for (int i = 0; i < 100; i++)
            values.put(Integer.toString(i), i);
        // System.out.println("HashMap:");
        benchMap(threadCount, iters, values);
        // System.out.println("ConcurrentHashMap:");
        // benchMap(2, 1000000, new ConcurrentHashMap<>(values));
        // System.out.println("CopyOnWriteMap:");
        // benchMap(2, 1000000, new CopyOnWriteMap<>(values));
    }


    private static void benchMap(int numThreads, final int iters, final Map<String, Integer> map) throws Exception {
        final List<String> keys = new ArrayList<>(map.keySet());
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread() {
                public void run() {
                    long start = System.nanoTime();
                    for (int j = 0; j < iters; j++)
                        map.get(keys.get(j % threads.size()));
                    System.out.println("Map access time: " + ((System.nanoTime() - start) / (double) iters));
                }
            });
        }
        for (Thread thread : threads)
            thread.start();
        for (Thread thread : threads)
            thread.join();
    }

    private static long systemMillis(int iters) {
        long total = 0;
        for (int i = 0; i < iters; i++)
            total += System.currentTimeMillis();
        return total;
    }

    private static long systemNanos(int iters) {
        long total = 0;
        for (int i = 0; i < iters; i++)
            total += System.currentTimeMillis();
        return total;
    }

}
