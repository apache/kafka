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

package org.apache.kafka.jmh.cache;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.cache.LRUCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This is a simple example of a JMH benchmark.
 *
 * The sample code provided by the JMH project is a great place to start learning how to write correct benchmarks:
 * http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LRUCacheBenchmark {

    private static final int DISTINCT_KEYS = 10_000;

    private static final String KEY = "the_key_to_use";

    private static final String VALUE = "the quick brown fox jumped over the lazy dog the olympics are about to start";

    private final String[] keys = new String[DISTINCT_KEYS];

    private final String[] values = new String[DISTINCT_KEYS];

    private LRUCache<String, String> lruCache;

    private long counter = 0;

    @Setup(Level.Trial)
    public void setUp() {
        for (int i = 0; i < DISTINCT_KEYS; ++i) {
            keys[i] = KEY + i;
            values[i] = VALUE + i;
        }
        lruCache = new LRUCache<>(100);
    }

    @Benchmark
    public String testCachePerformance() {
        counter++;
        int index = (int) (counter % DISTINCT_KEYS);
        String hashkey = keys[index];
        lruCache.put(hashkey, values[index]);
        return lruCache.get(hashkey);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LRUCacheBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(opt).run();
    }

}
