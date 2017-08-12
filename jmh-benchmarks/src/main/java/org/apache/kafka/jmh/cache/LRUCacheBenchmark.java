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

import org.apache.kafka.common.cache.LRUCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
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
public class LRUCacheBenchmark {

    private LRUCache<String, String> lruCache;

    private final String key = "the_key_to_use";
    private final String value = "the quick brown fox jumped over the lazy dog the olympics are about to start";
    int counter;


    @Setup(Level.Trial)
    public void setUpCaches() {
        lruCache = new LRUCache<>(100);
    }

    @Benchmark
    public String testCachePerformance() {
        counter++;
        lruCache.put(key + counter, value + counter);
        return lruCache.get(key + counter);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LRUCacheBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(opt).run();
    }

}
