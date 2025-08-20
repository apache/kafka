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
package org.apache.kafka.jmh.metrics;

import org.apache.kafka.coordinator.common.runtime.HdrHistogram;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Threads(10)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class HistogramBenchmark {

    /*
     * This benchmark compares the performance of the most commonly used in the Kafka codebase
     * Yammer histogram and the new HdrHistogram. It does it by focusing on the write path in a
     * multiple writers, multiple readers scenario.
     *
     * The benchmark relies on JMH Groups which allows us to distribute the number of worker threads
     * to the different benchmark methods.
     */

    private static final long MAX_VALUE = TimeUnit.MINUTES.toMillis(1L);

    private Histogram yammerHistogram;
    private HdrHistogram hdrHistogram;

    @Setup(Level.Trial)
    public void setUp() {
        yammerHistogram = KafkaYammerMetrics.defaultRegistry().newHistogram(new MetricName("a", "", ""), true);
        hdrHistogram = new HdrHistogram(MAX_VALUE, 3);
    }

    /*
     * The write benchmark methods below are the core of the benchmark. They use ThreadLocalRandom
     * to generate values to record. This is much faster than the actual histogram recording, so
     * the benchmark results are representative of the histogram implementation.
     */

    @Benchmark
    @Group("runner")
    @GroupThreads(3)
    public void writeYammerHistogram() {
        yammerHistogram.update(ThreadLocalRandom.current().nextLong(MAX_VALUE));
    }

    @Benchmark
    @Group("runner")
    @GroupThreads(3)
    public void writeHdrHistogram() {
        hdrHistogram.record(ThreadLocalRandom.current().nextLong(MAX_VALUE));
    }

    /*
     * The read benchmark methods below are not real benchmark methods!
     * They are there only to simulate the concurrent exercise of the read and the write paths in
     * the histogram implementations (with the read path exercised significantly less often). The
     * measurements for these benchmark methods should be ignored as although not optimized away
     * (that's why the methods have a return value), they practically measure the cost of a
     * System.currentTimeMillis() call.
     */

    @Benchmark
    @Group("runner")
    @GroupThreads(2)
    public double readYammerHistogram() {
        long now = System.currentTimeMillis();
        if (now % 199 == 0) {
            return yammerHistogram.getSnapshot().get999thPercentile();
        }
        return now;
    }

    @Benchmark
    @Group("runner")
    @GroupThreads(2)
    public double readHdrHistogram() {
        long now = System.currentTimeMillis();
        if (now % 199 == 0) {
            return hdrHistogram.measurePercentile(now, 99.9);
        }
        return now;
    }
}
