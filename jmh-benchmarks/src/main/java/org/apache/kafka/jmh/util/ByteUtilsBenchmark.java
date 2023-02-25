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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.ByteUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class ByteUtilsBenchmark {
    static final int DATA_SET_SAMPLE_SIZE = 1024 * 1024;
    int[] random_ints;
    long[] random_longs;
    Random random;

    @Setup(Level.Trial)
    public void setUpBenchmarkLevel() {
        // Initialize the random number generator with a seed so that for each benchmark it produces the same sequence
        // of random numbers. Note that it is important to initialize it again with the seed before every benchmark.
        random = new Random(1337);
    }

    @Setup(Level.Iteration)
    public void setUp() {
        random_ints = random.ints(DATA_SET_SAMPLE_SIZE).toArray();
        random_longs = random.longs(DATA_SET_SAMPLE_SIZE).toArray();
    }

    @Benchmark
    public void testSizeOfUnsignedVarint(Blackhole bk) {
        for (int random_value : this.random_ints) {
            bk.consume(ByteUtils.sizeOfUnsignedVarint(random_value));
        }
    }

    @Benchmark
    public void testSizeOfUnsignedVarintNew(Blackhole bk) {
        for (int random_value : this.random_ints) {
            bk.consume(ByteUtils.sizeOfUnsignedVarintNew(random_value));
        }
    }

    @Benchmark
    public void testSizeOfVarlong(Blackhole bk) {
        for (long random_value : this.random_longs) {
            bk.consume(ByteUtils.sizeOfUnsignedVarlong(random_value));
        }
    }

    @Benchmark
    public void testSizeOfVarlongNew(Blackhole bk) {
        for (long random_value : this.random_longs) {
            bk.consume(ByteUtils.sizeOfUnsignedVarlongNew(random_value));
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteUtilsBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(opt).run();
    }
}
