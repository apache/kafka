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

import java.util.concurrent.ThreadLocalRandom;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ByteUtilsBenchmark {
    private int input;

    @Setup(Level.Iteration)
    public void setUp() {
        input = ThreadLocalRandom.current().nextInt(2 * 1024 * 1024);
    }

    @Benchmark
    @Fork(3)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    public long testSizeOfUnsignedVarint() {
        return ByteUtils.sizeOfUnsignedVarint(input);
    }

    @Benchmark
    @Fork(3)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    public long testSizeOfUnsignedVarintMath() {
        int leadingZeros = Integer.numberOfLeadingZeros(input);
        return (38 - leadingZeros) / 7 + leadingZeros / 32;
    }

    @Benchmark
    @Fork(3)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 10, time = 1)
    public long testSizeOfUnsignedVarintOriginal() {
        int value = input;
        int bytes = 1;
        // use highestOneBit or numberOfLeadingZeros
        while ((value & 0xffffff80) != 0L) {
            bytes += 1;
            value >>>= 7;
        }
        return bytes;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteUtilsBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(opt).run();
    }



}
