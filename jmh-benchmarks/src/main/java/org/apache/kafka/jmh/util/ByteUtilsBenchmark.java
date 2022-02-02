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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ByteUtilsBenchmark {
    private static final int INPUT_COUNT = 10_000;
    private static final int MAX_INT = 2 * 1024 * 1024;

    private final int[] sizeOfInputs = new int[INPUT_COUNT];

    @Setup(Level.Trial)
    public void setUp() {
        for (int i = 0; i < INPUT_COUNT; ++i) {
            sizeOfInputs[i] = ThreadLocalRandom.current().nextInt(MAX_INT);
        }
    }

    @Benchmark
    public long testSizeOfUnsignedVarint() {
        long result = 0;
        for (final int input : sizeOfInputs) {
            result += ByteUtils.sizeOfUnsignedVarint(input);
        }
        return result;
    }

    @Benchmark
    public long testSizeOfUnsignedVarintMath() {
        long result = 0;
        for (final int input : sizeOfInputs) {
             int leadingZeros = Integer.numberOfLeadingZeros(input);
             result += (38 - leadingZeros) / 7 + leadingZeros / 32;
        }
        return result;
    }

    @Benchmark
    public long testSizeOfUnsignedVarintOriginal() {
        long result = 0;
        for (int input : sizeOfInputs) {
            int bytes = 1;
            // use highestOneBit or numberOfLeadingZeros
            while ((input & 0xffffff80) != 0L) {
                bytes += 1;
                input >>>= 7;
            }
            result += bytes;
        }
        return result;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteUtilsBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(opt).run();
    }



}
