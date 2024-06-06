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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BasicBenchmark {

    private static final Random RANDOM = new Random();
    private static final float[] FLOATS = getInOrderArray();
    private int index = 0;

    @Setup(Level.Iteration)
    public void init() {
        index = 0;
    }

    @Benchmark
    public double testMathSqrt() {
        return Math.sqrt(index++ % 5000);
    }

    @Benchmark
    public long testSystemMillis() {
        return System.currentTimeMillis();
    }

    @Benchmark
    public float testRandom() {
        return RANDOM.nextInt();
    }

    @Benchmark
    public float testBinarySearch() {
        return Arrays.binarySearch(FLOATS, FLOATS[index++ % FLOATS.length]);
    }

    private static float[] getInOrderArray() {
        float[] floats = new float[1024];
        for (int i = 0; i < floats.length; i++)
            floats[i] = RANDOM.nextFloat();
        Arrays.sort(floats);
        return floats;
    }
}
