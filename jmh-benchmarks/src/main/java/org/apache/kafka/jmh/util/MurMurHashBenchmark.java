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

import org.apache.kafka.common.utils.Utils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.SplittableRandom;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@Fork(2)
@BenchmarkMode({Throughput})
@OutputTimeUnit(MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class MurMurHashBenchmark {

    @Param({"42"})
    private int seed;

    @Param({"128", "256"})
    private int bytes;

    @Param({"false", "true"})
    private boolean direct;

    @Param({"false", "true"})
    private boolean littleEndian;

    private ByteBuffer byteBuffer;

    private byte[] bytesArray;

    @Setup
    public void setup() {
        final SplittableRandom random = new SplittableRandom(seed);
        bytesArray = new byte[bytes];
        byteBuffer = direct ? ByteBuffer.allocateDirect(bytes) : ByteBuffer.allocate(bytes);
        for (int i = 0; i < bytes; i++) {
            final byte b = (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1);
            byteBuffer.put(b);
            bytesArray[i] = b;
        }

        if (littleEndian) {
            byteBuffer.order(LITTLE_ENDIAN);
        }
        byteBuffer.flip();

        if (byteArrayMurmur2() != byteBufferMurmur2() || byteBufferMurmur2() != byteBufferMurmur2()) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    public int byteBufferMurmur2() {
        return Utils.murmur2(byteBuffer);
    }

    @Benchmark
    public int byteArrayMurmur2() {
        return Utils.murmur2(bytesArray);
    }
}
