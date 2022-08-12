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

import org.apache.kafka.common.utils.Crc32C;

import java.nio.ByteBuffer;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class Crc32CBenchmark {

    @Param({"false", "true"})
    private boolean direct;


    @Param({"false", "true"})
    private boolean readonly;

    @Param({"42"})
    private int seed;

    @Param({"128", "1024", "4096"})
    private int bytes;

    private ByteBuffer input;

    @Setup
    public void setup() {
        SplittableRandom random = new SplittableRandom(seed);
        input = direct ? ByteBuffer.allocateDirect(bytes) : ByteBuffer.allocate(bytes);
        for (int o = 0; o < bytes; o++) {
            input.put(o, (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1));
        }
        if (readonly) {
            input = input.asReadOnlyBuffer();
        }
    }

    @Benchmark
    public long checksum() {
        return Crc32C.compute(input, 0, bytes);
    }

}
