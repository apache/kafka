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

package org.apache.kafka.jmh.streams;

import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.StateSerdes;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class RawBytesExtractionBenchmark {
    private static final int DATA_SET_SAMPLE_SIZE = 16384;

    @State(Scope.Benchmark)
    public static class IterationStateForValues {
        protected byte[][] values;

        byte[][] getRandomValues() {
            return values;
        }
    }

    @State(Scope.Benchmark)
    public static class IterationStateForEmptyHeadersTimestamp extends IterationStateForValues {
        @Setup(Level.Iteration)
        public void setup() {
            this.values = new byte[DATA_SET_SAMPLE_SIZE][];
            for (int i = 0; i < DATA_SET_SAMPLE_SIZE; i++) {
                values[i] = new byte[1 + StateSerdes.TIMESTAMP_SIZE + 8];
                final ByteBuffer buf = ByteBuffer.wrap(values[i]);
                buf.put((byte) 0x00); // header size
                buf.putLong(123456789L); // timestamp
                buf.putLong((long) i); // non-header payload
            }
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testConvertToHeaderFormat(IterationStateForEmptyHeadersTimestamp state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(convertToHeaderFormatPre20303(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testConvertToHeaderFormatOpt(IterationStateForEmptyHeadersTimestamp state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(HeadersBytesStore.convertToHeaderFormat(randomValue));
        }
    }

    /**
     * Prior to KAFKA-20303 - HeadersBytesStore.convertToHeaderFormat
     */
    private static byte[] convertToHeaderFormatPre20303(final byte[] valueAndTimestamp) {
        if (valueAndTimestamp == null) {
            return null;
        }

        // Format: [headersSize(varint)][headersBytes][payload]
        // For empty headers:
        //   headersSize = varint(0) = [0x00]
        //   headersBytes = [] (empty, 0 bytes)
        // Result: [0x00][payload]
        return ByteBuffer
            .allocate(1 + valueAndTimestamp.length)
            .put((byte) 0x00)
            .put(valueAndTimestamp)
            .array();
    }
}
