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

import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.AggregationWithHeadersDeserializer;
import org.apache.kafka.streams.state.internals.ValueTimestampHeadersDeserializer;

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

import static org.apache.kafka.streams.state.internals.AggregationWithHeadersDeserializer.readBytes;
import static org.apache.kafka.streams.state.internals.AggregationWithHeadersDeserializer.readHeaders;

@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class RawBytesExtraction {
    private static final int DATA_SET_SAMPLE_SIZE = 16384;

    @State(Scope.Benchmark)
    public static class IterationStateForValues {
        protected byte[][] values;

        byte[][] getRandomValues() {
            return values;
        }
    }

    @State(Scope.Benchmark)
    public static class IterationStateForEmptyHeaders extends IterationStateForValues {
        @Setup(Level.Iteration)
        public void setup() {
            this.values = new byte[DATA_SET_SAMPLE_SIZE][];
            for (int i = 0; i < DATA_SET_SAMPLE_SIZE; i++) {
                values[i] = new byte[1 + 8];
                final ByteBuffer buf = ByteBuffer.wrap(values[i]);
                buf.put((byte) 0x00); // header size
                buf.putLong((long) i); // non-header payload
            }
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

    @State(Scope.Benchmark)
    public static class IterationStateForHeaders extends IterationStateForValues {
        @Setup(Level.Iteration)
        public void setup() {
            this.values = new byte[DATA_SET_SAMPLE_SIZE][];
            for (int i = 0; i < DATA_SET_SAMPLE_SIZE; i++) {
                values[i] = new byte[1 + 1 + (1 + 4) + (1 + 4) + 8];
                final ByteBuffer buf = ByteBuffer.wrap(values[i]);
                ByteUtils.writeVarint(11, buf);  // 1-byte header size of 11
                ByteUtils.writeVarint(1, buf);  // 1-byte header count of 1
                ByteUtils.writeVarint(4, buf);  // 1-byte header key size
                buf.putInt(i + 1); // 4-byte header key
                ByteUtils.writeVarint(4, buf);  // 1-byte header value size
                buf.putInt(i + 1); // 4-byte header value
                buf.putLong((long) i + 1); // non-header payload
            }
        }
    }

    
    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testRawAggregationWithoutHeaders(IterationStateForEmptyHeaders state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(rawAggregationPre20249(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testRawAggregationWithoutHeadersOpt(IterationStateForEmptyHeaders state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(AggregationWithHeadersDeserializer.rawAggregation(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testRawAggregationWithHeaders(IterationStateForHeaders state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(rawAggregationPre20249(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testRawAggregationWithHeadersOpt(IterationStateForHeaders state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(AggregationWithHeadersDeserializer.rawAggregation(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testRawValueWithoutHeaders(IterationStateForEmptyHeadersTimestamp state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(rawValuePre20249(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testRawValueWithoutHeadersOpt(IterationStateForEmptyHeadersTimestamp state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(ValueTimestampHeadersDeserializer.rawValue(randomValue));
        }
    }

    /**
     * Prior to KAFKA-20249: AggregationWithHeadersDeserializer - Extract the raw aggregation bytes from 
     * serialized AggregationWithHeaders, stripping the headers prefix. 
     */
    public static byte[] rawAggregationPre20249(final byte[] aggregationWithHeaders) {
        if (aggregationWithHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(aggregationWithHeaders);
        readHeaders(buffer); 
        return readBytes(buffer, buffer.remaining());
    }

    /**
     * Prior to KAFKA-20249: ValueAndTimestampDeserializer - Extract raw value from serialized 
     * ValueTimestampHeaders.
     */
    public static byte[] rawValuePre20249(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize + Long.BYTES);
        return readBytes(buffer, buffer.remaining());
    }
}
