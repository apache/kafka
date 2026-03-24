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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.Utils;

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
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

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
                buf.putLong((long) i + 1); // plain value
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
            bh.consume(Utils.rawAggregation(randomValue));
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
            bh.consume(Utils.rawAggregation(randomValue));
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
            bh.consume(Utils.rawPlainValue(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testRawTimestampedValueWithoutHeaders(IterationStateForEmptyHeadersTimestamp state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(rawTimestampedValuePre20249(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testRawTimestampedValueWithoutHeadersOpt(IterationStateForEmptyHeadersTimestamp state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(Utils.rawTimestampedValue(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testHeadersWithoutHeaders(IterationStateForEmptyHeaders state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(headersPre20249(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testHeadersWithoutHeadersOpt(IterationStateForEmptyHeaders state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(Utils.headers(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testTimestampWithoutHeaders(IterationStateForEmptyHeadersTimestamp state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(timestampPre20249(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testTimestampWithoutHeadersOpt(IterationStateForEmptyHeadersTimestamp state, Blackhole bh) {
        for (byte[] randomValue : state.getRandomValues()) {
            bh.consume(Utils.timestamp(randomValue));
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

    /**
     * Prior to KAFKA-20249: AggregationWithHeadersDeserializer - Extract the raw aggregation bytes from 
     * serialized AggregationWithHeaders, stripping the headers prefix. 
     */
    private static byte[] rawAggregationPre20249(final byte[] aggregationWithHeaders) {
        if (aggregationWithHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(aggregationWithHeaders);
        Utils.readHeaders(buffer); 
        return Utils.readBytes(buffer, buffer.remaining());
    }

    /**
     * Prior to KAFKA-20249: ValueAndTimestampDeserializer - Extract raw value from serialized 
     * ValueTimestampHeaders.
     */
    private static byte[] rawValuePre20249(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize + Long.BYTES);
        return Utils.readBytes(buffer, buffer.remaining());
    }

    /**
     * Prior to KAFKA-20249: Extract raw timestamped value (timestamp + value) from serialized ValueTimestampHeaders.
     * This strips the headers portion but keeps timestamp and value intact.
     *
     * Format conversion:
     * Input:  [headersSize(varint)][headers][timestamp(8)][value]
     * Output: [timestamp(8)][value]
     */
    private static byte[] rawTimestampedValuePre20249(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        // Skip headers, keep timestamp + value
        if (headersSize < 0 || headersSize > buffer.remaining() || buffer.remaining() - headersSize < StateSerdes.TIMESTAMP_SIZE) {
            throw new SerializationException(
                "Invalid format: headers size " + headersSize + 
                ", timestamp expected size " + StateSerdes.TIMESTAMP_SIZE + 
                ", but buffer size " + buffer.remaining()
            );
        }
        buffer.position(buffer.position() + headersSize);

        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /**
     * Prior to KAFKA-20249 - AggregationWithHeadersDeserializer - Extract headers from serialized AggregationWithHeaders
     */
    private static Headers headersPre20249(final byte[] rawAggregationWithHeaders) {
        if (rawAggregationWithHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawAggregationWithHeaders);
        return Utils.readHeaders(buffer);
    }

    /**
     * Prior to KAFKA-20249 - ValueTimestampHeadersDeserializer - Extract timestamp from serialized ValueTimestampHeaders.
     */
    private static long timestampPre20249(final byte[] rawValueTimestampHeaders) {
        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize);

        final byte[] rawTimestamp = Utils.readBytes(buffer, Long.BYTES);
        return LONG_DESERIALIZER.deserialize("", rawTimestamp);
    }
}
