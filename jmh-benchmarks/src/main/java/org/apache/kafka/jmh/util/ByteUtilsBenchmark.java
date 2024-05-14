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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.ByteUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
/**
 * This benchmark calculates the empirical evidence of different implementation for encoding/decoding a protobuf
 * <a href="https://protobuf.dev/programming-guides/encoding/#varints">VarInt</a> and VarLong.
 *
 * The benchmark uses JMH and calculates results for different sizes of variable length integer. We expect most of the
 * usage in Kafka code base to be 1 or 2 byte integers.
 */
public class ByteUtilsBenchmark {
    private static final int DATA_SET_SAMPLE_SIZE = 16384;

    @State(Scope.Benchmark)
    public static class BaseBenchmarkState {
        private ByteBuffer testBuffer;
        private SecureRandom random;
        @Setup(Level.Trial)
        public void setUpBenchmarkLevel() {
            // Initialize the random number generator with a seed so that for each benchmark it produces the same sequence
            // of random numbers. Note that it is important to initialize it again with the seed before every benchmark.
            random = new SecureRandom();
            random.setSeed(133713371337L);
        }

        @Setup(Level.Invocation)
        public void setUpInvocationBuffer() {
            testBuffer = ByteBuffer.allocate(10);
        }

        /**
         * Generates a random int64 number which occupies exactly bytesSet in the variable length encoding for int64
         *
         * Upper bound is set by finding the largest number that can be represented by N bits. This number is found
         * by bit shifting by N and subtracting decimal 1. For example, for 2 bytes = 16 bits, we calculate the
         * upper bound by:
         * 1. 0001 0000 0000 0000 0000 // bit shift by 16 = 65536 decimal
         * 2. Subtract 1 from 65536 = 65535
         * 3. 65535 is the upper bound which is represented in binary as 1111 1111 1111 1111 i.e. largest number
         *    that could be represented by 2 bytes.
         *
         * Hence, range of random number of different byte length is:
         * 1 byte - [1, 255)
         * 2 byte - [256, 65535)
         * ... and so on.
         */
        long generateRandomLongWithExactBytesSet(int bytesSet) {
            long lowerBound = 1L << ((bytesSet - 1) * 8);
            long upperBound = (1L << (bytesSet * 8)) - 1;
            if (lowerBound >= upperBound) {
                throw new IllegalArgumentException();
            }
            return lowerBound +
                random.longs(lowerBound, upperBound).findFirst()
                    .orElseThrow(() -> new IllegalStateException("Unable to create a random long in the range=[" + lowerBound + ", " + upperBound + "]"));
        }

        /**
         * Generates a random int32 number which occupies exactly bytesSet in the variable length encoding for int32
         *
         * @see {@link #generateRandomLongWithExactBytesSet(int)} for implementation details.
         *
         */
        int generateRandomIntWithExactBytesSet(int bytesSet) {
            int lowerBound = 1 << ((bytesSet - 1) * 8);
            int upperBound = (1 << (bytesSet * 8)) - 1;
            if (lowerBound >= upperBound) {
                throw new IllegalArgumentException();
            }
            return lowerBound + random.nextInt(upperBound - lowerBound);
        }

        public ByteBuffer getTestBuffer() {
            return testBuffer;
        }
    }

    @State(Scope.Benchmark)
    public static class IterationStateForLong extends BaseBenchmarkState {
        @Param({"1", "3", "5", "7"})
        int numNonZeroBytes;

        private long[] randomLongs;

        @Setup(Level.Iteration)
        public void setup() {
            randomLongs = new long[DATA_SET_SAMPLE_SIZE];
            for (int i = 0; i < DATA_SET_SAMPLE_SIZE; i++) {
                this.randomLongs[i] = generateRandomLongWithExactBytesSet(numNonZeroBytes);
            }
        }

        public long[] getRandomValues() {
            return randomLongs;
        }
    }

    @State(Scope.Benchmark)
    public static class IterationStateForInt extends BaseBenchmarkState {
        @Param({"1", "2", "3"})
        int numNonZeroBytes;
        private int[] randomInts;

        @Setup(Level.Iteration)
        public void setup() {
            randomInts = new int[DATA_SET_SAMPLE_SIZE];
            for (int i = 0; i < DATA_SET_SAMPLE_SIZE; i++) {
                this.randomInts[i] = generateRandomIntWithExactBytesSet(numNonZeroBytes);
            }
        }

        public int[] getRandomValues() {
            return randomInts;
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarintNetty(IterationStateForInt state, Blackhole bk) {
        for (int randomValue : state.getRandomValues()) {
            ByteUtils.writeUnsignedVarint(randomValue, state.getTestBuffer());
            // prepare for reading
            state.getTestBuffer().flip();
            bk.consume(ByteUtilsBenchmark.readUnsignedVarintNetty(state.getTestBuffer()));
            state.getTestBuffer().clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarintLegacy(IterationStateForInt state, Blackhole bk) {
        for (int randomValue : state.getRandomValues()) {
            ByteUtils.writeUnsignedVarint(randomValue, state.getTestBuffer());
            // prepare for reading
            state.getTestBuffer().flip();
            bk.consume(ByteUtilsBenchmark.readUnsignedVarintLegacy(state.getTestBuffer()));
            state.getTestBuffer().clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarintProtobuf(IterationStateForInt state, Blackhole bk) {
        for (int randomValue : state.getRandomValues()) {
            ByteUtils.writeUnsignedVarint(randomValue, state.getTestBuffer());
            // prepare for reading
            state.getTestBuffer().flip();
            bk.consume(ByteUtilsBenchmark.readUnsignedVarintProtoBuf(state.getTestBuffer()));
            state.getTestBuffer().clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarlongUnrolled(IterationStateForLong state, Blackhole bk) throws IOException {
        for (long randomValue : state.getRandomValues()) {
            ByteUtils.writeUnsignedVarlong(randomValue, state.getTestBuffer());
            // prepare for reading
            state.getTestBuffer().flip();
            bk.consume(ByteUtilsBenchmark.readUnsignedVarlongNetty(state.getTestBuffer()));
            state.getTestBuffer().clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarlongLegacy(IterationStateForLong state, Blackhole bk) {
        for (long randomValue : state.getRandomValues()) {
            ByteUtils.writeUnsignedVarlong(randomValue, state.getTestBuffer());
            // prepare for reading
            state.getTestBuffer().flip();
            bk.consume(ByteUtilsBenchmark.readUnsignedVarlongLegacy(state.getTestBuffer()));
            state.getTestBuffer().clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarintUnrolled(IterationStateForInt state) {
        for (int randomValue : state.getRandomValues()) {
            ByteUtilsBenchmark.writeUnsignedVarintUnrolled(randomValue, state.getTestBuffer());
            state.getTestBuffer().clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarintLegacy(IterationStateForInt state) {
        for (int randomValue : state.getRandomValues()) {
            ByteUtilsBenchmark.writeUnsignedVarintLegacy(randomValue, state.getTestBuffer());
            state.getTestBuffer().clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarlongUnrolled(IterationStateForLong state) {
        for (long randomValue : state.getRandomValues()) {
            ByteUtilsBenchmark.writeUnsignedVarlongUnrolled(randomValue, state.getTestBuffer());
            state.getTestBuffer().clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarlongLegacy(IterationStateForLong state) {
        for (long randomValue : state.getRandomValues()) {
            ByteUtilsBenchmark.writeUnsignedVarlongLegacy(randomValue, state.getTestBuffer());
            state.getTestBuffer().clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testSizeOfUnsignedVarint(IterationStateForInt state, Blackhole bk) {
        for (int randomValue : state.getRandomValues()) {
            bk.consume(ByteUtils.sizeOfUnsignedVarint(randomValue));
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testSizeOfUnsignedVarintSimple(IterationStateForInt state, Blackhole bk) {
        for (int randomValue : state.getRandomValues()) {
            int value = randomValue;
            int bytes = 1;
            while ((value & 0xffffff80) != 0L) {
                bytes += 1;
                value >>>= 7;
            }
            bk.consume(bytes);
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testSizeOfUnsignedVarlong(IterationStateForLong state, Blackhole bk) {
        for (long randomValue : state.getRandomValues()) {
            bk.consume(ByteUtils.sizeOfUnsignedVarlong(randomValue));
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteUtilsBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(opt).run();
    }


    /* Implementations */

    /*
     * Implementation in Trunk as of Apr 2023 / v3.4
     */
    private static int readUnsignedVarintLegacy(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw new IllegalArgumentException();
        }
        value |= b << i;
        return value;
    }

    /*
     * Implementation in Trunk as of Apr 2023 / v3.4
     */
    private static long readUnsignedVarlongLegacy(ByteBuffer buffer)  {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw new IllegalArgumentException();
        }
        value |= b << i;
        return value;
    }

    /**
     * Implementation copied from Protobuf's implementation.
     * see: https://github.com/protocolbuffers/protobuf/blob/f1c7820c9bd0e31f8b7d091092851441ad2716b6/java/core/src/main/java/com/google/protobuf/CodedInputStream.java#L1048
     */
    private static int readUnsignedVarintProtoBuf(ByteBuffer buf) {
        fastpath:
        {
            int tempPos = buf.position();
            int limit = buf.limit();

            if (limit == tempPos) {
                break fastpath;
            }

            final byte[] buffer = buf.array();
            int x;
            if ((x = buffer[tempPos++]) >= 0) {
                buf.position(tempPos);
                return x;
            } else if (limit - tempPos < 9) {
                break fastpath;
            } else if ((x ^= buffer[tempPos++] << 7) < 0) {
                x ^= ~0 << 7;
            } else if ((x ^= buffer[tempPos++] << 14) >= 0) {
                x ^= (~0 << 7) ^ (~0 << 14);
            } else if ((x ^= buffer[tempPos++] << 21) < 0) {
                x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21);
            } else {
                int y = buffer[tempPos++];
                x ^= y << 28;
                x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21) ^ (~0 << 28);
                if (y < 0
                    && buffer[tempPos++] < 0
                    && buffer[tempPos++] < 0
                    && buffer[tempPos++] < 0
                    && buffer[tempPos++] < 0
                    && buffer[tempPos++] < 0) {
                    break fastpath; // Will throw malformedVarint()
                }
            }
            buf.position(tempPos);
            return x;
        }
        return readUnsignedVarintLegacy(buf);
    }

    /**
     * Implementation copied from Netty
     * see: https://github.com/netty/netty/blob/59aa6e635b9996cf21cd946e64353270679adc73/codec/src/main/java/io/netty/handler/codec/protobuf/ProtobufVarint32FrameDecoder.java#L73
     */
    private static int readUnsignedVarintNetty(ByteBuffer buffer) {
        byte tmp = buffer.get();
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = buffer.get()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = buffer.get()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = buffer.get()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = buffer.get()) << 28;
                        if (tmp < 0) {
                            throw new IllegalArgumentException();
                        }
                    }
                }
            }
            return result;
        }
    }

    /**
     * Implementation extended from Int implementation from Netty
     * see: https://github.com/netty/netty/blob/59aa6e635b9996cf21cd946e64353270679adc73/codec/src/main/java/io/netty/handler/codec/protobuf/ProtobufVarint32FrameDecoder.java#L73
     */
    private static long readUnsignedVarlongNetty(ByteBuffer buffer) {
        byte tmp = buffer.get();
        if (tmp >= 0) {
            return tmp;
        } else {
            long result = tmp & 0x7f;
            if ((tmp = buffer.get()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 0x7f) << 7;
                if ((tmp = buffer.get()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 0x7f) << 14;
                    if ((tmp = buffer.get()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 0x7f) << 21;
                        if ((tmp = buffer.get()) >= 0) {
                            result |= (long) tmp << 28;
                        } else {
                            result |= (long) (tmp & 0x7f) << 28;
                            result = innerUnsignedReadVarLong(buffer, result);
                        }
                    }
                }
            }
            return result;
        }
    }

    private static long innerUnsignedReadVarLong(ByteBuffer buffer, long result) {
        byte tmp;
        if ((tmp = buffer.get()) >= 0) {
            result |= (long) tmp << 35;
        } else {
            result |= (long) (tmp & 0x7f) << 35;
            if ((tmp = buffer.get()) >= 0) {
                result |= (long) tmp << 42;
            } else {
                result |= (long) (tmp & 0x7f) << 42;
                if ((tmp = buffer.get()) >= 0) {
                    result |= (long) tmp << 49;
                } else {
                    result |= (long) (tmp & 0x7f) << 49;
                    if ((tmp = buffer.get()) >= 0) {
                        result |= (long) tmp << 56;
                    } else {
                        result |= (long) (tmp & 0x7f) << 56;
                        result |= (long) (tmp = buffer.get()) << 63;
                        if (tmp < 0) {
                            throw new IllegalArgumentException();
                        }
                    }
                }
            }
        }
        return result;
    }

    /*
     * Implementation in Trunk as of Apr 2023 / v3.4
     */
    private static void writeUnsignedVarintLegacy(int value, ByteBuffer buffer) {
        while ((value & 0xffffff80) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80);
            buffer.put(b);
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    /*
     * Implementation in Trunk as of Apr 2023 / v3.4
     */
    private static void writeUnsignedVarlongLegacy(long v, ByteBuffer buffer) {
        while ((v & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.put(b);
            v >>>= 7;
        }
        buffer.put((byte) v);
    }

    /*
     * Implementation extended for Long from the Int implementation at https://github.com/astei/varint-writing-showdown/tree/dev (MIT License)
     * see: https://github.com/astei/varint-writing-showdown/blob/6b1a4baec4b1f0ce65fa40cf0b282ec775fdf43e/src/jmh/java/me/steinborn/varintshowdown/res/SmartNoDataDependencyUnrolledVarIntWriter.java#L8
     */
    private static void writeUnsignedVarlongUnrolled(long value, ByteBuffer buffer) {
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            buffer.put((byte) value);
        } else {
            buffer.put((byte) (value & 0x7F | 0x80));
            if ((value & (0xFFFFFFFF << 14)) == 0) {
                buffer.put((byte) (value >>> 7));
            } else {
                buffer.put((byte) ((value >>> 7) & 0x7F | 0x80));
                if ((value & (0xFFFFFFFF << 21)) == 0) {
                    buffer.put((byte) (value >>> 14));
                } else {
                    buffer.put((byte) ((value >>> 14) & 0x7F | 0x80));
                    if ((value & (0xFFFFFFFF << 28)) == 0) {
                        buffer.put((byte) (value >>> 21));
                    } else {
                        buffer.put((byte) ((value >>> 21) & 0x7F | 0x80));
                        innerWriteUnsignedVarlongUnrolled(value, buffer);
                    }
                }
            }
        }
    }

    private static void innerWriteUnsignedVarlongUnrolled(long value, ByteBuffer buffer) {
        if ((value & (0xFFFFFFFFFFFFFFFFL << 35)) == 0) {
            buffer.put((byte) (value >>> 28));
        } else {
            buffer.put((byte) ((value >>> 28) & 0x7FL | 0x80));
            if ((value & (0xFFFFFFFFFFFFFFFFL << 42)) == 0) {
                buffer.put((byte) (value >>> 35));
            } else {
                buffer.put((byte) ((value >>> 35) & 0x7FL | 0x80));
                if ((value & (0xFFFFFFFFFFFFFFFFL << 49)) == 0) {
                    buffer.put((byte) (value >>> 42));
                } else {
                    buffer.put((byte) ((value >>> 42) & 0x7FL | 0x80));
                    if ((value & (0xFFFFFFFFFFFFFFFFL << 56)) == 0) {
                        buffer.put((byte) (value >>> 49));
                    } else {
                        buffer.put((byte) ((value >>> 49) & 0x7FL | 0x80));
                        if ((value & (0xFFFFFFFFFFFFFFFFL << 63)) == 0) {
                            buffer.put((byte) (value >>> 56));
                        } else {
                            buffer.put((byte) ((value >>> 56) & 0x7FL | 0x80));
                            buffer.put((byte) (value >>> 63));
                        }
                    }
                }
            }
        }
    }

    /*
     * Implementation copied from https://github.com/astei/varint-writing-showdown/tree/dev (MIT License)
     * see: https://github.com/astei/varint-writing-showdown/blob/6b1a4baec4b1f0ce65fa40cf0b282ec775fdf43e/src/jmh/java/me/steinborn/varintshowdown/res/SmartNoDataDependencyUnrolledVarIntWriter.java#L8
     */
    private static void writeUnsignedVarintUnrolled(int value, ByteBuffer buffer) {
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            buffer.put((byte) value);
        } else {
            buffer.put((byte) (value & 0x7F | 0x80));
            if ((value & (0xFFFFFFFF << 14)) == 0) {
                buffer.put((byte) ((value >>> 7) & 0xFF));
            } else {
                buffer.put((byte) ((value >>> 7) & 0x7F | 0x80));
                if ((value & (0xFFFFFFFF << 21)) == 0) {
                    buffer.put((byte) ((value >>> 14) & 0xFF));
                } else {
                    buffer.put((byte) ((value >>> 14) & 0x7F | 0x80));
                    if ((value & (0xFFFFFFFF << 28)) == 0) {
                        buffer.put((byte) ((value >>> 21) & 0xFF));
                    } else {
                        buffer.put((byte) ((value >>> 21) & 0x7F | 0x80));
                        buffer.put((byte) ((value >>> 28) & 0xFF));
                    }
                }
            }
        }
    }
}
