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
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class ChunkedBytesStreamTest {
    private static final Random RANDOM = new Random(1337);
    private final BufferSupplier supplier = BufferSupplier.NO_CACHING;

    @Test
    public void testEofErrorForMethodReadFully() throws IOException {
        ByteBuffer input = ByteBuffer.allocate(8);
        int lengthGreaterThanInput = input.capacity() + 1;
        byte[] got = new byte[lengthGreaterThanInput];
        try (InputStream is = new ChunkedBytesStream(new ByteBufferInputStream(input), supplier, 10, false)) {
            assertEquals(8, is.read(got, 0, got.length), "Should return 8 signifying end of input");
        }
    }

    @ParameterizedTest
    @MethodSource("provideSourceBytebuffersForTest")
    public void testCorrectnessForMethodReadFully(ByteBuffer input) throws IOException {
        byte[] got = new byte[input.array().length];
        try (InputStream is = new ChunkedBytesStream(new ByteBufferInputStream(input), supplier, 10, false)) {
            // perform a 2 pass read. this tests the scenarios where one pass may lead to partially consumed
            // intermediate buffer
            int toRead = RANDOM.nextInt(got.length);
            is.read(got, 0, toRead);
            is.read(got, toRead, got.length - toRead);
        }
        assertArrayEquals(input.array(), got);
    }

    @ParameterizedTest
    @MethodSource("provideCasesWithInvalidInputsForMethodRead")
    public void testInvalidInputsForMethodRead(byte[] b, int off, int len) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(16);

        try (InputStream is = new ChunkedBytesStream(new ByteBufferInputStream(buffer.duplicate()), supplier, 10, false)) {
            assertThrows(IndexOutOfBoundsException.class, () -> is.read(b, off, len));
        }
    }

    @ParameterizedTest
    @MethodSource("provideSourceBytebuffersForTest")
    public void testCorrectnessForMethodReadByte(ByteBuffer input) throws IOException {
        byte[] got = new byte[input.array().length];
        try (InputStream is = new ChunkedBytesStream(new ByteBufferInputStream(input), supplier, 10, false)) {
            int i = 0;
            while (i < got.length) {
                got[i++] = (byte) is.read();
            }
        }
        assertArrayEquals(input.array(), got);
    }

    @ParameterizedTest
    @MethodSource("provideSourceBytebuffersForTest")
    public void testCorrectnessForMethodRead(ByteBuffer inputBuf) throws IOException {
        int[] inputArr = new int[inputBuf.capacity()];
        for (int i = 0; i < inputArr.length; i++) {
            inputArr[i] = Byte.toUnsignedInt(inputBuf.get());
        }
        int[] got = new int[inputArr.length];
        inputBuf.rewind();
        try (InputStream is = new ChunkedBytesStream(new ByteBufferInputStream(inputBuf), supplier, 10, false)) {
            int i = 0;
            while (i < got.length) {
                got[i++] = is.read();
            }
        }
        assertArrayEquals(inputArr, got);
    }

    @Test
    public void testEndOfFileForMethodRead() throws IOException {
        ByteBuffer inputBuf = ByteBuffer.allocate(2);
        int lengthGreaterThanInput = inputBuf.capacity() + 1;

        try (InputStream is = new ChunkedBytesStream(new ByteBufferInputStream(inputBuf), supplier, 10, false)) {
            int cnt = 0;
            while (cnt++ < lengthGreaterThanInput) {
                int res = is.read();
                if (cnt > inputBuf.capacity())
                    assertEquals(-1, res, "end of file for read should be -1");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEndOfSourceForMethodSkip(boolean pushSkipToSourceStream) throws IOException {
        ByteBuffer inputBuf = ByteBuffer.allocate(16);
        RANDOM.nextBytes(inputBuf.array());
        inputBuf.rewind();

        final InputStream sourcestream = spy(new ByteBufferInputStream(inputBuf));
        try (InputStream is = new ChunkedBytesStream(sourcestream, supplier, 10, pushSkipToSourceStream)) {
            long res = is.skip(inputBuf.capacity() + 1);
            assertEquals(inputBuf.capacity(), res);
        }
    }

    @ParameterizedTest
    @MethodSource({"provideSourceSkipValuesForTest"})
    public void testCorrectnessForMethodSkip(int bytesToPreRead, ByteBuffer inputBuf, int numBytesToSkip, boolean pushSkipToSourceStream) throws IOException {
        int expectedInpLeftAfterSkip = inputBuf.remaining() - bytesToPreRead - numBytesToSkip;
        int expectedSkippedBytes = Math.min(inputBuf.remaining() - bytesToPreRead, numBytesToSkip);

        try (InputStream is = new ChunkedBytesStream(new ByteBufferInputStream(inputBuf.duplicate()), supplier, 10, pushSkipToSourceStream)) {
            int cnt = 0;
            while (cnt++ < bytesToPreRead) {
                int r = is.read();
                assertNotEquals(-1, r, "Unexpected end of data.");
            }

            long res = is.skip(numBytesToSkip);
            assertEquals(expectedSkippedBytes, res);

            // verify that we are able to read rest of the input
            cnt = 0;
            while (cnt++ < expectedInpLeftAfterSkip) {
                int readRes = is.read();
                assertNotEquals(-1, readRes, "Unexpected end of data.");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideEdgeCaseInputForMethodSkip")
    public void testEdgeCaseInputForMethodSkip(int bufferLength, long toSkip, boolean delegateSkipToSourceStream, long expected) throws IOException {
        ByteBuffer inputBuf = ByteBuffer.allocate(bufferLength);
        RANDOM.nextBytes(inputBuf.array());
        inputBuf.rewind();

        try (InputStream is = new ChunkedBytesStream(new ByteBufferInputStream(inputBuf.duplicate()), supplier, 10, delegateSkipToSourceStream)) {
            assertEquals(expected, is.skip(toSkip));
        }
    }

    private static Stream<Arguments> provideEdgeCaseInputForMethodSkip() {
        int bufferLength = 16;
        // Test toSkip larger than int and negative for both delegateToSourceStream true and false
        return Stream.of(
            Arguments.of(bufferLength, Integer.MAX_VALUE + 1L, true, bufferLength),
            Arguments.of(bufferLength, -1, true, 0),
            Arguments.of(bufferLength, Integer.MAX_VALUE + 1L, false, bufferLength),
            Arguments.of(bufferLength, -1, false, 0)
        );
    }

    private static Stream<Arguments> provideCasesWithInvalidInputsForMethodRead() {
        byte[] b = new byte[16];
        return Stream.of(
            // negative off
            Arguments.of(b, -1, b.length),
            // negative len
            Arguments.of(b, 0, -1),
            // overflow off + len
            Arguments.of(b, Integer.MAX_VALUE, 10),
            // len greater than size of target array
            Arguments.of(b, 0, b.length + 1),
            // off + len greater than size of target array
            Arguments.of(b, b.length - 1, 2)
        );
    }

    private static List<Arguments> provideSourceSkipValuesForTest() {
        ByteBuffer bufGreaterThanIntermediateBuf = ByteBuffer.allocate(16);
        RANDOM.nextBytes(bufGreaterThanIntermediateBuf.array());
        bufGreaterThanIntermediateBuf.position(bufGreaterThanIntermediateBuf.capacity());
        bufGreaterThanIntermediateBuf.flip();

        ByteBuffer bufMuchGreaterThanIntermediateBuf = ByteBuffer.allocate(100);
        RANDOM.nextBytes(bufMuchGreaterThanIntermediateBuf.array());
        bufMuchGreaterThanIntermediateBuf.position(bufMuchGreaterThanIntermediateBuf.capacity());
        bufMuchGreaterThanIntermediateBuf.flip();

        ByteBuffer emptyBuffer = ByteBuffer.allocate(2);

        ByteBuffer oneByteBuf = ByteBuffer.allocate(1).put((byte) 1);
        oneByteBuf.flip();

        List<List<Object>> testInputs = Arrays.asList(
            // empty source byte array
            Arrays.asList(0, emptyBuffer, 0),
            Arrays.asList(0, emptyBuffer, 1),
            Arrays.asList(1, emptyBuffer, 1),
            Arrays.asList(1, emptyBuffer, 0),
            // byte source array with 1 byte
            Arrays.asList(0, oneByteBuf, 0),
            Arrays.asList(0, oneByteBuf, 1),
            Arrays.asList(1, oneByteBuf, 0),
            Arrays.asList(1, oneByteBuf, 1),
            // byte source array with full read from intermediate buf
            Arrays.asList(0, bufGreaterThanIntermediateBuf.duplicate(), bufGreaterThanIntermediateBuf.capacity()),
            Arrays.asList(bufGreaterThanIntermediateBuf.capacity(), bufGreaterThanIntermediateBuf.duplicate(), 0),
            Arrays.asList(2, bufGreaterThanIntermediateBuf.duplicate(), 10),
            Arrays.asList(2, bufGreaterThanIntermediateBuf.duplicate(), 8)
        );

        Boolean[] tailArgs = new Boolean[]{true, false};
        List<Arguments> finalArguments = new ArrayList<>(2 * testInputs.size());
        for (List<Object> args : testInputs) {
            for (Boolean aBoolean : tailArgs) {
                List<Object> expandedArgs = new ArrayList<>(args);
                expandedArgs.add(aBoolean);
                finalArguments.add(Arguments.of(expandedArgs.toArray()));
            }
        }
        return finalArguments;
    }

    private static Stream<Arguments> provideSourceBytebuffersForTest() {
        ByteBuffer bufGreaterThanIntermediateBuf = ByteBuffer.allocate(16);
        RANDOM.nextBytes(bufGreaterThanIntermediateBuf.array());
        bufGreaterThanIntermediateBuf.position(bufGreaterThanIntermediateBuf.capacity());

        ByteBuffer bufMuchGreaterThanIntermediateBuf = ByteBuffer.allocate(100);
        RANDOM.nextBytes(bufMuchGreaterThanIntermediateBuf.array());
        bufMuchGreaterThanIntermediateBuf.position(bufMuchGreaterThanIntermediateBuf.capacity());

        return Stream.of(
            // empty byte array
            Arguments.of(ByteBuffer.allocate(2)),
            // byte array with 1 byte
            Arguments.of(ByteBuffer.allocate(1).put((byte) 1).flip()),
            // byte array with size < intermediate buffer
            Arguments.of(ByteBuffer.allocate(8).put("12345678".getBytes()).flip()),
            // byte array with size > intermediate buffer
            Arguments.of(bufGreaterThanIntermediateBuf.flip()),
            // byte array with size >> intermediate buffer
            Arguments.of(bufMuchGreaterThanIntermediateBuf.flip())
        );
    }
}
