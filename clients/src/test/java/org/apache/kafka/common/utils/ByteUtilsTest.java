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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ByteUtilsTest {
    private final byte x00 = 0x00;
    private final byte x01 = 0x01;
    private final byte x02 = 0x02;
    private final byte x0F = 0x0f;
    private final byte x07 = 0x07;
    private final byte x08 = 0x08;
    private final byte x3F = 0x3f;
    private final byte x40 = 0x40;
    private final byte x7E = 0x7E;
    private final byte x7F = 0x7F;
    private final byte xFF = (byte) 0xff;
    private final byte x80 = (byte) 0x80;
    private final byte x81 = (byte) 0x81;
    private final byte xBF = (byte) 0xbf;
    private final byte xC0 = (byte) 0xc0;
    private final byte xFE = (byte) 0xfe;

    @Test
    public void testReadUnsignedIntLEFromArray() {
        byte[] array1 = {0x01, 0x02, 0x03, 0x04, 0x05};
        assertEquals(0x04030201, ByteUtils.readUnsignedIntLE(array1, 0));
        assertEquals(0x05040302, ByteUtils.readUnsignedIntLE(array1, 1));

        byte[] array2 = {(byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4, (byte) 0xf5, (byte) 0xf6};
        assertEquals(0xf4f3f2f1, ByteUtils.readUnsignedIntLE(array2, 0));
        assertEquals(0xf6f5f4f3, ByteUtils.readUnsignedIntLE(array2, 2));
    }

    @Test
    public void testReadUnsignedIntLEFromInputStream() throws IOException {
        byte[] array1 = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
        ByteArrayInputStream is1 = new ByteArrayInputStream(array1);
        assertEquals(0x04030201, ByteUtils.readUnsignedIntLE(is1));
        assertEquals(0x08070605, ByteUtils.readUnsignedIntLE(is1));

        byte[] array2 = {(byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4, (byte) 0xf5, (byte) 0xf6, (byte) 0xf7, (byte) 0xf8};
        ByteArrayInputStream is2 = new ByteArrayInputStream(array2);
        assertEquals(0xf4f3f2f1, ByteUtils.readUnsignedIntLE(is2));
        assertEquals(0xf8f7f6f5, ByteUtils.readUnsignedIntLE(is2));
    }

    @Test
    public void testReadUnsignedInt() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        long writeValue = 133444;
        ByteUtils.writeUnsignedInt(buffer, writeValue);
        buffer.flip();
        long readValue = ByteUtils.readUnsignedInt(buffer);
        assertEquals(writeValue, readValue);
    }

    @Test
    public void testWriteUnsignedIntLEToArray() {
        int value1 = 0x04030201;

        byte[] array1 = new byte[4];
        ByteUtils.writeUnsignedIntLE(array1, 0, value1);
        assertArrayEquals(new byte[] {0x01, 0x02, 0x03, 0x04}, array1);

        array1 = new byte[8];
        ByteUtils.writeUnsignedIntLE(array1, 2, value1);
        assertArrayEquals(new byte[] {0, 0, 0x01, 0x02, 0x03, 0x04, 0, 0}, array1);

        int value2 = 0xf4f3f2f1;

        byte[] array2 = new byte[4];
        ByteUtils.writeUnsignedIntLE(array2, 0, value2);
        assertArrayEquals(new byte[] {(byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4}, array2);

        array2 = new byte[8];
        ByteUtils.writeUnsignedIntLE(array2, 2, value2);
        assertArrayEquals(new byte[] {0, 0, (byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4, 0, 0}, array2);
    }

    @Test
    public void testWriteUnsignedIntLEToOutputStream() throws IOException {
        int value1 = 0x04030201;
        ByteArrayOutputStream os1 = new ByteArrayOutputStream();
        ByteUtils.writeUnsignedIntLE(os1, value1);
        ByteUtils.writeUnsignedIntLE(os1, value1);
        assertArrayEquals(new byte[] {0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04}, os1.toByteArray());

        int value2 = 0xf4f3f2f1;
        ByteArrayOutputStream os2 = new ByteArrayOutputStream();
        ByteUtils.writeUnsignedIntLE(os2, value2);
        assertArrayEquals(new byte[] {(byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4}, os2.toByteArray());
    }

    @Test
    public void testUnsignedVarintSerde() throws Exception {
        assertUnsignedVarintSerde(0, new byte[] {x00});
        assertUnsignedVarintSerde(-1, new byte[] {xFF, xFF, xFF, xFF, x0F});
        assertUnsignedVarintSerde(1, new byte[] {x01});
        assertUnsignedVarintSerde(63, new byte[] {x3F});
        assertUnsignedVarintSerde(-64, new byte[] {xC0, xFF, xFF, xFF, x0F});
        assertUnsignedVarintSerde(64, new byte[] {x40});
        assertUnsignedVarintSerde(8191, new byte[] {xFF, x3F});
        assertUnsignedVarintSerde(-8192, new byte[] {x80, xC0, xFF, xFF, x0F});
        assertUnsignedVarintSerde(8192, new byte[] {x80, x40});
        assertUnsignedVarintSerde(-8193, new byte[] {xFF, xBF, xFF, xFF, x0F});
        assertUnsignedVarintSerde(1048575, new byte[] {xFF, xFF, x3F});
        assertUnsignedVarintSerde(1048576, new byte[] {x80, x80, x40});
        assertUnsignedVarintSerde(Integer.MAX_VALUE, new byte[] {xFF, xFF, xFF, xFF, x07});
        assertUnsignedVarintSerde(Integer.MIN_VALUE, new byte[] {x80, x80, x80, x80, x08});
    }

    @Test
    public void testVarintSerde() throws Exception {
        assertVarintSerde(0, new byte[] {x00});
        assertVarintSerde(-1, new byte[] {x01});
        assertVarintSerde(1, new byte[] {x02});
        assertVarintSerde(63, new byte[] {x7E});
        assertVarintSerde(-64, new byte[] {x7F});
        assertVarintSerde(64, new byte[] {x80, x01});
        assertVarintSerde(-65, new byte[] {x81, x01});
        assertVarintSerde(8191, new byte[] {xFE, x7F});
        assertVarintSerde(-8192, new byte[] {xFF, x7F});
        assertVarintSerde(8192, new byte[] {x80, x80, x01});
        assertVarintSerde(-8193, new byte[] {x81, x80, x01});
        assertVarintSerde(1048575, new byte[] {xFE, xFF, x7F});
        assertVarintSerde(-1048576, new byte[] {xFF, xFF, x7F});
        assertVarintSerde(1048576, new byte[] {x80, x80, x80, x01});
        assertVarintSerde(-1048577, new byte[] {x81, x80, x80, x01});
        assertVarintSerde(134217727, new byte[] {xFE, xFF, xFF, x7F});
        assertVarintSerde(-134217728, new byte[] {xFF, xFF, xFF, x7F});
        assertVarintSerde(134217728, new byte[] {x80, x80, x80, x80, x01});
        assertVarintSerde(-134217729, new byte[] {x81, x80, x80, x80, x01});
        assertVarintSerde(Integer.MAX_VALUE, new byte[] {xFE, xFF, xFF, xFF, x0F});
        assertVarintSerde(Integer.MIN_VALUE, new byte[] {xFF, xFF, xFF, xFF, x0F});
    }

    @Test
    public void testVarlongSerde() throws Exception {
        assertVarlongSerde(0, new byte[] {x00});
        assertVarlongSerde(-1, new byte[] {x01});
        assertVarlongSerde(1, new byte[] {x02});
        assertVarlongSerde(63, new byte[] {x7E});
        assertVarlongSerde(-64, new byte[] {x7F});
        assertVarlongSerde(64, new byte[] {x80, x01});
        assertVarlongSerde(-65, new byte[] {x81, x01});
        assertVarlongSerde(8191, new byte[] {xFE, x7F});
        assertVarlongSerde(-8192, new byte[] {xFF, x7F});
        assertVarlongSerde(8192, new byte[] {x80, x80, x01});
        assertVarlongSerde(-8193, new byte[] {x81, x80, x01});
        assertVarlongSerde(1048575, new byte[] {xFE, xFF, x7F});
        assertVarlongSerde(-1048576, new byte[] {xFF, xFF, x7F});
        assertVarlongSerde(1048576, new byte[] {x80, x80, x80, x01});
        assertVarlongSerde(-1048577, new byte[] {x81, x80, x80, x01});
        assertVarlongSerde(134217727, new byte[] {xFE, xFF, xFF, x7F});
        assertVarlongSerde(-134217728, new byte[] {xFF, xFF, xFF, x7F});
        assertVarlongSerde(134217728, new byte[] {x80, x80, x80, x80, x01});
        assertVarlongSerde(-134217729, new byte[] {x81, x80, x80, x80, x01});
        assertVarlongSerde(Integer.MAX_VALUE, new byte[] {xFE, xFF, xFF, xFF, x0F});
        assertVarlongSerde(Integer.MIN_VALUE, new byte[] {xFF, xFF, xFF, xFF, x0F});
        assertVarlongSerde(17179869183L, new byte[] {xFE, xFF, xFF, xFF, x7F});
        assertVarlongSerde(-17179869184L, new byte[] {xFF, xFF, xFF, xFF, x7F});
        assertVarlongSerde(17179869184L, new byte[] {x80, x80, x80, x80, x80, x01});
        assertVarlongSerde(-17179869185L, new byte[] {x81, x80, x80, x80, x80, x01});
        assertVarlongSerde(2199023255551L, new byte[] {xFE, xFF, xFF, xFF, xFF, x7F});
        assertVarlongSerde(-2199023255552L, new byte[] {xFF, xFF, xFF, xFF, xFF, x7F});
        assertVarlongSerde(2199023255552L, new byte[] {x80, x80, x80, x80, x80, x80, x01});
        assertVarlongSerde(-2199023255553L, new byte[] {x81, x80, x80, x80, x80, x80, x01});
        assertVarlongSerde(281474976710655L, new byte[] {xFE, xFF, xFF, xFF, xFF, xFF, x7F});
        assertVarlongSerde(-281474976710656L, new byte[] {xFF, xFF, xFF, xFF, xFF, xFF, x7F});
        assertVarlongSerde(281474976710656L, new byte[] {x80, x80, x80, x80, x80, x80, x80, x01});
        assertVarlongSerde(-281474976710657L, new byte[] {x81, x80, x80, x80, x80, x80, x80, 1});
        assertVarlongSerde(36028797018963967L, new byte[] {xFE, xFF, xFF, xFF, xFF, xFF, xFF, x7F});
        assertVarlongSerde(-36028797018963968L, new byte[] {xFF, xFF, xFF, xFF, xFF, xFF, xFF, x7F});
        assertVarlongSerde(36028797018963968L, new byte[] {x80, x80, x80, x80, x80, x80, x80, x80, x01});
        assertVarlongSerde(-36028797018963969L, new byte[] {x81, x80, x80, x80, x80, x80, x80, x80, x01});
        assertVarlongSerde(4611686018427387903L, new byte[] {xFE, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x7F});
        assertVarlongSerde(-4611686018427387904L, new byte[] {xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x7F});
        assertVarlongSerde(4611686018427387904L, new byte[] {x80, x80, x80, x80, x80, x80, x80, x80, x80, x01});
        assertVarlongSerde(-4611686018427387905L, new byte[] {x81, x80, x80, x80, x80, x80, x80, x80, x80, x01});
        assertVarlongSerde(Long.MAX_VALUE, new byte[] {xFE, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x01});
        assertVarlongSerde(Long.MIN_VALUE, new byte[] {xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x01});
    }

    @Test
    public void testInvalidVarint() {
        // varint encoding has one overflow byte
        ByteBuffer buf = ByteBuffer.wrap(new byte[] {xFF, xFF, xFF, xFF, xFF, x01});
        assertThrows(IllegalArgumentException.class, () -> ByteUtils.readVarint(buf));
    }

    @Test
    public void testInvalidVarlong() {
        // varlong encoding has one overflow byte
        ByteBuffer buf = ByteBuffer.wrap(new byte[] {xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF, x01});
        assertThrows(IllegalArgumentException.class, () -> ByteUtils.readVarlong(buf));
    }

    @Test
    public void testDouble() throws IOException {
        assertDoubleSerde(0.0, 0x0L);
        assertDoubleSerde(-0.0, 0x8000000000000000L);
        assertDoubleSerde(1.0, 0x3FF0000000000000L);
        assertDoubleSerde(-1.0, 0xBFF0000000000000L);
        assertDoubleSerde(123e45, 0x49B58B82C0E0BB00L);
        assertDoubleSerde(-123e45, 0xC9B58B82C0E0BB00L);
        assertDoubleSerde(Double.MIN_VALUE, 0x1L);
        assertDoubleSerde(-Double.MIN_VALUE, 0x8000000000000001L);
        assertDoubleSerde(Double.MAX_VALUE, 0x7FEFFFFFFFFFFFFFL);
        assertDoubleSerde(-Double.MAX_VALUE, 0xFFEFFFFFFFFFFFFFL);
        assertDoubleSerde(Double.NaN, 0x7FF8000000000000L);
        assertDoubleSerde(Double.POSITIVE_INFINITY, 0x7FF0000000000000L);
        assertDoubleSerde(Double.NEGATIVE_INFINITY, 0xFFF0000000000000L);
    }

    @Test
    public void testSizeOfUnsignedVarint() {
        // The old well-known implementation for sizeOfUnsignedVarint
        IntFunction<Integer> simpleImplementation = (int value) -> {
            int bytes = 1;
            while ((value & 0xffffff80) != 0L) {
                bytes += 1;
                value >>>= 7;
            }
            return bytes;
        };

        // compare the full range of values
        for (int i = 0; i < Integer.MAX_VALUE && i >= 0; i += 13) {
            final int actual = ByteUtils.sizeOfUnsignedVarint(i);
            final int expected = simpleImplementation.apply(i);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSizeOfVarlong() {
        // The old well-known implementation for sizeOfVarlong
        LongFunction<Integer> simpleImplementation = (long value) -> {
            long v = (value << 1) ^ (value >> 63);
            int bytes = 1;
            while ((v & 0xffffffffffffff80L) != 0L) {
                bytes += 1;
                v >>>= 7;
            }
            return bytes;
        };

        for (long l = 1; l < Long.MAX_VALUE && l >= 0; l = l << 1) {
            final int expected = simpleImplementation.apply(l);
            final int actual = ByteUtils.sizeOfVarlong(l);
            assertEquals(expected, actual);
        }

        // check zero as well
        assertEquals(simpleImplementation.apply(0), ByteUtils.sizeOfVarlong(0));
    }

    @Test
    public void testReadInt() {
        int[] values = {
            0, 1, -1, Byte.MAX_VALUE, Short.MAX_VALUE, 2 * Short.MAX_VALUE, Integer.MAX_VALUE / 2,
            Integer.MIN_VALUE / 2, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE
        };
        ByteBuffer buffer = ByteBuffer.allocate(4 * values.length);
        for (int i = 0; i < values.length; ++i) {
            buffer.putInt(i * 4, values[i]);
            assertEquals(values[i], ByteUtils.readIntBE(buffer.array(), i * 4), "Written value should match read value.");
        }
    }

    private void assertUnsignedVarintSerde(int value, byte[] expectedEncoding) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(32);
        ByteUtils.writeUnsignedVarint(value, buf);
        buf.flip();
        assertArrayEquals(expectedEncoding, Utils.toArray(buf));
        assertEquals(value, ByteUtils.readUnsignedVarint(buf.duplicate()));

        buf.rewind();
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buf));
        ByteUtils.writeUnsignedVarint(value, out);
        buf.flip();
        assertArrayEquals(expectedEncoding, Utils.toArray(buf));
        DataInputStream in = new DataInputStream(new ByteBufferInputStream(buf));
        assertEquals(value, ByteUtils.readUnsignedVarint(in));
    }

    private void assertVarintSerde(int value, byte[] expectedEncoding) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(32);
        ByteUtils.writeVarint(value, buf);
        buf.flip();
        assertArrayEquals(expectedEncoding, Utils.toArray(buf));
        assertEquals(value, ByteUtils.readVarint(buf.duplicate()));

        buf.rewind();
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buf));
        ByteUtils.writeVarint(value, out);
        buf.flip();
        assertArrayEquals(expectedEncoding, Utils.toArray(buf));
        DataInputStream in = new DataInputStream(new ByteBufferInputStream(buf));
        assertEquals(value, ByteUtils.readVarint(in));
    }

    private void assertVarlongSerde(long value, byte[] expectedEncoding) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(32);
        ByteUtils.writeVarlong(value, buf);
        buf.flip();
        assertEquals(value, ByteUtils.readVarlong(buf.duplicate()));
        assertArrayEquals(expectedEncoding, Utils.toArray(buf));

        buf.rewind();
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buf));
        ByteUtils.writeVarlong(value, out);
        buf.flip();
        assertArrayEquals(expectedEncoding, Utils.toArray(buf));
        DataInputStream in = new DataInputStream(new ByteBufferInputStream(buf));
        assertEquals(value, ByteUtils.readVarlong(in));
    }

    private void assertDoubleSerde(double value, long expectedLongValue) throws IOException {
        byte[] expectedEncoding = new byte[8];
        for (int i = 0; i < 8; i++) {
            expectedEncoding[7 - i] = (byte) (expectedLongValue & 0xFF);
            expectedLongValue >>= 8;
        }

        ByteBuffer buf = ByteBuffer.allocate(8);
        ByteUtils.writeDouble(value, buf);
        buf.flip();
        assertEquals(value, ByteUtils.readDouble(buf.duplicate()), 0.0);
        assertArrayEquals(expectedEncoding, Utils.toArray(buf));

        buf.rewind();
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buf));
        ByteUtils.writeDouble(value, out);
        buf.flip();
        assertArrayEquals(expectedEncoding, Utils.toArray(buf));
        DataInputStream in = new DataInputStream(new ByteBufferInputStream(buf));
        assertEquals(value, ByteUtils.readDouble(in), 0.0);
    }
}
