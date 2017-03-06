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

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ByteUtilsTest {

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
    public void testVarIntSerde() {
        assertVarIntSerde(0, 1);
        assertVarIntSerde(-1, 1);
        assertVarIntSerde(1, 1);
        assertVarIntSerde(Integer.MAX_VALUE, 5);
        assertVarIntSerde(Integer.MIN_VALUE, 5);
    }

    @Test
    public void testVarUnsignedIntSerde() {
        assertVarUnsignedIntSerde(0, 1);
        assertVarUnsignedIntSerde(1, 1);
        assertVarUnsignedIntSerde(Integer.MAX_VALUE * 2L, 5);
    }

    @Test
    public void testVarLongSerde() {
        assertVarLongSerde(0L, 1);
        assertVarLongSerde(-1L, 1);
        assertVarLongSerde(1L, 1);
        assertVarLongSerde(Integer.MAX_VALUE, 5);
        assertVarLongSerde(Integer.MIN_VALUE, 5);
        assertVarLongSerde(Long.MAX_VALUE, 10);
        assertVarLongSerde(Long.MIN_VALUE, 10);
    }

    private void assertVarIntSerde(int value, int expectedLength) {
        ByteBuffer buf = ByteBuffer.allocate(32);
        ByteUtils.writeVarInt(value, buf);
        buf.flip();
        assertEquals(expectedLength, buf.remaining());
        assertEquals(expectedLength, ByteUtils.bytesForVarIntEncoding(value));
        assertEquals(value, ByteUtils.readVarInt(buf));
    }

    private void assertVarUnsignedIntSerde(long value, int expectedLength) {
        ByteBuffer buf = ByteBuffer.allocate(32);
        ByteUtils.writeVarUnsignedInt(value, buf);
        buf.flip();
        assertEquals(expectedLength, buf.remaining());
        assertEquals(expectedLength, ByteUtils.bytesForVarUnsignedIntEncoding(value));
        assertEquals(value, ByteUtils.readVarUnsignedInt(buf));
    }

    private void assertVarLongSerde(long value, int expectedLength) {
        ByteBuffer buf = ByteBuffer.allocate(32);
        ByteUtils.writeVarLong(value, buf);
        buf.flip();
        assertEquals(expectedLength, buf.remaining());
        assertEquals(expectedLength, ByteUtils.bytesForVarLongEncoding(value));
        assertEquals(value, ByteUtils.readVarLong(buf));
    }

}
