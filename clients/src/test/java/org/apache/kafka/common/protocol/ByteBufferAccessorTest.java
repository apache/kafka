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
package org.apache.kafka.common.protocol;

import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ByteBufferAccessorTest {
    @Test
    public void testReadArray() {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        final byte[] testArray = new byte[] {0x4b, 0x61, 0x46};
        accessor.writeByteArray(testArray);
        accessor.writeInt(12345);
        accessor.flip();
        final byte[] testArray2 = accessor.readArray(3);
        assertArrayEquals(testArray, testArray2);
        assertEquals(12345, accessor.readInt());
        assertThrows(BufferUnderflowException.class, () -> accessor.readArray(3));
    }

    @Test
    public void testReadString() {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        String testString = "ABC";
        final byte[] testArray = testString.getBytes(StandardCharsets.UTF_8);
        accessor.writeByteArray(testArray);
        accessor.flip();
        assertEquals("ABC", accessor.readString(3));
        assertThrows(BufferUnderflowException.class, () -> accessor.readString(2));
    }

    @Test
    public void testReadByteBufferThrowsBufferUnderflowWhenLengthExceedsRemaining() {
        ByteBufferAccessor accessor = new ByteBufferAccessor(ByteBuffer.allocate(4));
        assertThrows(BufferUnderflowException.class, () -> accessor.readByteBuffer(5));
    }

    @Test
    public void testPrimitiveReadsThrowBufferUnderflowOnEmpty() {
        ByteBufferAccessor empty = new ByteBufferAccessor(ByteBuffer.allocate(0));
        assertThrows(BufferUnderflowException.class, empty::readByte);
        assertThrows(BufferUnderflowException.class, empty::readShort);
        assertThrows(BufferUnderflowException.class, empty::readInt);
        assertThrows(BufferUnderflowException.class, empty::readLong);
        assertThrows(BufferUnderflowException.class, empty::readDouble);
        assertThrows(BufferUnderflowException.class, empty::readUnsignedVarint);
        assertThrows(BufferUnderflowException.class, empty::readVarint);
        assertThrows(BufferUnderflowException.class, empty::readVarlong);
    }

    @Test
    public void testVarintReadsThrowOnMalformedVarint() {
        // Five continuation-bit bytes without a terminating byte: ByteUtils throws IllegalArgumentException.
        byte[] nonTerminating = new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80};
        assertThrows(IllegalArgumentException.class,
            () -> new ByteBufferAccessor(ByteBuffer.wrap(nonTerminating)).readUnsignedVarint());
        assertThrows(IllegalArgumentException.class,
            () -> new ByteBufferAccessor(ByteBuffer.wrap(nonTerminating)).readVarint());
    }
}
