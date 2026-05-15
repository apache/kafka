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
package org.apache.kafka.common.message;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.types.SchemaException;

import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleArraysMessageTest {
    @Test
    public void testArrayBoundsChecking() {
        // SimpleArraysMessageData takes 2 arrays
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {
            (byte) 0x7f, // Set size of first array to 126 which is larger than the size of this buffer
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00
        });
        final SimpleArraysMessageData out = new SimpleArraysMessageData();
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        assertThrows(BufferUnderflowException.class, () -> out.read(accessor, (short) 2));
    }

    @Test
    public void testArrayBoundsCheckingOtherArray() {
        // SimpleArraysMessageData takes 2 arrays
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {
            (byte) 0x01, // Set size of first array to 0
            (byte) 0x7e, // Set size of second array to 125 which is larger than the size of this buffer
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00
        });
        final SimpleArraysMessageData out = new SimpleArraysMessageData();
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        assertThrows(BufferUnderflowException.class, () -> out.read(accessor, (short) 2));
    }

    @Test
    public void testNonNullableStringSerializedAsNullThrowsSchemaException() {
        // SimpleArraysMessage v2 (flexible). Goats[0].Name is a non-nullable string,
        // but the wire encodes its length as -1 (null), which should trigger a SchemaException.
        ByteBuffer buf = ByteBuffer.wrap(new byte[]{
            0x02, // Goats array length+1 varint -> 1 element
            0x00, // Goats[0].Color (int8) = 0
            0x00, // Goats[0].Name length+1 varint -> length = -1 (null) <-- triggers SchemaException
            0x00, // Goats[0] tagged-field count varint = 0
            0x01, // Sheep array length+1 varint -> 0 elements
            0x00  // Message-level tagged-field count varint = 0
        });
        SimpleArraysMessageData out = new SimpleArraysMessageData();
        SchemaException ex = assertThrows(SchemaException.class,
            () -> out.read(new ByteBufferAccessor(buf), (short) 2));
        assertTrue(ex.getMessage().contains("non-nullable field"), ex.getMessage());
    }

    @Test
    public void testStringFieldWithInvalidLengthThrowsSchemaException() {
        // SimpleArraysMessage v2 (flexible). The Name field's varint decodes to length 0x8000
        // (> 0x7fff), which the schema's length guard should reject before any byte is read.
        ByteBuffer buf = ByteBuffer.wrap(new byte[]{
            0x02,                           // Goats array length+1 varint -> 1 element
            0x00,                           // Goats[0].Color (int8) = 0
            (byte) 0x81, (byte) 0x80, 0x02  // Goats[0].Name length+1 varint = 0x8001 -> length 0x8000
        });
        SimpleArraysMessageData out = new SimpleArraysMessageData();
        SchemaException ex = assertThrows(SchemaException.class,
            () -> out.read(new ByteBufferAccessor(buf), (short) 2));
        assertTrue(ex.getMessage().contains("invalid length"), ex.getMessage());
    }
}
