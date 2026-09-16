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
import org.apache.kafka.common.protocol.MessageUtil;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        assertEquals("Tried to allocate a collection of size 126, but there are only 7 bytes remaining.",
                assertThrows(RuntimeException.class, () -> out.read(accessor, (short) 2)).getMessage());
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
        assertEquals("Tried to allocate a collection of size 125, but there are only 6 bytes remaining.",
                assertThrows(RuntimeException.class, () -> out.read(accessor, (short) 2)).getMessage());
    }

    @Test
    public void testDeclaredLengthAboveInitialCapacityStillParses() {
        final int count = 1010;
        final ByteBuffer buf = ByteBuffer.allocate(4 + (count * 4));
        buf.putInt(count);
        for (int i = 0; i < count; i++) {
            buf.putInt(i);
        }
        buf.flip();
        final SimpleArraysMessageData out = new SimpleArraysMessageData();
        out.read(new ByteBufferAccessor(buf), (short) 0);
        assertEquals(count, out.sheep().size());
    }

    @Test
    public void testKeyedDeclaredLengthAboveInitialCapacityStillParses() {
        final int count = 1010;
        final ByteBuffer buf = ByteBuffer.allocate(4 + (count * 8));
        buf.putInt(count);
        for (int i = 0; i < count; i++) {
            buf.putInt(i);
            buf.putInt(i * 2);
        }
        buf.flip();
        final SimpleKeyedArraysMessageData out = new SimpleKeyedArraysMessageData();
        out.read(new ByteBufferAccessor(buf), (short) 0);
        assertEquals(count, out.keyedStructs().size());
    }

    @Test
    public void testArrayLengthAboveMaxIsRejected() {
        final int count = MessageUtil.MAX_ARRAY_LENGTH + 1;
        final ByteBuffer buf = ByteBuffer.allocate(4 + count);
        buf.putInt(count);
        buf.rewind();
        final SimpleArraysMessageData out = new SimpleArraysMessageData();
        final ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        assertEquals("Tried to read a collection of size " + count + ", which exceeds the maximum allowed size of "
                        + MessageUtil.MAX_ARRAY_LENGTH + ".",
                assertThrows(RuntimeException.class, () -> out.read(accessor, (short) 0)).getMessage());
    }

    @Test
    public void testKeyedArrayLengthAboveMaxIsRejected() {
        final int count = MessageUtil.MAX_ARRAY_LENGTH + 1;
        final ByteBuffer buf = ByteBuffer.allocate(4 + count);
        buf.putInt(count);
        buf.rewind();
        final SimpleKeyedArraysMessageData out = new SimpleKeyedArraysMessageData();
        final ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        assertEquals("Tried to read a collection of size " + count + ", which exceeds the maximum allowed size of "
                        + MessageUtil.MAX_ARRAY_LENGTH + ".",
                assertThrows(RuntimeException.class, () -> out.read(accessor, (short) 0)).getMessage());
    }
}
