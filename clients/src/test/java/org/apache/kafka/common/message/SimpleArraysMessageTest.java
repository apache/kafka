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
}
