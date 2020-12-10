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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ByteBufferChannelTest {

    @Test
    public void testWriteBufferArrayWithNonZeroPosition() {
        byte[] data = Utils.utf8("hello");
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.position(10);
        buffer.put(data);

        int limit = buffer.position();
        buffer.position(10);
        buffer.limit(limit);

        ByteBufferChannel channel = new ByteBufferChannel(buffer.remaining());
        ByteBuffer[] buffers = new ByteBuffer[] {buffer};
        channel.write(buffers);
        channel.close();
        ByteBuffer channelBuffer = channel.buffer();
        assertEquals(data.length, channelBuffer.remaining());
        assertEquals("hello", Utils.utf8(channelBuffer));
    }

    @Test
    public void testWriteMultiplesByteBuffers() {
        ByteBuffer[] buffers = new ByteBuffer[] {
            ByteBuffer.wrap(Utils.utf8("hello")),
            ByteBuffer.wrap(Utils.utf8("world"))
        };
        int size = Arrays.stream(buffers).mapToInt(ByteBuffer::remaining).sum();
        ByteBuffer buf;
        try (ByteBufferChannel channel = new ByteBufferChannel(size)) {
            channel.write(buffers, 1, 1);
            buf = channel.buffer();
        }
        assertEquals("world", Utils.utf8(buf));

        try (ByteBufferChannel channel = new ByteBufferChannel(size)) {
            channel.write(buffers, 0, 1);
            buf = channel.buffer();
        }
        assertEquals("hello", Utils.utf8(buf));

        try (ByteBufferChannel channel = new ByteBufferChannel(size)) {
            channel.write(buffers, 0, 2);
            buf = channel.buffer();
        }
        assertEquals("helloworld", Utils.utf8(buf));
    }

    @Test
    public void testInvalidArgumentsInWritsMultiplesByteBuffers() {
        try (ByteBufferChannel channel = new ByteBufferChannel(10)) {
            assertThrows(IndexOutOfBoundsException.class, () -> channel.write(new ByteBuffer[0], 1, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> channel.write(new ByteBuffer[0], -1, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> channel.write(new ByteBuffer[0], 0, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> channel.write(new ByteBuffer[0], 0, 1));
            assertEquals(0, channel.write(new ByteBuffer[0], 0, 0));
        }
    }
}
