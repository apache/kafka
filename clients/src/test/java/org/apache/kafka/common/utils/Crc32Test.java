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

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class Crc32Test {

    @Test
    public void testUpdateByteBuffer() {
        byte[] bytes = new byte[]{0, 1, 2, 3, 4, 5};
        doTestUpdateByteBuffer(bytes, ByteBuffer.allocate(bytes.length));
        doTestUpdateByteBuffer(bytes, ByteBuffer.allocateDirect(bytes.length));
    }

    private void doTestUpdateByteBuffer(byte[] bytes, ByteBuffer buffer) {
        buffer.put(bytes);
        buffer.flip();
        Crc32 bufferCrc = new Crc32();
        bufferCrc.update(buffer, buffer.remaining());
        assertEquals(Crc32.crc32(bytes), bufferCrc.getValue());
        assertEquals(0, buffer.position());
    }

    @Test
    public void testUpdateByteBufferWithOffsetPosition() {
        byte[] bytes = new byte[]{-2, -1, 0, 1, 2, 3, 4, 5};
        doTestUpdateByteBufferWithOffsetPosition(bytes, ByteBuffer.allocate(bytes.length), 2);
        doTestUpdateByteBufferWithOffsetPosition(bytes, ByteBuffer.allocateDirect(bytes.length), 2);
    }

    private void doTestUpdateByteBufferWithOffsetPosition(byte[] bytes, ByteBuffer buffer, int offset) {
        buffer.put(bytes);
        buffer.flip();
        buffer.position(offset);

        Crc32 bufferCrc = new Crc32();
        bufferCrc.update(buffer, buffer.remaining());
        assertEquals(Crc32.crc32(bytes, offset, buffer.remaining()), bufferCrc.getValue());
        assertEquals(offset, buffer.position());
    }

}