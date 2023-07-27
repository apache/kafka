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

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChecksumsTest {

    @Test
    public void testUpdateByteBuffer() {
        byte[] bytes = new byte[]{0, 1, 2, 3, 4, 5};
        doTestUpdateByteBuffer(bytes, ByteBuffer.allocate(bytes.length));
        doTestUpdateByteBuffer(bytes, ByteBuffer.allocateDirect(bytes.length));
    }

    private void doTestUpdateByteBuffer(byte[] bytes, ByteBuffer buffer) {
        buffer.put(bytes);
        buffer.flip();
        Checksum bufferCrc = Crc32C.create();
        Checksums.update(bufferCrc, buffer, buffer.remaining());
        assertEquals(Crc32C.compute(bytes, 0, bytes.length), bufferCrc.getValue());
        assertEquals(0, buffer.position());
    }

    @Test
    public void testUpdateByteBufferWithOffsetPosition() {
        byte[] bytes = new byte[]{-2, -1, 0, 1, 2, 3, 4, 5};
        doTestUpdateByteBufferWithOffsetPosition(bytes, ByteBuffer.allocate(bytes.length), 2);
        doTestUpdateByteBufferWithOffsetPosition(bytes, ByteBuffer.allocateDirect(bytes.length), 2);
    }

    @Test
    public void testUpdateInt() {
        final int value = 1000;
        final ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);

        Checksum crc1 = Crc32C.create();
        Checksum crc2 = Crc32C.create();

        Checksums.updateInt(crc1, value);
        crc2.update(buffer.array(), buffer.arrayOffset(), 4);

        assertEquals(crc1.getValue(), crc2.getValue(), "Crc values should be the same");
    }

    @Test
    public void testUpdateLong() {
        final long value = Integer.MAX_VALUE + 1;
        final ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);

        Checksum crc1 = Crc32C.create();
        Checksum crc2 = Crc32C.create();

        Checksums.updateLong(crc1, value);
        crc2.update(buffer.array(), buffer.arrayOffset(), 8);

        assertEquals(crc1.getValue(), crc2.getValue(), "Crc values should be the same");
    }

    private void doTestUpdateByteBufferWithOffsetPosition(byte[] bytes, ByteBuffer buffer, int offset) {
        buffer.put(bytes);
        buffer.flip();
        buffer.position(offset);

        Checksum bufferCrc = Crc32C.create();
        Checksums.update(bufferCrc, buffer, buffer.remaining());
        assertEquals(Crc32C.compute(bytes, offset, buffer.remaining()), bufferCrc.getValue());
        assertEquals(offset, buffer.position());
    }

}
