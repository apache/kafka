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
import java.util.zip.Checksum;

import static org.junit.Assert.assertEquals;

public abstract class AbstractChecksumTest {

    protected abstract Checksum createChecksum();

    protected abstract long computeChecksum(byte[] bytes);

    protected abstract long computeChecksum(byte[] bytes, int offset, int length);

    @Test
    public void testUpdateByteBuffer() {
        byte[] bytes = new byte[]{0, 1, 2, 3, 4, 5};
        doTestUpdateByteBuffer(bytes, ByteBuffer.allocate(bytes.length));
        doTestUpdateByteBuffer(bytes, ByteBuffer.allocateDirect(bytes.length));
    }

    private void doTestUpdateByteBuffer(byte[] bytes, ByteBuffer buffer) {
        buffer.put(bytes);
        buffer.flip();
        Checksum bufferCrc = createChecksum();
        Checksums.update(bufferCrc, buffer, buffer.remaining());
        assertEquals(computeChecksum(bytes), bufferCrc.getValue());
        assertEquals(0, buffer.position());
    }

    @Test
    public void testUpdateByteBufferWithOffsetPosition() {
        byte[] bytes = new byte[]{-2, -1, 0, 1, 2, 3, 4, 5};
        doTestUpdateByteBufferWithOffsetPosition(bytes, ByteBuffer.allocate(bytes.length), 2);
        doTestUpdateByteBufferWithOffsetPosition(bytes, ByteBuffer.allocateDirect(bytes.length), 2);
    }

    @Test
    public void testUpdate() {
        final byte[] bytes = "Any String you want".getBytes();
        final int len = bytes.length;

        Checksum crc1 = createChecksum();
        Checksum crc2 = createChecksum();
        Checksum crc3 = createChecksum();

        crc1.update(bytes, 0, len);
        for (int i = 0; i < len; i++)
            crc2.update(bytes[i]);
        crc3.update(bytes, 0, len / 2);
        crc3.update(bytes, len / 2, len - len / 2);

        assertEquals("Crc values should be the same", crc1.getValue(), crc2.getValue());
        assertEquals("Crc values should be the same", crc1.getValue(), crc3.getValue());
    }

    @Test
    public void testUpdateInt() {
        final int value = 1000;
        final ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);

        Checksum crc1 = createChecksum();
        Checksum crc2 = createChecksum();

        Checksums.updateInt(crc1, value);
        crc2.update(buffer.array(), buffer.arrayOffset(), 4);

        assertEquals("Crc values should be the same", crc1.getValue(), crc2.getValue());
    }

    private void doTestUpdateByteBufferWithOffsetPosition(byte[] bytes, ByteBuffer buffer, int offset) {
        buffer.put(bytes);
        buffer.flip();
        buffer.position(offset);

        Checksum bufferCrc = createChecksum();
        Checksums.update(bufferCrc, buffer, buffer.remaining());
        assertEquals(computeChecksum(bytes, offset, buffer.remaining()), bufferCrc.getValue());
        assertEquals(offset, buffer.position());
    }

}
