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
package org.apache.kafka.common.compress;

import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.ChunkedBytesStream;

import net.jpountz.xxhash.XXHashFactory;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.kafka.common.compress.Lz4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK;
import static org.apache.kafka.common.compress.Lz4BlockOutputStream.MAGIC;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChunkedBytesStreamLz4Test {
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    public void testRepeatedSkipAcrossEmptyCompressedBlocks(int emptyBlocks) throws IOException {
        ByteBuffer frame = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        frame.putInt(MAGIC);
        byte[] descriptor = {0x60, 0x40}; // Independent blocks, 64 KiB, no block or content checksum
        frame.put(descriptor);
        int hash = XXHashFactory.fastestInstance().hash32().hash(descriptor, 0, descriptor.length, 0);
        frame.put((byte) (hash >>> 8));
        frame.putInt(LZ4_FRAME_INCOMPRESSIBLE_MASK | 4).put(new byte[]{0, 1, 2, 3});
        for (int i = 0; i < emptyBlocks; i++) {
            // Empty input is encoded as a single zero token, not the frame's zero-sized EndMark.
            frame.putInt(1).put((byte) 0);
        }
        frame.putInt(LZ4_FRAME_INCOMPRESSIBLE_MASK | 10).put(new byte[]{4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
        frame.putInt(0);
        frame.flip();

        try (BufferSupplier supplier = BufferSupplier.create();
             InputStream stream = new ChunkedBytesStream(new Lz4BlockInputStream(frame, supplier, false), supplier, 2048, true)) {
            assertEquals(0, stream.read());
            skipBytes(stream, 4);
            assertEquals(5, stream.read());
            assertArrayEquals(new byte[]{6, 7, 8, 9, 10, 11, 12, 13}, stream.readAllBytes());
        }
    }

    // Repeat short skips as DefaultRecord does when skipping a key, value, or header.
    private void skipBytes(InputStream stream, int remaining) throws IOException {
        while (remaining > 0) {
            long skipped = stream.skip(remaining);
            if (skipped > 0 && skipped <= remaining) {
                remaining -= (int) skipped;
            } else if (skipped == 0) {
                if (stream.read() == -1) {
                    throw new EOFException();
                }
                remaining--;
            } else {
                throw new IOException("Invalid skip count");
            }
        }
    }
}
