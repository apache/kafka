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
package org.apache.kafka.common.record;

import net.jpountz.xxhash.XXHashFactory;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class KafkaLZ4Test {

    private final static Random RANDOM = new Random(0);

    private final boolean useBrokenFlagDescriptorChecksum;
    private final boolean ignoreFlagDescriptorChecksum;
    private final byte[] payload;
    private final boolean close;
    private final boolean blockChecksum;

    static class Payload {
        String name;
        byte[] payload;

        Payload(String name, byte[] payload) {
            this.name = name;
            this.payload = payload;
        }

        @Override
        public String toString() {
            return "Payload{" +
                   "size=" + payload.length +
                   ", name='" + name + '\'' +
                   '}';
        }
    }

    @Parameters(name = "{index} useBrokenFlagDescriptorChecksum={0}, ignoreFlagDescriptorChecksum={1}, blockChecksum={2}, close={3}, payload={4}")
    public static Collection<Object[]> data() {
        List<Payload> payloads = new ArrayList<>();

        payloads.add(new Payload("empty", new byte[]{}));
        payloads.add(new Payload("onebyte", new byte[]{1}));

        for (int size : Arrays.asList(1000, 1 << 16, (1 << 10) * 96)) {
            byte[] random = new byte[size];
            RANDOM.nextBytes(random);
            payloads.add(new Payload("random", random));

            byte[] ones = new byte[size];
            Arrays.fill(ones, (byte) 1);
            payloads.add(new Payload("ones", ones));
        }

        List<Object[]> values = new ArrayList<>();
        for (Payload payload : payloads)
            for (boolean broken : Arrays.asList(false, true))
                for (boolean ignore : Arrays.asList(false, true))
                    for (boolean blockChecksum : Arrays.asList(false, true))
                        for (boolean close : Arrays.asList(false, true))
                            values.add(new Object[]{broken, ignore, blockChecksum, close, payload});
        return values;
    }

    public KafkaLZ4Test(boolean useBrokenFlagDescriptorChecksum, boolean ignoreFlagDescriptorChecksum,
                        boolean blockChecksum, boolean close, Payload payload) {
        this.useBrokenFlagDescriptorChecksum = useBrokenFlagDescriptorChecksum;
        this.ignoreFlagDescriptorChecksum = ignoreFlagDescriptorChecksum;
        this.payload = payload.payload;
        this.close = close;
        this.blockChecksum = blockChecksum;
    }

    @Test
    public void testHeaderPrematureEnd() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        IOException e = assertThrows(IOException.class, () -> makeInputStream(buffer));
        assertEquals(KafkaLZ4BlockInputStream.PREMATURE_EOS, e.getMessage());
    }

    private KafkaLZ4BlockInputStream makeInputStream(ByteBuffer buffer) throws IOException {
        return new KafkaLZ4BlockInputStream(buffer, BufferSupplier.create(), ignoreFlagDescriptorChecksum);
    }

    @Test
    public void testNotSupported() throws Exception {
        byte[] compressed = compressedBytes();
        compressed[0] = 0x00;
        ByteBuffer buffer = ByteBuffer.wrap(compressed);
        IOException e = assertThrows(IOException.class, () -> makeInputStream(buffer));
        assertEquals(KafkaLZ4BlockInputStream.NOT_SUPPORTED, e.getMessage());
    }

    @Test
    public void testBadFrameChecksum() throws Exception {
        byte[] compressed = compressedBytes();
        compressed[6] = (byte) 0xFF;
        ByteBuffer buffer = ByteBuffer.wrap(compressed);

        if (ignoreFlagDescriptorChecksum) {
            makeInputStream(buffer);
        } else {
            IOException e = assertThrows(IOException.class, () -> makeInputStream(buffer));
            assertEquals(KafkaLZ4BlockInputStream.DESCRIPTOR_HASH_MISMATCH, e.getMessage());
        }
    }

    @Test
    public void testBadBlockSize() throws Exception {
        if (!close || (useBrokenFlagDescriptorChecksum && !ignoreFlagDescriptorChecksum))
            return;

        byte[] compressed = compressedBytes();
        ByteBuffer buffer = ByteBuffer.wrap(compressed).order(ByteOrder.LITTLE_ENDIAN);

        int blockSize = buffer.getInt(7);
        blockSize = (blockSize & LZ4_FRAME_INCOMPRESSIBLE_MASK) | (1 << 24 & ~LZ4_FRAME_INCOMPRESSIBLE_MASK);
        buffer.putInt(7, blockSize);

        IOException e = assertThrows(IOException.class, () -> testDecompression(buffer));
        assertThat(e.getMessage(), CoreMatchers.containsString("exceeded max"));
    }



    @Test
    public void testCompression() throws Exception {
        byte[] compressed = compressedBytes();

        // Check magic bytes stored as little-endian
        int offset = 0;
        assertEquals(0x04, compressed[offset++]);
        assertEquals(0x22, compressed[offset++]);
        assertEquals(0x4D, compressed[offset++]);
        assertEquals(0x18, compressed[offset++]);

        // Check flg descriptor
        byte flg = compressed[offset++];

        // 2-bit version must be 01
        int version = (flg >>> 6) & 3;
        assertEquals(1, version);

        // Reserved bits should always be 0
        int reserved = flg & 3;
        assertEquals(0, reserved);

        // Check block descriptor
        byte bd = compressed[offset++];

        // Block max-size
        int blockMaxSize = (bd >>> 4) & 7;
        // Only supported values are 4 (64KB), 5 (256KB), 6 (1MB), 7 (4MB)
        assertTrue(blockMaxSize >= 4);
        assertTrue(blockMaxSize <= 7);

        // Multiple reserved bit ranges in block descriptor
        reserved = bd & 15;
        assertEquals(0, reserved);
        reserved = (bd >>> 7) & 1;
        assertEquals(0, reserved);

        // If flg descriptor sets content size flag
        // there are 8 additional bytes before checksum
        boolean contentSize = ((flg >>> 3) & 1) != 0;
        if (contentSize)
            offset += 8;

        // Checksum applies to frame descriptor: flg, bd, and optional contentsize
        // so initial offset should be 4 (for magic bytes)
        int off = 4;
        int len = offset - 4;

        // Initial implementation of checksum incorrectly applied to full header
        // including magic bytes
        if (this.useBrokenFlagDescriptorChecksum) {
            off = 0;
            len = offset;
        }

        int hash = XXHashFactory.fastestInstance().hash32().hash(compressed, off, len, 0);

        byte hc = compressed[offset++];
        assertEquals((byte) ((hash >> 8) & 0xFF), hc);

        // Check EndMark, data block with size `0` expressed as a 32-bits value
        if (this.close) {
            offset = compressed.length - 4;
            assertEquals(0, compressed[offset++]);
            assertEquals(0, compressed[offset++]);
            assertEquals(0, compressed[offset++]);
            assertEquals(0, compressed[offset++]);
        }
    }

    @Test
    public void testArrayBackedBuffer() throws IOException {
        byte[] compressed = compressedBytes();
        testDecompression(ByteBuffer.wrap(compressed));
    }

    @Test
    public void testArrayBackedBufferSlice() throws IOException {
        byte[] compressed = compressedBytes();

        int sliceOffset = 12;

        ByteBuffer buffer = ByteBuffer.allocate(compressed.length + sliceOffset + 123);
        buffer.position(sliceOffset);
        buffer.put(compressed).flip();
        buffer.position(sliceOffset);

        ByteBuffer slice = buffer.slice();
        testDecompression(slice);

        int offset = 42;
        buffer = ByteBuffer.allocate(compressed.length + sliceOffset + offset);
        buffer.position(sliceOffset + offset);
        buffer.put(compressed).flip();
        buffer.position(sliceOffset);

        slice = buffer.slice();
        slice.position(offset);
        testDecompression(slice);
    }

    @Test
    public void testDirectBuffer() throws IOException {
        byte[] compressed = compressedBytes();
        ByteBuffer buffer;

        buffer = ByteBuffer.allocateDirect(compressed.length);
        buffer.put(compressed).flip();
        testDecompression(buffer);

        int offset = 42;
        buffer = ByteBuffer.allocateDirect(compressed.length + offset + 123);
        buffer.position(offset);
        buffer.put(compressed).flip();
        buffer.position(offset);
        testDecompression(buffer);
    }

    @Test
    public void testSkip() throws Exception {
        if (!close || (useBrokenFlagDescriptorChecksum && !ignoreFlagDescriptorChecksum)) return;

        final KafkaLZ4BlockInputStream in = makeInputStream(ByteBuffer.wrap(compressedBytes()));

        int n = 100;
        int remaining = payload.length;
        long skipped = in.skip(n);
        assertEquals(Math.min(n, remaining), skipped);

        n = 10000;
        remaining -= skipped;
        skipped = in.skip(n);
        assertEquals(Math.min(n, remaining), skipped);
    }

    private void testDecompression(ByteBuffer buffer) throws IOException {
        IOException error = null;
        try {
            KafkaLZ4BlockInputStream decompressed = makeInputStream(buffer);

            byte[] testPayload = new byte[payload.length];

            byte[] tmp = new byte[1024];
            int n, pos = 0, i = 0;
            while ((n = decompressed.read(tmp, i, tmp.length - i)) != -1) {
                i += n;
                if (i == tmp.length) {
                    System.arraycopy(tmp, 0, testPayload, pos, i);
                    pos += i;
                    i = 0;
                }
            }
            System.arraycopy(tmp, 0, testPayload, pos, i);
            pos += i;

            assertEquals(-1, decompressed.read(tmp, 0, tmp.length));
            assertEquals(this.payload.length, pos);
            assertArrayEquals(this.payload, testPayload);
        } catch (IOException e) {
            if (!ignoreFlagDescriptorChecksum && useBrokenFlagDescriptorChecksum) {
                assertEquals(KafkaLZ4BlockInputStream.DESCRIPTOR_HASH_MISMATCH, e.getMessage());
                error = e;
            } else if (!close) {
                assertEquals(KafkaLZ4BlockInputStream.PREMATURE_EOS, e.getMessage());
                error = e;
            } else {
                throw e;
            }
        }
        if (!ignoreFlagDescriptorChecksum && useBrokenFlagDescriptorChecksum) assertNotNull(error);
        if (!close) assertNotNull(error);
    }

    private byte[] compressedBytes() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        KafkaLZ4BlockOutputStream lz4 = new KafkaLZ4BlockOutputStream(
            output,
            KafkaLZ4BlockOutputStream.BLOCKSIZE_64KB,
            blockChecksum,
            useBrokenFlagDescriptorChecksum
        );
        lz4.write(this.payload, 0, this.payload.length);
        if (this.close) {
            lz4.close();
        } else {
            lz4.flush();
        }
        return output.toByteArray();
    }
}
