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

import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ChunkedBytesStream;

import net.jpountz.xxhash.XXHashFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.kafka.common.compress.Lz4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK;
import static org.apache.kafka.common.record.internal.CompressionType.LZ4;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Lz4CompressionTest {

    private static final Random RANDOM = new Random(0);

    @Test
    public void testLz4FramingMagicV0() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        Lz4Compression compression = new Lz4Compression.Builder().build();
        Lz4BlockOutputStream out = (Lz4BlockOutputStream) compression.wrapForOutput(
                new ByteBufferOutputStream(buffer), RecordBatch.MAGIC_VALUE_V0);
        assertTrue(out.useBrokenFlagDescriptorChecksum());

        buffer.rewind();

        ChunkedBytesStream in = (ChunkedBytesStream) compression.wrapForInput(buffer, RecordBatch.MAGIC_VALUE_V0, BufferSupplier.NO_CACHING);
        assertTrue(((Lz4BlockInputStream) in.sourceStream()).ignoreFlagDescriptorChecksum());
    }

    @Test
    public void testLz4FramingMagicV1() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        Lz4Compression compression = new Lz4Compression.Builder().build();
        Lz4BlockOutputStream out = (Lz4BlockOutputStream) compression.wrapForOutput(
                new ByteBufferOutputStream(buffer), RecordBatch.MAGIC_VALUE_V1);
        assertFalse(out.useBrokenFlagDescriptorChecksum());

        buffer.rewind();

        ChunkedBytesStream in = (ChunkedBytesStream) compression.wrapForInput(buffer, RecordBatch.MAGIC_VALUE_V1, BufferSupplier.create());
        assertFalse(((Lz4BlockInputStream) in.sourceStream()).ignoreFlagDescriptorChecksum());
    }

    @Test
    public void testCompressionDecompression() throws IOException {
        Lz4Compression.Builder builder = Compression.lz4();
        byte[] data = String.join("", Collections.nCopies(256, "data")).getBytes(StandardCharsets.UTF_8);

        for (byte magic : Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)) {
            for (int level : Arrays.asList(LZ4.minLevel(), LZ4.defaultLevel(), LZ4.maxLevel())) {
                Lz4Compression compression = builder.level(level).build();
                ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(4);
                try (OutputStream out = compression.wrapForOutput(bufferStream, magic)) {
                    out.write(data);
                    out.flush();
                }
                bufferStream.buffer().flip();

                try (InputStream inputStream = compression.wrapForInput(bufferStream.buffer(), magic, BufferSupplier.create())) {
                    byte[] result = new byte[data.length];
                    int read = inputStream.read(result);
                    assertEquals(data.length, read);
                    assertArrayEquals(data, result);
                }
            }
        }
    }

    @Test
    public void testCompressionLevels() {
        Lz4Compression.Builder builder = Compression.lz4();

        assertThrows(IllegalArgumentException.class, () -> builder.level(LZ4.minLevel() - 1));
        assertThrows(IllegalArgumentException.class, () -> builder.level(LZ4.maxLevel() + 1));

        builder.level(LZ4.minLevel());
        builder.level(LZ4.maxLevel());
    }

    private static class Payload {
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

    private static class Args {
        final boolean useBrokenFlagDescriptorChecksum;
        final boolean ignoreFlagDescriptorChecksum;
        final int level;
        final byte[] payload;
        final boolean close;
        final boolean blockChecksum;

        Args(boolean useBrokenFlagDescriptorChecksum, boolean ignoreFlagDescriptorChecksum,
             int level, boolean blockChecksum, boolean close, Payload payload) {
            this.useBrokenFlagDescriptorChecksum = useBrokenFlagDescriptorChecksum;
            this.ignoreFlagDescriptorChecksum = ignoreFlagDescriptorChecksum;
            this.level = level;
            this.blockChecksum = blockChecksum;
            this.close = close;
            this.payload = payload.payload;
        }

        @Override
        public String toString() {
            return "useBrokenFlagDescriptorChecksum=" + useBrokenFlagDescriptorChecksum +
                ", ignoreFlagDescriptorChecksum=" + ignoreFlagDescriptorChecksum +
                ", level=" + level +
                ", blockChecksum=" + blockChecksum +
                ", close=" + close +
                ", payload=" + Arrays.toString(payload);
        }
    }

    private static class Lz4ArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            List<Payload> payloads = new ArrayList<>();

            payloads.add(new Payload("empty", new byte[0]));
            payloads.add(new Payload("onebyte", new byte[]{1}));

            for (int size : Arrays.asList(1000, 1 << 16, (1 << 10) * 96)) {
                byte[] random = new byte[size];
                RANDOM.nextBytes(random);
                payloads.add(new Payload("random", random));

                byte[] ones = new byte[size];
                Arrays.fill(ones, (byte) 1);
                payloads.add(new Payload("ones", ones));
            }

            List<Arguments> arguments = new ArrayList<>();
            for (Payload payload : payloads)
                for (boolean broken : Arrays.asList(false, true))
                    for (boolean ignore : Arrays.asList(false, true))
                        for (boolean blockChecksum : Arrays.asList(false, true))
                            for (boolean close : Arrays.asList(false, true))
                                for (int level : Arrays.asList(LZ4.minLevel(), LZ4.defaultLevel(), LZ4.maxLevel()))
                                    arguments.add(Arguments.of(new Args(broken, ignore, level, blockChecksum, close, payload)));

            return arguments.stream();
        }
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider.class)
    public void testHeaderPrematureEnd(Args args) {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        IOException e = assertThrows(IOException.class, () -> makeInputStream(buffer, args.ignoreFlagDescriptorChecksum));
        assertEquals(Lz4BlockInputStream.PREMATURE_EOS, e.getMessage());
    }

    private Lz4BlockInputStream makeInputStream(ByteBuffer buffer, boolean ignoreFlagDescriptorChecksum) throws IOException {
        return new Lz4BlockInputStream(buffer, BufferSupplier.create(), ignoreFlagDescriptorChecksum);
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider.class)
    public void testNotSupported(Args args) throws Exception {
        byte[] compressed = compressedBytes(args);
        compressed[0] = 0x00;
        ByteBuffer buffer = ByteBuffer.wrap(compressed);
        IOException e = assertThrows(IOException.class, () -> makeInputStream(buffer, args.ignoreFlagDescriptorChecksum));
        assertEquals(Lz4BlockInputStream.NOT_SUPPORTED, e.getMessage());
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider.class)
    public void testBadFrameChecksum(Args args) throws Exception {
        byte[] compressed = compressedBytes(args);
        compressed[6] = (byte) 0xFF;
        ByteBuffer buffer = ByteBuffer.wrap(compressed);

        if (args.ignoreFlagDescriptorChecksum) {
            makeInputStream(buffer, args.ignoreFlagDescriptorChecksum);
        } else {
            IOException e = assertThrows(IOException.class, () -> makeInputStream(buffer, args.ignoreFlagDescriptorChecksum));
            assertEquals(Lz4BlockInputStream.DESCRIPTOR_HASH_MISMATCH, e.getMessage());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider.class)
    public void testBadBlockSize(Args args) throws Exception {
        if (!args.close || (args.useBrokenFlagDescriptorChecksum && !args.ignoreFlagDescriptorChecksum))
            return;

        byte[] compressed = compressedBytes(args);
        ByteBuffer buffer = ByteBuffer.wrap(compressed).order(ByteOrder.LITTLE_ENDIAN);

        int blockSize = buffer.getInt(7);
        blockSize = (blockSize & LZ4_FRAME_INCOMPRESSIBLE_MASK) | (1 << 24 & ~LZ4_FRAME_INCOMPRESSIBLE_MASK);
        buffer.putInt(7, blockSize);

        IOException e = assertThrows(IOException.class, () -> testDecompression(buffer, args));
        assertTrue(e.getMessage().contains("exceeded max"));
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider.class)
    public void testCompression(Args args) throws Exception {
        byte[] compressed = compressedBytes(args);

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
        if (args.useBrokenFlagDescriptorChecksum) {
            off = 0;
            len = offset;
        }

        int hash = XXHashFactory.fastestInstance().hash32().hash(compressed, off, len, 0);

        byte hc = compressed[offset++];
        assertEquals((byte) ((hash >> 8) & 0xFF), hc);

        // Check EndMark, data block with size `0` expressed as a 32-bits value
        if (args.close) {
            offset = compressed.length - 4;
            assertEquals(0, compressed[offset++]);
            assertEquals(0, compressed[offset++]);
            assertEquals(0, compressed[offset++]);
            assertEquals(0, compressed[offset++]);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider.class)
    public void testArrayBackedBuffer(Args args) throws IOException {
        byte[] compressed = compressedBytes(args);
        testDecompression(ByteBuffer.wrap(compressed), args);
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider.class)
    public void testArrayBackedBufferSlice(Args args) throws IOException {
        byte[] compressed = compressedBytes(args);

        int sliceOffset = 12;

        ByteBuffer buffer = ByteBuffer.allocate(compressed.length + sliceOffset + 123);
        buffer.position(sliceOffset);
        buffer.put(compressed).flip();
        buffer.position(sliceOffset);

        ByteBuffer slice = buffer.slice();
        testDecompression(slice, args);

        int offset = 42;
        buffer = ByteBuffer.allocate(compressed.length + sliceOffset + offset);
        buffer.position(sliceOffset + offset);
        buffer.put(compressed).flip();
        buffer.position(sliceOffset);

        slice = buffer.slice();
        slice.position(offset);
        testDecompression(slice, args);
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider.class)
    public void testDirectBuffer(Args args) throws IOException {
        byte[] compressed = compressedBytes(args);
        ByteBuffer buffer;

        buffer = ByteBuffer.allocateDirect(compressed.length);
        buffer.put(compressed).flip();
        testDecompression(buffer, args);

        int offset = 42;
        buffer = ByteBuffer.allocateDirect(compressed.length + offset + 123);
        buffer.position(offset);
        buffer.put(compressed).flip();
        buffer.position(offset);
        testDecompression(buffer, args);
    }

    @ParameterizedTest
    @ArgumentsSource(Lz4ArgumentsProvider.class)
    public void testSkip(Args args) throws Exception {
        if (!args.close || (args.useBrokenFlagDescriptorChecksum && !args.ignoreFlagDescriptorChecksum)) return;

        final Lz4BlockInputStream in = makeInputStream(ByteBuffer.wrap(compressedBytes(args)),
            args.ignoreFlagDescriptorChecksum);

        int n = 100;
        long remaining = args.payload.length;
        long skipped = in.skip(n);
        assertEquals(Math.min(n, remaining), skipped);

        n = 10000;
        remaining -= skipped;
        skipped = in.skip(n);
        assertEquals(Math.min(n, remaining), skipped);
    }

    private void testDecompression(ByteBuffer buffer, Args args) throws IOException {
        IOException error = null;
        try {
            Lz4BlockInputStream decompressed = makeInputStream(buffer, args.ignoreFlagDescriptorChecksum);

            byte[] testPayload = new byte[args.payload.length];

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
            assertEquals(args.payload.length, pos);
            assertArrayEquals(args.payload, testPayload);
        } catch (IOException e) {
            if (!args.ignoreFlagDescriptorChecksum && args.useBrokenFlagDescriptorChecksum) {
                assertEquals(Lz4BlockInputStream.DESCRIPTOR_HASH_MISMATCH, e.getMessage());
                error = e;
            } else if (!args.close) {
                assertEquals(Lz4BlockInputStream.PREMATURE_EOS, e.getMessage());
                error = e;
            } else {
                throw e;
            }
        }
        if (!args.ignoreFlagDescriptorChecksum && args.useBrokenFlagDescriptorChecksum) assertNotNull(error);
        if (!args.close) assertNotNull(error);
    }

    private byte[] compressedBytes(Args args) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Lz4BlockOutputStream lz4 = new Lz4BlockOutputStream(
            output,
            Lz4BlockOutputStream.BLOCKSIZE_64KB,
            args.level,
            args.blockChecksum,
            args.useBrokenFlagDescriptorChecksum
        );
        lz4.write(args.payload, 0, args.payload.length);
        if (args.close) {
            lz4.close();
        } else {
            lz4.flush();
        }
        return output.toByteArray();
    }
}
