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

import org.apache.kafka.common.compress.Lz4BlockOutputStream.BD;
import org.apache.kafka.common.compress.Lz4BlockOutputStream.FLG;
import org.apache.kafka.common.utils.internals.BufferSupplier;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.kafka.common.compress.Lz4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK;
import static org.apache.kafka.common.compress.Lz4BlockOutputStream.MAGIC;

/**
 * A partial implementation of the v1.5.1 LZ4 Frame format.
 *
 * @see <a href="https://github.com/lz4/lz4/wiki/lz4_Frame_format.md">LZ4 Frame Format</a>
 *
 * This class is not thread-safe.
 */
public final class Lz4BlockInputStream extends InputStream {

    public static final String PREMATURE_EOS = "Stream ended prematurely";
    public static final String NOT_SUPPORTED = "Stream unsupported (invalid magic bytes)";
    public static final String BLOCK_HASH_MISMATCH = "Block checksum mismatch";
    public static final String DESCRIPTOR_HASH_MISMATCH = "Stream frame descriptor corrupted";

    private static final LZ4SafeDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor();
    private static final XXHash32 CHECKSUM = XXHashFactory.fastestInstance().hash32();

    private static final RuntimeException BROKEN_LZ4_EXCEPTION;
    // https://issues.apache.org/jira/browse/KAFKA-9203
    // detect buggy lz4 libraries on the classpath
    static {
        RuntimeException exception = null;
        try {
            detectBrokenLz4Version();
        } catch (RuntimeException e) {
            exception = e;
        }
        BROKEN_LZ4_EXCEPTION = exception;
    }

    private final InputStream inStream;
    private final boolean ignoreFlagDescriptorChecksum;
    private final BufferSupplier bufferSupplier;
    // Per-block staging buffer. Holds one block plus its optional 4-byte checksum.
    private final ByteBuffer staging;
    private final ByteBuffer decompressionBuffer;
    // `flg` and `maxBlockSize` are effectively final, they are initialised in the `readHeader` method that is only
    // invoked from the constructor
    private FLG flg;
    private int maxBlockSize;

    // If a block is compressed, this is the same as `decompressionBuffer`. If a block is not compressed, this is
    // a slice of `in` to avoid unnecessary copies.
    private ByteBuffer decompressedBuffer;
    private boolean finished;

    /**
     * @param in The input stream supplying LZ4-compressed bytes
     * @param ignoreFlagDescriptorChecksum for compatibility with old kafka clients, ignore incorrect HC byte
     */
    public Lz4BlockInputStream(InputStream in, BufferSupplier bufferSupplier, boolean ignoreFlagDescriptorChecksum) throws IOException {
        if (BROKEN_LZ4_EXCEPTION != null) {
            throw BROKEN_LZ4_EXCEPTION;
        }
        this.inStream = in;
        this.ignoreFlagDescriptorChecksum = ignoreFlagDescriptorChecksum;
        this.bufferSupplier = bufferSupplier;
        readHeader();
        this.staging = bufferSupplier.get(maxBlockSize + 4).order(ByteOrder.LITTLE_ENDIAN);
        this.decompressionBuffer = bufferSupplier.get(maxBlockSize);
        this.finished = false;
    }

    /**
     * Check whether this stream is configured to ignore the Frame Descriptor checksum, which is useful for
     * compatibility with old client implementations that use incorrect checksum calculations.
     */
    public boolean ignoreFlagDescriptorChecksum() {
        return this.ignoreFlagDescriptorChecksum;
    }

    /**
     * Reads magic and frame descriptor from {@code inStream} into a 15-byte temporary array — the maximum
     * possible header size (4 magic + 1 FLG + 1 BD + 8 optional content size + 1 HC).
     */
    private void readHeader() throws IOException {
        byte[] hdr = new byte[15];
        // Need 6 bytes to know whether the content-size field is present
        readFully(hdr, 0, 6);
        int magic = ByteBuffer.wrap(hdr, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (magic != MAGIC) {
            throw new IOException(NOT_SUPPORTED);
        }
        flg = FLG.fromByte(hdr[4]);
        maxBlockSize = BD.fromByte(hdr[5]).getBlockMaximumSize();

        int descLen;
        if (flg.isContentSizeSet()) {
            // 8 content-size bytes + 1 HC byte
            readFully(hdr, 6, 9);
            descLen = 10;
        } else {
            // just the HC byte
            readFully(hdr, 6, 1);
            descLen = 2;
        }

        if (ignoreFlagDescriptorChecksum) {
            return;
        }
        int hash = CHECKSUM.hash(hdr, 4, descLen, 0);
        if (hdr[4 + descLen] != (byte) ((hash >> 8) & 0xFF)) {
            throw new IOException(DESCRIPTOR_HASH_MISMATCH);
        }
    }

    /**
     * Decodes the next block, populating {@link #decompressedBuffer} (or sets {@link #finished} on end-mark).
     */
    private void readBlock() throws IOException {
        int rawBlockSize = readFrameInt();
        boolean compressed = (rawBlockSize & LZ4_FRAME_INCOMPRESSIBLE_MASK) == 0;
        int blockSize = rawBlockSize & ~LZ4_FRAME_INCOMPRESSIBLE_MASK;

        // Check for EndMark
        if (blockSize == 0) {
            finished = true;
            if (flg.isContentChecksumSet()) {
                readFrameInt(); // TODO: verify this content checksum
            }
            return;
        }
        if (blockSize > maxBlockSize) {
            throw new IOException(String.format("Block size %d exceeded max: %d", blockSize, maxBlockSize));
        }

        fillStaging(blockSize);

        if (compressed) {
            try {
                int sz = DECOMPRESSOR.decompress(staging, 0, blockSize, decompressionBuffer, 0, maxBlockSize);
                decompressionBuffer.position(0);
                decompressionBuffer.limit(sz);
                decompressedBuffer = decompressionBuffer;
            } catch (LZ4Exception e) {
                throw new IOException(e);
            }
        } else {
            // Copy into decompressionBuffer because `staging` is reused for the next block, which would corrupt a slice.
            decompressionBuffer.clear();
            decompressionBuffer.put(staging.array(), staging.arrayOffset(), blockSize);
            decompressionBuffer.flip();
            decompressedBuffer = decompressionBuffer;
        }

        // Hash before the next readFrameInt overwrites staging.
        if (flg.isBlockChecksumSet()) {
            int computedHash = CHECKSUM.hash(staging, 0, blockSize, 0);
            if (computedHash != readFrameInt()) {
                throw new IOException(BLOCK_HASH_MISMATCH);
            }
        }
    }

    private int readFrameInt() throws IOException {
        fillStaging(4);
        return staging.getInt(0);
    }

    // Refill the staging buffer with exactly `count` bytes from the input stream. On return, staging is in
    // read mode: position=0, limit=count.
    private void fillStaging(int count) throws IOException {
        readFully(staging.array(), staging.arrayOffset(), count);
        staging.position(0);
        staging.limit(count);
    }

    private void readFully(byte[] dst, int off, int len) throws IOException {
        int read = 0;
        while (read < len) {
            int n = inStream.read(dst, off + read, len - read);
            if (n < 0) {
                throw new IOException(PREMATURE_EOS);
            }
            read += n;
        }
    }

    @Override
    public int read() throws IOException {
        if (finished) {
            return -1;
        }
        if (available() == 0) {
            readBlock();
        }
        if (finished) {
            return -1;
        }
        return decompressedBuffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        net.jpountz.util.SafeUtils.checkRange(b, off, len);
        if (finished) {
            return -1;
        }
        if (available() == 0) {
            readBlock();
        }
        if (finished) {
            return -1;
        }
        len = Math.min(len, available());
        decompressedBuffer.get(b, off, len);
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        if (finished) {
            return 0;
        }
        if (available() == 0) {
            readBlock();
        }
        if (finished) {
            return 0;
        }
        int skipped = (int) Math.min(n, available());
        decompressedBuffer.position(decompressedBuffer.position() + skipped);
        return skipped;
    }

    @Override
    public int available() {
        return decompressedBuffer == null ? 0 : decompressedBuffer.remaining();
    }

    @Override
    public void close() {
        if (staging != null) {
            bufferSupplier.release(staging);
        }
        if (decompressionBuffer != null) {
            bufferSupplier.release(decompressionBuffer);
        }
    }

    @Override
    public void mark(int readlimit) {
        throw new RuntimeException("mark not supported");
    }

    @Override
    public void reset() {
        throw new RuntimeException("reset not supported");
    }

    /**
     * Checks whether the version of lz4 on the classpath has the fix for reading from ByteBuffers with
     * non-zero array offsets (see https://github.com/lz4/lz4-java/pull/65)
     */
    static void detectBrokenLz4Version() {
        byte[] source = new byte[]{1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3};
        final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

        final byte[] compressed = new byte[compressor.maxCompressedLength(source.length)];
        final int compressedLength = compressor.compress(source, 0, source.length, compressed, 0,
                                                         compressed.length);

        // allocate an array-backed ByteBuffer with non-zero array-offset containing the compressed data
        // a buggy decompressor will read the data from the beginning of the underlying array instead of
        // the beginning of the ByteBuffer, failing to decompress the invalid data.
        final byte[] zeroes = {0, 0, 0, 0, 0};
        ByteBuffer nonZeroOffsetBuffer = ByteBuffer
            .allocate(zeroes.length + compressed.length) // allocates the backing array with extra space to offset the data
            .put(zeroes) // prepend invalid bytes (zeros) before the compressed data in the array
            .slice() // create a new ByteBuffer sharing the underlying array, offset to start on the compressed data
            .put(compressed); // write the compressed data at the beginning of this new buffer

        ByteBuffer dest = ByteBuffer.allocate(source.length);
        try {
            DECOMPRESSOR.decompress(nonZeroOffsetBuffer, 0, compressedLength, dest, 0, source.length);
        } catch (Exception e) {
            throw new RuntimeException("Kafka has detected a buggy lz4-java library (< 1.4.x) on the classpath."
                                       + " If you are using Kafka client libraries, make sure your application does not"
                                       + " accidentally override the version provided by Kafka or include multiple versions"
                                       + " of the library on the classpath. The lz4-java version on the classpath should"
                                       + " match the version the Kafka client libraries depend on. Adding -verbose:class"
                                       + " to your JVM arguments may help understand which lz4-java version is getting loaded.", e);
        }
    }
}
