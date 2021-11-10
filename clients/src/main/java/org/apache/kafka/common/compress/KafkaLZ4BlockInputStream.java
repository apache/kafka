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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.kafka.common.compress.KafkaLZ4BlockOutputStream.BD;
import org.apache.kafka.common.compress.KafkaLZ4BlockOutputStream.FLG;
import org.apache.kafka.common.utils.BufferSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.kafka.common.compress.KafkaLZ4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK;
import static org.apache.kafka.common.compress.KafkaLZ4BlockOutputStream.MAGIC;

/**
 * A partial implementation of the v1.5.1 LZ4 Frame format.
 *
 * @see <a href="https://github.com/lz4/lz4/wiki/lz4_Frame_format.md">LZ4 Frame Format</a>
 *
 * This class is not thread-safe.
 */
public final class KafkaLZ4BlockInputStream extends InputStream {

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

    private final ByteBuffer in;
    private final boolean ignoreFlagDescriptorChecksum;
    private final BufferSupplier bufferSupplier;
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
     * Create a new {@link InputStream} that will decompress data using the LZ4 algorithm.
     *
     * @param in The byte buffer to decompress
     * @param ignoreFlagDescriptorChecksum for compatibility with old kafka clients, ignore incorrect HC byte
     * @throws IOException
     */
    public KafkaLZ4BlockInputStream(ByteBuffer in, BufferSupplier bufferSupplier, boolean ignoreFlagDescriptorChecksum) throws IOException {
        if (BROKEN_LZ4_EXCEPTION != null) {
            throw BROKEN_LZ4_EXCEPTION;
        }
        this.ignoreFlagDescriptorChecksum = ignoreFlagDescriptorChecksum;
        this.in = in.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        this.bufferSupplier = bufferSupplier;
        readHeader();
        decompressionBuffer = bufferSupplier.get(maxBlockSize);
        finished = false;
    }

    /**
     * Check whether KafkaLZ4BlockInputStream is configured to ignore the
     * Frame Descriptor checksum, which is useful for compatibility with
     * old client implementations that use incorrect checksum calculations.
     */
    public boolean ignoreFlagDescriptorChecksum() {
        return this.ignoreFlagDescriptorChecksum;
    }

    /**
     * Reads the magic number and frame descriptor from input buffer.
     *
     * @throws IOException
     */
    private void readHeader() throws IOException {
        // read first 6 bytes into buffer to check magic and FLG/BD descriptor flags
        if (in.remaining() < 6) {
            throw new IOException(PREMATURE_EOS);
        }

        if (MAGIC != in.getInt()) {
            throw new IOException(NOT_SUPPORTED);
        }
        // mark start of data to checksum
        in.mark();

        flg = FLG.fromByte(in.get());
        maxBlockSize = BD.fromByte(in.get()).getBlockMaximumSize();

        if (flg.isContentSizeSet()) {
            if (in.remaining() < 8) {
                throw new IOException(PREMATURE_EOS);
            }
            in.position(in.position() + 8);
        }

        // Final byte of Frame Descriptor is HC checksum

        // Old implementations produced incorrect HC checksums
        if (ignoreFlagDescriptorChecksum) {
            in.position(in.position() + 1);
            return;
        }

        int len = in.position() - in.reset().position();

        int hash = CHECKSUM.hash(in, in.position(), len, 0);
        in.position(in.position() + len);
        if (in.get() != (byte) ((hash >> 8) & 0xFF)) {
            throw new IOException(DESCRIPTOR_HASH_MISMATCH);
        }
    }

    /**
     * Decompresses (if necessary) buffered data, optionally computes and validates a XXHash32 checksum, and writes the
     * result to a buffer.
     *
     * @throws IOException
     */
    private void readBlock() throws IOException {
        if (in.remaining() < 4) {
            throw new IOException(PREMATURE_EOS);
        }

        int blockSize = in.getInt();
        boolean compressed = (blockSize & LZ4_FRAME_INCOMPRESSIBLE_MASK) == 0;
        blockSize &= ~LZ4_FRAME_INCOMPRESSIBLE_MASK;

        // Check for EndMark
        if (blockSize == 0) {
            finished = true;
            if (flg.isContentChecksumSet())
                in.getInt(); // TODO: verify this content checksum
            return;
        } else if (blockSize > maxBlockSize) {
            throw new IOException(String.format("Block size %s exceeded max: %s", blockSize, maxBlockSize));
        }

        if (in.remaining() < blockSize) {
            throw new IOException(PREMATURE_EOS);
        }

        if (compressed) {
            try {
                final int bufferSize = DECOMPRESSOR.decompress(in, in.position(), blockSize, decompressionBuffer, 0,
                    maxBlockSize);
                decompressionBuffer.position(0);
                decompressionBuffer.limit(bufferSize);
                decompressedBuffer = decompressionBuffer;
            } catch (LZ4Exception e) {
                throw new IOException(e);
            }
        } else {
            decompressedBuffer = in.slice();
            decompressedBuffer.limit(blockSize);
        }

        // verify checksum
        if (flg.isBlockChecksumSet()) {
            int hash = CHECKSUM.hash(in, in.position(), blockSize, 0);
            in.position(in.position() + blockSize);
            if (hash != in.getInt()) {
                throw new IOException(BLOCK_HASH_MISMATCH);
            }
        } else {
            in.position(in.position() + blockSize);
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
        bufferSupplier.release(decompressionBuffer);
    }

    @Override
    public void mark(int readlimit) {
        throw new RuntimeException("mark not supported");
    }

    @Override
    public void reset() {
        throw new RuntimeException("reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
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
            throw new RuntimeException("Kafka has detected detected a buggy lz4-java library (< 1.4.x) on the classpath."
                                       + " If you are using Kafka client libraries, make sure your application does not"
                                       + " accidentally override the version provided by Kafka or include multiple versions"
                                       + " of the library on the classpath. The lz4-java version on the classpath should"
                                       + " match the version the Kafka client libraries depend on. Adding -verbose:class"
                                       + " to your JVM arguments may help understand which lz4-java version is getting loaded.", e);
        }
    }
}
