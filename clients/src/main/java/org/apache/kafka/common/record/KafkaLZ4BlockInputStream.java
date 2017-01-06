/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK;
import static org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.LZ4_MAX_HEADER_LENGTH;
import static org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.MAGIC;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.BD;
import org.apache.kafka.common.record.KafkaLZ4BlockOutputStream.FLG;
import org.apache.kafka.common.utils.Utils;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * A partial implementation of the v1.5.1 LZ4 Frame format.
 *
 * @see <a href="http://cyan4973.github.io/lz4/lz4_Frame_format.html">LZ4 Frame Format</a>
 */
public final class KafkaLZ4BlockInputStream extends FilterInputStream {

    public static final String PREMATURE_EOS = "Stream ended prematurely";
    public static final String NOT_SUPPORTED = "Stream unsupported (invalid magic bytes)";
    public static final String BLOCK_HASH_MISMATCH = "Block checksum mismatch";
    public static final String DESCRIPTOR_HASH_MISMATCH = "Stream frame descriptor corrupted";

    private final LZ4SafeDecompressor decompressor;
    private final XXHash32 checksum;
    private final byte[] buffer;
    private final byte[] compressedBuffer;
    private final int maxBlockSize;
    private final boolean ignoreFlagDescriptorChecksum;
    private FLG flg;
    private BD bd;
    private int bufferOffset;
    private int bufferSize;
    private boolean finished;

    /**
     * Create a new {@link InputStream} that will decompress data using the LZ4 algorithm.
     *
     * @param in The stream to decompress
     * @param ignoreFlagDescriptorChecksum for compatibility with old kafka clients, ignore incorrect HC byte
     * @throws IOException
     */
    public KafkaLZ4BlockInputStream(InputStream in, boolean ignoreFlagDescriptorChecksum) throws IOException {
        super(in);
        decompressor = LZ4Factory.fastestInstance().safeDecompressor();
        checksum = XXHashFactory.fastestInstance().hash32();
        this.ignoreFlagDescriptorChecksum = ignoreFlagDescriptorChecksum;
        readHeader();
        maxBlockSize = bd.getBlockMaximumSize();
        buffer = new byte[maxBlockSize];
        compressedBuffer = new byte[maxBlockSize];
        bufferOffset = 0;
        bufferSize = 0;
        finished = false;
    }

    /**
     * Create a new {@link InputStream} that will decompress data using the LZ4 algorithm.
     *
     * @param in The stream to decompress
     * @throws IOException
     */
    public KafkaLZ4BlockInputStream(InputStream in) throws IOException {
        this(in, false);
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
     * Reads the magic number and frame descriptor from the underlying {@link InputStream}.
     *
     * @throws IOException
     */
    private void readHeader() throws IOException {
        byte[] header = new byte[LZ4_MAX_HEADER_LENGTH];

        // read first 6 bytes into buffer to check magic and FLG/BD descriptor flags
        int headerOffset = 6;
        if (in.read(header, 0, headerOffset) != headerOffset) {
            throw new IOException(PREMATURE_EOS);
        }

        if (MAGIC != Utils.readUnsignedIntLE(header, headerOffset - 6)) {
            throw new IOException(NOT_SUPPORTED);
        }
        flg = FLG.fromByte(header[headerOffset - 2]);
        bd = BD.fromByte(header[headerOffset - 1]);

        if (flg.isContentSizeSet()) {
            if (in.read(header, headerOffset, 8) != 8)
                throw new IOException(PREMATURE_EOS);
            headerOffset += 8;
        }

        // Final byte of Frame Descriptor is HC checksum
        header[headerOffset++] = (byte) in.read();

        // Old implementations produced incorrect HC checksums
        if (ignoreFlagDescriptorChecksum)
            return;

        int offset = 4;
        int len = headerOffset - offset - 1; // dont include magic bytes or HC
        byte hash = (byte) ((checksum.hash(header, offset, len, 0) >> 8) & 0xFF);
        if (hash != header[headerOffset - 1])
            throw new IOException(DESCRIPTOR_HASH_MISMATCH);
    }

    /**
     * Decompresses (if necessary) buffered data, optionally computes and validates a XXHash32 checksum, and writes the
     * result to a buffer.
     *
     * @throws IOException
     */
    private void readBlock() throws IOException {
        int blockSize = Utils.readUnsignedIntLE(in);

        // Check for EndMark
        if (blockSize == 0) {
            finished = true;
            if (flg.isContentChecksumSet())
                Utils.readUnsignedIntLE(in); // TODO: verify this content checksum
            return;
        } else if (blockSize > maxBlockSize) {
            throw new IOException(String.format("Block size %s exceeded max: %s", blockSize, maxBlockSize));
        }

        boolean compressed = (blockSize & LZ4_FRAME_INCOMPRESSIBLE_MASK) == 0;
        byte[] bufferToRead;
        if (compressed) {
            bufferToRead = compressedBuffer;
        } else {
            blockSize &= ~LZ4_FRAME_INCOMPRESSIBLE_MASK;
            bufferToRead = buffer;
            bufferSize = blockSize;
        }

        if (in.read(bufferToRead, 0, blockSize) != blockSize) {
            throw new IOException(PREMATURE_EOS);
        }

        // verify checksum
        if (flg.isBlockChecksumSet() && Utils.readUnsignedIntLE(in) != checksum.hash(bufferToRead, 0, blockSize, 0)) {
            throw new IOException(BLOCK_HASH_MISMATCH);
        }

        if (compressed) {
            try {
                bufferSize = decompressor.decompress(compressedBuffer, 0, blockSize, buffer, 0, maxBlockSize);
            } catch (LZ4Exception e) {
                throw new IOException(e);
            }
        }

        bufferOffset = 0;
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

        return buffer[bufferOffset++] & 0xFF;
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
        System.arraycopy(buffer, bufferOffset, b, off, len);
        bufferOffset += len;
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
        n = Math.min(n, available());
        bufferOffset += n;
        return n;
    }

    @Override
    public int available() throws IOException {
        return bufferSize - bufferOffset;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new RuntimeException("mark not supported");
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new RuntimeException("reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

}
