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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ChunkedBytesStream;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.kafka.common.record.internal.CompressionType.ZSTD;

public class ZstdCompression implements Compression {

    private final int level;

    private ZstdCompression(int level) {
        this.level = level;
    }

    @Override
    public CompressionType type() {
        return ZSTD;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        try {
            // Set input buffer (uncompressed) to 16 KB (none by default) to ensure reasonable performance
            // in cases where the caller passes a small number of bytes to write (potentially a single byte).
            return new BufferedOutputStream(new ZstdOutputStreamNoFinalizer(bufferStream, RecyclingBufferPool.INSTANCE, level), 16 * 1024);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            return new ChunkedBytesStream(wrapForZstdInput(buffer, decompressionBufferSupplier),
                    decompressionBufferSupplier,
                    decompressionOutputSize(),
                    false);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    // visible for testing
    public static ZstdInputStreamNoFinalizer wrapForZstdInput(ByteBuffer buffer, BufferSupplier decompressionBufferSupplier) throws IOException {
        // We use our own BufferSupplier instead of com.github.luben.zstd.RecyclingBufferPool since our
        // implementation doesn't require locking or soft references. The buffer allocated by this buffer pool is
        // used by zstd-jni for 1\ reading compressed data from input stream into a buffer before passing it over JNI
        // 2\ implementation of skip inside zstd-jni where buffer is obtained and released with every call
        final BufferPool bufferPool = new BufferPool() {
            @Override
            public ByteBuffer get(int capacity) {
                return decompressionBufferSupplier.get(capacity);
            }

            @Override
            public void release(ByteBuffer buffer) {
                decompressionBufferSupplier.release(buffer);
            }
        };
        // Ideally, data from ZstdInputStreamNoFinalizer should be read in a bulk because every call to
        // `ZstdInputStreamNoFinalizer#read()` is a JNI call. The caller is expected to
        // balance the tradeoff between reading large amount of data vs. making multiple JNI calls.
        return new ZstdInputStreamNoFinalizer(new ByteBufferInputStream(buffer), bufferPool);
    }

    /**
     * Size of intermediate buffer which contains uncompressed data.
     * This size should be <= ZSTD_BLOCKSIZE_MAX
     * see: https://github.com/facebook/zstd/blob/189653a9c10c9f4224a5413a6d6a69dd01d7c3bd/lib/zstd.h#L854
     */
    @Override
    public int decompressionOutputSize() {
        // 16KB has been chosen based on legacy implementation introduced in https://github.com/apache/kafka/pull/6785
        return 16 * 1024;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZstdCompression that = (ZstdCompression) o;
        return level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level);
    }

    public static class Builder implements Compression.Builder<ZstdCompression> {
        private int level = ZSTD.defaultLevel();

        public Builder level(int level) {
            if (level < ZSTD.minLevel() || ZSTD.maxLevel() < level) {
                throw new IllegalArgumentException("zstd doesn't support given compression level: " + level);
            }

            this.level = level;
            return this;
        }

        @Override
        public ZstdCompression build() {
            return new ZstdCompression(level);
        }
    }
}
