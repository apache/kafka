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

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.CompressionConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public final class ZstdConfig extends CompressionConfig {
    /**
     * Zstd supports compression levels from 1 up to ZSTD_maxCLevel(), which is currently 22. 0 is reserved for
     * the default level that is controlled by the library (currently 3), and it also supports negative levels
     * as an experimental feature; It is why MIN_COMPRESSION_LEVEL is not defined here.
     *
     * For details, please refer the official zstd manual: http://facebook.github.io/zstd/zstd_manual.html
     */
    static final int MAX_COMPRESSION_LEVEL = Zstd.maxCompressionLevel();
    static final int DEFAULT_COMPRESSION_LEVEL = Zstd.defaultCompressionLevel();

    private final int level;

    private ZstdConfig(int level) {
        this.level = level;
    }

    public CompressionType type() {
        return CompressionType.ZSTD;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        try {
            // Set input buffer (uncompressed) to 16 KB (none by default) to ensure reasonable performance
            // in cases where the caller passes a small number of bytes to write (potentially a single byte).
            return new BufferedOutputStream(new ZstdOutputStreamNoFinalizer(bufferStream, RecyclingBufferPool.INSTANCE, this.level), 16 * 1024);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            // We use our own BufferSupplier instead of com.github.luben.zstd.RecyclingBufferPool since our
            // implementation doesn't require locking or soft references.
            BufferPool bufferPool = new BufferPool() {
                @Override
                public ByteBuffer get(int capacity) {
                    return decompressionBufferSupplier.get(capacity);
                }

                @Override
                public void release(ByteBuffer buffer) {
                    decompressionBufferSupplier.release(buffer);
                }
            };

            // Set output buffer (uncompressed) to 16 KB (none by default) to ensure reasonable performance
            // in cases where the caller reads a small number of bytes (potentially a single byte).
            return new BufferedInputStream(new ZstdInputStreamNoFinalizer(new ByteBufferInputStream(buffer),
                bufferPool), 16 * 1024);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public static class Builder extends CompressionConfig.Builder<ZstdConfig> {
        private int level = DEFAULT_COMPRESSION_LEVEL;

        public Builder level(int level) {
            if (MAX_COMPRESSION_LEVEL < level) {
                throw new IllegalArgumentException("zstd doesn't support given compression level: " + level);
            }

            this.level = level;
            return this;
        }

        @Override
        public ZstdConfig build() {
            return new ZstdConfig(this.level);
        }
    }
}
