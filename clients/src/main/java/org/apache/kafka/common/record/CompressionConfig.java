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

import org.apache.kafka.common.compress.GzipConfig;
import org.apache.kafka.common.compress.LZ4Config;
import org.apache.kafka.common.compress.NoneConfig;
import org.apache.kafka.common.compress.SnappyConfig;
import org.apache.kafka.common.compress.ZstdConfig;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class CompressionConfig {

    public final static NoneConfig NONE = none().build();

    // Instantiating this class directly is disallowed; use builder methods.
    protected CompressionConfig() {}

    public abstract CompressionType type();

    /**
     * Wrap bufferStream with an OutputStream that will compress data with this CompressionType.
     *
     * Note: Unlike {@link #wrapForInput}, {@link #wrapForOutput} cannot take {@link ByteBuffer}s directly.
     * Currently, {@link MemoryRecordsBuilder#writeDefaultBatchHeader()} and {@link MemoryRecordsBuilder#writeLegacyCompressedWrapperHeader()}
     * write to the underlying buffer in the given {@link ByteBufferOutputStream} after the compressed data has been written.
     * In the event that the buffer needs to be expanded while writing the data, access to the underlying buffer needs to be preserved.
     */
    public abstract OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion);

    /**
     * Wrap buffer with an InputStream that will decompress data with this CompressionType.
     *
     * @param decompressionBufferSupplier The supplier of ByteBuffer(s) used for decompression if supported.
     *                                    For small record batches, allocating a potentially large buffer (64 KB for LZ4)
     *                                    will dominate the cost of decompressing and iterating over the records in the
     *                                    batch. As such, a supplier that reuses buffers will have a significant
     *                                    performance impact.
     */
    public abstract InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier);

    public static abstract class Builder<T extends CompressionConfig> {
        public abstract T build();
    }

    public static Builder<? extends CompressionConfig> of(final CompressionType compressionType) {
        switch (compressionType) {
            case NONE:
                return none();
            case GZIP:
                return gzip();
            case SNAPPY:
                return snappy();
            case LZ4:
                return lz4();
            case ZSTD:
                return zstd();
            default:
                throw new IllegalArgumentException("Unknown compression type: " + compressionType.name);
        }
    }

    public static NoneConfig.Builder none() {
        return new NoneConfig.Builder();
    }

    public static GzipConfig.Builder gzip() {
        return new GzipConfig.Builder();
    }

    public static SnappyConfig.Builder snappy() {
        return new SnappyConfig.Builder();
    }

    public static LZ4Config.Builder lz4() {
        return new LZ4Config.Builder();
    }

    public static ZstdConfig.Builder zstd() {
        return new ZstdConfig.Builder();
    }
}
