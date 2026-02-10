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

import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface Compression {

    /**
     * The compression type for this compression codec
     */
    CompressionType type();

    /**
     * Wrap bufferStream with an OutputStream that will compress data with this Compression.
     *
     * @param bufferStream The buffer to write the compressed data to
     * @param messageVersion The record format version to use.
     * Note: Unlike {@link #wrapForInput}, this cannot take {@link ByteBuffer}s directly.
     * Currently, MemoryRecordsBuilder writes to the underlying buffer in the given {@link ByteBufferOutputStream} after the compressed data has been written.
     * In the event that the buffer needs to be expanded while writing the data, access to the underlying buffer needs to be preserved.
     */
    OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion);

    /**
     * Wrap buffer with an InputStream that will decompress data with this Compression.
     *
     * @param buffer The {@link ByteBuffer} instance holding the data to decompress.
     * @param messageVersion The record format version to use.
     * @param decompressionBufferSupplier The supplier of ByteBuffer(s) used for decompression if supported.
     * For small record batches, allocating a potentially large buffer (64 KB for LZ4)
     * will dominate the cost of decompressing and iterating over the records in the
     * batch. As such, a supplier that reuses buffers will have a significant
     * performance impact.
     */
    InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier);

    /**
     * Recommended size of buffer for storing decompressed output.
     */
    default int decompressionOutputSize() {
        throw new UnsupportedOperationException("Size of decompression buffer is not defined for this compression type=" + type().name);
    }

    interface Builder<T extends Compression> {
        T build();
    }

    static Builder<? extends Compression> of(final String compressionName) {
        CompressionType compressionType = CompressionType.forName(compressionName);
        return of(compressionType);
    }

    static Builder<? extends Compression> of(final CompressionType compressionType) {
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

    NoCompression NONE = none().build();

    static NoCompression.Builder none() {
        return new NoCompression.Builder();
    }

    static GzipCompression.Builder gzip() {
        return new GzipCompression.Builder();
    }

    static SnappyCompression.Builder snappy() {
        return new SnappyCompression.Builder();
    }

    static Lz4Compression.Builder lz4() {
        return new Lz4Compression.Builder();
    }

    static ZstdCompression.Builder zstd() {
        return new ZstdCompression.Builder();
    }
}
