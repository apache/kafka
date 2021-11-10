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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.compress.KafkaLZ4BlockInputStream;
import org.apache.kafka.common.compress.KafkaLZ4BlockOutputStream;
import org.apache.kafka.common.compress.SnappyFactory;
import org.apache.kafka.common.compress.ZstdFactory;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The compression type to use
 */
public enum CompressionType {
    NONE(0, "none", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
            return buffer;
        }

        @Override
        public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
            return new ByteBufferInputStream(buffer);
        }
    },

    // Shipped with the JDK
    GZIP(1, "gzip", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
            try {
                // Set input buffer (uncompressed) to 16 KB (none by default) and output buffer (compressed) to
                // 8 KB (0.5 KB by default) to ensure reasonable performance in cases where the caller passes a small
                // number of bytes to write (potentially a single byte)
                return new BufferedOutputStream(new GZIPOutputStream(buffer, 8 * 1024), 16 * 1024);
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
            try {
                // Set output buffer (uncompressed) to 16 KB (none by default) and input buffer (compressed) to
                // 8 KB (0.5 KB by default) to ensure reasonable performance in cases where the caller reads a small
                // number of bytes (potentially a single byte)
                return new BufferedInputStream(new GZIPInputStream(new ByteBufferInputStream(buffer), 8 * 1024),
                        16 * 1024);
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        }
    },

    // We should only load classes from a given compression library when we actually use said compression library. This
    // is because compression libraries include native code for a set of platforms and we want to avoid errors
    // in case the platform is not supported and the compression library is not actually used.
    // To ensure this, we only reference compression library code from classes that are only invoked when actual usage
    // happens.

    SNAPPY(2, "snappy", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
            return SnappyFactory.wrapForOutput(buffer);
        }

        @Override
        public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
            return SnappyFactory.wrapForInput(buffer);
        }
    },

    LZ4(3, "lz4", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
            try {
                return new KafkaLZ4BlockOutputStream(buffer, messageVersion == RecordBatch.MAGIC_VALUE_V0);
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public InputStream wrapForInput(ByteBuffer inputBuffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
            try {
                return new KafkaLZ4BlockInputStream(inputBuffer, decompressionBufferSupplier,
                                                    messageVersion == RecordBatch.MAGIC_VALUE_V0);
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }
    },

    ZSTD(4, "zstd", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
            return ZstdFactory.wrapForOutput(buffer);
        }

        @Override
        public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
            return ZstdFactory.wrapForInput(buffer, messageVersion, decompressionBufferSupplier);
        }
    };

    public final int id;
    public final String name;
    public final float rate;

    CompressionType(int id, String name, float rate) {
        this.id = id;
        this.name = name;
        this.rate = rate;
    }

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

    public static CompressionType forId(int id) {
        switch (id) {
            case 0:
                return NONE;
            case 1:
                return GZIP;
            case 2:
                return SNAPPY;
            case 3:
                return LZ4;
            case 4:
                return ZSTD;
            default:
                throw new IllegalArgumentException("Unknown compression type id: " + id);
        }
    }

    public static CompressionType forName(String name) {
        if (NONE.name.equals(name))
            return NONE;
        else if (GZIP.name.equals(name))
            return GZIP;
        else if (SNAPPY.name.equals(name))
            return SNAPPY;
        else if (LZ4.name.equals(name))
            return LZ4;
        else if (ZSTD.name.equals(name))
            return ZSTD;
        else
            throw new IllegalArgumentException("Unknown compression name: " + name);
    }
}
