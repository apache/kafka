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
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

/**
 * The compression type to use
 */
public enum CompressionType {
    NONE(0, "none", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, Integer level, Integer blockSize) {
            return buffer;
        }

        @Override
        public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
            return new ByteBufferInputStream(buffer);
        }
    },

    GZIP(1, "gzip", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, Integer level, Integer blockSize) {
            try {
                // Set input buffer (uncompressed) to 16 KB (none by default) and output buffer (compressed) to
                // 8 KB (0.5 KB by default) to ensure reasonable performance in cases where the caller passes a small
                // number of bytes to write (potentially a single byte)
                return new BufferedOutputStream(GZipOutputStream.of(buffer, level, blockSize), 16 * 1024);
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

    SNAPPY(2, "snappy", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, Integer level, Integer blockSize) {
            try {
                // Snappy does not support compression level; so given parameter is ignored.
                if (blockSize == null) {
                    return (OutputStream) SnappyConstructors.OUTPUT_WITHOUT_BLOCK_SIZE.invoke(buffer);
                } else {
                    return (OutputStream) SnappyConstructors.OUTPUT_WITH_BLOCK_SIZE.invoke(buffer, blockSize);
                }
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
            try {
                return (InputStream) SnappyConstructors.INPUT.invoke(new ByteBufferInputStream(buffer));
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }
    },

    LZ4(3, "lz4", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, Integer level, Integer blockSize) {
            try {
                return KafkaLZ4BlockOutputStream.of(buffer, messageVersion, level, blockSize);
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
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, Integer level, Integer blockSize) {
            try {
                // Zstd does not support block size configuration; so given parameter is ignored.
                if (level == null) {
                    return (OutputStream) ZstdConstructors.OUTPUT_WITHOUT_LEVEL.invoke(buffer);
                } else {
                    return (OutputStream) ZstdConstructors.OUTPUT_WITH_LEVEL.invoke(buffer, level);
                }
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
            try {
                return (InputStream) ZstdConstructors.INPUT.invoke(new ByteBufferInputStream(buffer));
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
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
     * Wrap bufferStream with an OutputStream that will compress data with this CompressionType with given buffer size and compression level.
     * <p>
     * Note: Unlike {@link #wrapForInput}, {@link #wrapForOutput} cannot take {@link ByteBuffer}s directly.
     * Currently, {@link MemoryRecordsBuilder#writeDefaultBatchHeader()} and {@link MemoryRecordsBuilder#writeLegacyCompressedWrapperHeader()}
     * write to the underlying buffer in the given {@link ByteBufferOutputStream} after the compressed data has been written.
     * In the event that the buffer needs to be expanded while writing the data, access to the underlying buffer needs to be preserved.
     *
     * @param level     The compression level to use. If null, it falls back to the default level.
     * @param blockSize The buffer size to use during compression. If null, it falls back to the default block size.
     */
    public abstract OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion, Integer level, Integer blockSize);

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

    // We should only have a runtime dependency on compression algorithms in case the native libraries don't support
    // some platforms.
    //
    // For Snappy and Zstd, we dynamically load the classes and rely on the initialization-on-demand holder idiom to ensure
    // they're only loaded if used.
    //
    // For LZ4 we are using org.apache.kafka classes, which should always be in the classpath, and would not trigger
    // an error until KafkaLZ4BlockInputStream is initialized, which only happens if LZ4 is actually used.

    private static class SnappyConstructors {
        static final MethodHandle INPUT = findConstructor("org.xerial.snappy.SnappyInputStream",
                MethodType.methodType(void.class, InputStream.class));
        static final MethodHandle OUTPUT_WITHOUT_BLOCK_SIZE = findConstructor("org.xerial.snappy.SnappyOutputStream",
                MethodType.methodType(void.class, OutputStream.class));
        static final MethodHandle OUTPUT_WITH_BLOCK_SIZE = findConstructor("org.xerial.snappy.SnappyOutputStream",
                MethodType.methodType(void.class, OutputStream.class, int.class));
    }

    private static class ZstdConstructors {
        static final MethodHandle INPUT = findConstructor("com.github.luben.zstd.ZstdInputStream",
                MethodType.methodType(void.class, InputStream.class));
        static final MethodHandle OUTPUT_WITHOUT_LEVEL = findConstructor("com.github.luben.zstd.ZstdOutputStream",
                MethodType.methodType(void.class, OutputStream.class));
        static final MethodHandle OUTPUT_WITH_LEVEL = findConstructor("com.github.luben.zstd.ZstdOutputStream",
                MethodType.methodType(void.class, OutputStream.class, int.class));
    }

    private static MethodHandle findConstructor(String className, MethodType methodType) {
        try {
            return MethodHandles.publicLookup().findConstructor(Class.forName(className), methodType);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

}
