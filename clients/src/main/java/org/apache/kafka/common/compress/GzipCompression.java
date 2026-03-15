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

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

import static org.apache.kafka.common.record.internal.CompressionType.GZIP;

public class GzipCompression implements Compression {

    private final int level;

    private GzipCompression(int level) {
        this.level = level;
    }

    @Override
    public CompressionType type() {
        return GZIP;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
        try {
            // Set input buffer (uncompressed) to 16 KB (none by default) and output buffer (compressed) to
            // 8 KB (0.5 KB by default) to ensure reasonable performance in cases where the caller passes a small
            // number of bytes to write (potentially a single byte)
            return new BufferedOutputStream(new GzipOutputStream(buffer, 8 * 1024, level), 16 * 1024);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            // Set input buffer (compressed) to 8 KB (GZIPInputStream uses 0.5 KB by default) to ensure reasonable
            // performance in cases where the caller reads a small number of bytes (potentially a single byte).
            //
            // Size of output buffer (uncompressed) is provided by decompressionOutputSize.
            //
            // ChunkedBytesStream is used to wrap the GZIPInputStream because the default implementation of
            // GZIPInputStream does not use an intermediate buffer for decompression in chunks.
            return new ChunkedBytesStream(new GZIPInputStream(new ByteBufferInputStream(buffer), 8 * 1024),
                                          decompressionBufferSupplier,
                                          decompressionOutputSize(),
                                          false);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public int decompressionOutputSize() {
        // 16KB has been chosen based on legacy implementation introduced in https://github.com/apache/kafka/pull/6785
        return 16 * 1024;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GzipCompression that = (GzipCompression) o;
        return level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level);
    }

    public static class Builder implements Compression.Builder<GzipCompression> {
        private int level = GZIP.defaultLevel();

        public Builder level(int level) {
            if ((level < GZIP.minLevel() || GZIP.maxLevel() < level) && level != GZIP.defaultLevel()) {
                throw new IllegalArgumentException("gzip doesn't support given compression level: " + level);
            }

            this.level = level;
            return this;
        }

        @Override
        public GzipCompression build() {
            return new GzipCompression(level);
        }
    }
}
