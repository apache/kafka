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
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ChunkedBytesStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.kafka.common.record.internal.CompressionType.LZ4;

public class Lz4Compression implements Compression {

    private final int level;

    private Lz4Compression(int level) {
        this.level = level;
    }

    @Override
    public CompressionType type() {
        return LZ4;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
        try {
            return new Lz4BlockOutputStream(buffer, level, messageVersion == RecordBatch.MAGIC_VALUE_V0);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer inputBuffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            return new ChunkedBytesStream(
                    new Lz4BlockInputStream(inputBuffer, decompressionBufferSupplier, messageVersion == RecordBatch.MAGIC_VALUE_V0),
                    decompressionBufferSupplier, decompressionOutputSize(), true);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public int decompressionOutputSize() {
        // Lz4BlockInputStream uses an internal intermediate buffer to store decompressed data. The size
        // of this buffer is based on legacy implementation based on skipArray introduced in
        // https://github.com/apache/kafka/pull/6785
        return 2 * 1024; // 2KB
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lz4Compression that = (Lz4Compression) o;
        return level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level);
    }

    public static class Builder implements Compression.Builder<Lz4Compression> {
        private int level = LZ4.defaultLevel();

        public Builder level(int level) {
            if (level < LZ4.minLevel() || LZ4.maxLevel() < level) {
                throw new IllegalArgumentException("lz4 doesn't support given compression level: " + level);
            }

            this.level = level;
            return this;
        }

        @Override
        public Lz4Compression build() {
            return new Lz4Compression(level);
        }
    }
}
