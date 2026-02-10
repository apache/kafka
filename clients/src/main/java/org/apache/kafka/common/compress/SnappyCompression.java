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

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class SnappyCompression implements Compression {

    private SnappyCompression() {}

    @Override
    public CompressionType type() {
        return CompressionType.SNAPPY;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        try {
            return new SnappyOutputStream(bufferStream);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        // SnappyInputStream uses default implementation of InputStream for skip. Default implementation of
        // SnappyInputStream allocates a new skip buffer every time, hence, we prefer our own implementation.
        try {
            return new ChunkedBytesStream(new SnappyInputStream(new ByteBufferInputStream(buffer)),
                                          decompressionBufferSupplier,
                                          decompressionOutputSize(),
                                          false);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public int decompressionOutputSize() {
        // SnappyInputStream already uses an intermediate buffer internally. The size
        // of this buffer is based on legacy implementation based on skipArray introduced in
        // https://github.com/apache/kafka/pull/6785
        return 2 * 1024; // 2KB
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SnappyCompression;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public static class Builder implements Compression.Builder<SnappyCompression> {

        @Override
        public SnappyCompression build() {
            return new SnappyCompression();
        }
    }

}
