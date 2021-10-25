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
import org.apache.kafka.common.record.CompressionConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public final class SnappyConfig extends CompressionConfig {
    public static final int MIN_BLOCK_SIZE = 1024;
    public static final int DEFAULT_BLOCK_SIZE = 32 * 1024;

    private final int blockSize;

    private SnappyConfig(int blockSize) {
        this.blockSize = blockSize;
    }

    public CompressionType getType() {
        return CompressionType.SNAPPY;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        try {
            return new SnappyOutputStream(bufferStream, this.blockSize);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            return new SnappyInputStream(new ByteBufferInputStream(buffer));
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public static class Builder extends CompressionConfig.Builder<SnappyConfig> {
        private int blockSize = DEFAULT_BLOCK_SIZE;

        public Builder setBlockSize(int blockSize) {
            if (blockSize < MIN_BLOCK_SIZE) {
                throw new IllegalArgumentException("snappy doesn't support given block size: " + blockSize);
            }

            this.blockSize = blockSize;
            return this;
        }

        @Override
        public SnappyConfig build() {
            return new SnappyConfig(this.blockSize);
        }
    }
}
