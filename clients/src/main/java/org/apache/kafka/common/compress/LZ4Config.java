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
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V0;

public final class LZ4Config extends CompressionConfig {
    public static final int MIN_BLOCK_SIZE = KafkaLZ4BlockOutputStream.BLOCKSIZE_64KB;
    public static final int MAX_BLOCK_SIZE = KafkaLZ4BlockOutputStream.BLOCKSIZE_4MB;
    public static final int DEFAULT_BLOCK_SIZE = MIN_BLOCK_SIZE;

    private final int blockSize;

    private LZ4Config(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public CompressionType getType() {
        return CompressionType.LZ4;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
        try {
            return new KafkaLZ4BlockOutputStream(buffer, this.blockSize, false, messageVersion == MAGIC_VALUE_V0);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer inputBuffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            return new KafkaLZ4BlockInputStream(inputBuffer, decompressionBufferSupplier,
                messageVersion == MAGIC_VALUE_V0);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public static class Builder extends CompressionConfig.Builder<LZ4Config> {
        private int blockSize = DEFAULT_BLOCK_SIZE;

        public LZ4Config.Builder setBlockSize(int blockSize) {
            if (blockSize < MIN_BLOCK_SIZE || MAX_BLOCK_SIZE < blockSize) {
                throw new IllegalArgumentException("lz4 doesn't support given block size: " + blockSize);
            }

            this.blockSize = blockSize;
            return this;
        }

        @Override
        public LZ4Config build() {
            return new LZ4Config(this.blockSize);
        }
    }
}
