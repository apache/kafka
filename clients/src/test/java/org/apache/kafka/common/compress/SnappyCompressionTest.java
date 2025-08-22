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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SnappyCompressionTest {

    @Test
    public void testCompressionDecompression() throws IOException {
        byte[] data = String.join("", Collections.nCopies(256, "data"))
            .getBytes(StandardCharsets.UTF_8);

        List<Byte> magics = Arrays.asList(
            RecordBatch.MAGIC_VALUE_V0,
            RecordBatch.MAGIC_VALUE_V1,
            RecordBatch.MAGIC_VALUE_V2
        );

        List<Integer> blocks = Arrays.asList(
            CompressionType.SNAPPY.minBlockSize(),
            CompressionType.SNAPPY.defaultBlockSize(),
            CompressionType.SNAPPY.maxBlockSize()
        );

        for (byte magic : magics) {
            for (int block : blocks) {
                SnappyCompression compression = Compression.snappy()
                    .blockSize(block)
                    .build();

                ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(4);
                try (OutputStream out = compression.wrapForOutput(bufferStream, magic)) {
                    out.write(data);
                    out.flush();
                }
                bufferStream.buffer().flip();

                try (InputStream in = compression.wrapForInput(
                    bufferStream.buffer(), magic, BufferSupplier.create())) {
                    byte[] result = new byte[data.length];
                    int read = in.read(result);
                    assertEquals(data.length, read);
                    assertArrayEquals(data, result);
                }
            }
        }
    }

    @Test
    public void testSnappyOptionBounds() {
        SnappyCompression.Builder builder = Compression.snappy();

        int min = CompressionType.SNAPPY.minBlockSize();
        int def = CompressionType.SNAPPY.defaultBlockSize();
        int max = CompressionType.SNAPPY.maxBlockSize();

        // blockSize bounds
        assertThrows(IllegalArgumentException.class, () -> builder.blockSize(min - 1));
        assertThrows(IllegalArgumentException.class, () -> builder.blockSize(max + 1));

        builder.blockSize(min);
        builder.blockSize(def);
        builder.blockSize(max);
    }

    @Test
    public void testSnappyBlockSizeValidator() {
        ConfigDef.Validator v = CompressionType.SNAPPY.blockSizeValidator();

        int min = CompressionType.SNAPPY.minBlockSize();
        int max = CompressionType.SNAPPY.maxBlockSize();

        for (int b = min; b <= max; b++) {
            v.ensureValid("snappy.block", b);
        }
        assertThrows(ConfigException.class, () -> v.ensureValid("snappy.block", min - 1));
        assertThrows(ConfigException.class, () -> v.ensureValid("snappy.block", max + 1));
    }
}
