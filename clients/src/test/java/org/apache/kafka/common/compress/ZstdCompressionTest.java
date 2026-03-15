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

import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.common.record.internal.CompressionType.ZSTD;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ZstdCompressionTest {

    @Test
    public void testCompressionDecompression() throws IOException {
        ZstdCompression.Builder builder = Compression.zstd();
        byte[] data = String.join("", Collections.nCopies(256, "data")).getBytes(StandardCharsets.UTF_8);

        for (byte magic : Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)) {
            for (int level : Arrays.asList(ZSTD.minLevel(), ZSTD.defaultLevel(), ZSTD.maxLevel())) {
                ZstdCompression compression = builder.level(level).build();
                ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(4);
                try (OutputStream out = compression.wrapForOutput(bufferStream, magic)) {
                    out.write(data);
                    out.flush();
                }
                bufferStream.buffer().flip();

                try (InputStream inputStream = compression.wrapForInput(bufferStream.buffer(), magic, BufferSupplier.create())) {
                    byte[] result = new byte[data.length];
                    int read = inputStream.read(result);
                    assertEquals(data.length, read);
                    assertArrayEquals(data, result);
                }
            }
        }
    }

    @Test
    public void testCompressionLevels() {
        ZstdCompression.Builder builder = Compression.zstd();

        assertThrows(IllegalArgumentException.class, () -> builder.level(ZSTD.minLevel() - 1));
        assertThrows(IllegalArgumentException.class, () -> builder.level(ZSTD.maxLevel() + 1));

        builder.level(ZSTD.minLevel());
        builder.level(ZSTD.maxLevel());
    }
}
