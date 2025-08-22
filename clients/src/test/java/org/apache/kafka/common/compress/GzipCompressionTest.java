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

import static org.apache.kafka.common.record.CompressionType.GZIP;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GzipCompressionTest {

    @Test
    public void testCompressionDecompression() throws IOException {
        byte[] data = String.join("", Collections.nCopies(256, "data"))
            .getBytes(StandardCharsets.UTF_8);

        List<Byte> magics = Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2);
        List<Integer> levels = Arrays.asList(GZIP.minLevel(), GZIP.defaultLevel(), GZIP.maxLevel());
        List<Integer> buffers = Arrays.asList(512, 4096, 8192, 32768);
        List<Integer> strategies = Arrays.asList(0, 1, 2);

        for (byte magic : magics) {
            for (int level : levels) {
                for (int buffer : buffers) {
                    for (int strategy : strategies) {
                        GzipCompression compression = Compression.gzip()
                            .level(level)
                            .bufferSize(buffer)
                            .strategy(strategy)
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
                            String ctx = String.format("magic=%d level=%d buffer=%d strategy=%d",
                                magic, level, buffer, strategy);
                            assertEquals(data.length, read, "bytes read mismatch: " + ctx);
                            assertArrayEquals(data, result, "data mismatch: " + ctx);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCompressionLevels() {
        GzipCompression.Builder builder = Compression.gzip();

        assertThrows(IllegalArgumentException.class, () -> builder.level(GZIP.minLevel() - 1));
        assertThrows(IllegalArgumentException.class, () -> builder.level(GZIP.maxLevel() + 1));

        builder.level(GZIP.minLevel());
        builder.level(GZIP.maxLevel());
    }

    @Test
    public void testGzipValidators() {
        // level validator
        ConfigDef.Validator levelV = GZIP.levelValidator();
        for (int lvl = GZIP.minLevel(); lvl <= GZIP.maxLevel(); lvl++) {
            levelV.ensureValid("level", lvl);
        }
        levelV.ensureValid("level", GZIP.defaultLevel());
        assertThrows(ConfigException.class, () -> levelV.ensureValid("level", GZIP.minLevel() - 1));
        assertThrows(ConfigException.class, () -> levelV.ensureValid("level", GZIP.maxLevel() + 1));

        // buffer size validator (>= 512)
        ConfigDef.Validator bufV = GZIP.bufferValidator();
        bufV.ensureValid("buffer", 512);
        bufV.ensureValid("buffer", 513);
        bufV.ensureValid("buffer", 8192);
        assertThrows(ConfigException.class, () -> bufV.ensureValid("buffer", -1));

        // strategy validator (0..2)
        ConfigDef.Validator stratV = GZIP.strategyValidator();
        stratV.ensureValid("strategy", 0);
        stratV.ensureValid("strategy", 1);
        stratV.ensureValid("strategy", 2);
        assertThrows(ConfigException.class, () -> stratV.ensureValid("strategy", -1));
        assertThrows(ConfigException.class, () -> stratV.ensureValid("strategy", 3));
    }

    @Test
    public void testBuilderOptions() throws Exception {
        GzipCompression compression = new GzipCompression.Builder()
            .bufferSize(8192)
            .strategy(java.util.zip.Deflater.FILTERED) // == 1
            .build();

        ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(32);
        byte[] payload = new byte[] {1, 2, 3};

        try (OutputStream out = compression.wrapForOutput(bufferStream, RecordBatch.MAGIC_VALUE_V2)) {
            out.write(payload);
        }

        bufferStream.buffer().flip();
        try (InputStream in = compression.wrapForInput(
            bufferStream.buffer(), RecordBatch.MAGIC_VALUE_V2, BufferSupplier.create())) {
            byte[] result = new byte[payload.length];
            int read = in.read(result);
            assertEquals(payload.length, read, "bytes read mismatch");
            assertArrayEquals(payload, result, "payload mismatch");
        }
    }

    @Test
    public void testCompressionOptionsBounds() {
        GzipCompression.Builder builder = Compression.gzip();

        // level bounds
        assertThrows(IllegalArgumentException.class, () -> builder.level(GZIP.minLevel() - 1));
        assertThrows(IllegalArgumentException.class, () -> builder.level(GZIP.maxLevel() + 1));
        builder.level(GZIP.minLevel());
        builder.level(GZIP.maxLevel());

        // bufferSize bounds (>= 512;)
        assertThrows(IllegalArgumentException.class, () -> builder.bufferSize(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.bufferSize(511));
        builder.bufferSize(512);
        builder.bufferSize(8192);

        // strategy bounds (valid: 0..2)
        assertThrows(IllegalArgumentException.class, () -> builder.strategy(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.strategy(3));
        builder.strategy(0);
        builder.strategy(1);
        builder.strategy(2);
    }
}
