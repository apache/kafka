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

import static org.apache.kafka.common.record.CompressionType.ZSTD;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ZstdCompressionTest {

    @Test
    public void testCompressionDecompression() throws IOException {
        byte[] data = String.join("", Collections.nCopies(256, "data"))
            .getBytes(StandardCharsets.UTF_8);

        List<Byte> magics = Arrays.asList(
            RecordBatch.MAGIC_VALUE_V0,
            RecordBatch.MAGIC_VALUE_V1,
            RecordBatch.MAGIC_VALUE_V2
        );

        List<Integer> levels = Arrays.asList(
            ZSTD.minLevel(),
            ZSTD.defaultLevel(),
            ZSTD.maxLevel()
        );

        List<Integer> windows = Arrays.asList(
            CompressionType.ZSTD.minWindowSize(),
            CompressionType.ZSTD.defaultWindowSize(),
            CompressionType.ZSTD.maxWindowSize()
        );

        List<Integer> workers = Arrays.asList(
            CompressionType.ZSTD.minWorkers(),
            CompressionType.ZSTD.defaultWorkers(),
            CompressionType.ZSTD.maxWorkers());

        for (byte magic : magics) {
            for (int level : levels) {
                for (int window : windows) {
                    for (int worker : workers) {
                        ZstdCompression compression = Compression.zstd()
                            .level(level)
                            .windowSize(window)
                            .workers(worker)
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
                            String ctx = String.format("magic=%d level=%d window=%d workers=%d",
                                magic, level, window, worker);
                            assertEquals(data.length, read, "bytes read mismatch: " + ctx);
                            assertArrayEquals(data, result, "data mismatch: " + ctx);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testZstdOptionBounds() {
        ZstdCompression.Builder builder = Compression.zstd();

        // level bounds
        assertThrows(IllegalArgumentException.class, () -> builder.level(ZSTD.minLevel() - 1));
        assertThrows(IllegalArgumentException.class, () -> builder.level(ZSTD.maxLevel() + 1));
        builder.level(ZSTD.minLevel());
        builder.level(ZSTD.defaultLevel());
        builder.level(ZSTD.maxLevel());

        // window bounds
        int minWin = CompressionType.ZSTD.minWindowSize();
        int maxWin = CompressionType.ZSTD.maxWindowSize();
        assertThrows(IllegalArgumentException.class, () -> builder.windowSize(minWin - 1));
        assertThrows(IllegalArgumentException.class, () -> builder.windowSize(maxWin + 1));
        builder.windowSize(minWin);
        builder.windowSize(CompressionType.ZSTD.defaultWindowSize());
        builder.windowSize(maxWin);

        // workers bounds
        int minW = CompressionType.ZSTD.minWorkers();
        int maxW = CompressionType.ZSTD.maxWorkers();
        assertThrows(IllegalArgumentException.class, () -> builder.workers(minW - 1));
        assertThrows(IllegalArgumentException.class, () -> builder.workers(maxW + 1));
        builder.workers(minW);
        builder.workers(CompressionType.ZSTD.defaultWorkers());
        builder.workers(maxW);
    }

    @Test
    public void testZstdValidators() {
        // level validator
        ConfigDef.Validator lvlV = ZSTD.levelValidator();
        for (int lvl = ZSTD.minLevel(); lvl <= ZSTD.maxLevel(); lvl++) {
            lvlV.ensureValid("zstd.level", lvl);
        }
        lvlV.ensureValid("zstd.level", ZSTD.defaultLevel());
        assertThrows(ConfigException.class, () -> lvlV.ensureValid("zstd.level", ZSTD.minLevel() - 1));
        assertThrows(ConfigException.class, () -> lvlV.ensureValid("zstd.level", ZSTD.maxLevel() + 1));

        // window validator
        ConfigDef.Validator winV = CompressionType.ZSTD.windowSizeValidator();
        winV.ensureValid("zstd.window", CompressionType.ZSTD.defaultWindowSize());
        winV.ensureValid("zstd.window", CompressionType.ZSTD.minWindowSize());
        winV.ensureValid("zstd.window", Math.min(
            CompressionType.ZSTD.maxWindowSize(),
            CompressionType.ZSTD.minWindowSize() + 1
        ));
        assertThrows(ConfigException.class, () -> winV.ensureValid("zstd.window", CompressionType.ZSTD.minWindowSize() - 1));
        assertThrows(ConfigException.class, () -> winV.ensureValid("zstd.window", CompressionType.ZSTD.maxWindowSize() + 1));

        // workers validator
        ConfigDef.Validator workersV = CompressionType.ZSTD.workersValidator();
        workersV.ensureValid("zstd.workers", CompressionType.ZSTD.defaultWorkers());
        workersV.ensureValid("zstd.workers", CompressionType.ZSTD.minWorkers());
        workersV.ensureValid("zstd.workers", CompressionType.ZSTD.maxWorkers());
        assertThrows(ConfigException.class, () -> workersV.ensureValid("zstd.workers", CompressionType.ZSTD.minWorkers() - 1));
        assertThrows(ConfigException.class, () -> workersV.ensureValid("zstd.workers", CompressionType.ZSTD.maxWorkers() + 1));
    }
}
