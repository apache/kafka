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
package org.apache.kafka.server.record;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.compress.GzipCompression;
import org.apache.kafka.common.compress.Lz4Compression;
import org.apache.kafka.common.compress.SnappyCompression;
import org.apache.kafka.common.compress.ZstdCompression;
import org.apache.kafka.common.record.CompressionType;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.zip.Deflater;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BrokerCompressionTypeTest {

    @Test
    public void testTargetCompressionType() {
        GzipCompression gzipWithOpts = Compression.gzip()
            .level(CompressionType.GZIP.maxLevel())
            .bufferSize(8192)
            .strategy(Deflater.HUFFMAN_ONLY)
            .build();

        assertEquals(gzipWithOpts,
            BrokerCompressionType.targetCompression(Optional.of(gzipWithOpts), CompressionType.ZSTD),
            "Producer gzip with options should be preserved");

        SnappyCompression snappyWithOpts = Compression.snappy()
            .blockSize(CompressionType.SNAPPY.maxBlockSize())
            .build();
        assertEquals(snappyWithOpts,
            BrokerCompressionType.targetCompression(Optional.of(snappyWithOpts), CompressionType.LZ4),
            "Producer snappy with options should be preserved");

        Lz4Compression lz4WithOpts = Compression.lz4()
            .level(CompressionType.LZ4.maxLevel())
            .blockSize(CompressionType.LZ4.maxBlockSize())
            .build();
        assertEquals(lz4WithOpts,
            BrokerCompressionType.targetCompression(Optional.of(lz4WithOpts), CompressionType.ZSTD),
            "Producer lz4 with options should be preserved");

        ZstdCompression zstdWithOpts = Compression.zstd()
            .level(CompressionType.ZSTD.maxLevel())
            .windowSize(CompressionType.ZSTD.maxWindowSize())
            .workers(2)
            .build();

        assertEquals(zstdWithOpts,
            BrokerCompressionType.targetCompression(Optional.of(zstdWithOpts), CompressionType.GZIP),
            "Producer zstd with options should be preserved");

        // --- When producer doesn't specify, fall back to broker-selected type with codec defaults ---
        GzipCompression gzipDefault = Compression.gzip().build();
        assertEquals(gzipDefault,
            BrokerCompressionType.targetCompression(Optional.empty(), CompressionType.GZIP),
            "Fallback to gzip defaults");

        SnappyCompression snappyDefault = Compression.snappy().build();
        assertEquals(snappyDefault,
            BrokerCompressionType.targetCompression(Optional.empty(), CompressionType.SNAPPY),
            "Fallback to snappy defaults");

        Lz4Compression lz4Default = Compression.lz4().build();
        assertEquals(lz4Default,
            BrokerCompressionType.targetCompression(Optional.empty(), CompressionType.LZ4),
            "Fallback to lz4 defaults");

        ZstdCompression zstdDefault = Compression.zstd().build();
        assertEquals(zstdDefault,
            BrokerCompressionType.targetCompression(Optional.empty(), CompressionType.ZSTD),
            "Fallback to zstd defaults");
    }

}
