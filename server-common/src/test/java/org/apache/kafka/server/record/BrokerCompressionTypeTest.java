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
import org.apache.kafka.common.record.internal.CompressionType;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BrokerCompressionTypeTest {

    @Test
    public void testTargetCompressionType() {
        GzipCompression gzipWithLevel = Compression.gzip().level(CompressionType.GZIP.maxLevel()).build();
        assertEquals(gzipWithLevel, BrokerCompressionType.targetCompression(Optional.of(gzipWithLevel), CompressionType.ZSTD));
        SnappyCompression snappy = Compression.snappy().build();
        assertEquals(snappy, BrokerCompressionType.targetCompression(Optional.of(snappy), CompressionType.LZ4));
        Lz4Compression lz4WithLevel = Compression.lz4().level(CompressionType.LZ4.maxLevel()).build();
        assertEquals(lz4WithLevel, BrokerCompressionType.targetCompression(Optional.of(lz4WithLevel), CompressionType.ZSTD));
        ZstdCompression zstdWithLevel = Compression.zstd().level(CompressionType.ZSTD.maxLevel()).build();
        assertEquals(zstdWithLevel, BrokerCompressionType.targetCompression(Optional.of(zstdWithLevel), CompressionType.GZIP));

        GzipCompression gzip = Compression.gzip().build();
        assertEquals(gzip, BrokerCompressionType.targetCompression(Optional.empty(), CompressionType.GZIP));
        assertEquals(snappy, BrokerCompressionType.targetCompression(Optional.empty(), CompressionType.SNAPPY));
        Lz4Compression lz4 = Compression.lz4().build();
        assertEquals(lz4, BrokerCompressionType.targetCompression(Optional.empty(), CompressionType.LZ4));
        ZstdCompression zstd = Compression.zstd().build();
        assertEquals(zstd, BrokerCompressionType.targetCompression(Optional.empty(), CompressionType.ZSTD));
    }

}
