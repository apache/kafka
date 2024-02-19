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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BrokerCompressionTypeTest {

    @Test
    public void testTargetCompressionType() {
        GzipCompression gzipWithLevel = new GzipCompression.Builder().level(GzipCompression.MAX_LEVEL).build();
        assertEquals(gzipWithLevel, BrokerCompressionType.GZIP.targetCompression(Optional.of(gzipWithLevel), CompressionType.ZSTD));
        SnappyCompression snappy = Compression.snappy().build();
        assertEquals(snappy, BrokerCompressionType.SNAPPY.targetCompression(Optional.of(snappy), CompressionType.LZ4));
        Lz4Compression lz4WithLevel = new Lz4Compression.Builder().level(Lz4Compression.MAX_LEVEL).build();
        assertEquals(lz4WithLevel, BrokerCompressionType.LZ4.targetCompression(Optional.of(lz4WithLevel), CompressionType.ZSTD));
        ZstdCompression zstdWithLevel = new ZstdCompression.Builder().level(ZstdCompression.MAX_LEVEL).build();
        assertEquals(zstdWithLevel, BrokerCompressionType.ZSTD.targetCompression(Optional.of(zstdWithLevel), CompressionType.GZIP));

        GzipCompression gzip = new GzipCompression.Builder().build();
        assertEquals(gzip, BrokerCompressionType.PRODUCER.targetCompression(Optional.empty(), CompressionType.GZIP));
        assertEquals(snappy, BrokerCompressionType.PRODUCER.targetCompression(Optional.empty(), CompressionType.SNAPPY));
        Lz4Compression lz4 = new Lz4Compression.Builder().build();
        assertEquals(lz4, BrokerCompressionType.PRODUCER.targetCompression(Optional.empty(), CompressionType.LZ4));
        ZstdCompression zstd = new ZstdCompression.Builder().build();
        assertEquals(zstd, BrokerCompressionType.PRODUCER.targetCompression(Optional.empty(), CompressionType.ZSTD));
    }

}
