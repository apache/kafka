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

package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BrokerCompressionTest {
    private final File tmpDir = TestUtils.tempDirectory();
    private final File logDir = TestUtils.randomPartitionLogDir(tmpDir);
    private final MockTime time = new MockTime(0, 0);

    @AfterEach
    public void tearDown() throws IOException {
        Utils.delete(tmpDir);
    }

    /**
     * Test broker-side compression configuration
     */
    @ParameterizedTest
    @MethodSource("allCompressionParameters")
    public void testBrokerSideCompression(CompressionType messageCompressionType, BrokerCompressionType brokerCompressionType) throws IOException {
        Compression messageCompression = Compression.of(messageCompressionType).build();

        /* Configure broker-side compression */
        try (UnifiedLog log = UnifiedLog.create(
            logDir,
            new LogConfig(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, brokerCompressionType.name)),
            0L,
            0L,
            time.scheduler,
            new BrokerTopicStats(),
            time,
            5 * 60 * 1000,
            new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
            TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
            new LogDirFailureChannel(10),
            true,
            Optional.empty()
        )) {
            /* Append two messages */
            log.appendAsLeader(
                    MemoryRecords.withRecords(messageCompression, 0,
                            new SimpleRecord("hello".getBytes()),
                            new SimpleRecord("there".getBytes())
                    ), 0
            );

            RecordBatch firstBatch = readFirstBatch(log);

            if (brokerCompressionType != BrokerCompressionType.PRODUCER) {
                Compression targetCompression = BrokerCompressionType.targetCompression(log.config().compression, null);
                assertEquals(targetCompression.type(), firstBatch.compressionType(), "Compression at offset 0 should produce " + brokerCompressionType);
            } else {
                assertEquals(messageCompressionType, firstBatch.compressionType(), "Compression at offset 0 should produce " + messageCompressionType);
            }
        }
    }

    private static RecordBatch readFirstBatch(UnifiedLog log) throws IOException {
        FetchDataInfo fetchInfo = log.read(0, 4096, FetchIsolation.LOG_END, true);
        return fetchInfo.records.batches().iterator().next();
    }

    private static Stream<Arguments> allCompressionParameters() {
        return Arrays.stream(BrokerCompressionType.values())
                .flatMap(brokerCompression -> Arrays.stream(CompressionType.values())
                .map(messageCompression -> Arguments.of(messageCompression, brokerCompression)));
    }
}
