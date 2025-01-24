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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProduceRequestTest {

    private final SimpleRecord simpleRecord = new SimpleRecord(System.currentTimeMillis(),
                                                               "key".getBytes(),
                                                               "value".getBytes());

    @Test
    public void shouldBeFlaggedAsTransactionalWhenTransactionalRecords() {
        final MemoryRecords memoryRecords = MemoryRecords.withTransactionalRecords(0, Compression.NONE, 1L,
            (short) 1, 1, 1, simpleRecord);

        final ProduceRequest request = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setName("topic")
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(1)
                            .setRecords(memoryRecords)))).iterator()))
            .setAcks((short) -1)
            .setTimeoutMs(10)).build();
        assertTrue(RequestUtils.hasTransactionalRecords(request));
    }

    @Test
    public void shouldNotBeFlaggedAsTransactionalWhenNoRecords() {
        final ProduceRequest request = createNonIdempotentNonTransactionalRecords();
        assertFalse(RequestUtils.hasTransactionalRecords(request));
    }

    @Test
    public void shouldNotBeFlaggedAsIdempotentWhenRecordsNotIdempotent() {
        final ProduceRequest request = createNonIdempotentNonTransactionalRecords();
        assertFalse(RequestTestUtils.hasIdempotentRecords(request));
    }

    @Test
    public void shouldBeFlaggedAsIdempotentWhenIdempotentRecords() {
        final MemoryRecords memoryRecords = MemoryRecords.withIdempotentRecords(1, Compression.NONE, 1L,
            (short) 1, 1, 1, simpleRecord);
        final ProduceRequest request = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setName("topic")
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(1)
                            .setRecords(memoryRecords)))).iterator()))
            .setAcks((short) -1)
            .setTimeoutMs(10)).build();
        assertTrue(RequestTestUtils.hasIdempotentRecords(request));
    }

    @Test
    public void testBuildWithCurrentMessageFormat() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
            Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        ProduceRequest.Builder requestBuilder = ProduceRequest.builder(
            new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                    new ProduceRequestData.TopicProduceData().setName("test").setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData().setIndex(9).setRecords(builder.build()))))
                    .iterator()))
                .setAcks((short) 1)
                .setTimeoutMs(5000),
            false);
        assertEquals(3, requestBuilder.oldestAllowedVersion());
        assertEquals(ApiKeys.PRODUCE.latestVersion(), requestBuilder.latestAllowedVersion());
    }

    @Test
    public void testV3AndAboveShouldContainOnlyOneRecordBatch() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, Compression.NONE, TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "1".getBytes(), "b".getBytes());
        builder.append(12L, null, "c".getBytes());
        builder.close();

        buffer.flip();

        ProduceRequest.Builder requestBuilder = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setName("test")
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(0)
                                .setRecords(MemoryRecords.readableRecords(buffer))))).iterator()))
            .setAcks((short) 1)
            .setTimeoutMs(5000));
        assertThrowsForAllVersions(requestBuilder, InvalidRecordException.class);
    }

    @Test
    public void testV3AndAboveCannotHaveNoRecordBatches() {
        ProduceRequest.Builder requestBuilder = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setName("test")
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(0)
                            .setRecords(MemoryRecords.EMPTY)))).iterator()))
            .setAcks((short) 1)
            .setTimeoutMs(5000));
        assertThrowsForAllVersions(requestBuilder, InvalidRecordException.class);
    }

    @Test
    public void testV3AndAboveCannotUseMagicV0() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0, Compression.NONE,
            TimestampType.NO_TIMESTAMP_TYPE, 0L);
        builder.append(10L, null, "a".getBytes());

        ProduceRequest.Builder requestBuilder = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setName("test")
                    .setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData()
                            .setIndex(0)
                            .setRecords(builder.build())))).iterator()))
            .setAcks((short) 1)
            .setTimeoutMs(5000));
        assertThrowsForAllVersions(requestBuilder, InvalidRecordException.class);
    }

    @Test
    public void testV3AndAboveCannotUseMagicV1() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, Compression.NONE,
            TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());

        ProduceRequest.Builder requestBuilder = ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setName("test")
                    .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                        .setIndex(0)
                        .setRecords(builder.build()))))
                .iterator()))
            .setAcks((short) 1)
            .setTimeoutMs(5000));
        assertThrowsForAllVersions(requestBuilder, InvalidRecordException.class);
    }

    @Test
    public void testV6AndBelowCannotUseZStdCompression() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, Compression.zstd().build(),
            TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());

        ProduceRequestData produceData = new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setName("test")
                    .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                        .setIndex(0)
                        .setRecords(builder.build()))))
                .iterator()))
            .setAcks((short) 1)
            .setTimeoutMs(1000);
        // Can't create ProduceRequest instance with version within [3, 7)
        for (short version = 3; version < 7; version++) {

            ProduceRequest.Builder requestBuilder = new ProduceRequest.Builder(version, version, produceData);
            assertThrowsForAllVersions(requestBuilder, UnsupportedCompressionTypeException.class);
        }

        // Works fine with current version (>= 7)
        ProduceRequest.builder(produceData);
    }

    @Test
    public void testMixedTransactionalData() {
        final long producerId = 15L;
        final short producerEpoch = 5;
        final int sequence = 10;

        final MemoryRecords nonTxnRecords = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("foo".getBytes()));
        final MemoryRecords txnRecords = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId,
            producerEpoch, sequence, new SimpleRecord("bar".getBytes()));

        ProduceRequest.Builder builder = ProduceRequest.builder(
            new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Arrays.asList(
                    new ProduceRequestData.TopicProduceData().setName("foo").setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(txnRecords))),
                    new ProduceRequestData.TopicProduceData().setName("foo").setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData().setIndex(1).setRecords(nonTxnRecords))))
                    .iterator()))
                .setAcks((short) -1)
                .setTimeoutMs(5000),
            true);
        final ProduceRequest request = builder.build();
        assertTrue(RequestUtils.hasTransactionalRecords(request));
        assertTrue(RequestTestUtils.hasIdempotentRecords(request));
    }

    @Test
    public void testMixedIdempotentData() {
        final long producerId = 15L;
        final short producerEpoch = 5;
        final int sequence = 10;

        final MemoryRecords nonIdempotentRecords = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("foo".getBytes()));
        final MemoryRecords idempotentRecords = MemoryRecords.withIdempotentRecords(Compression.NONE, producerId,
            producerEpoch, sequence, new SimpleRecord("bar".getBytes()));

        ProduceRequest.Builder builder = ProduceRequest.builder(
            new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Arrays.asList(
                    new ProduceRequestData.TopicProduceData().setName("foo").setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(idempotentRecords))),
                    new ProduceRequestData.TopicProduceData().setName("foo").setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData().setIndex(1).setRecords(nonIdempotentRecords))))
                    .iterator()))
                .setAcks((short) -1)
                .setTimeoutMs(5000),
            true);

        final ProduceRequest request = builder.build();
        assertFalse(RequestUtils.hasTransactionalRecords(request));
        assertTrue(RequestTestUtils.hasIdempotentRecords(request));
    }

    private static <T extends Throwable> void assertThrowsForAllVersions(ProduceRequest.Builder builder,
                                                                         Class<T> expectedType) {
        IntStream.range(builder.oldestAllowedVersion(), builder.latestAllowedVersion() + 1)
            .forEach(version -> assertThrows(expectedType, () -> builder.build((short) version).serialize()));
    }

    private ProduceRequest createNonIdempotentNonTransactionalRecords() {
        return ProduceRequest.builder(new ProduceRequestData()
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                new ProduceRequestData.TopicProduceData()
                    .setName("topic")
                    .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                        .setIndex(1)
                        .setRecords(MemoryRecords.withRecords(Compression.NONE, simpleRecord)))))
                .iterator()))
            .setAcks((short) -1)
            .setTimeoutMs(10)).build();
    }
}
