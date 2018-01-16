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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProduceRequestTest {

    private final SimpleRecord simpleRecord = new SimpleRecord(System.currentTimeMillis(),
                                                               "key".getBytes(),
                                                               "value".getBytes());

    @Test
    public void shouldBeFlaggedAsTransactionalWhenTransactionalRecords() throws Exception {
        final MemoryRecords memoryRecords = MemoryRecords.withTransactionalRecords(0, CompressionType.NONE, 1L,
                (short) 1, 1, 1, simpleRecord);
        final ProduceRequest request = ProduceRequest.Builder.forCurrentMagic((short) -1,
                10, Collections.singletonMap(new TopicPartition("topic", 1), memoryRecords)).build();
        assertTrue(request.isTransactional());
    }

    @Test
    public void shouldNotBeFlaggedAsTransactionalWhenNoRecords() throws Exception {
        final ProduceRequest request = createNonIdempotentNonTransactionalRecords();
        assertFalse(request.isTransactional());
    }

    @Test
    public void shouldNotBeFlaggedAsIdempotentWhenRecordsNotIdempotent() throws Exception {
        final ProduceRequest request = createNonIdempotentNonTransactionalRecords();
        assertFalse(request.isTransactional());
    }

    @Test
    public void shouldBeFlaggedAsIdempotentWhenIdempotentRecords() throws Exception {
        final MemoryRecords memoryRecords = MemoryRecords.withIdempotentRecords(1, CompressionType.NONE, 1L,
                (short) 1, 1, 1, simpleRecord);
        final ProduceRequest request = ProduceRequest.Builder.forCurrentMagic((short) -1, 10,
                Collections.singletonMap(new TopicPartition("topic", 1), memoryRecords)).build();
        assertTrue(request.isIdempotent());
    }

    @Test
    public void testBuildWithOldMessageFormat() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        Map<TopicPartition, MemoryRecords> produceData = new HashMap<>();
        produceData.put(new TopicPartition("test", 0), builder.build());

        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forMagic(RecordBatch.MAGIC_VALUE_V1, (short) 1,
                5000, produceData, null);
        assertEquals(2, requestBuilder.oldestAllowedVersion());
        assertEquals(2, requestBuilder.latestAllowedVersion());
    }

    @Test
    public void testBuildWithCurrentMessageFormat() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        Map<TopicPartition, MemoryRecords> produceData = new HashMap<>();
        produceData.put(new TopicPartition("test", 0), builder.build());

        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forMagic(RecordBatch.CURRENT_MAGIC_VALUE,
                (short) 1, 5000, produceData, null);
        assertEquals(3, requestBuilder.oldestAllowedVersion());
        assertEquals(ApiKeys.PRODUCE.latestVersion(), requestBuilder.latestAllowedVersion());
    }

    @Test
    public void testV3AndAboveShouldContainOnlyOneRecordBatch() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "1".getBytes(), "b".getBytes());
        builder.append(12L, null, "c".getBytes());
        builder.close();

        buffer.flip();

        Map<TopicPartition, MemoryRecords> produceData = new HashMap<>();
        produceData.put(new TopicPartition("test", 0), MemoryRecords.readableRecords(buffer));
        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forCurrentMagic((short) 1, 5000, produceData);
        assertThrowsInvalidRecordExceptionForAllVersions(requestBuilder);
    }

    @Test
    public void testV3AndAboveCannotHaveNoRecordBatches() {
        Map<TopicPartition, MemoryRecords> produceData = new HashMap<>();
        produceData.put(new TopicPartition("test", 0), MemoryRecords.EMPTY);
        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forCurrentMagic((short) 1, 5000, produceData);
        assertThrowsInvalidRecordExceptionForAllVersions(requestBuilder);
    }

    @Test
    public void testV3AndAboveCannotUseMagicV0() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0, CompressionType.NONE,
                TimestampType.NO_TIMESTAMP_TYPE, 0L);
        builder.append(10L, null, "a".getBytes());

        Map<TopicPartition, MemoryRecords> produceData = new HashMap<>();
        produceData.put(new TopicPartition("test", 0), builder.build());
        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forCurrentMagic((short) 1, 5000, produceData);
        assertThrowsInvalidRecordExceptionForAllVersions(requestBuilder);
    }

    @Test
    public void testV3AndAboveCannotUseMagicV1() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());

        Map<TopicPartition, MemoryRecords> produceData = new HashMap<>();
        produceData.put(new TopicPartition("test", 0), builder.build());
        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forCurrentMagic((short) 1, 5000, produceData);
        assertThrowsInvalidRecordExceptionForAllVersions(requestBuilder);
    }

    private void assertThrowsInvalidRecordExceptionForAllVersions(ProduceRequest.Builder builder) {
        for (short version = builder.oldestAllowedVersion(); version < builder.latestAllowedVersion(); version++) {
            assertThrowsInvalidRecordException(builder, version);
        }
    }

    private void assertThrowsInvalidRecordException(ProduceRequest.Builder builder, short version) {
        try {
            builder.build(version).toStruct();
            fail("Builder did not raise " + InvalidRecordException.class.getName() + " as expected");
        } catch (RuntimeException e) {
            assertTrue("Unexpected exception type " + e.getClass().getName(),
                    InvalidRecordException.class.isAssignableFrom(e.getClass()));
        }
    }

    private ProduceRequest createNonIdempotentNonTransactionalRecords() {
        final MemoryRecords memoryRecords = MemoryRecords.withRecords(CompressionType.NONE, simpleRecord);
        return ProduceRequest.Builder.forCurrentMagic((short) -1, 10,
                Collections.singletonMap(new TopicPartition("topic", 1), memoryRecords)).build();
    }
}
