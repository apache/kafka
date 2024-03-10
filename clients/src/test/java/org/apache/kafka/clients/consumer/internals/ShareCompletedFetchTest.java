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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShareCompletedFetchTest {

    private final static String TOPIC_NAME = "test";
    private final static TopicIdPartition TIP = new TopicIdPartition(Uuid.randomUuid(), 0, TOPIC_NAME);
    private final static long PRODUCER_ID = 1000L;
    private final static short PRODUCER_EPOCH = 0;

    @Test
    public void testSimple() {
        long fetchOffset = 5;
        int startingOffset = 10;
        int numRecords = 11;        // Records for 10-20
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(newRecords(startingOffset, numRecords, fetchOffset))
                .setAcquiredRecords(acquiredRecords(startingOffset, numRecords));

        Deserializers<String, String> deserializers = newStringDeserializers();

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

        List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(deserializers, 10, true).getInFlightRecords();
        assertEquals(10, records.size());
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals(10, record.offset());

        records = completedFetch.fetchRecords(deserializers, 10, true).getInFlightRecords();
        assertEquals(1, records.size());
        record = records.get(0);
        assertEquals(20, record.offset());

        records = completedFetch.fetchRecords(deserializers, 10, true).getInFlightRecords();
        assertEquals(0, records.size());
    }

    @Test
    public void testCommittedTransactionRecordsIncluded() {
        int numRecords = 10;
        Records rawRecords = newTransactionalRecords(numRecords);
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(rawRecords)
                .setAcquiredRecords(acquiredRecords(0, numRecords));
        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(deserializers, 10, true).getInFlightRecords();
            assertEquals(10, records.size());
        }
    }

    @Test
    public void testNegativeFetchCount() {
        long fetchOffset = 0;
        int startingOffset = 0;
        int numRecords = 10;
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(newRecords(startingOffset, numRecords, fetchOffset))
                .setAcquiredRecords(acquiredRecords(0, 10));

        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
            List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(deserializers, -10, true).getInFlightRecords();
            assertEquals(0, records.size());
        }
    }

    @Test
    public void testNoRecordsInFetch() {
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(0);

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(deserializers, 10, true).getInFlightRecords();
            assertEquals(0, records.size());
        }
    }

    @Test
    public void testCorruptedMessage() {
        // Create one good record and then one "corrupted" record.
        try (final MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, 0);
             final UUIDSerializer serializer = new UUIDSerializer()) {
            builder.append(new SimpleRecord(serializer.serialize(TOPIC_NAME, UUID.randomUUID())));
            builder.append(0L, "key".getBytes(), "value".getBytes());
            Records records = builder.build();

            ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(0)
                    .setRecords(records)
                    .setAcquiredRecords(acquiredRecords(0, 2));

            try (final Deserializers<UUID, UUID> deserializers = newUuidDeserializers()) {
                ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

                completedFetch.fetchRecords(deserializers, 10, false);

                assertThrows(RecordDeserializationException.class,
                        () -> completedFetch.fetchRecords(deserializers, 10, false));
            }
        }
    }

    private ShareCompletedFetch newShareCompletedFetch(ShareFetchResponseData.PartitionData partitionData) {
        LogContext logContext = new LogContext();

        return new ShareCompletedFetch(
                logContext,
                BufferSupplier.create(),
                TIP,
                partitionData,
                ApiKeys.SHARE_FETCH.latestVersion());
    }

    private static Deserializers<UUID, UUID> newUuidDeserializers() {
        return new Deserializers<>(new UUIDDeserializer(), new UUIDDeserializer());
    }

    private static Deserializers<String, String> newStringDeserializers() {
        return new Deserializers<>(new StringDeserializer(), new StringDeserializer());
    }

    private Records newRecords(long baseOffset, int count, long firstMessageId) {
        try (final MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset)) {
            for (int i = 0; i < count; i++)
                builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
            return builder.build();
        }
    }

    public static List<ShareFetchResponseData.AcquiredRecords> acquiredRecords(long baseOffset, int count) {
        ShareFetchResponseData.AcquiredRecords acquiredRecords = new ShareFetchResponseData.AcquiredRecords()
                .setBaseOffset(baseOffset)
                .setLastOffset(baseOffset + count);
        return Collections.singletonList(acquiredRecords);
    }

    private Records newTransactionalRecords(int numRecords) {
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0,
                time.milliseconds(),
                PRODUCER_ID,
                PRODUCER_EPOCH,
                0,
                true,
                RecordBatch.NO_PARTITION_LEADER_EPOCH)) {
            for (int i = 0; i < numRecords; i++)
                builder.append(new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

            builder.build();
        }

        writeTransactionMarker(buffer, numRecords, time);
        buffer.flip();

        return MemoryRecords.readableRecords(buffer);
    }

    private void writeTransactionMarker(ByteBuffer buffer,
                                        int offset,
                                        Time time) {
        MemoryRecords.writeEndTransactionalMarker(buffer,
                offset,
                time.milliseconds(),
                0,
                PRODUCER_ID,
                PRODUCER_EPOCH,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));
    }
}
