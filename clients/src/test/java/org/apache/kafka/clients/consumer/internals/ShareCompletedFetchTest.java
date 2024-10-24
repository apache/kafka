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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
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
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ShareCompletedFetchTest {
    private static final String TOPIC_NAME = "test";
    private static final TopicIdPartition TIP = new TopicIdPartition(Uuid.randomUuid(), 0, TOPIC_NAME);
    private static final long PRODUCER_ID = 1000L;
    private static final short PRODUCER_EPOCH = 0;

    @Test
    public void testSimple() {
        long startingOffset = 10L;
        int numRecordsPerBatch = 10;
        int numRecords = 20;        // Records for 10-29, in 2 equal batches
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(newRecords(startingOffset, numRecordsPerBatch, 2))
                .setAcquiredRecords(acquiredRecords(startingOffset, numRecords));

        Deserializers<String, String> deserializers = newStringDeserializers();

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 10, true);
        List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();
        assertEquals(10, records.size());
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals(10L, record.offset());
        assertEquals(Optional.of((short) 1), record.deliveryCount());
        Acknowledgements acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());

        batch = completedFetch.fetchRecords(deserializers, 10, true);
        records = batch.getInFlightRecords();
        assertEquals(10, records.size());
        record = records.get(0);
        assertEquals(20L, record.offset());
        assertEquals(Optional.of((short) 1), record.deliveryCount());
        acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());

        batch = completedFetch.fetchRecords(deserializers, 10, true);
        records = batch.getInFlightRecords();
        assertEquals(0, records.size());
        acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());
    }

    @Test
    public void testSoftMaxPollRecordLimit() {
        long startingOffset = 10L;
        int numRecords = 11;        // Records for 10-20, in a single batch
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(newRecords(startingOffset, numRecords))
            .setAcquiredRecords(acquiredRecords(startingOffset, numRecords));

        Deserializers<String, String> deserializers = newStringDeserializers();

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 10, true);
        List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();
        assertEquals(11, records.size());
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals(10L, record.offset());
        assertEquals(Optional.of((short) 1), record.deliveryCount());
        Acknowledgements acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());

        batch = completedFetch.fetchRecords(deserializers, 10, true);
        records = batch.getInFlightRecords();
        assertEquals(0, records.size());
        acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());
    }

    @Test
    public void testUnaligned() {
        long startingOffset = 10L;
        int numRecords = 10;
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(newRecords(startingOffset, numRecords + 500))
                .setAcquiredRecords(acquiredRecords(startingOffset + 500, numRecords));

        Deserializers<String, String> deserializers = newStringDeserializers();

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 10, true);
        List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();
        assertEquals(10, records.size());
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals(510L, record.offset());
        assertEquals(Optional.of((short) 1), record.deliveryCount());
        Acknowledgements acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());

        batch = completedFetch.fetchRecords(deserializers, 10, true);
        records = batch.getInFlightRecords();
        assertEquals(0, records.size());
        acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());
    }

    @Test
    public void testCommittedTransactionRecordsIncluded() {
        int numRecords = 10;
        Records rawRecords = newTransactionalRecords(numRecords);
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(rawRecords)
                .setAcquiredRecords(acquiredRecords(0L, numRecords));
        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 10, true);
            List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();
            assertEquals(10, records.size());
            Acknowledgements acknowledgements = batch.getAcknowledgements();
            assertEquals(0, acknowledgements.size());
        }
    }

    @Test
    public void testNegativeFetchCount() {
        int startingOffset = 0;
        int numRecords = 10;
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(newRecords(startingOffset, numRecords))
                .setAcquiredRecords(acquiredRecords(0L, 10));

        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
            ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, -10, true);
            List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();
            assertEquals(0, records.size());
            Acknowledgements acknowledgements = batch.getAcknowledgements();
            assertEquals(0, acknowledgements.size());
        }
    }

    @Test
    public void testNoRecordsInFetch() {
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(0);

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 10, true);
            List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();
            assertEquals(0, records.size());
            Acknowledgements acknowledgements = batch.getAcknowledgements();
            assertEquals(0, acknowledgements.size());
        }
    }

    @Test
    public void testCorruptedMessage() {
        // Create one good record and then two "corrupted" records and then another good record.
        try (final MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024),
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0);
             final UUIDSerializer serializer = new UUIDSerializer()) {
            builder.append(new SimpleRecord(serializer.serialize(TOPIC_NAME, UUID.randomUUID())));
            builder.append(0L, "key".getBytes(), "value".getBytes());
            Headers headers = new RecordHeaders();
            headers.add("hkey", "hvalue".getBytes());
            builder.append(10L, serializer.serialize("key", UUID.randomUUID()), "otherValue".getBytes(), headers.toArray());
            builder.append(new SimpleRecord(serializer.serialize(TOPIC_NAME, UUID.randomUUID())));
            Records records = builder.build();

            ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(0)
                    .setRecords(records)
                    .setAcquiredRecords(acquiredRecords(0L, 4));

            try (final Deserializers<UUID, UUID> deserializers = newUuidDeserializers()) {
                ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

                // Record 0 is returned by itself because record 1 fails to deserialize
                ShareInFlightBatch<UUID, UUID> batch = completedFetch.fetchRecords(deserializers, 10, false);
                assertNull(batch.getException());
                List<ConsumerRecord<UUID, UUID>> fetchedRecords = batch.getInFlightRecords();
                assertEquals(1, fetchedRecords.size());
                assertEquals(0L, fetchedRecords.get(0).offset());
                Acknowledgements acknowledgements = batch.getAcknowledgements();
                assertEquals(0, acknowledgements.size());

                // Record 1 then results in an empty batch
                batch = completedFetch.fetchRecords(deserializers, 10, false);
                assertEquals(RecordDeserializationException.class, batch.getException().getClass());
                RecordDeserializationException thrown = (RecordDeserializationException) batch.getException();
                assertEquals(RecordDeserializationException.DeserializationExceptionOrigin.KEY, thrown.origin());
                assertEquals(1, thrown.offset());
                assertEquals(TOPIC_NAME, thrown.topicPartition().topic());
                assertEquals(0, thrown.topicPartition().partition());
                assertEquals(0, thrown.timestamp());
                assertArrayEquals("key".getBytes(), org.apache.kafka.common.utils.Utils.toNullableArray(thrown.keyBuffer()));
                assertArrayEquals("value".getBytes(), Utils.toNullableArray(thrown.valueBuffer()));
                assertEquals(0, thrown.headers().toArray().length);
                fetchedRecords = batch.getInFlightRecords();
                assertEquals(0, fetchedRecords.size());
                acknowledgements = batch.getAcknowledgements();
                assertEquals(1, acknowledgements.size());
                assertEquals(AcknowledgeType.RELEASE, acknowledgements.get(1L));

                // Record 2 then results in an empty batch, because record 1 has now been skipped
                batch = completedFetch.fetchRecords(deserializers, 10, false);
                assertEquals(RecordDeserializationException.class, batch.getException().getClass());
                thrown = (RecordDeserializationException) batch.getException();
                assertEquals(RecordDeserializationException.DeserializationExceptionOrigin.VALUE, thrown.origin());
                assertEquals(2L, thrown.offset());
                assertEquals(TOPIC_NAME, thrown.topicPartition().topic());
                assertEquals(0, thrown.topicPartition().partition());
                assertEquals(10L, thrown.timestamp());
                assertNotNull(thrown.keyBuffer());
                assertArrayEquals("otherValue".getBytes(), Utils.toNullableArray(thrown.valueBuffer()));
                fetchedRecords = batch.getInFlightRecords();
                assertEquals(0, fetchedRecords.size());
                acknowledgements = batch.getAcknowledgements();
                assertEquals(1, acknowledgements.size());
                assertEquals(AcknowledgeType.RELEASE, acknowledgements.get(2L));

                // Record 3 is returned in the next batch, because record 2 has now been skipped
                batch = completedFetch.fetchRecords(deserializers, 10, false);
                assertNull(batch.getException());
                fetchedRecords = batch.getInFlightRecords();
                assertEquals(1, fetchedRecords.size());
                assertEquals(3L, fetchedRecords.get(0).offset());
                acknowledgements = batch.getAcknowledgements();
                assertEquals(0, acknowledgements.size());
            }
        }
    }

    @Test
    public void testAcquiredRecords() {
        int startingOffset = 0;
        int numRecords = 10;        // Records for 0-9

        // Acquiring records 0-2 and 6-8
        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>(acquiredRecords(0L, 3));
        acquiredRecords.addAll(acquiredRecords(6L, 3));
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(newRecords(startingOffset, numRecords))
                .setAcquiredRecords(acquiredRecords);

        Deserializers<String, String> deserializers = newStringDeserializers();

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

        List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(deserializers, 10, true).getInFlightRecords();
        assertEquals(6, records.size());
        // The first offset should be 0
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals(0L, record.offset());
        assertEquals(Optional.of((short) 1), record.deliveryCount());
        // The third offset should be 6
        record = records.get(3);
        assertEquals(6L, record.offset());
        assertEquals(Optional.of((short) 1), record.deliveryCount());

        records = completedFetch.fetchRecords(deserializers, 10, true).getInFlightRecords();
        assertEquals(0, records.size());
    }

    @Test
    public void testAcquireOddRecords() {
        int startingOffset = 0;
        int numRecords = 10;        // Records for 0-9

        // Acquiring all odd Records
        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>();
        for (long i = 1; i <= 9; i += 2) {
            acquiredRecords.add(acquiredRecords(i, 1).get(0));
        }

        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setRecords(newRecords(startingOffset, numRecords))
                .setAcquiredRecords(acquiredRecords);

        Deserializers<String, String> deserializers = newStringDeserializers();

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

        List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(deserializers, 10, true).getInFlightRecords();
        assertEquals(5, records.size());
        // The first offset should be 1
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals(1L, record.offset());
        assertEquals(Optional.of((short) 1), record.deliveryCount());
        // The second offset should be 3
        record = records.get(1);
        assertEquals(3L, record.offset());
        assertEquals(Optional.of((short) 1), record.deliveryCount());

        records = completedFetch.fetchRecords(deserializers, 10, true).getInFlightRecords();
        assertEquals(0, records.size());
    }

    private ShareCompletedFetch newShareCompletedFetch(ShareFetchResponseData.PartitionData partitionData) {
        LogContext logContext = new LogContext();
        ShareFetchMetricsRegistry shareFetchMetricsRegistry = new ShareFetchMetricsRegistry();
        ShareFetchMetricsManager shareFetchMetricsManager = new ShareFetchMetricsManager(new Metrics(), shareFetchMetricsRegistry);
        Set<TopicPartition> partitionSet = new HashSet<>();
        partitionSet.add(TIP.topicPartition());
        ShareFetchMetricsAggregator shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(shareFetchMetricsManager, partitionSet);

        return new ShareCompletedFetch(
                logContext,
                BufferSupplier.create(),
                TIP,
                partitionData,
                shareFetchMetricsAggregator,
                ApiKeys.SHARE_FETCH.latestVersion());
    }

    private static Deserializers<UUID, UUID> newUuidDeserializers() {
        return new Deserializers<>(new UUIDDeserializer(), new UUIDDeserializer());
    }

    private static Deserializers<String, String> newStringDeserializers() {
        return new Deserializers<>(new StringDeserializer(), new StringDeserializer());
    }

    private Records newRecords(long baseOffset, int count) {
        try (final MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024),
                Compression.NONE,
                TimestampType.CREATE_TIME,
                baseOffset)) {
            for (int i = 0; i < count; i++)
                builder.append(0L, "key".getBytes(), "value-".getBytes());
            return builder.build();
        }
    }

    private Records newRecords(long baseOffset, int numRecordsPerBatch, int batchCount) {
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        for (long b = 0; b < batchCount; b++) {
            try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                baseOffset + b * numRecordsPerBatch,
                time.milliseconds(),
                PRODUCER_ID,
                PRODUCER_EPOCH,
                0,
                true,
                RecordBatch.NO_PARTITION_LEADER_EPOCH)) {
                for (int i = 0; i < numRecordsPerBatch; i++)
                    builder.append(new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

                builder.build();
            }
        }

        buffer.flip();

        return MemoryRecords.readableRecords(buffer);
    }

    public static List<ShareFetchResponseData.AcquiredRecords> acquiredRecords(long firstOffset, int count) {
        ShareFetchResponseData.AcquiredRecords acquiredRecords = new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(firstOffset)
                .setLastOffset(firstOffset + count - 1)
                .setDeliveryCount((short) 1);
        return Collections.singletonList(acquiredRecords);
    }

    private Records newTransactionalRecords(int numRecords) {
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
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
