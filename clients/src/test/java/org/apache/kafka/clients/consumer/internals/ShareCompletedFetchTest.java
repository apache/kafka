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
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareCompletedFetchTest {
    private static final String TOPIC_NAME = "test";
    private static final TopicIdPartition TIP = new TopicIdPartition(Uuid.randomUuid(), 0, TOPIC_NAME);
    private static final Optional<Integer> DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS = Optional.of(30000);
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
        assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());

        batch = completedFetch.fetchRecords(deserializers, 10, true);
        records = batch.getInFlightRecords();
        assertEquals(10, records.size());
        record = records.get(0);
        assertEquals(20L, record.offset());
        assertEquals(Optional.of((short) 1), record.deliveryCount());
        acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());
        assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());

        batch = completedFetch.fetchRecords(deserializers, 10, true);
        records = batch.getInFlightRecords();
        assertEquals(0, records.size());
        acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());
        assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());
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
        assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());

        batch = completedFetch.fetchRecords(deserializers, 10, true);
        records = batch.getInFlightRecords();
        assertEquals(0, records.size());
        acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());
        assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());
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
        assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());

        batch = completedFetch.fetchRecords(deserializers, 10, true);
        records = batch.getInFlightRecords();
        assertEquals(0, records.size());
        acknowledgements = batch.getAcknowledgements();
        assertEquals(0, acknowledgements.size());
        assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());
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
            assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());
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
            assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());
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
            assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());
        }
    }

    @Test
    public void testRecordDeserializationException() {
        // Create one good record and then two records which do not deserialize and then another good record.
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
                assertEquals(RecordDeserializationException.class, batch.getException().cause().getClass());
                RecordDeserializationException thrown = (RecordDeserializationException) batch.getException().cause();
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
                assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());

                // Record 2 then results in an empty batch, because record 1 has now been skipped
                batch = completedFetch.fetchRecords(deserializers, 10, false);
                assertEquals(RecordDeserializationException.class, batch.getException().cause().getClass());
                thrown = (RecordDeserializationException) batch.getException().cause();
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
                assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());

                // Record 3 is returned in the next batch, because record 2 has now been skipped
                batch = completedFetch.fetchRecords(deserializers, 10, false);
                assertNull(batch.getException());
                fetchedRecords = batch.getInFlightRecords();
                assertEquals(1, fetchedRecords.size());
                assertEquals(3L, fetchedRecords.get(0).offset());
                acknowledgements = batch.getAcknowledgements();
                assertEquals(0, acknowledgements.size());
                assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.getAcquisitionLockTimeoutMs());
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
    public void testGapsForControlRecordsInAcquiredRange() {
        int numRecords = 10;
        // Create records with transaction markers (control records)
        Records rawRecords = newTransactionalRecords(numRecords);

        // Acquire all records including the control record (offset 10 is the commit marker)
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(rawRecords)
            .setAcquiredRecords(acquiredRecords(0L, numRecords + 1));

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 15, true);
            List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();

            // Should get 10 actual records (control records are filtered out)
            assertEquals(10, records.size());

            // Should have 1 gap for the control record at offset 10
            Acknowledgements acknowledgements = batch.getAcknowledgements();
            assertEquals(1, acknowledgements.size());
            assertNull(acknowledgements.get(10L), "Offset 10 (control record) should be a GAP (null)");
        }
    }

    @Test
    public void testMixedRecordsAndGaps() {
        int startingOffset = 0;

        // Acquire records 0-4 (exist), 10-14 (don't exist = gaps)
        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>();
        acquiredRecords.add(acquiredRecords(0L, 5).get(0));
        acquiredRecords.add(acquiredRecords(10L, 5).get(0));

        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(newRecords(startingOffset,  10))
            .setAcquiredRecords(acquiredRecords); // Acquire only records 0-4 and 10-14

        Deserializers<String, String> deserializers = newStringDeserializers();

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 20, true);
        List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();

        // Should get 5 actual records (0-4)
        assertEquals(5, records.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(i, records.get(i).offset());
        }

        // Should have 5 gaps (10-14) in acknowledgements
        Acknowledgements acknowledgements = batch.getAcknowledgements();
        assertEquals(5, acknowledgements.size());

        // Verify GAP acknowledgements for offsets 10-14
        for (long offset = 10L; offset <= 14L; offset++) {
            assertNull(acknowledgements.get(offset), "Offset " + offset + " should be a GAP (null)");
        }
    }

    @Test
    public void testAcknowledgementsIncludeOnlyGaps() {
        int startingOffset = 0;
        int numRecords = 10;        // Records for 0-9

        // Acquire only non-existent records 15-19 (all should be gaps)
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(newRecords(startingOffset, numRecords))  // Records 0-9
            .setAcquiredRecords(acquiredRecords(15L, 5));       // Acquire 15-19 (don't exist)

        Deserializers<String, String> deserializers = newStringDeserializers();

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 20, true);
        List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();

        // Should get no actual records
        assertEquals(0, records.size());

        // Should have 5 gaps (15-19) in acknowledgements
        Acknowledgements acknowledgements = batch.getAcknowledgements();
        assertEquals(5, acknowledgements.size());

        // Verify all are GAP acknowledgements
        for (long offset = 15L; offset <= 19L; offset++) {
            assertNull(acknowledgements.get(offset), "Offset " + offset + " should be a GAP (null)");
        }
    }

    @Test
    public void testGapsWithControlRecordsAtBeginningAndEnd() {
        // Create transactional records: control record, data records 1-5, control record at 6
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        // Write first control record (commit marker at offset 0)
        writeTransactionMarker(buffer, 0, time);

        // Write data records 1-5
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                1,
                time.milliseconds(),
                PRODUCER_ID,
                PRODUCER_EPOCH,
                0,
                true,
                RecordBatch.NO_PARTITION_LEADER_EPOCH)) {
            for (int i = 0; i < 5; i++)
                builder.append(new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));
            builder.build();
        }

        // Write second control record (commit marker at offset 6)
        writeTransactionMarker(buffer, 6, time);

        buffer.flip();
        Records records = MemoryRecords.readableRecords(buffer);

        // Acquire all offsets 0-6 (includes both control records and data records)
        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(records)
            .setAcquiredRecords(acquiredRecords(0L, 7));

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 10, true);
            List<ConsumerRecord<String, String>> fetchedRecords = batch.getInFlightRecords();

            // Should get 5 data records (1-5)
            assertEquals(5, fetchedRecords.size());
            assertEquals(1L, fetchedRecords.get(0).offset());
            assertEquals(5L, fetchedRecords.get(4).offset());

            // Should have 2 gaps for the control records (offsets 0 and 6)
            Acknowledgements acknowledgements = batch.getAcknowledgements();
            assertEquals(2, acknowledgements.size());
            assertNull(acknowledgements.get(0L), "Offset 0 (control record) should be a GAP (null)");
            assertNull(acknowledgements.get(6L), "Offset 6 (control record) should be a GAP (null)");
        }
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

    @Test
    public void testOverlappingAcquiredRecordsLogsErrorAndRetainsFirstOccurrence() {
        int startingOffset = 0;
        int numRecords = 20;        // Records for 0-19

        // Create overlapping acquired records: [0-9] and [5-14]
        // Offsets 5-9 will be duplicates
        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>();
        acquiredRecords.add(new ShareFetchResponseData.AcquiredRecords()
            .setFirstOffset(0L)
            .setLastOffset(9L)
            .setDeliveryCount((short) 1));
        acquiredRecords.add(new ShareFetchResponseData.AcquiredRecords()
            .setFirstOffset(5L)
            .setLastOffset(14L)
            .setDeliveryCount((short) 2));

        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(newRecords(startingOffset, numRecords))
            .setAcquiredRecords(acquiredRecords);

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);

        Deserializers<String, String> deserializers = newStringDeserializers();

        // Fetch records and verify that only 15 unique records are returned (0-14)
        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 20, true);
        List<ConsumerRecord<String, String>> records = batch.getInFlightRecords();
        
        // Should get 15 unique records: 0-9 from first range (with deliveryCount=1)
        // and 10-14 from second range (with deliveryCount=2)
        assertEquals(15, records.size());
        
        // Verify first occurrence (offset 5 should have deliveryCount=1 from first range)
        ConsumerRecord<String, String> record5 = records.stream()
            .filter(r -> r.offset() == 5L)
            .findFirst()
            .orElse(null);
        assertNotNull(record5);
        assertEquals(Optional.of((short) 1), record5.deliveryCount());
        
        // Verify offset 10 has deliveryCount=2 from second range
        ConsumerRecord<String, String> record10 = records.stream()
            .filter(r -> r.offset() == 10L)
            .findFirst()
            .orElse(null);
        assertNotNull(record10);
        assertEquals(Optional.of((short) 2), record10.deliveryCount());
        
        // Verify all offsets are unique
        Set<Long> offsetSet = new HashSet<>();
        for (ConsumerRecord<String, String> record : records) {
            assertTrue(offsetSet.add(record.offset()), "Duplicate offset found in results: " + record.offset());
        }
    }

    @Test
    public void testCrcFailureOnFirstBatchThenSubsequentFetch() {
        // Create 3 batches: corrupted batch (offsets 0-9), good batch (10-19), good batch (20-29)
        Records records = newRecordsWithCorruptedFirstBatch();

        // Acquire all 30 records (0-29)
        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>();
        acquiredRecords.add(acquiredRecords(0L, 10).get(0));
        acquiredRecords.add(acquiredRecords(10L, 10).get(0));
        acquiredRecords.add(acquiredRecords(20L, 10).get(0));

        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(records)
            .setAcquiredRecords(acquiredRecords);

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        Deserializers<String, String> deserializers = newStringDeserializers();

        // First fetch should fail with CRC error and reject the entire first batch
        ShareInFlightBatch<String, String> batch1 = completedFetch.fetchRecords(deserializers, 10, true);
        assertNotNull(batch1.getException(), "Should have exception for corrupted batch");
        assertEquals(CorruptRecordException.class, batch1.getException().cause().getClass());

        // Verify all 10 records from first batch are rejected
        Acknowledgements acks1 = batch1.getAcknowledgements();
        assertEquals(10, acks1.size(), "All records in corrupted batch should be rejected");
        for (long offset = 0; offset < 10; offset++) {
            assertEquals(AcknowledgeType.REJECT, acks1.get(offset),
                "Record at offset " + offset + " should be REJECT");
        }

        // No records should be returned
        assertEquals(0, batch1.getInFlightRecords().size());

        // Should successfully fetch records from the second batch (offsets 10-19)
        ShareInFlightBatch<String, String> batch2 = completedFetch.fetchRecords(deserializers, 10, true);
        assertNotNull(batch2, "Should return a batch");
        List<ConsumerRecord<String, String>> records2 = batch2.getInFlightRecords();
        assertEquals(10, records2.size(), "Should get 10 records from second batch");
        assertEquals(10L, records2.get(0).offset(), "First record should be offset 10");
        assertEquals(19L, records2.get(9).offset(), "Last record should be offset 19");

        // Third fetch should process the third batch correctly
        ShareInFlightBatch<String, String> batch3 = completedFetch.fetchRecords(deserializers, 10, true);
        List<ConsumerRecord<String, String>> records3 = batch3.getInFlightRecords();
        assertEquals(10, records3.size(), "Should get 10 records from third batch");
        assertEquals(20L, records3.get(0).offset(), "First record should be offset 20");
        assertEquals(29L, records3.get(9).offset(), "Last record should be offset 29");
    }

    @Test
    public void testCrcFailureWithGapsInBatch() {
        // Create corrupted batch with offsets 0-9
        Records records = newSingleCorruptedBatch(0L, 10);

        // Only acquire odd offsets: 1, 3, 5, 7, 9
        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>();
        for (long offset = 1; offset < 10; offset += 2) {
            acquiredRecords.add(acquiredRecords(offset, 1).get(0));
        }

        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(records)
            .setAcquiredRecords(acquiredRecords);

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        Deserializers<String, String> deserializers = newStringDeserializers();

        // Fetch should fail with CRC error
        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 10, true);
        assertNotNull(batch.getException());
        assertEquals(CorruptRecordException.class, batch.getException().cause().getClass());

        // Only the acquired records (odd offsets) should be rejected
        Acknowledgements acks = batch.getAcknowledgements();
        assertEquals(5, acks.size(), "Only 5 acquired records should be rejected");
        assertEquals(AcknowledgeType.REJECT, acks.get(1L));
        assertEquals(AcknowledgeType.REJECT, acks.get(3L));
        assertEquals(AcknowledgeType.REJECT, acks.get(5L));
        assertEquals(AcknowledgeType.REJECT, acks.get(7L));
        assertEquals(AcknowledgeType.REJECT, acks.get(9L));
    }

    @Test
    public void testCrcFailureWithUnalignedAcquiredRecords() {
        // Create corrupted batch with offsets 0-19
        Records records = newSingleCorruptedBatch(0L, 20);

        // Only acquire records 10-19 (second half of the corrupted batch)
        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>();
        acquiredRecords.add(acquiredRecords(10L, 10).get(0));

        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(records)
            .setAcquiredRecords(acquiredRecords);

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        Deserializers<String, String> deserializers = newStringDeserializers();

        // Fetch should fail with CRC error
        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 20, true);
        assertNotNull(batch.getException());

        // Only the acquired records (10-19) should be rejected
        Acknowledgements acks = batch.getAcknowledgements();
        assertEquals(10, acks.size());
        for (long offset = 10; offset < 20; offset++) {
            assertEquals(AcknowledgeType.REJECT, acks.get(offset));
        }
    }

    @Test
    public void testCorruptedRecordsWithCrcCheckDisabled() {
        // Create corrupted batch
        Records records = newSingleCorruptedBatch(0L, 10);

        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>();
        acquiredRecords.add(acquiredRecords(0L, 10).get(0));

        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(records)
            .setAcquiredRecords(acquiredRecords);

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        Deserializers<String, String> deserializers = newStringDeserializers();

        // Fetch with CRC check disabled - should process records despite corruption
        ShareInFlightBatch<String, String> batch = completedFetch.fetchRecords(deserializers, 10, false);

        // Should get records (they're corrupted, but we're not checking)
        List<ConsumerRecord<String, String>> fetchedRecords = batch.getInFlightRecords();
        assertEquals(10, fetchedRecords.size(), "Should get all records when CRC check is disabled");
    }

    @Test
    public void testCrcFailureAfterPartialFetch() {
        // Create 2 batches: good batch (offsets 0-4), corrupted batch (5-14)
        // Using smaller first batch so fetching with maxRecords = 10 will need to load the second batch
        Records records = newRecordsWithCorruptedSecondBatchSmallFirst();

        // Acquire all 15 records
        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>();
        acquiredRecords.add(acquiredRecords(0L, 15).get(0));

        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(records)
            .setAcquiredRecords(acquiredRecords);

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        Deserializers<String, String> deserializers = newStringDeserializers();

        // First fetch with maxRecords=10 should return 5 records from first batch,
        // then attempt to load second batch which is corrupted
        ShareInFlightBatch<String, String> batch1 = completedFetch.fetchRecords(deserializers, 10, true);
        List<ConsumerRecord<String, String>> records1 = batch1.getInFlightRecords();
        assertEquals(5, records1.size(), "Should get 5 records from first batch before hitting CRC error");
        assertEquals(0L, records1.get(0).offset());
        assertEquals(4L, records1.get(4).offset());
        assertTrue(batch1.hasCachedException(), "Should indicate there's a cached exception");

        // Second fetch should return the cached CRC exception for the second batch
        ShareInFlightBatch<String, String> batch2 = completedFetch.fetchRecords(deserializers, 10, true);
        assertNotNull(batch2.getException(), "Should have cached exception");
        assertEquals(CorruptRecordException.class, batch2.getException().cause().getClass());

        // Verify all 10 records from second batch are rejected
        Acknowledgements acks2 = batch2.getAcknowledgements();
        assertEquals(10, acks2.size());
        for (long offset = 5; offset < 15; offset++) {
            assertEquals(AcknowledgeType.REJECT, acks2.get(offset));
        }

        // No more records
        ShareInFlightBatch<String, String> batch3 = completedFetch.fetchRecords(deserializers, 10, true);
        assertEquals(0, batch3.getInFlightRecords().size());
    }

    @Test
    public void testMultipleConsecutiveCrcFailures() {
        // Create 4 batches: corrupted, corrupted, good, good
        Records records = newRecordsWithMultipleCorruptedBatches();

        // Acquire all records (0-39)
        List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = new ArrayList<>();
        acquiredRecords.add(acquiredRecords(0L, 40).get(0));

        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
            .setRecords(records)
            .setAcquiredRecords(acquiredRecords);

        ShareCompletedFetch completedFetch = newShareCompletedFetch(partitionData);
        Deserializers<String, String> deserializers = newStringDeserializers();

        // First corrupted batch (0-9)
        ShareInFlightBatch<String, String> batch1 = completedFetch.fetchRecords(deserializers, 10, true);
        assertNotNull(batch1.getException(), "First batch should have CRC exception");
        Acknowledgements acks1 = batch1.getAcknowledgements();
        assertEquals(10, acks1.size(), "First corrupted batch should reject 10 records");

        // Second corrupted batch (10-19)
        ShareInFlightBatch<String, String> batch2 = completedFetch.fetchRecords(deserializers, 10, true);
        assertNotNull(batch2.getException(), "Second batch should have CRC exception");
        Acknowledgements acks2 = batch2.getAcknowledgements();
        assertEquals(10, acks2.size(), "Second corrupted batch should reject 10 records");

        // Third batch should be good (20-29)
        ShareInFlightBatch<String, String> batch3 = completedFetch.fetchRecords(deserializers, 10, true);
        assertEquals(10, batch3.getInFlightRecords().size(), "Third batch should return 10 good records");
        assertEquals(20L, batch3.getInFlightRecords().get(0).offset());

        // Fourth batch should be good (30-39)
        ShareInFlightBatch<String, String> batch4 = completedFetch.fetchRecords(deserializers, 10, true);
        assertEquals(10, batch4.getInFlightRecords().size(), "Fourth batch should return 10 good records");
        assertEquals(30L, batch4.getInFlightRecords().get(0).offset());
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
            0,
            TIP,
            partitionData,
            DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS,
            shareFetchMetricsAggregator,
            ApiKeys.SHARE_FETCH.latestVersion());
    }

    private static Deserializers<UUID, UUID> newUuidDeserializers() {
        return new Deserializers<>(new UUIDDeserializer(), new UUIDDeserializer(), null);
    }

    private static Deserializers<String, String> newStringDeserializers() {
        return new Deserializers<>(new StringDeserializer(), new StringDeserializer(), null);
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
        return List.of(acquiredRecords);
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

    private void createBatch(ByteBuffer buffer, long baseOffset, int numRecords, Time time) {
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                baseOffset,
                time.milliseconds(),
                PRODUCER_ID,
                PRODUCER_EPOCH,
                0,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH)) {
            for (int i = 0; i < numRecords; i++) {
                builder.append(new SimpleRecord(time.milliseconds(), "key".getBytes(), ("value-" + (baseOffset + i)).getBytes()));
            }
            builder.build();
        }
    }

    private void corruptBatchCrc(ByteBuffer buffer, int batchStartPosition) {
        int currentPosition = buffer.position();

        // CRC is at offset 17 in the record batch (after base offset, batch length, partition leader epoch, and magic)
        // For v2 records: [baseOffset(8) | batchLength(4) | partitionLeaderEpoch(4) | magic(1) | crc(4) | ...]
        int crcPosition = batchStartPosition + 17;

        buffer.putInt(crcPosition, 0xDEADBEEF);

        buffer.position(currentPosition);
    }

    private Records newRecordsWithCorruptedFirstBatch() {
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        // First batch (corrupted) - offsets 0-9
        int pos1 = buffer.position();
        createBatch(buffer, 0L, 10, time);
        corruptBatchCrc(buffer, pos1);

        // Second batch (good) - offsets 10-19
        createBatch(buffer, 10L, 10, time);

        // Third batch (good) - offsets 20-29
        createBatch(buffer, 20L, 10, time);

        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private Records newRecordsWithCorruptedSecondBatchSmallFirst() {
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        // First batch (good) - offsets 0-4
        createBatch(buffer, 0L, 5, time);

        // Second batch (corrupted) - offsets 5-14
        int pos = buffer.position();
        createBatch(buffer, 5L, 10, time);
        corruptBatchCrc(buffer, pos);

        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private Records newSingleCorruptedBatch(long baseOffset, int numRecords) {
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        int pos = buffer.position();
        createBatch(buffer, baseOffset, numRecords, time);
        corruptBatchCrc(buffer, pos);

        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private Records newRecordsWithMultipleCorruptedBatches() {
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(8192);

        // First batch (corrupted) - offsets 0-9
        int pos1 = buffer.position();
        createBatch(buffer, 0L, 10, time);
        corruptBatchCrc(buffer, pos1);

        // Second batch (corrupted) - offsets 10-19
        int pos2 = buffer.position();
        createBatch(buffer, 10L, 10, time);
        corruptBatchCrc(buffer, pos2);

        // Third batch (good) - offsets 20-29
        createBatch(buffer, 20L, 10, time);

        // Fourth batch (good) - offsets 30-39
        createBatch(buffer, 30L, 10, time);

        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }
}
