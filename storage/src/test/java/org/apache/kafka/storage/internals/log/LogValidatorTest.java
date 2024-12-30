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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.LogValidator.ValidationResult;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogValidatorTest {
    private final Time time = Time.SYSTEM;
    private final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private final MetricsRecorder metricsRecorder = new MetricsRecorder();

    static class MetricsRecorder implements LogValidator.MetricsRecorder {
        public int recordInvalidMagicCount = 0;
        public int recordInvalidOffsetCount = 0;
        public int recordInvalidChecksumsCount = 0;
        public int recordInvalidSequenceCount = 0;
        public int recordNoKeyCompactedTopicCount = 0;

        public void recordInvalidMagic() {
            recordInvalidMagicCount += 1;
        }

        public void recordInvalidOffset() {
            recordInvalidOffsetCount += 1;
        }

        public void recordInvalidSequence() {
            recordInvalidSequenceCount += 1;
        }

        public void recordInvalidChecksums() {
            recordInvalidChecksumsCount += 1;
        }

        public void recordNoKeyCompactedTopic() {
            recordNoKeyCompactedTopicCount += 1;
        }
    }

    @Test
    public void testValidationOfBatchesWithNonSequentialInnerOffsets() {
        Arrays.stream(RecordVersion.values()).forEach(version -> {
            int numRecords = 20;
            Compression compression = Compression.gzip().build();
            MemoryRecords invalidRecords = recordsWithNonSequentialInnerOffsets(version.value, compression, numRecords);

            // Validation for v2 and above is strict for this case. For older formats, we fix invalid
            // internal offsets by rewriting the batch.
            if (version.value >= RecordBatch.MAGIC_VALUE_V2) {
                assertThrows(InvalidRecordException.class, () ->
                        validateMessages(invalidRecords, version.value, CompressionType.GZIP, compression)
                );
            } else {
                ValidationResult result = validateMessages(invalidRecords, version.value, CompressionType.GZIP, compression);
                List<Long> recordsResult = new ArrayList<>();
                result.validatedRecords.records().forEach(s -> recordsResult.add(s.offset()));
                assertEquals(LongStream.range(0, numRecords).boxed().collect(Collectors.toList()), recordsResult);
            }
        });
    }

    @ParameterizedTest
    @CsvSource({
        "0,gzip,none", "1,gzip,none", "2,gzip,none",
        "0,gzip,gzip", "1,gzip,gzip", "2,gzip,gzip",
        "0,snappy,gzip", "1,snappy,gzip", "2,snappy,gzip",
        "0,lz4,gzip", "1,lz4,gzip", "2,lz4,gzip",
        "2,none,none", "2,none,gzip",
        "2,zstd,gzip",
    })
    public void checkOnlyOneBatch(Byte magic, String sourceCompression,
                                   String targetCompression) {
        assertThrows(InvalidRecordException.class,
                () -> validateMessages(createTwoBatchedRecords(magic, Compression.of(sourceCompression).build()),
                        magic, CompressionType.forName(sourceCompression), Compression.of(targetCompression).build())
        );
    }


    private static Stream<Arguments> testAllCompression() {
        return Arrays.stream(CompressionType.values()).flatMap(source ->
                        Arrays.stream(CompressionType.values()).map(target ->
                                Arguments.of(source.name, target.name)));
    }

    @ParameterizedTest
    @MethodSource("testAllCompression")
    public void testBatchWithoutRecordsNotAllowed(String sourceCompressionName, String targetCompressionName) {
        long offset = 1234567;
        long producerId = 1324L;
        short producerEpoch = 10;
        int baseSequence = 984;
        boolean isTransactional = true;
        int partitionLeaderEpoch = 40;
        CompressionType sourceCompression = CompressionType.forName(sourceCompressionName);
        Compression targetCompression = Compression.of(targetCompressionName).build();


        ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.CURRENT_MAGIC_VALUE, producerId, producerEpoch,
                baseSequence, 0L, 5L, partitionLeaderEpoch, TimestampType.CREATE_TIME, System.currentTimeMillis(),
                isTransactional, false);
        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        assertThrows(InvalidRecordException.class, () -> new LogValidator(records,
                topicPartition,
                time,
                sourceCompression,
                targetCompression,
                false,
                RecordBatch.CURRENT_MAGIC_VALUE,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ));
    }

    @ParameterizedTest
    @CsvSource({"0,1,gzip", "1,0,gzip"})
    public void checkMismatchMagic(byte batchMagic, byte recordMagic, String compressionName) {
        Compression compression = Compression.of(compressionName).build();
        assertThrows(RecordValidationException.class,
                () -> validateMessages(recordsWithInvalidInnerMagic(batchMagic, recordMagic, compression
                        ), batchMagic, compression.type(), compression));

        assertTrue(metricsRecorder.recordInvalidMagicCount > 0);
    }
    @Test
    public void testCreateTimeUpConversionV1ToV2() {
        long timestamp = System.currentTimeMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V1, timestamp, compression);

        LogValidator validator = new LogValidator(
                records,
                topicPartition,
                time,
                CompressionType.GZIP,
                compression,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        );

        LogValidator.ValidationResult validatedResults = validator.validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        MemoryRecords validatedRecords = validatedResults.validatedRecords;

        for (RecordBatch batch : validatedRecords.batches()) {
            assertTrue(batch.isValid());
            maybeCheckBaseTimestamp(timestamp, batch);
            assertEquals(timestamp, batch.maxTimestamp());
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch());
            assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId());
            assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
        }

        assertEquals(timestamp, validatedResults.maxTimestampMs);
        assertEquals(2, validatedResults.shallowOffsetOfMaxTimestamp, "Offset of max timestamp should be the last offset 2.");
        assertTrue(validatedResults.messageSizeMaybeChanged, "Message size should have been changed");

        verifyRecordValidationStats(
                validatedResults.recordValidationStats,
                3,
                records,
                true
        );
    }

    @ParameterizedTest
    @CsvSource({"1", "2"})
    public void checkCreateTimeUpConversionFromV0(byte toMagic) {
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V0, RecordBatch.NO_TIMESTAMP, compression);
        LogValidator logValidator = new LogValidator(records,
                topicPartition,
                time,
                CompressionType.GZIP,
                compression,
                false,
                toMagic,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        );
        LogValidator.ValidationResult validatedResults = logValidator.validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );
        MemoryRecords validatedRecords = validatedResults.validatedRecords;

        for (RecordBatch batch : validatedRecords.batches()) {
            assertTrue(batch.isValid());
            maybeCheckBaseTimestamp(RecordBatch.NO_TIMESTAMP, batch);
            assertEquals(RecordBatch.NO_TIMESTAMP, batch.maxTimestamp());
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch());
            assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId());
            assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
        }
        assertEquals(RecordBatch.NO_TIMESTAMP, validatedResults.maxTimestampMs,
                "Max timestamp should be " + RecordBatch.NO_TIMESTAMP);
        assertEquals(-1, validatedResults.shallowOffsetOfMaxTimestamp);
        assertTrue(validatedResults.messageSizeMaybeChanged, "Message size should have been changed");

        verifyRecordValidationStats(validatedResults.recordValidationStats, 3, records, true);
    }

    @ParameterizedTest
    @CsvSource({"1", "2"})
    public void checkRecompression(byte magic) {
        long now = System.currentTimeMillis();
        // Set the timestamp of seq(1) (i.e. offset 1) as the max timestamp
        List<Long> timestampSeq = Arrays.asList(now - 1, now + 1, now);

        long producerId;
        short producerEpoch;
        int baseSequence;
        boolean isTransactional;
        int partitionLeaderEpoch;

        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            producerId = 1324L;
            producerEpoch = 10;
            baseSequence = 984;
            isTransactional = true;
            partitionLeaderEpoch = 40;
        } else {
            producerId = RecordBatch.NO_PRODUCER_ID;
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;
            baseSequence = RecordBatch.NO_SEQUENCE;
            isTransactional = false;
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
        }

        MemoryRecords records = MemoryRecords.withRecords(
                magic,
                0L,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                producerId,
                producerEpoch,
                baseSequence,
                partitionLeaderEpoch,
                isTransactional,
                new SimpleRecord(timestampSeq.get(0), "hello".getBytes()),
                new SimpleRecord(timestampSeq.get(1), "there".getBytes()),
                new SimpleRecord(timestampSeq.get(2), "beautiful".getBytes())
        );

        // V2 has single batch, and other versions have many single-record batches
        assertEquals(magic >= RecordBatch.MAGIC_VALUE_V2 ? 1 : 3, iteratorSize(records.batches().iterator()));

        LogValidator.ValidationResult validatingResults = new LogValidator(
                records,
                topicPartition,
                time,
                CompressionType.NONE,
                Compression.gzip().build(),
                false,
                magic,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                partitionLeaderEpoch,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0L),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        MemoryRecords validatedRecords = validatingResults.validatedRecords;

        int i = 0;
        for (RecordBatch batch : validatedRecords.batches()) {
            assertTrue(batch.isValid());
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            maybeCheckBaseTimestamp(timestampSeq.get(0), batch);
            assertEquals(batch.maxTimestamp(), batch.maxTimestamp());
            assertEquals(producerEpoch, batch.producerEpoch());
            assertEquals(producerId, batch.producerId());
            assertEquals(baseSequence, batch.baseSequence());
            assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());

            for (Record record : batch) {
                record.ensureValid();
                assertEquals((long) timestampSeq.get(i), record.timestamp());
                i++;
            }
        }

        assertEquals(now + 1, validatingResults.maxTimestampMs,
                "Max timestamp should be " + (now + 1));

        // Both V2 and V1 have single batch in the validated records when compression is enabled, and hence their shallow
        // OffsetOfMaxTimestamp is the last offset of the single batch
        assertEquals(1, iteratorSize(validatedRecords.batches().iterator()));
        assertEquals(2, validatingResults.shallowOffsetOfMaxTimestamp);
        assertTrue(validatingResults.messageSizeMaybeChanged,
                "Message size should have been changed");

        verifyRecordValidationStats(validatingResults.recordValidationStats, 3, records, true);
    }

    private MemoryRecords recordsWithInvalidInnerMagic(byte batchMagicValue, byte recordMagicValue, Compression codec) {
        List<LegacyRecord> records = new ArrayList<>();

        for (int id = 0; id < 20; id++) {
            records.add(LegacyRecord.create(
                    recordMagicValue,
                    RecordBatch.NO_TIMESTAMP,
                    Integer.toString(id).getBytes(),
                    Integer.toString(id).getBytes()
            ));
        }

        ByteBuffer buffer = ByteBuffer.allocate(Math.min(Math.max(
                records.stream().mapToInt(LegacyRecord::sizeInBytes).sum() / 2, 1024), 1 << 16));
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, batchMagicValue, codec,
                TimestampType.CREATE_TIME, 0L);

        AtomicLong offset = new AtomicLong(1234567);
        records.forEach(record -> {
            builder.appendUncheckedWithOffset(offset.get(), record);
            offset.incrementAndGet();
        });

        return builder.build();
    }

    private MemoryRecords recordsWithNonSequentialInnerOffsets(Byte magicValue, Compression compression, int numRecords) {
        List<SimpleRecord> records = IntStream.range(0, numRecords)
                .mapToObj(id -> new SimpleRecord(String.valueOf(id).getBytes()))
                .collect(Collectors.toList());

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magicValue, compression, TimestampType.CREATE_TIME, 0L);

        records.forEach(record ->
                assertDoesNotThrow(() -> builder.appendUncheckedWithOffset(0, record))
        );

        return builder.build();
    }

    @ParameterizedTest
    @CsvSource({"0,none,none", "1,none,none", "0,none,gzip", "1,none,gzip"})
    public void checkAllowMultiBatch(Byte magic, String sourceCompression, String targetCompression) {
        validateMessages(createTwoBatchedRecords(magic,  Compression.of(sourceCompression).build()), magic,
                CompressionType.forName(sourceCompression), Compression.of(targetCompression).build());
    }


    private ValidationResult validateMessages(MemoryRecords records,
                                              Byte magic,
                                              CompressionType sourceCompressionType,
                                              Compression targetCompressionType) {
        MockTime mockTime = new MockTime(0L, 0L);
        return new LogValidator(records,
                topicPartition,
                mockTime,
                sourceCompressionType,
                targetCompressionType,
                false,
                magic,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PRODUCER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.IBP_2_3_IV1
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier());
    }

    private MemoryRecords createTwoBatchedRecords(Byte magicValue,
                                                  Compression codec) {
        ByteBuffer buf = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, "1".getBytes(), "a".getBytes());
        builder.close();
        builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "2".getBytes(), "b".getBytes());
        builder.append(12L, "3".getBytes(), "c".getBytes());
        builder.close();

        buf.flip();
        return MemoryRecords.readableRecords(buf.slice());
    }

    private MemoryRecords createRecords(byte magicValue,
                                        long timestamp,
                                        Compression codec) {
        List<byte[]> records = Arrays.asList("hello".getBytes(), "there".getBytes(), "beautiful".getBytes());
        return createRecords(records, magicValue, timestamp, codec);
    }

    @ParameterizedTest
    @CsvSource({"1", "2"})
    public void checkCompressed(byte magic) {
        long now = System.currentTimeMillis();
        // set the timestamp of seq(1) (i.e. offset 1) as the max timestamp
        List<Long> timestampSeq = Arrays.asList(now - 1, now + 1, now);

        long producerId;
        short producerEpoch;
        int baseSequence;
        boolean isTransactional;
        int partitionLeaderEpoch;

        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            producerId = 1324L;
            producerEpoch = 10;
            baseSequence = 984;
            isTransactional = true;
            partitionLeaderEpoch = 40;
        } else {
            producerId = RecordBatch.NO_PRODUCER_ID;
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;
            baseSequence = RecordBatch.NO_SEQUENCE;
            isTransactional = false;
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
        }

        List<SimpleRecord> recordList = Arrays.asList(
                new SimpleRecord(timestampSeq.get(0), "hello".getBytes()),
                new SimpleRecord(timestampSeq.get(1), "there".getBytes()),
                new SimpleRecord(timestampSeq.get(2), "beautiful".getBytes())
        );

        MemoryRecords records = MemoryRecords.withRecords(
                magic,
                0L,
                Compression.gzip().build(),
                TimestampType.CREATE_TIME,
                producerId,
                producerEpoch,
                baseSequence,
                partitionLeaderEpoch,
                isTransactional,
                recordList.toArray(new SimpleRecord[0])
        );

        LogValidator validator = new LogValidator(
                records,
                topicPartition,
                time,
                CompressionType.GZIP,
                Compression.gzip().build(),
                false,
                magic,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                partitionLeaderEpoch,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        );

        LogValidator.ValidationResult validatedResults = validator.validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        MemoryRecords validatedRecords = validatedResults.validatedRecords;

        int i = 0;
        for (RecordBatch batch : validatedRecords.batches()) {
            assertTrue(batch.isValid());
            assertEquals(batch.timestampType(), TimestampType.CREATE_TIME);
            maybeCheckBaseTimestamp(timestampSeq.get(0), batch);
            assertEquals(batch.maxTimestamp(), batch.maxTimestamp());
            assertEquals(producerEpoch, batch.producerEpoch());
            assertEquals(producerId, batch.producerId());
            assertEquals(baseSequence, batch.baseSequence());
            assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());

            for (Record record : batch) {
                record.ensureValid();
                assertEquals((long) timestampSeq.get(i), record.timestamp());
                i++;
            }
        }

        assertEquals(now + 1, validatedResults.maxTimestampMs, "Max timestamp should be " + (now + 1));

        int expectedShallowOffsetOfMaxTimestamp = 2;
        assertEquals(expectedShallowOffsetOfMaxTimestamp, validatedResults.shallowOffsetOfMaxTimestamp, "Shallow offset of max timestamp should be 2");
        assertFalse(validatedResults.messageSizeMaybeChanged, "Message size should not have been changed");

        verifyRecordValidationStats(validatedResults.recordValidationStats, 0, records, true);
    }

    private MemoryRecords createRecords(List<byte[]> records,
                                        byte magicValue,
                                        long timestamp,
                                        Compression codec) {
        ByteBuffer buf = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L);

        AtomicInteger offset = new AtomicInteger(0);
        records.forEach(item ->
                builder.appendWithOffset(offset.getAndIncrement(), timestamp, null, item));
        return builder.build();
    }

    @Test
    void testRecordBatchWithCountOverrides() {
        // The batch to be written contains 3 records, so the correct lastOffsetDelta is 2
        validateRecordBatchWithCountOverrides(2, 3);
    }

    @ParameterizedTest
    @CsvSource({"0,3", "15,3", "-3,3"})
    void testInconsistentCountAndOffset(int lastOffsetDelta, int count) {
        // Count and offset range are inconsistent or invalid
        assertInvalidBatchCountOverrides(lastOffsetDelta, count);
    }

    @ParameterizedTest
    @CsvSource({"5,6", "1,2"})
    void testUnmatchedNumberOfRecords(int lastOffsetDelta, int count) {
        // Count and offset range are consistent, but do not match the actual number of records
        assertInvalidBatchCountOverrides(lastOffsetDelta, count);
    }

    @Test
    void testInvalidCreateTimeNonCompressedV1() {
        long now = System.currentTimeMillis();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V1, now - 1001L,
                Compression.NONE);
        assertThrows(RecordValidationException.class, () -> new LogValidator(
                records,
                topicPartition,
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.MAGIC_VALUE_V1,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ));
    }

    @Test
    public void testInvalidCreateTimeCompressedV1() {
        long now = System.currentTimeMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(
                RecordBatch.MAGIC_VALUE_V1,
                now - 1001L,
                compression
        );

        assertThrows(RecordValidationException.class, () ->
                new LogValidator(
                        records,
                        new TopicPartition("topic", 0),
                        time,
                        CompressionType.GZIP,
                        compression,
                        false,
                        RecordBatch.MAGIC_VALUE_V1,
                        TimestampType.CREATE_TIME,
                        1000L,
                        1000L,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        AppendOrigin.CLIENT,
                        MetadataVersion.latestTesting()
                ).validateMessagesAndAssignOffsets(
                        PrimitiveRef.ofLong(0),
                        metricsRecorder,
                        RequestLocal.withThreadConfinedCaching().bufferSupplier()
                )
        );
    }

    @Test
    public void testInvalidCreateTimeNonCompressedV2() {
        long now = System.currentTimeMillis();
        MemoryRecords records = createRecords(
                RecordBatch.MAGIC_VALUE_V2,
                now - 1001L,
                Compression.NONE
        );

        assertThrows(RecordValidationException.class, () ->
                new LogValidator(
                        records,
                        new TopicPartition("topic", 0),
                        time,
                        CompressionType.NONE,
                        Compression.NONE,
                        false,
                        RecordBatch.MAGIC_VALUE_V2,
                        TimestampType.CREATE_TIME,
                        1000L,
                        1000L,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        AppendOrigin.CLIENT,
                        MetadataVersion.latestTesting()
                ).validateMessagesAndAssignOffsets(
                        PrimitiveRef.ofLong(0),
                        metricsRecorder,
                        RequestLocal.withThreadConfinedCaching().bufferSupplier()
                )
        );
    }

    @ParameterizedTest
    @CsvSource({
        "0,gzip,gzip", "1,gzip,gzip",
        "0,lz4,lz4", "1,lz4,lz4",
        "0,snappy,snappy", "1,snappy,snappy",
    })
    public void checkInvalidChecksum(byte magic, String compressionName, String typeName) {
        Compression compression = Compression.of(compressionName).build();
        CompressionType type = CompressionType.forName(typeName);

        LegacyRecord record = LegacyRecord.create(magic, 0L, null, "hello".getBytes());
        ByteBuffer buf = record.buffer();

        // enforce modify crc to make checksum error
        buf.put(LegacyRecord.CRC_OFFSET, (byte) 0);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compression,
                TimestampType.CREATE_TIME, 0L);
        builder.appendUncheckedWithOffset(0, record);

        MemoryRecords memoryRecords = builder.build();
        LogValidator logValidator = new LogValidator(memoryRecords,
                topicPartition,
                time,
                type,
                compression,
                false,
                magic,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        );


        assertThrows(CorruptRecordException.class, () -> logValidator.validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ));

        assertTrue(metricsRecorder.recordInvalidChecksumsCount > 0);
    }

    private static Stream<Arguments> testInvalidSequenceArguments() {
        return Stream.of(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)
                .flatMap(magicValue -> Arrays.stream(CompressionType.values()).flatMap(source ->
                        Arrays.stream(CompressionType.values()).map(target ->
                                Arguments.of(magicValue, source.name, target.name))));
    }

    @ParameterizedTest
    @MethodSource("testInvalidSequenceArguments")
    public void checkInvalidSequence(byte magic, String compressionName, String typeName) {
        long producerId = 1234;
        short producerEpoch = 0;
        int baseSequence = 0;
        Compression compression = Compression.of(compressionName).build();
        CompressionType type = CompressionType.forName(typeName);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, compression,
                 0L, producerId, producerEpoch, baseSequence, false);
        builder.append(new SimpleRecord("hello".getBytes()));

        MemoryRecords memoryRecords = builder.build();
        ByteBuffer buf = memoryRecords.buffer();

        // overwrite baseSequence to make InvalidSequence
        // BASE_SEQUENCE_OFFSET is defined in DefaultRecordBatch and it is private
        // so we write this number directly.
        buf.putInt(53, -2);

        LogValidator logValidator = new LogValidator(memoryRecords,
                topicPartition,
                time,
                type,
                compression,
                false,
                magic,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        );


        assertThrows(InvalidRecordException.class, () -> logValidator.validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ));

        assertTrue(metricsRecorder.recordInvalidSequenceCount > 0);
    }

    @ParameterizedTest
    @CsvSource({
        "0,gzip,gzip", "1,gzip,gzip", "2,gzip,gzip",
        "0,lz4,lz4", "1,lz4,lz4", "2,lz4,lz4",
        "0,snappy,snappy", "1,snappy,snappy", "2,snappy,snappy",
        "2,zstd,zstd"
    })
    public void checkNoKeyCompactedTopic(byte magic, String compressionName, String typeName) {
        Compression codec = Compression.of(compressionName).build();
        CompressionType type = CompressionType.forName(typeName);

        MemoryRecords records = createRecords(magic, RecordBatch.NO_TIMESTAMP, codec);
        Assertions.assertThrows(RecordValidationException.class, () -> new LogValidator(
                records,
                topicPartition,
                time,
                type,
                codec,
                true,
                magic,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ));

        assertTrue(metricsRecorder.recordNoKeyCompactedTopicCount > 0);
    }

    @Test
    public void testInvalidCreateTimeCompressedV2() {
        long now = System.currentTimeMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(
                RecordBatch.MAGIC_VALUE_V2,
                now - 1001L,
                compression
        );

        assertThrows(RecordValidationException.class, () ->
                new LogValidator(
                        records,
                        new TopicPartition("topic", 0),
                        time,
                        CompressionType.GZIP,
                        compression,
                        false,
                        RecordBatch.MAGIC_VALUE_V2,
                        TimestampType.CREATE_TIME,
                        1000L,
                        1000L,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        AppendOrigin.CLIENT,
                        MetadataVersion.latestTesting()
                ).validateMessagesAndAssignOffsets(
                        PrimitiveRef.ofLong(0),
                        metricsRecorder,
                        RequestLocal.withThreadConfinedCaching().bufferSupplier()
                )
        );
    }

    @Test
    public void testAbsoluteOffsetAssignmentNonCompressed() {
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V0, RecordBatch.NO_TIMESTAMP, Compression.NONE);
        long offset = 1234567;

        checkOffsets(records, 0);

        checkOffsets(
                new LogValidator(
                        records,
                        topicPartition,
                        time,
                        CompressionType.NONE,
                        Compression.NONE,
                        false,
                        RecordBatch.MAGIC_VALUE_V0,
                        TimestampType.CREATE_TIME,
                        1000L,
                        1000L,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        AppendOrigin.CLIENT,
                        MetadataVersion.latestTesting()
                ).validateMessagesAndAssignOffsets(
                        PrimitiveRef.ofLong(offset),
                        metricsRecorder,
                        RequestLocal.withThreadConfinedCaching().bufferSupplier()
                ).validatedRecords, offset
        );
    }


    @Test
    public void testAbsoluteOffsetAssignmentCompressed() {
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V0, RecordBatch.NO_TIMESTAMP, compression);
        long offset = 1234567;

        checkOffsets(records, 0);

        checkOffsets(
                new LogValidator(
                        records,
                        new TopicPartition("topic", 0),
                        time,
                        CompressionType.GZIP,
                        compression,
                        false,
                        RecordBatch.MAGIC_VALUE_V0,
                        TimestampType.CREATE_TIME,
                        1000L,
                        1000L,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        AppendOrigin.CLIENT,
                        MetadataVersion.latestTesting()
                ).validateMessagesAndAssignOffsets(
                        PrimitiveRef.ofLong(offset),
                        metricsRecorder,
                        RequestLocal.withThreadConfinedCaching().bufferSupplier()
                ).validatedRecords,
                offset
        );
    }

    @Test
    public void testRelativeOffsetAssignmentNonCompressedV1() {
        long now = System.currentTimeMillis();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, Compression.NONE);
        long offset = 1234567;

        checkOffsets(records, 0);

        MemoryRecords messageWithOffset = new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.MAGIC_VALUE_V1,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords;

        checkOffsets(messageWithOffset, offset);
    }

    @Test
    public void testRelativeOffsetAssignmentNonCompressedV2() {
        long now = System.currentTimeMillis();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, Compression.NONE);
        long offset = 1234567;

        checkOffsets(records, 0);

        MemoryRecords messageWithOffset = new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords;

        checkOffsets(messageWithOffset, offset);
    }

    @Test
    public void testRelativeOffsetAssignmentCompressedV1() {
        long now = System.currentTimeMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, compression);
        long offset = 1234567;

        checkOffsets(records, 0);

        MemoryRecords compressedMessagesWithOffset = new LogValidator(
                records,
                topicPartition,
                time,
                CompressionType.GZIP,
                compression,
                false,
                RecordBatch.MAGIC_VALUE_V1,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords;

        checkOffsets(compressedMessagesWithOffset, offset);
    }

    @Test
    public void testRelativeOffsetAssignmentCompressedV2() {
        long now = System.currentTimeMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, compression);
        long offset = 1234567;

        checkOffsets(records, 0);

        MemoryRecords compressedMessagesWithOffset = new LogValidator(
                records,
                topicPartition,
                time,
                CompressionType.GZIP,
                compression,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords;

        checkOffsets(compressedMessagesWithOffset, offset);
    }

    @Test
    public void testOffsetAssignmentAfterUpConversionV0ToV1NonCompressed() {
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V0, RecordBatch.NO_TIMESTAMP, Compression.NONE);
        checkOffsets(records, 0);

        long offset = 1234567;

        LogValidator.ValidationResult validatedResults = new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.MAGIC_VALUE_V1,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        checkOffsets(validatedResults.validatedRecords, offset);
        verifyRecordValidationStats(
                validatedResults.recordValidationStats,
                3, // numConvertedRecords
                records,
                false // compressed
        );
    }

    @Test
    public void testOffsetAssignmentAfterUpConversionV0ToV2NonCompressed() {
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V0, RecordBatch.NO_TIMESTAMP, Compression.NONE);
        checkOffsets(records, 0);

        long offset = 1234567;

        LogValidator.ValidationResult validatedResults = new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        checkOffsets(validatedResults.validatedRecords, offset);
        verifyRecordValidationStats(
                validatedResults.recordValidationStats,
                3, // numConvertedRecords
                records,
                false // compressed
        );
    }

    @Test
    public void testOffsetAssignmentAfterUpConversionV0ToV1Compressed() {
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V0, RecordBatch.NO_TIMESTAMP, compression);
        checkOffsets(records, 0);

        long offset = 1234567;

        LogValidator.ValidationResult validatedResults = new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.GZIP,
                compression,
                false,
                RecordBatch.MAGIC_VALUE_V1,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        checkOffsets(validatedResults.validatedRecords, offset);
        verifyRecordValidationStats(
                validatedResults.recordValidationStats,
                3, // numConvertedRecords
                records,
                true // compressed
        );
    }

    @Test
    public void testOffsetAssignmentAfterUpConversionV0ToV2Compressed() {
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V0, RecordBatch.NO_TIMESTAMP, compression);
        checkOffsets(records, 0);

        long offset = 1234567;

        LogValidator.ValidationResult validatedResults = new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.GZIP,
                compression,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        checkOffsets(validatedResults.validatedRecords, offset);
        verifyRecordValidationStats(
                validatedResults.recordValidationStats,
                3, // numConvertedRecords
                records,
                true // compressed
        );
    }

    @Test
    public void testControlRecordsNotAllowedFromClients() {
        long offset = 1234567;
        EndTransactionMarker endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, 0);
        MemoryRecords records = MemoryRecords.withEndTransactionMarker(23423L, (short) 5, endTxnMarker);
        assertThrows(InvalidRecordException.class, () -> new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.CURRENT_MAGIC_VALUE,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ));
    }

    @Test
    public void testControlRecordsNotCompressed() {
        long offset = 1234567;
        EndTransactionMarker endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, 0);
        MemoryRecords records = MemoryRecords.withEndTransactionMarker(23423L, (short) 5, endTxnMarker);
        LogValidator.ValidationResult result = new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.snappy().build(),
                false,
                RecordBatch.CURRENT_MAGIC_VALUE,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.COORDINATOR,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );
        MemoryRecords validatedRecords = result.validatedRecords;
        assertEquals(1, TestUtils.toList(validatedRecords.batches()).size());
        assertFalse(TestUtils.toList(validatedRecords.batches()).get(0).isCompressed());
    }

    @Test
    public void testOffsetAssignmentAfterDownConversionV1ToV0NonCompressed() {
        long offset = 1234567;
        long now = System.currentTimeMillis();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, Compression.NONE);
        checkOffsets(records, 0);
        checkOffsets(new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.MAGIC_VALUE_V0,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords, offset);
    }

    @Test
    public void testOffsetAssignmentAfterDownConversionV1ToV0Compressed() {
        long offset = 1234567;
        long now = System.currentTimeMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, compression);
        checkOffsets(records, 0);
        checkOffsets(new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.GZIP,
                compression,
                false,
                RecordBatch.MAGIC_VALUE_V0,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords, offset);
    }

    @Test
    public void testOffsetAssignmentAfterUpConversionV1ToV2NonCompressed() {
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V1, RecordBatch.NO_TIMESTAMP, Compression.NONE);
        checkOffsets(records, 0);
        long offset = 1234567;
        checkOffsets(new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords, offset);
    }

    @Test
    public void testOffsetAssignmentAfterUpConversionV1ToV2Compressed() {
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V1, RecordBatch.NO_TIMESTAMP, compression);
        long offset = 1234567;
        checkOffsets(records, 0);
        checkOffsets(new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.GZIP,
                compression,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords, offset);
    }

    @Test
    public void testOffsetAssignmentAfterDownConversionV2ToV1NonCompressed() {
        long offset = 1234567;
        long now = System.currentTimeMillis();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, Compression.NONE);
        checkOffsets(records, 0);
        checkOffsets(new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.MAGIC_VALUE_V1,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords, offset);
    }

    @Test
    public void testOffsetAssignmentAfterDownConversionV2ToV1Compressed() {
        long offset = 1234567;
        long now = System.currentTimeMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, compression);
        checkOffsets(records, 0);
        checkOffsets(new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.GZIP,
                compression,
                false,
                RecordBatch.MAGIC_VALUE_V1,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords, offset);
    }

    @Test
    public void testDownConversionOfTransactionalRecordsNotPermitted() {
        long offset = 1234567;
        long producerId = 1344L;
        short producerEpoch = 16;
        int sequence = 0;
        MemoryRecords records = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
                new SimpleRecord("hello".getBytes()), new SimpleRecord("there".getBytes()), new SimpleRecord("beautiful".getBytes()));
        assertThrows(UnsupportedForMessageFormatException.class, () -> new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.GZIP,
                Compression.gzip().build(),
                false,
                RecordBatch.MAGIC_VALUE_V1,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ));
    }


    @Test
    public void testDownConversionOfIdempotentRecordsNotPermitted() {
        long offset = 1234567;
        long producerId = 1344L;
        short producerEpoch = 16;
        int sequence = 0;
        MemoryRecords records = MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, producerEpoch, sequence,
                new SimpleRecord("hello".getBytes()), new SimpleRecord("there".getBytes()), new SimpleRecord("beautiful".getBytes()));
        assertThrows(UnsupportedForMessageFormatException.class, () -> new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.GZIP,
                Compression.gzip().build(),
                false,
                RecordBatch.MAGIC_VALUE_V1,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ));
    }

    @Test
    public void testOffsetAssignmentAfterDownConversionV2ToV0NonCompressed() {
        long offset = 1234567;
        long now = System.currentTimeMillis();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, Compression.NONE);
        checkOffsets(records, 0);
        checkOffsets(new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                RecordBatch.MAGIC_VALUE_V0,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords, offset);
    }


    @Test
    public void testOffsetAssignmentAfterDownConversionV2ToV0Compressed() {
        long offset = 1234567;
        long now = System.currentTimeMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, compression);
        checkOffsets(records, 0);
        checkOffsets(new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.GZIP,
                compression,
                false,
                RecordBatch.MAGIC_VALUE_V0,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ).validatedRecords, offset);
    }

    @Test
    public void testNonIncreasingOffsetRecordBatchHasMetricsLogged() {
        ByteBuffer buf = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, RecordBatch.MAGIC_VALUE_V2, Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.appendWithOffset(0, RecordBatch.NO_TIMESTAMP, null, "hello".getBytes());
        builder.appendWithOffset(2, RecordBatch.NO_TIMESTAMP, null, "there".getBytes());
        builder.appendWithOffset(3, RecordBatch.NO_TIMESTAMP, null, "beautiful".getBytes());

        MemoryRecords records = builder.build();
        records.batches().iterator().next().setLastOffset(2);
        assertThrows(InvalidRecordException.class, () -> new LogValidator(
                records,
                new TopicPartition("topic", 0),
                time,
                CompressionType.GZIP,
                Compression.gzip().build(),
                false,
                RecordBatch.MAGIC_VALUE_V0,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        ));

        assertEquals(metricsRecorder.recordInvalidOffsetCount, 1);
    }

    @Test
    public void testZStdCompressedWithUnavailableIBPVersion() {
        // The timestamps should be overwritten
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, 1234L, Compression.NONE);
        assertThrows(UnsupportedCompressionTypeException.class, () ->
                new LogValidator(
                        records,
                        topicPartition,
                        time,
                        CompressionType.NONE,
                        Compression.zstd().build(),
                        false,
                        RecordBatch.MAGIC_VALUE_V2,
                        TimestampType.LOG_APPEND_TIME,
                        1000L,
                        1000L,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        AppendOrigin.CLIENT,
                        MetadataVersion.IBP_2_0_IV1
                ).validateMessagesAndAssignOffsets(
                        PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
                )
        );
    }

    @Test
    public void testInvalidTimestampExceptionHasBatchIndex() {
        long now = System.currentTimeMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, now - 1001L, compression);

        RecordValidationException e = assertThrows(RecordValidationException.class, () ->
                new LogValidator(
                        records,
                        topicPartition,
                        time,
                        CompressionType.GZIP,
                        compression,
                        false,
                        RecordBatch.MAGIC_VALUE_V1,
                        TimestampType.CREATE_TIME,
                        1000L,
                        1000L,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        AppendOrigin.CLIENT,
                        MetadataVersion.latestTesting()
                ).validateMessagesAndAssignOffsets(
                        PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
                )
        );

        assertInstanceOf(InvalidTimestampException.class, e.invalidException());
        assertFalse(e.recordErrors().isEmpty());
        assertEquals(3, e.recordErrors().size());
    }

    @Test
    public void testInvalidRecordExceptionHasBatchIndex() {
        RecordValidationException e = assertThrows(RecordValidationException.class, () -> {
            Compression compression = Compression.gzip().build();
            validateMessages(
                    recordsWithInvalidInnerMagic(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, compression),
                    RecordBatch.MAGIC_VALUE_V0,
                    CompressionType.GZIP,
                    compression
            );
        });

        assertInstanceOf(InvalidRecordException.class, e.invalidException());
        assertFalse(e.recordErrors().isEmpty());
        // recordsWithInvalidInnerMagic creates 20 records
        assertEquals(20, e.recordErrors().size());
        for (Object error : e.recordErrors()) {
            assertNotNull(error);
        }
    }

    @Test
    public void testBatchWithInvalidRecordsAndInvalidTimestamp() {
        Compression compression = Compression.gzip().build();
        List<LegacyRecord> records = new ArrayList<>();
        for (int id = 0; id < 5; id++) {
            records.add(LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0, 0L, null, String.valueOf(id).getBytes()));
        }

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, compression,
                TimestampType.CREATE_TIME, 0L);
        int offset = 0;

        // We want to mix in a record with an invalid timestamp range
        builder.appendUncheckedWithOffset(offset, LegacyRecord.create(RecordBatch.MAGIC_VALUE_V1,
                1200L, null, "timestamp".getBytes()));
        for (LegacyRecord record : records) {
            offset += 30;
            builder.appendUncheckedWithOffset(offset, record);
        }
        MemoryRecords invalidOffsetTimestampRecords = builder.build();

        RecordValidationException e = assertThrows(RecordValidationException.class, () ->
                validateMessages(invalidOffsetTimestampRecords,
                        RecordBatch.MAGIC_VALUE_V0, CompressionType.GZIP, compression)
        );
        // If there is a mix of both regular InvalidRecordException and InvalidTimestampException,
        // InvalidTimestampException takes precedence
        assertInstanceOf(InvalidTimestampException.class, e.invalidException());
        assertFalse(e.recordErrors().isEmpty());
        assertEquals(6, e.recordErrors().size());
    }

    @Test
    public void testRecordWithPastTimestampIsRejected() {
        long timestampBeforeMaxConfig = Duration.ofHours(24).toMillis(); // 24 hrs
        long timestampAfterMaxConfig = Duration.ofHours(1).toMillis(); // 1 hr
        long now = System.currentTimeMillis();
        long fiveMinutesBeforeThreshold = now - timestampBeforeMaxConfig - Duration.ofMinutes(5).toMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, fiveMinutesBeforeThreshold, compression);
        RecordValidationException e = assertThrows(RecordValidationException.class, () ->
                new LogValidator(
                        records,
                        topicPartition,
                        time,
                        CompressionType.GZIP,
                        compression,
                        false,
                        RecordBatch.MAGIC_VALUE_V2,
                        TimestampType.CREATE_TIME,
                        timestampBeforeMaxConfig,
                        timestampAfterMaxConfig,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        AppendOrigin.CLIENT,
                        MetadataVersion.latestTesting()
                ).validateMessagesAndAssignOffsets(
                        PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
                )
        );

        assertInstanceOf(InvalidTimestampException.class, e.invalidException());
        assertFalse(e.recordErrors().isEmpty());
        assertEquals(3, e.recordErrors().size());
    }

    @Test
    public void testRecordWithFutureTimestampIsRejected() {
        long timestampBeforeMaxConfig = Duration.ofHours(24).toMillis(); // 24 hrs
        long timestampAfterMaxConfig = Duration.ofHours(1).toMillis(); // 1 hr
        long now = System.currentTimeMillis();
        long fiveMinutesAfterThreshold = now + timestampAfterMaxConfig + Duration.ofMinutes(5).toMillis();
        Compression compression = Compression.gzip().build();
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, fiveMinutesAfterThreshold, compression);
        RecordValidationException e = assertThrows(RecordValidationException.class, () ->
                new LogValidator(
                        records,
                        topicPartition,
                        time,
                        CompressionType.GZIP,
                        compression,
                        false,
                        RecordBatch.MAGIC_VALUE_V2,
                        TimestampType.CREATE_TIME,
                        timestampBeforeMaxConfig,
                        timestampAfterMaxConfig,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        AppendOrigin.CLIENT,
                        MetadataVersion.latestTesting()
                ).validateMessagesAndAssignOffsets(
                        PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
                )
        );

        assertInstanceOf(InvalidTimestampException.class, e.invalidException());
        assertFalse(e.recordErrors().isEmpty());
        assertEquals(3, e.recordErrors().size());
    }


    @Test
    public void testDifferentLevelDoesNotCauseRecompression() {
        List<byte[]> records = Arrays.asList(
                String.join("", Collections.nCopies(256, "some")).getBytes(),
                String.join("", Collections.nCopies(256, "data")).getBytes()
        );

        // Records from the producer were created with gzip max level
        Compression gzipMax = Compression.gzip().level(CompressionType.GZIP.maxLevel()).build();
        MemoryRecords recordsGzipMax = createRecords(records, RecordBatch.MAGIC_VALUE_V2, RecordBatch.NO_TIMESTAMP, gzipMax);

        // The topic is configured with gzip min level
        Compression gzipMin = Compression.gzip().level(CompressionType.GZIP.minLevel()).build();
        MemoryRecords recordsGzipMin = createRecords(records, RecordBatch.MAGIC_VALUE_V2, RecordBatch.NO_TIMESTAMP, gzipMin);

        // Ensure data compressed with gzip max and min is different
        assertNotEquals(recordsGzipMax, recordsGzipMin);

        LogValidator validator = new LogValidator(recordsGzipMax,
                topicPartition,
                time,
                gzipMax.type(),
                gzipMin,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        );

        LogValidator.ValidationResult result = validator.validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        // Ensure validated records have not been changed so they are the same as the producer records
        assertEquals(recordsGzipMax, result.validatedRecords);
        assertNotEquals(recordsGzipMin, result.validatedRecords);
    }

    @Test
    public void testDifferentCodecCausesRecompression() {
        List<byte[]> records = Arrays.asList(
                "somedata".getBytes(),
                "moredata".getBytes()
        );

        // Records from the producer were created with gzip max level
        Compression gzipMax = Compression.gzip().level(CompressionType.GZIP.maxLevel()).build();
        MemoryRecords recordsGzipMax = createRecords(records, RecordBatch.MAGIC_VALUE_V2, RecordBatch.NO_TIMESTAMP, gzipMax);

        // The topic is configured with lz4 min level
        Compression lz4Min = Compression.lz4().level(CompressionType.GZIP.minLevel()).build();
        MemoryRecords recordsLz4Min = createRecords(records, RecordBatch.MAGIC_VALUE_V2, RecordBatch.NO_TIMESTAMP, lz4Min);

        LogValidator validator = new LogValidator(recordsGzipMax,
                topicPartition,
                time,
                gzipMax.type(),
                lz4Min,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.CREATE_TIME,
                5000L,
                5000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        );

        LogValidator.ValidationResult result = validator.validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        // Ensure validated records have been recompressed and match lz4 min level
        assertEquals(recordsLz4Min, result.validatedRecords);
    }

    @ParameterizedTest
    @CsvSource({"1", "2"})
    public void checkNonCompressed(byte magic) {
        long now = System.currentTimeMillis();
        // set the timestamp of seq(1) (i.e. offset 1) as the max timestamp
        long[] timestampSeq = new long[]{now - 1, now + 1, now};

        long producerId;
        short producerEpoch;
        int baseSequence;
        boolean isTransactional;
        int partitionLeaderEpoch;

        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            producerId = 1324L;
            producerEpoch = 10;
            baseSequence = 984;
            isTransactional = true;
            partitionLeaderEpoch = 40;
        } else {
            producerId = RecordBatch.NO_PRODUCER_ID;
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;
            baseSequence = RecordBatch.NO_SEQUENCE;
            isTransactional = false;
            partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
        }

        List<SimpleRecord> recordList = new ArrayList<>();
        recordList.add(new SimpleRecord(timestampSeq[0], "hello".getBytes()));
        recordList.add(new SimpleRecord(timestampSeq[1], "there".getBytes()));
        recordList.add(new SimpleRecord(timestampSeq[2], "beautiful".getBytes()));

        MemoryRecords records = MemoryRecords.withRecords(magic, 0L, Compression.NONE, TimestampType.CREATE_TIME, producerId,
                producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, recordList.toArray(new SimpleRecord[0]));

        PrimitiveRef.LongRef offsetCounter = PrimitiveRef.ofLong(0L);
        LogValidator.ValidationResult validatingResults = new LogValidator(
                records,
                topicPartition,
                time,
                CompressionType.NONE,
                Compression.NONE,
                false,
                magic,
                TimestampType.CREATE_TIME,
                1000L,
                1000L,
                partitionLeaderEpoch,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                offsetCounter,
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        MemoryRecords validatedRecords = validatingResults.validatedRecords;

        int i = 0;
        for (RecordBatch batch : validatedRecords.batches()) {
            assertTrue(batch.isValid());
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            maybeCheckBaseTimestamp(timestampSeq[0], batch);
            assertEquals(batch.maxTimestamp(), batch.maxTimestamp());

            assertEquals(producerEpoch, batch.producerEpoch());
            assertEquals(producerId, batch.producerId());
            assertEquals(baseSequence, batch.baseSequence());
            assertEquals(isTransactional, batch.isTransactional());
            assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());
            for (Record record : batch) {
                record.ensureValid();
                assertEquals(timestampSeq[i], record.timestamp());
                i += 1;
            }
        }

        assertEquals(i, offsetCounter.value);
        assertEquals(now + 1, validatingResults.maxTimestampMs,
                "Max timestamp should be " + (now + 1));

        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            assertEquals(1, iteratorSize(records.batches().iterator()));
            assertEquals(2, validatingResults.shallowOffsetOfMaxTimestamp);
        } else {
            assertEquals(3, iteratorSize(records.batches().iterator()));
            assertEquals(1, validatingResults.shallowOffsetOfMaxTimestamp);
        }

        assertFalse(validatingResults.messageSizeMaybeChanged,
                "Message size should not have been changed");
        verifyRecordValidationStats(validatingResults.recordValidationStats, 0, records, false);
    }

    private void assertInvalidBatchCountOverrides(int lastOffsetDelta, int count) {
        assertThrows(InvalidRecordException.class,
                () -> validateRecordBatchWithCountOverrides(lastOffsetDelta, count));
    }

    private void validateRecordBatchWithCountOverrides(int lastOffsetDelta, int count) {
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V2, 1234L, Compression.NONE);
        ByteBuffer buffer = records.buffer();
        buffer.putInt(DefaultRecordBatch.RECORDS_COUNT_OFFSET, count);
        buffer.putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta);

        new LogValidator(
                records,
                topicPartition,
                time,
                CompressionType.GZIP,
                Compression.gzip().build(),
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0L),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );
    }

    @ParameterizedTest
    @CsvSource({"1", "2"})
    public void checkLogAppendTimeWithoutRecompression(byte magic) {
        Compression compression = Compression.gzip().build();
        MockTime mockTime = new MockTime();
        MemoryRecords records = createRecords(magic, 1234L, compression);
        LogValidator.ValidationResult validatedResults = new LogValidator(
                records,
                topicPartition,
                mockTime,
                CompressionType.GZIP,
                compression,
                false,
                magic,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        MemoryRecords validatedRecords = validatedResults.validatedRecords;
        assertEquals(records.sizeInBytes(), validatedRecords.sizeInBytes(),
                "message set size should not change");
        long now = mockTime.milliseconds();
        for (RecordBatch batch : validatedRecords.batches())
            validateLogAppendTime(now, 1234L, batch);
        assertTrue(validatedRecords.batches().iterator().next().isValid(),
                "MessageSet should still valid");
        assertEquals(now, validatedResults.maxTimestampMs,
                "Max timestamp should be " + now);
        assertEquals(2, validatedResults.shallowOffsetOfMaxTimestamp,
                "The shallow offset of max timestamp should be the last offset 2 if logAppendTime is used");
        assertFalse(validatedResults.messageSizeMaybeChanged,
                "Message size should not have been changed");

        verifyRecordValidationStats(validatedResults.recordValidationStats, 0, records, true);
    }

    @ParameterizedTest
    @CsvSource({"1", "2"})
    public void checkLogAppendTimeWithRecompression(byte targetMagic) {
        Compression compression = Compression.gzip().build();
        MockTime mockTime = new MockTime();
        // The timestamps should be overwritten
        MemoryRecords records = createRecords(RecordBatch.MAGIC_VALUE_V0, RecordBatch.NO_TIMESTAMP, compression);
        LogValidator.ValidationResult validatedResults = new LogValidator(
                records,
                topicPartition,
                mockTime,
                CompressionType.GZIP,
                compression,
                false,
                targetMagic,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                PrimitiveRef.ofLong(0),
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );

        MemoryRecords validatedRecords = validatedResults.validatedRecords;
        assertEquals(iteratorSize(records.records().iterator()), iteratorSize(validatedRecords.records().iterator()),
                "message set size should not change");
        long now = mockTime.milliseconds();
        validatedRecords.batches().forEach(batch -> validateLogAppendTime(now, -1, batch));
        assertTrue(validatedRecords.batches().iterator().next().isValid(),
                "MessageSet should still valid");
        assertEquals(now, validatedResults.maxTimestampMs, String.format("Max timestamp should be %d", now));
        assertEquals(2, validatedResults.shallowOffsetOfMaxTimestamp,
                "The shallow offset of max timestamp should be 2 if logAppendTime is used");
        assertTrue(validatedResults.messageSizeMaybeChanged,
                "Message size may have been changed");

        RecordValidationStats stats = validatedResults.recordValidationStats;
        verifyRecordValidationStats(stats, 3, records, true);
    }

    @ParameterizedTest
    @CsvSource({"0", "1", "2"})
    public void checkLogAppendTimeNonCompressed(byte magic) {
        MockTime mockTime = new MockTime();
        // The timestamps should be overwritten
        MemoryRecords records = createRecords(magic, 1234L, Compression.NONE);
        PrimitiveRef.LongRef offsetCounter = PrimitiveRef.ofLong(0);
        LogValidator.ValidationResult validatedResults = new LogValidator(records,
                topicPartition,
                mockTime,
                CompressionType.NONE,
                Compression.NONE,
                false,
                magic,
                TimestampType.LOG_APPEND_TIME,
                1000L,
                1000L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                AppendOrigin.CLIENT,
                MetadataVersion.latestTesting()
        ).validateMessagesAndAssignOffsets(
                offsetCounter,
                metricsRecorder,
                RequestLocal.withThreadConfinedCaching().bufferSupplier()
        );
        assertEquals(offsetCounter.value, iteratorSize(records.records().iterator()));

        MemoryRecords validatedRecords = validatedResults.validatedRecords;
        assertEquals(iteratorSize(records.records().iterator()), iteratorSize(validatedRecords.records().iterator()), "message set size should not change");

        long now = mockTime.milliseconds();

        if (magic >= RecordBatch.MAGIC_VALUE_V1) {
            validatedRecords.batches().forEach(batch -> validateLogAppendTime(now, 1234L, batch));
        }

        if (magic == RecordBatch.MAGIC_VALUE_V0) {
            assertEquals(RecordBatch.NO_TIMESTAMP, validatedResults.maxTimestampMs);
        } else {
            assertEquals(now, validatedResults.maxTimestampMs);
        }

        assertFalse(validatedResults.messageSizeMaybeChanged, "Message size should not have been changed");

        int expectedMaxTimestampOffset;
        switch (magic) {
            case RecordBatch.MAGIC_VALUE_V0:
                expectedMaxTimestampOffset = -1;
                break;
            case RecordBatch.MAGIC_VALUE_V1:
                expectedMaxTimestampOffset = 0;
                break;
            default:
                expectedMaxTimestampOffset = 2;
                break;
        }
        assertEquals(expectedMaxTimestampOffset, validatedResults.shallowOffsetOfMaxTimestamp);
        verifyRecordValidationStats(validatedResults.recordValidationStats, 0, records, false);
    }

    /**
     * expectedLogAppendTime is only checked if batch.magic is V2 or higher
     */
    void validateLogAppendTime(long expectedLogAppendTime, long expectedBaseTimestamp, RecordBatch batch) {
        assertTrue(batch.isValid());
        assertEquals(batch.timestampType(), TimestampType.LOG_APPEND_TIME);
        assertEquals(expectedLogAppendTime, batch.maxTimestamp(), "Unexpected max timestamp of batch $batch");
        maybeCheckBaseTimestamp(expectedBaseTimestamp, batch);
        batch.forEach(record -> {
            record.ensureValid();
            assertEquals(expectedLogAppendTime, record.timestamp(), String.format("Unexpected timestamp of record %s", record));
        });

    }

    // Check that offsets are assigned consecutively from the given base offset
    private void checkOffsets(MemoryRecords records, long baseOffset) {
        Assertions.assertTrue(iteratorSize(records.records().iterator()) != 0, "Message set should not be empty");

        long offset = baseOffset;

        for (Record record : records.records()) {
            Assertions.assertEquals(offset, record.offset(), "Unexpected offset in message set iterator");
            offset += 1;
        }
    }

    private void maybeCheckBaseTimestamp(long expected, RecordBatch batch) {
        if (batch instanceof DefaultRecordBatch) {
            DefaultRecordBatch b = (DefaultRecordBatch) batch;
            assertEquals(expected, b.baseTimestamp(), "Unexpected base timestamp of batch " + batch);
        }
    }

    private static <T> int iteratorSize(Iterator<T> iterator) {
        int counter = 0;
        while (iterator.hasNext()) {
            iterator.next();
            counter += 1;
        }
        return counter;
    }

    public void verifyRecordValidationStats(RecordValidationStats stats, int numConvertedRecords, MemoryRecords records,
                                            boolean compressed) {
        assertNotNull(stats, "Records processing info is null");
        assertEquals(numConvertedRecords, stats.numRecordsConverted());
        if (numConvertedRecords > 0) {
            assertTrue(stats.conversionTimeNanos() >= 0, "Conversion time not recorded " + stats);
            assertTrue(stats.conversionTimeNanos() <= TimeUnit.MINUTES.toNanos(1), "Conversion time not valid " + stats);
        }
        int originalSize = records.sizeInBytes();
        long tempBytes = stats.temporaryMemoryBytes();
        if (numConvertedRecords > 0 && compressed)
            assertTrue(tempBytes > originalSize, "Temp bytes too small, orig=" + originalSize + " actual=" + tempBytes);
        else if (numConvertedRecords > 0 || compressed)
            assertTrue(tempBytes > 0, "Temp bytes not updated");
        else
            assertEquals(0, tempBytes);
    }
}