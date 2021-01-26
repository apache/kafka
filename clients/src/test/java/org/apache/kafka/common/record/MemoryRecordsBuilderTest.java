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
package org.apache.kafka.common.record;

import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.utils.Utils.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class MemoryRecordsBuilderTest {

    private static class Args {
        final int bufferOffset;
        final CompressionType compressionType;

        public Args(int bufferOffset, CompressionType compressionType) {
            this.bufferOffset = bufferOffset;
            this.compressionType = compressionType;
        }

        @Override
        public String toString() {
            return "bufferOffset=" + bufferOffset +
                ", compressionType=" + compressionType;
        }
    }

    private static class MemoryRecordsBuilderArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            List<Arguments> values = new ArrayList<>();
            for (int bufferOffset : Arrays.asList(0, 15))
                for (CompressionType compressionType : CompressionType.values())
                    values.add(Arguments.of(new Args(bufferOffset, compressionType)));
            return values.stream();
        }
    }

    private final Time time = Time.SYSTEM;

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteEmptyRecordSet(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V0;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(128, args);

        Supplier<MemoryRecordsBuilder> builderSupplier = () -> new MemoryRecordsBuilder(buffer, magic,
            args.compressionType, TimestampType.CREATE_TIME, 0L, 0L,
            RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
            false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        if (args.compressionType != CompressionType.ZSTD) {
            MemoryRecords records = builderSupplier.get().build();
            assertEquals(0, records.sizeInBytes());
            assertEquals(args.bufferOffset, buffer.position());
        } else {
            Exception e = assertThrows(IllegalArgumentException.class, () -> builderSupplier.get().build());
            assertEquals(e.getMessage(), "ZStandard compression is not supported for magic " + magic);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteTransactionalRecordSet(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, pid, epoch, sequence, true, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(System.currentTimeMillis(), "foo".getBytes(), "bar".getBytes());
        MemoryRecords records = builder.build();

        List<MutableRecordBatch> batches = Utils.toList(records.batches().iterator());
        assertEquals(1, batches.size());
        assertTrue(batches.get(0).isTransactional());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteTransactionalNotAllowedMagicV0(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        assertThrows(IllegalArgumentException.class, () -> new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0,
            args.compressionType, TimestampType.CREATE_TIME, 0L, 0L, pid, epoch, sequence,
            true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity()));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteTransactionalNotAllowedMagicV1(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        assertThrows(IllegalArgumentException.class, () -> new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1,
            args.compressionType, TimestampType.CREATE_TIME, 0L, 0L, pid, epoch, sequence,
            true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity()));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteControlBatchNotAllowedMagicV0(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        assertThrows(IllegalArgumentException.class, () -> new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0,
            args.compressionType, TimestampType.CREATE_TIME, 0L, 0L, pid, epoch, sequence,
            false, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity()));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteControlBatchNotAllowedMagicV1(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        assertThrows(IllegalArgumentException.class, () -> new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1,
            args.compressionType, TimestampType.CREATE_TIME, 0L, 0L, pid, epoch, sequence,
            false, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity()));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteTransactionalWithInvalidPID(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = RecordBatch.NO_PRODUCER_ID;
        short epoch = 15;
        int sequence = 2342;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType, TimestampType.CREATE_TIME,
            0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        assertThrows(IllegalArgumentException.class, builder::close);
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteIdempotentWithInvalidEpoch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = 9809;
        short epoch = RecordBatch.NO_PRODUCER_EPOCH;
        int sequence = 2342;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType, TimestampType.CREATE_TIME,
            0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        assertThrows(IllegalArgumentException.class, builder::close);
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteIdempotentWithInvalidBaseSequence(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType, TimestampType.CREATE_TIME,
            0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        assertThrows(IllegalArgumentException.class, builder::close);
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteEndTxnMarkerNonTransactionalBatch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, false, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        assertThrows(IllegalArgumentException.class, () -> builder.appendEndTxnMarker(RecordBatch.NO_TIMESTAMP,
            new EndTransactionMarker(ControlRecordType.ABORT, 0)));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteEndTxnMarkerNonControlBatch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        assertThrows(IllegalArgumentException.class, () -> builder.appendEndTxnMarker(RecordBatch.NO_TIMESTAMP,
                new EndTransactionMarker(ControlRecordType.ABORT, 0)));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteLeaderChangeControlBatchWithoutLeaderEpoch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        final int leaderId = 1;
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType, TimestampType.CREATE_TIME,
            0L, 0L,
            RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
            false, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        assertThrows(IllegalArgumentException.class, () -> builder.appendLeaderChangeMessage(RecordBatch.NO_TIMESTAMP,
            new LeaderChangeMessage()
                .setLeaderId(leaderId)
                .setVoters(Collections.emptyList())));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testWriteLeaderChangeControlBatch(Args args) {
        ByteBuffer buffer = allocateBuffer(128, args);

        final int leaderId = 1;
        final int leaderEpoch = 5;
        final List<Integer> voters = Arrays.asList(2, 3);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType, TimestampType.CREATE_TIME,
            0L, 0L,
            RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
            false, true, leaderEpoch, buffer.capacity());
        builder.appendLeaderChangeMessage(RecordBatch.NO_TIMESTAMP,
            new LeaderChangeMessage()
                .setLeaderId(leaderId)
                .setVoters(voters.stream().map(
                    voterId -> new Voter().setVoterId(voterId)).collect(Collectors.toList())));

        MemoryRecords built = builder.build();
        List<Record> records = TestUtils.toList(built.records());
        assertEquals(1, records.size());
        LeaderChangeMessage leaderChangeMessage = ControlRecordUtils.deserializeLeaderChangeMessage(records.get(0));

        assertEquals(leaderId, leaderChangeMessage.leaderId());
        assertEquals(voters, leaderChangeMessage.voters().stream().map(Voter::voterId).collect(Collectors.toList()));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testCompressionRateV0(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V0;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(1024, args);

        LegacyRecord[] records = new LegacyRecord[] {
                LegacyRecord.create(magic, 0L, "a".getBytes(), "1".getBytes()),
                LegacyRecord.create(magic, 1L, "b".getBytes(), "2".getBytes()),
                LegacyRecord.create(magic, 2L, "c".getBytes(), "3".getBytes()),
        };

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        int uncompressedSize = 0;
        for (LegacyRecord record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            builder.append(record);
        }

        MemoryRecords built = builder.build();
        if (args.compressionType == CompressionType.NONE) {
            assertEquals(1.0, builder.compressionRatio(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V0;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, builder.compressionRatio(), 0.00001);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testEstimatedSizeInBytes(Args args) {
        ByteBuffer buffer = allocateBuffer(1024, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        int previousEstimate = 0;
        for (int i = 0; i < 10; i++) {
            builder.append(new SimpleRecord(i, ("" + i).getBytes()));
            int currentEstimate = builder.estimatedSizeInBytes();
            assertTrue(currentEstimate > previousEstimate);
            previousEstimate = currentEstimate;
        }

        int bytesWrittenBeforeClose = builder.estimatedSizeInBytes();
        MemoryRecords records = builder.build();
        assertEquals(records.sizeInBytes(), builder.estimatedSizeInBytes());
        if (args.compressionType == CompressionType.NONE)
            assertEquals(records.sizeInBytes(), bytesWrittenBeforeClose);
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testCompressionRateV1(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V1;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(1024, args);

        LegacyRecord[] records = new LegacyRecord[] {
                LegacyRecord.create(magic, 0L, "a".getBytes(), "1".getBytes()),
                LegacyRecord.create(magic, 1L, "b".getBytes(), "2".getBytes()),
                LegacyRecord.create(magic, 2L, "c".getBytes(), "3".getBytes()),
        };

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        int uncompressedSize = 0;
        for (LegacyRecord record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            builder.append(record);
        }

        MemoryRecords built = builder.build();
        if (args.compressionType == CompressionType.NONE) {
            assertEquals(1.0, builder.compressionRatio(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V1;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, builder.compressionRatio(), 0.00001);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void buildUsingLogAppendTime(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V1;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(1024, args);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(0L, "b".getBytes(), "2".getBytes());
        builder.append(0L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(logAppendTime, info.maxTimestamp);

        if (args.compressionType != CompressionType.NONE)
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);
        else
            assertEquals(0L, info.shallowOffsetOfMaxTimestamp);

        for (RecordBatch batch : records.batches()) {
            assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
            for (Record record : batch)
                assertEquals(logAppendTime, record.timestamp());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void buildUsingCreateTime(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V1;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(1024, args);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(2L, "b".getBytes(), "2".getBytes());
        builder.append(1L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(2L, info.maxTimestamp);

        if (args.compressionType == CompressionType.NONE)
            assertEquals(1L, info.shallowOffsetOfMaxTimestamp);
        else
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        int i = 0;
        long[] expectedTimestamps = new long[] {0L, 2L, 1L};
        for (RecordBatch batch : records.batches()) {
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            for (Record record : batch)
                assertEquals(expectedTimestamps[i++], record.timestamp());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testAppendedChecksumConsistency(Args args) {
        assumeAtLeastV2OrNotZstd(RecordBatch.MAGIC_VALUE_V0, args.compressionType);
        assumeAtLeastV2OrNotZstd(RecordBatch.MAGIC_VALUE_V1, args.compressionType);

        ByteBuffer buffer = ByteBuffer.allocate(512);
        for (byte magic : Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)) {
            MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                    TimestampType.CREATE_TIME, 0L, LegacyRecord.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID,
                    RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                    RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
            Long checksumOrNull = builder.append(1L, "key".getBytes(), "value".getBytes());
            MemoryRecords memoryRecords = builder.build();
            List<Record> records = TestUtils.toList(memoryRecords.records());
            assertEquals(1, records.size());
            assertEquals(checksumOrNull, records.get(0).checksumOrNull());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testSmallWriteLimit(Args args) {
        // with a small write limit, we always allow at least one record to be added

        byte[] key = "foo".getBytes();
        byte[] value = "bar".getBytes();
        int writeLimit = 0;
        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, args.compressionType,
                TimestampType.CREATE_TIME, 0L, LegacyRecord.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, writeLimit);

        assertFalse(builder.isFull());
        assertTrue(builder.hasRoomFor(0L, key, value, Record.EMPTY_HEADERS));
        builder.append(0L, key, value);

        assertTrue(builder.isFull());
        assertFalse(builder.hasRoomFor(0L, key, value, Record.EMPTY_HEADERS));

        MemoryRecords memRecords = builder.build();
        List<Record> records = TestUtils.toList(memRecords.records());
        assertEquals(1, records.size());

        Record record = records.get(0);
        assertEquals(ByteBuffer.wrap(key), record.key());
        assertEquals(ByteBuffer.wrap(value), record.value());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void writePastLimit(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V1;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(64, args);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.setEstimatedCompressionRatio(0.5f);
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(1L, "b".getBytes(), "2".getBytes());

        assertFalse(builder.hasRoomFor(2L, "c".getBytes(), "3".getBytes(), Record.EMPTY_HEADERS));
        builder.append(2L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(2L, info.maxTimestamp);
        assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        long i = 0L;
        for (RecordBatch batch : records.batches()) {
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            for (Record record : batch)
                assertEquals(i++, record.timestamp());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testAppendAtInvalidOffset(Args args) {
        ByteBuffer buffer = allocateBuffer(1024, args);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V2, args.compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        builder.appendWithOffset(0L, System.currentTimeMillis(), "a".getBytes(), null);

        // offsets must increase monotonically
        assertThrows(IllegalArgumentException.class, () -> builder.appendWithOffset(0L, System.currentTimeMillis(),
            "b".getBytes(), null));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void convertV2ToV1UsingMixedCreateAndLogAppendTime(Args args) {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2,
                args.compressionType, TimestampType.LOG_APPEND_TIME, 0L);
        builder.append(10L, "1".getBytes(), "a".getBytes());
        builder.close();

        int sizeExcludingTxnMarkers = buffer.position();

        MemoryRecords.writeEndTransactionalMarker(buffer, 1L, System.currentTimeMillis(), 0, 15L, (short) 0,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));

        int position = buffer.position();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, args.compressionType,
                TimestampType.CREATE_TIME, 1L);
        builder.append(12L, "2".getBytes(), "b".getBytes());
        builder.append(13L, "3".getBytes(), "c".getBytes());
        builder.close();

        sizeExcludingTxnMarkers += buffer.position() - position;

        MemoryRecords.writeEndTransactionalMarker(buffer, 14L, System.currentTimeMillis(), 0, 1L, (short) 0,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        buffer.flip();

        Supplier<ConvertedRecords<MemoryRecords>> convertedRecordsSupplier = () ->
            MemoryRecords.readableRecords(buffer).downConvert(RecordBatch.MAGIC_VALUE_V1, 0, time);

        if (args.compressionType != CompressionType.ZSTD) {
            ConvertedRecords<MemoryRecords> convertedRecords = convertedRecordsSupplier.get();
            MemoryRecords records = convertedRecords.records();

            // Transactional markers are skipped when down converting to V1, so exclude them from size
            verifyRecordsProcessingStats(args.compressionType, convertedRecords.recordConversionStats(),
                3, 3, records.sizeInBytes(), sizeExcludingTxnMarkers);

            List<? extends RecordBatch> batches = Utils.toList(records.batches().iterator());
            if (args.compressionType != CompressionType.NONE) {
                assertEquals(2, batches.size());
                assertEquals(TimestampType.LOG_APPEND_TIME, batches.get(0).timestampType());
                assertEquals(TimestampType.CREATE_TIME, batches.get(1).timestampType());
            } else {
                assertEquals(3, batches.size());
                assertEquals(TimestampType.LOG_APPEND_TIME, batches.get(0).timestampType());
                assertEquals(TimestampType.CREATE_TIME, batches.get(1).timestampType());
                assertEquals(TimestampType.CREATE_TIME, batches.get(2).timestampType());
            }

            List<Record> logRecords = Utils.toList(records.records().iterator());
            assertEquals(3, logRecords.size());
            assertEquals(ByteBuffer.wrap("1".getBytes()), logRecords.get(0).key());
            assertEquals(ByteBuffer.wrap("2".getBytes()), logRecords.get(1).key());
            assertEquals(ByteBuffer.wrap("3".getBytes()), logRecords.get(2).key());
        } else {
            Exception e = assertThrows(UnsupportedCompressionTypeException.class, convertedRecordsSupplier::get);
            assertEquals("Down-conversion of zstandard-compressed batches is not supported", e.getMessage());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void convertToV1WithMixedV0AndV2Data(Args args) {
        CompressionType compressionType = args.compressionType;
        assumeAtLeastV2OrNotZstd(RecordBatch.MAGIC_VALUE_V0, compressionType);
        assumeAtLeastV2OrNotZstd(RecordBatch.MAGIC_VALUE_V1, compressionType);

        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0,
                compressionType, TimestampType.NO_TIMESTAMP_TYPE, 0L);
        builder.append(RecordBatch.NO_TIMESTAMP, "1".getBytes(), "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, compressionType,
                TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "2".getBytes(), "b".getBytes());
        builder.append(12L, "3".getBytes(), "c".getBytes());
        builder.close();

        buffer.flip();

        ConvertedRecords<MemoryRecords> convertedRecords = MemoryRecords.readableRecords(buffer)
                .downConvert(RecordBatch.MAGIC_VALUE_V1, 0, time);
        MemoryRecords records = convertedRecords.records();
        verifyRecordsProcessingStats(compressionType, convertedRecords.recordConversionStats(), 3, 2,
                records.sizeInBytes(), buffer.limit());

        List<? extends RecordBatch> batches = Utils.toList(records.batches().iterator());
        if (compressionType != CompressionType.NONE) {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(1, batches.get(1).baseOffset());
        } else {
            assertEquals(3, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(1, batches.get(1).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(2).magic());
            assertEquals(2, batches.get(2).baseOffset());
        }

        List<Record> logRecords = Utils.toList(records.records().iterator());
        assertEquals("1", utf8(logRecords.get(0).key()));
        assertEquals("2", utf8(logRecords.get(1).key()));
        assertEquals("3", utf8(logRecords.get(2).key()));

        convertedRecords = MemoryRecords.readableRecords(buffer).downConvert(RecordBatch.MAGIC_VALUE_V1, 2L, time);
        records = convertedRecords.records();

        batches = Utils.toList(records.batches().iterator());
        logRecords = Utils.toList(records.records().iterator());

        if (compressionType != CompressionType.NONE) {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(1, batches.get(1).baseOffset());
            assertEquals("1", utf8(logRecords.get(0).key()));
            assertEquals("2", utf8(logRecords.get(1).key()));
            assertEquals("3", utf8(logRecords.get(2).key()));
            verifyRecordsProcessingStats(compressionType, convertedRecords.recordConversionStats(), 3, 2,
                    records.sizeInBytes(), buffer.limit());
        } else {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(2, batches.get(1).baseOffset());
            assertEquals("1", utf8(logRecords.get(0).key()));
            assertEquals("3", utf8(logRecords.get(1).key()));
            verifyRecordsProcessingStats(compressionType, convertedRecords.recordConversionStats(), 3, 1,
                    records.sizeInBytes(), buffer.limit());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void shouldThrowIllegalStateExceptionOnBuildWhenAborted(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V0;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(128, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.abort();
        assertThrows(IllegalStateException.class, builder::build);
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void shouldResetBufferToInitialPositionOnAbort(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V0;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(128, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                                                                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                                                                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.abort();
        assertEquals(args.bufferOffset, builder.buffer().position());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void shouldThrowIllegalStateExceptionOnCloseWhenAborted(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V0;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(128, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                                                                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                                                                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.abort();
        try {
            builder.close();
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException e) {
            // ok
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void shouldThrowIllegalStateExceptionOnAppendWhenAborted(Args args) {
        byte magic = RecordBatch.MAGIC_VALUE_V0;
        assumeAtLeastV2OrNotZstd(magic, args.compressionType);

        ByteBuffer buffer = allocateBuffer(128, args);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, args.compressionType,
                                                                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                                                                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.abort();
        try {
            builder.append(0L, "a".getBytes(), "1".getBytes());
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException e) {
            // ok
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsBuilderArgumentsProvider.class)
    public void testBuffersDereferencedOnClose(Args args) {
        Runtime runtime = Runtime.getRuntime();
        int payloadLen = 1024 * 1024;
        ByteBuffer buffer = ByteBuffer.allocate(payloadLen * 2);
        byte[] key = new byte[0];
        byte[] value = new byte[payloadLen];
        new Random().nextBytes(value); // Use random payload so that compressed buffer is large
        List<MemoryRecordsBuilder> builders = new ArrayList<>(100);
        long startMem = 0;
        long memUsed = 0;
        int iterations =  0;
        while (iterations++ < 100) {
            buffer.rewind();
            MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V2, args.compressionType,
                    TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID,
                    RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                    RecordBatch.NO_PARTITION_LEADER_EPOCH, 0);
            builder.append(1L, new byte[0], value);
            builder.build();
            builders.add(builder);

            System.gc();
            memUsed = runtime.totalMemory() - runtime.freeMemory() - startMem;
            // Ignore memory usage during initialization
            if (iterations == 2)
                startMem = memUsed;
            else if (iterations > 2 && memUsed < (iterations - 2) * 1024)
                break;
        }
        assertTrue(iterations < 100, "Memory usage too high: " + memUsed);
    }

    private void verifyRecordsProcessingStats(CompressionType compressionType, RecordConversionStats processingStats,
                                              int numRecords, int numRecordsConverted, long finalBytes,
                                              long preConvertedBytes) {
        assertNotNull(processingStats, "Records processing info is null");
        assertEquals(numRecordsConverted, processingStats.numRecordsConverted());
        // Since nanoTime accuracy on build machines may not be sufficient to measure small conversion times,
        // only check if the value >= 0. Default is -1, so this checks if time has been recorded.
        assertTrue(processingStats.conversionTimeNanos() >= 0, "Processing time not recorded: " + processingStats);
        long tempBytes = processingStats.temporaryMemoryBytes();
        if (compressionType == CompressionType.NONE) {
            if (numRecordsConverted == 0)
                assertEquals(finalBytes, tempBytes);
            else if (numRecordsConverted == numRecords)
                assertEquals(preConvertedBytes + finalBytes, tempBytes);
            else {
                assertTrue(tempBytes > finalBytes && tempBytes < finalBytes + preConvertedBytes,
                    String.format("Unexpected temp bytes %d final %d pre %d", tempBytes, finalBytes, preConvertedBytes));
            }
        } else {
            long compressedBytes = finalBytes - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V0;
            assertTrue(tempBytes > compressedBytes,
                String.format("Uncompressed size expected temp=%d, compressed=%d", tempBytes, compressedBytes));
        }
    }

    private void assumeAtLeastV2OrNotZstd(byte magic, CompressionType compressionType) {
        assumeTrue(compressionType != CompressionType.ZSTD || magic >= MAGIC_VALUE_V2);
    }

    private ByteBuffer allocateBuffer(int size, Args args) {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.position(args.bufferOffset);
        return buffer;
    }
}
