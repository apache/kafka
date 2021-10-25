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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.record.MemoryRecords.RecordFilter;
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
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
import java.util.OptionalLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V0;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V1;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MemoryRecordsTest {

    private static class Args {
        final CompressionConfig compression;
        final byte magic;
        final long firstOffset;
        final long pid;
        final short epoch;
        final int firstSequence;

        public Args(byte magic, long firstOffset, CompressionConfig compression) {
            this.magic = magic;
            this.compression = compression;
            this.firstOffset = firstOffset;
            if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                pid = 134234L;
                epoch = 28;
                firstSequence = 777;
            } else {
                pid = RecordBatch.NO_PRODUCER_ID;
                epoch = RecordBatch.NO_PRODUCER_EPOCH;
                firstSequence = RecordBatch.NO_SEQUENCE;
            }
        }

        @Override
        public String toString() {
            return "magic=" + magic +
                ", firstOffset=" + firstOffset +
                ", CompressionConfig=" + compression;
        }
    }

    private static class MemoryRecordsArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            List<Arguments> arguments = new ArrayList<>();
            for (long firstOffset : asList(0L, 57L))
                for (CompressionType type: CompressionType.values()) {
                    List<Byte> magics = type == CompressionType.ZSTD
                            ? Collections.singletonList(RecordBatch.MAGIC_VALUE_V2)
                            : asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2);
                    for (byte magic : magics)
                        arguments.add(Arguments.of(new Args(magic, firstOffset, CompressionConfig.of(type).build())));
                }
            return arguments.stream();
        }
    }

    private static class V2MemoryRecordsArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            List<Arguments> arguments = new ArrayList<>();
            for (long firstOffset : asList(0L, 57L))
                for (CompressionType type: CompressionType.values()) {
                    arguments.add(Arguments.of(new Args(RecordBatch.MAGIC_VALUE_V2, firstOffset, CompressionConfig.of(type).build())));
                }
            return arguments.stream();
        }
    }

    private final long logAppendTime = System.currentTimeMillis();

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testIterator(Args args) {
        CompressionConfig compression = args.compression;
        byte magic = args.magic;
        long pid = args.pid;
        short epoch = args.epoch;
        int firstSequence = args.firstSequence;
        long firstOffset = args.firstOffset;
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        int partitionLeaderEpoch = 998;
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, compression,
                TimestampType.CREATE_TIME, firstOffset, logAppendTime, pid, epoch, firstSequence, false, false,
                partitionLeaderEpoch, buffer.limit());

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
            new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
            new SimpleRecord(3L, "c".getBytes(), "3".getBytes()),
            new SimpleRecord(4L, null, "4".getBytes()),
            new SimpleRecord(5L, "d".getBytes(), null),
            new SimpleRecord(6L, (byte[]) null, null)
        };

        for (SimpleRecord record : records)
            builder.append(record);

        MemoryRecords memoryRecords = builder.build();
        for (int iteration = 0; iteration < 2; iteration++) {
            int total = 0;
            for (RecordBatch batch : memoryRecords.batches()) {
                assertTrue(batch.isValid());
                assertEquals(compression.getType(), batch.compressionType());
                assertEquals(firstOffset + total, batch.baseOffset());

                if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                    assertEquals(pid, batch.producerId());
                    assertEquals(epoch, batch.producerEpoch());
                    assertEquals(firstSequence + total, batch.baseSequence());
                    assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());
                    assertEquals(records.length, batch.countOrNull().intValue());
                    assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                    assertEquals(records[records.length - 1].timestamp(), batch.maxTimestamp());
                } else {
                    assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId());
                    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch());
                    assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
                    assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, batch.partitionLeaderEpoch());
                    assertNull(batch.countOrNull());
                    if (magic == RecordBatch.MAGIC_VALUE_V0)
                        assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
                    else
                        assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                }

                int recordCount = 0;
                for (Record record : batch) {
                    record.ensureValid();
                    assertTrue(record.hasMagic(batch.magic()));
                    assertFalse(record.isCompressed());
                    assertEquals(firstOffset + total, record.offset());
                    assertEquals(records[total].key(), record.key());
                    assertEquals(records[total].value(), record.value());

                    if (magic >= RecordBatch.MAGIC_VALUE_V2)
                        assertEquals(firstSequence + total, record.sequence());

                    assertFalse(record.hasTimestampType(TimestampType.LOG_APPEND_TIME));
                    if (magic == RecordBatch.MAGIC_VALUE_V0) {
                        assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp());
                        assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                        assertTrue(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                    } else {
                        assertEquals(records[total].timestamp(), record.timestamp());
                        assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                        if (magic < RecordBatch.MAGIC_VALUE_V2)
                            assertTrue(record.hasTimestampType(TimestampType.CREATE_TIME));
                        else
                            assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                    }

                    total++;
                    recordCount++;
                }

                assertEquals(batch.baseOffset() + recordCount - 1, batch.lastOffset());
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testHasRoomForMethod(Args args) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), args.magic, args.compression,
                TimestampType.CREATE_TIME, 0L);
        builder.append(0L, "a".getBytes(), "1".getBytes());
        assertTrue(builder.hasRoomFor(1L, "b".getBytes(), "2".getBytes(), Record.EMPTY_HEADERS));
        builder.close();
        assertFalse(builder.hasRoomFor(1L, "b".getBytes(), "2".getBytes(), Record.EMPTY_HEADERS));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testHasRoomForMethodWithHeaders(Args args) {
        byte magic = args.magic;
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(120), magic, args.compression,
                TimestampType.CREATE_TIME, 0L);
        builder.append(logAppendTime, "key".getBytes(), "value".getBytes());
        RecordHeaders headers = new RecordHeaders();
        for (int i = 0; i < 10; ++i) headers.add("hello", "world.world".getBytes());
        // Make sure that hasRoomFor accounts for header sizes by letting a record without headers pass, but stopping
        // a record with a large number of headers.
        assertTrue(builder.hasRoomFor(logAppendTime, "key".getBytes(), "value".getBytes(), Record.EMPTY_HEADERS));
        if (magic < MAGIC_VALUE_V2) assertTrue(builder.hasRoomFor(logAppendTime, "key".getBytes(), "value".getBytes(), headers.toArray()));
        else assertFalse(builder.hasRoomFor(logAppendTime, "key".getBytes(), "value".getBytes(), headers.toArray()));
    }

    /**
     * This test verifies that the checksum returned for various versions matches hardcoded values to catch unintentional
     * changes to how the checksum is computed.
     */
    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testChecksum(Args args) {
        CompressionConfig compression = args.compression;
        byte magic = args.magic;
        // we get reasonable coverage with uncompressed and one compression type
        if (compression.getType() != CompressionType.NONE && compression.getType() != CompressionType.LZ4)
            return;

        SimpleRecord[] records = {
            new SimpleRecord(283843L, "key1".getBytes(), "value1".getBytes()),
            new SimpleRecord(1234L, "key2".getBytes(), "value2".getBytes())
        };
        RecordBatch batch = MemoryRecords.withRecords(magic, compression, records).batches().iterator().next();
        long expectedChecksum;
        if (magic == RecordBatch.MAGIC_VALUE_V0) {
            if (compression.getType() == CompressionType.NONE)
                expectedChecksum = 1978725405L;
            else
                expectedChecksum = 66944826L;
        } else if (magic == RecordBatch.MAGIC_VALUE_V1) {
            if (compression.getType() == CompressionType.NONE)
                expectedChecksum = 109425508L;
            else
                expectedChecksum = 1407303399L;
        } else {
            if (compression.getType() == CompressionType.NONE)
                expectedChecksum = 3851219455L;
            else
                expectedChecksum = 2745969314L;
        }
        assertEquals(expectedChecksum, batch.checksum(), "Unexpected checksum for magic " + magic +
            " and compression type " + compression);
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testFilterToPreservesPartitionLeaderEpoch(Args args) {
        byte magic = args.magic;
        int partitionLeaderEpoch = 67;

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, args.compression, TimestampType.CREATE_TIME,
                0L, RecordBatch.NO_TIMESTAMP, partitionLeaderEpoch);
        builder.append(10L, null, "a".getBytes());
        builder.append(11L, "1".getBytes(), "b".getBytes());
        builder.append(12L, null, "c".getBytes());

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        builder.build().filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(), filtered,
                Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        assertEquals(1, batches.size());

        MutableRecordBatch firstBatch = batches.get(0);
        if (magic < MAGIC_VALUE_V2) assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, firstBatch.partitionLeaderEpoch());
        else assertEquals(partitionLeaderEpoch, firstBatch.partitionLeaderEpoch());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testFilterToEmptyBatchRetention(Args args) {
        byte magic = args.magic;
        for (boolean isTransactional : Arrays.asList(true, false)) {
            ByteBuffer buffer = ByteBuffer.allocate(2048);
            long producerId = 23L;
            short producerEpoch = 5;
            long baseOffset = 3L;
            int baseSequence = 10;
            int partitionLeaderEpoch = 293;
            int numRecords = 2;

            Supplier<MemoryRecordsBuilder> supplier = () -> MemoryRecords.builder(buffer, magic, args.compression, TimestampType.CREATE_TIME,
                    baseOffset, RecordBatch.NO_TIMESTAMP, producerId, producerEpoch, baseSequence, isTransactional,
                    partitionLeaderEpoch);

            if (isTransactional && magic < RecordBatch.MAGIC_VALUE_V2) assertThrows(IllegalArgumentException.class, supplier::get);
            else {
                MemoryRecordsBuilder builder = supplier.get();
                builder.append(11L, "2".getBytes(), "b".getBytes());
                builder.append(12L, "3".getBytes(), "c".getBytes());
                if (magic < MAGIC_VALUE_V2) assertThrows(IllegalArgumentException.class, builder::close);
                else {
                    builder.close();
                    MemoryRecords records = builder.build();
                    ByteBuffer filtered = ByteBuffer.allocate(2048);
                    MemoryRecords.FilterResult filterResult = records.filterTo(new TopicPartition("foo", 0),
                            new MemoryRecords.RecordFilter(0, 0) {
                                @Override
                                protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                                    // retain all batches
                                    return new BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false);
                                }

                                @Override
                                protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                                    // delete the records
                                    return false;
                                }
                            }, filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

                    // Verify filter result
                    assertEquals(numRecords, filterResult.messagesRead());
                    assertEquals(records.sizeInBytes(), filterResult.bytesRead());
                    assertEquals(baseOffset + 1, filterResult.maxOffset());
                    assertEquals(0, filterResult.messagesRetained());
                    assertEquals(DefaultRecordBatch.RECORD_BATCH_OVERHEAD, filterResult.bytesRetained());
                    assertEquals(12, filterResult.maxTimestamp());
                    assertEquals(baseOffset + 1, filterResult.shallowOffsetOfMaxTimestamp());

                    // Verify filtered records
                    filtered.flip();
                    MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

                    List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
                    assertEquals(1, batches.size());

                    MutableRecordBatch batch = batches.get(0);
                    assertEquals(0, batch.countOrNull().intValue());
                    assertEquals(12L, batch.maxTimestamp());
                    assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                    assertEquals(baseOffset, batch.baseOffset());
                    assertEquals(baseOffset + 1, batch.lastOffset());
                    assertEquals(baseSequence, batch.baseSequence());
                    assertEquals(baseSequence + 1, batch.lastSequence());
                    assertEquals(isTransactional, batch.isTransactional());
                }
            }
        }
    }

    @Test
    public void testEmptyBatchRetention() {
        ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        long producerId = 23L;
        short producerEpoch = 5;
        long baseOffset = 3L;
        int baseSequence = 10;
        int partitionLeaderEpoch = 293;
        long timestamp = System.currentTimeMillis();

        DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.MAGIC_VALUE_V2, producerId, producerEpoch,
                baseSequence, baseOffset, baseOffset, partitionLeaderEpoch, TimestampType.CREATE_TIME,
                timestamp, false, false);
        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        MemoryRecords.FilterResult filterResult = records.filterTo(new TopicPartition("foo", 0),
                new MemoryRecords.RecordFilter(0, 0) {
                    @Override
                    protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                        // retain all batches
                        return new BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false);
                    }

                    @Override
                    protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                        return false;
                    }
                }, filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

        // Verify filter result
        assertEquals(0, filterResult.messagesRead());
        assertEquals(records.sizeInBytes(), filterResult.bytesRead());
        assertEquals(baseOffset, filterResult.maxOffset());
        assertEquals(0, filterResult.messagesRetained());
        assertEquals(DefaultRecordBatch.RECORD_BATCH_OVERHEAD, filterResult.bytesRetained());
        assertEquals(timestamp, filterResult.maxTimestamp());
        assertEquals(baseOffset, filterResult.shallowOffsetOfMaxTimestamp());
        assertTrue(filterResult.outputBuffer().position() > 0);

        // Verify filtered records
        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);
        assertEquals(DefaultRecordBatch.RECORD_BATCH_OVERHEAD, filteredRecords.sizeInBytes());
    }

    @Test
    public void testEmptyBatchDeletion() {
        for (final BatchRetention deleteRetention : Arrays.asList(BatchRetention.DELETE, BatchRetention.DELETE_EMPTY)) {
            ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
            long producerId = 23L;
            short producerEpoch = 5;
            long baseOffset = 3L;
            int baseSequence = 10;
            int partitionLeaderEpoch = 293;
            long timestamp = System.currentTimeMillis();

            DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.MAGIC_VALUE_V2, producerId, producerEpoch,
                    baseSequence, baseOffset, baseOffset, partitionLeaderEpoch, TimestampType.CREATE_TIME,
                    timestamp, false, false);
            buffer.flip();

            ByteBuffer filtered = ByteBuffer.allocate(2048);
            MemoryRecords records = MemoryRecords.readableRecords(buffer);
            MemoryRecords.FilterResult filterResult = records.filterTo(new TopicPartition("foo", 0),
                    new MemoryRecords.RecordFilter(0, 0) {
                        @Override
                        protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                            return new BatchRetentionResult(deleteRetention, false);
                        }

                        @Override
                        protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                            return false;
                        }
                    }, filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

            // Verify filter result
            assertEquals(0, filterResult.outputBuffer().position());

            // Verify filtered records
            filtered.flip();
            MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);
            assertEquals(0, filteredRecords.sizeInBytes());
        }
    }

    @Test
    public void testBuildEndTxnMarker() {
        long producerId = 73;
        short producerEpoch = 13;
        long initialOffset = 983L;
        int coordinatorEpoch = 347;
        int partitionLeaderEpoch = 29;

        EndTransactionMarker marker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch);
        MemoryRecords records = MemoryRecords.withEndTransactionMarker(initialOffset, System.currentTimeMillis(),
                partitionLeaderEpoch, producerId, producerEpoch, marker);
        // verify that buffer allocation was precise
        assertEquals(records.buffer().remaining(), records.buffer().capacity());

        List<MutableRecordBatch> batches = TestUtils.toList(records.batches());
        assertEquals(1, batches.size());

        RecordBatch batch = batches.get(0);
        assertTrue(batch.isControlBatch());
        assertEquals(producerId, batch.producerId());
        assertEquals(producerEpoch, batch.producerEpoch());
        assertEquals(initialOffset, batch.baseOffset());
        assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());
        assertTrue(batch.isValid());

        List<Record> createdRecords = TestUtils.toList(batch);
        assertEquals(1, createdRecords.size());

        Record record = createdRecords.get(0);
        record.ensureValid();
        EndTransactionMarker deserializedMarker = EndTransactionMarker.deserialize(record);
        assertEquals(ControlRecordType.COMMIT, deserializedMarker.controlType());
        assertEquals(coordinatorEpoch, deserializedMarker.coordinatorEpoch());
    }

    /**
     * This test is used to see if the base timestamp of the batch has been successfully
     * converted to a delete horizon for the tombstones / transaction markers of the batch.
     * It also verifies that the record timestamps remain correct as a delta relative to the delete horizon.
     */
    @ParameterizedTest
    @ArgumentsSource(V2MemoryRecordsArgumentsProvider.class)
    public void testBaseTimestampToDeleteHorizonConversion(Args args) {
        int partitionLeaderEpoch = 998;
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, args.magic, args.compression, TimestampType.CREATE_TIME,
                0L, RecordBatch.NO_TIMESTAMP, partitionLeaderEpoch);
        builder.append(5L, "0".getBytes(), "0".getBytes());
        builder.append(10L, "1".getBytes(), null);
        builder.append(15L, "2".getBytes(), "2".getBytes());

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        final long deleteHorizon = Integer.MAX_VALUE / 2;
        final RecordFilter recordFilter = new MemoryRecords.RecordFilter(deleteHorizon - 1, 1) {
            @Override
            protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                return true;
            }

            @Override
            protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                return new BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false);
            }
        };
        builder.build().filterTo(new TopicPartition("random", 0), recordFilter, filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);
        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        assertEquals(1, batches.size());
        assertEquals(OptionalLong.of(deleteHorizon), batches.get(0).deleteHorizonMs());

        CloseableIterator<Record> recordIterator = batches.get(0).streamingIterator(BufferSupplier.create());
        Record record = recordIterator.next();
        assertEquals(5L, record.timestamp());
        record = recordIterator.next();
        assertEquals(10L, record.timestamp());
        record = recordIterator.next();
        assertEquals(15L, record.timestamp());
        recordIterator.close();
    }

    @Test
    public void testBuildLeaderChangeMessage() {
        final int leaderId = 5;
        final int leaderEpoch = 20;
        final int voterId = 6;
        long initialOffset = 983L;

        LeaderChangeMessage leaderChangeMessage = new LeaderChangeMessage()
            .setLeaderId(leaderId)
            .setVoters(Collections.singletonList(
                new Voter().setVoterId(voterId)));
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecords records = MemoryRecords.withLeaderChangeMessage(
            initialOffset,
            System.currentTimeMillis(),
            leaderEpoch,
            buffer,
            leaderChangeMessage
        );

        List<MutableRecordBatch> batches = TestUtils.toList(records.batches());
        assertEquals(1, batches.size());

        RecordBatch batch = batches.get(0);
        assertTrue(batch.isControlBatch());
        assertEquals(initialOffset, batch.baseOffset());
        assertEquals(leaderEpoch, batch.partitionLeaderEpoch());
        assertTrue(batch.isValid());

        List<Record> createdRecords = TestUtils.toList(batch);
        assertEquals(1, createdRecords.size());

        Record record = createdRecords.get(0);
        record.ensureValid();
        assertEquals(ControlRecordType.LEADER_CHANGE, ControlRecordType.parse(record.key()));

        LeaderChangeMessage deserializedMessage = ControlRecordUtils.deserializeLeaderChangeMessage(record);
        assertEquals(leaderId, deserializedMessage.leaderId());
        assertEquals(1, deserializedMessage.voters().size());
        assertEquals(voterId, deserializedMessage.voters().get(0).voterId());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testFilterToBatchDiscard(Args args) {
        CompressionConfig compression = args.compression;
        byte magic = args.magic;

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, "1".getBytes(), "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "2".getBytes(), "b".getBytes());
        builder.append(12L, "3".getBytes(), "c".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 3L);
        builder.append(13L, "4".getBytes(), "d".getBytes());
        builder.append(20L, "5".getBytes(), "e".getBytes());
        builder.append(15L, "6".getBytes(), "f".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 6L);
        builder.append(16L, "7".getBytes(), "g".getBytes());
        builder.close();

        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryRecords.readableRecords(buffer).filterTo(new TopicPartition("foo", 0), new MemoryRecords.RecordFilter(0, 0) {
            @Override
            protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                // discard the second and fourth batches
                if (batch.lastOffset() == 2L || batch.lastOffset() == 6L)
                    return new BatchRetentionResult(BatchRetention.DELETE, false);
                return new BatchRetentionResult(BatchRetention.DELETE_EMPTY, false);
            }

            @Override
            protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                return true;
            }
        }, filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        if (compression.getType() != CompressionType.NONE || magic >= MAGIC_VALUE_V2) {
            assertEquals(2, batches.size());
            assertEquals(0, batches.get(0).lastOffset());
            assertEquals(5, batches.get(1).lastOffset());
        } else {
            assertEquals(5, batches.size());
            assertEquals(0, batches.get(0).lastOffset());
            assertEquals(1, batches.get(1).lastOffset());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testFilterToAlreadyCompactedLog(Args args) {
        byte magic = args.magic;
        CompressionConfig compression = args.compression;

        ByteBuffer buffer = ByteBuffer.allocate(2048);

        // create a batch with some offset gaps to simulate a compacted batch
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compression,
                TimestampType.CREATE_TIME, 0L);
        builder.appendWithOffset(5L, 10L, null, "a".getBytes());
        builder.appendWithOffset(8L, 11L, "1".getBytes(), "b".getBytes());
        builder.appendWithOffset(10L, 12L, null, "c".getBytes());

        builder.close();
        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryRecords.readableRecords(buffer).filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(),
                filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);
        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        assertEquals(1, batches.size());

        MutableRecordBatch batch = batches.get(0);
        List<Record> records = TestUtils.toList(batch);
        assertEquals(1, records.size());
        assertEquals(8L, records.get(0).offset());


        if (magic >= RecordBatch.MAGIC_VALUE_V1)
            assertEquals(new SimpleRecord(11L, "1".getBytes(), "b".getBytes()), new SimpleRecord(records.get(0)));
        else
            assertEquals(new SimpleRecord(RecordBatch.NO_TIMESTAMP, "1".getBytes(), "b".getBytes()),
                    new SimpleRecord(records.get(0)));

        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            // the new format preserves first and last offsets from the original batch
            assertEquals(0L, batch.baseOffset());
            assertEquals(10L, batch.lastOffset());
        } else {
            assertEquals(8L, batch.baseOffset());
            assertEquals(8L, batch.lastOffset());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testFilterToPreservesProducerInfo(Args args) {
        byte magic = args.magic;
        CompressionConfig compression = args.compression;
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        // non-idempotent, non-transactional
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        builder.append(11L, "1".getBytes(), "b".getBytes());
        builder.append(12L, null, "c".getBytes());

        builder.close();

        // idempotent
        long pid1 = 23L;
        short epoch1 = 5;
        int baseSequence1 = 10;
        MemoryRecordsBuilder idempotentBuilder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 3L,
                RecordBatch.NO_TIMESTAMP, pid1, epoch1, baseSequence1);
        idempotentBuilder.append(13L, null, "d".getBytes());
        idempotentBuilder.append(14L, "4".getBytes(), "e".getBytes());
        idempotentBuilder.append(15L, "5".getBytes(), "f".getBytes());
        if (magic < MAGIC_VALUE_V2) assertThrows(IllegalArgumentException.class, idempotentBuilder::close);
        else idempotentBuilder.close();


        // transactional
        long pid2 = 99384L;
        short epoch2 = 234;
        int baseSequence2 = 15;
        Supplier<MemoryRecordsBuilder> transactionSupplier = () -> MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 3L,
                RecordBatch.NO_TIMESTAMP, pid2, epoch2, baseSequence2, true, RecordBatch.NO_PARTITION_LEADER_EPOCH);

        if (magic < MAGIC_VALUE_V2) assertThrows(IllegalArgumentException.class, transactionSupplier::get);
        else {
            builder = transactionSupplier.get();
            builder.append(16L, "6".getBytes(), "g".getBytes());
            builder.append(17L, "7".getBytes(), "h".getBytes());
            builder.append(18L, null, "i".getBytes());
            builder.close();

            buffer.flip();

            ByteBuffer filtered = ByteBuffer.allocate(2048);
            MemoryRecords.readableRecords(buffer).filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(),
                    filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

            filtered.flip();
            MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

            List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
            assertEquals(3, batches.size());

            MutableRecordBatch firstBatch = batches.get(0);
            assertEquals(1, firstBatch.countOrNull().intValue());
            assertEquals(0L, firstBatch.baseOffset());
            assertEquals(2L, firstBatch.lastOffset());
            assertEquals(RecordBatch.NO_PRODUCER_ID, firstBatch.producerId());
            assertEquals(RecordBatch.NO_PRODUCER_EPOCH, firstBatch.producerEpoch());
            assertEquals(RecordBatch.NO_SEQUENCE, firstBatch.baseSequence());
            assertEquals(RecordBatch.NO_SEQUENCE, firstBatch.lastSequence());
            assertFalse(firstBatch.isTransactional());
            List<Record> firstBatchRecords = TestUtils.toList(firstBatch);
            assertEquals(1, firstBatchRecords.size());
            assertEquals(RecordBatch.NO_SEQUENCE, firstBatchRecords.get(0).sequence());
            assertEquals(new SimpleRecord(11L, "1".getBytes(), "b".getBytes()), new SimpleRecord(firstBatchRecords.get(0)));

            MutableRecordBatch secondBatch = batches.get(1);
            assertEquals(2, secondBatch.countOrNull().intValue());
            assertEquals(3L, secondBatch.baseOffset());
            assertEquals(5L, secondBatch.lastOffset());
            assertEquals(pid1, secondBatch.producerId());
            assertEquals(epoch1, secondBatch.producerEpoch());
            assertEquals(baseSequence1, secondBatch.baseSequence());
            assertEquals(baseSequence1 + 2, secondBatch.lastSequence());
            assertFalse(secondBatch.isTransactional());
            List<Record> secondBatchRecords = TestUtils.toList(secondBatch);
            assertEquals(2, secondBatchRecords.size());
            assertEquals(baseSequence1 + 1, secondBatchRecords.get(0).sequence());
            assertEquals(new SimpleRecord(14L, "4".getBytes(), "e".getBytes()), new SimpleRecord(secondBatchRecords.get(0)));
            assertEquals(baseSequence1 + 2, secondBatchRecords.get(1).sequence());
            assertEquals(new SimpleRecord(15L, "5".getBytes(), "f".getBytes()), new SimpleRecord(secondBatchRecords.get(1)));

            MutableRecordBatch thirdBatch = batches.get(2);
            assertEquals(2, thirdBatch.countOrNull().intValue());
            assertEquals(3L, thirdBatch.baseOffset());
            assertEquals(5L, thirdBatch.lastOffset());
            assertEquals(pid2, thirdBatch.producerId());
            assertEquals(epoch2, thirdBatch.producerEpoch());
            assertEquals(baseSequence2, thirdBatch.baseSequence());
            assertEquals(baseSequence2 + 2, thirdBatch.lastSequence());
            assertTrue(thirdBatch.isTransactional());
            List<Record> thirdBatchRecords = TestUtils.toList(thirdBatch);
            assertEquals(2, thirdBatchRecords.size());
            assertEquals(baseSequence2, thirdBatchRecords.get(0).sequence());
            assertEquals(new SimpleRecord(16L, "6".getBytes(), "g".getBytes()), new SimpleRecord(thirdBatchRecords.get(0)));
            assertEquals(baseSequence2 + 1, thirdBatchRecords.get(1).sequence());
            assertEquals(new SimpleRecord(17L, "7".getBytes(), "h".getBytes()), new SimpleRecord(thirdBatchRecords.get(1)));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testFilterToWithUndersizedBuffer(Args args) {
        byte magic = args.magic;
        CompressionConfig compression = args.compression;

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "1".getBytes(), new byte[128]);
        builder.append(12L, "2".getBytes(), "c".getBytes());
        builder.append(13L, null, "d".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 4L);
        builder.append(14L, null, "e".getBytes());
        builder.append(15L, "5".getBytes(), "f".getBytes());
        builder.append(16L, "6".getBytes(), "g".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 7L);
        builder.append(17L, "7".getBytes(), new byte[128]);
        builder.close();

        buffer.flip();

        ByteBuffer output = ByteBuffer.allocate(64);

        List<Record> records = new ArrayList<>();
        while (buffer.hasRemaining()) {
            output.rewind();

            MemoryRecords.FilterResult result = MemoryRecords.readableRecords(buffer)
                    .filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(), output, Integer.MAX_VALUE,
                            BufferSupplier.NO_CACHING);

            buffer.position(buffer.position() + result.bytesRead());
            result.outputBuffer().flip();

            if (output != result.outputBuffer())
                assertEquals(0, output.position());

            MemoryRecords filtered = MemoryRecords.readableRecords(result.outputBuffer());
            records.addAll(TestUtils.toList(filtered.records()));
        }

        assertEquals(5, records.size());
        for (Record record : records)
            assertNotNull(record.key());
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testFilterTo(Args args) {
        byte magic = args.magic;
        CompressionConfig compression = args.compression;

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "1".getBytes(), "b".getBytes());
        builder.append(12L, null, "c".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 3L);
        builder.append(13L, null, "d".getBytes());
        builder.append(20L, "4".getBytes(), "e".getBytes());
        builder.append(15L, "5".getBytes(), "f".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 6L);
        builder.append(16L, "6".getBytes(), "g".getBytes());
        builder.close();

        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryRecords.FilterResult result = MemoryRecords.readableRecords(buffer).filterTo(
                new TopicPartition("foo", 0), new RetainNonNullKeysFilter(), filtered, Integer.MAX_VALUE,
                BufferSupplier.NO_CACHING);

        filtered.flip();

        assertEquals(7, result.messagesRead());
        assertEquals(4, result.messagesRetained());
        assertEquals(buffer.limit(), result.bytesRead());
        assertEquals(filtered.limit(), result.bytesRetained());
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            assertEquals(20L, result.maxTimestamp());
            if (compression.getType() == CompressionType.NONE && magic < RecordBatch.MAGIC_VALUE_V2)
                assertEquals(4L, result.shallowOffsetOfMaxTimestamp());
            else
                assertEquals(5L, result.shallowOffsetOfMaxTimestamp());
        }

        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        final List<Long> expectedEndOffsets;
        final List<Long> expectedStartOffsets;
        final List<Long> expectedMaxTimestamps;

        if (magic < RecordBatch.MAGIC_VALUE_V2 && compression.getType() == CompressionType.NONE) {
            expectedEndOffsets = asList(1L, 4L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 5L, 6L);
            expectedMaxTimestamps = asList(11L, 20L, 15L, 16L);
        } else if (magic < RecordBatch.MAGIC_VALUE_V2) {
            expectedEndOffsets = asList(1L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 6L);
            expectedMaxTimestamps = asList(11L, 20L, 16L);
        } else {
            expectedEndOffsets = asList(2L, 5L, 6L);
            expectedStartOffsets = asList(1L, 3L, 6L);
            expectedMaxTimestamps = asList(11L, 20L, 16L);
        }

        assertEquals(expectedEndOffsets.size(), batches.size());

        for (int i = 0; i < expectedEndOffsets.size(); i++) {
            RecordBatch batch = batches.get(i);
            assertEquals(expectedStartOffsets.get(i).longValue(), batch.baseOffset());
            assertEquals(expectedEndOffsets.get(i).longValue(), batch.lastOffset());
            assertEquals(magic, batch.magic());
            assertEquals(compression.getType(), batch.compressionType());
            if (magic >= RecordBatch.MAGIC_VALUE_V1) {
                assertEquals(expectedMaxTimestamps.get(i).longValue(), batch.maxTimestamp());
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            } else {
                assertEquals(RecordBatch.NO_TIMESTAMP, batch.maxTimestamp());
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
            }
        }

        List<Record> records = TestUtils.toList(filteredRecords.records());
        assertEquals(4, records.size());

        Record first = records.get(0);
        assertEquals(1L, first.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(11L, first.timestamp());
        assertEquals("1", Utils.utf8(first.key(), first.keySize()));
        assertEquals("b", Utils.utf8(first.value(), first.valueSize()));

        Record second = records.get(1);
        assertEquals(4L, second.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(20L, second.timestamp());
        assertEquals("4", Utils.utf8(second.key(), second.keySize()));
        assertEquals("e", Utils.utf8(second.value(), second.valueSize()));

        Record third = records.get(2);
        assertEquals(5L, third.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(15L, third.timestamp());
        assertEquals("5", Utils.utf8(third.key(), third.keySize()));
        assertEquals("f", Utils.utf8(third.value(), third.valueSize()));

        Record fourth = records.get(3);
        assertEquals(6L, fourth.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(16L, fourth.timestamp());
        assertEquals("6", Utils.utf8(fourth.key(), fourth.keySize()));
        assertEquals("g", Utils.utf8(fourth.value(), fourth.valueSize()));
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testFilterToPreservesLogAppendTime(Args args) {
        byte magic = args.magic;
        CompressionConfig compression = args.compression;
        long pid = args.pid;
        short epoch = args.epoch;
        int firstSequence = args.firstSequence;
        long logAppendTime = System.currentTimeMillis();

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compression,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, pid, epoch, firstSequence);
        builder.append(10L, null, "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.LOG_APPEND_TIME, 1L, logAppendTime,
                pid, epoch, firstSequence);
        builder.append(11L, "1".getBytes(), "b".getBytes());
        builder.append(12L, null, "c".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.LOG_APPEND_TIME, 3L, logAppendTime,
                pid, epoch, firstSequence);
        builder.append(13L, null, "d".getBytes());
        builder.append(14L, "4".getBytes(), "e".getBytes());
        builder.append(15L, "5".getBytes(), "f".getBytes());
        builder.close();

        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryRecords.readableRecords(buffer).filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(),
                filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        assertEquals(magic < RecordBatch.MAGIC_VALUE_V2 && compression.getType() == CompressionType.NONE ? 3 : 2, batches.size());

        for (RecordBatch batch : batches) {
            assertEquals(compression.getType(), batch.compressionType());
            if (magic > RecordBatch.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
                assertEquals(logAppendTime, batch.maxTimestamp());
            }
        }
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testNextBatchSize(Args args) {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, args.magic, args.compression,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, args.pid, args.epoch, args.firstSequence);
        builder.append(10L, null, "abc".getBytes());
        builder.close();

        buffer.flip();
        int size = buffer.remaining();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assertEquals(size, records.firstBatchSize().intValue());
        assertEquals(0, buffer.position());

        buffer.limit(1); // size not in buffer
        assertNull(records.firstBatchSize());
        buffer.limit(Records.LOG_OVERHEAD); // magic not in buffer
        assertNull(records.firstBatchSize());
        buffer.limit(Records.HEADER_SIZE_UP_TO_MAGIC); // payload not in buffer
        assertEquals(size, records.firstBatchSize().intValue());

        buffer.limit(size);
        byte magic = buffer.get(Records.MAGIC_OFFSET);
        buffer.put(Records.MAGIC_OFFSET, (byte) 10);
        assertThrows(CorruptRecordException.class, records::firstBatchSize);
        buffer.put(Records.MAGIC_OFFSET, magic);

        buffer.put(Records.SIZE_OFFSET + 3, (byte) 0);
        assertThrows(CorruptRecordException.class, records::firstBatchSize);
    }

    @ParameterizedTest
    @ArgumentsSource(MemoryRecordsArgumentsProvider.class)
    public void testWithRecords(Args args) {
        CompressionConfig compression = args.compression;
        byte magic = args.magic;
        MemoryRecords memoryRecords = MemoryRecords.withRecords(magic, compression,
                new SimpleRecord(10L, "key1".getBytes(), "value1".getBytes()));
        String key = Utils.utf8(memoryRecords.batches().iterator().next().iterator().next().key());
        assertEquals("key1", key);
    }

    @Test
    public void testUnsupportedCompress() {
        BiFunction<Byte, CompressionType, MemoryRecords> builderBiFunction = (magic, compressionType) ->
                 MemoryRecords.withRecords(magic, CompressionConfig.of(compressionType).build(), new SimpleRecord(10L, "key1".getBytes(), "value1".getBytes()));

        Arrays.asList(MAGIC_VALUE_V0, MAGIC_VALUE_V1).forEach(magic -> {
            Exception e = assertThrows(IllegalArgumentException.class, () -> builderBiFunction.apply(magic, CompressionType.ZSTD));
            assertEquals(e.getMessage(), "ZStandard compression is not supported for magic " + magic);
        });
    }

    private static class RetainNonNullKeysFilter extends MemoryRecords.RecordFilter {
        public RetainNonNullKeysFilter() {
            super(0, 0);
        }

        @Override
        protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
            return new BatchRetentionResult(BatchRetention.DELETE_EMPTY, false);
        }

        @Override
        public boolean shouldRetainRecord(RecordBatch batch, Record record) {
            return record.hasKey();
        }
    }
}
