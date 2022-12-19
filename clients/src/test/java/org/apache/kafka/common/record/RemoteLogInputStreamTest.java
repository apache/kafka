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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V0;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V1;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.apache.kafka.common.record.TimestampType.CREATE_TIME;
import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoteLogInputStreamTest {

    private static class Args {
        private final byte magic;
        private final CompressionType compression;

        public Args(byte magic, CompressionType compression) {
            this.magic = magic;
            this.compression = compression;
        }

        @Override
        public String toString() {
            return "Args{magic=" + magic + ", compression=" + compression + "}";
        }
    }

    private static class RemoteLogInputStreamArgsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            List<Arguments> values = new ArrayList<>();
            for (byte magic : asList(MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2)) {
                for (CompressionType type : CompressionType.values()) {
                    values.add(Arguments.of(new Args(magic, type)));
                }
            }
            return values.stream();
        }
    }

    @ParameterizedTest
    @ArgumentsSource(RemoteLogInputStreamArgsProvider.class)
    public void testSimpleBatchIteration(Args args) throws IOException {
        byte magic = args.magic;
        CompressionType compression = args.compression;
        if (compression == CompressionType.ZSTD && magic < MAGIC_VALUE_V2)
            return;

        SimpleRecord firstBatchRecord = new SimpleRecord(3241324L, "a".getBytes(), "foo".getBytes());
        SimpleRecord secondBatchRecord = new SimpleRecord(234280L, "b".getBytes(), "bar".getBytes());

        File file = tempFile();
        try (FileRecords fileRecords = FileRecords.open(file)) {
            fileRecords.append(MemoryRecords.withRecords(magic, 0L, compression, CREATE_TIME, firstBatchRecord));
            fileRecords.append(MemoryRecords.withRecords(magic, 1L, compression, CREATE_TIME, secondBatchRecord));
            fileRecords.flush();
        }

        try (FileInputStream is = new FileInputStream(file)) {
            RemoteLogInputStream logInputStream = new RemoteLogInputStream(is);

            RecordBatch firstBatch = logInputStream.nextBatch();
            assertGenericRecordBatchData(args, firstBatch, 0L, 3241324L, firstBatchRecord);
            assertNoProducerData(firstBatch);

            RecordBatch secondBatch = logInputStream.nextBatch();
            assertGenericRecordBatchData(args, secondBatch, 1L, 234280L, secondBatchRecord);
            assertNoProducerData(secondBatch);

            assertNull(logInputStream.nextBatch());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(RemoteLogInputStreamArgsProvider.class)
    public void testBatchIterationWithMultipleRecordsPerBatch(Args args) throws IOException {
        byte magic = args.magic;
        CompressionType compression = args.compression;
        if (magic < MAGIC_VALUE_V2 && compression == CompressionType.NONE)
            return;

        if (compression == CompressionType.ZSTD && magic < MAGIC_VALUE_V2)
            return;

        SimpleRecord[] firstBatchRecords = new SimpleRecord[]{
            new SimpleRecord(3241324L, "a".getBytes(), "1".getBytes()),
            new SimpleRecord(234280L, "b".getBytes(), "2".getBytes())
        };

        SimpleRecord[] secondBatchRecords = new SimpleRecord[]{
            new SimpleRecord(238423489L, "c".getBytes(), "3".getBytes()),
            new SimpleRecord(897839L, null, "4".getBytes()),
            new SimpleRecord(8234020L, "e".getBytes(), null)
        };

        File file = tempFile();
        try (FileRecords fileRecords = FileRecords.open(file)) {
            fileRecords.append(MemoryRecords.withRecords(magic, 0L, compression, CREATE_TIME, firstBatchRecords));
            fileRecords.append(MemoryRecords.withRecords(magic, 1L, compression, CREATE_TIME, secondBatchRecords));
            fileRecords.flush();
        }

        try (FileInputStream is = new FileInputStream(file)) {
            RemoteLogInputStream logInputStream = new RemoteLogInputStream(is);

            RecordBatch firstBatch = logInputStream.nextBatch();
            assertNoProducerData(firstBatch);
            assertGenericRecordBatchData(args, firstBatch, 0L, 3241324L, firstBatchRecords);

            RecordBatch secondBatch = logInputStream.nextBatch();
            assertNoProducerData(secondBatch);
            assertGenericRecordBatchData(args, secondBatch, 1L, 238423489L, secondBatchRecords);

            assertNull(logInputStream.nextBatch());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(RemoteLogInputStreamArgsProvider.class)
    public void testBatchIterationV2(Args args) throws IOException {
        byte magic = args.magic;
        CompressionType compression = args.compression;
        if (magic != MAGIC_VALUE_V2)
            return;

        long producerId = 83843L;
        short producerEpoch = 15;
        int baseSequence = 234;
        int partitionLeaderEpoch = 9832;

        Header[] headers = new Header[]{new RecordHeader("header-key",
                "header-value".getBytes(StandardCharsets.UTF_8))};
        SimpleRecord[] firstBatchRecords = new SimpleRecord[]{
            new SimpleRecord(3241324L, "a".getBytes(), "1".getBytes()),
            // Add a record with headers.
            new SimpleRecord(234280L, "b".getBytes(), "2".getBytes(), headers)
        };

        SimpleRecord[] secondBatchRecords = new SimpleRecord[]{
            new SimpleRecord(238423489L, "c".getBytes(), "3".getBytes()),
            new SimpleRecord(897839L, null, "4".getBytes()),
            new SimpleRecord(8234020L, "e".getBytes(), null)
        };

        File file = tempFile();
        try (FileRecords fileRecords = FileRecords.open(file)) {
            fileRecords.append(MemoryRecords.withIdempotentRecords(magic, 15L, compression, producerId,
                                                                   producerEpoch, baseSequence, partitionLeaderEpoch, firstBatchRecords));
            fileRecords.append(MemoryRecords.withTransactionalRecords(magic, 27L, compression, producerId,
                                                                      producerEpoch, baseSequence + firstBatchRecords.length, partitionLeaderEpoch, secondBatchRecords));
            fileRecords.flush();
        }

        try (FileInputStream is = new FileInputStream(file)) {
            RemoteLogInputStream logInputStream = new RemoteLogInputStream(is);

            RecordBatch firstBatch = logInputStream.nextBatch();
            assertProducerData(firstBatch, producerId, producerEpoch, baseSequence, false, firstBatchRecords);
            assertGenericRecordBatchData(args, firstBatch, 15L, 3241324L, firstBatchRecords);
            assertEquals(partitionLeaderEpoch, firstBatch.partitionLeaderEpoch());

            RecordBatch secondBatch = logInputStream.nextBatch();
            assertProducerData(secondBatch, producerId, producerEpoch, baseSequence + firstBatchRecords.length,
                               true, secondBatchRecords);
            assertGenericRecordBatchData(args, secondBatch, 27L, 238423489L, secondBatchRecords);
            assertEquals(partitionLeaderEpoch, secondBatch.partitionLeaderEpoch());

            assertNull(logInputStream.nextBatch());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(RemoteLogInputStreamArgsProvider.class)
    public void testBatchIterationIncompleteBatch(Args args) throws IOException {
        byte magic = args.magic;
        CompressionType compression = args.compression;
        if (compression == CompressionType.ZSTD && magic < MAGIC_VALUE_V2)
            return;

        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            SimpleRecord firstBatchRecord = new SimpleRecord(100L, "foo".getBytes());
            SimpleRecord secondBatchRecord = new SimpleRecord(200L, "bar".getBytes());

            fileRecords.append(MemoryRecords.withRecords(magic, 0L, compression, CREATE_TIME, firstBatchRecord));
            fileRecords.append(MemoryRecords.withRecords(magic, 1L, compression, CREATE_TIME, secondBatchRecord));
            fileRecords.flush();
            fileRecords.truncateTo(fileRecords.sizeInBytes() - 13);

            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords, 0, fileRecords.sizeInBytes());

            FileLogInputStream.FileChannelRecordBatch firstBatch = logInputStream.nextBatch();
            assertNoProducerData(firstBatch);
            assertGenericRecordBatchData(args, firstBatch, 0L, 100L, firstBatchRecord);

            assertNull(logInputStream.nextBatch());
        }
    }

    private void assertProducerData(RecordBatch batch, long producerId, short producerEpoch, int baseSequence,
                                    boolean isTransactional, SimpleRecord... records) {
        assertEquals(producerId, batch.producerId());
        assertEquals(producerEpoch, batch.producerEpoch());
        assertEquals(baseSequence, batch.baseSequence());
        assertEquals(baseSequence + records.length - 1, batch.lastSequence());
        assertEquals(isTransactional, batch.isTransactional());
    }

    private void assertNoProducerData(RecordBatch batch) {
        assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch());
        assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
        assertEquals(RecordBatch.NO_SEQUENCE, batch.lastSequence());
        assertFalse(batch.isTransactional());
    }

    private void assertGenericRecordBatchData(Args args,
                                              RecordBatch batch,
                                              long baseOffset,
                                              long maxTimestamp,
                                              SimpleRecord... records) {
        byte magic = args.magic;
        CompressionType compression = args.compression;
        assertEquals(magic, batch.magic());
        assertEquals(compression, batch.compressionType());

        if (magic == MAGIC_VALUE_V0) {
            assertEquals(NO_TIMESTAMP_TYPE, batch.timestampType());
        } else {
            assertEquals(CREATE_TIME, batch.timestampType());
            assertEquals(maxTimestamp, batch.maxTimestamp());
        }

        assertEquals(baseOffset + records.length - 1, batch.lastOffset());
        if (magic >= MAGIC_VALUE_V2)
            assertEquals(Integer.valueOf(records.length), batch.countOrNull());

        assertEquals(baseOffset, batch.baseOffset());
        assertTrue(batch.isValid());

        List<Record> batchRecords = TestUtils.toList(batch);
        for (int i = 0; i < records.length; i++) {
            assertEquals(baseOffset + i, batchRecords.get(i).offset());
            assertEquals(records[i].key(), batchRecords.get(i).key());
            assertEquals(records[i].value(), batchRecords.get(i).value());
            assertArrayEquals(records[i].headers(), batchRecords.get(i).headers());
            if (magic == MAGIC_VALUE_V0)
                assertEquals(NO_TIMESTAMP, batchRecords.get(i).timestamp());
            else
                assertEquals(records[i].timestamp(), batchRecords.get(i).timestamp());
        }
    }
}
