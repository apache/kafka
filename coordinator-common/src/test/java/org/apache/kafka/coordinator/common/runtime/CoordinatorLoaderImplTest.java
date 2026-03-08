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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "resource"})
@Timeout(60)
class CoordinatorLoaderImplTest {

    private static class StringKeyValueDeserializer implements Deserializer<Map.Entry<String, String>> {

        @Override
        public Map.Entry<String, String> deserialize(ByteBuffer key, ByteBuffer value) throws RuntimeException {
            return Map.entry(
                StandardCharsets.UTF_8.decode(key).toString(),
                StandardCharsets.UTF_8.decode(value).toString()
            );
        }
    }

    @Test
    void testNonexistentPartition() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.empty();
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.empty();
        Deserializer<Map.Entry<String, String>> serde = mock(Deserializer.class);
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            assertFutureThrows(NotLeaderOrFollowerException.class, loader.load(tp, coordinator));
        }
    }

    @Test
    void testLoadingIsRejectedWhenClosed() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(mock(UnifiedLog.class));
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.empty();
        Deserializer<Map.Entry<String, String>> serde = mock(Deserializer.class);
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            loader.close();
            assertFutureThrows(RuntimeException.class, loader.load(tp, coordinator));
        }
    }

    @Test
    void testLoading() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(9L);
        Deserializer<Map.Entry<String, String>> serde = new StringKeyValueDeserializer();
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            when(log.logStartOffset()).thenReturn(0L);
            when(log.highWatermark()).thenReturn(0L);

            FetchDataInfo readResult1 = logReadResult(0, Arrays.asList(
                new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                new SimpleRecord("k2".getBytes(), "v2".getBytes())
            ));

            when(log.read(0L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult1);

            FetchDataInfo readResult2 = logReadResult(2, Arrays.asList(
                new SimpleRecord("k3".getBytes(), "v3".getBytes()),
                new SimpleRecord("k4".getBytes(), "v4".getBytes()),
                new SimpleRecord("k5".getBytes(), "v5".getBytes())
            ));

            when(log.read(2L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult2);

            FetchDataInfo readResult3 = logReadResult(5, 100L, (short) 5, Arrays.asList(
                new SimpleRecord("k6".getBytes(), "v6".getBytes()),
                new SimpleRecord("k7".getBytes(), "v7".getBytes())
            ));

            when(log.read(5L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult3);

            FetchDataInfo readResult4 = logReadResult(
                7,
                100L,
                (short) 5,
                ControlRecordType.COMMIT
            );

            when(log.read(7L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult4);

            FetchDataInfo readResult5 = logReadResult(
                8,
                500L,
                (short) 10,
                ControlRecordType.ABORT
            );

            when(log.read(8L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult5);

            CoordinatorLoader.LoadSummary summary = loader.load(tp, coordinator).get(10, TimeUnit.SECONDS);
            assertNotNull(summary);
            // Includes 7 normal + 2 control (COMMIT, ABORT)
            assertEquals(9, summary.numRecords());

            verify(coordinator).replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k1", "v1"));
            verify(coordinator).replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k2", "v2"));
            verify(coordinator).replay(2L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k3", "v3"));
            verify(coordinator).replay(3L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k4", "v4"));
            verify(coordinator).replay(4L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k5", "v5"));
            verify(coordinator).replay(5L, 100L, (short) 5, Map.entry("k6", "v6"));
            verify(coordinator).replay(6L, 100L, (short) 5, Map.entry("k7", "v7"));
            verify(coordinator).replayEndTransactionMarker(100L, (short) 5, TransactionResult.COMMIT);
            verify(coordinator).replayEndTransactionMarker(500L, (short) 10, TransactionResult.ABORT);
            verify(coordinator).updateLastWrittenOffset(2L);
            verify(coordinator).updateLastWrittenOffset(5L);
            verify(coordinator).updateLastWrittenOffset(7L);
            verify(coordinator).updateLastWrittenOffset(8L);
            verify(coordinator).updateLastCommittedOffset(0L);
        }
    }

    @Test
    void testLoadingStoppedWhenClosed() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(100L);
        Deserializer<Map.Entry<String, String>> serde = new StringKeyValueDeserializer();
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            when(log.logStartOffset()).thenReturn(0L);

            FetchDataInfo readResult = logReadResult(0, Arrays.asList(
                new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                new SimpleRecord("k2".getBytes(), "v2".getBytes())
            ));

            CountDownLatch latch = new CountDownLatch(1);
            when(log.read(
                anyLong(),
                eq(1000),
                eq(FetchIsolation.LOG_END),
                eq(true)
            )).thenAnswer((InvocationOnMock invocation) -> {
                latch.countDown();
                return readResult;
            });

            CompletableFuture<CoordinatorLoader.LoadSummary> result = loader.load(tp, coordinator);
            boolean completed = latch.await(10, TimeUnit.SECONDS);
            assertTrue(completed, "Log read timeout: Latch did not count down in time.");
            loader.close();

            RuntimeException ex = assertFutureThrows(RuntimeException.class, result);
            assertNotNull(ex);
            assertEquals("Coordinator loader is closed.", ex.getMessage());
        }
    }

    @Test
    void testUnknownRecordTypeAreIgnored() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(2L);
        StringKeyValueDeserializer serde = mock(StringKeyValueDeserializer.class);
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            when(log.logStartOffset()).thenReturn(0L);

            FetchDataInfo readResult = logReadResult(0, Arrays.asList(
                new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                new SimpleRecord("k2".getBytes(), "v2".getBytes())
            ));

            when(log.read(0L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult);

            when(serde.deserialize(any(ByteBuffer.class), any(ByteBuffer.class)))
                .thenThrow(new Deserializer.UnknownRecordTypeException((short) 1))
                .thenReturn(Map.entry("k2", "v2"));

            loader.load(tp, coordinator).get(10, TimeUnit.SECONDS);

            verify(coordinator).replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k2", "v2"));
        }
    }

    @Test
    void testDeserializationErrorFailsTheLoading() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(2L);
        StringKeyValueDeserializer serde = mock(StringKeyValueDeserializer.class);
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            when(log.logStartOffset()).thenReturn(0L);

            FetchDataInfo readResult = logReadResult(0, Arrays.asList(
                new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                new SimpleRecord("k2".getBytes(), "v2".getBytes())
            ));

            when(log.read(0L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult);

            when(serde.deserialize(any(ByteBuffer.class), any(ByteBuffer.class)))
                .thenThrow(new RuntimeException("Error!"));

            RuntimeException ex = assertFutureThrows(RuntimeException.class, loader.load(tp, coordinator));

            assertNotNull(ex);
            assertEquals(String.format("Deserializing record DefaultRecord(offset=0, timestamp=-1, key=2 bytes, value=2 bytes) from %s failed.", tp), ex.getMessage());
        }
    }

    @Test
    void testLoadGroupAndOffsetsWithCorruptedLog() throws Exception {
        // Simulate a case where startOffset < endOffset but log is empty. This could theoretically happen
        // when all the records are expired and the active segment is truncated or when the partition
        // is accidentally corrupted.
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(10L);
        StringKeyValueDeserializer serde = mock(StringKeyValueDeserializer.class);
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            when(log.logStartOffset()).thenReturn(0L);

            FetchDataInfo readResult = logReadResult(0, List.of());

            when(log.read(0L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult);

            assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void testLoadSummary() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(5L);
        StringKeyValueDeserializer serde = new StringKeyValueDeserializer();
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);
        MockTime time = new MockTime();

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            time,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            long startTimeMs = time.milliseconds();
            when(log.logStartOffset()).thenReturn(0L);

            FetchDataInfo readResult1 = logReadResult(0, Arrays.asList(
                new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                new SimpleRecord("k2".getBytes(), "v2".getBytes())
            ));

            when(log.read(0L, 1000, FetchIsolation.LOG_END, true))
                .thenAnswer((InvocationOnMock invocation) -> {
                    time.sleep(1000);
                    return readResult1;
                });

            FetchDataInfo readResult2 = logReadResult(2, Arrays.asList(
                new SimpleRecord("k3".getBytes(), "v3".getBytes()),
                new SimpleRecord("k4".getBytes(), "v4".getBytes()),
                new SimpleRecord("k5".getBytes(), "v5".getBytes())
            ));

            when(log.read(2L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult2);

            CoordinatorLoader.LoadSummary summary = loader.load(tp, coordinator).get(10, TimeUnit.SECONDS);
            assertEquals(startTimeMs, summary.startTimeMs());
            assertEquals(startTimeMs + 1000, summary.endTimeMs());
            assertEquals(5, summary.numRecords());
            assertEquals(readResult1.records.sizeInBytes() + readResult2.records.sizeInBytes(), summary.numBytes());
        }
    }

    @Test
    void testUpdateLastWrittenOffsetOnBatchLoaded() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(7L);
        StringKeyValueDeserializer serde = new StringKeyValueDeserializer();
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            when(log.logStartOffset()).thenReturn(0L);
            when(log.highWatermark()).thenReturn(0L, 0L, 2L);

            FetchDataInfo readResult1 = logReadResult(0, Arrays.asList(
                new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                new SimpleRecord("k2".getBytes(), "v2".getBytes())
            ));

            when(log.read(0L, 1000, FetchIsolation.LOG_END, true))
                    .thenReturn(readResult1);

            FetchDataInfo readResult2 = logReadResult(2, Arrays.asList(
                new SimpleRecord("k3".getBytes(), "v3".getBytes()),
                new SimpleRecord("k4".getBytes(), "v4".getBytes()),
                new SimpleRecord("k5".getBytes(), "v5".getBytes())
            ));

            when(log.read(2L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult2);

            FetchDataInfo readResult3 = logReadResult(5, Arrays.asList(
                new SimpleRecord("k6".getBytes(), "v6".getBytes()),
                new SimpleRecord("k7".getBytes(), "v7".getBytes())
            ));

            when(log.read(5L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult3);

            assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS));

            verify(coordinator).replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k1", "v1"));
            verify(coordinator).replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k2", "v2"));
            verify(coordinator).replay(2L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k3", "v3"));
            verify(coordinator).replay(3L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k4", "v4"));
            verify(coordinator).replay(4L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k5", "v5"));
            verify(coordinator).replay(5L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k6", "v6"));
            verify(coordinator).replay(6L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k7", "v7"));
            verify(coordinator, times(0)).updateLastWrittenOffset(0L);
            verify(coordinator, times(1)).updateLastWrittenOffset(2L);
            verify(coordinator, times(1)).updateLastWrittenOffset(5L);
            verify(coordinator, times(1)).updateLastWrittenOffset(7L);
            verify(coordinator, times(1)).updateLastCommittedOffset(0L);
            verify(coordinator, times(1)).updateLastCommittedOffset(2L);
            verify(coordinator, times(0)).updateLastCommittedOffset(5L);
        }
    }

    @Test
    void testUpdateLastWrittenOffsetAndUpdateLastCommittedOffsetNoRecordsRead() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(0L);
        StringKeyValueDeserializer serde = new StringKeyValueDeserializer();
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            when(log.logStartOffset()).thenReturn(0L);
            when(log.highWatermark()).thenReturn(0L);

            assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS));

            verify(coordinator, times(0)).updateLastWrittenOffset(anyLong());
            verify(coordinator, times(0)).updateLastCommittedOffset(anyLong());
        }
    }

    @Test
    void testUpdateLastWrittenOffsetOnBatchLoadedWhileHighWatermarkAhead() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(7L);
        StringKeyValueDeserializer serde = new StringKeyValueDeserializer();
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            when(log.logStartOffset()).thenReturn(0L);
            when(log.highWatermark()).thenReturn(5L, 7L, 7L);

            FetchDataInfo readResult1 = logReadResult(0, Arrays.asList(
                new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                new SimpleRecord("k2".getBytes(), "v2".getBytes())
            ));

            when(log.read(0L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult1);

            FetchDataInfo readResult2 = logReadResult(2, Arrays.asList(
                new SimpleRecord("k3".getBytes(), "v3".getBytes()),
                new SimpleRecord("k4".getBytes(), "v4".getBytes()),
                new SimpleRecord("k5".getBytes(), "v5".getBytes())
            ));

            when(log.read(2L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult2);

            FetchDataInfo readResult3 = logReadResult(5, Arrays.asList(
                new SimpleRecord("k6".getBytes(), "v6".getBytes()),
                new SimpleRecord("k7".getBytes(), "v7".getBytes())
            ));

            when(log.read(5L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult3);

            assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS));

            verify(coordinator).replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k1", "v1"));
            verify(coordinator).replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k2", "v2"));
            verify(coordinator).replay(2L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k3", "v3"));
            verify(coordinator).replay(3L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k4", "v4"));
            verify(coordinator).replay(4L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k5", "v5"));
            verify(coordinator).replay(5L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k6", "v6"));
            verify(coordinator).replay(6L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k7", "v7"));
            verify(coordinator, times(0)).updateLastWrittenOffset(0L);
            verify(coordinator, times(0)).updateLastWrittenOffset(2L);
            verify(coordinator, times(0)).updateLastWrittenOffset(5L);
            verify(coordinator, times(1)).updateLastWrittenOffset(7L);
            verify(coordinator, times(0)).updateLastCommittedOffset(0L);
            verify(coordinator, times(0)).updateLastCommittedOffset(2L);
            verify(coordinator, times(0)).updateLastCommittedOffset(5L);
            verify(coordinator, times(1)).updateLastCommittedOffset(7L);
        }
    }

    @Test
    void testUpdateLastWrittenOffsetCommitInterval() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = partition -> Optional.of(7L);
        StringKeyValueDeserializer serde = new StringKeyValueDeserializer();
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            2L
        )) {
            when(log.logStartOffset()).thenReturn(0L);
            when(log.highWatermark()).thenReturn(7L);

            FetchDataInfo readResult1 = logReadResult(0, Arrays.asList(
                new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                new SimpleRecord("k2".getBytes(), "v2".getBytes())
            ));

            when(log.read(0L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult1);

            FetchDataInfo readResult2 = logReadResult(2, Arrays.asList(
                new SimpleRecord("k3".getBytes(), "v3".getBytes()),
                new SimpleRecord("k4".getBytes(), "v4".getBytes()),
                new SimpleRecord("k5".getBytes(), "v5".getBytes())
            ));

            when(log.read(2L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult2);

            FetchDataInfo readResult3 = logReadResult(5, Arrays.asList(
                new SimpleRecord("k6".getBytes(), "v6".getBytes())
            ));

            when(log.read(5L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult3);

            FetchDataInfo readResult4 = logReadResult(6, Arrays.asList(
                new SimpleRecord("k7".getBytes(), "v7".getBytes())
            ));

            when(log.read(6L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult4);

            assertNotNull(loader.load(tp, coordinator).get(10, TimeUnit.SECONDS));

            verify(coordinator).replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k1", "v1"));
            verify(coordinator).replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k2", "v2"));
            verify(coordinator).replay(2L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k3", "v3"));
            verify(coordinator).replay(3L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k4", "v4"));
            verify(coordinator).replay(4L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k5", "v5"));
            verify(coordinator).replay(5L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k6", "v6"));
            verify(coordinator).replay(6L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Map.entry("k7", "v7"));
            verify(coordinator, times(0)).updateLastWrittenOffset(0L);
            verify(coordinator, times(1)).updateLastWrittenOffset(2L);
            verify(coordinator, times(1)).updateLastWrittenOffset(5L);
            verify(coordinator, times(0)).updateLastWrittenOffset(6L);
            verify(coordinator, times(1)).updateLastWrittenOffset(7L);
            verify(coordinator, times(0)).updateLastCommittedOffset(0L);
            verify(coordinator, times(1)).updateLastCommittedOffset(2L);
            verify(coordinator, times(1)).updateLastCommittedOffset(5L);
            verify(coordinator, times(0)).updateLastCommittedOffset(6L);
            verify(coordinator, times(1)).updateLastCommittedOffset(7L);
        }
    }

    @Test
    void testPartitionGoesOfflineDuringLoad() throws Exception {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = mock(UnifiedLog.class);
        Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier = partition -> Optional.of(log);
        Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier = mock(Function.class);
        StringKeyValueDeserializer serde = new StringKeyValueDeserializer();
        CoordinatorPlayback<Map.Entry<String, String>> coordinator = mock(CoordinatorPlayback.class);

        try (CoordinatorLoaderImpl<Map.Entry<String, String>> loader = new CoordinatorLoaderImpl<>(
            Time.SYSTEM,
            partitionLogSupplier,
            partitionLogEndOffsetSupplier,
            serde,
            1000,
            CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
        )) {
            when(log.logStartOffset()).thenReturn(0L);
            when(log.highWatermark()).thenReturn(0L);
            when(partitionLogEndOffsetSupplier.apply(tp)).thenReturn(Optional.of(5L)).thenReturn(Optional.of(-1L));

            FetchDataInfo readResult1 = logReadResult(0, Arrays.asList(
                new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                new SimpleRecord("k2".getBytes(), "v2".getBytes())
            ));

            when(log.read(0L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult1);

            FetchDataInfo readResult2 = logReadResult(2, Arrays.asList(
                new SimpleRecord("k3".getBytes(), "v3".getBytes()),
                new SimpleRecord("k4".getBytes(), "v4".getBytes()),
                new SimpleRecord("k5".getBytes(), "v5".getBytes())
            ));

            when(log.read(2L, 1000, FetchIsolation.LOG_END, true))
                .thenReturn(readResult2);

            assertFutureThrows(NotLeaderOrFollowerException.class, loader.load(tp, coordinator));
        }
    }

    private FetchDataInfo logReadResult(long startOffset, List<SimpleRecord> records) throws IOException {
        return logReadResult(startOffset, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, records);
    }

    private FetchDataInfo logReadResult(
        long startOffset,
        long producerId,
        short producerEpoch,
        List<SimpleRecord> records
    ) throws IOException {
        FileRecords fileRecords = mock(FileRecords.class);
        MemoryRecords memoryRecords;
        if (producerId == RecordBatch.NO_PRODUCER_ID) {
            memoryRecords = MemoryRecords.withRecords(
                startOffset,
                Compression.NONE,
                records.toArray(new SimpleRecord[0])
            );
        } else {
            memoryRecords = MemoryRecords.withTransactionalRecords(
                startOffset,
                Compression.NONE,
                producerId,
                producerEpoch,
                0,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                records.toArray(new SimpleRecord[0])
            );
        }

        when(fileRecords.sizeInBytes()).thenReturn(memoryRecords.sizeInBytes());

        doAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0, ByteBuffer.class);
            buffer.put(memoryRecords.buffer().duplicate());
            buffer.flip();
            return null;
        }).when(fileRecords).readInto(any(ByteBuffer.class), ArgumentMatchers.anyInt());

        return new FetchDataInfo(new LogOffsetMetadata(startOffset), fileRecords);
    }

    private FetchDataInfo logReadResult(
        long startOffset,
        long producerId,
        short producerEpoch,
        ControlRecordType controlRecordType
    ) throws IOException {
        FileRecords fileRecords = mock(FileRecords.class);
        MemoryRecords memoryRecords = MemoryRecords.withEndTransactionMarker(
            startOffset,
            0L,
            RecordBatch.NO_PARTITION_LEADER_EPOCH,
            producerId,
            producerEpoch,
            new EndTransactionMarker(controlRecordType, 0)
        );

        when(fileRecords.sizeInBytes()).thenReturn(memoryRecords.sizeInBytes());

        doAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0, ByteBuffer.class);
            buffer.put(memoryRecords.buffer().duplicate());
            buffer.flip();
            return null;
        }).when(fileRecords).readInto(any(ByteBuffer.class), ArgumentMatchers.anyInt());

        return new FetchDataInfo(new LogOffsetMetadata(startOffset), fileRecords);
    }

}
