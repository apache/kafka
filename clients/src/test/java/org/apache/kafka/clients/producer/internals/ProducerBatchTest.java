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
package org.apache.kafka.clients.producer.internals;

import java.util.Optional;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V0;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V1;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ProducerBatchTest {

    private final long now = 1488748346917L;

    private final MemoryRecordsBuilder memoryRecordsBuilder = MemoryRecords.builder(ByteBuffer.allocate(512),
            CompressionType.NONE, TimestampType.CREATE_TIME, 128);

    @Test
    public void testBatchAbort() throws Exception {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        MockCallback callback = new MockCallback();
        FutureRecordMetadata future = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, callback, now);

        KafkaException exception = new KafkaException();
        batch.abort(exception);
        assertTrue(future.isDone());
        assertEquals(1, callback.invocations);
        assertEquals(exception, callback.exception);
        assertNull(callback.metadata);

        // subsequent completion should be ignored
        assertFalse(batch.complete(500L, 2342342341L));
        assertFalse(batch.completeExceptionally(new KafkaException(), index -> new KafkaException()));
        assertEquals(1, callback.invocations);

        assertTrue(future.isDone());
        try {
            future.get();
            fail("Future should have thrown");
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }
    }

    @Test
    public void testBatchCannotAbortTwice() throws Exception {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        MockCallback callback = new MockCallback();
        FutureRecordMetadata future = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, callback, now);
        KafkaException exception = new KafkaException();
        batch.abort(exception);
        assertEquals(1, callback.invocations);
        assertEquals(exception, callback.exception);
        assertNull(callback.metadata);

        try {
            batch.abort(new KafkaException());
            fail("Expected exception from abort");
        } catch (IllegalStateException e) {
            // expected
        }

        assertEquals(1, callback.invocations);
        assertTrue(future.isDone());
        try {
            future.get();
            fail("Future should have thrown");
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }
    }

    @Test
    public void testBatchCannotCompleteTwice() throws Exception {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        MockCallback callback = new MockCallback();
        FutureRecordMetadata future = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, callback, now);
        batch.complete(500L, 10L);
        assertEquals(1, callback.invocations);
        assertNull(callback.exception);
        assertNotNull(callback.metadata);
        assertThrows(IllegalStateException.class, () -> batch.complete(1000L, 20L));
        RecordMetadata recordMetadata = future.get();
        assertEquals(500L, recordMetadata.offset());
        assertEquals(10L, recordMetadata.timestamp());
    }

    @Test
    public void testSplitPreservesHeaders() {
        for (CompressionType compressionType : CompressionType.values()) {
            MemoryRecordsBuilder builder = MemoryRecords.builder(
                    ByteBuffer.allocate(1024),
                    MAGIC_VALUE_V2,
                    compressionType,
                    TimestampType.CREATE_TIME,
                    0L);
            ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), builder, now);
            Header header = new RecordHeader("header-key", "header-value".getBytes());

            while (true) {
                FutureRecordMetadata future = batch.tryAppend(
                        now, "hi".getBytes(), "there".getBytes(),
                        new Header[]{header}, null, now);
                if (future == null) {
                    break;
                }
            }
            Deque<ProducerBatch> batches = batch.split(200);
            assertTrue(batches.size() >= 2, "This batch should be split to multiple small batches.");

            for (ProducerBatch splitProducerBatch : batches) {
                for (RecordBatch splitBatch : splitProducerBatch.records().batches()) {
                    for (Record record : splitBatch) {
                        assertEquals(1, record.headers().length, "Header size should be 1.");
                        assertEquals("header-key", record.headers()[0].key(), "Header key should be 'header-key'.");
                        assertEquals("header-value", new String(record.headers()[0].value()), "Header value should be 'header-value'.");
                    }
                }
            }
        }
    }

    @Test
    public void testSplitPreservesMagicAndCompressionType() {
        for (byte magic : Arrays.asList(MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2)) {
            for (CompressionType compressionType : CompressionType.values()) {
                if (compressionType == CompressionType.NONE && magic < MAGIC_VALUE_V2)
                    continue;

                if (compressionType == CompressionType.ZSTD && magic < MAGIC_VALUE_V2)
                    continue;

                MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), magic,
                        compressionType, TimestampType.CREATE_TIME, 0L);

                ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), builder, now);
                while (true) {
                    FutureRecordMetadata future = batch.tryAppend(now, "hi".getBytes(), "there".getBytes(),
                            Record.EMPTY_HEADERS, null, now);
                    if (future == null)
                        break;
                }

                Deque<ProducerBatch> batches = batch.split(512);
                assertTrue(batches.size() >= 2);

                for (ProducerBatch splitProducerBatch : batches) {
                    assertEquals(magic, splitProducerBatch.magic());
                    assertTrue(splitProducerBatch.isSplitBatch());

                    for (RecordBatch splitBatch : splitProducerBatch.records().batches()) {
                        assertEquals(magic, splitBatch.magic());
                        assertEquals(0L, splitBatch.baseOffset());
                        assertEquals(compressionType, splitBatch.compressionType());
                    }
                }
            }
        }
    }

    /**
     * A {@link ProducerBatch} configured using a timestamp preceding its create time is interpreted correctly
     * as not expired by {@link ProducerBatch#hasReachedDeliveryTimeout(long, long)}.
     */
    @Test
    public void testBatchExpiration() {
        long deliveryTimeoutMs = 10240;
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        // Set `now` to 2ms before the create time.
        assertFalse(batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now - 2));
        // Set `now` to deliveryTimeoutMs.
        assertTrue(batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now + deliveryTimeoutMs));
    }

    /**
     * A {@link ProducerBatch} configured using a timestamp preceding its create time is interpreted correctly
     * * as not expired by {@link ProducerBatch#hasReachedDeliveryTimeout(long, long)}.
     */
    @Test
    public void testBatchExpirationAfterReenqueue() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        // Set batch.retry = true
        batch.reenqueued(now);
        // Set `now` to 2ms before the create time.
        assertFalse(batch.hasReachedDeliveryTimeout(10240, now - 2L));
    }

    @Test
    public void testShouldNotAttemptAppendOnceRecordsBuilderIsClosedForAppends() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        FutureRecordMetadata result0 = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, null, now);
        assertNotNull(result0);
        assertTrue(memoryRecordsBuilder.hasRoomFor(now, null, new byte[10], Record.EMPTY_HEADERS));
        memoryRecordsBuilder.closeForRecordAppends();
        assertFalse(memoryRecordsBuilder.hasRoomFor(now, null, new byte[10], Record.EMPTY_HEADERS));
        assertNull(batch.tryAppend(now + 1, null, new byte[10], Record.EMPTY_HEADERS, null, now + 1));
    }

    @Test
    public void testCompleteExceptionallyWithRecordErrors() {
        int recordCount = 5;
        RuntimeException topLevelException = new RuntimeException();

        Map<Integer, RuntimeException> recordExceptionMap = new HashMap<>();
        recordExceptionMap.put(0, new RuntimeException());
        recordExceptionMap.put(3, new RuntimeException());

        Function<Integer, RuntimeException> recordExceptions = batchIndex ->
            recordExceptionMap.getOrDefault(batchIndex, topLevelException);

        testCompleteExceptionally(recordCount, topLevelException, recordExceptions);
    }

    @Test
    public void testCompleteExceptionallyWithNullRecordErrors() {
        int recordCount = 5;
        RuntimeException topLevelException = new RuntimeException();
        assertThrows(NullPointerException.class, () ->
            testCompleteExceptionally(recordCount, topLevelException, null));
    }

    /**
     * This tests that leader is correctly maintained & leader-change is correctly detected across retries
     * of the batch. It does so by testing primarily testing methods
     * 1. maybeUpdateLeaderEpoch
     * 2. hasLeaderChangedForTheOngoingRetry
     */

    @Test
    public void testWithLeaderChangesAcrossRetries() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);

        // Starting state for the batch, no attempt made to send it yet.
        assertEquals(Optional.empty(), batch.currentLeaderEpoch());
        assertEquals(0, batch.attemptsWhenLeaderLastChanged()); // default value
        batch.maybeUpdateLeaderEpoch(Optional.empty());
        assertFalse(batch.hasLeaderChangedForTheOngoingRetry());

        // 1st attempt[Not a retry] to send the batch.
        // Check leader isn't flagged as a new leader.
        int batchLeaderEpoch = 100;
        batch.maybeUpdateLeaderEpoch(Optional.of(batchLeaderEpoch));
        assertFalse(batch.hasLeaderChangedForTheOngoingRetry(), "batch leader is assigned for 1st time");
        assertEquals(batchLeaderEpoch, batch.currentLeaderEpoch().get());
        assertEquals(0, batch.attemptsWhenLeaderLastChanged());

        // 2nd attempt[1st retry] to send the batch to a new leader.
        // Check leader change is detected.
        batchLeaderEpoch = 101;
        batch.reenqueued(0);
        batch.maybeUpdateLeaderEpoch(Optional.of(batchLeaderEpoch));
        assertTrue(batch.hasLeaderChangedForTheOngoingRetry(), "batch leader has changed");
        assertEquals(batchLeaderEpoch, batch.currentLeaderEpoch().get());
        assertEquals(1, batch.attemptsWhenLeaderLastChanged());

        // 2nd attempt[1st retry] still ongoing, yet to be made.
        // Check same leaderEpoch(101) is still considered as a leader-change.
        batch.maybeUpdateLeaderEpoch(Optional.of(batchLeaderEpoch));
        assertTrue(batch.hasLeaderChangedForTheOngoingRetry(), "batch leader has changed");
        assertEquals(batchLeaderEpoch, batch.currentLeaderEpoch().get());
        assertEquals(1, batch.attemptsWhenLeaderLastChanged());

        // 3rd attempt[2nd retry] to the same leader-epoch(101).
        // Check same leaderEpoch(101) as not detected as a leader-change.
        batch.reenqueued(0);
        batch.maybeUpdateLeaderEpoch(Optional.of(batchLeaderEpoch));
        assertFalse(batch.hasLeaderChangedForTheOngoingRetry(), "batch leader has not changed");
        assertEquals(batchLeaderEpoch, batch.currentLeaderEpoch().get());
        assertEquals(1, batch.attemptsWhenLeaderLastChanged());
    }

    private void testCompleteExceptionally(
        int recordCount,
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        ProducerBatch batch = new ProducerBatch(
            new TopicPartition("topic", 1),
            memoryRecordsBuilder,
            now
        );

        List<FutureRecordMetadata> futures = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            futures.add(batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, null, now));
        }
        assertEquals(recordCount, batch.recordCount);

        batch.completeExceptionally(topLevelException, recordExceptions);
        assertTrue(batch.isDone());

        for (int i = 0; i < futures.size(); i++) {
            FutureRecordMetadata future = futures.get(i);
            RuntimeException caughtException = TestUtils.assertFutureThrows(future, RuntimeException.class);
            RuntimeException expectedException = recordExceptions.apply(i);
            assertEquals(expectedException, caughtException);
        }
    }

    private static class MockCallback implements Callback {
        private int invocations = 0;
        private RecordMetadata metadata;
        private Exception exception;

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            invocations++;
            this.metadata = metadata;
            this.exception = exception;
        }
    }

}
