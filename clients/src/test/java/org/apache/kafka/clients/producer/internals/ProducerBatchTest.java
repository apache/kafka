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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProducerBatchTest {

    private final long now = 1488748346917L;

    private final MemoryRecordsBuilder memoryRecordsBuilder = MemoryRecords.builder(ByteBuffer.allocate(128),
            CompressionType.NONE, TimestampType.CREATE_TIME, 128);

    /**
     * A {@link ProducerBatch} configured using a very large linger value and a timestamp preceding its create
     * time is interpreted correctly as not expired when the linger time is larger than the difference
     * between now and create time by {@link ProducerBatch#maybeExpire(int, long, long, long, boolean)}.
     */
    @Test
    public void testLargeLingerOldNowExpire() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        // Set `now` to 2ms before the create time.
        assertFalse(batch.maybeExpire(10240, 100L, now - 2L, Long.MAX_VALUE, false));
    }

    /**
     * A {@link ProducerBatch} configured using a very large retryBackoff value with retry = true and a timestamp
     * preceding its create time is interpreted correctly as not expired when the retryBackoff time is larger than the
     * difference between now and create time by {@link ProducerBatch#maybeExpire(int, long, long, long, boolean)}.
     */
    @Test
    public void testLargeRetryBackoffOldNowExpire() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        // Set batch.retry = true
        batch.reenqueued(now);
        // Set `now` to 2ms before the create time.
        assertFalse(batch.maybeExpire(10240, Long.MAX_VALUE, now - 2L, 10240L, false));
    }

    /**
     * A {@link ProducerBatch#maybeExpire(int, long, long, long, boolean)} call with a now value before the create
     * time of the ProducerBatch is correctly recognized as not expired when invoked with parameter isFull = true.
     */
    @Test
    public void testLargeFullOldNowExpire() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        // Set `now` to 2ms before the create time.
        assertFalse(batch.maybeExpire(10240, 10240L, now - 2L, 10240L, true));
    }

    @Test
    public void testShouldNotAttemptAppendOnceRecordsBuilderIsClosedForAppends() {
        ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 1), memoryRecordsBuilder, now);
        FutureRecordMetadata result0 = batch.tryAppend(now, null, new byte[10], Record.EMPTY_HEADERS, null, now);
        assertNotNull(result0);
        assertTrue(memoryRecordsBuilder.hasRoomFor(now, null, new byte[10]));
        memoryRecordsBuilder.closeForRecordAppends();
        assertFalse(memoryRecordsBuilder.hasRoomFor(now, null, new byte[10]));
        assertEquals(null, batch.tryAppend(now + 1, null, new byte[10], Record.EMPTY_HEADERS, null, now + 1));
    }
}
