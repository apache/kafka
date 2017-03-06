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

import java.nio.ByteBuffer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class RecordBatchTest {

    /**
     * Arbitrary constant timestamp used as reproducible "now" reference.
     */
    private static final long REFERENCE_NOW = 1488748346917L;

    /**
     * Arbitrary constant MemoryRecordsBuilder for instantiating RecordBatch,
     * when the specifics of its MemoryRecordsBuilder are not relevant.
     */
    private static final MemoryRecordsBuilder MEMORY_RECORDS_BUILDER =
        new MemoryRecordsBuilder(
            ByteBuffer.allocate(0), Record.CURRENT_MAGIC_VALUE,
            CompressionType.NONE,
            TimestampType.CREATE_TIME, 0L, Record.NO_TIMESTAMP, 0
        );

    /**
     * A RecordBatch configured using very large linger value, in combination
     * with a timestamp preceding the create time of a RecordBatch, is
     * interpreted correctly as not expired when the linger time is larger than
     * the difference between now and create time by RecordBatch#maybeExpire.
     */
    @Test
    public void testLargeLingerOldNowExpire() {
        final long now = REFERENCE_NOW;
        final RecordBatch batch = new RecordBatch(
            new TopicPartition("topic", 1), MEMORY_RECORDS_BUILDER, now
        );
        // Use `now` 2ms before the create time.
        assertFalse(
            batch.maybeExpire(10240, 100L, now - 2L, Long.MAX_VALUE, false)
        );
    }

    /**
     * A RecordBatch configured using very large retryBackoff value, in combination
     * with a timestamp preceding the create time of a RecordBatch, is
     * interpreted correctly as not expired when the retryBackoff time is larger than
     * the difference between now and create time by RecordBatch#maybeExpire.
     */
    @Test
    public void testLargeRetryBackoffOldNowExpire() {
        final long now = REFERENCE_NOW;
        final RecordBatch batch = new RecordBatch(
            new TopicPartition("topic", 1), MEMORY_RECORDS_BUILDER, now
        );
        // Use `now` 2ms before the create time.
        assertFalse(
            batch.maybeExpire(10240, Long.MAX_VALUE, now - 2L, 10240L, false)
        );
    }

    /**
     * A RecordBatch#maybeExpire call with a now value before the create time of
     * the RecordBatch as correctly recognized as not expired when invoked with
     * parameter {@code isFull} equal to {@code true}.
     */
    @Test
    public void testLargeFullOldNowExpire() {
        final long now = REFERENCE_NOW;
        final RecordBatch batch = new RecordBatch(
            new TopicPartition("topic", 1), MEMORY_RECORDS_BUILDER, now
        );
        // Use `now` 2ms before the create time.
        assertFalse(
            batch.maybeExpire(10240, 10240L, now - 2L, 10240L, true)
        );
    }
}
