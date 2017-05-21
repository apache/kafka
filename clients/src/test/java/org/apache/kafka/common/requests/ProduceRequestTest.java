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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProduceRequestTest {

    private final SimpleRecord simpleRecord = new SimpleRecord(System.currentTimeMillis(),
                                                               "key".getBytes(),
                                                               "value".getBytes());

    @Test
    public void shouldBeFlaggedAsTransactionalWhenTransactionalRecords() throws Exception {
        final MemoryRecords memoryRecords = MemoryRecords.withTransactionalRecords(0,
                                                                                   CompressionType.NONE,
                                                                                   1L,
                                                                                   (short) 1,
                                                                                   1,
                                                                                   1,
                                                                                   simpleRecord);
        final ProduceRequest request = new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE,
                                                                  (short) -1,
                                                                  10,
                                                                  Collections.singletonMap(
                                                                          new TopicPartition("topic", 1), memoryRecords)).build();
        assertTrue(request.isTransactional());
    }

    @Test
    public void shouldNotBeFlaggedAsTransactionalWhenNoRecords() throws Exception {
        final ProduceRequest request = createNonIdempotentNonTransactionalRecords();
        assertFalse(request.isTransactional());
    }

    @Test
    public void shouldNotBeFlaggedAsIdempotentWhenRecordsNotIdempotent() throws Exception {
        final ProduceRequest request = createNonIdempotentNonTransactionalRecords();
        assertFalse(request.isTransactional());
    }

    @Test
    public void shouldBeFlaggedAsIdempotentWhenIdempotentRecords() throws Exception {
        final MemoryRecords memoryRecords = MemoryRecords.withIdempotentRecords(1,
                                                                                CompressionType.NONE,
                                                                                1L,
                                                                                (short) 1,
                                                                                1,
                                                                                1,
                                                                                simpleRecord);

        final ProduceRequest request = new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE,
                                                                  (short) -1,
                                                                  10,
                                                                  Collections.singletonMap(
                                                                          new TopicPartition("topic", 1), memoryRecords)).build();
        assertTrue(request.isIdempotent());

    }

    private ProduceRequest createNonIdempotentNonTransactionalRecords() {
        final MemoryRecords memoryRecords = MemoryRecords.withRecords(CompressionType.NONE,
                                                                      simpleRecord);
        return new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE,
                                          (short) -1,
                                          10,
                                          Collections.singletonMap(
                                                  new TopicPartition("topic", 1), memoryRecords)).build();
    }
}