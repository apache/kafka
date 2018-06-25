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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Non-parameterized MemoryRecords tests.
 */
public class SimpleMemoryRecordsTest {

    @Test
    public void testToStringIfLz4ChecksumIsCorrupted() {
        long timestamp = 1000000;
        MemoryRecords memoryRecords = MemoryRecords.withRecords(CompressionType.LZ4,
                new SimpleRecord(timestamp, "key1".getBytes(), "value1".getBytes()),
                new SimpleRecord(timestamp + 1, "key2".getBytes(), "value2".getBytes()));
        // Change the lz4 checksum value (not the kafka record crc) so that it doesn't match the contents
        int lz4ChecksumOffset = 6;
        memoryRecords.buffer().array()[DefaultRecordBatch.RECORD_BATCH_OVERHEAD + lz4ChecksumOffset] = 0;
        assertEquals("[(record=CORRUPTED)]", memoryRecords.toString());
    }

}
