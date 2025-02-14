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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchBuilderTest {
    private final StringSerde serde = new StringSerde();
    private final MockTime time = new MockTime();

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testBuildBatch(CompressionType compressionType) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        long baseOffset = 57;
        long logAppendTime = time.milliseconds();
        int leaderEpoch = 15;
        Compression compression = Compression.of(compressionType).build();
        BatchBuilder<String> builder = new BatchBuilder<>(
            buffer,
            serde,
            compression,
            baseOffset,
            logAppendTime,
            leaderEpoch,
            buffer.limit()
        );

        List<String> records = Arrays.asList(
            "a",
            "ap",
            "app",
            "appl",
            "apple"
        );

        records.forEach(record -> builder.appendRecord(record, null));
        MemoryRecords builtRecordSet = builder.build();
        assertTrue(builder.bytesNeeded(Collections.singletonList("a"), null).isPresent());
        assertThrows(IllegalStateException.class, () -> builder.appendRecord("a", null));

        List<MutableRecordBatch> builtBatches = Utils.toList(builtRecordSet.batchIterator());
        assertEquals(1, builtBatches.size());
        assertEquals(records, builder.records());

        MutableRecordBatch batch = builtBatches.get(0);
        assertEquals(5, batch.countOrNull());
        assertEquals(compressionType, batch.compressionType());
        assertEquals(baseOffset, batch.baseOffset());
        assertEquals(logAppendTime, batch.maxTimestamp());
        assertFalse(batch.isControlBatch());
        assertEquals(leaderEpoch, batch.partitionLeaderEpoch());

        List<String> builtRecords = Utils.toList(batch).stream()
            .map(record -> Utils.utf8(record.value()))
            .toList();
        assertEquals(records, builtRecords);
    }


    @ParameterizedTest
    @ValueSource(ints = {128, 157, 256, 433, 512, 777, 1024})
    public void testHasRoomForUncompressed(int batchSize) {
        ByteBuffer buffer = ByteBuffer.allocate(batchSize);
        long baseOffset = 57;
        long logAppendTime = time.milliseconds();
        int leaderEpoch = 15;

        BatchBuilder<String> builder = new BatchBuilder<>(
            buffer,
            serde,
            Compression.NONE,
            baseOffset,
            logAppendTime,
            leaderEpoch,
            buffer.limit()
        );

        String record = "i am a record";

        while (builder.bytesNeeded(Collections.singletonList(record), null).isEmpty()) {
            builder.appendRecord(record, null);
        }

        // Approximate size should be exact when compression is not used
        int sizeInBytes = builder.approximateSizeInBytes();
        MemoryRecords records = builder.build();
        assertEquals(sizeInBytes, records.sizeInBytes());
        assertTrue(sizeInBytes <= batchSize, "Built batch size "
            + sizeInBytes + " is larger than max batch size " + batchSize);
    }
}
