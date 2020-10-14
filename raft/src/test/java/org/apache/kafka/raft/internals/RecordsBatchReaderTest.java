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

import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordsBatchReaderTest {
    private final StringSerde serde = new StringSerde();
    private final BufferSupplier bufferSupplier = BufferSupplier.create();

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testReadBatch(CompressionType compressionType) {
        long baseOffset = 57;
        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord(Utils.utf8("a")),
            new SimpleRecord(Utils.utf8("b")),
            new SimpleRecord(Utils.utf8("c"))
        };
        MemoryRecords memRecords = MemoryRecords.withRecords(baseOffset, compressionType, records);
        RecordsBatchReader<String> reader = new RecordsBatchReader<>(
            baseOffset,
            memRecords,
            serde,
            bufferSupplier,
            null
        );

        assertTrue(reader.hasNext());
        assertEquals(Arrays.asList("a", "b", "c"), reader.next().records());
        assertFalse(reader.hasNext());
        assertThrows(NoSuchElementException.class, () -> reader.next());
    }

}