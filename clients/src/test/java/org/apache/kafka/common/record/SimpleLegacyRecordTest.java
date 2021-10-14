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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SimpleLegacyRecordTest {

    @Test
    public void testCompressedIterationWithNullValue() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));
        AbstractLegacyRecordBatch.writeHeader(out, 0L, LegacyRecord.RECORD_OVERHEAD_V1);
        LegacyRecord.write(out, RecordBatch.MAGIC_VALUE_V1, 1L, (byte[]) null, null,
                CompressionType.GZIP, TimestampType.CREATE_TIME);

        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assertThrows(InvalidRecordException.class, () -> records.records().iterator().hasNext());
    }

    @Test
    public void testCompressedIterationWithEmptyRecords() throws Exception {
        ByteBuffer emptyCompressedValue = ByteBuffer.allocate(64);
        OutputStream gzipOutput = CompressionType.GZIP.wrapForOutput(new ByteBufferOutputStream(emptyCompressedValue),
                RecordBatch.MAGIC_VALUE_V1);
        gzipOutput.close();
        emptyCompressedValue.flip();

        ByteBuffer buffer = ByteBuffer.allocate(128);
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));
        AbstractLegacyRecordBatch.writeHeader(out, 0L, LegacyRecord.RECORD_OVERHEAD_V1 + emptyCompressedValue.remaining());
        LegacyRecord.write(out, RecordBatch.MAGIC_VALUE_V1, 1L, null, Utils.toArray(emptyCompressedValue),
                CompressionType.GZIP, TimestampType.CREATE_TIME);

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assertThrows(InvalidRecordException.class, () -> records.records().iterator().hasNext());
    }

    /* This scenario can happen if the record size field is corrupt and we end up allocating a buffer that is too small */
    @Test
    public void testIsValidWithTooSmallBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        LegacyRecord record = new LegacyRecord(buffer);
        assertFalse(record.isValid());
        assertThrows(CorruptRecordException.class, record::ensureValid);

    }

    @Test
    public void testIsValidWithChecksumMismatch() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        // set checksum
        buffer.putInt(2);
        LegacyRecord record = new LegacyRecord(buffer);
        assertFalse(record.isValid());
        assertThrows(CorruptRecordException.class, record::ensureValid);
    }

}
