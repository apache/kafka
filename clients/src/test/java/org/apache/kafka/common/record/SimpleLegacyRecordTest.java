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

import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class SimpleLegacyRecordTest {

    @Test(expected = InvalidRecordException.class)
    public void testCompressedIterationWithNullValue() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));
        AbstractLegacyRecordBatch.writeHeader(out, 0L, LegacyRecord.RECORD_OVERHEAD_V1);
        LegacyRecord.write(out, RecordBatch.MAGIC_VALUE_V1, 1L, (byte[]) null, null,
                CompressionType.GZIP, TimestampType.CREATE_TIME);

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        if (records.records().iterator().hasNext())
            fail("Iteration should have caused invalid record error");
    }

    @Test(expected = InvalidRecordException.class)
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
        if (records.records().iterator().hasNext())
            fail("Iteration should have caused invalid record error");
    }

    /* This scenario can happen if the record size field is corrupt and we end up allocating a buffer that is too small */
    @Test(expected = InvalidRecordException.class)
    public void testIsValidWithTooSmallBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        LegacyRecord record = new LegacyRecord(buffer);
        assertFalse(record.isValid());
        record.ensureValid();
    }

    @Test(expected = InvalidRecordException.class)
    public void testIsValidWithChecksumMismatch() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        // set checksum
        buffer.putInt(2);
        LegacyRecord record = new LegacyRecord(buffer);
        assertFalse(record.isValid());
        record.ensureValid();
    }

}
