/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.record;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(value = Parameterized.class)
public class MemoryRecordsBuilderTest {

    private final CompressionType compressionType;
    private final int bufferOffset;

    public MemoryRecordsBuilderTest(int bufferOffset, CompressionType compressionType) {
        this.bufferOffset = bufferOffset;
        this.compressionType = compressionType;
    }

    @Test
    public void testCompressionRateV0() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        Record[] records = new Record[] {
                Record.create(Record.MAGIC_VALUE_V0, 0L, "a".getBytes(), "1".getBytes()),
                Record.create(Record.MAGIC_VALUE_V0, 1L, "b".getBytes(), "2".getBytes()),
                Record.create(Record.MAGIC_VALUE_V0, 2L, "c".getBytes(), "3".getBytes()),
        };

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, Record.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, buffer.capacity());

        int uncompressedSize = 0;
        for (Record record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            builder.append(record);
        }

        MemoryRecords built = builder.build();
        if (compressionType == CompressionType.NONE) {
            assertEquals(1.0, builder.compressionRate(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - Record.RECORD_OVERHEAD_V0;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, builder.compressionRate(), 0.00001);
        }
    }

    @Test
    public void testCompressionRateV1() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        Record[] records = new Record[] {
                Record.create(Record.MAGIC_VALUE_V1, 0L, "a".getBytes(), "1".getBytes()),
                Record.create(Record.MAGIC_VALUE_V1, 1L, "b".getBytes(), "2".getBytes()),
                Record.create(Record.MAGIC_VALUE_V1, 2L, "c".getBytes(), "3".getBytes()),
        };

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, Record.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, buffer.capacity());

        int uncompressedSize = 0;
        for (Record record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            builder.append(record);
        }

        MemoryRecords built = builder.build();
        if (compressionType == CompressionType.NONE) {
            assertEquals(1.0, builder.compressionRate(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - Record.RECORD_OVERHEAD_V1;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, builder.compressionRate(), 0.00001);
        }
    }

    @Test
    public void buildUsingLogAppendTime() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, Record.MAGIC_VALUE_V1, compressionType,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(0L, "b".getBytes(), "2".getBytes());
        builder.append(0L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(logAppendTime, info.maxTimestamp);

        assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        for (Record record : records.records()) {
            assertEquals(TimestampType.LOG_APPEND_TIME, record.timestampType());
            assertEquals(logAppendTime, record.timestamp());
        }
    }

    @Test
    public void convertUsingLogAppendTime() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, Record.MAGIC_VALUE_V1, compressionType,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, buffer.capacity());

        builder.convertAndAppend(Record.create(Record.MAGIC_VALUE_V0, 0L, "a".getBytes(), "1".getBytes()));
        builder.convertAndAppend(Record.create(Record.MAGIC_VALUE_V0, 0L, "b".getBytes(), "2".getBytes()));
        builder.convertAndAppend(Record.create(Record.MAGIC_VALUE_V0, 0L, "c".getBytes(), "3".getBytes()));
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(logAppendTime, info.maxTimestamp);
        assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        for (Record record : records.records()) {
            assertEquals(TimestampType.LOG_APPEND_TIME, record.timestampType());
            assertEquals(logAppendTime, record.timestamp());
        }
    }

    @Test
    public void buildUsingCreateTime() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, Record.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(2L, "b".getBytes(), "2".getBytes());
        builder.append(1L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(2L, info.maxTimestamp);

        if (compressionType == CompressionType.NONE)
            assertEquals(1L, info.shallowOffsetOfMaxTimestamp);
        else
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        int i = 0;
        long[] expectedTimestamps = new long[] {0L, 2L, 1L};
        for (Record record : records.records()) {
            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            assertEquals(expectedTimestamps[i++], record.timestamp());
        }
    }

    @Test
    public void writePastLimit() {
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, Record.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(1L, "b".getBytes(), "2".getBytes());

        assertFalse(builder.hasRoomFor("c".getBytes(), "3".getBytes()));
        builder.append(2L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(2L, info.maxTimestamp);
        assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        long i = 0L;
        for (Record record : records.records()) {
            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            assertEquals(i++, record.timestamp());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendAtInvalidOffset() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, Record.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, buffer.capacity());

        builder.appendWithOffset(0L, System.currentTimeMillis(), "a".getBytes(), null);

        // offsets must increase monotonically
        builder.appendWithOffset(0L, System.currentTimeMillis(), "b".getBytes(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendWithInvalidMagic() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, Record.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, buffer.capacity());

        builder.append(Record.create(Record.MAGIC_VALUE_V0, 0L, "a".getBytes(), null));
    }

    @Test
    public void convertUsingCreateTime() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, Record.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, buffer.capacity());

        builder.convertAndAppend(Record.create(Record.MAGIC_VALUE_V0, 0L, "a".getBytes(), "1".getBytes()));
        builder.convertAndAppend(Record.create(Record.MAGIC_VALUE_V0, 0L, "b".getBytes(), "2".getBytes()));
        builder.convertAndAppend(Record.create(Record.MAGIC_VALUE_V0, 0L, "c".getBytes(), "3".getBytes()));
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(Record.NO_TIMESTAMP, info.maxTimestamp);
        assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        for (Record record : records.records()) {
            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            assertEquals(Record.NO_TIMESTAMP, record.timestamp());
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (int bufferOffset : Arrays.asList(0, 15))
            for (CompressionType compressionType : CompressionType.values())
                values.add(new Object[] {bufferOffset, compressionType});
        return values;
    }

}
