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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DefaultRecordTest {

    @Test
    public void testBasicSerde() {
        Header[] headers = new Header[] {
            new RecordHeader("foo", "value".getBytes()),
            new RecordHeader("bar", (byte[]) null),
            new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
        };

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("hi".getBytes(), "there".getBytes()),
            new SimpleRecord(null, "there".getBytes()),
            new SimpleRecord("hi".getBytes(), null),
            new SimpleRecord(null, null),
            new SimpleRecord(15L, "hi".getBytes(), "there".getBytes(), headers)
        };

        for (SimpleRecord record : records) {
            int baseSequence = 723;
            long baseOffset = 37;
            int offsetDelta = 10;
            long baseTimestamp = System.currentTimeMillis();
            long timestampDelta = 323;

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            DefaultRecord.writeTo(buffer, offsetDelta, timestampDelta, record.key(), record.value(), record.headers());
            buffer.flip();

            DefaultRecord logRecord = DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, null);
            assertNotNull(logRecord);
            assertEquals(baseOffset + offsetDelta, logRecord.offset());
            assertEquals(baseSequence + offsetDelta, logRecord.sequence());
            assertEquals(baseTimestamp + timestampDelta, logRecord.timestamp());
            assertEquals(record.key(), logRecord.key());
            assertEquals(record.value(), logRecord.value());
            assertArrayEquals(record.headers(), logRecord.headers());
            assertEquals(DefaultRecord.sizeInBytes(offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers()), logRecord.sizeInBytes());
        }
    }

    @Test
    public void testSerdeNoSequence() {
        ByteBuffer key = ByteBuffer.wrap("hi".getBytes());
        ByteBuffer value = ByteBuffer.wrap("there".getBytes());
        long baseOffset = 37;
        int offsetDelta = 10;
        long baseTimestamp = System.currentTimeMillis();
        long timestampDelta = 323;

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        DefaultRecord.writeTo(buffer, offsetDelta, timestampDelta, key, value, new Header[0]);
        buffer.flip();

        DefaultRecord record = DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, RecordBatch.NO_SEQUENCE, null);
        assertNotNull(record);
        assertEquals(RecordBatch.NO_SEQUENCE, record.sequence());
    }

}
