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

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EosLogRecordTest {

    @Test
    public void testBasicSerde() {
        KafkaRecord[] records = new KafkaRecord[] {
            new KafkaRecord("hi".getBytes(), "there".getBytes()),
            new KafkaRecord(null, "there".getBytes()),
            new KafkaRecord("hi".getBytes(), null),
            new KafkaRecord(null, null)
        };

        for (boolean isControlRecord : Arrays.asList(true, false)) {
            for (KafkaRecord record : records) {
                int baseSequence = 723;
                long baseOffset = 37;
                int offsetDelta = 10;
                long baseTimestamp = System.currentTimeMillis();
                long timestampDelta = 323;

                ByteBuffer buffer = ByteBuffer.allocate(1024);
                EosLogRecord.writeTo(buffer, isControlRecord, offsetDelta, timestampDelta, record.key(), record.value());
                buffer.flip();

                EosLogRecord logRecord = EosLogRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, null);
                assertNotNull(logRecord);
                assertEquals(baseOffset + offsetDelta, logRecord.offset());
                assertEquals(baseSequence + offsetDelta, logRecord.sequence());
                assertEquals(baseTimestamp + timestampDelta, logRecord.timestamp());
                assertEquals(record.key(), logRecord.key());
                assertEquals(record.value(), logRecord.value());
                assertEquals(isControlRecord, logRecord.isControlRecord());
            }
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
        EosLogRecord.writeTo(buffer, false, offsetDelta, timestampDelta, key, value);
        buffer.flip();

        EosLogRecord record = EosLogRecord.readFrom(buffer, baseOffset, baseTimestamp, LogEntry.NO_SEQUENCE, null);
        assertNotNull(record);
        assertEquals(LogEntry.NO_SEQUENCE, record.sequence());
    }

}
