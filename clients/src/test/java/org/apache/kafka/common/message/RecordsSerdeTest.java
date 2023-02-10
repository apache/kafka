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
package org.apache.kafka.common.message;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RecordsSerdeTest {

    @Test
    public void testSerdeRecords() {
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
            new SimpleRecord("foo".getBytes()),
            new SimpleRecord("bar".getBytes()));

        SimpleRecordsMessageData message = new SimpleRecordsMessageData()
            .setTopic("foo")
            .setRecordSet(records);

        testAllRoundTrips(message);
    }

    @Test
    public void testSerdeNullRecords() {
        SimpleRecordsMessageData message = new SimpleRecordsMessageData()
            .setTopic("foo");
        assertNull(message.recordSet());

        testAllRoundTrips(message);
    }

    @Test
    public void testSerdeEmptyRecords() {
        SimpleRecordsMessageData message = new SimpleRecordsMessageData()
            .setTopic("foo")
            .setRecordSet(MemoryRecords.EMPTY);
        testAllRoundTrips(message);
    }

    private void testAllRoundTrips(SimpleRecordsMessageData message) {
        for (short version = SimpleRecordsMessageData.LOWEST_SUPPORTED_VERSION;
             version <= SimpleRecordsMessageData.HIGHEST_SUPPORTED_VERSION;
             version++) {
            testRoundTrip(message, version);
        }
    }

    private void testRoundTrip(SimpleRecordsMessageData message, short version) {
        ByteBuffer buf = MessageUtil.toByteBuffer(message, version);
        SimpleRecordsMessageData message2 = deserialize(buf.duplicate(), version);
        assertEquals(message, message2);
        assertEquals(message.hashCode(), message2.hashCode());
    }

    private SimpleRecordsMessageData deserialize(ByteBuffer buffer, short version) {
        ByteBufferAccessor readable = new ByteBufferAccessor(buffer);
        return new SimpleRecordsMessageData(readable, version);
    }

}
