/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class SourceNodeRecordDeserializerTest {

    private final ConsumerRecord<byte[], byte[]> rawRecord = new ConsumerRecord<>("topic",
                                                                                  1,
                                                                                  1,
                                                                                  10,
                                                                                  TimestampType.LOG_APPEND_TIME,
                                                                                  5,
                                                                                  3,
                                                                                  5,
                                                                                  new byte[0],
                                                                                  new byte[0]);


    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfKeyFailsToDeserialize() throws Exception {
        final SourceNodeRecordDeserializer recordDeserializer = new SourceNodeRecordDeserializer(
                new TheSourceNode(true, false));
        recordDeserializer.deserialize(rawRecord);
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfKeyValueFailsToDeserialize() throws Exception {
        final SourceNodeRecordDeserializer recordDeserializer = new SourceNodeRecordDeserializer(
                new TheSourceNode(false, true));
        recordDeserializer.deserialize(rawRecord);
    }

    @Test
    public void shouldReturnNewConsumerRecordWithDeserializedValueWhenNoExceptions() throws Exception {
        final SourceNodeRecordDeserializer recordDeserializer = new SourceNodeRecordDeserializer(
                new TheSourceNode(false, false, "key", "value"));
        final ConsumerRecord<Object, Object> record = recordDeserializer.deserialize(rawRecord);
        assertEquals(rawRecord.topic(), record.topic());
        assertEquals(rawRecord.partition(), record.partition());
        assertEquals(rawRecord.offset(), record.offset());
        assertEquals(rawRecord.checksum(), record.checksum());
        assertEquals("key", record.key());
        assertEquals("value", record.value());
        assertEquals(rawRecord.timestamp(), record.timestamp());
        assertEquals(TimestampType.CREATE_TIME, record.timestampType());
    }

    static class TheSourceNode extends SourceNode {
        private final boolean keyThrowsException;
        private final boolean valueThrowsException;
        private final Object key;
        private final Object value;

        TheSourceNode(final boolean keyThrowsException, final boolean valueThrowsException) {
            this(keyThrowsException, valueThrowsException, null, null);
        }

        @SuppressWarnings("unchecked")
        TheSourceNode(final boolean keyThrowsException,
                      final boolean valueThrowsException,
                      final Object key,
                      final Object value) {
            super("", Collections.EMPTY_LIST, null, null);
            this.keyThrowsException = keyThrowsException;
            this.valueThrowsException = valueThrowsException;
            this.key = key;
            this.value = value;
        }

        @Override
        public Object deserializeKey(final String topic, final byte[] data) {
            if (keyThrowsException) {
                throw new RuntimeException();
            }
            return key;
        }

        @Override
        public Object deserializeValue(final String topic, final byte[] data) {
            if (valueThrowsException) {
                throw new RuntimeException();
            }
            return value;
        }
    }

}