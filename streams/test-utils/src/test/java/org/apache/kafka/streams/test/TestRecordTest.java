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
package org.apache.kafka.streams.test;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestRecordTest {
    private final StringSerializer stringSerializer = new StringSerializer();
    private final LongSerializer longSerializer = new LongSerializer();

    @Test
    public void shouldAllowNullTopic() {
        new TestRecord<>(null, "key", 42L, 0L, stringSerializer, longSerializer);
    }

    @Test
    public void shouldAllowNullKeySerializerIfKeyIsNull() {
        new TestRecord<>("anyTopic", null, 42L, 0L, null, longSerializer);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfKeySerializerIsNull() {
        new TestRecord<>("anyTopic", "key", 42L, 0L, null, longSerializer);
    }

    @Test
    public void shouldCallKeySerializerIfKeyIsNull() {
        final byte[] nullKey = new byte[0];

        final TestRecord record = new TestRecord<>(
            "anyTopic",
            null,
            42L,
            0L,
            new Serializer<String>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {}

                @Override
                public byte[] serialize(String topic, String data) {
                    return nullKey;
                }

                @Override
                public void close() {}
            },
            longSerializer);

        assertEquals(nullKey, record.key());
    }

    @Test
    public void shouldAllowNullValueSerializerIfValueIsNull() {
        new TestRecord<>("anyTopic", "key", null, 0L, stringSerializer, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfValueSerializerIsNull() {
        new TestRecord<>("anyTopic", "key", 42L, 0L, stringSerializer, null);
    }

    @Test
    public void shouldCallValueSerializerIfValueIsNull() {
        final byte[] nullValue = new byte[0];

        final TestRecord record = new TestRecord<>(
            "anyTopic",
            "key",
            null,
            0L,
            stringSerializer,
            new Serializer<String>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {}

                @Override
                public byte[] serialize(String topic, String data) {
                    return nullValue;
                }

                @Override
                public void close() {}
            });

        assertEquals(nullValue, record.value());
    }

    @Test
    public void shouldCreateTestRecord() {
        final TestRecord record = new TestRecord<>("anyTopic", "key", 42L, 21L, stringSerializer, longSerializer);

        assertEquals("anyTopic", record.topicName());
        assertArrayEquals(stringSerializer.serialize("anyTopic", "key"), record.key());
        assertArrayEquals(longSerializer.serialize("anyTopic", 42L), record.value());
        assertEquals(21L, record.timestamp());
    }

}
