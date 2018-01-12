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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ConsumerRecordFactoryConsumer {
    private final StringSerializer stringSerializer = new StringSerializer();
    private final IntegerSerializer integerSerializer = new IntegerSerializer();

    private final String topicName = "topic";
    private final String otherTopicName = "otherTopic";
    private final String key = "key";
    private final Integer value = 42;
    private final long timestamp = 21L;
    private final byte[] rawKey = stringSerializer.serialize(topicName, key);
    private final byte[] rawValue = integerSerializer.serialize(topicName, value);

    private final ConsumerRecordFactory<byte[], Integer> factory =
        new ConsumerRecordFactory<>(topicName, new ByteArraySerializer(), integerSerializer, 0L);

    private ConsumerRecord<byte[], byte[]> record;

    @Test
    public void shouldAdvanceTime() {
        factory.advanceTimeMs(3L);
        record = factory.create(topicName, rawKey, value);
        verifyRecord(topicName, rawKey, rawValue, 3L, record);

        factory.advanceTimeMs(2L);
        record = factory.create(topicName, rawKey, value);
        verifyRecord(topicName, rawKey, rawValue, 5L, record);
    }

    @Test
    public void shouldNotAllowToCreateTopicWithNullTopicName() {
        final String nullTopicName = null;

        try {
            factory.create(nullTopicName, rawKey, value, timestamp);
            fail("Should have throw IllegalArgumentException");
        } catch (final IllegalArgumentException expected) { /* pass */ }

        try {
            factory.create(nullTopicName, rawKey, value);
            fail("Should have throw IllegalArgumentException");
        } catch (final IllegalArgumentException expected) { /* pass */ }

        try {
            factory.create(nullTopicName, value, timestamp);
            fail("Should have throw IllegalArgumentException");
        } catch (final IllegalArgumentException expected) { /* pass */ }

        try {
            factory.create(nullTopicName, value);
            fail("Should have throw IllegalArgumentException");
        } catch (final IllegalArgumentException expected) { /* pass */ }

        try {
            factory.create(nullTopicName, Collections.singletonList(KeyValue.pair(rawKey, value)));
            fail("Should have throw IllegalArgumentException");
        } catch (final IllegalArgumentException expected) { /* pass */ }

        try {
            factory.create(nullTopicName, Collections.singletonList(KeyValue.pair(rawKey, value)), timestamp, 2L);
            fail("Should have throw IllegalArgumentException");
        } catch (final IllegalArgumentException expected) { /* pass */ }
    }

    @Test
    public void shouldRequireCustomTopicNameIfNotDefaultFactoryTopicName() {
        final ConsumerRecordFactory<byte[], Integer> factory =
            new ConsumerRecordFactory<>(new ByteArraySerializer(), integerSerializer);

        try {
            factory.create(rawKey, value, timestamp);
            fail("Should have throw IllegalStateException");
        } catch (final IllegalStateException expected) { /* pass */ }

        try {
            factory.create(rawKey, value);
            fail("Should have throw IllegalStateException");
        } catch (final IllegalStateException expected) { /* pass */ }

        try {
            factory.create(value, timestamp);
            fail("Should have throw IllegalStateException");
        } catch (final IllegalStateException expected) { /* pass */ }

        try {
            factory.create(value);
            fail("Should have throw IllegalStateException");
        } catch (final IllegalStateException expected) { /* pass */ }

        try {
            factory.create(Collections.singletonList(KeyValue.pair(rawKey, value)));
            fail("Should have throw IllegalStateException");
        } catch (final IllegalStateException expected) { /* pass */ }

        try {
            factory.create(Collections.singletonList(KeyValue.pair(rawKey, value)), timestamp, 2L);
            fail("Should have throw IllegalStateException");
        } catch (final IllegalStateException expected) { /* pass */ }
    }

    @Test
    public void shouldCreateConsumerRecordWithOtherTopicNameAndTimestamp() {
        record = factory.create(otherTopicName, rawKey, value, timestamp);
        verifyRecord(otherTopicName, rawKey, rawValue, timestamp, record);
    }

    @Test
    public void shouldCreateConsumerRecordWithTimestamp() {
        record = factory.create(rawKey, value, timestamp);
        verifyRecord(topicName, rawKey, rawValue, timestamp, record);
    }

    @Test
    public void shouldCreateConsumerRecordWithOtherTopicName() {
        record = factory.create(otherTopicName, rawKey, value);
        verifyRecord(otherTopicName, rawKey, rawValue, 0L, record);

        factory.advanceTimeMs(3L);
        record = factory.create(otherTopicName, rawKey, value);
        verifyRecord(otherTopicName, rawKey, rawValue, 3L, record);
    }

    @Test
    public void shouldCreateConsumerRecord() {
        record = factory.create(rawKey, value);
        verifyRecord(topicName, rawKey, rawValue, 0L, record);

        factory.advanceTimeMs(3L);
        record = factory.create(rawKey, value);
        verifyRecord(topicName, rawKey, rawValue, 3L, record);
    }

    @Test
    public void shouldCreateNullKeyConsumerRecordWithOtherTopicNameAndTimestampWithTimetamp() {
        record = factory.create(value, timestamp);
        verifyRecord(topicName, null, rawValue, timestamp, record);
    }

    @Test
    public void shouldCreateNullKeyConsumerRecordWithTimestampWithTimetamp() {
        record = factory.create(value, timestamp);
        verifyRecord(topicName, null, rawValue, timestamp, record);
    }

    @Test
    public void shouldCreateNullKeyConsumerRecord() {
        record = factory.create(value);
        verifyRecord(topicName, null, rawValue, 0L, record);

        factory.advanceTimeMs(3L);
        record = factory.create(value);
        verifyRecord(topicName, null, rawValue, 3L, record);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCreateConsumerRecordsFromKeyValuePairs() {
        final ConsumerRecordFactory<String, Integer> factory =
            new ConsumerRecordFactory<>(topicName, stringSerializer, integerSerializer, 0L);

        final KeyValue[] keyValuePairs = new KeyValue[5];
        final KeyValue[] rawKeyValuePairs = new KeyValue[keyValuePairs.length];

        for (int i = 0; i < keyValuePairs.length; ++i) {
            keyValuePairs[i] = KeyValue.pair(key + "-" + i, value + i);
            rawKeyValuePairs[i] = KeyValue.pair(
                stringSerializer.serialize(topicName, key + "-" + i),
                integerSerializer.serialize(topicName, value + i));
        }

        final List<ConsumerRecord<byte[], byte[]>> records =
            factory.create(Arrays.<KeyValue<String, Long>>asList(keyValuePairs));

        for (int i = 0; i < keyValuePairs.length; ++i) {
            verifyRecord(
                topicName,
                (byte[]) rawKeyValuePairs[i].key,
                (byte[]) rawKeyValuePairs[i].value,
                0L,
                records.get(i));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCreateConsumerRecordsFromKeyValuePairsWithTimestampAndIncrements() {
        final ConsumerRecordFactory<String, Integer> factory =
            new ConsumerRecordFactory<>(topicName, stringSerializer, integerSerializer, timestamp, 2L);

        final KeyValue[] keyValuePairs = new KeyValue[5];
        final KeyValue[] rawKeyValuePairs = new KeyValue[keyValuePairs.length];

        for (int i = 0; i < keyValuePairs.length; ++i) {
            keyValuePairs[i] = KeyValue.pair(key + "-" + i, value + i);
            rawKeyValuePairs[i] = KeyValue.pair(
                stringSerializer.serialize(topicName, key + "-" + i),
                integerSerializer.serialize(topicName, value + i));
        }

        final List<ConsumerRecord<byte[], byte[]>> records =
            factory.create(Arrays.<KeyValue<String, Long>>asList(keyValuePairs));

        for (int i = 0; i < keyValuePairs.length; ++i) {
            verifyRecord(
                topicName,
                (byte[]) rawKeyValuePairs[i].key,
                (byte[]) rawKeyValuePairs[i].value,
                timestamp + 2L * i,
                records.get(i));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCreateConsumerRecordsFromKeyValuePairsWithCustomTimestampAndIncrementsAndNotAdvanceTime() {
        final ConsumerRecordFactory<String, Integer> factory =
            new ConsumerRecordFactory<>(topicName, stringSerializer, integerSerializer, 0L);

        final KeyValue[] keyValuePairs = new KeyValue[5];
        final KeyValue[] rawKeyValuePairs = new KeyValue[keyValuePairs.length];

        for (int i = 0; i < keyValuePairs.length; ++i) {
            keyValuePairs[i] = KeyValue.pair(key + "-" + i, value + i);
            rawKeyValuePairs[i] = KeyValue.pair(
                stringSerializer.serialize(topicName, key + "-" + i),
                integerSerializer.serialize(topicName, value + i));
        }

        final List<ConsumerRecord<byte[], byte[]>> records =
            factory.create(Arrays.<KeyValue<String, Long>>asList(keyValuePairs), timestamp, 2L);

        for (int i = 0; i < keyValuePairs.length; ++i) {
            verifyRecord(
                topicName,
                (byte[]) rawKeyValuePairs[i].key,
                (byte[]) rawKeyValuePairs[i].value,
                timestamp + 2L * i,
                records.get(i));
        }

        // should not have incremented internally tracked time
        record = factory.create(value);
        verifyRecord(topicName, null, rawValue, 0L, record);
    }

    private void verifyRecord(final String topicName,
                              final byte[] rawKey,
                              final byte[] rawValue,
                              final long timestamp,
                              final ConsumerRecord<byte[], byte[]> record) {
        assertEquals(topicName, record.topic());
        assertArrayEquals(rawKey, record.key());
        assertArrayEquals(rawValue, record.value());
        assertEquals(timestamp, record.timestamp());
    }

}
