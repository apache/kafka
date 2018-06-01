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

public class ConsumerRecordFactoryTest {
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

    private final ConsumerRecordFactory<byte[], Integer> defaultFactory =
        new ConsumerRecordFactory<>(new ByteArraySerializer(), integerSerializer);

    @Test
    public void shouldAdvanceTime() {
        factory.advanceTimeMs(3L);
        verifyRecord(topicName, rawKey, rawValue, 3L, factory.create(topicName, rawKey, value));

        factory.advanceTimeMs(2L);
        verifyRecord(topicName, rawKey, rawValue, 5L, factory.create(topicName, rawKey, value));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullTopicName() {
        factory.create(null, rawKey, value, timestamp);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullHeaders() {
        factory.create(topicName, rawKey, value, null, timestamp);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullTopicNameWithDefaultTimestamp() {
        factory.create(null, rawKey, value);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullTopicNameWithNullKey() {
        factory.create((String) null, value, timestamp);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullTopicNameWithNullKeyAndDefaultTimestamp() {
        factory.create((String) null, value);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullTopicNameWithKeyValuePairs() {
        factory.create(null, Collections.singletonList(KeyValue.pair(rawKey, value)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullTopicNameWithKeyValuePairsAndCustomTimestamps() {
        factory.create(null, Collections.singletonList(KeyValue.pair(rawKey, value)), timestamp, 2L);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRequireCustomTopicNameIfNotDefaultFactoryTopicName() {
        defaultFactory.create(rawKey, value, timestamp);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRequireCustomTopicNameIfNotDefaultFactoryTopicNameWithDefaultTimestamp() {
        defaultFactory.create(rawKey, value);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRequireCustomTopicNameIfNotDefaultFactoryTopicNameWithNullKey() {
        defaultFactory.create(value, timestamp);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRequireCustomTopicNameIfNotDefaultFactoryTopicNameWithNullKeyAndDefaultTimestamp() {
        defaultFactory.create(value);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRequireCustomTopicNameIfNotDefaultFactoryTopicNameWithKeyValuePairs() {
        defaultFactory.create(Collections.singletonList(KeyValue.pair(rawKey, value)));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRequireCustomTopicNameIfNotDefaultFactoryTopicNameWithKeyValuePairsAndCustomTimestamps() {
        defaultFactory.create(Collections.singletonList(KeyValue.pair(rawKey, value)), timestamp, 2L);
    }

    @Test
    public void shouldCreateConsumerRecordWithOtherTopicNameAndTimestamp() {
        verifyRecord(otherTopicName, rawKey, rawValue, timestamp, factory.create(otherTopicName, rawKey, value, timestamp));
    }

    @Test
    public void shouldCreateConsumerRecordWithTimestamp() {
        verifyRecord(topicName, rawKey, rawValue, timestamp, factory.create(rawKey, value, timestamp));
    }

    @Test
    public void shouldCreateConsumerRecordWithOtherTopicName() {
        verifyRecord(otherTopicName, rawKey, rawValue, 0L, factory.create(otherTopicName, rawKey, value));

        factory.advanceTimeMs(3L);
        verifyRecord(otherTopicName, rawKey, rawValue, 3L, factory.create(otherTopicName, rawKey, value));
    }

    @Test
    public void shouldCreateConsumerRecord() {
        verifyRecord(topicName, rawKey, rawValue, 0L, factory.create(rawKey, value));

        factory.advanceTimeMs(3L);
        verifyRecord(topicName, rawKey, rawValue, 3L, factory.create(rawKey, value));
    }

    @Test
    public void shouldCreateNullKeyConsumerRecordWithOtherTopicNameAndTimestampWithTimetamp() {
        verifyRecord(topicName, null, rawValue, timestamp, factory.create(value, timestamp));
    }

    @Test
    public void shouldCreateNullKeyConsumerRecordWithTimestampWithTimestamp() {
        verifyRecord(topicName, null, rawValue, timestamp, factory.create(value, timestamp));
    }

    @Test
    public void shouldCreateNullKeyConsumerRecord() {
        verifyRecord(topicName, null, rawValue, 0L, factory.create(value));

        factory.advanceTimeMs(3L);
        verifyRecord(topicName, null, rawValue, 3L, factory.create(value));
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
            factory.create(Arrays.<KeyValue<String, Integer>>asList(keyValuePairs));

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
            factory.create(Arrays.<KeyValue<String, Integer>>asList(keyValuePairs));

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
            factory.create(Arrays.<KeyValue<String, Integer>>asList(keyValuePairs), timestamp, 2L);

        for (int i = 0; i < keyValuePairs.length; ++i) {
            verifyRecord(
                topicName,
                (byte[]) rawKeyValuePairs[i].key,
                (byte[]) rawKeyValuePairs[i].value,
                timestamp + 2L * i,
                records.get(i));
        }

        // should not have incremented internally tracked time
        verifyRecord(topicName, null, rawValue, 0L, factory.create(value));
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
