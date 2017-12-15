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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestRecordFactoryTest {
    private final StringSerializer stringSerializer = new StringSerializer();
    private final LongSerializer longSerializer = new LongSerializer();

    private final String topicName = "topic";
    private final String key = "key";
    private final Long value = 42L;
    private final long timestamp = 21L;
    private final KeyValue<String, Long> keyValuePair = KeyValue.pair(key, value);
    private final byte[] rawKey = stringSerializer.serialize(topicName, key);
    private final byte[] rawValue = longSerializer.serialize(topicName, value);

    private final MockTime time = new MockTime(0L, timestamp, 0L);
    private final TestRecordFactory<String, Long> factory = new TestRecordFactory<>(topicName, stringSerializer, longSerializer, time);

    private TestRecord<String, Long> record;

    @Test
    public void shouldCreateTestRecord() {
        record = factory.create(key, value, timestamp);
        verifyRecord(topicName, rawKey, rawValue, timestamp, record);
    }

    @Test
    public void shouldCreateRecordsWithTimeTimestamp() {
        record = factory.create(key, value);
        verifyRecord(topicName, rawKey, rawValue, time.milliseconds(), record);

        time.sleep(3L);
        record = factory.create(key, value);
        verifyRecord(topicName, rawKey, rawValue, time.milliseconds(), record);
    }

    @Test
    public void shouldCreateRecordWithNullKey() {
        record = factory.create(value, timestamp);
        verifyRecord(topicName, null, rawValue, timestamp, record);

    }

    @Test
    public void shouldCreateRecordWithNullKeyAndTimeTimestamp() {
        record = factory.create(value);
        verifyRecord(topicName, null, rawValue, time.milliseconds(), record);

        time.sleep(3L);
        record = factory.create(value);
        verifyRecord(topicName, null, rawValue, time.milliseconds(), record);
    }

    @Test
    public void shouldCreateRecordFromKeyValuePair() {
        record = factory.create(keyValuePair, timestamp);
        verifyRecord(topicName, rawKey, rawValue, timestamp, record);
    }

    @Test
    public void shouldCreateRecordFromKeyValuePairWithTimeTimestamp() {
        record = factory.create(keyValuePair);
        verifyRecord(topicName, rawKey, rawValue, time.milliseconds(), record);

        time.sleep(3L);
        record = factory.create(keyValuePair);
        verifyRecord(topicName, rawKey, rawValue, time.milliseconds(), record);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCreateRecordsFromKeyValuePairsWithTimestampIncrements() {
        final KeyValue[] keyValuePairs = new KeyValue[5];
        final KeyValue[] rawKeyValuePairs = new KeyValue[keyValuePairs.length];

        for (int i = 0; i < keyValuePairs.length; ++i) {
            keyValuePairs[i] = KeyValue.pair(key + "-" + i, value + i);
            rawKeyValuePairs[i] = KeyValue.pair(
                stringSerializer.serialize(topicName, key + "-" + i),
                longSerializer.serialize(topicName, value + i));
        }

        final List<TestRecord<String, Long>> records = factory.create(Arrays.<KeyValue<String, Long>>asList(keyValuePairs), timestamp);

        for (int i = 0; i < keyValuePairs.length; ++i) {
            verifyRecord(
                topicName,
                (byte[]) rawKeyValuePairs[i].key,
                (byte[]) rawKeyValuePairs[i].value,
                timestamp + i,
                records.get(i));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCreateRecordsFromKeyValuePairsWithCustomTimestampIncrements() {
        final KeyValue[] keyValuePairs = new KeyValue[5];
        final KeyValue[] rawKeyValuePairs = new KeyValue[keyValuePairs.length];

        for (int i = 0; i < keyValuePairs.length; ++i) {
            keyValuePairs[i] = KeyValue.pair(key + "-" + i, value + i);
            rawKeyValuePairs[i] = KeyValue.pair(
                stringSerializer.serialize(topicName, key + "-" + i),
                longSerializer.serialize(topicName, value + i));
        }

        final List<TestRecord<String, Long>> records = factory.create(Arrays.<KeyValue<String, Long>>asList(keyValuePairs), timestamp, 2L);

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
    public void shouldCreateRecordsFromKeyValuePairsWithTimeIncrements() {
        final KeyValue[] keyValuePairs = new KeyValue[5];
        final KeyValue[] rawKeyValuePairs = new KeyValue[keyValuePairs.length];

        for (int i = 0; i < keyValuePairs.length; ++i) {
            keyValuePairs[i] = KeyValue.pair(key + "-" + i, value + i);
            rawKeyValuePairs[i] = KeyValue.pair(
                stringSerializer.serialize(topicName, key + "-" + i),
                longSerializer.serialize(topicName, value + i));
        }

        time.setAutoTickMs(3L);
        final List<TestRecord<String, Long>> records = factory.create(Arrays.<KeyValue<String, Long>>asList(keyValuePairs));

        for (int i = 0; i < keyValuePairs.length; ++i) {
            verifyRecord(
                topicName,
                (byte[]) rawKeyValuePairs[i].key,
                (byte[]) rawKeyValuePairs[i].value,
                timestamp + 3L * i,
                records.get(i));
        }
    }

    private void verifyRecord(final String topicName,
                              final byte[] rawKey,
                              final byte[] rawValue,
                              final long timestamp,
                              final TestRecord record) {
        assertEquals(topicName, record.topicName());
        assertArrayEquals(rawKey, record.key());
        assertArrayEquals(rawValue, record.value());
        assertEquals(timestamp, record.timestamp());
    }

}
