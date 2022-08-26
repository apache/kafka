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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerRecordTest {

    @Test
    public void testShortConstructor() {
        String topic = "topic";
        int partition = 0;
        long offset = 23;
        String key = "key";
        String value = "value";

        ConsumerRecord<String, String> record = new ConsumerRecord<>(topic, partition, offset, key, value);
        assertEquals(topic, record.topic());
        assertEquals(partition, record.partition());
        assertEquals(offset, record.offset());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(TimestampType.NO_TIMESTAMP_TYPE, record.timestampType());
        assertEquals(ConsumerRecord.NO_TIMESTAMP, record.timestamp());
        assertEquals(ConsumerRecord.NULL_SIZE, record.serializedKeySize());
        assertEquals(ConsumerRecord.NULL_SIZE, record.serializedValueSize());
        assertEquals(Optional.empty(), record.leaderEpoch());
        assertEquals(new RecordHeaders(), record.headers());
    }

    @Test
    @Deprecated
    public void testConstructorsWithChecksum() {
        String topic = "topic";
        int partition = 0;
        long offset = 23;
        long timestamp = 23434217432432L;
        TimestampType timestampType = TimestampType.CREATE_TIME;
        String key = "key";
        String value = "value";
        long checksum = 50L;
        int serializedKeySize = 100;
        int serializedValueSize = 1142;

        ConsumerRecord<String, String> record = new ConsumerRecord<>(topic, partition, offset, timestamp, timestampType,
            checksum, serializedKeySize, serializedValueSize, key, value);
        assertEquals(topic, record.topic());
        assertEquals(partition, record.partition());
        assertEquals(offset, record.offset());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(timestampType, record.timestampType());
        assertEquals(timestamp, record.timestamp());
        assertEquals(serializedKeySize, record.serializedKeySize());
        assertEquals(serializedValueSize, record.serializedValueSize());
        assertEquals(Optional.empty(), record.leaderEpoch());
        assertEquals(new RecordHeaders(), record.headers());

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("header key", "header value".getBytes(StandardCharsets.UTF_8)));
        record = new ConsumerRecord<>(topic, partition, offset, timestamp, timestampType,
                checksum, serializedKeySize, serializedValueSize, key, value, headers);
        assertEquals(topic, record.topic());
        assertEquals(partition, record.partition());
        assertEquals(offset, record.offset());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(timestampType, record.timestampType());
        assertEquals(timestamp, record.timestamp());
        assertEquals(serializedKeySize, record.serializedKeySize());
        assertEquals(serializedValueSize, record.serializedValueSize());
        assertEquals(Optional.empty(), record.leaderEpoch());
        assertEquals(headers, record.headers());

        Optional<Integer> leaderEpoch = Optional.of(10);
        record = new ConsumerRecord<>(topic, partition, offset, timestamp, timestampType,
                checksum, serializedKeySize, serializedValueSize, key, value, headers, leaderEpoch);
        assertEquals(topic, record.topic());
        assertEquals(partition, record.partition());
        assertEquals(offset, record.offset());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(timestampType, record.timestampType());
        assertEquals(timestamp, record.timestamp());
        assertEquals(serializedKeySize, record.serializedKeySize());
        assertEquals(serializedValueSize, record.serializedValueSize());
        assertEquals(leaderEpoch, record.leaderEpoch());
        assertEquals(headers, record.headers());
    }

}
