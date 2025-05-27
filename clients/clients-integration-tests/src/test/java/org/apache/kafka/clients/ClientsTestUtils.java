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
package org.apache.kafka.clients;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientsTestUtils {

    private static final String KEY_PREFIX = "key ";
    private static final String VALUE_PREFIX = "value ";

    private ClientsTestUtils() {}

    public static List<ConsumerRecord<byte[], byte[]>> consumeRecords(
        Consumer<byte[], byte[]> consumer,
        int numRecords
    ) throws InterruptedException {
        return consumeRecords(consumer, numRecords, Integer.MAX_VALUE);
    }

    public static List<ConsumerRecord<byte[], byte[]>> consumeRecords(
        Consumer<byte[], byte[]> consumer,
        int numRecords,
        int maxPollRecords
    ) throws InterruptedException {
        List<ConsumerRecord<byte[], byte[]>> consumedRecords = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            var records = consumer.poll(Duration.ofMillis(100));
            records.forEach(consumedRecords::add);
            assertTrue(records.count() <= maxPollRecords);
            return consumedRecords.size() >= numRecords;
        }, 60000, "Timed out before consuming expected " + numRecords + " records.");

        return consumedRecords;
    }

    public static void consumeAndVerifyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp,
        int numRecords,
        int startingOffset,
        int startingKeyAndValueIndex,
        long startingTimestamp,
        long timestampIncrement
    ) throws InterruptedException {
        consumeAndVerifyRecords(
            consumer,
            tp,
            numRecords,
            Integer.MAX_VALUE,
            startingOffset,
            startingKeyAndValueIndex,
            startingTimestamp,
            timestampIncrement
        );
    }

    public static void consumeAndVerifyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp,
        int numRecords,
        int maxPollRecords,
        int startingOffset,
        int startingKeyAndValueIndex,
        long startingTimestamp,
        long timestampIncrement
    ) throws InterruptedException {
        var records = consumeRecords(consumer, numRecords, maxPollRecords);
        for (var i = 0; i < numRecords; i++) {
            var record = records.get(i);
            var offset = startingOffset + i;

            assertEquals(tp.topic(), record.topic());
            assertEquals(tp.partition(), record.partition());

            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            var timestamp = startingTimestamp + i * (timestampIncrement > 0 ? timestampIncrement : 1);
            assertEquals(timestamp, record.timestamp());

            assertEquals(offset, record.offset());
            var keyAndValueIndex = startingKeyAndValueIndex + i;
            assertEquals(KEY_PREFIX + keyAndValueIndex, new String(record.key()));
            assertEquals(VALUE_PREFIX + keyAndValueIndex, new String(record.value()));
            // this is true only because K and V are byte arrays
            assertEquals((KEY_PREFIX + keyAndValueIndex).length(), record.serializedKeySize());
            assertEquals((VALUE_PREFIX + keyAndValueIndex).length(), record.serializedValueSize());
        }
    }

    public static void consumeAndVerifyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp,
        int numRecords,
        int startingOffset,
        int startingKeyAndValueIndex,
        long startingTimestamp
    ) throws InterruptedException {
        consumeAndVerifyRecords(consumer, tp, numRecords, startingOffset, startingKeyAndValueIndex, startingTimestamp, -1);
    }

    public static void consumeAndVerifyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp,
        int numRecords,
        int startingOffset
    ) throws InterruptedException {
        consumeAndVerifyRecords(consumer, tp, numRecords, startingOffset, 0, 0, -1);
    }

    public static void sendRecords(
        ClusterInstance cluster,
        TopicPartition tp,
        int numRecords,
        long startingTimestamp,
        long timestampIncrement
    ) {
        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (var i = 0; i < numRecords; i++) {
                sendRecord(producer, tp, startingTimestamp, i, timestampIncrement);
            }
            producer.flush();
        }
    }

    public static void sendRecords(
        ClusterInstance cluster,
        TopicPartition tp,
        int numRecords,
        long startingTimestamp
    ) {
        sendRecords(cluster, tp, numRecords, startingTimestamp, -1);
    }

    public static void sendRecords(
        ClusterInstance cluster,
        TopicPartition tp,
        int numRecords
    ) {
        sendRecords(cluster, tp, numRecords, System.currentTimeMillis());
    }

    public static List<ProducerRecord<byte[], byte[]>> sendRecords(
        Producer<byte[], byte[]> producer,
        TopicPartition tp,
        int numRecords,
        long startingTimestamp,
        long timestampIncrement
    ) {
        List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (var i = 0; i < numRecords; i++) {
            var record = sendRecord(producer, tp, startingTimestamp, i, timestampIncrement);
            records.add(record);
        }
        producer.flush();
        return records;
    }

    public static void sendRecords(
        Producer<byte[], byte[]> producer,
        TopicPartition tp,
        int numRecords,
        long startingTimestamp
    ) {
        for (var i = 0; i < numRecords; i++) {
            sendRecord(producer, tp, startingTimestamp, i, -1);
        }
        producer.flush();
    }

    public static void awaitAssignment(
        Consumer<byte[], byte[]> consumer,
        Set<TopicPartition> expectedAssignment
    ) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100));
            return consumer.assignment().equals(expectedAssignment);
        }, "Timed out while awaiting expected assignment " + expectedAssignment + ". " +
                "The current assignment is " + consumer.assignment()
        );
    }

    private static ProducerRecord<byte[], byte[]> sendRecord(
        Producer<byte[], byte[]> producer,
        TopicPartition tp,
        long startingTimestamp,
        int numRecord,
        long timestampIncrement
    ) {
        var timestamp = startingTimestamp + numRecord * (timestampIncrement > 0 ? timestampIncrement : 1);
        var record = new ProducerRecord<>(
            tp.topic(),
            tp.partition(),
            timestamp,
            (KEY_PREFIX + numRecord).getBytes(),
            (VALUE_PREFIX + numRecord).getBytes()
        );
        producer.send(record);
        return record;
    }
}
