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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRecordTest {
    private final String key = "testKey";
    private final int value = 1;
    private final Headers headers = new RecordHeaders(
            new Header[]{
                new RecordHeader("foo", "value".getBytes()),
                new RecordHeader("bar", null),
                new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
            });
    private final Instant recordTime = Instant.parse("2019-06-01T10:00:00Z");
    private final long recordMs = recordTime.toEpochMilli();

    @Test
    public void testFields() {
        final TestRecord<String, Integer> testRecord = new TestRecord<>(key, value, headers, recordTime);
        assertThat(testRecord.key(), equalTo(key));
        assertThat(testRecord.value(), equalTo(value));
        assertThat(testRecord.headers(), equalTo(headers));
        assertThat(testRecord.timestamp(), equalTo(recordMs));

        assertThat(testRecord.getKey(), equalTo(key));
        assertThat(testRecord.getValue(), equalTo(value));
        assertThat(testRecord.getHeaders(), equalTo(headers));
        assertThat(testRecord.getRecordTime(), equalTo(recordTime));
    }

    @Test
    public void testMultiFieldMatcher() {
        final TestRecord<String, Integer> testRecord = new TestRecord<>(key, value, headers, recordTime);

        assertThat(testRecord, allOf(
                hasProperty("key", equalTo(key)),
                hasProperty("value", equalTo(value)),
                hasProperty("headers", equalTo(headers))));

        assertThat(testRecord, allOf(
                hasProperty("key", equalTo(key)),
                hasProperty("value", equalTo(value)),
                hasProperty("headers", equalTo(headers)),
                hasProperty("recordTime", equalTo(recordTime))));

        assertThat(testRecord, allOf(
                hasProperty("key", equalTo(key)),
                hasProperty("value", equalTo(value))));
    }


    @Test
    public void testEqualsAndHashCode() {
        final TestRecord<String, Integer> testRecord = new TestRecord<>(key, value, headers, recordTime);
        assertEquals(testRecord, testRecord);
        assertEquals(testRecord.hashCode(), testRecord.hashCode());

        final TestRecord<String, Integer> equalRecord = new TestRecord<>(key, value, headers, recordTime);
        assertEquals(testRecord, equalRecord);
        assertEquals(testRecord.hashCode(), equalRecord.hashCode());

        final TestRecord<String, Integer> equalRecordMs = new TestRecord<>(key, value, headers, recordMs);
        assertEquals(testRecord, equalRecordMs);
        assertEquals(testRecord.hashCode(), equalRecordMs.hashCode());

        final Headers headers2 = new RecordHeaders(
                new Header[]{
                    new RecordHeader("foo", "value".getBytes()),
                    new RecordHeader("bar", null),
                });
        final TestRecord<String, Integer> headerMismatch = new TestRecord<>(key, value, headers2, recordTime);
        assertNotEquals(testRecord, headerMismatch);

        final TestRecord<String, Integer> keyMisMatch = new TestRecord<>("test-mismatch", value, headers, recordTime);
        assertNotEquals(testRecord, keyMisMatch);

        final TestRecord<String, Integer> valueMisMatch = new TestRecord<>(key, 2, headers, recordTime);
        assertNotEquals(testRecord, valueMisMatch);

        final TestRecord<String, Integer> timeMisMatch = new TestRecord<>(key, value, headers, recordTime.plusMillis(1));
        assertNotEquals(testRecord, timeMisMatch);

        final TestRecord<String, Integer> nullFieldsRecord = new TestRecord<>(null, null, null, (Instant) null);
        assertEquals(nullFieldsRecord, nullFieldsRecord);
        assertEquals(nullFieldsRecord.hashCode(), nullFieldsRecord.hashCode());
    }

    @Test
    public void testPartialConstructorEquals() {
        final TestRecord<String, Integer> record1 = new TestRecord<>(value);
        assertThat(record1, equalTo(new TestRecord<>(null, value, null, (Instant) null)));

        final TestRecord<String, Integer> record2 = new TestRecord<>(key, value);
        assertThat(record2, equalTo(new TestRecord<>(key, value, null, (Instant) null)));

        final TestRecord<String, Integer> record3 = new TestRecord<>(key, value, headers);
        assertThat(record3, equalTo(new TestRecord<>(key, value, headers, (Long) null)));

        final TestRecord<String, Integer> record4 = new TestRecord<>(key, value, recordTime);
        assertThat(record4, equalTo(new TestRecord<>(key, value, null, recordMs)));
    }

    @Test
    public void testInvalidRecords() {
        assertThrows(IllegalArgumentException.class,
            () -> new TestRecord<>(key, value, headers,  -1L));
    }

    @Test
    public void testToString() {
        final TestRecord<String, Integer> testRecord = new TestRecord<>(key, value, headers, recordTime);
        assertThat(testRecord.toString(), equalTo("TestRecord[key=testKey, value=1, "
                + "headers=RecordHeaders(headers = [RecordHeader(key = foo, value = [118, 97, 108, 117, 101]), "
                + "RecordHeader(key = bar, value = null), RecordHeader(key = \"A\\u00ea\\u00f1\\u00fcC\", value = [118, 97, 108, 117, 101])], isReadOnly = false), "
                + "recordTime=2019-06-01T10:00:00Z, partition=-1]"));
    }

    @Test
    public void testConsumerRecord() {
        final String topicName = "topic";
        final ConsumerRecord<String, Integer> consumerRecord = new ConsumerRecord<>(topicName, 1, 0, recordMs,
            TimestampType.CREATE_TIME, 0, 0, key, value, headers, Optional.empty());
        final TestRecord<String, Integer> testRecord = new TestRecord<>(consumerRecord);
        final TestRecord<String, Integer> expectedRecord = new TestRecord<>(key, value, headers, recordTime, 1);
        assertEquals(expectedRecord, testRecord);
    }

    @Test
    public void testConsumerRecordWithNegativePartition() {
        final String topicName = "topic";
        final ConsumerRecord<String, Integer> consumerRecord = new ConsumerRecord<>(topicName, -1, 0, recordMs,
            TimestampType.CREATE_TIME, 0, 0, key, value, headers, Optional.empty());
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new TestRecord<>(consumerRecord));
        assertEquals("Invalid partition: -1. Partition number should always be non-negative.",
            exception.getMessage());
    }

    @Test
    public void testConsumerRecordWithNoTimestamp() {
        final String topicName = "topic";
        final ConsumerRecord<String, String> record = new ConsumerRecord<>(
                topicName, 0, 0L, ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                0, 0, "key", "value", new RecordHeaders(), Optional.empty()
        );
        final TestRecord<String, String> testRecord = new TestRecord<>(record);
        assertNull(testRecord.timestamp());
    }

    @Test
    public void testProducerRecord() {
        final String topicName = "topic";
        final ProducerRecord<String, Integer> producerRecord =
            new ProducerRecord<>(topicName, 1, recordMs, key, value, headers);
        final TestRecord<String, Integer> testRecord = new TestRecord<>(producerRecord);
        final TestRecord<String, Integer> expectedRecord = new TestRecord<>(key, value, headers, recordTime, 1);
        assertEquals(expectedRecord, testRecord);
    }

    @Test
    public void testProducerRecordWithNullTimestamp() {
        final String topicName = "topic";
        final ProducerRecord<String, String> record = new ProducerRecord<>(
            topicName, null, null, "key", "value", new RecordHeaders()
        );
        final TestRecord<String, String> testRecord = new TestRecord<>(record);
        assertNull(testRecord.timestamp());
    }

    @Test
    public void testProducerRecordWithoutPartition() {
        final String topicName = "topic";
        final ProducerRecord<String, Integer> producerRecord =
            new ProducerRecord<>(topicName, null, recordMs, key, value, headers);
        final TestRecord<String, Integer> testRecord = new TestRecord<>(producerRecord);
        assertEquals(-1, testRecord.partition());
    }

    @Test
    public void testPartitionDefaultsToUnset() {
        // Records built without an explicit partition, default to -1
        assertEquals(-1, new TestRecord<>(key, value, headers, recordTime).partition());
        assertEquals(-1, new TestRecord<>(key, value, headers, recordMs).partition());
        assertEquals(-1, new TestRecord<>(key, value, headers).partition());
        assertEquals(-1, new TestRecord<>(key, value).partition());
        assertEquals(-1, new TestRecord<>(value).partition());
    }

    @Test
    public void testExplicitPartitionConstructor() {
        // Records built with an explicit partition.
        final TestRecord<String, Integer> testRecord = new TestRecord<>(key, value, headers, recordTime, 3);
        assertEquals(3, testRecord.partition());
    }

    @Test
    public void testExplicitNoPartitionConstructor() {
        // Records built with the NO_PARTITION sentinel.
        final TestRecord<String, Integer> testRecord =
            new TestRecord<>(key, value, headers, recordTime, -1);
        assertEquals(-1, testRecord.partition());
    }

    @Test
    public void testInvalidNegativePartitionConstructor() {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new TestRecord<>(key, value, headers, recordTime, -2)
        );
        assertEquals(
            "Invalid partition: -2. Partition number should always be non-negative or -1.",
            exception.getMessage()
        );
    }

    @Test
    public void testEqualsConsidersPartition() {
        // equals()/hashCode() take the partition into account.
        final TestRecord<String, Integer> record1 = new TestRecord<>(key, value, headers, recordTime, 0);
        final TestRecord<String, Integer> record2 = new TestRecord<>(key, value, headers, recordTime, 1);
        assertNotEquals(record1, record2);

        final TestRecord<String, Integer> record1Again = new TestRecord<>(key, value, headers, recordTime, 0);
        assertEquals(record1, record1Again);
        assertEquals(record1.hashCode(), record1Again.hashCode());

        // an unset (default) partition differs from an explicit one
        assertNotEquals(new TestRecord<>(key, value, headers, recordTime), record1);
    }

    @Test
    public void testEqualsIgnorePartition() {
        // equalsIgnorePartition() matches on every field except the partition.
        final TestRecord<String, Integer> record1 = new TestRecord<>(key, value, headers, recordTime, 0);
        final TestRecord<String, Integer> record2 = new TestRecord<>(key, value, headers, recordTime, 1);
        assertNotEquals(record1, record2);
        assertTrue(record1.equalsIgnorePartition(record2));
        assertTrue(record2.equalsIgnorePartition(record1));

        // a genuine field mismatch is still detected
        assertFalse(record1.equalsIgnorePartition(new TestRecord<>("other", value, headers, recordTime, 0)));
        assertFalse(record1.equalsIgnorePartition(new TestRecord<>(key, 2, headers, recordTime, 0)));

        // reflexive / null guards
        assertTrue(record1.equalsIgnorePartition(record1));
        assertFalse(record1.equalsIgnorePartition(null));
    }

    @Test
    public void testToStringIncludesPartitionWhenSet() {
        final TestRecord<String, Integer> testRecord = new TestRecord<>(key, value, headers, recordTime, 2);
        assertThat(testRecord.toString(), equalTo("TestRecord[key=testKey, value=1, "
            + "headers=RecordHeaders(headers = [RecordHeader(key = foo, value = [118, 97, 108, 117, 101]), "
            + "RecordHeader(key = bar, value = null), RecordHeader(key = \"A\\u00ea\\u00f1\\u00fcC\", value = [118, 97, 108, 117, 101])], isReadOnly = false), "
            + "recordTime=2019-06-01T10:00:00Z, partition=2]"));
    }
}
