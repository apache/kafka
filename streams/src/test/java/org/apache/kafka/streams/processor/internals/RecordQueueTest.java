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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RecordQueueTest {
    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();
    private final TimestampExtractor timestampExtractor = new MockTimestampExtractor();

    final InternalMockProcessorContext context = new InternalMockProcessorContext(
        StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class),
        new MockRecordCollector()
    );
    private final MockSourceNode<Integer, Integer, ?, ?> mockSourceNodeWithMetrics
        = new MockSourceNode<>(intDeserializer, intDeserializer);
    private final RecordQueue queue = new RecordQueue(
        new TopicPartition("topic", 1),
        mockSourceNodeWithMetrics,
        timestampExtractor,
        new LogAndFailExceptionHandler(),
        context,
        new LogContext());
    private final RecordQueue queueThatSkipsDeserializeErrors = new RecordQueue(
        new TopicPartition("topic", 1),
        mockSourceNodeWithMetrics,
        timestampExtractor,
        new LogAndContinueExceptionHandler(),
        context,
        new LogContext());

    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);

    @Before
    public void before() {
        mockSourceNodeWithMetrics.init(context);
    }

    @After
    public void after() {
        mockSourceNodeWithMetrics.close();
    }

    @Test
    public void testTimeTracking() {
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertEquals(RecordQueue.UNKNOWN, queue.headRecordTimestamp());
        assertNull(queue.headRecordOffset());

        // add three 3 out-of-order records with timestamp 2, 1, 3
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 3, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        queue.addRawRecords(list1);

        assertEquals(3, queue.size());
        assertEquals(2L, queue.headRecordTimestamp());
        assertEquals(2L, queue.headRecordOffset().longValue());

        // poll the first record, now with 1, 3
        assertEquals(2L, queue.poll().timestamp);
        assertEquals(2, queue.size());
        assertEquals(1L, queue.headRecordTimestamp());
        assertEquals(1L, queue.headRecordOffset().longValue());

        // poll the second record, now with 3
        assertEquals(1L, queue.poll().timestamp);
        assertEquals(1, queue.size());
        assertEquals(3L, queue.headRecordTimestamp());
        assertEquals(3L, queue.headRecordOffset().longValue());

        // add three 3 out-of-order records with timestamp 4, 1, 2
        // now with 3, 4, 1, 2
        final List<ConsumerRecord<byte[], byte[]>> list2 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        queue.addRawRecords(list2);

        assertEquals(4, queue.size());
        assertEquals(3L, queue.headRecordTimestamp());
        assertEquals(3L, queue.headRecordOffset().longValue());

        // poll the third record, now with 4, 1, 2
        assertEquals(3L, queue.poll().timestamp);
        assertEquals(3, queue.size());
        assertEquals(4L, queue.headRecordTimestamp());
        assertEquals(4L, queue.headRecordOffset().longValue());

        // poll the rest records
        assertEquals(4L, queue.poll().timestamp);
        assertEquals(1L, queue.headRecordTimestamp());
        assertEquals(1L, queue.headRecordOffset().longValue());

        assertEquals(1L, queue.poll().timestamp);
        assertEquals(2L, queue.headRecordTimestamp());
        assertEquals(2L, queue.headRecordOffset().longValue());

        assertEquals(2L, queue.poll().timestamp);
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertEquals(RecordQueue.UNKNOWN, queue.headRecordTimestamp());
        assertNull(queue.headRecordOffset());

        // add three more records with 4, 5, 6
        final List<ConsumerRecord<byte[], byte[]>> list3 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 5, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 6, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        queue.addRawRecords(list3);

        assertEquals(3, queue.size());
        assertEquals(4L, queue.headRecordTimestamp());
        assertEquals(4L, queue.headRecordOffset().longValue());

        // poll one record again, the timestamp should advance now
        assertEquals(4L, queue.poll().timestamp);
        assertEquals(2, queue.size());
        assertEquals(5L, queue.headRecordTimestamp());
        assertEquals(5L, queue.headRecordOffset().longValue());

        // clear the queue
        queue.clear();
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertEquals(RecordQueue.UNKNOWN, queue.headRecordTimestamp());
        assertEquals(RecordQueue.UNKNOWN, queue.partitionTime());
        assertNull(queue.headRecordOffset());

        // re-insert the three records with 4, 5, 6
        queue.addRawRecords(list3);

        assertEquals(3, queue.size());
        assertEquals(4L, queue.headRecordTimestamp());
        assertEquals(4L, queue.headRecordOffset().longValue());
    }

    @Test
    public void shouldTrackPartitionTimeAsMaxProcessedTimestamp() {
        assertTrue(queue.isEmpty());
        assertThat(queue.size(), is(0));
        assertThat(queue.headRecordTimestamp(), is(RecordQueue.UNKNOWN));
        assertThat(queue.partitionTime(), is(RecordQueue.UNKNOWN));

        // add three 3 out-of-order records with timestamp 2, 1, 3, 4
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 3, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        queue.addRawRecords(list1);
        assertThat(queue.partitionTime(), is(RecordQueue.UNKNOWN));

        queue.poll();
        assertThat(queue.partitionTime(), is(2L));

        queue.poll();
        assertThat(queue.partitionTime(), is(2L));

        queue.poll();
        assertThat(queue.partitionTime(), is(3L));
    }

    @Test
    public void shouldSetTimestampAndRespectMaxTimestampPolicy() {
        assertTrue(queue.isEmpty());
        assertThat(queue.size(), is(0));
        assertThat(queue.headRecordTimestamp(), is(RecordQueue.UNKNOWN));
        assertThat(queue.partitionTime(), is(RecordQueue.UNKNOWN));

        queue.setPartitionTime(150L);
        assertThat(queue.partitionTime(), is(150L));

        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 200, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 100, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 300, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 400, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        queue.addRawRecords(list1);
        assertThat(queue.partitionTime(), is(150L));

        queue.poll();
        assertThat(queue.partitionTime(), is(200L));

        queue.setPartitionTime(500L);
        assertThat(queue.partitionTime(), is(500L));

        queue.poll();
        assertThat(queue.partitionTime(), is(500L));
    }

    @Test
    public void shouldThrowStreamsExceptionWhenKeyDeserializationFails() {
        final byte[] key = Serdes.Long().serializer().serialize("foo", 1L);
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, key, recordValue));

        final StreamsException exception = assertThrows(
            StreamsException.class,
            () -> queue.addRawRecords(records)
        );
        assertThat(exception.getCause(), instanceOf(SerializationException.class));
    }

    @Test
    public void shouldThrowStreamsExceptionWhenValueDeserializationFails() {
        final byte[] value = Serdes.Long().serializer().serialize("foo", 1L);
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, value));

        final StreamsException exception = assertThrows(
            StreamsException.class,
            () -> queue.addRawRecords(records)
        );
        assertThat(exception.getCause(), instanceOf(SerializationException.class));
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenKeyDeserializationFailsWithSkipHandler() {
        final byte[] key = Serdes.Long().serializer().serialize("foo", 1L);
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, key, recordValue));

        queueThatSkipsDeserializeErrors.addRawRecords(records);
        assertEquals(0, queueThatSkipsDeserializeErrors.size());
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenValueDeserializationFailsWithSkipHandler() {
        final byte[] value = Serdes.Long().serializer().serialize("foo", 1L);
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, value));

        queueThatSkipsDeserializeErrors.addRawRecords(records);
        assertEquals(0, queueThatSkipsDeserializeErrors.size());
    }

    @Test
    public void shouldThrowOnNegativeTimestamp() {
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, -1L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        final RecordQueue queue = new RecordQueue(
            new TopicPartition("topic", 1),
            mockSourceNodeWithMetrics,
            new FailOnInvalidTimestamp(),
            new LogAndContinueExceptionHandler(),
            new InternalMockProcessorContext(),
            new LogContext());

        final StreamsException exception = assertThrows(
            StreamsException.class,
            () -> queue.addRawRecords(records)
        );
        assertThat(exception.getMessage(), equalTo("Input record ConsumerRecord(topic = topic, partition = 1, " +
            "leaderEpoch = null, offset = 1, CreateTime = -1, serialized key size = 0, serialized value size = 0, " +
            "headers = RecordHeaders(headers = [], isReadOnly = false), key = 1, value = 10) has invalid (negative) " +
            "timestamp. Possibly because a pre-0.10 producer client was used to write this record to Kafka without " +
            "embedding a timestamp, or because the input topic was created before upgrading the Kafka cluster to 0.10+. " +
            "Use a different TimestampExtractor to process this data."));
    }

    @Test
    public void shouldDropOnNegativeTimestamp() {
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, -1L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        final RecordQueue queue = new RecordQueue(
            new TopicPartition("topic", 1),
            mockSourceNodeWithMetrics,
            new LogAndSkipOnInvalidTimestamp(),
            new LogAndContinueExceptionHandler(),
            new InternalMockProcessorContext(),
            new LogContext());
        queue.addRawRecords(records);

        assertEquals(0, queue.size());
    }

    @Test
    public void shouldPassPartitionTimeToTimestampExtractor() {

        final PartitionTimeTrackingTimestampExtractor timestampExtractor = new PartitionTimeTrackingTimestampExtractor();
        final RecordQueue queue = new RecordQueue(
            new TopicPartition("topic", 1),
            mockSourceNodeWithMetrics,
            timestampExtractor,
            new LogAndFailExceptionHandler(),
            context,
            new LogContext());

        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertEquals(RecordQueue.UNKNOWN, queue.headRecordTimestamp());

        // add three 3 out-of-order records with timestamp 2, 1, 3, 4
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 3, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        assertEquals(RecordQueue.UNKNOWN, timestampExtractor.partitionTime);

        queue.addRawRecords(list1);

        // no (known) timestamp has yet been passed to the timestamp extractor
        assertEquals(RecordQueue.UNKNOWN, timestampExtractor.partitionTime);

        queue.poll();
        assertEquals(2L, timestampExtractor.partitionTime);

        queue.poll();
        assertEquals(2L, timestampExtractor.partitionTime);

        queue.poll();
        assertEquals(3L, timestampExtractor.partitionTime);

    }

    private static class PartitionTimeTrackingTimestampExtractor implements TimestampExtractor {
        private long partitionTime = RecordQueue.UNKNOWN;

        public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
            if (partitionTime < this.partitionTime) {
                throw new IllegalStateException("Partition time should not decrease");
            }
            this.partitionTime = partitionTime;
            return record.offset();
        }
    }
}
