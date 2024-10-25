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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.streams.processor.internals.ClientUtils.consumerRecordSizeInBytes;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOPIC_LEVEL_GROUP;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordQueueTest {
    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();
    private final TimestampExtractor timestampExtractor = new MockTimestampExtractor();

    private final Metrics metrics = new Metrics();
    private final StreamsMetricsImpl streamsMetrics =
        new StreamsMetricsImpl(metrics, "mock", new MockTime());

    @SuppressWarnings("rawtypes")
    final InternalMockProcessorContext context = new InternalMockProcessorContext<>(
        StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class),
        new MockRecordCollector(),
        metrics
    );
    private final MockSourceNode<Integer, Integer> mockSourceNodeWithMetrics
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

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void before() {
        mockSourceNodeWithMetrics.init(context);
    }

    @AfterEach
    public void after() {
        mockSourceNodeWithMetrics.close();
    }

    @Test
    public void testConsumedSensor() {
        final List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 3, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.empty()));

        queue.addRawRecords(records);

        final String threadId = Thread.currentThread().getName();
        final String taskId = context.taskId().toString();
        final String processorNodeId = mockSourceNodeWithMetrics.name();
        final String topic = "topic";
        final Metric recordsConsumed = context.metrics().metrics().get(
            new MetricName("records-consumed-total",
                           TOPIC_LEVEL_GROUP,
                           "The total number of records consumed from this topic",
                           streamsMetrics.topicLevelTagMap(threadId, taskId, processorNodeId, topic))
        );
        final Metric bytesConsumed = context.metrics().metrics().get(
            new MetricName("bytes-consumed-total",
                           TOPIC_LEVEL_GROUP,
                           "The total number of bytes consumed from this topic",
                           streamsMetrics.topicLevelTagMap(threadId, taskId, processorNodeId, topic))
        );

        double totalBytes = 0D;
        double totalRecords = 0D;

        queue.poll(5L);
        ++totalRecords;
        totalBytes += consumerRecordSizeInBytes(records.get(0));

        assertThat(bytesConsumed.metricValue(), equalTo(totalBytes));
        assertThat(recordsConsumed.metricValue(), equalTo(totalRecords));

        queue.poll(6L);
        ++totalRecords;
        totalBytes += consumerRecordSizeInBytes(records.get(1));

        assertThat(bytesConsumed.metricValue(), equalTo(totalBytes));
        assertThat(recordsConsumed.metricValue(), equalTo(totalRecords));

        queue.poll(7L);
        ++totalRecords;
        totalBytes += consumerRecordSizeInBytes(records.get(2));

        assertThat(bytesConsumed.metricValue(), equalTo(totalBytes));
        assertThat(recordsConsumed.metricValue(), equalTo(totalRecords));
    }

    @Test
    public void testTimeTracking() {
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertEquals(RecordQueue.UNKNOWN, queue.headRecordTimestamp());
        assertNull(queue.headRecordOffset());

        // add three 3 out-of-order records with timestamp 2, 1, 3
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.of(1)),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.of(1)),
            new ConsumerRecord<>("topic", 1, 3, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.of(2)));


        queue.addRawRecords(list1);

        assertEquals(3, queue.size());
        assertEquals(2L, queue.headRecordTimestamp());
        assertEquals(2L, queue.headRecordOffset().longValue());
        assertEquals(Optional.of(1), queue.headRecordLeaderEpoch());

        // poll the first record, now with 1, 3
        assertEquals(2L, queue.poll(0).timestamp);
        assertEquals(2, queue.size());
        assertEquals(1L, queue.headRecordTimestamp());
        assertEquals(1L, queue.headRecordOffset().longValue());
        assertEquals(Optional.of(1), queue.headRecordLeaderEpoch());

        // poll the second record, now with 3
        assertEquals(1L, queue.poll(0).timestamp);
        assertEquals(1, queue.size());
        assertEquals(3L, queue.headRecordTimestamp());
        assertEquals(3L, queue.headRecordOffset().longValue());
        assertEquals(Optional.of(2), queue.headRecordLeaderEpoch());

        // add three 3 out-of-order records with timestamp 4, 1, 2
        // now with 3, 4, 1, 2
        final List<ConsumerRecord<byte[], byte[]>> list2 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.of(2)),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.of(1)),
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.of(1)));

        queue.addRawRecords(list2);

        assertEquals(4, queue.size());
        assertEquals(3L, queue.headRecordTimestamp());
        assertEquals(3L, queue.headRecordOffset().longValue());
        assertEquals(Optional.of(2), queue.headRecordLeaderEpoch());

        // poll the third record, now with 4, 1, 2
        assertEquals(3L, queue.poll(0).timestamp);
        assertEquals(3, queue.size());
        assertEquals(4L, queue.headRecordTimestamp());
        assertEquals(4L, queue.headRecordOffset().longValue());
        assertEquals(Optional.of(2), queue.headRecordLeaderEpoch());

        // poll the rest records
        assertEquals(4L, queue.poll(0).timestamp);
        assertEquals(1L, queue.headRecordTimestamp());
        assertEquals(1L, queue.headRecordOffset().longValue());
        assertEquals(Optional.of(1), queue.headRecordLeaderEpoch());

        assertEquals(1L, queue.poll(0).timestamp);
        assertEquals(2L, queue.headRecordTimestamp());
        assertEquals(2L, queue.headRecordOffset().longValue());
        assertEquals(Optional.of(1), queue.headRecordLeaderEpoch());


        assertEquals(2L, queue.poll(0).timestamp);
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertEquals(RecordQueue.UNKNOWN, queue.headRecordTimestamp());
        assertNull(queue.headRecordOffset());
        assertNull(queue.headRecordLeaderEpoch());

        // add three more records with 4, 5, 6
        final List<ConsumerRecord<byte[], byte[]>> list3 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.of(2)),
            new ConsumerRecord<>("topic", 1, 5, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.of(3)),
            new ConsumerRecord<>("topic", 1, 6, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue, new RecordHeaders(), Optional.of(3)));

        queue.addRawRecords(list3);

        assertEquals(3, queue.size());
        assertEquals(4L, queue.headRecordTimestamp());
        assertEquals(4L, queue.headRecordOffset().longValue());
        assertEquals(Optional.of(2), queue.headRecordLeaderEpoch());

        // poll one record again, the timestamp should advance now
        assertEquals(4L, queue.poll(0).timestamp);
        assertEquals(2, queue.size());
        assertEquals(5L, queue.headRecordTimestamp());
        assertEquals(Optional.of(3), queue.headRecordLeaderEpoch());

        // clear the queue
        queue.clear();
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertEquals(RecordQueue.UNKNOWN, queue.headRecordTimestamp());
        assertEquals(RecordQueue.UNKNOWN, queue.partitionTime());
        assertNull(queue.headRecordOffset());
        assertNull(queue.headRecordLeaderEpoch());

        // re-insert the three records with 4, 5, 6
        queue.addRawRecords(list3);

        assertEquals(3, queue.size());
        assertEquals(4L, queue.headRecordTimestamp());
        assertEquals(4L, queue.headRecordOffset().longValue());
        assertEquals(Optional.of(2), queue.headRecordLeaderEpoch());
    }

    @Test
    public void shouldTrackPartitionTimeAsMaxProcessedTimestamp() {
        assertTrue(queue.isEmpty());
        assertThat(queue.size(), is(0));
        assertThat(queue.headRecordTimestamp(), is(RecordQueue.UNKNOWN));
        assertThat(queue.partitionTime(), is(RecordQueue.UNKNOWN));

        // add three 3 out-of-order records with timestamp 2, 1, 3, 4
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 3, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()));

        queue.addRawRecords(list1);
        assertThat(queue.partitionTime(), is(RecordQueue.UNKNOWN));

        queue.poll(0);
        assertThat(queue.partitionTime(), is(2L));

        queue.poll(0);
        assertThat(queue.partitionTime(), is(2L));

        queue.poll(0);
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
            new ConsumerRecord<>("topic", 1, 200, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 100, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 300, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 400, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()));

        queue.addRawRecords(list1);
        assertThat(queue.partitionTime(), is(150L));

        queue.poll(0);
        assertThat(queue.partitionTime(), is(200L));

        queue.setPartitionTime(500L);
        assertThat(queue.partitionTime(), is(500L));

        queue.poll(0);
        assertThat(queue.partitionTime(), is(500L));
    }

    @Test
    public void shouldThrowStreamsExceptionWhenKeyDeserializationFails() {
        final byte[] key = Serdes.Long().serializer().serialize("foo", 1L);
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, key, recordValue,
                new RecordHeaders(), Optional.empty()));

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
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, value,
                new RecordHeaders(), Optional.empty()));

        final StreamsException exception = assertThrows(
            StreamsException.class,
            () -> queue.addRawRecords(records)
        );
        assertThat(exception.getCause(), instanceOf(SerializationException.class));
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenKeyDeserializationFailsWithSkipHandler() {
        final byte[] key = Serdes.Long().serializer().serialize("foo", 1L);
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic", 1, 1, 0L,
            TimestampType.CREATE_TIME, 0, 0, key, recordValue,
            new RecordHeaders(), Optional.empty());
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(record);

        queueThatSkipsDeserializeErrors.addRawRecords(records);
        assertEquals(1, queueThatSkipsDeserializeErrors.size());
        assertEquals(new CorruptedRecord(record), queueThatSkipsDeserializeErrors.poll(0));
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenValueDeserializationFailsWithSkipHandler() {
        final byte[] value = Serdes.Long().serializer().serialize("foo", 1L);
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic", 1, 1, 0L,
            TimestampType.CREATE_TIME, 0, 0, recordKey, value,
            new RecordHeaders(), Optional.empty());
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            record);

        queueThatSkipsDeserializeErrors.addRawRecords(records);
        assertEquals(1, queueThatSkipsDeserializeErrors.size());
        assertEquals(new CorruptedRecord(record), queueThatSkipsDeserializeErrors.poll(0));
    }

    @Test
    public void shouldThrowOnNegativeTimestamp() {
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, -1L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()));

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
            "leaderEpoch = null, offset = 1, CreateTime = -1, deliveryCount = null, serialized key size = 0, " +
            "serialized value size = 0, headers = RecordHeaders(headers = [], isReadOnly = false), key = 1, value = 10) " +
            "has invalid (negative) timestamp. Possibly because a pre-0.10 producer client was used to write this record " +
            "to Kafka without embedding a timestamp, or because the input topic was created before upgrading the Kafka " +
            "cluster to 0.10+. Use a different TimestampExtractor to process this data."));
    }

    @Test
    public void shouldDropOnNegativeTimestamp() {
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, -1L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()));

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
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 3, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0, 0, recordKey, recordValue,
                new RecordHeaders(), Optional.empty()));

        assertEquals(RecordQueue.UNKNOWN, timestampExtractor.partitionTime);

        queue.addRawRecords(list1);

        // no (known) timestamp has yet been passed to the timestamp extractor
        assertEquals(RecordQueue.UNKNOWN, timestampExtractor.partitionTime);

        queue.poll(0);
        assertEquals(2L, timestampExtractor.partitionTime);

        queue.poll(0);
        assertEquals(2L, timestampExtractor.partitionTime);

        queue.poll(0);
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
