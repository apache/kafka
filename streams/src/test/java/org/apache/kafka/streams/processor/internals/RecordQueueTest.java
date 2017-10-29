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
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RecordQueueTest {
    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();
    private final TimestampExtractor timestampExtractor = new MockTimestampExtractor();
    private final String[] topics = {"topic"};

    final MockProcessorContext context = new MockProcessorContext(StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class),
            new RecordCollectorImpl(null, null,  new LogContext("record-queue-test ")));
    private final MockSourceNode mockSourceNodeWithMetrics = new MockSourceNode<>(topics, intDeserializer, intDeserializer);
    private final RecordQueue queue = new RecordQueue(
        new TopicPartition(topics[0], 1),
        mockSourceNodeWithMetrics,
        timestampExtractor,
        new LogAndFailExceptionHandler(),
        context,
        new LogContext());
    private final RecordQueue queueThatSkipsDeserializeErrors = new RecordQueue(
        new TopicPartition(topics[0], 1),
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

        // add three 3 out-of-order records with timestamp 2, 1, 3
        List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 3, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        queue.addRawRecords(list1);

        assertEquals(3, queue.size());
        assertEquals(1L, queue.timestamp());

        // poll the first record, now with 1, 3
        assertEquals(2L, queue.poll().timestamp);
        assertEquals(2, queue.size());
        assertEquals(1L, queue.timestamp());

        // poll the second record, now with 3
        assertEquals(1L, queue.poll().timestamp);
        assertEquals(1, queue.size());
        assertEquals(3L, queue.timestamp());

        // add three 3 out-of-order records with timestamp 4, 1, 2
        // now with 3, 4, 1, 2
        List<ConsumerRecord<byte[], byte[]>> list2 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        queue.addRawRecords(list2);

        assertEquals(4, queue.size());
        assertEquals(3L, queue.timestamp());

        // poll the third record, now with 4, 1, 2
        assertEquals(3L, queue.poll().timestamp);
        assertEquals(3, queue.size());
        assertEquals(3L, queue.timestamp());

        // poll the rest records
        assertEquals(4L, queue.poll().timestamp);
        assertEquals(3L, queue.timestamp());

        assertEquals(1L, queue.poll().timestamp);
        assertEquals(3L, queue.timestamp());

        assertEquals(2L, queue.poll().timestamp);
        assertEquals(0, queue.size());
        assertEquals(3L, queue.timestamp());

        // add three more records with 4, 5, 6
        List<ConsumerRecord<byte[], byte[]>> list3 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 4, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 5, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 6, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        queue.addRawRecords(list3);

        assertEquals(3, queue.size());
        assertEquals(4L, queue.timestamp());

        // poll one record again, the timestamp should advance now
        assertEquals(4L, queue.poll().timestamp);
        assertEquals(2, queue.size());
        assertEquals(5L, queue.timestamp());
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionWhenKeyDeserializationFails() {
        final byte[] key = Serdes.Long().serializer().serialize("foo", 1L);
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
                new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, key, recordValue));

        queue.addRawRecords(records);
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionWhenValueDeserializationFails() {
        final byte[] value = Serdes.Long().serializer().serialize("foo", 1L);
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
                new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, value));

        queue.addRawRecords(records);
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenKeyDeserializationFailsWithSkipHandler() throws Exception {
        final byte[] key = Serdes.Long().serializer().serialize("foo", 1L);
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, key, recordValue));
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);

        queueThatSkipsDeserializeErrors.addRawRecords(records);
        assertEquals(0, queueThatSkipsDeserializeErrors.size());
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenValueDeserializationFailsWithSkipHandler() throws Exception {
        final byte[] value = Serdes.Long().serializer().serialize("foo", 1L);
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
            new ConsumerRecord<>("topic", 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, value));

        queueThatSkipsDeserializeErrors.addRawRecords(records);
        assertEquals(0, queueThatSkipsDeserializeErrors.size());
    }


    @Test(expected = StreamsException.class)
    public void shouldThrowOnNegativeTimestamp() {
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
                new ConsumerRecord<>("topic", 1, 1, -1L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        final RecordQueue queue = new RecordQueue(new TopicPartition(topics[0], 1),
                                                  new MockSourceNode<>(topics, intDeserializer, intDeserializer),
                                                  new FailOnInvalidTimestamp(),
                                                  new LogAndContinueExceptionHandler(),
                                                  null,
                                                  new LogContext());
        queue.addRawRecords(records);
    }

    @Test
    public void shouldDropOnNegativeTimestamp() {
        final List<ConsumerRecord<byte[], byte[]>> records = Collections.singletonList(
                new ConsumerRecord<>("topic", 1, 1, -1L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue));

        final RecordQueue queue = new RecordQueue(new TopicPartition(topics[0], 1),
                                                  new MockSourceNode<>(topics, intDeserializer, intDeserializer),
                                                  new LogAndSkipOnInvalidTimestamp(),
                                                  new LogAndContinueExceptionHandler(),
                                                  null,
                                                  new LogContext());
        queue.addRawRecords(records);

        assertEquals(0, queue.size());
    }
}
