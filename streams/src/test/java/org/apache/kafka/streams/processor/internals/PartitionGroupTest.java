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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.Assert.assertEquals;

public class PartitionGroupTest {
    private final LogContext logContext = new LogContext();
    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();
    private final TimestampExtractor timestampExtractor = new MockTimestampExtractor();
    private final String[] topics = {"topic"};
    private final TopicPartition partition1 = new TopicPartition(topics[0], 1);
    private final TopicPartition partition2 = new TopicPartition(topics[0], 2);
    private final RecordQueue queue1 = new RecordQueue(
        partition1,
        new MockSourceNode<>(topics, intDeserializer, intDeserializer),
        timestampExtractor,
        new LogAndContinueExceptionHandler(),
        new InternalMockProcessorContext(),
        logContext
    );
    private final RecordQueue queue2 = new RecordQueue(
        partition2,
        new MockSourceNode<>(topics, intDeserializer, intDeserializer),
        timestampExtractor,
        new LogAndContinueExceptionHandler(),
        new InternalMockProcessorContext(),
        logContext
    );

    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);

    private final Metrics metrics = new Metrics();
    private final MetricName lastLatenessValue = new MetricName("record-lateness-last-value", "", "", mkMap());

    private final PartitionGroup group = new PartitionGroup(
        mkMap(mkEntry(partition1, queue1), mkEntry(partition2, queue2)),
        getValueSensor(metrics, lastLatenessValue)
    );

    private static Sensor getValueSensor(final Metrics metrics, final MetricName metricName) {
        final Sensor lastRecordedValue = metrics.sensor(metricName.name());
        lastRecordedValue.add(metricName, new Value());
        return lastRecordedValue;
    }

    @Test
    public void testTimeTracking() {
        assertEquals(0, group.numBuffered());

        // add three 3 records with timestamp 1, 3, 5 to partition-1
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 3L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));

        group.addRawRecords(partition1, list1);

        // add three 3 records with timestamp 2, 4, 6 to partition-2
        final List<ConsumerRecord<byte[], byte[]>> list2 = Arrays.asList(
            new ConsumerRecord<>("topic", 2, 2L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 2, 4L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 2, 6L, recordKey, recordValue));

        group.addRawRecords(partition2, list2);
        // 1:[1, 3, 5]
        // 2:[2, 4, 6]
        // st: -1 since no records was being processed yet

        verifyBuffered(6, 3, 3);
        assertEquals(-1L, group.timestamp());
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

        StampedRecord record;
        final PartitionGroup.RecordInfo info = new PartitionGroup.RecordInfo();

        // get one record, now the time should be advanced
        record = group.nextRecord(info);
        // 1:[3, 5]
        // 2:[2, 4, 6]
        // st: 1
        assertEquals(partition1, info.partition());
        verifyTimes(record, 1L, 1L);
        verifyBuffered(5, 2, 3);
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one record, now the time should be advanced
        record = group.nextRecord(info);
        // 1:[3, 5]
        // 2:[4, 6]
        // st: 2
        assertEquals(partition2, info.partition());
        verifyTimes(record, 2L, 2L);
        verifyBuffered(4, 2, 2);
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

        // add 2 more records with timestamp 2, 4 to partition-1
        final List<ConsumerRecord<byte[], byte[]>> list3 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 2L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 4L, recordKey, recordValue));

        group.addRawRecords(partition1, list3);
        // 1:[3, 5, 2, 4]
        // 2:[4, 6]
        // st: 2 (just adding records shouldn't change it)
        verifyBuffered(6, 4, 2);
        assertEquals(2L, group.timestamp());
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one record, time should be advanced
        record = group.nextRecord(info);
        // 1:[5, 2, 4]
        // 2:[4, 6]
        // st: 3
        assertEquals(partition1, info.partition());
        verifyTimes(record, 3L, 3L);
        verifyBuffered(5, 3, 2);
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one record, time should be advanced
        record = group.nextRecord(info);
        // 1:[5, 2, 4]
        // 2:[6]
        // st: 4
        assertEquals(partition2, info.partition());
        verifyTimes(record, 4L, 4L);
        verifyBuffered(4, 3, 1);
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one more record, time should be advanced
        record = group.nextRecord(info);
        // 1:[2, 4]
        // 2:[6]
        // st: 5
        assertEquals(partition1, info.partition());
        verifyTimes(record, 5L, 5L);
        verifyBuffered(3, 2, 1);
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one more record, time should not be advanced
        record = group.nextRecord(info);
        // 1:[4]
        // 2:[6]
        // st: 5
        assertEquals(partition1, info.partition());
        verifyTimes(record, 2L, 5L);
        verifyBuffered(2, 1, 1);
        assertEquals(3.0, metrics.metric(lastLatenessValue).metricValue());

        // get one more record, time should not be advanced
        record = group.nextRecord(info);
        // 1:[]
        // 2:[6]
        // st: 5
        assertEquals(partition1, info.partition());
        verifyTimes(record, 4L, 5L);
        verifyBuffered(1, 0, 1);
        assertEquals(1.0, metrics.metric(lastLatenessValue).metricValue());

        // get one more record, time should be advanced
        record = group.nextRecord(info);
        // 1:[]
        // 2:[]
        // st: 6
        assertEquals(partition2, info.partition());
        verifyTimes(record, 6L, 6L);
        verifyBuffered(0, 0, 0);
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

    }

    @Test
    public void shouldChooseNextRecordBasedOnHeadTimestamp() {
        assertEquals(0, group.numBuffered());

        // add three 3 records with timestamp 1, 5, 3 to partition-1
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 3L, recordKey, recordValue));

        group.addRawRecords(partition1, list1);

        verifyBuffered(3, 3, 0);
        assertEquals(-1L, group.timestamp());
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

        StampedRecord record;
        final PartitionGroup.RecordInfo info = new PartitionGroup.RecordInfo();

        // get first two records from partition 1
        record = group.nextRecord(info);
        assertEquals(record.timestamp, 1L);
        record = group.nextRecord(info);
        assertEquals(record.timestamp, 5L);

        // add three 3 records with timestamp 2, 4, 6 to partition-2
        final List<ConsumerRecord<byte[], byte[]>> list2 = Arrays.asList(
            new ConsumerRecord<>("topic", 2, 2L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 2, 4L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 2, 6L, recordKey, recordValue));

        group.addRawRecords(partition2, list2);
        // 1:[3]
        // 2:[2, 4, 6]

        // get one record, next record should be ts=2 from partition 2
        record = group.nextRecord(info);
        // 1:[3]
        // 2:[4, 6]
        assertEquals(record.timestamp, 2L);

        // get one record, next up should have ts=3 from partition 1 (even though it has seen a larger max timestamp =5)
        record = group.nextRecord(info);
        // 1:[]
        // 2:[4, 6]
        assertEquals(record.timestamp, 3L);
    }

    private void verifyTimes(final StampedRecord record, final long recordTime, final long streamTime) {
        assertEquals(recordTime, record.timestamp);
        assertEquals(streamTime, group.timestamp());
    }

    private void verifyBuffered(final int totalBuffered, final int partitionOneBuffered, final int partitionTwoBuffered) {
        assertEquals(totalBuffered, group.numBuffered());
        assertEquals(partitionOneBuffered, group.numBuffered(partition1));
        assertEquals(partitionTwoBuffered, group.numBuffered(partition2));
    }
}
