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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.is;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionGroupTest {
    private final LogContext logContext = new LogContext();
    private final Time time = new MockTime();
    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();
    private final TimestampExtractor timestampExtractor = new MockTimestampExtractor();
    private final TopicPartition unknownPartition = new TopicPartition("unknown-partition", 0);
    private final String errMessage = "Partition " + unknownPartition + " not found.";
    private final String[] topics = {"topic"};
    private final TopicPartition partition1 = createPartition1();
    private final TopicPartition partition2 = createPartition2();
    private final RecordQueue queue1 = createQueue1();
    private final RecordQueue queue2 = createQueue2();

    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);

    private final Metrics metrics = new Metrics();
    private final MetricName lastLatenessValue = new MetricName("record-lateness-last-value", "", "", mkMap());

    private PartitionGroup group;

    private static Sensor getValueSensor(final Metrics metrics, final MetricName metricName) {
        final Sensor lastRecordedValue = metrics.sensor(metricName.name());
        lastRecordedValue.add(metricName, new Value());
        return lastRecordedValue;
    }

    @Before
    public void setUp() {
        group = new PartitionGroup(
                mkMap(mkEntry(partition1, queue1), mkEntry(partition2, queue2)),
                getValueSensor(metrics, lastLatenessValue)
        );
    }

    @Test
    public void testTimeTracking() {
        testFirstBatch();
        testSecondBatch();
    }

    private RecordQueue createQueue1() {
        return new RecordQueue(
                partition1,
                new MockSourceNode<>(intDeserializer, intDeserializer),
                timestampExtractor,
                new LogAndContinueExceptionHandler(),
                new InternalMockProcessorContext(),
                logContext
        );
    }

    private RecordQueue createQueue2() {
        return new RecordQueue(
                partition2,
                new MockSourceNode<>(intDeserializer, intDeserializer),
                timestampExtractor,
                new LogAndContinueExceptionHandler(),
                new InternalMockProcessorContext(),
                logContext
        );
    }

    private TopicPartition createPartition1() {
        return new TopicPartition(topics[0], 1);
    }

    private TopicPartition createPartition2() {
        return new TopicPartition(topics[0], 2);
    }

    private void testFirstBatch() {
        StampedRecord record;
        final PartitionGroup.RecordInfo info = new PartitionGroup.RecordInfo();
        assertThat(group.numBuffered(), is(0));

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
        assertThat(group.partitionTimestamp(partition1), is(RecordQueue.UNKNOWN));
        assertThat(group.partitionTimestamp(partition2), is(RecordQueue.UNKNOWN));
        assertThat(group.headRecordOffset(partition1), is(1L));
        assertThat(group.headRecordOffset(partition2), is(2L));
        assertThat(group.streamTime(), is(RecordQueue.UNKNOWN));
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(0.0));

        // get one record, now the time should be advanced
        record = group.nextRecord(info, time.milliseconds());
        // 1:[3, 5]
        // 2:[2, 4, 6]
        // st: 1
        assertThat(info.partition(), equalTo(partition1));
        assertThat(group.partitionTimestamp(partition1), is(1L));
        assertThat(group.partitionTimestamp(partition2), is(RecordQueue.UNKNOWN));
        assertThat(group.headRecordOffset(partition1), is(3L));
        assertThat(group.headRecordOffset(partition2), is(2L));
        verifyTimes(record, 1L, 1L);
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(0.0));

        // get one record, now the time should be advanced
        record = group.nextRecord(info, time.milliseconds());
        // 1:[3, 5]
        // 2:[4, 6]
        // st: 2
        assertThat(info.partition(), equalTo(partition2));
        assertThat(group.partitionTimestamp(partition1), is(1L));
        assertThat(group.partitionTimestamp(partition2), is(2L));
        assertThat(group.headRecordOffset(partition1), is(3L));
        assertThat(group.headRecordOffset(partition2), is(4L));
        verifyTimes(record, 2L, 2L);
        verifyBuffered(4, 2, 2);
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());
    }

    private void testSecondBatch() {
        StampedRecord record;
        final PartitionGroup.RecordInfo info = new PartitionGroup.RecordInfo();

        // add 2 more records with timestamp 2, 4 to partition-1
        final List<ConsumerRecord<byte[], byte[]>> list3 = Arrays.asList(
                new ConsumerRecord<>("topic", 1, 2L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 4L, recordKey, recordValue));

        group.addRawRecords(partition1, list3);
        // 1:[3, 5, 2, 4]
        // 2:[4, 6]
        // st: 2 (just adding records shouldn't change it)
        verifyBuffered(6, 4, 2);
        assertThat(group.partitionTimestamp(partition1), is(1L));
        assertThat(group.partitionTimestamp(partition2), is(2L));
        assertThat(group.headRecordOffset(partition1), is(3L));
        assertThat(group.headRecordOffset(partition2), is(4L));
        assertThat(group.streamTime(), is(2L));
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(0.0));

        // get one record, time should be advanced
        record = group.nextRecord(info, time.milliseconds());
        // 1:[5, 2, 4]
        // 2:[4, 6]
        // st: 3
        assertThat(info.partition(), equalTo(partition1));
        assertThat(group.partitionTimestamp(partition1), is(3L));
        assertThat(group.partitionTimestamp(partition2), is(2L));
        assertThat(group.headRecordOffset(partition1), is(5L));
        assertThat(group.headRecordOffset(partition2), is(4L));
        verifyTimes(record, 3L, 3L);
        verifyBuffered(5, 3, 2);
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(0.0));

        // get one record, time should be advanced
        record = group.nextRecord(info, time.milliseconds());
        // 1:[5, 2, 4]
        // 2:[6]
        // st: 4
        assertThat(info.partition(), equalTo(partition2));
        assertThat(group.partitionTimestamp(partition1), is(3L));
        assertThat(group.partitionTimestamp(partition2), is(4L));
        assertThat(group.headRecordOffset(partition1), is(5L));
        assertThat(group.headRecordOffset(partition2), is(6L));
        verifyTimes(record, 4L, 4L);
        verifyBuffered(4, 3, 1);
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(0.0));

        // get one more record, time should be advanced
        record = group.nextRecord(info, time.milliseconds());
        // 1:[2, 4]
        // 2:[6]
        // st: 5
        assertThat(info.partition(), equalTo(partition1));
        assertThat(group.partitionTimestamp(partition1), is(5L));
        assertThat(group.partitionTimestamp(partition2), is(4L));
        assertThat(group.headRecordOffset(partition1), is(2L));
        assertThat(group.headRecordOffset(partition2), is(6L));
        verifyTimes(record, 5L, 5L);
        verifyBuffered(3, 2, 1);
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(0.0));

        // get one more record, time should not be advanced
        record = group.nextRecord(info, time.milliseconds());
        // 1:[4]
        // 2:[6]
        // st: 5
        assertThat(info.partition(), equalTo(partition1));
        assertThat(group.partitionTimestamp(partition1), is(5L));
        assertThat(group.partitionTimestamp(partition2), is(4L));
        assertThat(group.headRecordOffset(partition1), is(4L));
        assertThat(group.headRecordOffset(partition2), is(6L));
        verifyTimes(record, 2L, 5L);
        verifyBuffered(2, 1, 1);
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(3.0));

        // get one more record, time should not be advanced
        record = group.nextRecord(info, time.milliseconds());
        // 1:[]
        // 2:[6]
        // st: 5
        assertThat(info.partition(), equalTo(partition1));
        assertThat(group.partitionTimestamp(partition1), is(5L));
        assertThat(group.partitionTimestamp(partition2), is(4L));
        assertNull(group.headRecordOffset(partition1));
        assertThat(group.headRecordOffset(partition2), is(6L));
        verifyTimes(record, 4L, 5L);
        verifyBuffered(1, 0, 1);
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(1.0));

        // get one more record, time should be advanced
        record = group.nextRecord(info, time.milliseconds());
        // 1:[]
        // 2:[]
        // st: 6
        assertThat(info.partition(), equalTo(partition2));
        assertThat(group.partitionTimestamp(partition1), is(5L));
        assertThat(group.partitionTimestamp(partition2), is(6L));
        assertNull(group.headRecordOffset(partition1));
        assertNull(group.headRecordOffset(partition2));
        verifyTimes(record, 6L, 6L);
        verifyBuffered(0, 0, 0);
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(0.0));
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
        assertEquals(-1L, group.streamTime());
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());

        StampedRecord record;
        final PartitionGroup.RecordInfo info = new PartitionGroup.RecordInfo();

        // get first two records from partition 1
        record = group.nextRecord(info, time.milliseconds());
        assertEquals(record.timestamp, 1L);
        record = group.nextRecord(info, time.milliseconds());
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
        record = group.nextRecord(info, time.milliseconds());
        // 1:[3]
        // 2:[4, 6]
        assertEquals(record.timestamp, 2L);

        // get one record, next up should have ts=3 from partition 1 (even though it has seen a larger max timestamp =5)
        record = group.nextRecord(info, time.milliseconds());
        // 1:[]
        // 2:[4, 6]
        assertEquals(record.timestamp, 3L);
    }

    private void verifyTimes(final StampedRecord record, final long recordTime, final long streamTime) {
        assertThat(record.timestamp, is(recordTime));
        assertThat(group.streamTime(), is(streamTime));
    }

    private void verifyBuffered(final int totalBuffered, final int partitionOneBuffered, final int partitionTwoBuffered) {
        assertEquals(totalBuffered, group.numBuffered());
        assertEquals(partitionOneBuffered, group.numBuffered(partition1));
        assertEquals(partitionTwoBuffered, group.numBuffered(partition2));
    }

    @Test
    public void shouldSetPartitionTimestampAndStreamTime() {
        group.setPartitionTime(partition1, 100L);
        assertEquals(100L, group.partitionTimestamp(partition1));
        assertEquals(100L, group.streamTime());
        group.setPartitionTime(partition2, 50L);
        assertEquals(50L, group.partitionTimestamp(partition2));
        assertEquals(100L, group.streamTime());
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponAddRecordsIfPartitionUnknown() {
        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.addRawRecords(unknownPartition, null));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponNumBufferedIfPartitionUnknown() {
        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.numBuffered(unknownPartition));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponSetPartitionTimestampIfPartitionUnknown() {
        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.setPartitionTime(unknownPartition, 0L));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponGetPartitionTimestampIfPartitionUnknown() {
        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.partitionTimestamp(unknownPartition));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponGetHeadRecordOffsetIfPartitionUnknown() {
        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.headRecordOffset(unknownPartition));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldEmptyPartitionsOnClear() {
        final List<ConsumerRecord<byte[], byte[]>> list = Arrays.asList(
                new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 3L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list);
        group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds());
        group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds());

        group.clear();

        assertThat(group.numBuffered(), equalTo(0));
        assertThat(group.streamTime(), equalTo(RecordQueue.UNKNOWN));
        assertThat(group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds()), equalTo(null));
        assertThat(group.partitionTimestamp(partition1), equalTo(RecordQueue.UNKNOWN));

        group.addRawRecords(partition1, list);
    }

    @Test
    public void shouldUpdatePartitionQueuesShrink() {
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
                new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);
        final List<ConsumerRecord<byte[], byte[]>> list2 = Arrays.asList(
                new ConsumerRecord<>("topic", 2, 2L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 2, 4L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 2, 6L, recordKey, recordValue));
        group.addRawRecords(partition2, list2);
        assertEquals(list1.size() + list2.size(), group.numBuffered());
        assertTrue(group.allPartitionsBuffered());
        group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds());

        // shrink list of queues
        group.updatePartitions(mkSet(createPartition2()), p -> {
            fail("should not create any queues");
            return null;
        });

        assertTrue(group.allPartitionsBuffered());  // because didn't add any new partitions
        assertEquals(list2.size(), group.numBuffered());
        assertEquals(1, group.streamTime());
        assertThrows(IllegalStateException.class, () -> group.partitionTimestamp(partition1));
        assertThat(group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds()), notNullValue());  // can access buffered records
        assertThat(group.partitionTimestamp(partition2), equalTo(2L));
    }

    @Test
    public void shouldUpdatePartitionQueuesExpand() {
        group = new PartitionGroup(
                mkMap(mkEntry(partition1, queue1)),
                getValueSensor(metrics, lastLatenessValue)
        );
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
                new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);

        assertEquals(list1.size(), group.numBuffered());
        assertTrue(group.allPartitionsBuffered());
        group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds());

        // expand list of queues
        group.updatePartitions(mkSet(createPartition1(), createPartition2()), p -> {
            assertEquals(createPartition2(), p);
            return createQueue2();
        });

        assertFalse(group.allPartitionsBuffered());  // because added new partition
        assertEquals(1, group.numBuffered());
        assertEquals(1, group.streamTime());
        assertThat(group.partitionTimestamp(partition1), equalTo(1L));
        assertThat(group.partitionTimestamp(partition2), equalTo(RecordQueue.UNKNOWN));
        assertThat(group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds()), notNullValue());  // can access buffered records
    }

    @Test
    public void shouldUpdatePartitionQueuesShrinkAndExpand() {
        group = new PartitionGroup(
                mkMap(mkEntry(partition1, queue1)),
                getValueSensor(metrics, lastLatenessValue)
        );
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
                new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);
        assertEquals(list1.size(), group.numBuffered());
        assertTrue(group.allPartitionsBuffered());
        group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds());

        // expand and shrink list of queues
        group.updatePartitions(mkSet(createPartition2()), p -> {
            assertEquals(createPartition2(), p);
            return createQueue2();
        });

        assertFalse(group.allPartitionsBuffered());  // because added new partition
        assertEquals(0, group.numBuffered());
        assertEquals(1, group.streamTime());
        assertThrows(IllegalStateException.class, () -> group.partitionTimestamp(partition1));
        assertThat(group.partitionTimestamp(partition2), equalTo(RecordQueue.UNKNOWN));
        assertThat(group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds()), nullValue());  // all available records removed
    }
}
