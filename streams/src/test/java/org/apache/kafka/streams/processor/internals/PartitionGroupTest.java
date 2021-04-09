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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionGroupTest {

    private final long maxTaskIdleMs = StreamsConfig.MAX_TASK_IDLE_MS_DISABLED;
    private final LogContext logContext = new LogContext("[test] ");
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
    private final Sensor enforcedProcessingSensor = metrics.sensor(UUID.randomUUID().toString());
    private final MetricName lastLatenessValue = new MetricName("record-lateness-last-value", "", "", mkMap());


    private static Sensor getValueSensor(final Metrics metrics, final MetricName metricName) {
        final Sensor lastRecordedValue = metrics.sensor(metricName.name());
        lastRecordedValue.add(metricName, new Value());
        return lastRecordedValue;
    }

    @Test
    public void testTimeTracking() {
        final PartitionGroup group = getBasicGroup();

        testFirstBatch(group);
        testSecondBatch(group);
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

    private void testFirstBatch(final PartitionGroup group) {
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

        verifyBuffered(6, 3, 3, group);
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
        verifyTimes(record, 1L, 1L, group);
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
        verifyTimes(record, 2L, 2L, group);
        verifyBuffered(4, 2, 2, group);
        assertEquals(0.0, metrics.metric(lastLatenessValue).metricValue());
    }

    private void testSecondBatch(final PartitionGroup group) {
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
        verifyBuffered(6, 4, 2, group);
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
        verifyTimes(record, 3L, 3L, group);
        verifyBuffered(5, 3, 2, group);
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
        verifyTimes(record, 4L, 4L, group);
        verifyBuffered(4, 3, 1, group);
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
        verifyTimes(record, 5L, 5L, group);
        verifyBuffered(3, 2, 1, group);
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
        verifyTimes(record, 2L, 5L, group);
        verifyBuffered(2, 1, 1, group);
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
        verifyTimes(record, 4L, 5L, group);
        verifyBuffered(1, 0, 1, group);
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
        verifyTimes(record, 6L, 6L, group);
        verifyBuffered(0, 0, 0, group);
        assertThat(metrics.metric(lastLatenessValue).metricValue(), is(0.0));
    }

    @Test
    public void shouldChooseNextRecordBasedOnHeadTimestamp() {
        final PartitionGroup group = getBasicGroup();

        assertEquals(0, group.numBuffered());

        // add three 3 records with timestamp 1, 5, 3 to partition-1
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
                new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 3L, recordKey, recordValue));

        group.addRawRecords(partition1, list1);

        verifyBuffered(3, 3, 0, group);
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

    private void verifyTimes(final StampedRecord record,
                             final long recordTime,
                             final long streamTime,
                             final PartitionGroup group) {
        assertThat(record.timestamp, is(recordTime));
        assertThat(group.streamTime(), is(streamTime));
    }

    private void verifyBuffered(final int totalBuffered,
                                final int partitionOneBuffered,
                                final int partitionTwoBuffered,
                                final PartitionGroup group) {
        assertEquals(totalBuffered, group.numBuffered());
        assertEquals(partitionOneBuffered, group.numBuffered(partition1));
        assertEquals(partitionTwoBuffered, group.numBuffered(partition2));
    }

    @Test
    public void shouldSetPartitionTimestampAndStreamTime() {
        final PartitionGroup group = getBasicGroup();

        group.setPartitionTime(partition1, 100L);
        assertEquals(100L, group.partitionTimestamp(partition1));
        assertEquals(100L, group.streamTime());
        group.setPartitionTime(partition2, 50L);
        assertEquals(50L, group.partitionTimestamp(partition2));
        assertEquals(100L, group.streamTime());
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponAddRecordsIfPartitionUnknown() {
        final PartitionGroup group = getBasicGroup();

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.addRawRecords(unknownPartition, null));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponNumBufferedIfPartitionUnknown() {
        final PartitionGroup group = getBasicGroup();

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.numBuffered(unknownPartition));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponSetPartitionTimestampIfPartitionUnknown() {
        final PartitionGroup group = getBasicGroup();

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.setPartitionTime(unknownPartition, 0L));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponGetPartitionTimestampIfPartitionUnknown() {
        final PartitionGroup group = getBasicGroup();

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.partitionTimestamp(unknownPartition));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionUponGetHeadRecordOffsetIfPartitionUnknown() {
        final PartitionGroup group = getBasicGroup();

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> group.headRecordOffset(unknownPartition));
        assertThat(errMessage, equalTo(exception.getMessage()));
    }

    @Test
    public void shouldEmptyPartitionsOnClear() {
        final PartitionGroup group = getBasicGroup();

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
        final PartitionGroup group = getBasicGroup();

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
        assertTrue(group.allPartitionsBufferedLocally());
        group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds());

        // shrink list of queues
        group.updatePartitions(mkSet(createPartition2()), p -> {
            fail("should not create any queues");
            return null;
        });

        assertTrue(group.allPartitionsBufferedLocally());  // because didn't add any new partitions
        assertEquals(list2.size(), group.numBuffered());
        assertEquals(1, group.streamTime());
        assertThrows(IllegalStateException.class, () -> group.partitionTimestamp(partition1));
        assertThat(group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds()), notNullValue());  // can access buffered records
        assertThat(group.partitionTimestamp(partition2), equalTo(2L));
    }

    @Test
    public void shouldUpdatePartitionQueuesExpand() {
        final PartitionGroup group = new PartitionGroup(
            logContext,
            mkMap(mkEntry(partition1, queue1)),
            tp -> OptionalLong.of(0L),
            getValueSensor(metrics, lastLatenessValue),
            enforcedProcessingSensor,
            maxTaskIdleMs
        );
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
                new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);

        assertEquals(list1.size(), group.numBuffered());
        assertTrue(group.allPartitionsBufferedLocally());
        group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds());

        // expand list of queues
        group.updatePartitions(mkSet(createPartition1(), createPartition2()), p -> {
            assertEquals(createPartition2(), p);
            return createQueue2();
        });

        assertFalse(group.allPartitionsBufferedLocally());  // because added new partition
        assertEquals(1, group.numBuffered());
        assertEquals(1, group.streamTime());
        assertThat(group.partitionTimestamp(partition1), equalTo(1L));
        assertThat(group.partitionTimestamp(partition2), equalTo(RecordQueue.UNKNOWN));
        assertThat(group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds()), notNullValue());  // can access buffered records
    }

    @Test
    public void shouldUpdatePartitionQueuesShrinkAndExpand() {
        final PartitionGroup group = new PartitionGroup(
            logContext,
            mkMap(mkEntry(partition1, queue1)),
            tp -> OptionalLong.of(0L),
            getValueSensor(metrics, lastLatenessValue),
            enforcedProcessingSensor,
            maxTaskIdleMs
        );
        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
                new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
                new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);
        assertEquals(list1.size(), group.numBuffered());
        assertTrue(group.allPartitionsBufferedLocally());
        group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds());

        // expand and shrink list of queues
        group.updatePartitions(mkSet(createPartition2()), p -> {
            assertEquals(createPartition2(), p);
            return createQueue2();
        });

        assertFalse(group.allPartitionsBufferedLocally());  // because added new partition
        assertEquals(0, group.numBuffered());
        assertEquals(1, group.streamTime());
        assertThrows(IllegalStateException.class, () -> group.partitionTimestamp(partition1));
        assertThat(group.partitionTimestamp(partition2), equalTo(RecordQueue.UNKNOWN));
        assertThat(group.nextRecord(new PartitionGroup.RecordInfo(), time.milliseconds()), nullValue());  // all available records removed
    }

    @Test
    public void shouldNeverWaitIfIdlingIsDisabled() {
        final PartitionGroup group = new PartitionGroup(
            logContext,
            mkMap(
                mkEntry(partition1, queue1),
                mkEntry(partition2, queue2)
            ),
            tp -> OptionalLong.of(0L),
            getValueSensor(metrics, lastLatenessValue),
            enforcedProcessingSensor,
            StreamsConfig.MAX_TASK_IDLE_MS_DISABLED
        );

        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);

        assertThat(group.allPartitionsBufferedLocally(), is(false));
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(PartitionGroup.class)) {
            LogCaptureAppender.setClassLoggerToTrace(PartitionGroup.class);
            assertThat(group.readyToProcess(0L), is(true));
            assertThat(
                appender.getEvents(),
                hasItem(Matchers.allOf(
                    Matchers.hasProperty("level", equalTo("TRACE")),
                    Matchers.hasProperty("message", equalTo(
                        "[test] Ready for processing because max.task.idle.ms is disabled.\n" +
                            "\tThere may be out-of-order processing for this task as a result.\n" +
                            "\tBuffered partitions: [topic-1]\n" +
                            "\tNon-buffered partitions: [topic-2]"
                    ))
                ))
            );
        }
    }

    @Test
    public void shouldBeReadyIfAllPartitionsAreBuffered() {
        final PartitionGroup group = new PartitionGroup(
            logContext,
            mkMap(
                mkEntry(partition1, queue1),
                mkEntry(partition2, queue2)
            ),
            tp -> OptionalLong.of(0L),
            getValueSensor(metrics, lastLatenessValue),
            enforcedProcessingSensor,
            0L
        );

        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);

        final List<ConsumerRecord<byte[], byte[]>> list2 = Arrays.asList(
            new ConsumerRecord<>("topic", 2, 1L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 2, 5L, recordKey, recordValue));
        group.addRawRecords(partition2, list2);

        assertThat(group.allPartitionsBufferedLocally(), is(true));
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(PartitionGroup.class)) {
            LogCaptureAppender.setClassLoggerToTrace(PartitionGroup.class);
            assertThat(group.readyToProcess(0L), is(true));
            assertThat(
                appender.getEvents(),
                hasItem(Matchers.allOf(
                    Matchers.hasProperty("level", equalTo("TRACE")),
                    Matchers.hasProperty("message", equalTo("[test] All partitions were buffered locally, so this task is ready for processing."))
                ))
            );
        }
    }

    @Test
    public void shouldWaitForFetchesWhenMetadataIsIncomplete() {
        final HashMap<TopicPartition, OptionalLong> lags = new HashMap<>();
        final PartitionGroup group = new PartitionGroup(
            logContext,
            mkMap(
                mkEntry(partition1, queue1),
                mkEntry(partition2, queue2)
            ),
            tp -> lags.getOrDefault(tp, OptionalLong.empty()),
            getValueSensor(metrics, lastLatenessValue),
            enforcedProcessingSensor,
            0L
        );

        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);

        assertThat(group.allPartitionsBufferedLocally(), is(false));
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(PartitionGroup.class)) {
            LogCaptureAppender.setClassLoggerToTrace(PartitionGroup.class);
            assertThat(group.readyToProcess(0L), is(false));
            assertThat(
                appender.getEvents(),
                hasItem(Matchers.allOf(
                    Matchers.hasProperty("level", equalTo("TRACE")),
                    Matchers.hasProperty("message", equalTo("[test] Waiting to fetch data for topic-2"))
                ))
            );
        }
        lags.put(partition2, OptionalLong.of(0L));
        assertThat(group.readyToProcess(0L), is(true));
    }

    @Test
    public void shouldWaitForPollWhenLagIsNonzero() {
        final HashMap<TopicPartition, OptionalLong> lags = new HashMap<>();
        final PartitionGroup group = new PartitionGroup(
            logContext,
            mkMap(
                mkEntry(partition1, queue1),
                mkEntry(partition2, queue2)
            ),
            tp -> lags.getOrDefault(tp, OptionalLong.empty()),
            getValueSensor(metrics, lastLatenessValue),
            enforcedProcessingSensor,
            0L
        );

        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);

        lags.put(partition2, OptionalLong.of(1L));

        assertThat(group.allPartitionsBufferedLocally(), is(false));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(PartitionGroup.class)) {
            LogCaptureAppender.setClassLoggerToTrace(PartitionGroup.class);
            assertThat(group.readyToProcess(0L), is(false));
            assertThat(
                appender.getEvents(),
                hasItem(Matchers.allOf(
                    Matchers.hasProperty("level", equalTo("TRACE")),
                    Matchers.hasProperty("message", equalTo("[test] Lag for topic-2 is currently 1, but no data is buffered locally. Waiting to buffer some records."))
                ))
            );
        }
    }

    @Test
    public void shouldIdleAsSpecifiedWhenLagIsZero() {
        final PartitionGroup group = new PartitionGroup(
            logContext,
            mkMap(
                mkEntry(partition1, queue1),
                mkEntry(partition2, queue2)
            ),
            tp -> OptionalLong.of(0L),
            getValueSensor(metrics, lastLatenessValue),
            enforcedProcessingSensor,
            1L
        );

        final List<ConsumerRecord<byte[], byte[]>> list1 = Arrays.asList(
            new ConsumerRecord<>("topic", 1, 1L, recordKey, recordValue),
            new ConsumerRecord<>("topic", 1, 5L, recordKey, recordValue));
        group.addRawRecords(partition1, list1);

        assertThat(group.allPartitionsBufferedLocally(), is(false));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(PartitionGroup.class)) {
            LogCaptureAppender.setClassLoggerToTrace(PartitionGroup.class);
            assertThat(group.readyToProcess(0L), is(false));
            assertThat(
                appender.getEvents(),
                hasItem(Matchers.allOf(
                    Matchers.hasProperty("level", equalTo("TRACE")),
                    Matchers.hasProperty("message", equalTo("[test] Lag for topic-2 is currently 0 and current time is 0. Waiting for new data to be produced for configured idle time 1 (deadline is 1)."))
                ))
            );
        }

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(PartitionGroup.class)) {
            LogCaptureAppender.setClassLoggerToTrace(PartitionGroup.class);
            assertThat(group.readyToProcess(1L), is(true));
            assertThat(
                appender.getEvents(),
                hasItem(Matchers.allOf(
                    Matchers.hasProperty("level", equalTo("TRACE")),
                    Matchers.hasProperty("message", equalTo(
                        "[test] Continuing to process although some partitions are empty on the broker.\n" +
                            "\tThere may be out-of-order processing for this task as a result.\n" +
                            "\tPartitions with local data: [topic-1].\n" +
                            "\tPartitions we gave up waiting for, with their corresponding deadlines: {topic-2=1}.\n" +
                            "\tConfigured max.task.idle.ms: 1.\n" +
                            "\tCurrent wall-clock time: 1."
                    ))
                ))
            );
        }

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(PartitionGroup.class)) {
            LogCaptureAppender.setClassLoggerToTrace(PartitionGroup.class);
            assertThat(group.readyToProcess(2L), is(true));
            assertThat(
                appender.getEvents(),
                hasItem(Matchers.allOf(
                    Matchers.hasProperty("level", equalTo("TRACE")),
                    Matchers.hasProperty("message", equalTo(
                        "[test] Continuing to process although some partitions are empty on the broker.\n" +
                            "\tThere may be out-of-order processing for this task as a result.\n" +
                            "\tPartitions with local data: [topic-1].\n" +
                            "\tPartitions we gave up waiting for, with their corresponding deadlines: {topic-2=1}.\n" +
                            "\tConfigured max.task.idle.ms: 1.\n" +
                            "\tCurrent wall-clock time: 2."
                    ))
                ))
            );
        }
    }

    private PartitionGroup getBasicGroup() {
        return new PartitionGroup(
            logContext,
            mkMap(
                mkEntry(partition1, queue1),
                mkEntry(partition2, queue2)
            ),
            tp -> OptionalLong.of(0L),
            getValueSensor(metrics, lastLatenessValue),
            enforcedProcessingSensor,
            maxTaskIdleMs
        );
    }
}
