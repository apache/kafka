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
import org.apache.kafka.streams.processor.internals.AbstractPartitionGroup.RecordInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class SynchronizedPartitionGroupTest {

    @Mock
    private AbstractPartitionGroup wrapped;

    private SynchronizedPartitionGroup synchronizedPartitionGroup;

    private AutoCloseable closeable;

    @BeforeEach
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        synchronizedPartitionGroup = new SynchronizedPartitionGroup(wrapped);
    }

    @AfterEach
    public void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    public void testReadyToProcess() {
        final long wallClockTime = 0L;
        when(wrapped.readyToProcess(wallClockTime)).thenReturn(true);

        synchronizedPartitionGroup.readyToProcess(wallClockTime);

        verify(wrapped, times(1)).readyToProcess(wallClockTime);
    }

    @Test
    public void testUpdatePartitions() {
        final Set<TopicPartition> inputPartitions = Collections.singleton(new TopicPartition("topic", 0));
        @SuppressWarnings("unchecked") final Function<TopicPartition, RecordQueue> recordQueueCreator = (Function<TopicPartition, RecordQueue>) mock(Function.class);

        synchronizedPartitionGroup.updatePartitions(inputPartitions, recordQueueCreator);

        verify(wrapped, times(1)).updatePartitions(inputPartitions, recordQueueCreator);
    }

    @Test
    public void testSetPartitionTime() {
        final TopicPartition partition = new TopicPartition("topic", 0);
        final long partitionTime = 12345678L;

        synchronizedPartitionGroup.setPartitionTime(partition, partitionTime);

        verify(wrapped, times(1)).setPartitionTime(partition, partitionTime);
    }

    @Test
    public void testNextRecord() {
        final RecordInfo info = mock(RecordInfo.class);
        final long wallClockTime = 12345678L;
        final StampedRecord stampedRecord = mock(StampedRecord.class);
        when(wrapped.nextRecord(info, wallClockTime)).thenReturn(stampedRecord);

        final StampedRecord result = synchronizedPartitionGroup.nextRecord(info, wallClockTime);

        assertEquals(stampedRecord, result);
        verify(wrapped, times(1)).nextRecord(info, wallClockTime);
    }

    @Test
    public void testAddRawRecords() {
        final TopicPartition partition = new TopicPartition("topic", 0);
        @SuppressWarnings("unchecked") final Iterable<ConsumerRecord<byte[], byte[]>> rawRecords = (Iterable<ConsumerRecord<byte[], byte[]>>) mock(Iterable.class);
        when(wrapped.addRawRecords(partition, rawRecords)).thenReturn(1);

        final int result = synchronizedPartitionGroup.addRawRecords(partition, rawRecords);

        assertEquals(1, result);
        verify(wrapped, times(1)).addRawRecords(partition, rawRecords);
    }

    @Test
    public void testPartitionTimestamp() {
        final TopicPartition partition = new TopicPartition("topic", 0);
        final long timestamp = 12345678L;
        when(wrapped.partitionTimestamp(partition)).thenReturn(timestamp);

        final long result = synchronizedPartitionGroup.partitionTimestamp(partition);

        assertEquals(timestamp, result);
        verify(wrapped, times(1)).partitionTimestamp(partition);
    }

    @Test
    public void testStreamTime() {
        final long streamTime = 12345678L;
        when(wrapped.streamTime()).thenReturn(streamTime);

        final long result = synchronizedPartitionGroup.streamTime();

        assertEquals(streamTime, result);
        verify(wrapped, times(1)).streamTime();
    }

    @Test
    public void testHeadRecordOffset() {
        final TopicPartition partition = new TopicPartition("topic", 0);
        final Long recordOffset = 0L;
        when(wrapped.headRecordOffset(partition)).thenReturn(recordOffset);

        final Long result = synchronizedPartitionGroup.headRecordOffset(partition);

        assertEquals(recordOffset, result);
        verify(wrapped, times(1)).headRecordOffset(partition);
    }

    @Test
    public void testNumBuffered() {
        final int numBuffered = 1;
        when(wrapped.numBuffered()).thenReturn(numBuffered);

        final int result = synchronizedPartitionGroup.numBuffered();

        assertEquals(numBuffered, result);
        verify(wrapped, times(1)).numBuffered();
    }

    @Test
    public void testNumBufferedWithTopicPartition() {
        final TopicPartition partition = new TopicPartition("topic", 0);
        final int numBuffered = 1;
        when(wrapped.numBuffered(partition)).thenReturn(numBuffered);

        final int result = synchronizedPartitionGroup.numBuffered(partition);

        assertEquals(numBuffered, result);
        verify(wrapped, times(1)).numBuffered(partition);
    }

    @Test
    public void testClear() {
        synchronizedPartitionGroup.clear();

        verify(wrapped, times(1)).clear();
    }

    @Test
    public void testUpdateLags() {
        synchronizedPartitionGroup.updateLags();

        verify(wrapped, times(1)).updateLags();
    }

    @Test
    public void testClose() {
        synchronizedPartitionGroup.close();

        verify(wrapped, times(1)).close();
    }

    @Test
    public void testPartitions() {
        final Set<TopicPartition> partitions = Collections.singleton(new TopicPartition("topic", 0));
        when(wrapped.partitions()).thenReturn(partitions);

        final Set<TopicPartition> result = synchronizedPartitionGroup.partitions();

        assertEquals(partitions, result);
        verify(wrapped, times(1)).partitions();
    }
}