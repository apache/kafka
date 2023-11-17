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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.mirror.MirrorSourceTask.PartitionState;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

public class MirrorSourceTaskTest {

    private static ConsumerRecord<byte[], byte[]> getRecordOfBytes(TopicPartition partition, int byteSize) {
        return new ConsumerRecord<>(
                partition.topic(),
                partition.partition(),
                0,
                null,
                getRandomString(byteSize).getBytes(StandardCharsets.UTF_8));
    }

    private static String getRandomString(int length) {
        String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(candidateChars.charAt(random.nextInt(candidateChars.length())));
        }
        return sb.toString();
    }

    @Test
    public void testSerde() {
        byte[] key = new byte[]{'a', 'b', 'c', 'd', 'e'};
        byte[] value = new byte[]{'f', 'g', 'h', 'i', 'j', 'k'};
        Headers headers = new RecordHeaders();
        headers.add("header1", new byte[]{'l', 'm', 'n', 'o'});
        headers.add("header2", new byte[]{'p', 'q', 'r', 's', 't'});
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic1", 2, 3L, 4L,
            TimestampType.CREATE_TIME, 5, 6, key, value, headers, Optional.empty());
        @SuppressWarnings("unchecked")
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(null, null, "cluster7",
                new DefaultReplicationPolicy(), 50, producer, null, null, null);
        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(consumerRecord);
        assertEquals("cluster7.topic1", sourceRecord.topic(),
                "Failure on cluster7.topic1 consumerRecord serde");
        assertEquals(2, sourceRecord.kafkaPartition().intValue(),
                "sourceRecord kafka partition is incorrect");
        assertEquals(new TopicPartition("topic1", 2), MirrorUtils.unwrapPartition(sourceRecord.sourcePartition()),
                "topic1 unwrapped from sourcePartition is incorrect");
        assertEquals(3L, MirrorUtils.unwrapOffset(sourceRecord.sourceOffset()).longValue(),
                "sourceRecord's sourceOffset is incorrect");
        assertEquals(4L, sourceRecord.timestamp().longValue(),
                "sourceRecord's timestamp is incorrect");
        assertEquals(key, sourceRecord.key(), "sourceRecord's key is incorrect");
        assertEquals(value, sourceRecord.value(), "sourceRecord's value is incorrect");
        assertEquals(headers.lastHeader("header1").value(), sourceRecord.headers().lastWithName("header1").value(),
                "sourceRecord's header1 is incorrect");
        assertEquals(headers.lastHeader("header2").value(), sourceRecord.headers().lastWithName("header2").value(),
                "sourceRecord's header2 is incorrect");
    }

    @Test
    public void testOffsetSync() {
        MirrorSourceTask.PartitionState partitionState = new MirrorSourceTask.PartitionState(50);

        assertTrue(partitionState.update(0, 100), "always emit offset sync on first update");
        assertTrue(partitionState.shouldSyncOffsets, "should sync offsets");
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
        assertTrue(partitionState.update(2, 102), "upstream offset skipped -> resync");
        partitionState.reset();
        assertFalse(partitionState.update(3, 152), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(4, 153), "one past target offset");
        partitionState.reset();
        assertFalse(partitionState.update(5, 154), "no sync");
        partitionState.reset();
        assertFalse(partitionState.update(6, 203), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(7, 204), "one past target offset");
        partitionState.reset();
        assertTrue(partitionState.update(2, 206), "upstream reset");
        partitionState.reset();
        assertFalse(partitionState.update(3, 207), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(4, 3), "downstream reset");
        partitionState.reset();
        assertFalse(partitionState.update(5, 4), "no sync");
        assertTrue(partitionState.update(7, 6), "sync");
        assertTrue(partitionState.update(7, 6), "sync");
        assertTrue(partitionState.update(8, 7), "sync");
        assertTrue(partitionState.update(10, 57), "sync");
        partitionState.reset();
        assertFalse(partitionState.update(11, 58), "sync");
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
    }

    @Test
    public void testZeroOffsetSync() {
        MirrorSourceTask.PartitionState partitionState = new MirrorSourceTask.PartitionState(0);

        // if max offset lag is zero, should always emit offset syncs
        assertTrue(partitionState.update(0, 100), "zeroOffsetSync downStreamOffset 100 is incorrect");
        assertTrue(partitionState.shouldSyncOffsets, "should sync offsets");
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
        assertTrue(partitionState.update(2, 102), "zeroOffsetSync downStreamOffset 102 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(3, 153), "zeroOffsetSync downStreamOffset 153 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(4, 154), "zeroOffsetSync downStreamOffset 154 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(5, 155), "zeroOffsetSync downStreamOffset 155 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(6, 207), "zeroOffsetSync downStreamOffset 207 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(2, 208), "zeroOffsetSync downStreamOffset 208 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(3, 209), "zeroOffsetSync downStreamOffset 209 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(4, 3), "zeroOffsetSync downStreamOffset 3 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(5, 4), "zeroOffsetSync downStreamOffset 4 is incorrect");
        assertTrue(partitionState.update(7, 6), "zeroOffsetSync downStreamOffset 6 is incorrect");
        assertTrue(partitionState.update(7, 6), "zeroOffsetSync downStreamOffset 6 is incorrect");
        assertTrue(partitionState.update(8, 7), "zeroOffsetSync downStreamOffset 7 is incorrect");
        assertTrue(partitionState.update(10, 57), "zeroOffsetSync downStreamOffset 57 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(11, 58), "zeroOffsetSync downStreamOffset 58 is incorrect");
    }

    @Test
    public void testPoll() {
        // Create a consumer mock
        byte[] key1 = "abc".getBytes();
        byte[] value1 = "fgh".getBytes();
        byte[] key2 = "123".getBytes();
        byte[] value2 = "456".getBytes();
        List<ConsumerRecord<byte[], byte[]>> consumerRecordsList =  new ArrayList<>();
        String topicName = "test";
        String headerKey = "key";
        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader(headerKey, "value".getBytes()),
        });
        consumerRecordsList.add(new ConsumerRecord<>(topicName, 0, 0, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key1.length, value1.length, key1, value1, headers, Optional.empty()));
        consumerRecordsList.add(new ConsumerRecord<>(topicName, 1, 1, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key2.length, value2.length, key2, value2, headers, Optional.empty()));
        ConsumerRecords<byte[], byte[]> consumerRecords =
                new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(topicName, 0), consumerRecordsList));

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        when(consumer.poll(any())).thenReturn(consumerRecords);

        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);

        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, 50, producer, null, null, null);
        List<SourceRecord> sourceRecords = mirrorSourceTask.poll();

        assertEquals(2, sourceRecords.size());
        for (int i = 0; i < sourceRecords.size(); i++) {
            SourceRecord sourceRecord = sourceRecords.get(i);
            ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecordsList.get(i);
            assertEquals(consumerRecord.key(), sourceRecord.key(),
                    "consumerRecord key does not equal sourceRecord key");
            assertEquals(consumerRecord.value(), sourceRecord.value(),
                    "consumerRecord value does not equal sourceRecord value");
            // We expect that the topicname will be based on the replication policy currently used
            assertEquals(replicationPolicy.formatRemoteTopic(sourceClusterName, topicName),
                    sourceRecord.topic(), "topicName not the same as the current replicationPolicy");
            // We expect that MirrorMaker will keep the same partition assignment
            assertEquals(consumerRecord.partition(), sourceRecord.kafkaPartition().intValue(),
                    "partition assignment not the same as the current replicationPolicy");
            // Check header values
            List<Header> expectedHeaders = new ArrayList<>();
            consumerRecord.headers().forEach(expectedHeaders::add);
            List<org.apache.kafka.connect.header.Header> taskHeaders = new ArrayList<>();
            sourceRecord.headers().forEach(taskHeaders::add);
            compareHeaders(expectedHeaders, taskHeaders);
        }
    }


    @Test
    public void testRecordAge() {
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);

        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);
        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask task =
                new MirrorSourceTask(
                        consumer,
                        metrics,
                        sourceClusterName,
                        replicationPolicy,
                        50,
                        null,
                        new Semaphore(1),
                        new HashMap<>(),
                        "offsetSyncsTopic");

        TopicPartition partition = new TopicPartition("topicName", 0);

        int recordCount = 10;
        when(consumer.poll(any())).thenReturn(createMockConsumerRecords(partition, recordCount));

        task.poll();

        ArgumentCaptor<Long> recordAgeCaptor = ArgumentCaptor.forClass(Long.class);
        TopicPartition convertedPartition =
                new TopicPartition(
                        replicationPolicy.formatRemoteTopic(sourceClusterName, partition.topic()),
                        partition.partition());

        verify(metrics, times(recordCount)).recordAge(eq(convertedPartition), recordAgeCaptor.capture());

        List<Long> listOfRecordAges = recordAgeCaptor.getAllValues();
        assertEquals(recordCount, listOfRecordAges.size());
        listOfRecordAges.forEach(recordAge -> assertTrue(recordAge >= 0));
    }

    @Test
    public void testRecordBytes() {
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);

        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);
        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask task =
                new MirrorSourceTask(
                        consumer,
                        metrics,
                        sourceClusterName,
                        replicationPolicy,
                        50,
                        null,
                        new Semaphore(1),
                        new HashMap<>(),
                        "offsetSyncsTopic");

        TopicPartition partition = new TopicPartition("topicName", 0);

        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                getRecordOfBytes(partition, 4),
                getRecordOfBytes(partition, 1),
                getRecordOfBytes(partition, 0)
        );
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Collections.singletonMap(partition, records)));

        task.poll();

        ArgumentCaptor<Long> recordBytesCaptor = ArgumentCaptor.forClass(Long.class);
        TopicPartition convertedPartition =
                new TopicPartition(
                        replicationPolicy.formatRemoteTopic(sourceClusterName, partition.topic()),
                        partition.partition());

        int recordCount = records.size();
        verify(metrics, times(recordCount)).recordBytes(eq(convertedPartition), recordBytesCaptor.capture());

        List<Long> listOfRecordBytes = recordBytesCaptor.getAllValues();
        assertEquals(recordCount, listOfRecordBytes.size());
        assertTrue(listOfRecordBytes.contains(4L));
        assertTrue(listOfRecordBytes.contains(1L));
        assertTrue(listOfRecordBytes.contains(0L));
    }

    @Test
    public void testRecordLatency() {
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);
        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        @SuppressWarnings("unchecked")
        KafkaProducer<byte[], byte[]> offsetProducer = mock(KafkaProducer.class);
        MirrorSourceTask task =
                new MirrorSourceTask(
                        consumer,
                        metrics,
                        sourceClusterName,
                        replicationPolicy,
                        50,
                        offsetProducer,
                        new Semaphore(1),
                        new HashMap<>(),
                        "offsetSyncsTopic");

        TopicPartition partition = new TopicPartition("topicName", 0);

        int recordCount = 10;
        ConsumerRecords<byte[], byte[]> consumerRecords = createMockConsumerRecords(partition, recordCount);
        when(consumer.poll(any())).thenReturn(consumerRecords);

        TopicPartition convertedPartition =
                new TopicPartition(
                        replicationPolicy.formatRemoteTopic(sourceClusterName, partition.topic()),
                        partition.partition());

        ArgumentCaptor<Long> recordLatencyCaptor = ArgumentCaptor.forClass(Long.class);
        consumerRecords.forEach(record -> {
            SourceRecord convertedRecord = task.convertRecord(record);
            long currentTimestamp = System.currentTimeMillis();
            task.commitRecord(convertedRecord, dummyRecordMetadata());
            verify(metrics, times(1)).replicationLatency(eq(convertedPartition), recordLatencyCaptor.capture());
            clearInvocations(metrics);
            long recordLatency = recordLatencyCaptor.getValue();
            // mock records are created with no timestamp, which defaults the timestamp to -1
            // we verify that the calculated record latency is at least as large as the timestamp at the time of commit, minus -1
            assertTrue(recordLatency >= currentTimestamp + 1L);
        });

        List<Long> listOfRecordLatencies = recordLatencyCaptor.getAllValues();
        assertEquals(recordCount, listOfRecordLatencies.size());
    }

    @Test
    public void testPositiveReplicationOffsetLag() {
        @SuppressWarnings("unchecked") KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked") KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);

        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);
        MirrorSourceTask task = new MirrorSourceTask(
                consumer, metrics, "primary", new DefaultReplicationPolicy(), 50,
                producer, new Semaphore(1), new HashMap<>(), "offsetSyncsTopic");

        TopicPartition partition = new TopicPartition("topicOne", 0);

        int recordCount = 10;
        when(consumer.poll(any())).thenReturn(createMockConsumerRecords(partition, recordCount));
        when(producer.send(any())).thenReturn(null);

        List<SourceRecord> sourceRecords = task.poll();
        long lag = 5;
        sourceRecords.forEach(r -> {
            r = addLagToRecord(r, lag);
            task.commitRecord(r, dummyRecordMetadata());
        });
        task.poll();

        // at first poll, no records are committed - we expect full lag, equal to the number of records
        verify(metrics, times(1)).replicationOffsetLag(partition, recordCount);

        //at second poll, we expect full lag, equal to the number we added to each source offset
        verify(metrics, times(1)).replicationOffsetLag(partition, lag + 1); // +1

    }

    @Test
    public void testNegativeReplicationOffsetLag() {
        @SuppressWarnings("unchecked") KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked") KafkaProducer<byte[], byte[]> offsetSyncProducer = mock(KafkaProducer.class);

        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);
        MirrorSourceTask task = new MirrorSourceTask(
                consumer, metrics, "primary", new DefaultReplicationPolicy(), 50,
                offsetSyncProducer, new Semaphore(1), new HashMap<>(), "offsetSyncsTopic");


        TopicPartition partition = new TopicPartition("topicOne", 0);

        int recordCount = 10;
        when(consumer.poll(any())).thenReturn(createMockConsumerRecords(partition, recordCount));
        when(offsetSyncProducer.send(any())).thenReturn(null);

        List<SourceRecord> sourceRecords = task.poll();
        long lag = recordCount + 1;
        sourceRecords.forEach(r -> {
            r = addLagToRecord(r, lag);
            task.commitRecord(r, dummyRecordMetadata());
        });
        task.poll();

        // at first poll, no records are committed - we expect full lag, equal to the number of records
        verify(metrics, times(1)).replicationOffsetLag(partition, recordCount);

        //at second poll, we expect the metric reporting to be skipped since the reported lag is negative
        verify(metrics, times(1)).replicationOffsetLag(any(), anyLong());
    }

    private RecordMetadata dummyRecordMetadata() {
        return new RecordMetadata(
                new TopicPartition("topic", 0),
                0,
                0, 0, 0, 0);
    }

    private SourceRecord addLagToRecord(SourceRecord record, long lag) {
        long offset = MirrorUtils.unwrapOffset(record.sourceOffset());
        Map<String, Object> offsetPlusLag = MirrorUtils.wrapOffset(offset - lag);

        return new SourceRecord(
                record.sourcePartition(),
                offsetPlusLag,
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp());
    }

    private <K, V> ConsumerRecords<K, V> createMockConsumerRecords(TopicPartition topicPartition, int count) {
        List<ConsumerRecord<K, V>> consumerRecords = new ArrayList<>();
        IntStream.range(0, count).forEach(i -> {
            ConsumerRecord<K, V> record = new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, null, null);
            consumerRecords.add(record);
        });
        return new ConsumerRecords<>(Collections.singletonMap(topicPartition, consumerRecords));
    }

    @Test
    public void testCommitRecordWithNullMetadata() {
        // Create a consumer mock
        byte[] key1 = "abc".getBytes();
        byte[] value1 = "fgh".getBytes();
        String topicName = "test";
        String headerKey = "key";
        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader(headerKey, "value".getBytes()),
        });

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);

        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, 50, producer, null, null, null);

        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, 0, 0, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key1.length, value1.length, key1, value1, headers, Optional.empty()));

        // Expect that commitRecord will not throw an exception
        mirrorSourceTask.commitRecord(sourceRecord, null);
        verifyNoInteractions(producer);
    }

    @Test
    public void testSendSyncEvent() {
        byte[] recordKey = "key".getBytes();
        byte[] recordValue = "value".getBytes();
        int maxOffsetLag = 50;
        int recordPartition = 0;
        int recordOffset = 0;
        int metadataOffset = 100;
        String topicName = "topic";
        String sourceClusterName = "sourceCluster";

        RecordHeaders headers = new RecordHeaders();
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);
        Semaphore outstandingOffsetSyncs = new Semaphore(1);
        PartitionState partitionState = new PartitionState(maxOffsetLag);
        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, maxOffsetLag, producer, outstandingOffsetSyncs, partitionStates, topicName);

        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, recordPartition,
                recordOffset, System.currentTimeMillis(), TimestampType.CREATE_TIME, recordKey.length,
                recordValue.length, recordKey, recordValue, headers, Optional.empty()));

        TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(sourceRecord.sourcePartition());
        partitionStates.put(sourceTopicPartition, partitionState);
        RecordMetadata recordMetadata = new RecordMetadata(sourceTopicPartition, metadataOffset, 0, 0, 0, recordPartition);

        ArgumentCaptor<Callback> producerCallback = ArgumentCaptor.forClass(Callback.class);
        when(producer.send(any(), producerCallback.capture())).thenAnswer(mockInvocation -> {
            producerCallback.getValue().onCompletion(null, null);
            return null;
        });

        mirrorSourceTask.commitRecord(sourceRecord, recordMetadata);
        // We should have dispatched this sync to the producer
        verify(producer, times(1)).send(any(), any());

        mirrorSourceTask.commit();
        // No more syncs should take place; we've been able to publish all of them so far
        verify(producer, times(1)).send(any(), any());

        recordOffset = 2;
        metadataOffset = 102;
        recordMetadata = new RecordMetadata(sourceTopicPartition, metadataOffset, 0, 0, 0, recordPartition);
        sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, recordPartition,
                recordOffset, System.currentTimeMillis(), TimestampType.CREATE_TIME, recordKey.length,
                recordValue.length, recordKey, recordValue, headers, Optional.empty()));

        // Do not release outstanding sync semaphore
        doReturn(null).when(producer).send(any(), producerCallback.capture());

        mirrorSourceTask.commitRecord(sourceRecord, recordMetadata);
        // We should have dispatched this sync to the producer
        verify(producer, times(2)).send(any(), any());

        mirrorSourceTask.commit();
        // No more syncs should take place; we've been able to publish all of them so far
        verify(producer, times(2)).send(any(), any());

        // Do not send sync event
        recordOffset = 4;
        metadataOffset = 104;
        recordMetadata = new RecordMetadata(sourceTopicPartition, metadataOffset, 0, 0, 0, recordPartition);
        sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, recordPartition,
                recordOffset, System.currentTimeMillis(), TimestampType.CREATE_TIME, recordKey.length,
                recordValue.length, recordKey, recordValue, headers, Optional.empty()));

        mirrorSourceTask.commitRecord(sourceRecord, recordMetadata);
        mirrorSourceTask.commit();

        // We should not have dispatched any more syncs to the producer; there were too many already in flight
        verify(producer, times(2)).send(any(), any());

        // Now the in-flight sync has been ack'd
        producerCallback.getValue().onCompletion(null, null);
        mirrorSourceTask.commit();
        // We should dispatch the offset sync that was queued but previously not sent to the producer now
        verify(producer, times(3)).send(any(), any());

        // Ack the latest sync immediately
        producerCallback.getValue().onCompletion(null, null);

        // Should send sync event
        recordOffset = 6;
        metadataOffset = 106;
        recordMetadata = new RecordMetadata(sourceTopicPartition, metadataOffset, 0, 0, 0, recordPartition);
        sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, recordPartition,
                recordOffset, System.currentTimeMillis(), TimestampType.CREATE_TIME, recordKey.length,
                recordValue.length, recordKey, recordValue, headers, Optional.empty()));

        mirrorSourceTask.commitRecord(sourceRecord, recordMetadata);
        // We should have dispatched this sync to the producer
        verify(producer, times(4)).send(any(), any());

        mirrorSourceTask.commit();
        // No more syncs should take place; we've been able to publish all of them so far
        verify(producer, times(4)).send(any(), any());
    }

    private void compareHeaders(List<Header> expectedHeaders, List<org.apache.kafka.connect.header.Header> taskHeaders) {
        assertEquals(expectedHeaders.size(), taskHeaders.size());
        for (int i = 0; i < expectedHeaders.size(); i++) {
            Header expectedHeader = expectedHeaders.get(i);
            org.apache.kafka.connect.header.Header taskHeader = taskHeaders.get(i);
            assertEquals(expectedHeader.key(), taskHeader.key(),
                    "taskHeader's key expected to equal " + taskHeader.key());
            assertEquals(expectedHeader.value(), taskHeader.value(),
                    "taskHeader's value expected to equal " + taskHeader.value().toString());
        }
    }

}
