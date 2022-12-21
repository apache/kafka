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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

public class MirrorSourceTaskTest {

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
                new DefaultReplicationPolicy(), 50, producer);
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

        long upstreamOffset = 0;
        long downstreamOffset = 100;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "always emit offset sync on first update");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "always emit offset sync on first update");

        upstreamOffset = 2;
        downstreamOffset = 102;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "upstream offset skipped -> resync");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "upstream offset skipped -> resync");

        upstreamOffset = 3;
        downstreamOffset = 152;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertNotEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "no sync");
        assertNotEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "no sync");

        upstreamOffset = 4;
        downstreamOffset = 153;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertNotEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "no sync");
        assertNotEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "no sync");

        upstreamOffset = 5;
        downstreamOffset = 154;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertNotEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "no sync");
        assertNotEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "no sync");

        upstreamOffset = 6;
        downstreamOffset = 205;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "one past target offset");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "one past target offset");

        upstreamOffset = 2;
        downstreamOffset = 206;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "upstream reset");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "upstream reset");

        upstreamOffset = 3;
        downstreamOffset = 207;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertNotEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "no sync");
        assertNotEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "no sync");

        upstreamOffset = 4;
        downstreamOffset = 3;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "downstream reset");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "downstream reset");

        upstreamOffset = 5;
        downstreamOffset = 4;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertNotEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "no sync");
        assertNotEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "no sync");
    }

    @Test
    public void testZeroOffsetSync() {
        MirrorSourceTask.PartitionState partitionState = new MirrorSourceTask.PartitionState(0);

        // if max offset lag is zero, should always emit offset syncs
        long upstreamOffset = 0;
        long downstreamOffset = 100;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 100 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 100 is incorrect");

        upstreamOffset = 2;
        downstreamOffset = 102;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 102 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 102 is incorrect");

        upstreamOffset = 3;
        downstreamOffset = 153;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 153 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 153 is incorrect");

        upstreamOffset = 4;
        downstreamOffset = 154;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 154 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 154 is incorrect");

        upstreamOffset = 5;
        downstreamOffset = 155;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 155 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 155 is incorrect");

        upstreamOffset = 6;
        downstreamOffset = 207;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 207 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 207 is incorrect");

        upstreamOffset = 2;
        downstreamOffset = 208;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 208 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 208 is incorrect");

        upstreamOffset = 3;
        downstreamOffset = 209;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 209 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 209 is incorrect");

        upstreamOffset = 4;
        downstreamOffset = 3;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 3 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 3 is incorrect");

        upstreamOffset = 5;
        downstreamOffset = 4;
        partitionState.update(upstreamOffset, downstreamOffset);
        assertEquals(upstreamOffset, partitionState.lastSyncUpstreamOffset, "zeroOffsetSync downStreamOffset 4 is incorrect");
        assertEquals(downstreamOffset, partitionState.lastSyncDownstreamOffset, "zeroOffsetSync downStreamOffset 4 is incorrect");
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
                replicationPolicy, 50, producer);
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
                replicationPolicy, 50, producer);

        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, 0, 0, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key1.length, value1.length, key1, value1, headers, Optional.empty()));

        // Expect that commitRecord will not throw an exception
        mirrorSourceTask.commitRecord(sourceRecord, null);
        verifyNoInteractions(producer);
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
