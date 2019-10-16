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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MockConsumerTest {
    
    private MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    @Test
    public void testSimpleMock() {
        consumer.subscribe(Collections.singleton("test"));
        assertEquals(0, consumer.poll(Duration.ZERO).count());
        consumer.rebalance(Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1)));
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key1", "value1");
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key2", "value2");
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        consumer.seekToBeginning(Collections.singleton(new TopicPartition("test", 0)));
        ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(1));
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
        assertFalse(iter.hasNext());
        final TopicPartition tp = new TopicPartition("test", 0);
        assertEquals(2L, consumer.position(tp));
        consumer.commitSync();
        assertEquals(2L, consumer.committed(Collections.singleton(tp)).get(tp).offset());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSimpleMockDeprecated() {
        consumer.subscribe(Collections.singleton("test"));
        assertEquals(0, consumer.poll(1000).count());
        consumer.rebalance(Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1)));
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key1", "value1");
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key2", "value2");
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        consumer.seekToBeginning(Collections.singleton(new TopicPartition("test", 0)));
        ConsumerRecords<String, String> recs = consumer.poll(1);
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
        assertFalse(iter.hasNext());
        final TopicPartition tp = new TopicPartition("test", 0);
        assertEquals(2L, consumer.position(tp));
        consumer.commitSync();
        assertEquals(2L, consumer.committed(Collections.singleton(tp)).get(tp).offset());
    }

    @Test
    public void testConsumerRecordsIsEmptyWhenReturningNoRecords() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.addRecord(new ConsumerRecord<>("test", 0, 0, null, null));
        consumer.seekToEnd(Collections.singleton(partition));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        assertThat(records.count(), is(0));
        assertThat(records.isEmpty(), is(true));
    }

    @Test
    public void testConsumerResetsOffsetIfOutOfRange() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 0, null, null));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 1, null, null));
        consumer.poll(Duration.ofMillis(1)); //Initialize consumer position
        consumer.truncatePartition(partition, 1L);
        consumer.seek(partition, 0L);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        assertThat(records.count(), is(1));
        ConsumerRecord<String, String> record = records.records(partition).get(0);
        assertThat(record.offset(), is(1L));
    }

    @Test
    public void testConsumerCanSeekToPreviouslyPolledMessage() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 0, null, null));
        consumer.poll(Duration.ofMillis(1)); //Skip past first message
        consumer.seek(partition, 0L);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        assertThat(records.count(), is(1));
        ConsumerRecord<String, String> record = records.records(partition).get(0);
        assertThat(record.offset(), is(0L));
    }

    @Test
    public void testConsumerCanAutoSetLogBounds() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 0, null, null));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 1, null, null));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 2, null, null));
        consumer.seekToBeginning(Collections.singleton(partition));
        ConsumerRecords<String, String> beginningRecords = consumer.poll(Duration.ofMillis(1));
        assertThat(beginningRecords.count(), is(3));
        ConsumerRecord<String, String> beginningRecord = beginningRecords.records(partition).get(0);
        assertThat(beginningRecord.offset(), is(0L));
        consumer.seekToEnd(Collections.singleton(partition));
        ConsumerRecords<String, String> endRecords = consumer.poll(Duration.ofMillis(1));
        assertThat(endRecords.count(), is(0));
        assertThat(consumer.beginningOffsets(Collections.singleton(partition)).get(partition), is(0L));
        assertThat(consumer.endOffsets(Collections.singleton(partition)).get(partition), is(3L));
    }

    @Test
    public void testConsumerCanCompactIndividualOffsets() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 0, null, null));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 1, null, null));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 2, null, null));
        consumer.seekToBeginning(Collections.singleton(partition));
        consumer.removeOffsets(partition, 1L);
        ConsumerRecords<String, String> beginningRecords = consumer.poll(Duration.ofMillis(1));
        assertThat(beginningRecords.count(), is(2));
        assertThat(beginningRecords.records(partition).get(0).offset(), is(0L));
        assertThat(beginningRecords.records(partition).get(1).offset(), is(2L));
        assertThat(consumer.beginningOffsets(Collections.singleton(partition)).get(partition), is(0L));
        assertThat(consumer.endOffsets(Collections.singleton(partition)).get(partition), is(3L));
    }

    @Test
    public void testConsumerCanTruncateLog() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 0, null, null));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 1, null, null));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 2, null, null));
        consumer.seekToBeginning(Collections.singleton(partition));
        consumer.truncatePartition(partition, 1L);
        ConsumerRecords<String, String> beginningRecords = consumer.poll(Duration.ofMillis(1));
        assertThat(beginningRecords.count(), is(2));
        assertThat(beginningRecords.records(partition).get(0).offset(), is(1L));
        assertThat(beginningRecords.records(partition).get(1).offset(), is(2L));
        consumer.beginningOffsets(Collections.singleton(partition));
        assertThat(consumer.beginningOffsets(Collections.singleton(partition)).get(partition), is(1L));
        assertThat(consumer.endOffsets(Collections.singleton(partition)).get(partition), is(3L));
    }

    @Test
    public void testConsumerRespectsMaxPollRecords() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 0, null, null));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 1, null, null));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), 2, null, null));
        consumer.seekToBeginning(Collections.singleton(partition));
        consumer.setMaxPollRecords(2);
        ConsumerRecords<String, String> firstPoll = consumer.poll(Duration.ofMillis(1));
        assertThat(firstPoll.count(), is(2));
        assertThat(firstPoll.records(partition).get(0).offset(), is(0L));
        assertThat(firstPoll.records(partition).get(1).offset(), is(1L));
        ConsumerRecords<String, String> secondPoll = consumer.poll(Duration.ofMillis(1));
        assertThat(secondPoll.count(), is(1));
        assertThat(secondPoll.records(partition).get(0).offset(), is(2L));
    }

}
