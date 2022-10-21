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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MockConsumerTest {
    
    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    @Test
    public void testSimpleMock() {
        consumer.subscribe(Collections.singleton("test"));
        assertEquals(0, consumer.poll(Duration.ZERO).count());
        consumer.rebalance(Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1)));
        // Mock consumers need to seek manually since they cannot automatically reset offsets
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("test", 0), 0L);
        beginningOffsets.put(new TopicPartition("test", 1), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
        consumer.seek(new TopicPartition("test", 0), 0);
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0, 0, 0L, TimestampType.CREATE_TIME,
            0, 0, "key1", "value1", new RecordHeaders(), Optional.empty());
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0, 1, 0L, TimestampType.CREATE_TIME,
            0, 0, "key2", "value2", new RecordHeaders(), Optional.empty());
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
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
        // Mock consumers need to seek manually since they cannot automatically reset offsets
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("test", 0), 0L);
        beginningOffsets.put(new TopicPartition("test", 1), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
        consumer.seek(new TopicPartition("test", 0), 0);
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0, 0, 0L, TimestampType.CREATE_TIME,
            0, 0, "key1", "value1", new RecordHeaders(), Optional.empty());
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0, 1, 0L, TimestampType.CREATE_TIME,
            0, 0, "key2", "value2", new RecordHeaders(), Optional.empty());
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        ConsumerRecords<String, String> recs = consumer.poll(1);
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
        assertFalse(iter.hasNext());
        final TopicPartition tp = new TopicPartition("test", 0);
        assertEquals(2L, consumer.position(tp));
        consumer.commitSync();
        assertEquals(2L, consumer.committed(Collections.singleton(tp)).get(tp).offset());
        assertEquals(new ConsumerGroupMetadata("dummy.group.id", 1, "1", Optional.empty()),
            consumer.groupMetadata());
    }

    @Test
    public void testConsumerRecordsIsEmptyWhenReturningNoRecords() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.addRecord(new ConsumerRecord<>("test", 0, 0, null, null));
        consumer.updateEndOffsets(Collections.singletonMap(partition, 1L));
        consumer.seekToEnd(Collections.singleton(partition));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        assertEquals(0, records.count());
        assertTrue(records.isEmpty());
    }

    @Test
    public void shouldNotClearRecordsForPausedPartitions() {
        TopicPartition partition0 = new TopicPartition("test", 0);
        Collection<TopicPartition> testPartitionList = Collections.singletonList(partition0);
        consumer.assign(testPartitionList);
        consumer.addRecord(new ConsumerRecord<>("test", 0, 0, null, null));
        consumer.updateBeginningOffsets(Collections.singletonMap(partition0, 0L));
        consumer.seekToBeginning(testPartitionList);

        consumer.pause(testPartitionList);
        consumer.poll(Duration.ofMillis(1));
        consumer.resume(testPartitionList);
        ConsumerRecords<String, String> recordsSecondPoll = consumer.poll(Duration.ofMillis(1));
        assertEquals(1, recordsSecondPoll.count());
    }

    @Test
    public void endOffsetsShouldBeIdempotent() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.updateEndOffsets(Collections.singletonMap(partition, 10L));
        // consumer.endOffsets should NOT change the value of end offsets
        assertEquals(10L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        assertEquals(10L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        assertEquals(10L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        consumer.updateEndOffsets(Collections.singletonMap(partition, 11L));
        // consumer.endOffsets should NOT change the value of end offsets
        assertEquals(11L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        assertEquals(11L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        assertEquals(11L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
    }

    @Test
    public void testNextOffsetAfterSeekMiddle() {
        // this test serves primarily as documentation of the expected seek behaviour in the middle of the queue

        //set up
        TopicPartition partition = new TopicPartition("test", 0);
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(Collections.singleton(partition));
        consumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(partition, 10L));

        // pre-populate consumer with 10 records
        for (int i = 0; i < 10; i++) {
            consumer.addRecord(new ConsumerRecord<>("test", 0, i, "key" + i, "value" + i));
        }

        // assert consumer is by default at the beginning when polled
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(0, cr.offset());
            break;
        }

        // assert seek returns the offset specified when seeking to an arbitrary middle offset
        consumer.seek(partition, 5);
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(5, cr.offset());
            break;
        }

        //add some non-sequential records
        for (int i = 10; i < 20; i += 2) {
            consumer.addRecord(new ConsumerRecord<>("test", 0, i, "key" + i, "value" + i));
        }

        // assert seek returns the next greater offset if the offset specified is unavailable
        consumer.seek(partition, 15);
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(16, cr.offset());
            break;
        }
    }

    @Test
    public void testNextOffsetAfterSeekBeginning() {
        // this test serves primarily as documentation of the expected seek behaviour at the beginning of the queue

        //set up
        TopicPartition partition = new TopicPartition("test", 0);
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(Collections.singleton(partition));
        consumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(partition, 10L));

        // pre-populate consumer with 10 records
        for (int i = 0; i < 10; i++) {
            consumer.addRecord(new ConsumerRecord<>("test", 0, i, "key" + i, "value" + i));
        }

        // assert consumer is by default at the beginning when polled
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(0, cr.offset());
            break;
        }

        // assert seek 0 returns first record
        consumer.seek(partition, 0);
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(0, cr.offset());
            break;
        }

        //assert seek to beginning is equivalent to seek 0
        consumer.seekToBeginning(Collections.singleton(partition));
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(0, cr.offset());
            break;
        }

        //assert seek to beginning returns lowest offset when 0 is unavailable
        TopicPartition partition2 = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.updateBeginningOffsets(Collections.singletonMap(partition2, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(partition2, 10L));
        for (int i = 4; i < 10; i++) {
            consumer.addRecord(new ConsumerRecord<>("test", 0, i, "key" + i, "value" + i));
        }

        // assert seek 0 returns earliest known record
        consumer.seek(partition2, 0);
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(4, cr.offset());
            break;
        }

        //assert seek to beginning retrieves earliest known offset
        consumer.seekToBeginning(Collections.singleton(partition2));
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(4, cr.offset());
            break;
        }
    }

    @Test
    public void testNextOffsetAfterSeekEnd() {
        // this test serves primarily as documentation of the expected seek behaviour at the end of the queue

        //set up
        TopicPartition partition = new TopicPartition("test", 0);
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(Collections.singleton(partition));
        consumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(partition, 10L));

        // pre-populate consumer with 10 records
        for (int i = 0; i < 10; i++) {
            consumer.addRecord(new ConsumerRecord<>("test", 0, i, "key" + i, "value" + i));
        }

        //assert seek to first nonexistent record returns no records at that position yet
        consumer.seek(partition, 10);
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(0, cr.offset());
            break;
        }

        //assert seek to end returns no records, same as first nonexistent record
        consumer.seekToEnd(Collections.singleton(partition));
        assertEquals(0, consumer.poll(Duration.ofNanos(1)).count());

        //seek past end, assert no records present
        consumer.seek(partition, 11);
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(0, cr.offset());
            break;
        }

        //add some records, ensure poll skips records before position we specified in last seek
        for (int i = 10; i < 14; i++) {
            consumer.addRecord(new ConsumerRecord<>("test", 0, i, "key" + i, "value" + i));
        }
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(11, cr.offset());
            break;
        }

        //assert seek to end returns no records, and returns next record when added, even if not sequential
        //at this point, last known offset is 13, 15 is added next
        consumer.seekToEnd(Collections.singleton(partition));
        assertEquals(0, consumer.poll(Duration.ofNanos(1)).count());
        for (int i = 15; i < 16; i++) {
            consumer.addRecord(new ConsumerRecord<>("test", 0, i, "key" + i, "value" + i));
        }
        for (ConsumerRecord<String, String> cr : consumer.poll(Duration.ofNanos(1))) {
            assertEquals(15, cr.offset());
            break;
        }
    }

}
