/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class StateConsumerTest {

    private static final long FLUSH_INTERVAL = 1000L;
    private final TopicPartition topicOne = new TopicPartition("topic-one", 1);
    private final TopicPartition topicTwo = new TopicPartition("topic-two", 1);
    private final MockTime time = new MockTime();
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final Map<TopicPartition, Long> partitionOffsets = new HashMap<>();
    private GlobalStreamThread.StateConsumer stateConsumer;
    private StateMaintainerStub stateMaintainer;

    @Before
    public void setUp() throws Exception {
        partitionOffsets.put(topicOne, 20L);
        partitionOffsets.put(topicTwo, 30L);
        stateMaintainer = new StateMaintainerStub(partitionOffsets);
        stateConsumer = new GlobalStreamThread.StateConsumer(consumer, stateMaintainer, time, 10L, FLUSH_INTERVAL);
    }

    @Test
    public void shouldAssignPartitionsToConsumer() throws Exception {
        stateConsumer.initialize();
        assertEquals(Utils.mkSet(topicOne, topicTwo), consumer.assignment());
    }

    @Test
    public void shouldSeekToInitialOffsets() throws Exception {
        stateConsumer.initialize();
        assertEquals(20L, consumer.position(topicOne));
        assertEquals(30L, consumer.position(topicTwo));
    }

    @Test
    public void shouldUpdateStateWithReceivedRecordsForPartition() throws Exception {
        stateConsumer.initialize();
        consumer.addRecord(new ConsumerRecord<>("topic-one", 1, 20L, new byte[0], new byte[0]));
        consumer.addRecord(new ConsumerRecord<>("topic-one", 1, 21L, new byte[0], new byte[0]));
        stateConsumer.pollAndUpdate();
        assertEquals(2, stateMaintainer.updatedPartitions.get(topicOne).intValue());
    }

    @Test
    public void shouldUpdateStateWithReceivedRecordsForAllTopicPartition() throws Exception {
        stateConsumer.initialize();
        consumer.addRecord(new ConsumerRecord<>("topic-one", 1, 20L, new byte[0], new byte[0]));
        consumer.addRecord(new ConsumerRecord<>("topic-two", 1, 31L, new byte[0], new byte[0]));
        consumer.addRecord(new ConsumerRecord<>("topic-two", 1, 32L, new byte[0], new byte[0]));
        stateConsumer.pollAndUpdate();
        assertEquals(1, stateMaintainer.updatedPartitions.get(topicOne).intValue());
        assertEquals(2, stateMaintainer.updatedPartitions.get(topicTwo).intValue());
    }

    @Test
    public void shouldFlushStoreWhenFlushIntervalHasLapsed() throws Exception {
        stateConsumer.initialize();
        consumer.addRecord(new ConsumerRecord<>("topic-one", 1, 20L, new byte[0], new byte[0]));
        time.sleep(FLUSH_INTERVAL);

        stateConsumer.pollAndUpdate();
        assertTrue(stateMaintainer.flushed);
    }

    @Test
    public void shouldNotFlushOffsetsWhenFlushIntervalHasNotLapsed() throws Exception {
        stateConsumer.initialize();
        consumer.addRecord(new ConsumerRecord<>("topic-one", 1, 20L, new byte[0], new byte[0]));
        time.sleep(FLUSH_INTERVAL / 2);
        stateConsumer.pollAndUpdate();
        assertFalse(stateMaintainer.flushed);
    }

    @Test
    public void shouldNotFlushWhenFlushIntervalIsZero() throws Exception {
        stateConsumer = new GlobalStreamThread.StateConsumer(consumer, stateMaintainer, time, 10L, -1);
        stateConsumer.initialize();
        time.sleep(100);
        stateConsumer.pollAndUpdate();
        assertFalse(stateMaintainer.flushed);
    }

    @Test
    public void shouldCloseConsumer() throws Exception {
        stateConsumer.close();
        assertTrue(consumer.closed());
    }

    @Test
    public void shouldCloseStateMaintainer() throws Exception {
        stateConsumer.close();
        assertTrue(stateMaintainer.closed);
    }


    private static class StateMaintainerStub implements GlobalStateMaintainer {
        private final Map<TopicPartition, Long> partitionOffsets;
        private final Map<TopicPartition, Integer> updatedPartitions = new HashMap<>();
        private boolean flushed;
        private boolean closed;

        public StateMaintainerStub(final Map<TopicPartition, Long> partitionOffsets) {
            this.partitionOffsets = partitionOffsets;
        }

        @Override
        public Map<TopicPartition, Long> initialize() {
            return partitionOffsets;
        }

        public void flushState() {
            flushed = true;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public void update(final ConsumerRecord<byte[], byte[]> record) {
            final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            if (!updatedPartitions.containsKey(tp)) {
                updatedPartitions.put(tp, 0);
            }
            updatedPartitions.put(tp, updatedPartitions.get(tp) + 1);
        }

    }

}