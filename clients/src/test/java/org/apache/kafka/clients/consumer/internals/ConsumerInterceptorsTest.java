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
package org.apache.kafka.clients.consumer.internals;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerInterceptorsTest {
    private final int filterPartition1 = 5;
    private final int filterPartition2 = 6;
    private final String topic = "test";
    private final int partition = 1;
    private final TopicPartition tp = new TopicPartition(topic, partition);
    private final TopicPartition filterTopicPart1 = new TopicPartition("test5", filterPartition1);
    private final TopicPartition filterTopicPart2 = new TopicPartition("test6", filterPartition2);
    private final ConsumerRecord<Integer, Integer> consumerRecord = new ConsumerRecord<>(topic, partition, 0, 0L,
        TimestampType.CREATE_TIME, 0, 0, 1, 1, new RecordHeaders(), Optional.empty());
    private int onCommitCount = 0;
    private int onConsumeCount = 0;

    /**
     * Test consumer interceptor that filters records in onConsume() intercept
     */
    private class FilterConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {
        private int filterPartition;
        private boolean throwExceptionOnConsume = false;
        private boolean throwExceptionOnCommit = false;

        FilterConsumerInterceptor(int filterPartition) {
            this.filterPartition = filterPartition;
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
            onConsumeCount++;
            if (throwExceptionOnConsume)
                throw new KafkaException("Injected exception in FilterConsumerInterceptor.onConsume.");

            // filters out topic/partitions with partition == FILTER_PARTITION
            Map<TopicPartition, List<ConsumerRecord<K, V>>> recordMap = new HashMap<>();
            for (TopicPartition tp : records.partitions()) {
                if (tp.partition() != filterPartition)
                    recordMap.put(tp, records.records(tp));
            }
            return new ConsumerRecords<K, V>(recordMap);
        }

        @Override
        public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            onCommitCount++;
            if (throwExceptionOnCommit)
                throw new KafkaException("Injected exception in FilterConsumerInterceptor.onCommit.");
        }

        @Override
        public void close() {
        }

        // if 'on' is true, onConsume will always throw an exception
        public void injectOnConsumeError(boolean on) {
            throwExceptionOnConsume = on;
        }

        // if 'on' is true, onConsume will always throw an exception
        public void injectOnCommitError(boolean on) {
            throwExceptionOnCommit = on;
        }
    }

    @Test
    public void testOnConsumeChain() {
        List<ConsumerInterceptor<Integer, Integer>>  interceptorList = new ArrayList<>();
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaConsumer, but ok for testing interceptor callbacks
        FilterConsumerInterceptor<Integer, Integer> interceptor1 = new FilterConsumerInterceptor<>(filterPartition1);
        FilterConsumerInterceptor<Integer, Integer> interceptor2 = new FilterConsumerInterceptor<>(filterPartition2);
        interceptorList.add(interceptor1);
        interceptorList.add(interceptor2);
        ConsumerInterceptors<Integer, Integer> interceptors = new ConsumerInterceptors<>(interceptorList);

        // verify that onConsumer modifies ConsumerRecords
        Map<TopicPartition, List<ConsumerRecord<Integer, Integer>>> records = new HashMap<>();
        List<ConsumerRecord<Integer, Integer>> list1 = new ArrayList<>();
        list1.add(consumerRecord);
        List<ConsumerRecord<Integer, Integer>> list2 = new ArrayList<>();
        list2.add(new ConsumerRecord<>(filterTopicPart1.topic(), filterTopicPart1.partition(), 0, 0L,
            TimestampType.CREATE_TIME, 0, 0, 1, 1, new RecordHeaders(), Optional.empty()));
        List<ConsumerRecord<Integer, Integer>> list3 = new ArrayList<>();
        list3.add(new ConsumerRecord<>(filterTopicPart2.topic(), filterTopicPart2.partition(), 0, 0L, TimestampType.CREATE_TIME,
            0, 0, 1, 1, new RecordHeaders(), Optional.empty()));
        records.put(tp, list1);
        records.put(filterTopicPart1, list2);
        records.put(filterTopicPart2, list3);
        ConsumerRecords<Integer, Integer> consumerRecords = new ConsumerRecords<>(records);
        ConsumerRecords<Integer, Integer> interceptedRecords = interceptors.onConsume(consumerRecords);
        assertEquals(1, interceptedRecords.count());
        assertTrue(interceptedRecords.partitions().contains(tp));
        assertFalse(interceptedRecords.partitions().contains(filterTopicPart1));
        assertFalse(interceptedRecords.partitions().contains(filterTopicPart2));
        assertEquals(2, onConsumeCount);

        // verify that even if one of the intermediate interceptors throws an exception, all interceptors' onConsume are called
        interceptor1.injectOnConsumeError(true);
        ConsumerRecords<Integer, Integer> partInterceptedRecs = interceptors.onConsume(consumerRecords);
        assertEquals(2, partInterceptedRecs.count());
        assertTrue(partInterceptedRecs.partitions().contains(filterTopicPart1));  // since interceptor1 threw exception
        assertFalse(partInterceptedRecs.partitions().contains(filterTopicPart2)); // interceptor2 should still be called
        assertEquals(4, onConsumeCount);

        // if all interceptors throw an exception, records should be unmodified
        interceptor2.injectOnConsumeError(true);
        ConsumerRecords<Integer, Integer> noneInterceptedRecs = interceptors.onConsume(consumerRecords);
        assertEquals(noneInterceptedRecs, consumerRecords);
        assertEquals(3, noneInterceptedRecs.count());
        assertEquals(6, onConsumeCount);

        interceptors.close();
    }

    @Test
    public void testOnCommitChain() {
        List<ConsumerInterceptor<Integer, Integer>> interceptorList = new ArrayList<>();
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaConsumer, but ok for testing interceptor callbacks
        FilterConsumerInterceptor<Integer, Integer> interceptor1 = new FilterConsumerInterceptor<>(filterPartition1);
        FilterConsumerInterceptor<Integer, Integer> interceptor2 = new FilterConsumerInterceptor<>(filterPartition2);
        interceptorList.add(interceptor1);
        interceptorList.add(interceptor2);
        ConsumerInterceptors<Integer, Integer> interceptors = new ConsumerInterceptors<>(interceptorList);

        // verify that onCommit is called for all interceptors in the chain
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(0));
        interceptors.onCommit(offsets);
        assertEquals(2, onCommitCount);

        // verify that even if one of the interceptors throws an exception, all interceptors' onCommit are called
        interceptor1.injectOnCommitError(true);
        interceptors.onCommit(offsets);
        assertEquals(4, onCommitCount);

        interceptors.close();
    }
}
