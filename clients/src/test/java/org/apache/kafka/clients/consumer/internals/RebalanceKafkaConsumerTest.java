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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.MockMetricsReporter;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class RebalanceKafkaConsumerTest {
    private final String topic1 = "test1";
    private final String topic2 = "test2";
    private final TopicPartition t1p = new TopicPartition(topic1, 0);
    private final TopicPartition t2p = new TopicPartition(topic2, 0);
    private final Map<TopicPartition, Long> startOffsets = new HashMap<>();
    private final Map<TopicPartition, Long> endOffsets = new HashMap<>();

    @Before
    public void setUpOffsets() {
        startOffsets.put(t1p, 0L);
        startOffsets.put(t2p, 1L);
        endOffsets.put(t1p, 4L);
        endOffsets.put(t1p, 5L);
    }

    @Test
    public void testPositionsAfterInstantiation() {
        final RebalanceKafkaConsumer<byte[], byte[]> consumer = newConsumer();
        assertTrue(consumer.position(t1p) == 0L);
        assertTrue(consumer.position(t2p) == 1L);
    }

    @Test
    public void testConsumerIfContainsSubsciption() {
        final Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(t1p, 1L);
        assertTrue(newConsumer(offsets, offsets).subscription().size() == 0);
    }

    private RebalanceKafkaConsumer<byte[], byte[]> newConsumer() {
        return newConsumer(startOffsets, endOffsets);
    }

    private RebalanceKafkaConsumer<byte[], byte[]> newConsumer(Map<TopicPartition, Long> beginningOffsets,
                                                               Map<TopicPartition, Long> finishingOffsets) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "my.consumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        return newConsumer(props, beginningOffsets, finishingOffsets);
    }

    private RebalanceKafkaConsumer<byte[], byte[]> newConsumer(Map<String, Object> props,
                                                               Map<TopicPartition, Long> beginningOffsets,
                                                               Map<TopicPartition, Long> finishingOffsets) {
        return new RebalanceKafkaConsumer<>(props,
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer(),
                beginningOffsets,
                finishingOffsets);
    }
}
