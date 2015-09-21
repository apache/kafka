/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamThreadTest {

    private TopicPartition t1p1 = new TopicPartition("topic1", 1);
    private TopicPartition t1p2 = new TopicPartition("topic1", 2);
    private TopicPartition t2p1 = new TopicPartition("topic2", 1);
    private TopicPartition t2p2 = new TopicPartition("topic2", 2);

    private final StreamingConfig config = new StreamingConfig(new Properties() {
        {
            setProperty(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            setProperty(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            setProperty(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            setProperty(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            setProperty(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.test.MockTimestampExtractor");
            setProperty(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
            setProperty(StreamingConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
        }
    });

    private ByteArraySerializer serializer = new ByteArraySerializer();

    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionAssignmentChange() throws Exception {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");

        StreamThread thread = new StreamThread(builder, config, producer, consumer);

        ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

        assertTrue(thread.tasks().isEmpty());

        List<TopicPartition> revokedPartitions;
        List<TopicPartition> assignedPartitions;
        Set<TopicPartition> expectedGroup1;
        Set<TopicPartition> expectedGroup2;

        revokedPartitions = Collections.emptyList();
        assignedPartitions = Collections.singletonList(t1p1);
        expectedGroup1 = new HashSet<>(Arrays.asList(t1p1));

        rebalanceListener.onPartitionsRevoked(consumer, revokedPartitions);
        rebalanceListener.onPartitionsAssigned(consumer, assignedPartitions);

        assertTrue(thread.tasks().containsKey(1));
        assertEquals(expectedGroup1, thread.tasks().get(1).partitions());
        assertEquals(1, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Collections.singletonList(t1p2);
        expectedGroup2 = new HashSet<>(Arrays.asList(t1p2));

        rebalanceListener.onPartitionsRevoked(consumer, revokedPartitions);
        rebalanceListener.onPartitionsAssigned(consumer, assignedPartitions);

        assertTrue(thread.tasks().containsKey(2));
        assertEquals(expectedGroup2, thread.tasks().get(2).partitions());
        assertEquals(1, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Arrays.asList(t1p1, t1p2);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        expectedGroup2 = new HashSet<>(Collections.singleton(t1p2));

        rebalanceListener.onPartitionsRevoked(consumer, revokedPartitions);
        rebalanceListener.onPartitionsAssigned(consumer, assignedPartitions);

        assertTrue(thread.tasks().containsKey(1));
        assertTrue(thread.tasks().containsKey(2));
        assertEquals(expectedGroup1, thread.tasks().get(1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(2).partitions());
        assertEquals(2, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Arrays.asList(t1p1, t1p2, t2p1, t2p2);
        expectedGroup1 = new HashSet<>(Arrays.asList(t1p1, t2p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t1p2, t2p2));

        rebalanceListener.onPartitionsRevoked(consumer, revokedPartitions);
        rebalanceListener.onPartitionsAssigned(consumer, assignedPartitions);

        assertTrue(thread.tasks().containsKey(1));
        assertTrue(thread.tasks().containsKey(2));
        assertEquals(expectedGroup1, thread.tasks().get(1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(2).partitions());
        assertEquals(2, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Collections.emptyList();

        rebalanceListener.onPartitionsRevoked(consumer, revokedPartitions);
        rebalanceListener.onPartitionsAssigned(consumer, assignedPartitions);

        assertTrue(thread.tasks().isEmpty());
    }
}
