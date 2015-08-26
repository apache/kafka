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

package org.apache.kafka.streaming.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streaming.processor.TimestampExtractor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streaming.processor.internals.StreamGroup;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.MockSourceNode;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamGroupTest {

    private static Serializer<Integer> serializer = new IntegerSerializer();
    private static Deserializer<Integer> deserializer = new IntegerDeserializer();

    @SuppressWarnings("unchecked")
    @Test
    public void testAddPartition() {

        MockIngestor mockIngestor = new MockIngestor();

        StreamGroup streamGroup = new StreamGroup(
            new MockProcessorContext(serializer, deserializer),
            mockIngestor,
            new TimestampExtractor() {
                public long extract(String topic, Object key, Object value) {
                    if (topic.equals("topic1"))
                        return ((Integer) key).longValue();
                    else
                        return ((Integer) key).longValue() / 10L + 5L;
                }
            },
            3
        );

        TopicPartition partition1 = new TopicPartition("topic1", 1);
        TopicPartition partition2 = new TopicPartition("topic2", 1);
        MockSourceNode<Integer, Integer> source1 = new MockSourceNode(deserializer, deserializer);
        MockSourceNode<Integer, Integer> source2 = new MockSourceNode(deserializer, deserializer);
        MockSourceNode<Integer, Integer> source3 = new MockSourceNode(deserializer, deserializer);

        streamGroup.addPartition(partition1, source1);
        mockIngestor.addPartitionStreamToGroup(streamGroup, partition1);

        streamGroup.addPartition(partition2, source2);
        mockIngestor.addPartitionStreamToGroup(streamGroup, partition2);

        Exception exception = null;
        try {
            streamGroup.addPartition(partition1, source3);
        } catch (Exception ex) {
            exception = ex;
        }
        assertTrue(exception != null);

        byte[] recordValue = serializer.serialize(null, new Integer(10));

        mockIngestor.addRecords(partition1, records(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 1, serializer.serialize(partition1.topic(), new Integer(10)), recordValue),
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 2, serializer.serialize(partition1.topic(), new Integer(20)), recordValue)
        ));

        mockIngestor.addRecords(partition2, records(
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 1, serializer.serialize(partition1.topic(), new Integer(300)), recordValue),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 2, serializer.serialize(partition1.topic(), new Integer(400)), recordValue),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 3, serializer.serialize(partition1.topic(), new Integer(500)), recordValue),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 4, serializer.serialize(partition1.topic(), new Integer(600)), recordValue)
        ));

        streamGroup.process();
        assertEquals(source1.numReceived, 1);
        assertEquals(source2.numReceived, 0);

        assertEquals(mockIngestor.paused.size(), 1);
        assertTrue(mockIngestor.paused.contains(partition2));

        mockIngestor.addRecords(partition1, records(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 3, serializer.serialize(partition1.topic(), new Integer(30)), recordValue),
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 4, serializer.serialize(partition1.topic(), new Integer(40)), recordValue),
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 5, serializer.serialize(partition1.topic(), new Integer(50)), recordValue)
        ));

        streamGroup.process();
        assertEquals(source1.numReceived, 2);
        assertEquals(source2.numReceived, 0);

        assertEquals(mockIngestor.paused.size(), 2);
        assertTrue(mockIngestor.paused.contains(partition1));
        assertTrue(mockIngestor.paused.contains(partition2));

        streamGroup.process();
        assertEquals(source1.numReceived, 3);
        assertEquals(source2.numReceived, 0);

        streamGroup.process();
        assertEquals(source1.numReceived, 3);
        assertEquals(source2.numReceived, 1);

        assertEquals(mockIngestor.paused.size(), 1);
        assertTrue(mockIngestor.paused.contains(partition2));

        streamGroup.process();
        assertEquals(source1.numReceived, 4);
        assertEquals(source2.numReceived, 1);

        assertEquals(mockIngestor.paused.size(), 1);

        streamGroup.process();
        assertEquals(source1.numReceived, 4);
        assertEquals(source2.numReceived, 2);

        assertEquals(mockIngestor.paused.size(), 0);

        streamGroup.process();
        assertEquals(source1.numReceived, 5);
        assertEquals(source2.numReceived, 2);

        streamGroup.process();
        assertEquals(source1.numReceived, 5);
        assertEquals(source2.numReceived, 3);

        streamGroup.process();
        assertEquals(source1.numReceived, 5);
        assertEquals(source2.numReceived, 4);

        assertEquals(mockIngestor.paused.size(), 0);

        streamGroup.process();
        assertEquals(source1.numReceived, 5);
        assertEquals(source2.numReceived, 4);
    }

    private Iterable<ConsumerRecord<byte[], byte[]>> records(ConsumerRecord<byte[], byte[]>... recs) {
        return Arrays.asList(recs);
    }
}
