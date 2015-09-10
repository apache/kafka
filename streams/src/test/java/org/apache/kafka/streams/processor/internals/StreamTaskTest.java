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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.test.MockSourceNode;
import org.junit.Test;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamTaskTest {

    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();
    private final Serializer<byte[]> bytesSerializer = new ByteArraySerializer();

    private final TopicPartition partition1 = new TopicPartition("topic1", 1);
    private final TopicPartition partition2 = new TopicPartition("topic2", 1);
    private final HashSet<TopicPartition> partitions = new HashSet<>(Arrays.asList(partition1, partition2));

    private final MockSourceNode source1 = new MockSourceNode<>(intDeserializer, intDeserializer);
    private final MockSourceNode source2 = new MockSourceNode<>(intDeserializer, intDeserializer);
    private final ProcessorTopology topology = new ProcessorTopology(
        Arrays.asList((ProcessorNode) source1, (ProcessorNode) source2),
        new HashMap<String, SourceNode>() {
            {
                put("topic1", source1);
                put("topic2", source2);
            }
        },
        Collections.<String, SinkNode>emptyMap()
    );

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

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockProducer<byte[], byte[]> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer);
    private final StreamTask task = new StreamTask(0, consumer, producer, partitions, topology, config);

    @Before
    public void setup() {
        consumer.assign(Arrays.asList(partition1, partition2));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessOrder() {
        byte[] recordValue = intSerializer.serialize(null, 10);

        task.addRecords(partition1, records(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 1, intSerializer.serialize(partition1.topic(), 10), recordValue),
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 2, intSerializer.serialize(partition1.topic(), 20), recordValue),
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 3, intSerializer.serialize(partition1.topic(), 30), recordValue)
        ));

        task.addRecords(partition2, records(
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 1, intSerializer.serialize(partition1.topic(), 25), recordValue),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 2, intSerializer.serialize(partition1.topic(), 35), recordValue),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 3, intSerializer.serialize(partition1.topic(), 45), recordValue)
        ));

        assertEquals(task.process(), 5);
        assertEquals(source1.numReceived, 1);
        assertEquals(source2.numReceived, 0);

        assertEquals(task.process(), 4);
        assertEquals(source1.numReceived, 1);
        assertEquals(source2.numReceived, 1);

        assertEquals(task.process(), 3);
        assertEquals(source1.numReceived, 2);
        assertEquals(source2.numReceived, 1);

        assertEquals(task.process(), 2);
        assertEquals(source1.numReceived, 3);
        assertEquals(source2.numReceived, 1);

        assertEquals(task.process(), 1);
        assertEquals(source1.numReceived, 3);
        assertEquals(source2.numReceived, 2);

        assertEquals(task.process(), 0);
        assertEquals(source1.numReceived, 3);
        assertEquals(source2.numReceived, 3);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPauseResume() {
        byte[] recordValue = intSerializer.serialize(null, 10);

        task.addRecords(partition1, records(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 1, intSerializer.serialize(partition1.topic(), 10), recordValue),
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 2, intSerializer.serialize(partition1.topic(), 20), recordValue)
        ));

        task.addRecords(partition2, records(
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 3, intSerializer.serialize(partition1.topic(), 35), recordValue),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 4, intSerializer.serialize(partition1.topic(), 45), recordValue),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 5, intSerializer.serialize(partition1.topic(), 55), recordValue),
            new ConsumerRecord<>(partition2.topic(), partition2.partition(), 6, intSerializer.serialize(partition1.topic(), 65), recordValue)
        ));

        assertEquals(task.process(), 5);
        assertEquals(source1.numReceived, 1);
        assertEquals(source2.numReceived, 0);

        assertEquals(consumer.paused().size(), 1);
        assertTrue(consumer.paused().contains(partition2));

        task.addRecords(partition1, records(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 3, intSerializer.serialize(partition1.topic(), 30), recordValue),
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 4, intSerializer.serialize(partition1.topic(), 40), recordValue),
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 5, intSerializer.serialize(partition1.topic(), 50), recordValue)
        ));

        assertEquals(consumer.paused().size(), 2);
        assertTrue(consumer.paused().contains(partition1));
        assertTrue(consumer.paused().contains(partition2));

        assertEquals(task.process(), 7);
        assertEquals(source1.numReceived, 1);
        assertEquals(source2.numReceived, 1);

        assertEquals(consumer.paused().size(), 1);
        assertTrue(consumer.paused().contains(partition1));

        assertEquals(task.process(), 6);
        assertEquals(source1.numReceived, 2);
        assertEquals(source2.numReceived, 1);

        assertEquals(consumer.paused().size(), 0);
    }

    private Iterable<ConsumerRecord<byte[], byte[]>> records(ConsumerRecord<byte[], byte[]>... recs) {
        return Arrays.asList(recs);
    }
}
