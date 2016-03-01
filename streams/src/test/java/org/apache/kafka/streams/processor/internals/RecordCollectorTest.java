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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by yasuhiro on 2/8/16.
 */
public class RecordCollectorTest {

    private List<PartitionInfo> infos = Arrays.asList(
            new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0])
    );

    private Cluster cluster = new Cluster(Collections.singletonList(Node.noNode()), infos, Collections.<String>emptySet());


    private final ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private final StringSerializer stringSerializer = new StringSerializer();

    private final StreamPartitioner<String, String> streamPartitioner = new StreamPartitioner<String, String>() {
        @Override
        public Integer partition(String key, String value, int numPartitions) {
            return Integer.parseInt(key) % numPartitions;
        }
    };

    @Test
    public void testSpecificPartition() {

        RecordCollector collector = new RecordCollector(
                new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
        );

        collector.send(new ProducerRecord<>("topic1", 0, "999", "0"), stringSerializer, stringSerializer);
        collector.send(new ProducerRecord<>("topic1", 0, "999", "0"), stringSerializer, stringSerializer);
        collector.send(new ProducerRecord<>("topic1", 0, "999", "0"), stringSerializer, stringSerializer);

        collector.send(new ProducerRecord<>("topic1", 1, "999", "0"), stringSerializer, stringSerializer);
        collector.send(new ProducerRecord<>("topic1", 1, "999", "0"), stringSerializer, stringSerializer);

        collector.send(new ProducerRecord<>("topic1", 2, "999", "0"), stringSerializer, stringSerializer);

        Map<TopicPartition, Long> offsets = collector.offsets();

        assertEquals((Long) 2L, offsets.get(new TopicPartition("topic1", 0)));
        assertEquals((Long) 1L, offsets.get(new TopicPartition("topic1", 1)));
        assertEquals((Long) 0L, offsets.get(new TopicPartition("topic1", 2)));

        // ignore StreamPartitioner
        collector.send(new ProducerRecord<>("topic1", 0, "999", "0"), stringSerializer, stringSerializer, streamPartitioner);
        collector.send(new ProducerRecord<>("topic1", 1, "999", "0"), stringSerializer, stringSerializer, streamPartitioner);
        collector.send(new ProducerRecord<>("topic1", 2, "999", "0"), stringSerializer, stringSerializer, streamPartitioner);

        assertEquals((Long) 3L, offsets.get(new TopicPartition("topic1", 0)));
        assertEquals((Long) 2L, offsets.get(new TopicPartition("topic1", 1)));
        assertEquals((Long) 1L, offsets.get(new TopicPartition("topic1", 2)));
    }

    @Test
    public void testStreamPartitioner() {

        RecordCollector collector = new RecordCollector(
                new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
        );

        collector.send(new ProducerRecord<>("topic1", "3", "0"), stringSerializer, stringSerializer, streamPartitioner);
        collector.send(new ProducerRecord<>("topic1", "9", "0"), stringSerializer, stringSerializer, streamPartitioner);
        collector.send(new ProducerRecord<>("topic1", "27", "0"), stringSerializer, stringSerializer, streamPartitioner);
        collector.send(new ProducerRecord<>("topic1", "81", "0"), stringSerializer, stringSerializer, streamPartitioner);
        collector.send(new ProducerRecord<>("topic1", "243", "0"), stringSerializer, stringSerializer, streamPartitioner);

        collector.send(new ProducerRecord<>("topic1", "28", "0"), stringSerializer, stringSerializer, streamPartitioner);
        collector.send(new ProducerRecord<>("topic1", "82", "0"), stringSerializer, stringSerializer, streamPartitioner);
        collector.send(new ProducerRecord<>("topic1", "244", "0"), stringSerializer, stringSerializer, streamPartitioner);

        collector.send(new ProducerRecord<>("topic1", "245", "0"), stringSerializer, stringSerializer, streamPartitioner);

        Map<TopicPartition, Long> offsets = collector.offsets();

        assertEquals((Long) 4L, offsets.get(new TopicPartition("topic1", 0)));
        assertEquals((Long) 2L, offsets.get(new TopicPartition("topic1", 1)));
        assertEquals((Long) 0L, offsets.get(new TopicPartition("topic1", 2)));
    }

}
