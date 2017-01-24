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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WindowedStreamPartitionerTest {

    private String topicName = "topic";

    private IntegerSerializer intSerializer = new IntegerSerializer();
    private StringSerializer stringSerializer = new StringSerializer();

    private List<PartitionInfo> infos = Arrays.asList(
            new PartitionInfo(topicName, 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 3, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 4, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 5, Node.noNode(), new Node[0], new Node[0])
    );

    private Cluster cluster = new Cluster("cluster", Collections.singletonList(Node.noNode()), infos,
            Collections.<String>emptySet(), Collections.<String>emptySet());

    @Test
    public void testCopartitioning() {

        Random rand = new Random();

        DefaultPartitioner defaultPartitioner = new DefaultPartitioner();

        WindowedSerializer<Integer> windowedSerializer = new WindowedSerializer<>(intSerializer);
        WindowedStreamPartitioner<Integer, String> streamPartitioner = new WindowedStreamPartitioner<>(windowedSerializer);

        for (int k = 0; k < 10; k++) {
            Integer key = rand.nextInt();
            byte[] keyBytes = intSerializer.serialize(topicName, key);

            String value = key.toString();
            byte[] valueBytes = stringSerializer.serialize(topicName, value);

            Integer expected = defaultPartitioner.partition("topic", key, keyBytes, value, valueBytes, cluster);

            for (int w = 1; w < 10; w++) {
                TimeWindow window = new TimeWindow(10 * w, 20 * w);

                Windowed<Integer> windowedKey = new Windowed<>(key, window);
                Integer actual = streamPartitioner.partition(windowedKey, value, infos.size());

                assertEquals(expected, actual);
            }
        }
    }

    @Test
    public void testWindowedSerializerNoArgConstructors() {
        Map<String, String> props = new HashMap<>();
        // test key[value].serializer.inner.class takes precedence over serializer.inner.class
        WindowedSerializer<StringSerializer> windowedSerializer = new WindowedSerializer<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host:1");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put("key.serializer.inner.class", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("serializer.inner.class", "org.apache.kafka.common.serialization.StringSerializer");
        windowedSerializer.configure(props, true);
        Serializer<?> inner = windowedSerializer.innerSerializer();
        assertNotNull("Inner serializer should be not null", inner);
        assertTrue("Inner serializer type should be StringSerializer", inner instanceof StringSerializer);
        // test serializer.inner.class
        props.put("serializer.inner.class", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.remove("key.serializer.inner.class");
        props.remove("value.serializer.inner.class");
        WindowedSerializer<?> windowedSerializer1 = new WindowedSerializer<>();
        windowedSerializer1.configure(props, false);
        Serializer<?> inner1 = windowedSerializer1.innerSerializer();
        assertNotNull("Inner serializer should be not null", inner1);
        assertTrue("Inner serializer type should be ByteArraySerializer", inner1 instanceof ByteArraySerializer);
    }

    @Test
    public void testWindowedDeserializerNoArgConstructors() {
        Map<String, String> props = new HashMap<>();
        // test key[value].deserializer.inner.class takes precedence over serializer.inner.class
        WindowedDeserializer<StringSerializer> windowedDeserializer = new WindowedDeserializer<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host:1");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put("key.deserializer.inner.class", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("deserializer.inner.class", "org.apache.kafka.common.serialization.StringDeserializer");
        windowedDeserializer.configure(props, true);
        Deserializer<?> inner = windowedDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be StringDeserializer", inner instanceof StringDeserializer);
        // test deserializer.inner.class
        props.put("deserializer.inner.class", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.remove("key.deserializer.inner.class");
        props.remove("value.deserializer.inner.class");
        WindowedDeserializer<?> windowedDeserializer1 = new WindowedDeserializer<>();
        windowedDeserializer1.configure(props, false);
        Deserializer<?> inner1 = windowedDeserializer1.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner1);
        assertTrue("Inner deserializer type should be ByteArrayDeserializer", inner1 instanceof ByteArrayDeserializer);
    }
}
