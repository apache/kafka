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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KafkaStreamsInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KafkaStreamsInstancesTest {

    private KafkaStreamsInstances discovery;
    private HostInfo hostOne;
    private HostInfo hostTwo;
    private HostInfo hostThree;
    private TopicPartition tp1;
    private TopicPartition tp2;
    private TopicPartition tp3;
    private Map<HostInfo, Set<TopicPartition>> hostToPartitions;
    private KStreamBuilder builder;

    @Before
    public void before() {
        builder = new KStreamBuilder();
        builder.stream("topic-one")
                .groupByKey()
                .count("table-one");

        builder.stream("topic-two")
                .groupByKey()
                .count("table-two");

        builder.stream("topic-three")
                .groupByKey()
                .count("table-three");

        builder.setApplicationId("appId");

        tp1 = new TopicPartition("topic-one", 0);
        tp2 = new TopicPartition("topic-two", 0);
        tp3 = new TopicPartition("topic-three", 0);

        hostOne = new HostInfo("host-one", 8080);
        hostTwo = new HostInfo("host-two", 9090);
        hostThree = new HostInfo("host-three", 7070);
        hostToPartitions = new HashMap<>();
        hostToPartitions.put(hostOne, Collections.singleton(tp1));
        hostToPartitions.put(hostTwo, Collections.singleton(tp2));
        hostToPartitions.put(hostThree, Collections.singleton(tp3));

        discovery = new KafkaStreamsInstances(hostToPartitions, builder);

    }

    @Test
    public void shouldGetAllStreamInstances() throws Exception {
        final KafkaStreamsInstance one = new KafkaStreamsInstance(hostOne, Collections.singleton("table-one"), Collections.singleton(tp1));
        final KafkaStreamsInstance two = new KafkaStreamsInstance(hostTwo, Collections.singleton("table-two"), Collections.singleton(tp2));
        final KafkaStreamsInstance three = new KafkaStreamsInstance(hostThree, Collections.singleton("table-three"), Collections.singleton(tp3));

        Collection<KafkaStreamsInstance> actual = discovery.getAllStreamsInstances();
        assertEquals(3, actual.size());
        assertTrue(actual.contains(one));
        assertTrue(actual.contains(two));
        assertTrue(actual.contains(three));
    }

    @Test
    public void shouldGetAllStreamsInstancesWithNo() throws Exception {
        builder.stream("topic-four").filter(new Predicate<Object, Object>() {
            @Override
            public boolean test(final Object key, final Object value) {
                return true;
            }
        }).to("some-other-topic");

        final TopicPartition tp4 = new TopicPartition("topic-four", 1);
        final HostInfo hostFour = new HostInfo("host-four", 8080);
        hostToPartitions.put(hostFour, Utils.mkSet(tp4));

        final KafkaStreamsInstance expected = new KafkaStreamsInstance(hostFour, Collections.<String>emptySet(), Collections.singleton(tp4));
        final KafkaStreamsInstances discovery = new KafkaStreamsInstances(hostToPartitions, builder);
        final Collection<KafkaStreamsInstance> actual = discovery.getAllStreamsInstances();
        assertTrue(actual.contains(expected));
    }

    @Test
    public void shouldGetInstancesForStoreName() throws Exception {
        final KafkaStreamsInstance instance = new KafkaStreamsInstance(hostOne, Collections.singleton("table-one"), Collections.singleton(tp1));
        final Collection<KafkaStreamsInstance> actual = discovery.getAllStreamsInstancesWithStore("table-one");
        assertEquals(1, actual.size());
        assertEquals(instance, actual.iterator().next());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsNullOnGetAllInstancesWithStore() throws Exception {
        discovery.getAllStreamsInstancesWithStore(null);
    }

    @Test
    public void shouldReturnEmptyCollectionOnGetAllInstancesWithStoreWhenStoreDoesntExist() throws Exception {
        final Collection<KafkaStreamsInstance> actual = discovery.getAllStreamsInstancesWithStore("not-a-store");
        assertTrue(actual.isEmpty());
    }

    @Test
    public void shouldGetInstanceWithKey() throws Exception {
        final TopicPartition tp4 = new TopicPartition("topic-three", 1);
        hostToPartitions.put(hostTwo, Utils.mkSet(tp2, tp4));
        final KafkaStreamsInstances discovery = new KafkaStreamsInstances(hostToPartitions, builder);

        final KafkaStreamsInstance expected = new KafkaStreamsInstance(hostThree, Collections.singleton("table-three"), Collections.singleton(tp3));

        final KafkaStreamsInstance actual = discovery.getStreamsInstanceWithKey("table-three", "the-key",
                Serdes.String().serializer());

        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetInstanceWithKeyAndCustomPartitioner() throws Exception {
        final TopicPartition tp4 = new TopicPartition("topic-three", 1);
        hostToPartitions.put(hostTwo, Utils.mkSet(tp2, tp4));
        final KafkaStreamsInstances discovery = new KafkaStreamsInstances(hostToPartitions, builder);

        final KafkaStreamsInstance expected = new KafkaStreamsInstance(hostTwo, Collections.singleton("table-three"), Collections.singleton(tp4));

        KafkaStreamsInstance actual = discovery.getStreamsInstanceWithKey("table-three", "the-key", new StreamPartitioner<String, Object>() {
            @Override
            public Integer partition(final String key, final Object value, final int numPartitions) {
                return 1;
            }
        });

        assertEquals(expected, actual);
    }

    @Test
    public void shouldReturnNullOnGetWithKeyWhenStoreDoesntExist() throws Exception {
        final KafkaStreamsInstance actual = discovery.getStreamsInstanceWithKey("not-a-store",
                "key",
                Serdes.String().serializer());
        assertNull(actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenKeyIsNull() throws Exception {
        discovery.getStreamsInstanceWithKey("table-three", null, Serdes.String().serializer());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenSerializerIsNull() throws Exception {
        discovery.getStreamsInstanceWithKey("table-three", "key", (Serializer) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsNull() throws Exception {
        discovery.getStreamsInstanceWithKey(null, "key", Serdes.String().serializer());
    }

    @SuppressWarnings("unchecked")
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfStreamPartitionerIsNull() throws Exception {
        discovery.getStreamsInstanceWithKey(null, "key", (StreamPartitioner) null);
    }


}
