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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
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

public class StreamsMetadataStateTest {

    private StreamsMetadataState discovery;
    private HostInfo hostOne;
    private HostInfo hostTwo;
    private HostInfo hostThree;
    private TopicPartition topic1P0;
    private TopicPartition topic2P0;
    private TopicPartition topic3P0;
    private Map<HostInfo, Set<TopicPartition>> hostToPartitions;
    private KStreamBuilder builder;
    private TopicPartition topic1P1;
    private TopicPartition topic2P1;
    private TopicPartition topic4P0;

    @Before
    public void before() {
        builder = new KStreamBuilder();
        final KStream<Object, Object> one = builder.stream("topic-one");
        one.groupByKey().count("table-one");

        final KStream<Object, Object> two = builder.stream("topic-two");
        two.groupByKey().count("table-two");

        builder.stream("topic-three")
                .groupByKey()
                .count("table-three");

        builder.merge(one, two).groupByKey().count("merged-table");

        builder.stream("topic-four").mapValues(new ValueMapper<Object, Object>() {
            @Override
            public Object apply(final Object value) {
                return value;
            }
        });

        builder.setApplicationId("appId");

        topic1P0 = new TopicPartition("topic-one", 0);
        topic1P1 = new TopicPartition("topic-one", 1);
        topic2P0 = new TopicPartition("topic-two", 0);
        topic2P1 = new TopicPartition("topic-two", 1);
        topic3P0 = new TopicPartition("topic-three", 0);
        topic4P0 = new TopicPartition("topic-four", 0);

        hostOne = new HostInfo("host-one", 8080);
        hostTwo = new HostInfo("host-two", 9090);
        hostThree = new HostInfo("host-three", 7070);
        hostToPartitions = new HashMap<>();
        hostToPartitions.put(hostOne, Utils.mkSet(topic1P0, topic2P1, topic4P0));
        hostToPartitions.put(hostTwo, Utils.mkSet(topic2P0, topic1P1));
        hostToPartitions.put(hostThree, Collections.singleton(topic3P0));

        discovery = new StreamsMetadataState(builder);
        discovery.onChange(hostToPartitions);
    }

    @Test
    public void shouldGetAllStreamInstances() throws Exception {
        final StreamsMetadata one = new StreamsMetadata(hostOne, Utils.mkSet("table-one", "table-two", "merged-table"),
                Utils.mkSet(topic1P0, topic2P1, topic4P0));
        final StreamsMetadata two = new StreamsMetadata(hostTwo, Utils.mkSet("table-two", "table-one", "merged-table"),
                Utils.mkSet(topic2P0, topic1P1));
        final StreamsMetadata three = new StreamsMetadata(hostThree, Collections.singleton("table-three"),
                Collections.singleton(topic3P0));

        Collection<StreamsMetadata> actual = discovery.getAllMetadata();
        assertEquals(3, actual.size());
        assertTrue("expected " + actual + " to contain " + one, actual.contains(one));
        assertTrue("expected " + actual + " to contain " + two, actual.contains(two));
        assertTrue("expected " + actual + " to contain " + three, actual.contains(three));
    }

    @Test
    public void shouldGetAllStreamsInstancesWithNoStores() throws Exception {
        builder.stream("topic-five").filter(new Predicate<Object, Object>() {
            @Override
            public boolean test(final Object key, final Object value) {
                return true;
            }
        }).to("some-other-topic");

        final TopicPartition tp5 = new TopicPartition("topic-five", 1);
        final HostInfo hostFour = new HostInfo("host-four", 8080);
        hostToPartitions.put(hostFour, Utils.mkSet(tp5));
        discovery.onChange(hostToPartitions);

        final StreamsMetadata expected = new StreamsMetadata(hostFour, Collections.<String>emptySet(),
                Collections.singleton(tp5));
        final Collection<StreamsMetadata> actual = discovery.getAllMetadata();
        assertTrue("expected " + actual + " to contain " + expected, actual.contains(expected));
    }

    @Test
    public void shouldGetInstancesForStoreName() throws Exception {
        final StreamsMetadata one = new StreamsMetadata(hostOne, Utils.mkSet("table-one", "table-two", "merged-table"),
                Utils.mkSet(topic1P0, topic2P1, topic4P0));
        final StreamsMetadata two = new StreamsMetadata(hostTwo, Utils.mkSet("table-two", "table-one", "merged-table"),
                Utils.mkSet(topic2P0, topic1P1));
        final Collection<StreamsMetadata> actual = discovery.getAllMetadataForStore("table-one");
        assertEquals(2, actual.size());
        assertTrue("expected " + actual + " to contain " + one, actual.contains(one));
        assertTrue("expected " + actual + " to contain " + two, actual.contains(two));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsNullOnGetAllInstancesWithStore() throws Exception {
        discovery.getAllMetadataForStore(null);
    }

    @Test
    public void shouldReturnEmptyCollectionOnGetAllInstancesWithStoreWhenStoreDoesntExist() throws Exception {
        final Collection<StreamsMetadata> actual = discovery.getAllMetadataForStore("not-a-store");
        assertTrue(actual.isEmpty());
    }

    @Test
    public void shouldGetInstanceWithKey() throws Exception {
        final TopicPartition tp4 = new TopicPartition("topic-three", 1);
        hostToPartitions.put(hostTwo, Utils.mkSet(topic2P0, tp4));
        discovery.onChange(hostToPartitions);

        final StreamsMetadata expected = new StreamsMetadata(hostThree, Collections.singleton("table-three"),
                Collections.singleton(topic3P0));

        final StreamsMetadata actual = discovery.getMetadataWithKey("table-three", "the-key",
                Serdes.String().serializer());

        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetInstanceWithKeyAndCustomPartitioner() throws Exception {
        final TopicPartition tp4 = new TopicPartition("topic-three", 1);
        hostToPartitions.put(hostTwo, Utils.mkSet(topic2P0, tp4));

        discovery.onChange(hostToPartitions);

        final StreamsMetadata expected = new StreamsMetadata(hostTwo, Utils.mkSet("table-two", "table-three", "merged-table"),
                Utils.mkSet(topic2P0, tp4));

        StreamsMetadata actual = discovery.getMetadataWithKey("table-three", "the-key", new StreamPartitioner<String, Object>() {
            @Override
            public Integer partition(final String key, final Object value, final int numPartitions) {
                return 1;
            }
        });
        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetInstanceWithKeyWithMergedStreams() throws Exception {
        final TopicPartition topic2P2 = new TopicPartition("topic-two", 2);
        hostToPartitions.put(hostTwo, Utils.mkSet(topic2P0, topic1P1, topic2P2));
        discovery.onChange(hostToPartitions);

        final StreamsMetadata expected = new StreamsMetadata(hostTwo, Utils.mkSet("table-two", "table-one", "merged-table"),
                Utils.mkSet(topic2P0, topic1P1, topic2P2));

        final StreamsMetadata actual = discovery.getMetadataWithKey("merged-table", "123", new StreamPartitioner<String, Object>() {
            @Override
            public Integer partition(final String key, final Object value, final int numPartitions) {
                return 2;
            }
        });

        assertEquals(expected, actual);

    }

    @Test
    public void shouldReturnNullOnGetWithKeyWhenStoreDoesntExist() throws Exception {
        final StreamsMetadata actual = discovery.getMetadataWithKey("not-a-store",
                "key",
                Serdes.String().serializer());
        assertNull(actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenKeyIsNull() throws Exception {
        discovery.getMetadataWithKey("table-three", null, Serdes.String().serializer());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenSerializerIsNull() throws Exception {
        discovery.getMetadataWithKey("table-three", "key", (Serializer) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsNull() throws Exception {
        discovery.getMetadataWithKey(null, "key", Serdes.String().serializer());
    }

    @SuppressWarnings("unchecked")
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfStreamPartitionerIsNull() throws Exception {
        discovery.getMetadataWithKey(null, "key", (StreamPartitioner) null);
    }


}
