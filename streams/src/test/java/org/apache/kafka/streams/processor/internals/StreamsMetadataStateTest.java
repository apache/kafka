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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StreamsMetadataStateTest {

    private StreamsMetadataState metadataState;
    private StreamsMetadataState metadataState1;
    private HostInfo hostOne;
    private HostInfo hostTwo;
    private HostInfo hostThree;
    private TopicPartition topic1P0;
    private TopicPartition topic2P0;
    private TopicPartition topic3P0;
    private TaskId task0p0;
    private TaskId task0p1;
    private TaskId task2p0;
    private TaskId task1p0;
    private TaskId task4p0;
    private Map<HostInfo, Set<TopicPartition>> hostToPartitions;
    private Map<HostInfo, Set<TaskId>> hostToTasks;
    private StreamsBuilder builder;
    private TopicPartition topic1P1;
    private TopicPartition topic2P1;
    private TopicPartition topic4P0;
    private InternalTopologyBuilder internalTopologyBuilder;
    private Cluster cluster;
    private final String globalTable = "global-table";
    private StreamPartitioner<String, Object> partitioner;

    @Before
    public void before() {
        builder = new StreamsBuilder();
        final KStream<Object, Object> one = builder.stream("topic-one");
        one.groupByKey().count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("table-one"));

        final KStream<Object, Object> two = builder.stream("topic-two");
        two.groupByKey().count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("table-two"));

        builder.stream("topic-three")
                .groupByKey()
                .count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("table-three"));

        one.merge(two).groupByKey().count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("merged-table"));

        builder.stream("topic-four").mapValues(new ValueMapper<Object, Object>() {
            @Override
            public Object apply(final Object value) {
                return value;
            }
        });

        builder.globalTable("global-topic",
                            Consumed.with(null, null),
                            Materialized.<Object, Object, KeyValueStore<Bytes, byte[]>>as(globalTable));

        builder.stream("topic-five").filter(new Predicate<Object, Object>() {
            @Override
            public boolean test(final Object key, final Object value) {
                return true;
            }
        }).to("some-other-topic");

        TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("appId");

        topic1P0 = new TopicPartition("topic-one", 0);
        topic1P1 = new TopicPartition("topic-one", 1);
        topic2P0 = new TopicPartition("topic-two", 0);
        topic2P1 = new TopicPartition("topic-two", 1);
        topic3P0 = new TopicPartition("topic-three", 0);
        topic4P0 = new TopicPartition("topic-four", 0);

        task0p0 = new TaskId(0, 0);
        task0p1 = new TaskId(0, 1);
        task1p0 = new TaskId(1, 0);
        task2p0 = new TaskId(2, 0);
        task4p0 = new TaskId(4, 0);


        hostOne = new HostInfo("host-one", 8080);
        hostTwo = new HostInfo("host-two", 9090);
        hostThree = new HostInfo("host-three", 7070);
        hostToPartitions = new HashMap<>();
        hostToPartitions.put(hostOne, Utils.mkSet(topic1P0, topic2P0, topic4P0));
        hostToPartitions.put(hostTwo, Utils.mkSet(topic2P1, topic1P1));
        hostToPartitions.put(hostThree, Collections.singleton(topic3P0));

        hostToTasks = new HashMap<>();
        hostToTasks.put(hostOne, Utils.mkSet(task0p0, task2p0));
        hostToTasks.put(hostTwo, Utils.mkSet(task0p1));
        hostToTasks.put(hostThree, Utils.mkSet(task1p0));

        internalTopologyBuilder = new InternalTopologyBuilder();

        final List<PartitionInfo> partitionInfos = Arrays.asList(
                new PartitionInfo("topic-one", 0, null, null, null),
                new PartitionInfo("topic-one", 1, null, null, null),
                new PartitionInfo("topic-two", 0, null, null, null),
                new PartitionInfo("topic-two", 1, null, null, null),
                new PartitionInfo("topic-three", 0, null, null, null),
                new PartitionInfo("topic-four", 0, null, null, null));

        cluster = new Cluster(null, Collections.<Node>emptyList(), partitionInfos, Collections.<String>emptySet(), Collections.<String>emptySet());
        metadataState = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.build()), hostOne);
        metadataState.onChangeOldVersion(hostToPartitions, cluster, 3);
        partitioner = new StreamPartitioner<String, Object>() {
            @Override
            public Integer partition(final String topic, final String key, final Object value, final int numPartitions) {
                return 1;
            }
        };
        metadataState1 = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.build()), hostOne);
        metadataState1.onChangeNewVersion(hostToTasks, cluster, 4);
        partitioner = new StreamPartitioner<String, Object>() {
            @Override
            public Integer partition(final String topic, final String key, final Object value, final int numPartitions) {
                return 1;
            }
        };
    }

    @Test
    public void shouldNotThrowNPEWhenOnChangeNotCalled() {
        new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.build()), hostOne).getAllMetadataForStore("store");
    }

    @Test
    public void shouldGetAllStreamInstances() {
        final StreamsMetadata one = new StreamsMetadata(hostOne, Utils.mkSet(globalTable, "table-one", "table-two", "merged-table"),
                null, Utils.mkSet(task0p0, task2p0));
        final StreamsMetadata two = new StreamsMetadata(hostTwo, Utils.mkSet(globalTable, "table-two", "table-one", "merged-table"),
                null, Utils.mkSet(task0p1));
        final StreamsMetadata three = new StreamsMetadata(hostThree, Utils.mkSet(globalTable, "table-three"),
                null, Utils.mkSet(task1p0));

        final Collection<StreamsMetadata> actual = metadataState1.getAllMetadata();
        assertEquals(3, actual.size());
        assertTrue("expected " + actual + " to contain " + one, actual.contains(one));
        assertTrue("expected " + actual + " to contain " + two, actual.contains(two));
        assertTrue("expected " + actual + " to contain " + three, actual.contains(three));
    }

    @Test
    public void shouldGetAllStreamsInstancesWithNoStoresOldVersion() {

        final TopicPartition tp5 = new TopicPartition("topic-five", 1);
        final HostInfo hostFour = new HostInfo("host-four", 8080);
        hostToPartitions.put(hostFour, Utils.mkSet(tp5));

        metadataState.onChangeOldVersion(hostToPartitions, cluster.withPartitions(Collections.singletonMap(tp5, new PartitionInfo("topic-five", 1, null, null, null))), 3);

        final StreamsMetadata expected = new StreamsMetadata(hostFour, Collections.singleton(globalTable),
                Collections.singleton(tp5), null);
        final Collection<StreamsMetadata> actual = metadataState.getAllMetadata();
        assertTrue("expected " + actual + " to contain " + expected, actual.contains(expected));
    }

    @Test
    public void shouldGetAllStreamsInstancesWithNoStoresNewVersion() {

        final TaskId t5 = new TaskId(4, 1);
        final HostInfo hostFour = new HostInfo("host-four", 8080);
        hostToTasks.put(hostFour, Utils.mkSet(t5));
        final TopicPartition tp5 = new TopicPartition("topic-five", 1);

        metadataState1.onChangeNewVersion(hostToTasks, cluster.withPartitions(Collections.singletonMap(tp5, new PartitionInfo("topic-five", 1, null, null, null))), 4);

        final StreamsMetadata expected = new StreamsMetadata(hostFour, Collections.singleton(globalTable),
                null, Collections.singleton(t5));
        final Collection<StreamsMetadata> actual = metadataState1.getAllMetadata();
        assertTrue("expected " + actual + " to contain " + expected, actual.contains(expected));
    }

    @Test
    public void shouldGetInstancesForStoreName() {
        final StreamsMetadata one = new StreamsMetadata(hostOne, Utils.mkSet(globalTable, "table-one", "table-two", "merged-table"), null,
                Utils.mkSet(task0p0, task2p0));
        final StreamsMetadata two = new StreamsMetadata(hostTwo, Utils.mkSet(globalTable, "table-two", "table-one", "merged-table"), null,
                Utils.mkSet(task0p1));
        final Collection<StreamsMetadata> actual = metadataState1.getAllMetadataForStore("table-one");
        assertEquals(2, actual.size());
        assertTrue("expected " + actual + " to contain " + one, actual.contains(one));
        assertTrue("expected " + actual + " to contain " + two, actual.contains(two));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfStoreNameIsNullOnGetAllInstancesWithStore() {
        metadataState1.getAllMetadataForStore(null);
    }

    @Test
    public void shouldReturnEmptyCollectionOnGetAllInstancesWithStoreWhenStoreDoesntExist() {
        final Collection<StreamsMetadata> actual = metadataState1.getAllMetadataForStore("not-a-store");
        assertTrue(actual.isEmpty());
    }

    @Test
    public void shouldGetInstanceWithKey() {
        final TopicPartition tp4 = new TopicPartition("topic-three", 1);
        final TaskId task1p1 = new TaskId(1, 1);
        hostToTasks.put(hostTwo, Utils.mkSet(task1p1));

        metadataState1.onChangeNewVersion(hostToTasks, cluster.withPartitions(Collections.singletonMap(tp4, new PartitionInfo("topic-three", 1, null, null, null))), 4);

        final StreamsMetadata expected = new StreamsMetadata(hostThree, Utils.mkSet(globalTable, "table-three"), null,
                Collections.singleton(task1p0));

        final StreamsMetadata actual = metadataState1.getMetadataWithKey("table-three",
                                                                    "the-key",
                                                                    Serdes.String().serializer());

        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetInstanceWithKeyAndCustomPartitioner() {
        final TopicPartition tp4 = new TopicPartition("topic-three", 1);
        final TaskId task1p1 = new TaskId(1, 1);
        hostToTasks.put(hostTwo, Utils.mkSet(task0p0, task1p1));

        metadataState1.onChangeNewVersion(hostToTasks, cluster.withPartitions(Collections.singletonMap(tp4, new PartitionInfo("topic-three", 1, null, null, null))), 4);

        final StreamsMetadata expected = new StreamsMetadata(hostTwo, Utils.mkSet(globalTable, "table-one", "table-two", "table-three", "merged-table"), null, Utils.mkSet(task0p0, task1p1));

        final StreamsMetadata actual = metadataState1.getMetadataWithKey("table-three", "the-key", partitioner);
        assertEquals(expected, actual);
    }

    @Test
    public void shouldReturnNotAvailableWhenClusterIsEmpty() {
        metadataState1.onChangeNewVersion(Collections.<HostInfo, Set<TaskId>>emptyMap(), Cluster.empty(), 4);
        final StreamsMetadata result = metadataState1.getMetadataWithKey("table-one", "a", Serdes.String().serializer());
        assertEquals(StreamsMetadata.NOT_AVAILABLE, result);
    }

    @Test
    public void shouldGetInstanceWithKeyWithMergedStreams() {
        final TopicPartition topic2P2 = new TopicPartition("topic-two", 2);
        final TaskId task0p2 = new TaskId(0, 2);

        hostToTasks.put(hostTwo, Utils.mkSet(task0p0, task0p1, task0p2));
        metadataState1.onChangeNewVersion(hostToTasks, cluster.withPartitions(Collections.singletonMap(topic2P2, new PartitionInfo("topic-two", 2, null, null, null))), 4);

        final StreamsMetadata expected = new StreamsMetadata(hostTwo, Utils.mkSet("global-table", "table-two", "table-one", "merged-table"),
                null, Utils.mkSet(task0p0, task0p1, task0p2));

        final StreamsMetadata actual = metadataState1.getMetadataWithKey("merged-table", "123", new StreamPartitioner<String, Object>() {
            @Override
            public Integer partition(final String topic, final String key, final Object value, final int numPartitions) {
                return 2;
            }
        });

        assertEquals(expected, actual);

    }

    @Test
    public void shouldReturnNullOnGetWithKeyWhenStoreDoesntExist() {
        final StreamsMetadata actual = metadataState1.getMetadataWithKey("not-a-store",
                "key",
                Serdes.String().serializer());
        assertNull(actual);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowWhenKeyIsNull() {
        metadataState1.getMetadataWithKey("table-three", null, Serdes.String().serializer());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowWhenSerializerIsNull() {
        metadataState1.getMetadataWithKey("table-three", "key", (Serializer) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfStoreNameIsNull() {
        metadataState1.getMetadataWithKey(null, "key", Serdes.String().serializer());
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowIfStreamPartitionerIsNull() {
        metadataState1.getMetadataWithKey(null, "key", (StreamPartitioner) null);
    }

    @Test
    public void shouldHaveGlobalStoreInAllMetadata() {
        final Collection<StreamsMetadata> metadata = metadataState1.getAllMetadataForStore(globalTable);
        assertEquals(3, metadata.size());
        for (final StreamsMetadata streamsMetadata : metadata) {
            assertTrue(streamsMetadata.stateStoreNames().contains(globalTable));
        }
    }

    @Test
    public void shouldGetMyMetadataForGlobalStoreWithKey() {
        final StreamsMetadata metadata = metadataState1.getMetadataWithKey(globalTable, "key", Serdes.String().serializer());
        assertEquals(hostOne, metadata.hostInfo());
    }

    @Test
    public void shouldGetAnyHostForGlobalStoreByKeyIfMyHostUnknown() {
        final StreamsMetadataState streamsMetadataState = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.build()), StreamsMetadataState.UNKNOWN_HOST);
        streamsMetadataState.onChangeNewVersion(hostToTasks, cluster, 4);
        assertNotNull(streamsMetadataState.getMetadataWithKey(globalTable, "key", Serdes.String().serializer()));
    }

    @Test
    public void shouldGetMyMetadataForGlobalStoreWithKeyAndPartitioner() {
        final StreamsMetadata metadata = metadataState1.getMetadataWithKey(globalTable, "key", partitioner);
        assertEquals(hostOne, metadata.hostInfo());
    }

    @Test
    public void shouldGetAnyHostForGlobalStoreByKeyAndPartitionerIfMyHostUnknown() {
        final StreamsMetadataState streamsMetadataState = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.build()), StreamsMetadataState.UNKNOWN_HOST);
        streamsMetadataState.onChangeNewVersion(hostToTasks, cluster, 4);
        assertNotNull(streamsMetadataState.getMetadataWithKey(globalTable, "key", partitioner));
    }


}
