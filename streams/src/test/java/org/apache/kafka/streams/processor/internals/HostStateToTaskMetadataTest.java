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
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.HostState;
import org.apache.kafka.streams.state.TaskMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class HostStateToTaskMetadataTest {

    private HostStateToTaskMetadata discovery;
    private HostState hostOne;
    private HostState hostTwo;
    private HostState hostThree;
    private TopicPartition tp1;
    private TopicPartition tp2;
    private TopicPartition tp3;
    private Map<HostState, Set<TopicPartition>> hostToPartitions;
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

        hostOne = new HostState("host-one", 8080);
        hostTwo = new HostState("host-two", 9090);
        hostThree = new HostState("host-three", 7070);
        hostToPartitions = new HashMap<>();
        hostToPartitions.put(hostOne, Collections.singleton(tp1));
        hostToPartitions.put(hostTwo, Collections.singleton(tp2));
        hostToPartitions.put(hostThree, Collections.singleton(tp3));

        discovery = new HostStateToTaskMetadata(hostToPartitions, builder);

    }

    @Test
    public void shouldGetAllTasks() throws Exception {
        final Map<HostState, TaskMetadata> expected = new HashMap<>();
        expected.put(hostOne, new TaskMetadata(Collections.singleton("table-one"), Collections.singleton(tp1)));
        expected.put(hostTwo, new TaskMetadata(Collections.singleton("table-two"), Collections.singleton(tp2)));
        expected.put(hostThree, new TaskMetadata(Collections.singleton("table-three"), Collections.singleton(tp3)));

        final Map<HostState, TaskMetadata> actual = discovery.getAllTasks();
        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetTasksForStoreName() throws Exception {
        final Map<HostState, TaskMetadata> expected = new HashMap<>();
        expected.put(hostOne, new TaskMetadata(Collections.singleton("table-one"), Collections.singleton(tp1)));

        final Map<HostState, TaskMetadata> actual = discovery.getAllTasksWithStore("table-one");
        assertEquals(actual, expected);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsNullOnGetAllTasksWithStore() throws Exception {
        discovery.getAllTasksWithStore(null);
    }

    @Test
    public void shouldReturnEmptyMapOnGetAllTasksWithStoreWhenStoreDoesntExist() throws Exception {
        final Map<HostState, TaskMetadata> allTasksWithStore = discovery.getAllTasksWithStore("not-a-store");
        assertEquals(Collections.emptyMap(), allTasksWithStore);
    }

    @Test
    public void shouldGetTaskWithKey() throws Exception {
        final TopicPartition tp4 = new TopicPartition("topic-three", 1);
        hostToPartitions.put(hostTwo, Utils.mkSet(tp2, tp4));
        final HostStateToTaskMetadata discovery = new HostStateToTaskMetadata(hostToPartitions, builder);
        final Map<HostState, TaskMetadata> expected = new HashMap<>();

        expected.put(hostThree, new TaskMetadata(Collections.singleton("table-three"), Collections.singleton(tp3)));

        final Map<HostState, TaskMetadata> actual = discovery.getTaskWithKey("table-three", "the-key",
                Serdes.String().serializer());

        assertEquals(expected, actual);
    }

    @Test
    public void shouldGetTaskWithKeyAndCustomPartitioner() throws Exception {
        final TopicPartition tp4 = new TopicPartition("topic-three", 1);
        hostToPartitions.put(hostTwo, Utils.mkSet(tp2, tp4));
        final HostStateToTaskMetadata discovery = new HostStateToTaskMetadata(hostToPartitions, builder);
        final Map<HostState, TaskMetadata> expected = new HashMap<>();

        expected.put(hostTwo, new TaskMetadata(Collections.singleton("table-three"), Collections.singleton(tp4)));

        final Map<HostState, TaskMetadata> actual = discovery.getTaskWithKey("table-three", "the-key", new StreamPartitioner<String, Object>() {
            @Override
            public Integer partition(final String key, final Object value, final int numPartitions) {
                return 1;
            }
        });

        assertEquals(expected, actual);
    }

    @Test
    public void shouldReturnEmptyMapOnGetTaskWithKeyWhenStoreDoesntExist() throws Exception {
        final Map<HostState, TaskMetadata> allTasksWithStore = discovery.getTaskWithKey("not-a-store",
                "key",
                Serdes.String().serializer());
        assertEquals(Collections.emptyMap(), allTasksWithStore);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenKeyIsNull() throws Exception {
        discovery.getTaskWithKey("table-three", null, Serdes.String().serializer());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenSerializerIsNull() throws Exception {
        discovery.getTaskWithKey("table-three", "key", (Serializer) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsNull() throws Exception {
        discovery.getTaskWithKey(null, "key", Serdes.String().serializer());
    }

    @SuppressWarnings("unchecked")
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfStreamPartitionerIsNull() throws Exception {
        discovery.getTaskWithKey(null, "key", (StreamPartitioner) null);
    }


}
