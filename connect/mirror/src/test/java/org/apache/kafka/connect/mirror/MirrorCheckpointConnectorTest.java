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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.Checkpoint.CONSUMER_GROUP_ID_KEY;
import static org.apache.kafka.connect.mirror.MirrorUtils.PARTITION_KEY;
import static org.apache.kafka.connect.mirror.MirrorUtils.TOPIC_KEY;
import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class MirrorCheckpointConnectorTest {

    private static final String CONSUMER_GROUP = "consumer-group-1";
    private static final Map<String, ?> SOURCE_OFFSET = MirrorUtils.wrapOffset(0);

    @Test
    public void testMirrorCheckpointConnectorDisabled() {
        // disable the checkpoint emission
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(
            makeProps("emit.checkpoints.enabled", "false"));

        Set<String> knownConsumerGroups = new HashSet<>();
        knownConsumerGroups.add(CONSUMER_GROUP);

        // MirrorCheckpointConnector as minimum to run taskConfig()
        // expect no task will be created
        List<Map<String, String>> output = new MirrorCheckpointConnector(knownConsumerGroups, config).taskConfigs(1);
        assertEquals(0, output.size(), "MirrorCheckpointConnector not disabled");
    }

    @Test
    public void testMirrorCheckpointConnectorEnabled() {
        // enable the checkpoint emission
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(
                makeProps("emit.checkpoints.enabled", "true"));

        Set<String> knownConsumerGroups = new HashSet<>();
        knownConsumerGroups.add(CONSUMER_GROUP);
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups,
                config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect 1 task will be created
        assertEquals(1, output.size(),
                "MirrorCheckpointConnectorEnabled for " + CONSUMER_GROUP + " has incorrect size");
        assertEquals(CONSUMER_GROUP, output.get(0).get(MirrorCheckpointConfig.TASK_CONSUMER_GROUPS),
                "MirrorCheckpointConnectorEnabled for " + CONSUMER_GROUP + " failed");
    }

    @Test
    public void testNoConsumerGroup() {
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps());
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(new HashSet<>(), config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect no task will be created
        assertEquals(0, output.size(), "ConsumerGroup shouldn't exist");
    }

    @Test
    public void testConsumerGroupInitializeTimeout() {
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps());
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(null, config);

        assertThrows(
                RetriableException.class,
                () -> connector.taskConfigs(1),
                "taskConfigs should throw exception when initial loading ConsumerGroup timeout"
        );
    }

    @Test
    public void testReplicationDisabled() {
        // disable the replication
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps("enabled", "false"));

        Set<String> knownConsumerGroups = new HashSet<>();
        knownConsumerGroups.add(CONSUMER_GROUP);
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups, config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect no task will be created
        assertEquals(0, output.size(), "Replication isn't disabled");
    }

    @Test
    public void testReplicationEnabled() {
        // enable the replication
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps("enabled", "true"));

        Set<String> knownConsumerGroups = new HashSet<>();
        knownConsumerGroups.add(CONSUMER_GROUP);
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups, config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect 1 task will be created
        assertEquals(1, output.size(), "Replication for consumer-group-1 has incorrect size");
        assertEquals(CONSUMER_GROUP, output.get(0).get(MirrorCheckpointConfig.TASK_CONSUMER_GROUPS),
                "Replication for consumer-group-1 failed");
    }

    @Test
    public void testFindConsumerGroups() throws Exception {
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps());
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(Collections.emptySet(), config);
        connector = spy(connector);

        Collection<ConsumerGroupListing> groups = Arrays.asList(
                new ConsumerGroupListing("g1", true),
                new ConsumerGroupListing("g2", false));
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        doReturn(groups).when(connector).listConsumerGroups();
        doReturn(true).when(connector).shouldReplicateByTopicFilter(anyString());
        doReturn(true).when(connector).shouldReplicateByGroupFilter(anyString());

        Map<String, Map<TopicPartition, OffsetAndMetadata>> groupToOffsets = new HashMap<>();
        groupToOffsets.put("g1", offsets);
        groupToOffsets.put("g2", offsets);
        doReturn(groupToOffsets).when(connector).listConsumerGroupOffsets(anyList());
        Set<String> groupFound = connector.findConsumerGroups();

        Set<String> expectedGroups = groups.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toSet());
        assertEquals(expectedGroups, groupFound,
                "Expected groups are not the same as findConsumerGroups");

        doReturn(false).when(connector).shouldReplicateByTopicFilter(anyString());
        Set<String> topicFilterGroupFound = connector.findConsumerGroups();
        assertEquals(Collections.emptySet(), topicFilterGroupFound);
    }

    @Test
    public void testFindConsumerGroupsInCommonScenarios() throws Exception {
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps());
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(Collections.emptySet(), config);
        connector = spy(connector);

        Collection<ConsumerGroupListing> groups = Arrays.asList(
                new ConsumerGroupListing("g1", true),
                new ConsumerGroupListing("g2", false),
                new ConsumerGroupListing("g3", false),
                new ConsumerGroupListing("g4", false));
        Map<TopicPartition, OffsetAndMetadata> offsetsForGroup1 = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> offsetsForGroup2 = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> offsetsForGroup3 = new HashMap<>();
        offsetsForGroup1.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        offsetsForGroup1.put(new TopicPartition("t2", 0), new OffsetAndMetadata(0));
        offsetsForGroup2.put(new TopicPartition("t2", 0), new OffsetAndMetadata(0));
        offsetsForGroup2.put(new TopicPartition("t3", 0), new OffsetAndMetadata(0));
        offsetsForGroup3.put(new TopicPartition("t3", 0), new OffsetAndMetadata(0));
        doReturn(groups).when(connector).listConsumerGroups();
        doReturn(false).when(connector).shouldReplicateByTopicFilter("t1");
        doReturn(true).when(connector).shouldReplicateByTopicFilter("t2");
        doReturn(false).when(connector).shouldReplicateByTopicFilter("t3");
        doReturn(true).when(connector).shouldReplicateByGroupFilter("g1");
        doReturn(true).when(connector).shouldReplicateByGroupFilter("g2");
        doReturn(true).when(connector).shouldReplicateByGroupFilter("g3");
        doReturn(false).when(connector).shouldReplicateByGroupFilter("g4");

        Map<String, Map<TopicPartition, OffsetAndMetadata>> groupToOffsets = new HashMap<>();
        groupToOffsets.put("g1", offsetsForGroup1);
        groupToOffsets.put("g2", offsetsForGroup2);
        groupToOffsets.put("g3", offsetsForGroup3);
        doReturn(groupToOffsets).when(connector).listConsumerGroupOffsets(Arrays.asList("g1", "g2", "g3"));

        Set<String> groupFound = connector.findConsumerGroups();
        Set<String> verifiedSet = new HashSet<>();
        verifiedSet.add("g1");
        verifiedSet.add("g2");
        assertEquals(verifiedSet, groupFound);
    }

    @Test
    public void testAlterOffsetsIncorrectPartitionKey() {
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector();
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, Collections.singletonMap(
                Collections.singletonMap("unused_partition_key", "unused_partition_value"),
                SOURCE_OFFSET
        )));

        // null partitions are invalid
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, Collections.singletonMap(
                null,
                SOURCE_OFFSET
        )));
    }

    @Test
    public void testAlterOffsetsMissingPartitionKey() {
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector();

        Function<Map<String, ?>, Boolean> alterOffsets = partition -> connector.alterOffsets(null, Collections.singletonMap(
                partition,
                SOURCE_OFFSET
        ));

        Map<String, ?> validPartition = sourcePartition("consumer-app-1", "t", 3);
        // Sanity check to make sure our valid partition is actually valid
        assertTrue(alterOffsets.apply(validPartition));

        for (String key : Arrays.asList(CONSUMER_GROUP_ID_KEY, TOPIC_KEY, PARTITION_KEY)) {
            Map<String, ?> invalidPartition = new HashMap<>(validPartition);
            invalidPartition.remove(key);
            assertThrows(ConnectException.class, () -> alterOffsets.apply(invalidPartition));
        }
    }

    @Test
    public void testAlterOffsetsInvalidPartitionPartition() {
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector();
        Map<String, Object> partition = sourcePartition("consumer-app-2", "t", 3);
        partition.put(PARTITION_KEY, "a string");
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, Collections.singletonMap(
                partition,
                SOURCE_OFFSET
        )));
    }

    @Test
    public void testAlterOffsetsMultiplePartitions() {
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector();

        Map<String, ?> partition1 = sourcePartition("consumer-app-3", "t1", 0);
        Map<String, ?> partition2 = sourcePartition("consumer-app-4", "t1", 1);

        Map<Map<String, ?>, Map<String, ?>> offsets = new HashMap<>();
        offsets.put(partition1, SOURCE_OFFSET);
        offsets.put(partition2, SOURCE_OFFSET);

        assertTrue(connector.alterOffsets(null, offsets));
    }

    @Test
    public void testAlterOffsetsIncorrectOffsetKey() {
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector();

        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                sourcePartition("consumer-app-5", "t1", 2),
                Collections.singletonMap("unused_offset_key", 0)
        );
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, offsets));
    }

    @Test
    public void testAlterOffsetsOffsetValues() {
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector();

        Function<Object, Boolean> alterOffsets = offset -> connector.alterOffsets(null, Collections.singletonMap(
                sourcePartition("consumer-app-6", "t", 5),
                Collections.singletonMap(MirrorUtils.OFFSET_KEY, offset)
        ));

        assertThrows(ConnectException.class, () -> alterOffsets.apply("nan"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(null));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(new Object()));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(3.14));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(-420));
        assertThrows(ConnectException.class, () -> alterOffsets.apply("-420"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply("10"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(10));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(((long) Integer.MAX_VALUE) + 1));
        assertTrue(() -> alterOffsets.apply(0));
    }

    @Test
    public void testSuccessfulAlterOffsets() {
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector();

        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                sourcePartition("consumer-app-7", "t2", 0),
                SOURCE_OFFSET
        );

        // Expect no exception to be thrown when a valid offsets map is passed. An empty offsets map is treated as valid
        // since it could indicate that the offsets were reset previously or that no offsets have been committed yet
        // (for a reset operation)
        assertTrue(connector.alterOffsets(null, offsets));
        assertTrue(connector.alterOffsets(null, Collections.emptyMap()));
    }

    @Test
    public void testAlterOffsetsTombstones() {
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector();

        Function<Map<String, ?>, Boolean> alterOffsets = partition -> connector.alterOffsets(
                null,
                Collections.singletonMap(partition, null)
        );

        Map<String, Object> partition = sourcePartition("consumer-app-2", "t", 3);
        assertTrue(() -> alterOffsets.apply(partition));
        partition.put(PARTITION_KEY, "a string");
        assertTrue(() -> alterOffsets.apply(partition));
        partition.remove(PARTITION_KEY);
        assertTrue(() -> alterOffsets.apply(partition));

        assertTrue(() -> alterOffsets.apply(null));
        assertTrue(() -> alterOffsets.apply(Collections.emptyMap()));
        assertTrue(() -> alterOffsets.apply(Collections.singletonMap("unused_partition_key", "unused_partition_value")));
    }

    private static Map<String, Object> sourcePartition(String consumerGroupId, String topic, int partition) {
        Map<String, Object> result = new HashMap<>();
        result.put(CONSUMER_GROUP_ID_KEY, consumerGroupId);
        result.put(TOPIC_KEY, topic);
        result.put(PARTITION_KEY, partition);
        return result;
    }

}
