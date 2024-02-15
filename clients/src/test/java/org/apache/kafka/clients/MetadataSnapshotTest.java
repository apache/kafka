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
package org.apache.kafka.clients;

import java.util.OptionalInt;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataSnapshotTest {

    @Test
    public void testMissingLeaderEndpoint() {
        // Although the broker attempts to ensure leader information is available, the
        // client metadata cache may retain partition metadata across multiple responses.
        // For example, separate responses may contain conflicting leader epochs for
        // separate partitions and the client will always retain the highest.

        TopicPartition topicPartition = new TopicPartition("topic", 0);

        MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(
                Errors.NONE,
                topicPartition,
                Optional.of(5),
                Optional.of(10),
                Arrays.asList(5, 6, 7),
                Arrays.asList(5, 6, 7),
                Collections.emptyList());

        Map<Integer, Node> nodesById = new HashMap<>();
        nodesById.put(6, new Node(6, "localhost", 2077));
        nodesById.put(7, new Node(7, "localhost", 2078));
        nodesById.put(8, new Node(8, "localhost", 2079));

        MetadataSnapshot cache = new MetadataSnapshot("clusterId",
                nodesById,
                Collections.singleton(partitionMetadata),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                null,
                Collections.emptyMap());

        Cluster cluster = cache.cluster();
        assertNull(cluster.leaderFor(topicPartition));

        PartitionInfo partitionInfo = cluster.partition(topicPartition);
        Map<Integer, Node> replicas = Arrays.stream(partitionInfo.replicas())
                .collect(Collectors.toMap(Node::id, Function.identity()));
        assertNull(partitionInfo.leader());
        assertEquals(3, replicas.size());
        assertTrue(replicas.get(5).isEmpty());
        assertEquals(nodesById.get(6), replicas.get(6));
        assertEquals(nodesById.get(7), replicas.get(7));
    }

    @Test
    public void testMergeWithThatPreExistingPartitionIsRetainedPostMerge() {
        // Set up a Metadata Cache with 1 topic partition belong topic1.
        String topic1 = "topic1";
        TopicPartition topic1Partition = new TopicPartition(topic1, 1);
        MetadataResponse.PartitionMetadata partitionMetadata1 = new MetadataResponse.PartitionMetadata(
            Errors.NONE,
            topic1Partition,
            Optional.of(5),
            Optional.of(10),
            Arrays.asList(5, 6, 7),
            Arrays.asList(5, 6, 7),
            Collections.emptyList());

        Map<Integer, Node> nodesById = new HashMap<>();
        nodesById.put(6, new Node(6, "localhost", 2077));
        nodesById.put(7, new Node(7, "localhost", 2078));
        nodesById.put(8, new Node(8, "localhost", 2079));

        Map<String, Uuid> topicsIds = new HashMap<>();
        Uuid topic1Id = Uuid.randomUuid();
        topicsIds.put(topic1Partition.topic(), topic1Id);

        MetadataSnapshot cache = new MetadataSnapshot("clusterId",
            nodesById,
            Collections.singleton(partitionMetadata1),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            null,
            topicsIds);

        Cluster cluster = cache.cluster();
        assertEquals(1, cluster.topics().size());
        assertEquals(topic1Id, cluster.topicId(topic1));
        assertEquals(topic1, cluster.topicName(topic1Id));

        // Merge the metadata cache with a new partition topic2Partition.
        String topic2 = "topic2";
        TopicPartition topic2Partition = new TopicPartition(topic2, 2);
        MetadataResponse.PartitionMetadata partitionMetadata2 = new MetadataResponse.PartitionMetadata(
            Errors.NONE,
            topic2Partition,
            Optional.of(5),
            Optional.of(10),
            Arrays.asList(5, 6, 7),
            Arrays.asList(5, 6, 7),
            Collections.emptyList());
        topicsIds = new HashMap<>();
        Uuid topic2Id = Uuid.randomUuid();
        topicsIds.put(topic2Partition.topic(), topic2Id);
        cache = cache.mergeWith("clusterId", nodesById, Collections.singleton(partitionMetadata2),
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, topicsIds, (topic, retain) -> true);
        cluster = cache.cluster();

        // Verify topic1Partition is retained & topic2Partition is added.
        assertEquals(2, cluster.topics().size());

        assertEquals(topic1Id, cluster.topicId(topic1));
        assertEquals(topic1, cluster.topicName(topic1Id));

        assertEquals(topic2Id, cluster.topicId(topic2));
        assertEquals(topic2, cluster.topicName(topic2Id));
    }

    @Test
    public void testTopicNamesCacheBuiltFromTopicIds() {
        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put("topic1", Uuid.randomUuid());
        topicIds.put("topic2", Uuid.randomUuid());

        MetadataSnapshot cache = new MetadataSnapshot("clusterId",
                Collections.singletonMap(6, new Node(6, "localhost", 2077)),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                null,
                topicIds);

        Map<Uuid, String> expectedNamesCache =
                topicIds.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue,
                        Map.Entry::getKey));
        assertEquals(expectedNamesCache, cache.topicNames());
    }

    @Test
    public void testEmptyTopicNamesCacheBuiltFromTopicIds() {
        Map<String, Uuid> topicIds = new HashMap<>();

        MetadataSnapshot cache = new MetadataSnapshot("clusterId",
                Collections.singletonMap(6, new Node(6, "localhost", 2077)),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                null,
                topicIds);
        assertEquals(Collections.emptyMap(), cache.topicNames());
    }

    @Test
    public void testLeaderEpochFor() {
        // Setup partition 0 with a leader-epoch of 10.
        TopicPartition topicPartition1 = new TopicPartition("topic", 0);
        MetadataResponse.PartitionMetadata partitionMetadata1 = new MetadataResponse.PartitionMetadata(
            Errors.NONE,
            topicPartition1,
            Optional.of(5),
            Optional.of(10),
            Arrays.asList(5, 6, 7),
            Arrays.asList(5, 6, 7),
            Collections.emptyList());

        // Setup partition 1 with an unknown leader epoch.
        TopicPartition topicPartition2 = new TopicPartition("topic", 1);
        MetadataResponse.PartitionMetadata partitionMetadata2 = new MetadataResponse.PartitionMetadata(
            Errors.NONE,
            topicPartition2,
            Optional.of(5),
            Optional.empty(),
            Arrays.asList(5, 6, 7),
            Arrays.asList(5, 6, 7),
            Collections.emptyList());

        Map<Integer, Node> nodesById = new HashMap<>();
        nodesById.put(5, new Node(5, "localhost", 2077));
        nodesById.put(6, new Node(6, "localhost", 2078));
        nodesById.put(7, new Node(7, "localhost", 2079));

        MetadataSnapshot cache = new MetadataSnapshot("clusterId",
            nodesById,
            Arrays.asList(partitionMetadata1, partitionMetadata2),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            null,
            Collections.emptyMap());

        assertEquals(OptionalInt.of(10), cache.leaderEpochFor(topicPartition1));

        assertEquals(OptionalInt.empty(), cache.leaderEpochFor(topicPartition2));

        assertEquals(OptionalInt.empty(), cache.leaderEpochFor(new TopicPartition("topic_missing", 0)));
    }

}
