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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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

public class MetadataCacheTest {

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

        MetadataCache cache = new MetadataCache("clusterId",
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

}
