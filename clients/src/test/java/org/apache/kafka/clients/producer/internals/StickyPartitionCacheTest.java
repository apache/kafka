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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class StickyPartitionCacheTest {
    private final static Node[] NODES = new Node[] {
        new Node(0, "localhost", 99),
        new Node(1, "localhost", 100),
        new Node(2, "localhost", 101),
        new Node(11, "localhost", 102)
    };
    final static String TOPIC_A = "topicA";
    final static String TOPIC_B = "topicB";
    final static String TOPIC_C = "topicC";

    @Test
    public void testStickyPartitionCache() {
        List<PartitionInfo> allPartitions = asList(new PartitionInfo(TOPIC_A, 0, NODES[0], NODES, NODES),
            new PartitionInfo(TOPIC_A, 1, NODES[1], NODES, NODES),
            new PartitionInfo(TOPIC_A, 2, NODES[2], NODES, NODES),
            new PartitionInfo(TOPIC_B, 0, NODES[0], NODES, NODES)
        );
        Cluster testCluster = new Cluster("clusterId", asList(NODES), allPartitions,
            Collections.emptySet(), Collections.emptySet());
        StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

        int partA = stickyPartitionCache.partition(TOPIC_A, testCluster);
        assertEquals(partA, stickyPartitionCache.partition(TOPIC_A, testCluster));

        int partB = stickyPartitionCache.partition(TOPIC_B, testCluster);
        assertEquals(partB, stickyPartitionCache.partition(TOPIC_B, testCluster));

        int changedPartA = stickyPartitionCache.nextPartition(TOPIC_A, testCluster, partA);
        assertEquals(changedPartA, stickyPartitionCache.partition(TOPIC_A, testCluster));
        assertNotEquals(partA, changedPartA);
        int changedPartA2 = stickyPartitionCache.partition(TOPIC_A, testCluster);
        assertEquals(changedPartA2, changedPartA);

        // We do not want to change partitions because the previous partition does not match the current sticky one.
        int changedPartA3 = stickyPartitionCache.nextPartition(TOPIC_A, testCluster, partA);
        assertEquals(changedPartA3, changedPartA2);

        // Check that we can still use the partitioner when there is only one partition
        int changedPartB = stickyPartitionCache.nextPartition(TOPIC_B, testCluster, partB);
        assertEquals(changedPartB, stickyPartitionCache.partition(TOPIC_B, testCluster));
    }
    
    @Test
    public void unavailablePartitionsTest() {
        // Partition 1 in topic A and partition 0 in topic B are unavailable partitions.
        List<PartitionInfo> allPartitions = asList(new PartitionInfo(TOPIC_A, 0, NODES[0], NODES, NODES),
            new PartitionInfo(TOPIC_A, 1, null, NODES, NODES),
            new PartitionInfo(TOPIC_A, 2, NODES[2], NODES, NODES),
            new PartitionInfo(TOPIC_B, 0, null, NODES, NODES),
            new PartitionInfo(TOPIC_B, 1, NODES[0], NODES, NODES),
            new PartitionInfo(TOPIC_C, 0, null, NODES, NODES)
        );
        
        Cluster testCluster = new Cluster("clusterId", asList(NODES[0], NODES[1], NODES[2]), allPartitions,
            Collections.emptySet(), Collections.emptySet());
        StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();
        
        // Assure we never choose partition 1 because it is unavailable.
        int partA = stickyPartitionCache.partition(TOPIC_A, testCluster);
        assertNotEquals(1, partA);
        for (int aPartitions = 0; aPartitions < 100; aPartitions++) {
            partA = stickyPartitionCache.nextPartition(TOPIC_A, testCluster, partA);
            assertNotEquals(1, stickyPartitionCache.partition(TOPIC_A, testCluster));
        }
        
        // Assure we always choose partition 1 for topic B.
        int partB = stickyPartitionCache.partition(TOPIC_B, testCluster);
        assertEquals(1, partB);
        for (int bPartitions = 0; bPartitions < 100; bPartitions++) {
            partB = stickyPartitionCache.nextPartition(TOPIC_B, testCluster, partB);
            assertEquals(1, stickyPartitionCache.partition(TOPIC_B, testCluster));
        }
        
        // Assure that we still choose the partition when there are no partitions available.
        int partC = stickyPartitionCache.partition(TOPIC_C, testCluster);
        assertEquals(0, partC);
        partC = stickyPartitionCache.nextPartition(TOPIC_C, testCluster, partC);
        assertEquals(0, partC);
    }
}
