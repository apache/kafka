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
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class StickyPartitionCacheTest {
    private Node node0 = new Node(0, "localhost", 99);
    private Node node1 = new Node(1, "localhost", 100);
    private Node node2 = new Node(2, "localhost", 101);
    private Node[] nodes = new Node[] {node0, node1, node2};

    @Test
    public void testStickyPartitionCache() {
        final String topicA = "topicA";
        final String topicB = "topicB";

        List<PartitionInfo> allPartitions = asList(new PartitionInfo(topicA, 0, node0, nodes, nodes),
            new PartitionInfo(topicA, 1, node1, nodes, nodes),
            new PartitionInfo(topicA, 2, node2, nodes, nodes),
            new PartitionInfo(topicB, 0, node0, nodes, nodes)
        );
        Cluster testCluster = new Cluster("clusterId", asList(node0, node1, node2), allPartitions,
            Collections.<String>emptySet(), Collections.<String>emptySet());
        StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

        int partA = stickyPartitionCache.partition(topicA, testCluster);
        assertEquals(partA, stickyPartitionCache.partition(topicA, testCluster));

        int partB = stickyPartitionCache.partition(topicB, testCluster);
        assertEquals(partB, stickyPartitionCache.partition(topicB, testCluster));

        int changedPartA = stickyPartitionCache.nextPartition(topicA, testCluster, partA);
        assertEquals(changedPartA, stickyPartitionCache.partition(topicA, testCluster));
        assertNotEquals(partA, changedPartA);
        int changedPartA2 = stickyPartitionCache.partition(topicA, testCluster);
        assertEquals(changedPartA2, changedPartA);

        // We do not want to change partitions because the previous partition does not match the current sticky one.
        int changedPartA3 = stickyPartitionCache.nextPartition(topicA, testCluster, partA);
        assertEquals(changedPartA3, changedPartA2);

        // Check that the we can still use the partitioner when there is only one partition
        int changedPartB = stickyPartitionCache.nextPartition(topicB, testCluster, partB);
        assertEquals(changedPartB, stickyPartitionCache.partition(topicB, testCluster));
    }
    
    @Test
    public void unavailablePartitionsTest() {
        final String topicA = "topicA";
        final String topicB = "topicB";
        final String topicC = "topicC";
        
        // Partition 1 in topic A and partition 0 in topic B are unavailable partitions.
        List<PartitionInfo> allPartitions = asList(new PartitionInfo(topicA, 0, node0, nodes, nodes),
            new PartitionInfo(topicA, 1, null, nodes, nodes),
            new PartitionInfo(topicA, 2, node2, nodes, nodes),
            new PartitionInfo(topicB, 0, null, nodes, nodes),
            new PartitionInfo(topicB, 1, node0, nodes, nodes),
            new PartitionInfo(topicC, 0, null, nodes, nodes)
        );
        
        Cluster testCluster = new Cluster("clusterId", asList(node0, node1), allPartitions,
            Collections.<String>emptySet(), Collections.<String>emptySet());
        StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();
        
        // Assure we never choose partition 1 because it is unavailable.
        int partA = stickyPartitionCache.partition(topicA, testCluster);
        assertNotEquals(1, partA);
        for (int aPartitions = 0; aPartitions < 100; aPartitions++) {
            partA = stickyPartitionCache.nextPartition(topicA, testCluster, partA);
            assertNotEquals(1, stickyPartitionCache.partition(topicA, testCluster));
        }
        
        // Assure we always choose partition 1 for topic B.
        int partB = stickyPartitionCache.partition(topicB, testCluster);
        assertEquals(1, partB);
        for (int bPartitions = 0; bPartitions < 100; bPartitions++) {
            partB = stickyPartitionCache.nextPartition(topicB, testCluster, partB);
            assertEquals(1, stickyPartitionCache.partition(topicB, testCluster));
        }
        
        // Assure that we still choose the partition when there are no partitions available.
        int partC = stickyPartitionCache.partition(topicC, testCluster);
        assertEquals(0, partC);
        partC = stickyPartitionCache.nextPartition(topicC, testCluster, partC);
        assertEquals(0, partC);
    }
}
