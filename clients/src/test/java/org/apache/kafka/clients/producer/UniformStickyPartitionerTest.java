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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UniformStickyPartitionerTest {
    private final static Node[] NODES = new Node[] {
        new Node(0, "localhost", 99),
        new Node(1, "localhost", 100),
        new Node(2, "localhost", 101)
    };

    private final static String TOPIC_A = "TOPIC_A";
    private final static String TOPIC_B = "TOPIC_B";

    @SuppressWarnings("deprecation")
    @Test
    public void testRoundRobinWithUnavailablePartitions() {
        // Intentionally make the partition list not in partition order to test the edge
        // cases.
        List<PartitionInfo> partitions = asList(
                new PartitionInfo("test", 1, null, NODES, NODES),
                new PartitionInfo("test", 2, NODES[1], NODES, NODES),
                new PartitionInfo("test", 0, NODES[0], NODES, NODES));
        // When there are some unavailable partitions, we want to make sure that (1) we
        // always pick an available partition,
        // and (2) the available partitions are selected in a sticky way.
        int countForPart0 = 0;
        int countForPart2 = 0;
        int part = 0;
        Partitioner partitioner = new UniformStickyPartitioner();
        Cluster cluster = new Cluster("clusterId", asList(NODES[0], NODES[1], NODES[2]), partitions,
            Collections.emptySet(), Collections.emptySet());
        for (int i = 0; i < 50; i++) {
            part = partitioner.partition("test", null, null, null, null, cluster);
            assertTrue(part == 0 || part == 2, "We should never choose a leader-less node in round robin");
            if (part == 0)
                countForPart0++;
            else
                countForPart2++;
        }
        // Simulates switching the sticky partition on a new batch.
        partitioner.onNewBatch("test", cluster, part);       
        for (int i = 1; i <= 50; i++) {
            part = partitioner.partition("test", null, null, null, null, cluster);
            assertTrue(part == 0 || part == 2, "We should never choose a leader-less node in round robin");
            if (part == 0)
                countForPart0++;
            else
                countForPart2++;
        }
        assertEquals(countForPart0, countForPart2, "The distribution between two available partitions should be even");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRoundRobinWithKeyBytes() {
        List<PartitionInfo> allPartitions = asList(new PartitionInfo(TOPIC_A, 0, NODES[0], NODES, NODES),
                new PartitionInfo(TOPIC_A, 1, NODES[1], NODES, NODES), new PartitionInfo(TOPIC_A, 2, NODES[1], NODES, NODES),
                new PartitionInfo(TOPIC_B, 0, NODES[0], NODES, NODES));
        Cluster testCluster = new Cluster("clusterId", asList(NODES[0], NODES[1], NODES[2]), allPartitions,
                Collections.emptySet(), Collections.emptySet());

        final Map<Integer, Integer> partitionCount = new HashMap<>();

        final byte[] keyBytes = "key".getBytes();
        int partition = 0;
        Partitioner partitioner = new UniformStickyPartitioner();
        for (int i = 0; i < 30; ++i) {
            partition = partitioner.partition(TOPIC_A, null, keyBytes, null, null, testCluster);
            Integer count = partitionCount.get(partition);
            if (null == count)
                count = 0;
            partitionCount.put(partition, count + 1);

            if (i % 5 == 0) {
                partitioner.partition(TOPIC_B, null, keyBytes, null, null, testCluster);
            }
        }
        // Simulate a batch filling up and switching the sticky partition.
        partitioner.onNewBatch(TOPIC_A, testCluster, partition);
        partitioner.onNewBatch(TOPIC_B, testCluster, 0);
        
        // Save old partition to ensure that the wrong partition does not trigger a new batch.
        int oldPart = partition; 
        
        for (int i = 0; i < 30; ++i) {
            partition = partitioner.partition(TOPIC_A, null, keyBytes, null, null, testCluster);
            Integer count = partitionCount.get(partition);
            if (null == count)
                count = 0;
            partitionCount.put(partition, count + 1);

            if (i % 5 == 0) {
                partitioner.partition(TOPIC_B, null, keyBytes, null, null, testCluster);
            }
        }
        
        int newPart = partition;
        
        // Attempt to switch the partition with the wrong previous partition. Sticky partition should not change.
        partitioner.onNewBatch(TOPIC_A, testCluster, oldPart);
        
        for (int i = 0; i < 30; ++i) {
            partition = partitioner.partition(TOPIC_A, null, keyBytes, null, null, testCluster);
            Integer count = partitionCount.get(partition);
            if (null == count)
                count = 0;
            partitionCount.put(partition, count + 1);

            if (i % 5 == 0) {
                partitioner.partition(TOPIC_B, null, keyBytes, null, null, testCluster);
            }
        } 
               
        assertEquals(30, partitionCount.get(oldPart).intValue());
        assertEquals(60, partitionCount.get(newPart).intValue());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRoundRobinWithNullKeyBytes() {
        List<PartitionInfo> allPartitions = asList(new PartitionInfo(TOPIC_A, 0, NODES[0], NODES, NODES),
                new PartitionInfo(TOPIC_A, 1, NODES[1], NODES, NODES), new PartitionInfo(TOPIC_A, 2, NODES[1], NODES, NODES),
                new PartitionInfo(TOPIC_B, 0, NODES[0], NODES, NODES));
        Cluster testCluster = new Cluster("clusterId", asList(NODES[0], NODES[1], NODES[2]), allPartitions,
                Collections.emptySet(), Collections.emptySet());

        final Map<Integer, Integer> partitionCount = new HashMap<>();

        int partition = 0;
        Partitioner partitioner = new UniformStickyPartitioner();
        for (int i = 0; i < 30; ++i) {
            partition = partitioner.partition(TOPIC_A, null, null, null, null, testCluster);
            Integer count = partitionCount.get(partition);
            if (null == count)
                count = 0;
            partitionCount.put(partition, count + 1);

            if (i % 5 == 0) {
                partitioner.partition(TOPIC_B, null, null, null, null, testCluster);
            }
        }
        // Simulate a batch filling up and switching the sticky partition.
        partitioner.onNewBatch(TOPIC_A, testCluster, partition);
        partitioner.onNewBatch(TOPIC_B, testCluster, 0);

        // Save old partition to ensure that the wrong partition does not trigger a new batch.
        int oldPart = partition;

        for (int i = 0; i < 30; ++i) {
            partition = partitioner.partition(TOPIC_A, null, null, null, null, testCluster);
            Integer count = partitionCount.get(partition);
            if (null == count)
                count = 0;
            partitionCount.put(partition, count + 1);

            if (i % 5 == 0) {
                partitioner.partition(TOPIC_B, null, null, null, null, testCluster);
            }
        }

        int newPart = partition;

        // Attempt to switch the partition with the wrong previous partition. Sticky partition should not change.
        partitioner.onNewBatch(TOPIC_A, testCluster, oldPart);

        for (int i = 0; i < 30; ++i) {
            partition = partitioner.partition(TOPIC_A, null, null, null, null, testCluster);
            Integer count = partitionCount.get(partition);
            if (null == count)
                count = 0;
            partitionCount.put(partition, count + 1);

            if (i % 5 == 0) {
                partitioner.partition(TOPIC_B, null, null, null, null, testCluster);
            }
        }

        assertEquals(30, partitionCount.get(oldPart).intValue());
        assertEquals(60, partitionCount.get(newPart).intValue());
    }
}

