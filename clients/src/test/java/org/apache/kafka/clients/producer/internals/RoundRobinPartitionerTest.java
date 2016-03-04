/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the RoundRobinPartitioner.
 */
public class RoundRobinPartitionerTest {
    // Define our topic
    private String topic = "test";

    // Create our partitioner
    private Partitioner partitioner = new RoundRobinPartitioner();

    // Create a few nodes
    private Node node0 = new Node(0, "localhost", 99);
    private Node node1 = new Node(1, "localhost", 100);
    private Node node2 = new Node(2, "localhost", 101);
    private Node node3 = new Node(3, "localhost", 102);

    /**
     * Tests that we round robin the available partitions.
     */
    @Test
    public void testRoundRobinWithUnavailablePartitions() {
        Node[] nodes = new Node[] {node0, node1, node2};

        // Intentionally make the partition list not in partition order to test the edge cases.
        List<PartitionInfo> partitions = asList(
                new PartitionInfo(topic, 1, null, nodes, nodes),
                new PartitionInfo(topic, 2, node1, nodes, nodes),
                new PartitionInfo(topic, 0, node0, nodes, nodes));
        Cluster cluster = new Cluster(asList(node0, node1, node2), partitions, Collections.<String>emptySet());

        // When there are some unavailable partitions, we want to make sure that (1) we always pick an available partition,
        // and (2) the available partitions are selected in a round robin way.
        int countForPart0 = 0;
        int countForPart2 = 0;
        for (int i = 1; i <= 100; i++) {
            int part = partitioner.partition("test", null, null, null, null, cluster);
            assertTrue("We should never choose a leader-less node in round robin", part == 0 || part == 2);
            if (part == 0) {
                countForPart0++;
            } else {
                countForPart2++;
            }
        }
        assertEquals("The distribution between two available partitions should be even", countForPart0, countForPart2);
    }

    /**
     * Tests that we round robin the available partitions.
     */
    @Test
    public void testRoundRobinPartitions() {
        Node[] nodes = new Node[] {node0, node1, node2, node3};

        // Intentionally make the partition list not in partition order to test the edge cases.
        List<PartitionInfo> partitions = asList(
                new PartitionInfo(topic, 1, node2, nodes, nodes),
                new PartitionInfo(topic, 2, node1, nodes, nodes),
                new PartitionInfo(topic, 0, node0, nodes, nodes),
                new PartitionInfo(topic, 3, node3, nodes, nodes));

        Cluster cluster = new Cluster(asList(node0, node1, node2, node3), partitions, Collections.<String>emptySet());

        // When there are some unavailable partitions, we want to make sure that (1) we always pick an available partition,
        // and (2) the available partitions are selected in a round robin way.
        int[] nodeCounts = {0, 0, 0, 0};
        for (int i = 1; i <= 100; i++) {
            int part = partitioner.partition("test", null, null, null, null, cluster);
            assertTrue("Node should exist", part >= 0 && part <= 3);
            nodeCounts[part]++;
        }
        assertEquals("Distribution should be event for all nodes", 25, nodeCounts[0]);
        assertEquals("Distribution should be event for all nodes", 25, nodeCounts[1]);
        assertEquals("Distribution should be event for all nodes", 25, nodeCounts[2]);
        assertEquals("Distribution should be event for all nodes", 25, nodeCounts[3]);
    }
}