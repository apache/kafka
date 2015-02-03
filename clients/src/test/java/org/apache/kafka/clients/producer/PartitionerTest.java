/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.kafka.clients.producer.internals.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

public class PartitionerTest {

    private byte[] key = "key".getBytes();
    private Partitioner partitioner = new Partitioner();
    private Node node0 = new Node(0, "localhost", 99);
    private Node node1 = new Node(1, "localhost", 100);
    private Node node2 = new Node(2, "localhost", 101);
    private Node[] nodes = new Node[] {node0, node1, node2};
    private String topic = "test";
    private List<PartitionInfo> partitions = asList(new PartitionInfo(topic, 0, node0, nodes, nodes),
                                                    new PartitionInfo(topic, 1, node1, nodes, nodes),
                                                    new PartitionInfo(topic, 2, null, nodes, nodes));
    private Cluster cluster = new Cluster(asList(node0, node1, node2), partitions);

    @Test
    public void testUserSuppliedPartitioning() {
        assertEquals("If the user supplies a partition we should use it.", 0, partitioner.partition("test", key, 0, cluster));
    }

    @Test
    public void testKeyPartitionIsStable() {
        int partition = partitioner.partition("test", key, null, cluster);
        assertEquals("Same key should yield same partition", partition, partitioner.partition("test", key, null, cluster));
    }

    @Test
    public void testRoundRobinIsStable() {
        int startPart = partitioner.partition("test", null, null, cluster);
        for (int i = 1; i <= 100; i++) {
            int partition = partitioner.partition("test", null, null, cluster);
            assertEquals("Should yield a different partition each call with round-robin partitioner", partition, (startPart + i) % 2);
        }
    }

    @Test
    public void testRoundRobinWithDownNode() {
        for (int i = 0; i < partitions.size(); i++) {
            int part = partitioner.partition("test", null, null, cluster);
            assertTrue("We should never choose a leader-less node in round robin", part >= 0 && part < 2);
        }
    }
}
