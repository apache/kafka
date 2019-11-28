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

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class DefaultPartitionerTest {
    private byte[] keyBytes = "key".getBytes();
    private final static Node[] NODES = new Node[] {
        new Node(0, "localhost", 99),
        new Node(1, "localhost", 100),
        new Node(12, "localhost", 101)
    };
    private final static String TOPIC = "test";
    // Intentionally make the partition list not in partition order to test the edge cases.
    private final static List<PartitionInfo> PARTITIONS = asList(new PartitionInfo(TOPIC, 1, null, NODES, NODES),
                                                    new PartitionInfo(TOPIC, 2, NODES[1], NODES, NODES),
                                                    new PartitionInfo(TOPIC, 0, NODES[0], NODES, NODES));

    @Test
    public void testKeyPartitionIsStable() {
        final Partitioner partitioner = new DefaultPartitioner();
        final Cluster cluster = new Cluster("clusterId", asList(NODES), PARTITIONS,
            Collections.<String>emptySet(), Collections.<String>emptySet());
        int partition = partitioner.partition("test",  null, keyBytes, null, null, cluster);
        assertEquals("Same key should yield same partition", partition, partitioner.partition("test", null, keyBytes, null, null, cluster));
    }
}
