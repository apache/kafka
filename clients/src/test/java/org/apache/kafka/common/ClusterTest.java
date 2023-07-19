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
package org.apache.kafka.common;

import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClusterTest {

    private final static Node[] NODES = new Node[] {
        new Node(0, "localhost", 99),
        new Node(1, "localhost", 100),
        new Node(2, "localhost", 101),
        new Node(11, "localhost", 102)
    };

    private final static String TOPIC_A = "topicA";
    private final static String TOPIC_B = "topicB";
    private final static String TOPIC_C = "topicC";
    private final static String TOPIC_D = "topicD";
    private final static String TOPIC_E = "topicE";

    @Test
    public void testBootstrap() {
        String ipAddress = "140.211.11.105";
        String hostName = "www.example.com";
        Cluster cluster = Cluster.bootstrap(Arrays.asList(
            new InetSocketAddress(ipAddress, 9002),
            new InetSocketAddress(hostName, 9002)
        ));
        Set<String> expectedHosts = Utils.mkSet(ipAddress, hostName);
        Set<String> actualHosts = new HashSet<>();
        for (Node node : cluster.nodes())
            actualHosts.add(node.host());
        assertEquals(expectedHosts, actualHosts);
    }

    @Test
    public void testReturnUnmodifiableCollections() {
        List<PartitionInfo> allPartitions = asList(new PartitionInfo(TOPIC_A, 0, NODES[0], NODES, NODES),
            new PartitionInfo(TOPIC_A, 1, null, NODES, NODES),
            new PartitionInfo(TOPIC_A, 2, NODES[2], NODES, NODES),
            new PartitionInfo(TOPIC_B, 0, null, NODES, NODES),
            new PartitionInfo(TOPIC_B, 1, NODES[0], NODES, NODES),
            new PartitionInfo(TOPIC_C, 0, null, NODES, NODES),
            new PartitionInfo(TOPIC_D, 0, NODES[1], NODES, NODES),
            new PartitionInfo(TOPIC_E, 0, NODES[0], NODES, NODES)
        );
        Set<String> unauthorizedTopics = Utils.mkSet(TOPIC_C);
        Set<String> invalidTopics = Utils.mkSet(TOPIC_D);
        Set<String> internalTopics = Utils.mkSet(TOPIC_E);
        Cluster cluster = new Cluster("clusterId", asList(NODES), allPartitions, unauthorizedTopics,
            invalidTopics, internalTopics, NODES[1]);

        assertThrows(UnsupportedOperationException.class, () -> cluster.invalidTopics().add("foo"));
        assertThrows(UnsupportedOperationException.class, () -> cluster.internalTopics().add("foo"));
        assertThrows(UnsupportedOperationException.class, () -> cluster.unauthorizedTopics().add("foo"));
        assertThrows(UnsupportedOperationException.class, () -> cluster.topics().add("foo"));
        assertThrows(UnsupportedOperationException.class, () -> cluster.nodes().add(NODES[3]));
        assertThrows(UnsupportedOperationException.class, () -> cluster.partitionsForTopic(TOPIC_A).add(
            new PartitionInfo(TOPIC_A, 3, NODES[0], NODES, NODES)));
        assertThrows(UnsupportedOperationException.class, () -> cluster.availablePartitionsForTopic(TOPIC_B).add(
            new PartitionInfo(TOPIC_B, 2, NODES[0], NODES, NODES)));
        assertThrows(UnsupportedOperationException.class, () -> cluster.partitionsForNode(NODES[1].id()).add(
            new PartitionInfo(TOPIC_B, 2, NODES[1], NODES, NODES)));
    }

}
