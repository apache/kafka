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

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClusterTest {

    private static final Node[] NODES = new Node[] {
        new Node(0, "localhost", 99),
        new Node(1, "localhost", 100),
        new Node(2, "localhost", 101),
        new Node(11, "localhost", 102)
    };

    private static final String TOPIC_A = "topicA";
    private static final String TOPIC_B = "topicB";
    private static final String TOPIC_C = "topicC";
    private static final String TOPIC_D = "topicD";
    private static final String TOPIC_E = "topicE";

    @Test
    public void testBootstrap() {
        String ipAddress = "140.211.11.105";
        String hostName = "www.example.com";
        Cluster cluster = Cluster.bootstrap(Arrays.asList(
            new InetSocketAddress(ipAddress, 9002),
            new InetSocketAddress(hostName, 9002)
        ));
        Set<String> expectedHosts = Set.of(ipAddress, hostName);
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
        Set<String> unauthorizedTopics = Set.of(TOPIC_C);
        Set<String> invalidTopics = Set.of(TOPIC_D);
        Set<String> internalTopics = Set.of(TOPIC_E);
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

    @Test
    public void testNotEquals() {
        String clusterId1 = "clusterId1";
        String clusterId2 = "clusterId2";
        Node node0 = new Node(0, "host0", 100);
        Node node1 = new Node(1, "host1", 100);
        Set<PartitionInfo> partitions1 = Collections.singleton(new PartitionInfo("topic1", 0, node0, new Node[]{node0, node1}, new Node[]{node0}));
        Set<PartitionInfo> partitions2 = Collections.singleton(new PartitionInfo("topic2", 0, node0, new Node[]{node1, node0}, new Node[]{node1}));
        Set<String> unauthorizedTopics1 = Collections.singleton("topic1");
        Set<String> unauthorizedTopics2 = Collections.singleton("topic2");
        Set<String> invalidTopics1 = Collections.singleton("topic1");
        Set<String> invalidTopics2 = Collections.singleton("topic2");
        Set<String> internalTopics1 = Collections.singleton("topic3");
        Set<String> internalTopics2 = Collections.singleton("topic4");
        Node controller1 = new Node(2, "host2", 100);
        Node controller2 = new Node(3, "host3", 100); 
        Map<String, Uuid> topicIds1 = Collections.singletonMap("topic1", Uuid.randomUuid());
        Map<String, Uuid> topicIds2 = Collections.singletonMap("topic2", Uuid.randomUuid());

        Cluster cluster1 = new Cluster(clusterId1, Collections.singletonList(node0), partitions1,
            unauthorizedTopics1, invalidTopics1, internalTopics1, controller1, topicIds1);
        Cluster differentTopicIds = new Cluster(clusterId1, Collections.singletonList(node0), partitions1,
            unauthorizedTopics1, invalidTopics1, internalTopics1, controller1, topicIds2);
        Cluster differentController = new Cluster(clusterId1, Collections.singletonList(node0), partitions1,
            unauthorizedTopics1, invalidTopics1, internalTopics1, controller2, topicIds1);
        Cluster differentInternalTopics = new Cluster(clusterId1, Collections.singletonList(node0), partitions1,
            unauthorizedTopics1, invalidTopics1, internalTopics2, controller1, topicIds1);
        Cluster differentInvalidTopics = new Cluster(clusterId1, Collections.singletonList(node0), partitions1,
            unauthorizedTopics1, invalidTopics2, internalTopics1, controller1, topicIds1);
        Cluster differentUnauthorizedTopics = new Cluster(clusterId1, Collections.singletonList(node0), partitions1,
            unauthorizedTopics2, invalidTopics1, internalTopics1, controller1, topicIds1);
        Cluster differentPartitions = new Cluster(clusterId1, Collections.singletonList(node0), partitions2,
            unauthorizedTopics1, invalidTopics1, internalTopics1, controller1, topicIds1);
        Cluster differentNodes = new Cluster(clusterId1, Arrays.asList(node0, node1), partitions1,
            unauthorizedTopics1, invalidTopics1, internalTopics1, controller1, topicIds1);
        Cluster differentClusterId = new Cluster(clusterId2, Collections.singletonList(node0), partitions1,
            unauthorizedTopics1, invalidTopics1, internalTopics1, controller1, topicIds1);

        assertNotEquals(cluster1, differentTopicIds);
        assertNotEquals(cluster1, differentController);
        assertNotEquals(cluster1, differentInternalTopics);
        assertNotEquals(cluster1, differentInvalidTopics);
        assertNotEquals(cluster1, differentUnauthorizedTopics);
        assertNotEquals(cluster1, differentPartitions);
        assertNotEquals(cluster1, differentNodes);
        assertNotEquals(cluster1, differentClusterId);
    }

    @Test
    public void testEquals() {
        String clusterId1 = "clusterId1";
        Node node1 = new Node(1, "host0", 100);
        Node node1duplicate = new Node(1, "host0", 100);
        Set<PartitionInfo> partitions1 = Collections.singleton(new PartitionInfo("topic1", 0, node1, new Node[]{node1}, new Node[]{node1}));
        Set<PartitionInfo> partitions1duplicate = Collections.singleton(new PartitionInfo("topic1", 0, node1duplicate, new Node[]{node1duplicate}, new Node[]{node1duplicate}));
        Set<String> unauthorizedTopics1 = Collections.singleton("topic1");
        Set<String> invalidTopics1 = Collections.singleton("topic1");
        Set<String> internalTopics1 = Collections.singleton("topic3");
        Node controller1 = new Node(2, "host0", 100);
        Node controller1duplicate = new Node(2, "host0", 100);
        Uuid topicId1 = Uuid.randomUuid();
        Map<String, Uuid> topicIds1 = Collections.singletonMap("topic1", topicId1);
        Map<String, Uuid> topicIds1duplicate = Collections.singletonMap("topic1", topicId1);

        Cluster cluster1 = new Cluster(clusterId1, Collections.singletonList(node1), partitions1, unauthorizedTopics1,
            invalidTopics1, internalTopics1, controller1, topicIds1);
        Cluster cluster1duplicate = new Cluster(clusterId1, Collections.singletonList(node1duplicate), partitions1duplicate,
            unauthorizedTopics1, invalidTopics1, internalTopics1, controller1duplicate, topicIds1duplicate);
        assertEquals(cluster1, cluster1duplicate);
    }
}
